//! Selected commands on the original SDK task context.
use super::*;

impl PiControlHost<'_> {
    pub(crate) async fn run_selected(
        &self,
        context: Arc<TaskContext>,
        fence: LeaseFence,
        command: WorkflowCommand,
        payload: Value,
        reconcile: bool,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<TaskCommandProgress> {
        let caller = self.caller(&context, &fence, work).await?;
        let owner = &caller.0;
        let scratch = &caller.1.binding.scratch.volume;
        let mut io = HistoryWork::new(PreparationIo::from_native_work(work)?);
        if context.output_volume() != Some(scratch)
            || !context
                .scope()
                .grants()
                .contains(&scratch.capability(VolumeOperation::Write, &mut Some(&mut io))?)
        {
            return Err(denied(
                "selected output lacks its sealed private scratch WRITE grant",
            ));
        }
        owner.require_volume_grant(scratch, VolumeOperation::Write, &mut Some(&mut io))?;
        let bytes = crate::pi_root_provider::guarded_pi_bytes(
            &payload,
            crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64,
            work,
        )?;
        drop(payload);
        let input = crate::pi_root_provider::guarded_pi_decode::<PiSelectedCommand>(
            &bytes.0,
            crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64,
            crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64 * 8,
            acyclic_harness::wire::ErrorCode::Invalid,
            acyclic_harness::JsonValueSchema::None,
            SELECTED_DIAGNOSTIC_LABEL_BYTES,
            work,
        )?;
        drop(bytes);
        let input = &input.0;
        if input.caller != owner.task_id() || input.sdk_operation.is_empty() {
            return Err(denied(
                "selected command changed its caller or SDK operation",
            ));
        }
        let operation = original_operation(
            self.protected.policy().root_task,
            input.caller,
            &input.sdk_operation,
            work,
        )?;
        let role = match command.kind.as_str() {
            "acyclic.pi.inspect" => "selected-inspect",
            "acyclic.pi.diff" => "selected-diff",
            "acyclic.pi.import" => "selected-import",
            "acyclic.pi.writeback" if input.approval_digest.is_some() => {
                "selected-writeback-publish"
            }
            "acyclic.pi.writeback" => "selected-writeback",
            _ => return Err(Error::Unsupported("not a selected Pi command".into())),
        };
        if command.operation_id != derived(operation, role, work)? {
            return Err(denied("selected command changed its effect operation"));
        }
        let cursor = input
            .cursor
            .as_deref()
            .map(|cursor| decode_cursor(cursor, work))
            .transpose()?;
        let cursor = cursor.as_ref().map(|(cursor, _)| cursor);
        if cursor.is_some_and(|cursor| {
            cursor.caller != input.caller
                || cursor.source != input.source
                || cursor.kind != command.kind
                || cursor.directory != input.directory
                || input
                    .root
                    .as_deref()
                    .is_some_and(|root| root != cursor.root)
        }) {
            return Err(denied(
                "selected cursor changed its caller, source, directory, root or kind",
            ));
        }
        let writeback = command.kind == "acyclic.pi.writeback";
        let selection_operation = derived(
            operation,
            if writeback {
                "selected-writeback"
            } else {
                "selected-import"
            },
            work,
        )?;
        let import_operation = if writeback {
            derived(selection_operation, "writeback-import", work)?
        } else {
            selection_operation
        };
        let prior = if matches!(
            command.kind.as_str(),
            "acyclic.pi.import" | "acyclic.pi.writeback"
        ) {
            self.read_import_intent(&context, import_operation, work)
                .await?
        } else {
            None
        };
        let pinned = cursor
            .map(|cursor| match &cursor.cursor {
                PageCursor::Inspect(cursor) => cursor.snapshot,
                PageCursor::Diff(cursor) => cursor.after,
            })
            .or_else(|| prior.as_ref().map(|(intent, _)| intent.source_snapshot));
        let source = self
            .selected(
                &context,
                owner,
                input.source,
                input.root.as_deref(),
                pinned,
                operation,
                work,
            )
            .await?;
        if cursor.is_some_and(|cursor| cursor.root != source.key) {
            return Err(denied("cursor changed its resolved source root"));
        }
        let maximum = input.maximum_entries.unwrap_or(128);
        match command.kind.as_str() {
            "acyclic.pi.inspect" => {
                let next = match cursor.map(|cursor| &cursor.cursor) {
                    Some(PageCursor::Inspect(cursor)) => Some(cursor),
                    None => None,
                    _ => return Err(denied("diff continuation used for inspection")),
                };
                let page = source
                    .inspect(&input.directory, next, maximum, work)
                    .await?;
                let cursor = page
                    .1
                    .as_ref()
                    .map(|cursor| {
                        encode_cursor(
                            input,
                            &source.key,
                            &command.kind,
                            PageCursor::Inspect(cursor),
                            work,
                        )
                    })
                    .transpose()?;
                ready(
                    &PiPageResponse {
                        source: input.source,
                        root: &source.key,
                        volume: Some(&source.volume),
                        before_root: None,
                        before_snapshot: None,
                        snapshot: source.snapshot,
                        directory: &input.directory,
                        entries: &page.0,
                        cursor: cursor.as_ref().map(|(cursor, _)| cursor.as_str()),
                    },
                    work,
                )
            }
            "acyclic.pi.diff" => {
                let native = self.native()?;
                let baseline = native
                    .frozen_source(input.source)
                    .ok_or_else(|| denied("diff lost the child's original frozen source"))?;
                let task = native
                    .selected_source_task(input.source)
                    .ok_or_else(|| denied("diff lost its authorized source task"))?;
                let before = self
                    .selected(
                        &context,
                        owner,
                        task,
                        Some(&source.key),
                        Some(baseline),
                        operation,
                        work,
                    )
                    .await?;
                let next = match cursor.map(|cursor| &cursor.cursor) {
                    Some(PageCursor::Diff(cursor)) => Some(cursor),
                    None => None,
                    _ => return Err(denied("inspection continuation used for diff")),
                };
                let page =
                    diff_native(&before, &source, &input.directory, next, maximum, work).await?;
                let cursor = page
                    .1
                    .as_ref()
                    .map(|cursor| {
                        encode_cursor(
                            input,
                            &source.key,
                            &command.kind,
                            PageCursor::Diff(cursor),
                            work,
                        )
                    })
                    .transpose()?;
                ready(
                    &PiPageResponse {
                        source: input.source,
                        root: &source.key,
                        volume: None,
                        before_root: Some(&before.key),
                        before_snapshot: Some(before.snapshot),
                        snapshot: source.snapshot,
                        directory: &input.directory,
                        entries: &page.0,
                        cursor: cursor.as_ref().map(|(cursor, _)| cursor.as_str()),
                    },
                    work,
                )
            }
            _ => {
                if writeback
                    && (input.caller != self.protected.policy().root_task
                        || input.source == input.caller
                        || input.destination.is_some_and(|task| task != input.caller))
                {
                    return Err(denied("writeback destination must be the original Root"));
                }
                let Some(imported) = self
                    .import_selection(
                        &caller.1.binding,
                        input,
                        &source,
                        import_operation,
                        prior.as_ref().map(|(intent, _)| intent),
                        reconcile,
                        work,
                    )
                    .await?
                else {
                    return Ok(TaskCommandProgress::Pending);
                };
                if writeback {
                    self.writeback(
                        input,
                        &source,
                        imported,
                        selection_operation,
                        reconcile,
                        work,
                    )
                    .await
                } else {
                    ready(
                        &PiImportResponse {
                            status: "imported",
                            source: input.source,
                            destination: input.destination.unwrap_or(input.caller),
                            root: &source.key,
                            operation: &input.sdk_operation,
                            original_operation: import_operation,
                            snapshot: imported,
                        },
                        work,
                    )
                }
            }
        }
    }
}
