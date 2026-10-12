//! Merge for portable recursive tools.

use super::*;

impl<P, A, O> RecursiveExecutor<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn apply_merge(
        &self,
        context: &ToolContext,
        owner: &RecursiveToolOwner<P, A, O>,
        merge: &RecursiveMergeToolPlan,
        cancellation: &CancellationToken,
        output_io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<Value> {
        let temporary_io = recursive_temporary_io(output_io)?;
        let io = temporary_io.as_ref();
        #[cfg(not(target_arch = "wasm32"))]
        let native_work = io.and_then(crate::filesystem::PreparationIo::original_native_work);
        let binding = self
            .source
            .as_deref()
            .expect("recursive source was authorized")
            .merge(context, &merge.arguments, Some(merge), cancellation, io)?
            .await?;
        validate_merge_binding(context, merge.arguments.source, &binding, io)?;
        check_merge_binding(merge, &binding, io)?;
        let controller = ParentProjectController::new(
            binding.filesystem.as_ref(),
            binding.destination.reducer(),
            &binding.verifier,
            &binding.scope,
            &binding.project,
            io,
        )?;
        let inspected = match &binding.source_read {
            Some(handle) => {
                controller
                    .prepare_project_import_from_selected_source(
                        &merge.source_project,
                        &merge.source_generation,
                        Some(&merge.expected_target_generation),
                        handle,
                        &owner.journal_owner,
                        context.task(),
                        context.operation_id(),
                    )
                    .await?
            }
            None => {
                controller
                    .prepare_project_import(
                        &merge.source_project,
                        &merge.source_generation,
                        Some(&merge.expected_target_generation),
                    )
                    .await?
            }
        };
        'owner: {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = native_work {
                owner
                    .runtime
                    .task_host()
                    .verify_dispatch_owner_with_native_work(
                        merge.caller,
                        owner.journal_owner.task_binding().1,
                        work,
                    )?
                    .await?;
                break 'owner;
            }
            cancellation
                .check()
                .map_err(|error| Error::Storage(error.to_string()))?;
            owner
                .runtime
                .task_host()
                .verify_dispatch_owner(merge.caller, owner.journal_owner.task_binding().1.clone())
                .await?;
        }
        let outcome = {
            let mut sides = BTreeMap::new();
            for selection in &merge.arguments.selections {
                let conflict = crate::filesystem::project_workspaces::decode_conflict(
                    &selection.conflict,
                    io,
                )?;
                let side = match selection.side {
                    MergeToolSide::Base => ConflictSide::Base,
                    MergeToolSide::Target => ConflictSide::Ours,
                    MergeToolSide::Source => ConflictSide::Theirs,
                };
                if sides.contains_key(&conflict) {
                    return Err(recursive_reject(
                        io,
                        crate::wire::ErrorCode::Invalid,
                        "merge conflict selected twice",
                    ));
                }
                if let Some(io) = io {
                    let bytes =
                        std::mem::size_of::<(acyclic_fs::MergeConflict, ConflictSide)>() as u64;
                    io.retain_owned(bytes)?;
                    io.allocation_attempt(bytes)?;
                }
                sides.insert(conflict, side);
            }
            let (outcome, custody) = controller
                .apply_project_merge(&inspected, context.operation_id(), sides)
                .await?;
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(custody) = custody {
                io.expect("native controller IO")
                    .absorb_native_custody(custody)?;
            }
            #[cfg(target_arch = "wasm32")]
            let _ = custody;
            outcome
        };
        'owner: {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = native_work {
                owner
                    .runtime
                    .task_host()
                    .verify_dispatch_owner_with_native_work(
                        merge.caller,
                        owner.journal_owner.task_binding().1,
                        work,
                    )?
                    .await?;
                break 'owner;
            }
            owner
                .runtime
                .task_host()
                .verify_dispatch_owner(merge.caller, owner.journal_owner.task_binding().1.clone())
                .await?;
            cancellation
                .check()
                .map_err(|error| Error::Storage(error.to_string()))?;
        }
        #[derive(Serialize)]
        struct Applied<'a> {
            status: &'static str,
            receipt: &'a ProjectMergeReceipt,
        }
        #[derive(Serialize)]
        struct Generation<'a> {
            status: &'static str,
            generation: &'a GenerationRef,
        }
        #[derive(Serialize)]
        struct Conflicted<'a> {
            status: &'static str,
            #[serde(serialize_with = "recursive_conflict_keys")]
            conflicts: &'a [acyclic_fs::MergeConflict],
            truncated: bool,
        }
        #[derive(Serialize)]
        struct Status {
            status: &'static str,
        }
        match &outcome {
            JoinOutcome::Applied(_) | JoinOutcome::AlreadyApplied(_) => {
                let (receipt, custody) = controller.merge_receipt(
                    &inspected,
                    &outcome,
                    &merge.source,
                    context.operation_id(),
                    &merge.notice,
                )?;
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(custody) = custody {
                    io.expect("native controller IO")
                        .absorb_native_custody(custody)?;
                }
                #[cfg(target_arch = "wasm32")]
                let _ = custody;
                encode(
                    &Applied {
                        status: "applied",
                        receipt: &receipt,
                    },
                    output_io,
                )
            }
            JoinOutcome::NoChanges(generation) | JoinOutcome::StaleTarget(generation) => {
                let reference = crate::filesystem::fork_preparer_io::generation_ref(
                    binding.project.provider(),
                    generation.id(),
                    io,
                )?;
                let status = if matches!(&outcome, JoinOutcome::NoChanges(_)) {
                    "no_changes"
                } else {
                    "stale_target"
                };
                encode(
                    &Generation {
                        status,
                        generation: &reference,
                    },
                    output_io,
                )
            }
            JoinOutcome::Conflicted {
                conflicts,
                truncated,
            } => encode(
                &Conflicted {
                    status: "conflicted",
                    conflicts,
                    truncated: *truncated,
                },
                output_io,
            ),
            JoinOutcome::Fenced => encode(&Status { status: "fenced" }, output_io),
            JoinOutcome::IdempotencyConflict => encode(
                &Status {
                    status: "idempotency_conflict",
                },
                output_io,
            ),
        }
    }

    pub(super) async fn publish_merge(
        &self,
        context: &ToolContext,
        owner: &RecursiveToolOwner<P, A, O>,
        merge: &RecursiveMergeToolPlan,
        receipt: Option<&ProjectMergeReceipt>,
        cancellation: &CancellationToken,
        output_io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<Value> {
        let temporary_io = recursive_temporary_io(output_io)?;
        let io = temporary_io.as_ref();
        let mut binding = self
            .source
            .as_deref()
            .expect("recursive source was authorized")
            .merge(context, &merge.arguments, Some(merge), cancellation, io)?
            .await?;
        validate_merge_binding(context, merge.arguments.source, &binding, io)?;
        check_merge_binding(merge, &binding, io)?;
        let receipt = receipt.ok_or_else(|| {
            recursive_reject(
                io,
                crate::wire::ErrorCode::Invalid,
                "merge publication has no provider receipt",
            )
        })?;
        validate_merge_receipt(merge, receipt, io)?;
        if receipt.operation_id != context.operation_id() {
            return Err(recursive_reject(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "merge receipt belongs to another original effect",
            ));
        }
        let receipt_copy =
            crate::executor::with_history_io(io, |work| receipt.clone_with_work_and_custody(work))?
                .0;
        let receipt_copy = recursive_box(receipt_copy, io)?;
        let key = crate::executor::with_history_io(io, |work| crate::executor::preparation_format(format_args!("recursive-merge-publication:{}", context.operation_id()), work))?;
        let command = Command {
            operation_id: context.operation_id(),
            idempotency_key: IdempotencyKey(key),
            expected_revision: binding.destination.reducer().revision(),
            scope: binding.scope,
            causal_parent: None,
            action: Action::PublishProjectMerge {
                receipt: receipt_copy,
            },
        };
        let mut history = io.map(crate::filesystem::PreparationIo::history_io);
        let mut observer = history
            .as_mut()
            .map(|history| history as &mut dyn crate::store::history::HistoryIo);
        binding
            .destination
            .publish(
                command,
                Some(owner.journal_owner.as_ref()),
                crate::distributed::JournalWrite::Fresh,
                false,
                &mut observer,
            )
            .await?;
        encode(receipt, output_io)
    }
}
