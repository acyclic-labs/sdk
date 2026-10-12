use super::*;

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn verify_canonical_publication_with_io(
        &self,
        operation_id: OperationId,
        event: &ExecutionEvent,
        limits: Limits,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        if let Some(io) = io {
            io.check_cancellation()?;
        }
        let ExecutionEvent::ContextCompacted {
            step,
            projection,
            compaction,
            ..
        } = event
        else {
            return Ok(());
        };
        let mut history = io.map(PreparationIo::history_io);
        limits.validate_file(
            projection,
            &mut history
                .as_mut()
                .map(|history| history as &mut dyn crate::store::history::HistoryIo),
        )?;
        let projection =
            crate::executor::load_json::<crate::executor::ResponseProjection>(self, projection, io)
                .await?;
        crate::context::validate_projected_context(&projection.0.context, limits, io)?;
        let Some(reference) = &projection.0.checkpoint else {
            if projection.0.canonical.is_some() {
                return Err(canonical_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "canonical publication has no checkpoint",
                ));
            }
            return Ok(());
        };
        let mut history = io.map(PreparationIo::history_io);
        limits.validate_file(
            reference,
            &mut history
                .as_mut()
                .map(|history| history as &mut dyn crate::store::history::HistoryIo),
        )?;
        let envelope = crate::executor::load_json::<crate::context::CanonicalContextCheckpoint>(
            self, reference, io,
        )
        .await?;
        envelope.0.validate(limits, io)?;
        if *step != 0
            || envelope.0.operation_id != operation_id
            || projection.0.canonical.as_ref() != Some(&envelope.0.source)
            || &envelope.0.compaction != compaction
        {
            return Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "canonical publication differs from its envelope",
            ));
        }
        let source =
            crate::executor::load_json::<crate::context::Context>(self, &envelope.0.source, io)
                .await?;
        let retained =
            crate::executor::load_json::<crate::context::Context>(self, &envelope.0.retained, io)
                .await?;
        let proof =
            crate::executor::load_json::<crate::context::CompactionReference>(self, compaction, io)
                .await?;
        envelope
            .0
            .validate_projection(&source.0, &retained.0, &proof.0, limits, io)?;
        self.verify_canonical_source_with_io(&envelope.0, &source.0, limits, io)
            .await?;
        self.verify_canonical_summary_with_io(
            operation_id,
            &envelope.0,
            &source.0,
            &proof.0,
            limits,
            io,
        )
        .await
    }

    pub(super) async fn verify_canonical_source_with_io(
        &self,
        envelope: &crate::context::CanonicalContextCheckpoint,
        source: &crate::context::Context,
        limits: Limits,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let result = async {
            let mut history = io.map(PreparationIo::history_io);
            limits.validate(
                &mut history
                    .as_mut()
                    .map(|history| history as &mut dyn crate::store::history::HistoryIo),
            )?;
            self.require_operation(envelope.operation_id, io)?;
            if self.execution.is_some() {
                let association_hold;
                let association_before =
                    io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
                let records = self
                    .replay_page(envelope.operation_id, 0, 1, true, io)
                    .await?;
                association_hold = io
                    .map(|io| {
                        io.guard_owned(
                            io.owned_bytes()?
                                .checked_sub(association_before)
                                .ok_or_else(|| {
                                    io.invalid("checkpoint association custody underflows")
                                })?,
                        )
                    })
                    .transpose()?;
                drop(records);
                drop(association_hold);
            }
            let previous = match &envelope.selection.checkpoint {
                Some(reference) => Some(
                    crate::executor::load_canonical_checkpoint(self, reference, limits, None, io)
                        .await?,
                ),
                None => None,
            };
            let after = previous
                .as_ref()
                .map_or(0, |(previous, _)| previous.selection.conversation_revision);
            let verifier = self.input_verifier.as_ref().ok_or_else(|| {
                canonical_boundary(
                    io,
                    crate::wire::ErrorCode::Unsupported,
                    "canonical source reader is not bound",
                )
            })?;
            let history_limits = if let Some(limits) = self.history_read_limits {
                limits
            } else {
                let maximum_events = u32::try_from(limits.context_messages).map_err(|_| {
                    canonical_boundary(
                        io,
                        crate::wire::ErrorCode::Invalid,
                        "canonical delta count exceeds portable history bound",
                    )
                })?;
                let maximum_bytes = (acyclic_stream::MAX_RECORD_BYTES as u64)
                    .checked_mul(2)
                    .and_then(|bytes| bytes.checked_mul(u64::from(maximum_events)))
                    .ok_or_else(|| {
                        canonical_boundary(
                            io,
                            crate::wire::ErrorCode::Invalid,
                            "canonical delta byte bound overflows",
                        )
                    })?;
                crate::store::HistoryReadLimits {
                    maximum_events,
                    maximum_bytes,
                }
            };
            let mut borrowed = io.map(PreparationIo::history_io);
            let reader = crate::store::HistoryReader::new(
                &self.stream,
                self.verifier.audience(),
                &self.verifier,
                &mut borrowed
                    .as_mut()
                    .map(|work| work as &mut dyn crate::store::HistoryIo),
            )?;
            let event = match borrowed.as_mut() {
                Some(work) => {
                    crate::store::operations::find_operation_bounded_with_work(
                        &self.stream,
                        self.verifier.audience(),
                        &self.verifier,
                        self.turn_operation(envelope.operation_id),
                        history_limits.maximum_bytes,
                        true,
                        &mut Some(work as &mut dyn crate::store::history::HistoryIo),
                    )
                    .await?
                    .0
                }
                None => reader
                    .operation_event(
                        self.turn_operation(envelope.operation_id),
                        2 * acyclic_stream::MAX_RECORD_BYTES as u64,
                        &mut None,
                    )
                    .await
                    .map(|(event, _)| event)?,
            }
            .ok_or_else(|| {
                canonical_boundary(
                    io,
                    crate::wire::ErrorCode::Conflict,
                    "checkpoint source selection is not the committed admission",
                )
            })?;
            let crate::core::EventPayload::ModelContextSelected { selection } = &event.payload
            else {
                return Err(canonical_boundary(
                    io,
                    crate::wire::ErrorCode::Conflict,
                    "checkpoint operation is not a model-context selection",
                ));
            };
            if selection != &envelope.selection {
                return Err(canonical_boundary(
                    io,
                    crate::wire::ErrorCode::Conflict,
                    "checkpoint source selection is not the committed admission",
                ));
            }
            let authority = match borrowed.as_mut() {
                Some(work) => crate::store::history::HistoryIo::clone_authority(
                    work,
                    self.verifier.audience(),
                )?,
                None => self.verifier.audience().clone(),
            };
            let cursor = crate::store::HistoryCursor {
                authority,
                after_revision: 0,
                through_revision: event.revision,
            };
            let loaded = reader
                .conversation_range(
                    &cursor,
                    after,
                    envelope.selection.conversation_revision,
                    history_limits,
                    &mut borrowed
                        .as_mut()
                        .map(|work| work as &mut dyn crate::store::history::HistoryIo),
                )
                .await?;
            let delta = project_canonical_delta_with_io(
                self,
                loaded,
                &envelope.selection,
                verifier.as_ref(),
                limits,
                io,
            )
            .await?;
            let expected = match previous {
                Some((_, retained)) => {
                    crate::context::ContextPipeline::continue_base(retained, delta, limits, io)?
                }
                None => delta,
            };
            let matches = match io {
                Some(io) => {
                    let _actual_hold;
                    let actual = io.encode(source, limits.render_bytes, true)?;
                    _actual_hold = io.guard_bytes(&actual)?;
                    let _expected_hold;
                    let expected = io.encode(&expected, limits.render_bytes, true)?;
                    _expected_hold = io.guard_bytes(&expected)?;
                    io.check_cancellation()?;
                    actual == expected
                }
                None => source == &expected,
            };
            if !matches {
                return Err(canonical_boundary(
                    io,
                    crate::wire::ErrorCode::Conflict,
                    "checkpoint source differs from its canonical base and delta",
                ));
            }
            Ok(())
        }
        .await;
        if let Some(io) = io {
            io.release(io.owned_bytes()? - before)?;
        }
        result
    }

    pub(super) async fn verify_canonical_summary_with_io(
        &self,
        operation: OperationId,
        envelope: &crate::context::CanonicalContextCheckpoint,
        source: &crate::context::Context,
        proof: &crate::context::CompactionReference,
        limits: Limits,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        use crate::executor::ModelPurpose;
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let result = async {
            let summary = proof
                .summary
                .as_ref()
                .filter(|summary| summary.step == 0)
                .ok_or_else(|| {
                    canonical_boundary(
                        io,
                        crate::wire::ErrorCode::Invalid,
                        "canonical summary belongs to another step",
                    )
                })?;
            let (_, records) = crate::executor::replay_execution(
                self,
                operation,
                limits.model_events_per_step.saturating_add(2),
                |event| {
                    matches!(
                        event,
                        ExecutionEvent::ContextPrepared { step: 0, .. }
                            | ExecutionEvent::ModelStarted {
                                step: 0,
                                purpose: ModelPurpose::Summary,
                                ..
                            }
                            | ExecutionEvent::Model {
                                step: 0,
                                purpose: ModelPurpose::Summary,
                                ..
                            }
                    )
                },
                io,
            )
            .await?;
            let mut prepared = None;
            let mut request = None;
            let mut output = String::new();
            let mut admission = ModelEventAdmission::default();
            for record in &records {
                crate::filesystem::fork_preparer_io::item(io, 0)?;
                match &record.event {
                    ExecutionEvent::ContextPrepared { projection, .. } => {
                        prepared = Some(
                            crate::executor::load_json::<crate::executor::ResponseProjection>(
                                self, projection, io,
                            )
                            .await?,
                        );
                    }
                    ExecutionEvent::ModelStarted {
                        request: reference, ..
                    } => {
                        request = Some(
                            crate::executor::load_model_request(self, reference, limits, io)
                                .await?,
                        );
                    }
                    ExecutionEvent::Model { event, .. } => {
                        let event =
                            crate::executor::load_json::<ModelEvent>(self, event, io).await?;
                        admission.observe(&event.0, limits, io)?;
                        match &event.0 {
                            ModelEvent::Content { delta } => {
                                canonical_push_text(&mut output, delta, limits.render_bytes, io)?;
                            }
                            ModelEvent::Completed { .. } | ModelEvent::Reasoning { .. } => {}
                            ModelEvent::ToolCall { .. } => {
                                return Err(canonical_boundary(
                                    io,
                                    crate::wire::ErrorCode::Invalid,
                                    "canonical summary requested a tool",
                                ));
                            }
                        }
                    }
                    _ => {
                        return Err(canonical_boundary(
                            io,
                            crate::wire::ErrorCode::Storage,
                            "canonical summary selection is invalid",
                        ));
                    }
                }
            }
            if prepared
                .as_ref()
                .and_then(|projection| projection.0.canonical.as_ref())
                != Some(&envelope.source)
                || !admission.completed
                || output.is_empty()
            {
                return Err(canonical_boundary(
                    io,
                    crate::wire::ErrorCode::Conflict,
                    "canonical summary lacks its prepared source or settled output",
                ));
            }
            let request = request.ok_or_else(|| {
                canonical_boundary(
                    io,
                    crate::wire::ErrorCode::Conflict,
                    "canonical summary has no admitted request",
                )
            })?;
            verify_summary_prefix_with_io(&request, source, summary.source_messages, limits, io)?;
            let mut history = io.map(PreparationIo::history_io);
            limits.validate_file(
                &summary.output,
                &mut history
                    .as_mut()
                    .map(|history| history as &mut dyn crate::store::history::HistoryIo),
            )?;
            let bytes = match io {
                #[cfg(not(target_arch = "wasm32"))]
                Some(io) if io.original_native_work().is_some() => {
                    let work = io
                        .original_native_work()
                        .expect("matched original native work");
                    let bytes = self.load_with_native_work(&summary.output, work)?.await?;
                    bytes
                }
                Some(io) => io.receive_content(
                    self.load_measured(&summary.output, io.remaining()?, io.cancellation())
                        .await,
                )?,
                None => self.load(&summary.output).await?,
            };
            if let Some(io) = io {
                io.check_cancellation()?;
            }
            let matches = bytes.as_ref() == output.as_bytes();
            if !matches {
                return Err(canonical_boundary(
                    io,
                    crate::wire::ErrorCode::Conflict,
                    "canonical summary output differs from admitted observations",
                ));
            }
            Ok(())
        }
        .await;
        if let Some(io) = io {
            io.release(io.owned_bytes()? - before)?;
        }
        result
    }
}

pub(super) fn canonical_push_text(
    output: &mut String,
    delta: &str,
    maximum: u64,
    io: Option<&PreparationIo>,
) -> Result<()> {
    let length = output
        .len()
        .checked_add(delta.len())
        .filter(|length| *length as u64 <= maximum)
        .ok_or_else(|| {
            canonical_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "canonical summary output exceeds bounds",
            )
        })?;
    if let Some(io) = io {
        let old = output.capacity();
        let grows = length > old;
        let copy = acyclic_fs::WorkCounters {
            bytes_copied: delta.len() as u64 + if grows { output.len() as u64 } else { 0 },
            ..Default::default()
        };
        io.preflight(copy)?;
        if grows {
            io.retain_owned((length - old) as u64)?;
            io.allocation_attempt((length - old) as u64)?;
            output
                .try_reserve_exact(length - output.len())
                .map_err(|_| {
                    canonical_boundary(
                        Some(io),
                        crate::wire::ErrorCode::Storage,
                        "canonical summary allocation failed",
                    )
                })?;
            if output.capacity() > length {
                io.retain_owned((output.capacity() - length) as u64)?;
            }
        }
        output.push_str(delta);
        io.charge(copy)?;
    } else {
        output.push_str(delta);
    }
    Ok(())
}

pub(super) fn verify_summary_prefix_with_io(
    request: &crate::model::PreparedModelRequest,
    source: &crate::context::Context,
    count: u32,
    limits: Limits,
    io: Option<&PreparationIo>,
) -> Result<()> {
    let count = count as usize;
    let expected = count.checked_add(1).ok_or_else(|| {
        canonical_boundary(
            io,
            crate::wire::ErrorCode::Invalid,
            "summary prefix count overflows",
        )
    })?;
    let messages = &request.request().messages;
    let matches = match (messages.get(..count), source.messages.get(..count)) {
        (Some(actual), Some(expected_prefix)) if messages.len() == expected => match io {
            Some(io) => {
                let _actual_hold;
                let actual = io.encode(&actual, limits.render_bytes, true)?;
                _actual_hold = io.guard_bytes(&actual)?;
                let _expected_hold;
                let expected = io.encode(&expected_prefix, limits.render_bytes, true)?;
                _expected_hold = io.guard_bytes(&expected)?;
                io.check_cancellation()?;
                actual == expected
            }
            None => actual == expected_prefix,
        },
        _ => false,
    };
    if !matches {
        return Err(canonical_boundary(
            io,
            crate::wire::ErrorCode::Conflict,
            "canonical summary request differs from its source prefix",
        ));
    }
    Ok(())
}

pub(super) async fn project_canonical_delta_with_io<P, A, O>(
    journal: &FilesystemExecutionJournal<P, A, O>,
    loaded: Vec<crate::conversation::ConversationMessage>,
    selection: &crate::conversation::ModelContextSelection,
    verifier: &dyn ContentResidencyVerifier,
    limits: Limits,
    io: Option<&PreparationIo>,
) -> Result<crate::context::Context>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    let mut selected_ids = selection.message_ids.iter();
    for message in &loaded {
        crate::filesystem::fork_preparer_io::item(io, 0)?;
        if matches!(
            message.kind,
            crate::conversation::MessageKind::System
                | crate::conversation::MessageKind::User
                | crate::conversation::MessageKind::Assistant
                | crate::conversation::MessageKind::ToolCall
                | crate::conversation::MessageKind::ToolResult
        ) {
            if selected_ids.next() != Some(&message.id) {
                return Err(canonical_boundary(
                    io,
                    crate::wire::ErrorCode::Conflict,
                    "checkpoint skips uncovered canonical history",
                ));
            }
        }
    }
    if selected_ids.next().is_some() {
        return Err(canonical_boundary(
            io,
            crate::wire::ErrorCode::Conflict,
            "checkpoint skips uncovered canonical history",
        ));
    }
    if io.is_some() {
        for message in &loaded {
            crate::filesystem::fork_preparer_io::item(io, 0)?;
            journal
                .verify_input_file_with_io(&message.content, io)
                .await?;
            match &message.attachments {
                crate::conversation::ReferencedAttachments::Inline { items } => {
                    for item in items {
                        crate::filesystem::fork_preparer_io::item(io, 0)?;
                        journal.verify_input_file_with_io(&item.file, io).await?;
                    }
                }
                crate::conversation::ReferencedAttachments::Manifest { manifest, .. } => {
                    journal.verify_input_file_with_io(manifest, io).await?;
                }
            }
        }
    }
    let mut borrowed = io.map(PreparationIo::history_io);
    let (history, _) = crate::conversation::ConversationState::from_wire(
        crate::conversation::ConversationStateWire {
            agent: None,
            messages: loaded,
            archived_prefix: None,
        },
        acyclic_stream::MAX_RECORD_BYTES as u64,
        &mut borrowed
            .as_mut()
            .map(|work| work as &mut dyn crate::store::history::HistoryIo),
    )?;
    let mut message_ids = Vec::new();
    for id in &selection.message_ids {
        crate::executor::stock_vec_push(&mut message_ids, *id, io)?;
    }
    let selection = crate::conversation::ModelContextSelection {
        conversation_revision: selection.conversation_revision,
        message_ids,
        checkpoint: selection
            .checkpoint
            .as_ref()
            .map(|file| crate::executor::with_history_io(io, |work| file.copy(work)))
            .transpose()?,
    };
    let resolver = JournalSelectedResolver {
        journal,
        inner: verifier,
    };
    let resolver: &dyn ContentResidencyVerifier = if io.is_some() { &resolver } else { verifier };
    let selected = select_model_context(
        &history,
        selection.conversation_revision,
        selection,
        resolver,
        limits,
        limits.attachments,
        io,
    )
    .await?;
    let input = crate::executor::with_history_io(io, |work| {
        crate::model::clone_content(
            &selected
                .messages
                .last()
                .ok_or_else(|| {
                    canonical_boundary(
                        io,
                        crate::wire::ErrorCode::Invalid,
                        "canonical input is missing",
                    )
                })?
                .content,
            work,
        )
        .map(|(copy, _)| copy)
    })?;
    let mut selected_delta = selected;
    selected_delta.selection.checkpoint = None;
    crate::context::ContextPipeline::base_context(
        &crate::context::ContextInput {
            input,
            selected_context: Some(selected_delta),
            step: 0,
            prior_messages: Vec::new(),
        },
        limits,
        io,
    )
}
