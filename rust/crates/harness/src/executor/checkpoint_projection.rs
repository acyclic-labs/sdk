use super::*;

/// Imports a checkpoint and its complete canonical delta through a logical cut.
#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "combines the existing owner-bound checkpoint, indexed history and content providers without constructing replacement authority"
)]
pub async fn load_canonical_checkpoint_through<P: acyclic_stream::StreamProvider>(
    journal: &dyn ExecutionJournal,
    reference: &FileRef,
    limits: Limits,
    scope: &RuntimeScope,
    history: &crate::store::HistoryReader<P>,
    cursor: &crate::store::HistoryCursor,
    through_sequence: u64,
    history_limits: crate::store::HistoryReadLimits,
    resolver: &dyn crate::conversation::ContentResidencyVerifier,
    io: Option<&PreparationIo>,
) -> Result<(
    crate::context::CanonicalContextCheckpoint,
    crate::context::Context,
)> {
    let limits = crate::context::restrict_context_limits(limits, scope.limits(), io)?;
    if history_limits.maximum_events == 0 || history_limits.maximum_bytes == 0 {
        return Err(stock_boundary(
            io,
            crate::wire::ErrorCode::Invalid,
            "checkpoint delta bounds are invalid",
        ));
    }
    let (envelope, mut retained, consumed) = load_canonical_checkpoint_at(
        journal,
        reference,
        limits,
        scope,
        history,
        cursor,
        history_limits.maximum_bytes,
        io,
    )
    .await?;
    let after_sequence = envelope.selection.conversation_revision;
    let count = through_sequence
        .checked_sub(after_sequence)
        .filter(|count| *count <= u64::from(history_limits.maximum_events - 1))
        .ok_or_else(|| {
            stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "checkpoint delta exceeds history work bound",
            )
        })?;
    retained.current_input_index = None;
    if count == 0 {
        return Ok((envelope, retained));
    }
    let remaining_bytes = history_limits
        .maximum_bytes
        .checked_sub(consumed)
        .filter(|bytes| *bytes > 0)
        .ok_or_else(|| {
            stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "checkpoint delta exceeds history byte bound",
            )
        })?;
    let range_limits = crate::store::HistoryReadLimits {
        maximum_events: history_limits.maximum_events - 1,
        maximum_bytes: remaining_bytes,
    };
    #[cfg(feature = "filesystem")]
    let _records_hold;
    #[cfg(feature = "filesystem")]
    let _view_workspace;
    let view = match io {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        Some(io) if io.original_native_work().is_some() => {
            let before = io.owned_bytes()?;
            let mut observer =
                crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(
                    io.original_native_work()
                        .expect("native branch retains original work"),
                )?);
            let mut work: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut observer);
            let records = history
                .conversation_range(
                    cursor,
                    after_sequence,
                    through_sequence,
                    range_limits,
                    &mut work,
                )
                .await?;
            let (view, _) = crate::conversation::ConversationState::from_wire(
                crate::conversation::ConversationStateWire {
                    agent: None,
                    messages: records,
                    archived_prefix: None,
                },
                acyclic_stream::MAX_RECORD_BYTES as u64,
                &mut work,
            )?;
            observer.return_staged_output()?;
            io.absorb_native_custody(observer.into_custody()?)?;
            _records_hold = Some(io.guard_owned(io.owned_bytes()? - before)?);
            _view_workspace = None;
            view
        }
        _ => {
            let records = match io {
                #[cfg(feature = "filesystem")]
                Some(io) => {
                    let before = io.owned_bytes()?;
                    let mut observer = io.history_io();
                    let value = history
                        .conversation_range(
                            cursor,
                            after_sequence,
                            through_sequence,
                            range_limits,
                            &mut Some(&mut observer),
                        )
                        .await?;
                    _records_hold = Some(io.guard_owned(io.owned_bytes()? - before)?);
                    value
                }
                _ => {
                    #[cfg(feature = "filesystem")]
                    {
                        _records_hold = None;
                    }
                    history
                        .conversation_range(
                            cursor,
                            after_sequence,
                            through_sequence,
                            range_limits,
                            &mut None,
                        )
                        .await?
                }
            };
            #[cfg(feature = "filesystem")]
            if let Some(io) = io {
                let before = io.owned_bytes()?;
                let mut observer = io.history_io();
                let (view, _) = crate::conversation::ConversationState::from_wire(
                    crate::conversation::ConversationStateWire {
                        agent: None,
                        messages: records,
                        archived_prefix: None,
                    },
                    acyclic_stream::MAX_RECORD_BYTES as u64,
                    &mut Some(&mut observer),
                )?;
                _view_workspace = Some(io.guard_owned(io.owned_bytes()? - before)?);
                view
            } else {
                _view_workspace = None;
                crate::conversation::ConversationState::from_wire(
                    crate::conversation::ConversationStateWire {
                        agent: None,
                        messages: records,
                        archived_prefix: None,
                    },
                    acyclic_stream::MAX_RECORD_BYTES as u64,
                    &mut None,
                )
                .map(|(state, _)| state)?
            }
            #[cfg(not(feature = "filesystem"))]
            crate::conversation::ConversationState::from_wire(
                crate::conversation::ConversationStateWire {
                    agent: None,
                    messages: records,
                    archived_prefix: None,
                },
                acyclic_stream::MAX_RECORD_BYTES as u64,
                &mut None,
            )
            .map(|(state, _)| state)?
        }
    };
    #[cfg(feature = "filesystem")]
    if let Some(io) = io.filter(|io| !stock_is_native(Some(io))) {
        let bytes = (view.messages().len() * std::mem::size_of::<uuid::Uuid>()) as u64;
        io.retain_owned(bytes)?;
        io.allocation_attempt(bytes)?;
    }
    let mut message_ids = if stock_is_native(io) {
        Vec::new()
    } else {
        Vec::with_capacity(view.messages().len())
    };
    for message in view.messages() {
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            io.charge(acyclic_fs::WorkCounters {
                items_examined: 1,
                ..Default::default()
            })?;
        }
        if matches!(
            message.kind,
            crate::conversation::MessageKind::System
                | crate::conversation::MessageKind::User
                | crate::conversation::MessageKind::Assistant
                | crate::conversation::MessageKind::ToolCall
                | crate::conversation::MessageKind::ToolResult
        ) {
            if stock_is_native(io) {
                stock_vec_push(&mut message_ids, message.id, io)?;
            } else {
                message_ids.push(message.id);
            }
        }
    }
    #[cfg(feature = "filesystem")]
    let message_ids_bytes = if stock_is_native(io) {
        message_ids.capacity() * std::mem::size_of::<uuid::Uuid>()
    } else {
        view.messages().len() * std::mem::size_of::<uuid::Uuid>()
    };
    let resolver = CheckpointDeltaResolver {
        inner: resolver,
        scope,
        limits,
    };
    for message in view.messages() {
        resolver.authorize(&message.content, io)?;
        match &message.attachments {
            crate::conversation::ReferencedAttachments::Inline { items } => {
                for attachment in items {
                    resolver.authorize(&attachment.file, io)?;
                }
            }
            crate::conversation::ReferencedAttachments::Manifest { manifest, .. } => {
                resolver.authorize(manifest, io)?;
            }
        }
    }
    let selected = crate::projection::select_model_context(
        &view,
        through_sequence,
        crate::conversation::ModelContextSelection {
            conversation_revision: through_sequence,
            message_ids,
            checkpoint: None,
        },
        &resolver,
        limits,
        limits.attachments,
        io,
    )
    .await?;
    verify_model_contents_scoped(
        journal,
        selected.messages.iter().map(|message| &message.content),
        limits,
        Some(scope),
        io,
    )
    .await?;
    drop(selected.selection);
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        io.release(message_ids_bytes as u64)?;
    }
    let retained = crate::context::apply_context_projection(
        retained,
        selected.messages,
        crate::context::ContextRenderMode::Prompt,
        crate::context::ContextPlacement::Append,
        limits,
        io,
    )?;
    Ok((envelope, retained))
}

impl CheckpointDeltaResolver<'_> {
    fn authorize(&self, file: &FileRef, io: Option<&PreparationIo>) -> Result<()> {
        with_history_io(io, |work| self.limits.validate_file(file, work))?;
        if !with_history_io(io, |work| {
            crate::runtime::read_granted_with_work(self.scope.grants(), file, work)
        })? {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "attenuated task cannot read checkpoint delta",
            ));
        }
        Ok(())
    }
}

impl crate::conversation::ContentResidencyVerifier for CheckpointDeltaResolver<'_> {
    fn supports_read(
        &self,
        volume: &crate::conversation::VolumeRef,
        io: Option<&PreparationIo>,
    ) -> Result<bool> {
        self.inner.supports_read(volume, io)
    }

    fn verify<'a>(
        &'a self,
        _: &'a FileRef,
        _: u64,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "projection-only reader",
            ))
        })
    }

    fn read<'a>(
        &'a self,
        file: &'a FileRef,
        maximum_bytes: u64,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        self.authorize(file, io)?;
        let maximum_bytes = maximum_bytes.min(self.limits.render_bytes);
        if file.descriptor().byte_length() > maximum_bytes {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "checkpoint delta exceeds render bound",
            ));
        }
        if !self.inner.supports_read(file.volume(), io)? {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "checkpoint delta reader has no bounded capability",
            ));
        }
        let read = self.inner.read(file, maximum_bytes, io)?;
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let bytes = read.await?;
            if file.descriptor().media_type() == "application/vnd.acyclic.harness.attachments+json"
            {
                #[cfg(feature = "filesystem")]
                let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
                #[cfg(feature = "filesystem")]
                let _items_hold;
                let items =
                    crate::conversation::decode_attachment_manifest(file, &bytes, None, io)?;
                #[cfg(feature = "filesystem")]
                {
                    _items_hold = io
                        .map(|io| io.guard_owned(io.owned_bytes()? - before))
                        .transpose()?;
                }
                if items.len() > self.limits.attachments {
                    return Err(stock_boundary(
                        io,
                        crate::wire::ErrorCode::Invalid,
                        "checkpoint delta manifest exceeds item bound",
                    ));
                }
                for item in &items {
                    self.authorize(&item.file, io)?;
                }
            }
            Ok(bytes)
        })
    }
}

/// Loads a published checkpoint under the optional admission's grants and bounds.
pub async fn load_canonical_checkpoint(
    journal: &dyn ExecutionJournal,
    reference: &FileRef,
    limits: Limits,
    scope: Option<&RuntimeScope>,
    io: Option<&PreparationIo>,
) -> Result<(
    crate::context::CanonicalContextCheckpoint,
    crate::context::Context,
)> {
    let limits = match scope {
        Some(scope) => crate::context::restrict_context_limits(limits, scope.limits(), io)?,
        None => limits,
    };
    validate_checkpoint_json_reference(reference, limits, io)?;
    let envelope =
        load_json::<crate::context::CanonicalContextCheckpoint>(journal, reference, io).await?;
    envelope.0.validate(limits, io)?;
    #[cfg(feature = "filesystem")]
    let publication_before = io.map(|io| io.owned_bytes()).transpose()?.unwrap_or(0);
    #[cfg(feature = "filesystem")]
    let _publication_hold;
    let publication =
        canonical_checkpoint_for_operation(journal, envelope.0.operation_id, limits, io).await?;
    #[cfg(feature = "filesystem")]
    {
        _publication_hold = io
            .map(|io| io.guard_owned(io.owned_bytes()? - publication_before))
            .transpose()?;
    }
    if publication.as_ref() != Some(reference) {
        return Err(stock_boundary(
            io,
            crate::wire::ErrorCode::Conflict,
            "checkpoint is not published by its owning execution",
        ));
    }
    for file in [
        &envelope.0.source,
        &envelope.0.retained,
        &envelope.0.compaction,
    ] {
        validate_checkpoint_json_reference(file, limits, io)?;
    }
    let source = load_json::<crate::context::Context>(journal, &envelope.0.source, io).await?;
    let retained = load_json::<crate::context::Context>(journal, &envelope.0.retained, io).await?;
    let proof =
        load_json::<crate::context::CompactionReference>(journal, &envelope.0.compaction, io)
            .await?;
    envelope
        .0
        .validate_projection(&source.0, &retained.0, &proof.0, limits, io)?;
    for file in retained.0.metadata.values() {
        with_history_io(io, |work| limits.validate_file(file, work))?;
        if let Some(scope) = scope
            && !with_history_io(io, |work| {
                crate::runtime::read_granted_with_work(scope.grants(), file, work)
            })?
        {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "attenuated task cannot read checkpoint metadata",
            ));
        }
    }
    verify_model_contents_scoped(
        journal,
        retained.0.messages.iter().map(|message| &message.content),
        limits,
        scope,
        io,
    )
    .await?;
    for file in retained.0.metadata.values() {
        match io {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            Some(io) if io.original_native_work().is_some() => {
                journal
                    .verify_input_file_with_native_work(
                        file,
                        io.original_native_work()
                            .expect("native branch retains original work"),
                    )?
                    .await?;
            }
            #[cfg(feature = "filesystem")]
            Some(io) => io.receive_content(
                journal
                    .verify_input_file_measured(file, io.remaining()?, io.cancellation())
                    .await,
            )?,
            _ => journal.verify_input_file(file).await?,
        }
    }
    if proof
        .0
        .summary
        .as_ref()
        .is_none_or(|summary| summary.step != 0)
    {
        return Err(stock_boundary(
            io,
            crate::wire::ErrorCode::Storage,
            "canonical checkpoint summary belongs to another step",
        ));
    }
    Ok((retain_preparation(envelope), retain_preparation(retained)))
}
