use super::*;

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(in crate::filesystem) fn context_history_limits(
        &self,
        limits: Limits,
    ) -> Result<crate::store::HistoryReadLimits> {
        limits.validate(&mut None)?;
        Ok(if let Some(limits) = self.history_read_limits {
            limits
        } else {
            let maximum_events = u32::try_from(limits.context_messages).map_err(|_| {
                Error::Invalid("canonical delta count exceeds portable history bound".into())
            })?;
            let maximum_bytes = (acyclic_stream::MAX_RECORD_BYTES as u64)
                .checked_mul(2)
                .and_then(|bytes| bytes.checked_mul(u64::from(maximum_events)))
                .ok_or_else(|| Error::Invalid("canonical delta byte bound overflows".into()))?;
            crate::store::HistoryReadLimits {
                maximum_events,
                maximum_bytes,
            }
        })
    }

    /// Refreshes the shared authoritative conversation reducer.
    pub(in crate::filesystem) async fn conversation_projection(
        &self,
        limits: Limits,
    ) -> Result<tokio::sync::MappedMutexGuard<'_, StreamAggregate<P>>> {
        limits.validate(&mut None)?;
        let verifier = self.input_verifier.as_ref().ok_or_else(|| {
            Error::Unsupported("conversation content verifier is not bound".into())
        })?;
        let mut cached = self.conversation_projection.lock().await;
        if cached.is_none() {
            *cached = Some(
                StreamAggregate::open(
                    &self.stream,
                    self.verifier.audience().clone(),
                    self.verifier.clone(),
                    self.schemas.clone(),
                    None,
                    crate::store::default_projection_read_limits(),
                    crate::store::DEFAULT_TERMINAL_EFFECTS,
                    None,
                    &mut None,
                )
                .await?
                .with_content_verifier(Arc::clone(verifier)),
            );
        }
        let mut aggregate = tokio::sync::MutexGuard::try_map(cached, Option::as_mut)
            .map_err(|_| Error::Storage("conversation projection is absent".into()))?;
        aggregate.set_limits(limits)?;
        let through = aggregate.tail_revision().await?;
        if through.saturating_sub(aggregate.reducer().revision())
            > u64::from(crate::store::DEFAULT_PROJECTION_EVENTS)
        {
            *aggregate = StreamAggregate::open(
                &self.stream,
                self.verifier.audience().clone(),
                self.verifier.clone(),
                self.schemas.clone(),
                None,
                crate::store::default_projection_read_limits(),
                crate::store::DEFAULT_TERMINAL_EFFECTS,
                None,
                &mut None,
            )
            .await?
            .with_content_verifier(Arc::clone(verifier));
            aggregate.set_limits(limits)?;
            return Ok(aggregate);
        }
        while !aggregate
            .refresh_through(through, EXECUTION_REPLAY_PAGE_RECORDS)
            .await?
        {}
        Ok(aggregate)
    }
    pub(super) async fn verify_selected_context_with_io(
        &self,
        operation_id: OperationId,
        selected: &SelectedModelContext,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        self.require_operation(operation_id, io)?;
        let turn_operation = self.turn_operation(operation_id);
        {
            let association_hold;
            let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
            let records = self.replay_page(operation_id, 0, 1, true, io).await?;
            association_hold = io
                .map(|io| {
                    io.guard_owned(io.owned_bytes()?.checked_sub(before).ok_or_else(|| {
                        crate::filesystem::interaction_host::interaction_boundary(
                            Some(io),
                            crate::wire::ErrorCode::Invalid,
                            "selected association custody underflows",
                        )
                    })?)
                })
                .transpose()?;
            drop(records);
            drop(association_hold);
        }
        let _query_hold;
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let verifier = self.input_verifier.as_ref().ok_or_else(|| {
            crate::filesystem::interaction_host::interaction_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "conversation content verifier is not bound",
            )
        })?;
        let limits = self
            .owner
            .as_ref()
            .map_or_else(Limits::default, |(owner, _)| owner.input_limits());
        let mut borrowed = io.map(PreparationIo::history_io);
        let mut work = borrowed
            .as_mut()
            .map(|work| work as &mut dyn crate::store::history::HistoryIo);
        limits.validate(&mut work)?;
        if selected.selection.message_ids.len() > limits.context_messages {
            return Err(crate::filesystem::interaction_host::interaction_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "selected history exceeds context message limit",
            ));
        }
        let lookup_bytes = 2 * acyclic_stream::MAX_RECORD_BYTES as u64;
        let (event, _) = crate::store::operations::find_operation_bounded_with_work(
            &self.stream,
            self.verifier.audience(),
            &self.verifier,
            turn_operation,
            lookup_bytes,
            true,
            &mut work,
        )
        .await?;
        let event = event.ok_or_else(|| {
            crate::filesystem::interaction_host::interaction_boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "turn has no committed model-context selection",
            )
        })?;
        let crate::core::EventPayload::ModelContextSelected {
            selection: committed,
        } = event.payload
        else {
            return Err(crate::filesystem::interaction_host::interaction_boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "turn operation is not a model-context selection",
            ));
        };
        if committed != selected.selection {
            return Err(crate::filesystem::interaction_host::interaction_boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "model-context selection does not match the committed turn",
            ));
        }
        let slots = committed
            .message_ids
            .len()
            .checked_mul(std::mem::size_of::<crate::conversation::ConversationMessage>())
            .ok_or_else(|| {
                crate::filesystem::interaction_host::interaction_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "selected history vector size overflows",
                )
            })?;
        if let Some(io) = io {
            io.retain_owned(slots as u64)?;
            io.allocation_attempt(slots as u64)?;
        }
        let mut loaded = Vec::with_capacity(committed.message_ids.len());
        for id in &committed.message_ids {
            let (message, _) = crate::store::operations::find_message_with_work(
                &self.stream,
                self.verifier.audience(),
                &self.verifier,
                *id,
                event.revision,
                lookup_bytes,
                &mut work,
            )
            .await?;
            loaded.push(message.ok_or_else(|| {
                crate::filesystem::interaction_host::interaction_boundary(
                    io,
                    crate::wire::ErrorCode::Storage,
                    "committed selected message is missing at its cutoff",
                )
            })?);
        }
        let (historical, _) = crate::conversation::ConversationState::from_wire(
            crate::conversation::ConversationStateWire {
                agent: None,
                messages: loaded,
                archived_prefix: None,
            },
            acyclic_stream::MAX_RECORD_BYTES as u64,
            &mut work,
        )?;
        let (messages, attachments, render_bytes) = self.owner.as_ref().map_or(
            (
                crate::conversation::MAX_PORTABLE_COUNT,
                crate::conversation::MAX_PORTABLE_COUNT,
                self.maximum_payload_bytes,
            ),
            |(owner, _)| {
                let limits = owner.input_limits();
                (
                    limits.context_messages,
                    limits.attachments,
                    limits.render_bytes,
                )
            },
        );
        let resolver = JournalSelectedResolver {
            journal: self,
            inner: verifier.as_ref(),
        };
        for message in historical.messages() {
            if message.kind != crate::conversation::MessageKind::ToolCall {
                self.verify_input_file_with_io(&message.content, io).await?;
            }
            match &message.attachments {
                crate::conversation::ReferencedAttachments::Inline { items } => {
                    for item in items {
                        self.verify_input_file_with_io(&item.file, io).await?;
                    }
                }
                crate::conversation::ReferencedAttachments::Manifest {
                    manifest,
                    item_count,
                } if message.kind == crate::conversation::MessageKind::ToolCall => {
                    if *item_count as usize > attachments {
                        return Err(canonical_boundary(
                            io,
                            crate::wire::ErrorCode::Invalid,
                            "selected manifest exceeds attachment limit",
                        ));
                    }
                    let (items, hold) =
                        crate::conversation::load_manifest(&resolver, manifest, *item_count, io)
                            .await?;
                    for item in &items {
                        self.verify_input_file_with_io(&item.file, io).await?;
                    }
                    drop(items);
                    drop(hold);
                }
                crate::conversation::ReferencedAttachments::Manifest { .. } => {}
            }
        }
        let projected = crate::projection::select_model_context(
            &historical,
            committed.conversation_revision,
            committed,
            &resolver,
            crate::conversation::Limits {
                context_messages: messages,
                attachments: attachments,
                render_bytes: render_bytes,
                ..Default::default()
            },
            attachments,
            io,
        )
        .await?;
        _query_hold = io
            .map(|io| {
                io.guard_owned(io.owned_bytes()?.checked_sub(before).ok_or_else(|| {
                    crate::filesystem::interaction_host::interaction_boundary(
                        Some(io),
                        crate::wire::ErrorCode::Invalid,
                        "selected query custody underflows",
                    )
                })?)
            })
            .transpose()?;
        if &projected != selected {
            return Err(crate::filesystem::interaction_host::interaction_boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "model context differs from committed conversation projection",
            ));
        }
        drop(projected);
        drop(historical);
        Ok(())
    }

    pub(super) fn authorize_selected_file_with_io(
        &self,
        file: &FileRef,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        if let Some((owner, _)) = &self.owner {
            owner.validate_input_file(file, io)
        } else {
            let mut borrowed = io.map(PreparationIo::history_io);
            file.validate(
                &mut borrowed
                    .as_mut()
                    .map(|work| work as &mut dyn crate::store::history::HistoryIo),
            )
        }
    }
}

pub(super) struct JournalSelectedResolver<'a, P, A, O> {
    pub(super) journal: &'a FilesystemExecutionJournal<P, A, O>,
    pub(super) inner: &'a dyn ContentResidencyVerifier,
}

impl<P, A, O> crate::conversation::ContentResidencyVerifier for JournalSelectedResolver<'_, P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn verify<'a>(
        &'a self,
        _file: &'a FileRef,
        _maximum_bytes: u64,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        Err(canonical_boundary(
            io,
            crate::wire::ErrorCode::Unsupported,
            "projection-only reader",
        ))
    }

    fn supports_read(&self, volume: &VolumeRef, io: Option<&PreparationIo>) -> Result<bool> {
        self.inner.supports_read(volume, io)
    }

    fn read<'a>(
        &'a self,
        file: &'a FileRef,
        maximum_bytes: u64,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        self.journal.authorize_selected_file_with_io(file, io)?;
        self.inner.read(file, maximum_bytes, io)
    }
}
