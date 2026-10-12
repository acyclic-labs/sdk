use super::*;

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) fn stage_identity_with_io(
        &self,
        operation_id: OperationId,
        idempotency_key: &str,
        io: Option<&PreparationIo>,
    ) -> Result<(IdempotencyKey, String)> {
        let digest = {
            let _input_hold;
            let input = crate::executor::with_history_io(io, |work| crate::contract::preparation_format(format_args!("{operation_id}:{idempotency_key}"), work))?;
            _input_hold = io
                .map(|io| io.guard_owned(input.capacity() as u64))
                .transpose()?;
            if let Some(io) = io {
                let hashing = acyclic_fs::WorkCounters {
                    bytes_hashed: input.len() as u64,
                    ..Default::default()
                };
                io.preflight(hashing)?;
                io.charge(hashing)?;
            }
            blake3::hash(input.as_bytes())
        };
        let key = IdempotencyKey::new(crate::executor::with_history_io(io, |work| crate::contract::preparation_format(format_args!("journal-{}", digest.to_hex()), work))?)?;
        let path = crate::executor::with_history_io(io, |work| crate::contract::preparation_format(format_args!(".system/execution/{operation_id}/{}.json", digest.to_hex()), work))?;
        Ok((key, path))
    }

    pub(super) async fn stage_with_io(
        &self,
        operation_id: OperationId,
        idempotency_key: &str,
        bytes: &[u8],
        media_type: &'static str,
        io: Option<&PreparationIo>,
    ) -> Result<FileRef> {
        if bytes.len() as u64 > self.maximum_payload_bytes {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "execution journal payload exceeds limit",
            ));
        }
        let _identity_hold;
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let (key, path) = self.stage_identity_with_io(operation_id, idempotency_key, io)?;
        _identity_hold = io
            .map(|io| {
                let retained = io.owned_bytes()?.checked_sub(before).ok_or_else(|| {
                    stock_boundary(
                        Some(io),
                        crate::wire::ErrorCode::Invalid,
                        "staging identity ownership underflow",
                    )
                })?;
                io.guard_owned(retained)
            })
            .transpose()?;
        let grant = crate::executor::with_history_io(io, |work| {
            ContentGrant::verify(
                &self.verifier,
                &self.scope,
                &self.volume,
                VolumeOperation::Write,
                io.map_or(Ok(u64::MAX), |io| {
                    io.remaining().map(|budget| budget.bytes_encoded)
                })?,
                work,
            )
        })?;
        let grant_hold = io
            .map(|io| io.guard_owned(grant.logical_owned_bytes()?))
            .transpose()?;
        let result = self
            .host
            .put_internal_content(
                &self.volume,
                &grant,
                &path,
                bytes,
                media_type,
                "execution.json",
                self.maximum_payload_bytes,
                &key,
                InternalContentClass::Execution,
                io,
            )
            .await;
        drop(grant);
        drop(grant_hold);
        result
    }

    pub(super) async fn load_content(
        &self,
        reference: &FileRef,
        io: Option<&PreparationIo>,
    ) -> Result<acyclic_stream::OwnedBytes> {
        if reference.volume() != &self.volume {
            return Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "journal content belongs to another private volume",
            ));
        }
        let maximum = io
            .map(|io| io.remaining().map(|budget| budget.bytes_encoded))
            .transpose()?
            .unwrap_or(u64::MAX);
        crate::executor::with_history_io(io, |work| {
            ContentGrant::authorize_volume(
                &self.verifier,
                &self.scope,
                &self.volume,
                VolumeOperation::Read,
                maximum,
                work,
            )
        })?;
        if io.is_none() {
            reference.validate(&mut None)?;
        }
        let bytes = self
            .host
            .read_pinned_io(reference, self.maximum_payload_bytes, io)
            .await?;
        if io.is_none() {
            self.host.retain_file_generation(reference).await?;
        }
        Ok(bytes)
    }

    pub(super) async fn verify_event_refs_with_io(
        &self,
        event: &ExecutionEvent,
        io: Option<&PreparationIo>,
    ) -> Result<Option<ModelEvent>> {
        let refs = match event {
            ExecutionEvent::ContextPrepared {
                projection,
                accounting,
                ..
            } => [Some(projection), accounting.as_ref(), None],
            ExecutionEvent::ContextCompacted {
                projection,
                compaction,
                accounting,
                ..
            } => [Some(projection), Some(compaction), Some(accounting)],
            ExecutionEvent::ModelStarted { request, .. } => [Some(request), None, None],
            ExecutionEvent::Model { event, .. } => [Some(event), None, None],
            ExecutionEvent::ToolStarted { invocation, .. } => [Some(invocation), None, None],
            ExecutionEvent::ToolCompleted {
                result, projection, ..
            } => [Some(result), Some(projection), None],
            ExecutionEvent::Started { .. } | ExecutionEvent::ToolFailed { .. } => {
                [None, None, None]
            }
        };
        let mut model = None;
        for reference in refs.into_iter().flatten() {
            if reference.volume() != &self.volume {
                return Err(canonical_boundary(
                    io,
                    crate::wire::ErrorCode::Unauthorized,
                    "journal event refers to another private volume",
                ));
            }
            let _bytes_hold;
            crate::model::charge_items(io, 1)?;
            let owned_bytes = self.load_content(reference, io).await?;
            _bytes_hold = io
                .map(|io| io.guard_bytes(&owned_bytes))
                .transpose()?
                .flatten();
            let bytes = owned_bytes.as_ref();
            if let ExecutionEvent::ModelStarted { request_digest, .. } = event {
                let limits = self
                    .owner
                    .as_ref()
                    .map_or_else(Limits::default, |(owner, _)| owner.input_limits());
                let before = io.map(PreparationIo::owned_bytes).transpose()?;
                let _prepared_hold;
                let prepared = match io {
                    Some(io) => {
                        crate::model::PreparedModelRequest::decode(&bytes, limits, Some(io))?
                    }
                    None => crate::model::PreparedModelRequest::decode(&bytes, limits, None)?,
                };
                _prepared_hold = match (io, before) {
                    (Some(io), Some(before)) => Some(io.guard_owned(
                        io.owned_bytes()?.checked_sub(before).ok_or_else(|| {
                            stock_boundary(
                                Some(io),
                                crate::wire::ErrorCode::Invalid,
                                "prepared model request custody underflow",
                            )
                        })?,
                    )?),
                    _ => None,
                };
                if &prepared.request_digest() != request_digest {
                    return Err(stock_boundary(
                        io,
                        crate::wire::ErrorCode::Invalid,
                        "model start digest differs from its retained request",
                    ));
                }
            }
            if matches!(event, ExecutionEvent::Model { .. }) {
                if reference.descriptor().media_type() != "application/json" {
                    return Err(canonical_boundary(
                        io,
                        crate::wire::ErrorCode::Storage,
                        "model observation is not JSON",
                    ));
                }
                model = Some(match io {
                    Some(io) => {
                        let parsed = io.decode(
                            std::marker::PhantomData::<ModelEvent>,
                            &bytes,
                            crate::wire::ErrorCode::Storage,
                        )?;
                        let _canonical_hold;
                        let canonical = io.encode(&parsed.0, self.maximum_payload_bytes, true)?;
                        _canonical_hold = io.guard_bytes(&canonical)?;
                        if canonical.as_ref() != bytes {
                            return Err(canonical_boundary(
                                Some(io),
                                crate::wire::ErrorCode::Storage,
                                "execution journal JSON is not canonical",
                            ));
                        }
                        crate::filesystem::retain_decoded(parsed)
                    }
                    None => decode_json::<ModelEvent>(&bytes, None)?.0,
                });
            }
        }
        Ok(model)
    }

    pub(super) async fn verify_input_file_with_io(
        &self,
        reference: &FileRef,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        if reference.descriptor().byte_length() > self.maximum_payload_bytes {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "turn input file exceeds journal limit",
            ));
        }
        if let Some((owner, _)) = &self.owner
            && owner.verify_created_input_with_io(reference, io).await?
        {
            return Ok(());
        }
        if reference.volume().provider() != &self.host.provider {
            let verifier = self.input_verifier.as_ref().ok_or_else(|| {
                canonical_boundary(
                    io,
                    crate::wire::ErrorCode::Unsupported,
                    "turn input file belongs to another content provider",
                )
            })?;
            return verifier
                .verify(reference, self.maximum_payload_bytes, io)?
                .await;
        }
        if let Some(io) = io {
            crate::model::charge_items(Some(io), 1)?;
if let Some(io) = Some(io) { io.retain_owned(reference.logical_owned_bytes()?)?; }
            reference.validate(&mut None)?;
            if reference.volume().class() == VolumeClass::AgentPrivate
                && reference.volume().owner() != self.volume.owner()
            {
                if crate::conversation::is_internal_path(reference.path())
                    && !crate::conversation::is_inherited_context_path(reference.path())
                {
                    return Err(canonical_boundary(
                        Some(io),
                        crate::wire::ErrorCode::Unauthorized,
                        "internal storage file is not public content",
                    ));
                }
                let mut work = io.history_io();
                self.verifier.verify(&self.scope, &mut Some(&mut work))?;
                if !io.contains_capability(self.scope.capabilities(), reference, "file:read:")? {
                    return Err(canonical_boundary(
                        Some(io),
                        crate::wire::ErrorCode::Unauthorized,
                        "exact file read is not granted",
                    ));
                }
            } else {
                crate::executor::with_history_io(Some(io), |work| {
                    ContentGrant::authorize_volume(
                        &self.verifier,
                        &self.scope,
                        reference.volume(),
                        VolumeOperation::Read,
                        io.remaining()?.bytes_encoded,
                        work,
                    )
                })?;
            }
            let before = io.owned_bytes()?;
            let _bytes_hold;
            let bytes = self
                .host
                .read_pinned_io(reference, self.maximum_payload_bytes, Some(io))
                .await?;
            _bytes_hold =
                io.guard_owned(io.owned_bytes()?.checked_sub(before).ok_or_else(|| {
                    stock_boundary(
                        Some(io),
                        crate::wire::ErrorCode::Invalid,
                        "input file custody underflow",
                    )
                })?)?;
            drop(bytes);
        } else {
            let grant = if reference.volume().class() == VolumeClass::AgentPrivate
                && reference.volume().owner() != self.volume.owner()
            {
                ContentGrant::verify_file_read(&self.verifier, &self.scope, reference)?
            } else {
                ContentGrant::verify(
                    &self.verifier,
                    &self.scope,
                    reference.volume(),
                    VolumeOperation::Read,
                    self.maximum_payload_bytes,
                    &mut None,
                )?
            };
            self.host
                .read_content(reference, &grant, self.maximum_payload_bytes, None)
                .await?;
        }
        Ok(())
    }

    pub(super) async fn verify_model_content_with_io(
        &self,
        content: &ModelContent,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        let mut count = 0_usize;
        for _ in content.file_refs() {
            crate::model::charge_items(io, 1)?;
            count = count.checked_add(1).ok_or_else(|| {
                stock_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "model reference count overflow",
                )
            })?;
        }
        let references_hold;
        let bytes = (count as u64)
            .checked_mul(std::mem::size_of::<(
                &FileRef,
                Option<&crate::model::NativeConfigurationBinding>,
            )>() as u64)
            .ok_or_else(|| {
                stock_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "model reference ownership overflow",
                )
            })?;
        if let Some(io) = io {
            io.retain_owned(bytes)?;
            references_hold = Some(io.guard_owned(bytes)?);
            if count != 0 {
                io.allocation_attempt(bytes)?;
            }
        } else {
            references_hold = None;
        }
        let mut references = Vec::with_capacity(count);
        for (file, binding) in content.file_bindings() {
            if let Some((owner, _)) = &self.owner {
                owner.validate_input_file(file, io)?;
            }
            if file.descriptor().byte_length() > self.maximum_payload_bytes {
                return Err(stock_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "model content exceeds journal limit",
                ));
            }
            references.push((file, binding));
        }
        for (_, binding) in &references {
            if let Some(binding) = binding {
                let (owner, _) = self.owner.as_ref().ok_or_else(|| {
                    canonical_boundary(
                        io,
                        crate::wire::ErrorCode::Unsupported,
                        "native options require an original task owner",
                    )
                })?;
                owner.validate_native_configuration(binding, io)?;
            }
        }
        for (_, binding) in &references {
            if let Some(binding) = binding {
                let (owner, _) = self.owner.as_ref().ok_or_else(|| {
                    canonical_boundary(
                        io,
                        crate::wire::ErrorCode::Unsupported,
                        "native options require an original task owner",
                    )
                })?;
                owner.verify_native_configuration(binding, io).await?;
            }
        }
        for (file, _) in &references {
            let mut configuration = false;
            for (_, binding) in &references {
                crate::model::charge_items(io, 1)?;
                if binding.is_some_and(|binding| &binding.configuration.content == *file) {
                    configuration = true;
                    break;
                }
            }
            if !configuration {
                self.verify_input_file_with_io(file, io).await?;
            }
        }
        drop(references);
        drop(references_hold);
        Ok(())
    }
    pub(super) async fn verify_tool_approval_with_io(
        &self,
        id: InteractionId,
        operation: OperationId,
        digest: [u8; 32],
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        self.require_operation(operation, io)?;
        let host = self.interactions.as_ref().ok_or_else(|| {
            crate::filesystem::interaction_host::interaction_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "task execution has no conversation interaction owner",
            )
        })?;
        let mut borrowed = io.map(PreparationIo::history_io);
        let mut work = borrowed
            .as_mut()
            .map(|work| work as &mut dyn crate::store::history::HistoryIo);
        if let Some((owner, _)) = &self.owner {
            owner.require_interaction_grant(&mut work)?;
            owner.verify(true, &mut work).await?;
        }
        host.verify_approval_with_io(id, operation, digest, io)
            .await?;
        if let Some((owner, _)) = &self.owner {
            owner.verify(true, &mut work).await?;
        }
        Ok(())
    }
}

use crate::executor::{stock_boundary};
