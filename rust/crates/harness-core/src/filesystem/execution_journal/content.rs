use super::*;

pub(super) fn operation_digest(
    operation: OperationId,
    key: &str,
    io: Option<&PreparationIo>,
) -> Result<blake3::Hash> {
    let (_hold, input) = {
        let input = crate::executor::with_history_io(io, |work| {
            crate::executor::preparation_format(format_args!("{operation}:{key}"), work)
        })?;
        let hold = io
            .map(|io| io.guard_owned(input.capacity() as u64))
            .transpose()?;
        (hold, input)
    };
    crate::model::hash_bytes(input.as_bytes(), io).map(blake3::Hash::from)
}

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    #[cfg(any(test, not(target_arch = "wasm32")))]
    #[doc(hidden)]
    pub async fn staged_ref_with_io(
        &self,
        operation: OperationId,
        key: &str,
        io: Option<&PreparationIo>,
    ) -> Result<FileRef> {
        let (_identity_hold, key, path) = {
            let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
            let (key, path) = self.stage_identity_with_io(operation, key, io)?;
            let hold = io
                .map(|io| io.guard_owned(io.owned_since(before)?))
                .transpose()?;
            (hold, key, path)
        };
        crate::executor::with_history_io(io, |work| {
            ContentGrant::authorize_volume(
                &self.verifier,
                &self.scope,
                &self.volume,
                VolumeOperation::Read,
                io.map(|io| io.remaining().map(|budget| budget.bytes_encoded))
                    .transpose()?
                    .unwrap_or(u64::MAX),
                work,
            )
        })?;
        let (file, bytes) = self
            .host
            .content_for_operation_record_io(
                super::super::PublicationLookup {
                    volume: &self.volume,
                    key: &key,
                    expected_path: Some(&path),
                    maximum_bytes: self.maximum_payload_bytes,
                    internal_class: Some(InternalContentClass::Execution),
                    purpose: super::super::PublicationPurpose::Content,
                    scope: &self.scope,
                    ceiling: None,
                },
                io,
            )
            .await?
            .ok_or_else(|| {
                canonical_boundary(
                    io,
                    crate::wire::ErrorCode::NotFound,
                    "original staged payload was not published",
                )
            })?;
        if let Some(bytes) = bytes {
            let hold = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten();
            drop(bytes);
            drop(hold);
        }
        Ok(file)
    }

    pub(super) fn stage_identity_with_io(
        &self,
        operation_id: OperationId,
        idempotency_key: &str,
        io: Option<&PreparationIo>,
    ) -> Result<(IdempotencyKey, String)> {
        let digest = operation_digest(operation_id, idempotency_key, io)?;
        let key = IdempotencyKey::new(crate::executor::with_history_io(io, |work| {
            crate::executor::preparation_format(format_args!("journal-{}", digest.to_hex()), work)
        })?)?;
        let path = crate::executor::with_history_io(io, |work| {
            crate::executor::preparation_format(
                format_args!(".system/execution/{operation_id}/{}.json", digest.to_hex()),
                work,
            )
        })?;
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
        let (_identity_hold, key, path) = {
            let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
            let (key, path) = self.stage_identity_with_io(operation_id, idempotency_key, io)?;
            let _identity_hold = io
                .map(|io| {
                    let retained = io.owned_since(before)?;
                    io.guard_owned(retained)
                })
                .transpose()?;
            (_identity_hold, key, path)
        };
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
        #[cfg(feature = "test-support")]
        if result.is_ok() {
            tracing::trace!(target: "acyclic_harness::journal_work",
                payload_stages = 1_u64, payload_stage_bytes = bytes.len() as u64,
                "execution payload published");
        }
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
            .read_pinned_receipt_io(reference, self.maximum_payload_bytes, io)
            .await?
            .retain()
            .await?;
        #[cfg(feature = "test-support")]
        tracing::trace!(target: "acyclic_harness::journal_work",
            payload_loads = 1_u64, payload_load_bytes = bytes.len() as u64,
            "execution payload loaded");
        Ok(bytes)
    }

    pub(super) fn authorize_inline_event(
        &self,
        event: &str,
        operation: VolumeOperation,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        if event.len() as u64 > self.inline_model_event_bytes() {
            return Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "inline model observation exceeds its bound",
            ));
        }
        crate::executor::with_history_io(io, |work| {
            ContentGrant::authorize_volume(
                &self.verifier,
                &self.scope,
                &self.volume,
                operation,
                io.map(|io| io.remaining().map(|budget| budget.bytes_encoded))
                    .transpose()?
                    .unwrap_or(u64::MAX),
                work,
            )
        })
    }

    pub(super) async fn verify_event_refs_with_io(
        &self,
        event: &ExecutionEvent,
        io: Option<&PreparationIo>,
    ) -> Result<Option<ModelEvent>> {
        #[cfg(not(target_arch = "wasm32"))]
        let recheck = io
            .and_then(PreparationIo::original_native_work)
            .map(|work| PreparationIo::from_native_work(&work.recheck_admitted()))
            .transpose()?;
        #[cfg(not(target_arch = "wasm32"))]
        let check_io = recheck.as_ref().or(io);
        #[cfg(target_arch = "wasm32")]
        let check_io = io;
        let result = async {
            let io = check_io;
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
                ExecutionEvent::Started { .. }
                | ExecutionEvent::ModelInline { .. }
                | ExecutionEvent::ModelStopped { .. }
                | ExecutionEvent::ToolFailed { .. } => [None, None, None],
            };
            let mut model = match event {
                ExecutionEvent::ModelInline { event, .. } => {
                    self.authorize_inline_event(event, VolumeOperation::Read, io)?;
                    Some(crate::executor::retain_preparation(decode_json::<
                        ModelEvent,
                    >(
                        event.as_bytes(),
                        io,
                    )?)?)
                }
                _ => None,
            };
            for reference in refs.into_iter().flatten() {
                let _bytes_hold;
                crate::model::charge_items(io, 1)?;
                let owned_bytes = self.load_content(reference, io).await?;
                _bytes_hold = io
                    .map(|io| io.guard_bytes(&owned_bytes))
                    .transpose()?
                    .flatten();
                let bytes = owned_bytes.as_ref();
                if let ExecutionEvent::ModelStarted { request_digest, .. } = event {
                    let limits = Limits::default();
                    let before = io.map(PreparationIo::owned_bytes).transpose()?;
                    if reference.descriptor().media_type()
                        == crate::executor::transcript::REQUEST_MEDIA
                    {
                        let scope = crate::filesystem::PreparationScope::new(io)?;
                        let manifest = crate::executor::retain_preparation(decode_json::<
                            crate::executor::transcript::RequestManifest,
                        >(
                            bytes, scope.io()
                        )?)?;
                        if manifest.authenticates(
                            &self.verifier,
                            limits,
                            *request_digest,
                            scope.io(),
                        )? {
                            continue;
                        }
                    }
                    let _prepared_hold;
                    let prepared = if reference.descriptor().media_type()
                        == crate::executor::transcript::REQUEST_MEDIA
                    {
                        crate::executor::load_model_request(self, reference, limits, io).await?
                    } else {
                        crate::model::PreparedModelRequest::decode(bytes, limits, io)?
                    };
                    _prepared_hold = match (io, before) {
                        (Some(io), Some(before)) => Some(io.guard_owned(io.owned_since(before)?)?),
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
                                bytes,
                                crate::wire::ErrorCode::Storage,
                            )?;
                            let _canonical_hold;
                            let canonical =
                                io.encode(&parsed.0, self.maximum_payload_bytes, true)?;
                            _canonical_hold = io.guard_bytes(&canonical)?;
                            if canonical.as_ref() != bytes {
                                return Err(canonical_boundary(
                                    Some(io),
                                    crate::wire::ErrorCode::Storage,
                                    "execution journal JSON is not canonical",
                                ));
                            }
                            crate::filesystem::retain_decoded(parsed)?
                        }
                        None => decode_json::<ModelEvent>(bytes, None)?.0,
                    });
                }
            }
            Ok(model)
        }
        .await;
        #[cfg(not(target_arch = "wasm32"))]
        if let (Ok(model), Some(recheck), Some(io)) = (&result, recheck, io) {
            let held = model
                .as_ref()
                .map_or(Ok(0), ModelEvent::logical_owned_bytes)?;
            io.absorb_native_custody(recheck.into_native_custody(held)?)?;
        }
        result
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
            if let Some(io) = Some(io) {
                io.retain_owned(reference.logical_owned_bytes()?)?;
            }
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
                if !io.contains_capability(
                    self.scope.capabilities(),
                    reference,
                    acyclic_fs::access::FILE_READ_PREFIX,
                )? {
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
            _bytes_hold = io.guard_owned(io.owned_since(before)?)?;
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

    pub(super) async fn verify_model_content_with_io<const RELEASE: bool>(
        &self,
        content: &ModelContent,
        io: Option<&PreparationIo>,
        context: Option<&crate::runtime::TaskContext>,
        summary_files: &[&FileRef],
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
            if file.descriptor().byte_length() > self.maximum_payload_bytes {
                return Err(stock_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "model content exceeds journal limit",
                ));
            }
            references.push((file, binding));
        }
        if io.is_none()
            && references.iter().any(|(_, binding)| binding.is_some())
            && let Some(context) = context
        {
            context
                .verify_model_contents(std::iter::once(content))
                .await?;
            for (file, _) in &references {
                self.verify_input_file_with_io(file, io).await?;
            }
            return Ok(());
        }
        if references.iter().any(|(_, binding)| binding.is_some()) {
            return Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "native options require their turn context",
            ));
        }
        for (file, _) in &references {
            if summary_files.contains(file) {
                let _bytes_hold;
                let bytes = self.load_content(file, io).await?;
                // Only the accounting test instantiates the omitted-release control.
                _bytes_hold = if RELEASE {
                    io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten()
                } else {
                    None
                };
                drop(bytes);
            } else {
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
        if !self
            .published_decision_for(id, Some((operation, digest)), io)
            .await?
            .approved()
        {
            return Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "tool installation lacks a resolved approval",
            ));
        }
        Ok(())
    }
}

use crate::executor::stock_boundary;
