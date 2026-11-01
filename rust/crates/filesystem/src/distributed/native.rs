use super::*;

pub(super) const GENERATION_INDEX_DOMAIN: &[u8] = b"fs-generation-v1\0";
pub(super) const OPERATION_INDEX_DOMAIN: &[u8] = b"fs-operation-v1\0";

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn retain_native_custody(
    retained: &mut AuthorityAllocation,
    incoming: Option<&mut crate::NativeAllocation>,
    original: Option<&crate::NativeMaterializationWork>,
) -> Result<(), AuthorityStoreError> {
    if retained.is_some() || incoming.is_some() {
        let original = original.ok_or(AuthorityStoreError::NativeOriginalUnsupported)?;
        for allocation in retained.iter().chain(incoming.as_deref()) {
            let context = allocation
                .work()
                .authority_read_original()
                .unwrap_or(allocation.work());
            if !context.shares_invocation_context(original) {
                return Err(crate::NativeTreeMaterializationError::InvalidLayout.into());
            }
        }
    }
    if let Some(incoming) = incoming {
        // Keep both guards unchanged on context mismatch or overflow.
        match retained {
            Some(retained) => retained.absorb(incoming)?,
            retained => *retained = Some(incoming.split_off(incoming.bytes())?),
        }
    }
    Ok(())
}

impl<'a> AuthorityWork<'a> {
    #[cfg(not(target_arch = "wasm32"))]
    pub(super) fn retain_request_custody(
        &mut self,
        retained: &mut AuthorityAllocation,
    ) -> Result<(), AuthorityStoreError> {
        // Keep the original runtime read context cached in this work owner.
        retain_native_custody(retained, self.request_allocation.as_mut(), self.original)
    }

    pub(super) fn release(&mut self, bytes: u64) -> Result<(), AuthorityStoreError> {
        if self.is_native() {
            return Ok(());
        }
        self.allocations.release(bytes).map_err(|error| {
            error.into_work_or(AuthorityStoreError::RejectedStatic(
                "fork allocation custody mismatch",
            ))
        })
    }

    pub(super) fn release_request(&mut self) -> Result<(), AuthorityStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if self.is_native() {
            drop(self.request_allocation.take());
            return Ok(());
        }
        self.allocations
            .release_scratch_to(self.scratch_floor)
            .map_err(authority_error)
    }

    pub(super) fn hold_opaque(&mut self, bytes: u64) -> Result<(), AuthorityStoreError> {
        if self.is_native() {
            return self.check();
        }
        self.accounting()
            .claim_owned(bytes, 0)
            .map_err(authority_error)?;
        Ok(())
    }

    pub(super) fn vec<T>(&mut self, capacity: usize) -> Result<Vec<T>, AuthorityStoreError> {
        let (value, allocation) = crate::async_storage::authority_parts(self.vec_owned(capacity)?);
        let _ = &allocation;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(mut allocation) = allocation {
            match &mut self.request_allocation {
                Some(request) => {
                    if request.bytes() == 0
                        && request.work().authority_read_original().is_some()
                        && !request.work().shares_allocation_context(allocation.work())
                    {
                        *request = allocation;
                    } else if let Err(error) = request.absorb(&mut allocation) {
                        drop(value);
                        return Err(error.into());
                    }
                }
                request => *request = Some(allocation),
            }
        }
        Ok(value)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(super) fn native_meter(
        &mut self,
    ) -> Result<Arc<dyn acyclic_native_runtime::NativeReadMeter>, AuthorityStoreError> {
        let original = self
            .original
            .ok_or(AuthorityStoreError::NativeOriginalUnsupported)?;
        if let Some(allocation) = &self.request_allocation
            && let Some(meter) = allocation.work().authority_read_meter_for(original)
        {
            original.admit(WorkCounters::UNCHARGED)?;
            return Ok(meter);
        }
        if self
            .request_allocation
            .as_ref()
            .is_some_and(|a| a.bytes() == 0)
        {
            drop(self.request_allocation.take());
        }
        let mut allocation = original.authority_read_allocation()?;
        let meter = allocation.work().runtime_meter();
        if let Some(request) = &mut self.request_allocation {
            // Public rebinding must retain the old request's paid capacity.
            if !request.work().shares_allocation_context(original) {
                return Ok(meter);
            }
            allocation.absorb(request)?;
        }
        self.request_allocation = Some(allocation);
        Ok(meter)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(super) fn native_io_failure(&self, error: std::io::Error) -> AuthorityStoreError {
        match self.original {
            Some(original) => AuthorityStoreError::NativeOriginal(original.runtime_error(error)),
            None => AuthorityStoreError::Io(error),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(super) fn result<T>(
        &self,
        result: acyclic_stream::NativeStreamResult<T>,
    ) -> Result<(T, ProviderAllocation), AuthorityStoreError> {
        let (value, allocation) = result
            .map_err(|error| self.native_io_failure(error))?
            .map_err(|error| match error {
                acyclic_stream::StreamError::NotFound => AuthorityStoreError::Missing,
                error => AuthorityStoreError::from(error),
            })?
            .into_parts();
        Ok((value, Some(allocation)))
    }

    pub(super) fn bytes(&mut self, payload: &[u8]) -> Result<Bytes, AuthorityStoreError> {
        let buffer = self.byte_buffer(&[], &[], &[payload])?;
        #[cfg(not(target_arch = "wasm32"))]
        if self.original.is_some() {
            let mut buffer = buffer;
            buffer
                .1
                .as_mut()
                .ok_or(crate::NativeTreeMaterializationError::MissingCustody)?
                .grow(crate::kernel::allocation::bytes_owner_control_bytes::<
                    ForkBytes,
                >()?)?;
            return match buffer {
                (bytes, Some(allocation)) => Ok(Bytes::from_owner(ForkBytes {
                    bytes,
                    _allocation: allocation,
                })),
                (_, None) => Err(crate::NativeTreeMaterializationError::MissingCustody.into()),
            };
        }
        self.plain_bytes(buffer.0)
    }

    pub(super) fn plain_bytes(&mut self, buffer: Vec<u8>) -> Result<Bytes, AuthorityStoreError> {
        let owner = WorkCounters {
            allocation_operations: u64::from(buffer.capacity() != buffer.len()),
            ..WorkCounters::UNCHARGED
        };
        self.admit(owner)?;
        let bytes = Bytes::from(buffer);
        self.charge(owner)?;
        Ok(bytes)
    }

    pub(super) fn stream_bytes(
        &mut self,
        domain: &[u8],
        fields: &[&[u8]],
    ) -> Result<acyclic_stream::OwnedBytes, AuthorityStoreError> {
        self.scoped_stream_bytes(&[], domain, fields)
    }

    fn scoped_stream_bytes(
        &mut self,
        scope: &[u8],
        domain: &[u8],
        fields: &[&[u8]],
    ) -> Result<acyclic_stream::OwnedBytes, AuthorityStoreError> {
        let buffer = self.byte_buffer(scope, domain, fields)?;
        #[cfg(not(target_arch = "wasm32"))]
        let mut buffer = buffer;
        #[cfg(not(target_arch = "wasm32"))]
        if self.is_native() {
            return acyclic_stream::OwnedBytes::from_vec_with_guard_in_place(
                &mut buffer.0,
                &mut buffer.1,
                &self.native_meter()?,
            )
            .map_err(|error| self.native_io_failure(error));
        }
        self.plain_bytes(buffer.0).map(Into::into)
    }

    fn byte_buffer(
        &mut self,
        scope: &[u8],
        domain: &[u8],
        fields: &[&[u8]],
    ) -> Result<(Vec<u8>, AuthorityAllocation), AuthorityStoreError> {
        let length = fields
            .iter()
            .try_fold(domain.len(), |n, field| n.checked_add(field.len()))
            .and_then(|n| n.checked_add(scope.len()))
            .ok_or(crate::WorkError::Overflow)?;
        let writes = WorkCounters {
            bytes_encoded: if scope.is_empty() && domain.is_empty() {
                0
            } else {
                length as u64
            },
            bytes_copied: length as u64,
            ..WorkCounters::UNCHARGED
        };
        self.admit(writes)?;
        let mut buffer = self.vec_owned::<u8>(length)?;
        buffer.value.extend_from_slice(scope);
        buffer.value.extend_from_slice(domain);
        for field in fields {
            buffer.value.extend_from_slice(field);
        }
        self.charge(writes)?;
        Ok(crate::async_storage::authority_parts(buffer))
    }

    pub(super) fn path(
        &mut self,
        placement: &AuthorityPlacement,
        authority: AuthorityId,
        suffix: &str,
        generation: Option<GenerationId>,
    ) -> Result<acyclic_stream::StreamPath, AuthorityStoreError> {
        match generation {
            Some(generation) => {
                self.identity_path(placement, authority, suffix, Some(&generation.to_bytes()))
            }
            None => self.identity_path(placement, authority, suffix, None),
        }
    }

    pub(super) fn operation_path(
        &mut self,
        placement: &AuthorityPlacement,
        authority: AuthorityId,
        operation: OperationId,
    ) -> Result<StreamPath, AuthorityStoreError> {
        // Fixed-width authority and operation identities share one index parent.
        // Per-authority index directories otherwise consume one extra provider
        // path for every authority, even when it has only its initial operation.
        self.prefixed_identity_path(
            placement.operations_prefix(),
            authority,
            "",
            Some(&operation.into_bytes()),
        )
    }

    fn identity_path(
        &mut self,
        placement: &AuthorityPlacement,
        authority: AuthorityId,
        suffix: &str,
        identity: Option<&[u8]>,
    ) -> Result<StreamPath, AuthorityStoreError> {
        self.prefixed_identity_path(placement.authorities_prefix(), authority, suffix, identity)
    }

    fn prefixed_identity_path(
        &mut self,
        prefix: &[u8],
        authority: AuthorityId,
        suffix: &str,
        identity: Option<&[u8]>,
    ) -> Result<StreamPath, AuthorityStoreError> {
        let length = prefix
            .len()
            .checked_add(32)
            .and_then(|n| n.checked_add(suffix.len()))
            .and_then(|n| n.checked_add(identity.map_or(0, |bytes| bytes.len() * 2)))
            .ok_or(crate::WorkError::Overflow)?;
        let writes = codec_work(length as u64, (prefix.len() + suffix.len()) as u64, 0);
        self.admit(writes)?;
        // Inline scratch preserves the common path's original allocation.
        // Longer values spill into caller-funded storage without a width refusal.
        if length <= 128 {
            self.path_buffer::<128>(prefix, authority, suffix, identity, length, writes)
        } else if length <= 65_535 {
            self.path_buffer::<65_535>(prefix, authority, suffix, identity, length, writes)
        } else {
            let mut text = self.vec_owned::<u8>(length)?;
            text.value
                .extend(identity_path_bytes(prefix, authority, suffix, identity));
            self.charge(writes)?;
            let path = std::str::from_utf8(&text.value)
                .map_err(|_| AuthorityStoreError::RejectedStatic("fork path is not UTF-8"))?;
            self.own_path(path)
        }
    }

    #[inline(never)]
    fn path_buffer<const N: usize>(
        &mut self,
        prefix: &[u8],
        authority: AuthorityId,
        suffix: &str,
        identity: Option<&[u8]>,
        length: usize,
        writes: WorkCounters,
    ) -> Result<StreamPath, AuthorityStoreError> {
        let mut text = [0_u8; N];
        let output = text.get_mut(..length).ok_or(crate::WorkError::Overflow)?;
        for (slot, byte) in output
            .iter_mut()
            .zip(identity_path_bytes(prefix, authority, suffix, identity))
        {
            *slot = byte;
        }
        self.charge(writes)?;
        let text = std::str::from_utf8(text.get(..length).ok_or(crate::WorkError::Overflow)?)
            .map_err(|_| AuthorityStoreError::RejectedStatic("fork path is not UTF-8"))?;
        self.own_path(text)
    }

    pub(super) fn own_path(&mut self, text: &str) -> Result<StreamPath, AuthorityStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if self.is_native() {
            return StreamPath::new_native(text, &self.native_meter()?)
                .map_err(|error| self.native_io_failure(error))?
                .map_err(AuthorityStoreError::NativeStream);
        }
        let copying = codec_work(0, text.len() as u64, 0);
        self.admit(copying)?;
        self.accounting()
            .claim_owned(text.len() as u64, 1)
            .map_err(authority_error)?;
        let path = StreamPath::new(text).map_err(map_stream_error)?;
        self.charge(copying)?;
        Ok(path)
    }

    pub(super) fn lease_path(
        &mut self,
        workspace: crate::WorkspaceId,
        lease: crate::OperationLeaseId,
        expiry: u64,
    ) -> Result<acyclic_stream::StreamPath, AuthorityStoreError> {
        let mut text = [0_u8; crate::operation_window::lease_path::MAX_ENCODED_BYTES];
        let length = crate::operation_window::lease_path::encode(
            workspace,
            lease,
            expiry,
            &mut text,
            Some(self),
        )?;
        let text = std::str::from_utf8(text.get(..length).ok_or(crate::WorkError::Overflow)?)
            .map_err(|_| authority_corrupt("lease path is not UTF-8", self))?;
        self.own_path(text)
    }

    pub(super) fn key(
        &mut self,
        scope: &[u8],
        domain: &[u8],
        fields: &[&[u8]],
    ) -> Result<acyclic_stream::IdempotencyKey, AuthorityStoreError> {
        let bytes = self.scoped_stream_bytes(scope, domain, fields)?;
        acyclic_stream::IdempotencyKey::new(bytes).map_err(|error| {
            if self.is_native() {
                AuthorityStoreError::NativeStream(error)
            } else {
                map_stream_error(error)
            }
        })
    }

    pub(super) fn folded_key(
        &mut self,
        domain: &[u8],
        left: &acyclic_stream::IdempotencyKey,
        right: &acyclic_stream::IdempotencyKey,
    ) -> Result<acyclic_stream::IdempotencyKey, AuthorityStoreError> {
        let fields = [left.as_bytes(), right.as_bytes()];
        let bytes = fields
            .iter()
            .try_fold(domain.len() as u64, |bytes, field| {
                bytes.checked_add(8)?.checked_add(field.len() as u64)
            })
            .ok_or(crate::WorkError::Overflow)?;
        let hashing = codec_work(0, 0, bytes);
        self.admit(hashing)?;
        let mut hasher = blake3::Hasher::new();
        hasher.update(domain);
        for field in fields {
            hasher.update(&(field.len() as u64).to_le_bytes());
            hasher.update(field);
        }
        let digest = hasher.finalize();
        self.charge(hashing)?;
        self.key(&[], domain, &[digest.as_bytes()])
    }

    /// Copies only request containers; immutable payload/path owners are shared.
    /// Preparation, submission and Unknown retries all use the original ledger.
    pub(super) fn copy_commit(
        &mut self,
        request: &CommitRequest,
    ) -> Result<CommitRequest, AuthorityStoreError> {
        let examined = WorkCounters {
            items_examined: (request.conditions.len() + request.mutations.len()) as u64,
            ..WorkCounters::UNCHARGED
        };
        self.admit(examined)?;
        let mut conditions = self.vec(request.conditions.len())?;
        conditions.extend(request.conditions.iter().cloned());
        let mut mutations = self.vec(request.mutations.len())?;
        for mutation in &request.mutations {
            let (CommitMutation::Append { records, .. } | CommitMutation::Fork { records, .. }) =
                mutation;
            let copied = WorkCounters {
                items_examined: records.len() as u64,
                ..WorkCounters::UNCHARGED
            };
            self.admit(copied)?;
            let mut copied_records = self.vec(records.len())?;
            copied_records.extend(records.iter().cloned());
            self.charge(copied)?;
            mutations.push(match mutation {
                CommitMutation::Append { path, .. } => CommitMutation::Append {
                    path: path.clone(),
                    records: copied_records,
                },
                CommitMutation::Fork {
                    exclude,
                    source,
                    destination,
                    at_tail,
                    ..
                } => {
                    let copied = WorkCounters {
                        items_examined: exclude.len() as u64,
                        ..WorkCounters::UNCHARGED
                    };
                    self.admit(copied)?;
                    let mut exclusions = self.vec(exclude.len())?;
                    exclusions.extend(exclude.iter().cloned());
                    self.charge(copied)?;
                    CommitMutation::Fork {
                        exclude: exclusions,
                        source: source.clone(),
                        destination: destination.clone(),
                        at_tail: *at_tail,
                        records: copied_records,
                    }
                }
            });
        }
        self.charge(examined)?;
        Ok(CommitRequest {
            conditions,
            mutations,
            idempotency_key: request.idempotency_key.clone(),
            deadline_unix_millis: request.deadline_unix_millis,
        })
    }

    pub(super) fn request_write_work(
        &self,
        request: &CommitRequest,
    ) -> Result<WorkCounters, AuthorityStoreError> {
        let physical = u64::from(!self.is_native());
        let mut delta = WorkCounters {
            backend_write_operations: physical,
            durability_operations: physical,
            ..WorkCounters::UNCHARGED
        };
        for mutation in &request.mutations {
            let (CommitMutation::Append { records, .. } | CommitMutation::Fork { records, .. }) =
                mutation;
            for record in records {
                delta = delta.checked_add(WorkCounters {
                    authority_records_appended: 1,
                    authority_bytes_written: record.payload_bytes() as u64,
                    ..WorkCounters::UNCHARGED
                })?;
            }
        }
        Ok(delta)
    }

    pub(super) fn publication_record(
        &mut self,
        record: &crate::streams_record::BorrowedStreamsDurableRecord<'_>,
        generation: Option<GenerationId>,
    ) -> Result<acyclic_stream::AppendRecord, AuthorityStoreError> {
        let operation = record.operation_id.into_bytes();
        let reference = generation.map(|generation| generation.to_bytes());
        // Streams admits value and indexed headers together. Reserve the exact
        // header bytes before the canonical encoder allocates the durable value.
        let header_bytes = OPERATION_INDEX_DOMAIN.len()
            + operation.len()
            + reference.as_ref().map_or(0, |reference| {
                GENERATION_INDEX_DOMAIN.len() + reference.len()
            });
        let value = StreamsDurableRecord::encode_borrowed_with(
            record,
            STREAM_RECORD_LIMIT - header_bytes as u64,
            |domain, fields| self.stream_bytes(domain, fields),
        )
        .map_err(AuthorityStoreError::DurableCodec)??;
        let headers = match reference {
            Some(reference) => {
                let split = GENERATION_INDEX_DOMAIN.len() + reference.len();
                let keys = self.stream_bytes(
                    GENERATION_INDEX_DOMAIN,
                    &[reference.as_slice(), OPERATION_INDEX_DOMAIN, &operation],
                )?;
                // The two domains establish canonical order without sorting.
                self.record_keys([keys.slice(..split), keys.slice(split..)])?
            }
            None => {
                let key = self.stream_bytes(OPERATION_INDEX_DOMAIN, &[&operation])?;
                self.record_keys([key])?
            }
        };
        Ok(acyclic_stream::AppendRecord { value, headers })
    }

    fn record_keys<const N: usize>(
        &mut self,
        keys: [acyclic_stream::OwnedBytes; N],
    ) -> Result<acyclic_stream::RecordHeaders, AuthorityStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if self.is_native() {
            return acyclic_stream::RecordHeaders::from_keys_native(keys, &self.native_meter()?)
                .map_err(|error| self.native_io_failure(error))?
                .map_err(Into::into);
        }
        let mut owned = self.vec(N)?;
        owned.extend(keys);
        self.accounting()
            .claim_owned(acyclic_stream::RecordHeaders::control_bytes() as u64, 1)
            .map_err(authority_error)?;
        acyclic_stream::RecordHeaders::new(owned, Vec::new()).map_err(Into::into)
    }

    pub(super) fn append_record<const N: usize, V: Into<acyclic_stream::AppendRecord>>(
        &mut self,
        mutations: &mut Vec<acyclic_stream::CommitMutation>,
        path: acyclic_stream::StreamPath,
        values: [V; N],
    ) -> Result<(), AuthorityStoreError> {
        let mut records = self.vec(N)?;
        records.extend(values.into_iter().map(Into::into));
        if mutations.len() == mutations.capacity() {
            return Err(AuthorityStoreError::RejectedStatic(
                "request exceeded its admitted vector capacity",
            ));
        }
        mutations.push(acyclic_stream::CommitMutation::Append { path, records });
        Ok(())
    }

    pub(super) fn decode_epoch(
        &mut self,
        bytes: &[u8],
        domain: &[u8],
    ) -> Result<Epoch, AuthorityStoreError> {
        if self.is_native() {
            self.check()?;
        }
        let bytes = record_bytes::<8>(bytes, domain)
            .ok_or_else(|| authority_corrupt("invalid Stream epoch record", self))?;
        // Ordinary epoch reads retain main's unmetered fixed-header decode.
        let bytes = if self.is_native() {
            self.copy_array(bytes)?
        } else {
            *bytes
        };
        Epoch::new(u64::from_le_bytes(bytes))
            .map_err(|_| authority_corrupt("zero Stream epoch", self))
    }

    pub(super) fn decode_durable<'record>(
        &mut self,
        bytes: &'record [u8],
    ) -> Result<crate::streams_record::BorrowedStreamsDurableRecord<'record>, AuthorityStoreError>
    {
        let native = self.is_native();
        crate::kernel::decode_owned(&mut self.accounting(), |observe| {
            StreamsDurableRecord::decode_borrowed_with(bytes, STREAM_RECORD_LIMIT, observe)?
                .map_err(|error| {
                    crate::FsError::Authority(if native {
                        AuthorityStoreError::DurableCodec(error)
                    } else {
                        AuthorityStoreError::Corrupt(error.to_string())
                    })
                })
        })
        .map(|(record, _)| record)
        .map_err(authority_error)
    }

    pub(super) fn decode_fixed<const N: usize>(
        &mut self,
        value: &[u8],
        domain: &[u8],
        invalid: &'static str,
    ) -> Result<[u8; N], AuthorityStoreError> {
        let bytes =
            record_bytes::<N>(value, domain).ok_or_else(|| authority_corrupt(invalid, self))?;
        self.copy_array(bytes)
    }

    fn copy_array<const N: usize>(
        &mut self,
        bytes: &[u8; N],
    ) -> Result<[u8; N], AuthorityStoreError> {
        let copied = codec_work(0, N as u64, 0);
        self.admit(copied)?;
        let bytes = *bytes;
        self.charge(copied)?;
        Ok(bytes)
    }

    pub(super) fn decode_gate(
        &mut self,
        value: &[u8],
    ) -> Result<Option<OperationId>, AuthorityStoreError> {
        if value == PUBLICATION_GATE_FREE {
            return Ok(None);
        }
        self.decode_fixed::<16>(
            value,
            PUBLICATION_GATE_ACTIVE,
            "invalid publication gate record",
        )
        .map(|bytes| Some(OperationId::from_bytes(bytes)))
    }

    pub(super) fn finish<T>(&self, value: T) -> Result<T, AuthorityStoreError> {
        self.check()?;
        Ok(value)
    }
}

#[cfg(not(target_arch = "wasm32"))]
struct ForkBytes {
    bytes: Vec<u8>,
    _allocation: crate::NativeAllocation,
}

#[cfg(not(target_arch = "wasm32"))]
impl AsRef<[u8]> for ForkBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

/// Provider failures do not expose whether a lost acknowledgement made records durable.
pub(super) fn commit_work(
    submitted: WorkCounters,
    result: &Result<acyclic_stream::CommitOutcome, acyclic_stream::StreamError>,
) -> WorkCounters {
    if matches!(result, Ok(acyclic_stream::CommitOutcome::Committed(_))) {
        submitted
    } else {
        WorkCounters {
            backend_write_operations: 1,
            ..WorkCounters::default()
        }
    }
}

pub(super) fn codec_work(encoded: u64, copied: u64, hashed: u64) -> WorkCounters {
    WorkCounters {
        bytes_encoded: encoded,
        bytes_copied: copied,
        bytes_hashed: hashed,
        ..WorkCounters::UNCHARGED
    }
}

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
#[path = "native_path_tests.rs"]
mod path_tests;
#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
#[path = "native_tests.rs"]
mod tests;

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
#[path = "native_lease_path_tests.rs"]
mod lease_path_tests;

/// Exact canonical encoding into either inline scratch or funded spill storage.
fn identity_path_bytes<'a>(
    prefix: &'a [u8],
    authority: AuthorityId,
    suffix: &'a str,
    identity: Option<&'a [u8]>,
) -> impl Iterator<Item = u8> + 'a {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let hex = |byte: u8| [HEX[usize::from(byte >> 4)], HEX[usize::from(byte & 15)]];
    prefix
        .iter()
        .copied()
        .chain(authority.into_bytes().into_iter().flat_map(hex))
        .chain(suffix.bytes())
        .chain(identity.into_iter().flatten().copied().flat_map(hex))
}
