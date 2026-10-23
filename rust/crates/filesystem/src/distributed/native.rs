use super::*;

impl<'a> AuthorityWork<'a> {
    pub(super) fn absorb(&mut self, child: &AuthorityWork<'_>) -> Result<(), AuthorityStoreError> {
        if !self.is_native() {
            self.work = self
                .work
                .with_backend(child.spent(), self.allocations.live_bytes())?;
            self.check()?;
        }
        Ok(())
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
        self.release(self.allocations.live_bytes())
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
        let (value, allocation) = self.vec_owned(capacity)?;
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
        let original = self.original.expect("native SDK funding");
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
        AuthorityStoreError::NativeOriginal(
            self.original
                .expect("native SDK funding")
                .runtime_error(error),
        )
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
        let buffer = self.byte_buffer(&[], &[payload])?;
        #[cfg(not(target_arch = "wasm32"))]
        if self.original.is_some() {
            let mut buffer = buffer;
            buffer.1.as_mut().expect("native buffer custody").grow(
                crate::kernel::allocation::bytes_owner_control_bytes::<ForkBytes>()?,
            )?;
            return Ok(Bytes::from_owner(ForkBytes {
                bytes: buffer.0,
                _allocation: buffer.1.expect("native buffer custody"),
            }));
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
        let buffer = self.byte_buffer(domain, fields)?;
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
        domain: &[u8],
        fields: &[&[u8]],
    ) -> Result<(Vec<u8>, AuthorityAllocation), AuthorityStoreError> {
        let length = fields
            .iter()
            .try_fold(domain.len(), |n, field| n.checked_add(field.len()))
            .ok_or(crate::WorkError::Overflow)?;
        let writes = WorkCounters {
            bytes_encoded: if domain.is_empty() { 0 } else { length as u64 },
            bytes_copied: length as u64,
            ..WorkCounters::UNCHARGED
        };
        self.admit(writes)?;
        let mut buffer = self.vec_owned::<u8>(length)?;
        buffer.0.extend_from_slice(domain);
        for field in fields {
            buffer.0.extend_from_slice(field);
        }
        self.charge(writes)?;
        Ok(buffer)
    }

    pub(super) fn path(
        &mut self,
        authority: AuthorityId,
        suffix: &str,
        generation: Option<GenerationId>,
    ) -> Result<acyclic_stream::StreamPath, AuthorityStoreError> {
        match generation {
            Some(generation) => {
                self.identity_path(authority, suffix, Some(generation.digest().as_bytes()))
            }
            None => self.identity_path(authority, suffix, None),
        }
    }

    pub(super) fn operation_path(
        &mut self,
        authority: AuthorityId,
        operation: OperationId,
    ) -> Result<StreamPath, AuthorityStoreError> {
        // Fixed-width authority and operation identities share one index parent.
        // Per-authority index directories otherwise consume one extra provider
        // path for every authority, even when it has only its initial operation.
        self.prefixed_identity_path(
            b"fs/operations/",
            authority,
            "",
            Some(&operation.into_bytes()),
        )
    }

    fn identity_path(
        &mut self,
        authority: AuthorityId,
        suffix: &str,
        identity: Option<&[u8]>,
    ) -> Result<StreamPath, AuthorityStoreError> {
        self.prefixed_identity_path(b"fs/authorities/", authority, suffix, identity)
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
            .filter(|n| *n <= acyclic_stream::MAX_PATH_BYTES)
            .ok_or(AuthorityStoreError::RejectedStatic("fork path is too long"))?;
        let writes = codec_work(length as u64, (prefix.len() + suffix.len()) as u64, 0);
        self.admit(writes)?;
        // Every SDK authority suffix fits here. Keep the full Stream bound
        // available in a separate frame for longer provider-internal suffixes.
        if length <= 128 {
            self.path_buffer::<128>(prefix, authority, suffix, identity, length, writes)
        } else {
            self.path_buffer::<{ acyclic_stream::MAX_PATH_BYTES }>(
                prefix, authority, suffix, identity, length, writes,
            )
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
        text.get_mut(..prefix.len())
            .ok_or(crate::WorkError::Overflow)?
            .copy_from_slice(prefix);
        let cursor = prefix.len() + 32;
        hex::encode_to_slice(
            authority.into_bytes(),
            text.get_mut(prefix.len()..cursor)
                .ok_or(crate::WorkError::Overflow)?,
        )
        .map_err(|_| crate::WorkError::Overflow)?;
        text.get_mut(cursor..cursor + suffix.len())
            .ok_or(crate::WorkError::Overflow)?
            .copy_from_slice(suffix.as_bytes());
        if let Some(identity) = identity {
            hex::encode_to_slice(
                identity,
                text.get_mut(cursor + suffix.len()..length)
                    .ok_or(crate::WorkError::Overflow)?,
            )
            .map_err(|_| crate::WorkError::Overflow)?;
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
        domain: &[u8],
        fields: &[&[u8]],
    ) -> Result<acyclic_stream::IdempotencyKey, AuthorityStoreError> {
        let bytes = self.stream_bytes(domain, fields)?;
        acyclic_stream::IdempotencyKey::new(bytes).map_err(|error| {
            if self.is_native() {
                AuthorityStoreError::NativeStream(error)
            } else {
                map_stream_error(error)
            }
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

    pub(super) fn append_record<const N: usize>(
        &mut self,
        mutations: &mut Vec<acyclic_stream::CommitMutation>,
        path: acyclic_stream::StreamPath,
        values: [acyclic_stream::OwnedBytes; N],
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

    pub(super) fn durable_digest(
        &mut self,
        authority: AuthorityId,
        record: &crate::streams_record::BorrowedStreamsDurableRecord<'_>,
    ) -> Result<Digest, AuthorityStoreError> {
        let hashing = codec_work(
            0,
            0,
            crate::foundation::AUTHORITY_COMMIT_DIGEST_ENVELOPE_BYTES
                .checked_add(record.payload.len() as u64)
                .ok_or(crate::WorkError::Overflow)?,
        );
        self.admit(hashing)?;
        let digest = authority_commit_digest(
            authority,
            record.epoch,
            record.sequence,
            record.operation_id,
            record.fingerprint,
            record.previous_digest,
            record.payload,
        );
        self.charge(hashing)?;
        Ok(digest)
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
