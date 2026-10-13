use super::*;

impl<'a> AuthorityWork<'a> {
    pub(super) fn operation(&self) -> Result<AuthorityWork<'a>, AuthorityStoreError> {
        self.check()?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = self.original {
            return Ok(Self::native(original, self.cancellation));
        }
        Ok(Self::new(self.remaining()?, self.cancellation))
    }

    pub(super) fn absorb(&mut self, child: &AuthorityWork<'_>) -> Result<(), AuthorityStoreError> {
        if self.is_native() {
            return Ok(());
        }
        let mut delta = child.spent();
        delta.peak_allocation_bytes = self
            .allocations
            .live_bytes()
            .checked_add(delta.peak_allocation_bytes)
            .ok_or(crate::WorkError::Overflow)?;
        self.charge(delta)
    }

    pub(super) fn record_completed(
        &mut self,
        delta: WorkCounters,
    ) -> Result<(), AuthorityStoreError> {
        self.charge(delta)?;
        self.check()
    }

    pub(super) fn admit(&self, delta: WorkCounters) -> Result<(), AuthorityStoreError> {
        self.check()?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = self.original {
            return original.admit(delta).map_err(AuthorityStoreError::from);
        }
        let mut admitted = delta;
        admitted.peak_allocation_bytes = delta
            .peak_allocation_bytes
            .max(self.allocations.live_bytes())
            .checked_add(self.opaque_live_upper_bytes)
            .ok_or(crate::WorkError::Overflow)?;
        self.work
            .admit(&admitted, &self.budget)
            .map_err(AuthorityStoreError::from)
    }

    pub(super) fn charge(&mut self, delta: WorkCounters) -> Result<(), AuthorityStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = self.original {
            let result = original.record_admitted(delta);
            return result.map_err(AuthorityStoreError::from);
        }
        self.work.try_add_assign(&delta)?;
        self.check()
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
        let next = self
            .opaque_live_upper_bytes
            .checked_add(bytes)
            .ok_or(crate::WorkError::Overflow)?;
        let peak = self
            .allocations
            .live_bytes()
            .checked_add(next)
            .ok_or(crate::WorkError::Overflow)?;
        self.work.admit(
            &WorkCounters {
                peak_allocation_bytes: peak,
                ..WorkCounters::default()
            },
            &self.budget,
        )?;
        self.opaque_live_upper_bytes = next;
        Ok(())
    }

    pub(super) fn release_opaque(&mut self, bytes: u64) -> Result<(), AuthorityStoreError> {
        if self.is_native() {
            return Ok(());
        }
        self.opaque_live_upper_bytes = self
            .opaque_live_upper_bytes
            .checked_sub(bytes)
            .ok_or(crate::WorkError::Overflow)?;
        Ok(())
    }

    pub(super) fn vec<T>(&mut self, capacity: usize) -> Result<Vec<T>, AuthorityStoreError> {
        let (value, allocation) = self.vec_owned(capacity)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(mut allocation) = allocation {
            if let Some(request) = &mut self.request_allocation {
                if let Err(error) = request.absorb(&mut allocation) {
                    drop(value);
                    return Err(error.into());
                }
            } else {
                self.request_allocation = Some(allocation);
            }
        }
        #[cfg(target_arch = "wasm32")]
        let _ = allocation;
        Ok(value)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(super) fn native_meter(&self) -> Arc<dyn acyclic_native_runtime::NativeReadMeter> {
        self.original.expect("native SDK funding").state.clone()
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
            .map_err(AuthorityStoreError::from)?
            .into_parts();
        Ok((value, Some(allocation)))
    }

    pub(super) fn vec_owned<T>(
        &mut self,
        capacity: usize,
    ) -> Result<(Vec<T>, AuthorityAllocation), AuthorityStoreError> {
        self.check()?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = self.original {
            let buffer = crate::native_host::buffer::<T, _>(capacity, &mut &*original)?;
            self.check()?;
            return Ok((buffer.0, Some(buffer.1)));
        }
        let bytes = capacity
            .checked_mul(std::mem::size_of::<T>())
            .ok_or(crate::WorkError::Overflow)? as u64;
        self.admit(WorkCounters {
            allocation_operations: u64::from(bytes != 0),
            peak_allocation_bytes: self
                .allocations
                .live_bytes()
                .checked_add(bytes)
                .ok_or(crate::WorkError::Overflow)?,
            ..WorkCounters::UNCHARGED
        })?;
        let allocation_error = |error: crate::kernel::allocation::AllocationError| {
            error.into_work_or(AuthorityStoreError::RejectedStatic(
                "fork allocation failed",
            ))
        };
        self.allocations
            .claim_bytes(bytes, u64::from(bytes != 0), &mut self.work, self.budget)
            .map_err(allocation_error)?;
        let mut values = Vec::new();
        values
            .try_reserve_exact(capacity)
            .map_err(|_| AuthorityStoreError::RejectedStatic("fork allocation failed"))?;
        let actual = values
            .capacity()
            .checked_mul(std::mem::size_of::<T>())
            .ok_or(crate::WorkError::Overflow)? as u64;
        // Successful reserve guarantees capacity >= the requested element count.
        self.allocations
            .record_owned_bytes(actual - bytes, &mut self.work)
            .map_err(allocation_error)?;
        self.check()?;
        Ok((values, None))
    }

    pub(super) fn bytes(&mut self, payload: &[u8]) -> Result<Bytes, AuthorityStoreError> {
        let buffer = self.byte_buffer(&[], &[payload])?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = self.original {
            let mut buffer = buffer;
            let mut control = original.claim(
                crate::kernel::allocation::bytes_owner_control_bytes::<ForkBytes>()?,
                1,
                WorkCounters::UNCHARGED,
            )?;
            if let Err(error) = buffer
                .1
                .as_mut()
                .expect("native buffer custody")
                .absorb(&mut control)
            {
                drop(buffer);
                return Err(error.into());
            }
            return Ok(Bytes::from_owner(ForkBytes {
                bytes: buffer.0,
                _allocation: buffer.1.expect("native buffer custody"),
            }));
        }
        self.plain_bytes(buffer.0)
    }

    fn plain_bytes(&mut self, buffer: Vec<u8>) -> Result<Bytes, AuthorityStoreError> {
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
        let mut buffer = self.byte_buffer(domain, fields)?;
        #[cfg(not(target_arch = "wasm32"))]
        if self.is_native() {
            return acyclic_stream::OwnedBytes::from_vec_with_guard_in_place(
                &mut buffer.0,
                &mut buffer.1,
                self.native_meter(),
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
        const PREFIX: &[u8] = b"fs/authorities/";
        let length = PREFIX
            .len()
            .checked_add(32)
            .and_then(|n| n.checked_add(suffix.len()))
            .and_then(|n| n.checked_add(if generation.is_some() { 64 } else { 0 }))
            .filter(|n| *n <= acyclic_stream::MAX_PATH_BYTES)
            .ok_or_else(|| AuthorityStoreError::RejectedStatic("fork path is too long"))?;
        let writes = WorkCounters {
            bytes_encoded: length as u64,
            bytes_copied: (PREFIX.len() + suffix.len()) as u64,
            ..WorkCounters::default()
        };
        self.admit(writes)?;
        let mut text = [0_u8; acyclic_stream::MAX_PATH_BYTES];
        text[..PREFIX.len()].copy_from_slice(PREFIX);
        let cursor = PREFIX.len() + 32;
        hex::encode_to_slice(authority.into_bytes(), &mut text[PREFIX.len()..cursor])
            .expect("authority hex width");
        text[cursor..cursor + suffix.len()].copy_from_slice(suffix.as_bytes());
        if let Some(generation) = generation {
            hex::encode_to_slice(
                generation.digest().as_bytes(),
                &mut text[cursor + suffix.len()..length],
            )
            .expect("generation hex width");
        }
        self.charge(writes)?;
        let text = std::str::from_utf8(&text[..length])
            .map_err(|_| AuthorityStoreError::RejectedStatic("fork path is not UTF-8"))?;
        self.own_path(text)
    }

    pub(super) fn own_path(
        &mut self,
        text: &str,
    ) -> Result<acyclic_stream::StreamPath, AuthorityStoreError> {
        let length = text.len();
        #[cfg(not(target_arch = "wasm32"))]
        if self.is_native() {
            let result = acyclic_stream::StreamPath::new_native(text, &self.native_meter());
            return result
                .map_err(|error| self.native_io_failure(error))?
                .map_err(AuthorityStoreError::NativeStream);
        }
        self.admit(WorkCounters {
            allocation_operations: 1,
            peak_allocation_bytes: self
                .allocations
                .live_bytes()
                .checked_add(length as u64)
                .ok_or(crate::WorkError::Overflow)?,
            bytes_copied: length as u64,
            ..WorkCounters::default()
        })?;
        let path = acyclic_stream::StreamPath::new(text).map_err(map_stream_error)?;
        self.allocations
            .claim_bytes(length as u64, 1, &mut self.work, WorkBudget::UNBOUNDED)
            .map_err(|error| {
                error.into_work_or(AuthorityStoreError::Rejected(
                    "fork allocation failed".to_owned(),
                ))
            })?;
        self.charge(WorkCounters {
            bytes_copied: length as u64,
            ..WorkCounters::default()
        })?;
        Ok(path)
    }

    pub(super) fn lease_path(
        &mut self,
        workspace: crate::WorkspaceId,
        lease: crate::OperationLeaseId,
        expiry: u64,
    ) -> Result<acyclic_stream::StreamPath, AuthorityStoreError> {
        let mut text = [0_u8; acyclic_stream::MAX_PATH_BYTES];
        let length = crate::operation_window::stream_lease_path_encode(
            workspace,
            lease,
            expiry,
            &mut text,
            Some(&mut |completed, encoded, copied| {
                let work = WorkCounters {
                    bytes_encoded: encoded,
                    bytes_copied: copied,
                    ..WorkCounters::UNCHARGED
                };
                if completed {
                    self.charge(work)
                } else {
                    self.admit(work)
                }
            }),
        )?;
        let text = std::str::from_utf8(&text[..length])
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
                    authority_bytes_written: record.len() as u64,
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
        records.extend(values);
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
        let bytes = record_bytes::<8>(bytes, domain);
        let delta = WorkCounters {
            bytes_copied: if self.is_native() && bytes.is_some() {
                8
            } else {
                0
            },
            ..WorkCounters::UNCHARGED
        };
        // Ordinary epoch reads retain main's unmetered fixed-header decode.
        if self.is_native() {
            self.admit(delta)?;
        }
        let result = bytes
            .ok_or_else(|| authority_corrupt("invalid Stream epoch record", self))
            .and_then(|bytes| {
                Epoch::new(u64::from_le_bytes(*bytes))
                    .map_err(|_| authority_corrupt("zero Stream epoch", self))
            });
        if self.is_native() {
            self.charge(delta)?;
        }
        result
    }

    pub(super) fn observe_record(
        &mut self,
        step: crate::kernel::ReadAllocationStep,
        invalid: &'static str,
    ) -> Result<(), AuthorityStoreError> {
        use crate::kernel::ReadAllocationStep::{BeforeCopy, BeforeHash, Copied, Hashed};
        let delta = match step {
            BeforeCopy(bytes) | Copied(bytes) => WorkCounters {
                bytes_copied: bytes,
                ..WorkCounters::UNCHARGED
            },
            BeforeHash(bytes) | Hashed(bytes) => WorkCounters {
                bytes_hashed: bytes,
                ..WorkCounters::UNCHARGED
            },
            _ => return Err(authority_corrupt(invalid, self)),
        };
        match step {
            BeforeCopy(_) | BeforeHash(_) => self.admit(delta),
            Copied(_) | Hashed(_) => self.record_completed(delta),
            _ => unreachable!("validated decoder step"),
        }
    }

    pub(super) fn decode_durable<'record>(
        &mut self,
        bytes: &'record [u8],
    ) -> Result<crate::streams_record::BorrowedStreamsDurableRecord<'record>, AuthorityStoreError>
    {
        let result =
            StreamsDurableRecord::decode_borrowed_with(bytes, STREAM_RECORD_LIMIT, |step| {
                self.observe_record(step, "durable envelope emitted an allocating decoder step")
            })?;
        result.map_err(|error| {
            if self.is_native() {
                AuthorityStoreError::DurableCodec(error)
            } else {
                AuthorityStoreError::Corrupt(error.to_string())
            }
        })
    }

    pub(super) fn durable_digest(
        &mut self,
        authority: AuthorityId,
        record: &crate::streams_record::BorrowedStreamsDurableRecord<'_>,
    ) -> Result<Digest, AuthorityStoreError> {
        let hashing = WorkCounters {
            bytes_hashed: crate::foundation::AUTHORITY_COMMIT_DIGEST_ENVELOPE_BYTES
                .checked_add(record.payload.len() as u64)
                .ok_or(crate::WorkError::Overflow)?,
            ..WorkCounters::UNCHARGED
        };
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
        let copied = WorkCounters {
            bytes_copied: N as u64,
            ..WorkCounters::UNCHARGED
        };
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

pub(super) fn authority_error(error: crate::FsError) -> AuthorityStoreError {
    match error {
        crate::FsError::Authority(error) => error,
        #[cfg(not(target_arch = "wasm32"))]
        crate::FsError::NativeKernel(error) => AuthorityStoreError::NativeOriginal(error),
        crate::FsError::Work(error) => error.into(),
        crate::FsError::Cancelled(_) => AuthorityStoreError::Cancelled,
        crate::FsError::WorkspaceDeleted => AuthorityStoreError::Retired,
        other => AuthorityStoreError::Corrupt(other.to_string()),
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
