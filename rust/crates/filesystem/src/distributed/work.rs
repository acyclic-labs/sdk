use super::*;

impl<'a> AuthorityWork<'a> {
    pub(super) fn operation(work: &AuthorityWork<'a>) -> Result<Self, AuthorityStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = work.original {
            original.admit(WorkCounters::UNCHARGED)?;
            work.cancellation
                .check()
                .map_err(|_| AuthorityStoreError::Cancelled)?;
            return Ok(Self::native(original, work.cancellation));
        }
        Ok(Self::new(work.remaining()?, work.cancellation))
    }

    pub(super) fn operation_work(&self) -> Result<AuthorityWork<'a>, AuthorityStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = self.original {
            return Ok(AuthorityWork::native(original, self.cancellation));
        }
        self.check()?;
        Ok(AuthorityWork::new(self.remaining()?, self.cancellation))
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

    pub(super) fn is_native(&self) -> bool {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.original.is_some()
        }
        #[cfg(target_arch = "wasm32")]
        {
            false
        }
    }

    pub(super) fn record_completed(
        &mut self,
        delta: WorkCounters,
    ) -> Result<(), AuthorityStoreError> {
        self.charge(delta)?;
        self.check()
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(super) fn native_meter(&self) -> Arc<dyn acyclic_native_runtime::NativeReadMeter> {
        let original = self
            .original
            .expect("native SDK boundary requires original funding");
        original.state.clone()
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(super) fn native_io_failure(&self, error: std::io::Error) -> AuthorityStoreError {
        let original = self
            .original
            .expect("native I/O relay requires original funding");
        AuthorityStoreError::NativeOriginal(original.runtime_error(error))
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(super) fn result<T>(
        &self,
        result: acyclic_stream::NativeStreamResult<T>,
    ) -> Result<(T, ProviderAllocation), AuthorityStoreError> {
        let original = self
            .original
            .expect("native SDK relay requires original funding");
        let output = result
            .map_err(|error| AuthorityStoreError::NativeOriginal(original.runtime_error(error)))?
            .map_err(AuthorityStoreError::from)?;
        let (value, allocation) = output.into_parts();
        Ok((value, Some(allocation)))
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

    pub(super) fn vec_owned<T>(
        &mut self,
        capacity: usize,
    ) -> Result<(Vec<T>, AuthorityAllocation), AuthorityStoreError> {
        self.check()?;
        let bytes = capacity
            .checked_mul(std::mem::size_of::<T>())
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or(crate::WorkError::Overflow)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = self.original {
            original.admit(WorkCounters::UNCHARGED)?;
            self.cancellation
                .check()
                .map_err(|_| AuthorityStoreError::Cancelled)?;
            let buffer =
                crate::native_host::buffer::<T, _>(capacity, &mut &*original).map_err(|error| {
                    match error {
                        crate::NativeTreeMaterializationError::AllocationFailed(_) => {
                            AuthorityStoreError::RejectedStatic("fork allocation failed")
                        }
                        error => error.into(),
                    }
                })?;
            self.cancellation
                .check()
                .map_err(|_| AuthorityStoreError::Cancelled)?;
            return Ok((buffer.0, Some(buffer.1)));
        }
        if self.opaque_live_upper_bytes != 0 {
            let peak = self
                .allocations
                .live_bytes()
                .checked_add(bytes)
                .and_then(|live| live.checked_add(self.opaque_live_upper_bytes))
                .ok_or(crate::WorkError::Overflow)?;
            self.work.admit(
                &WorkCounters {
                    peak_allocation_bytes: peak,
                    ..WorkCounters::default()
                },
                &self.budget,
            )?;
        }
        self.allocations
            .claim_bytes(bytes, u64::from(bytes != 0), &mut self.work, self.budget)
            .map_err(|error| {
                error.into_work_or(AuthorityStoreError::RejectedStatic(
                    "fork allocation failed",
                ))
            })?;
        let mut value = Vec::<T>::new();
        if value.try_reserve_exact(capacity).is_err() {
            drop(value);
            self.release(bytes)?;
            return Err(AuthorityStoreError::RejectedStatic(
                "fork allocation failed",
            ));
        }
        let actual = value
            .capacity()
            .checked_mul(std::mem::size_of::<T>())
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or(crate::WorkError::Overflow)?;
        let excess = actual
            .checked_sub(bytes)
            .ok_or(crate::WorkError::Overflow)?;
        self.allocations
            .record_owned_bytes(excess, &mut self.work)
            .map_err(|error| {
                error.into_work_or(AuthorityStoreError::RejectedStatic(
                    "fork allocation failed",
                ))
            })?;
        let verified = self.work.verify(self.budget).and_then(|()| {
            if self.opaque_live_upper_bytes == 0 {
                return Ok(());
            }
            let peak = self
                .allocations
                .live_bytes()
                .checked_add(self.opaque_live_upper_bytes)
                .ok_or(crate::WorkError::Overflow)?;
            self.work.admit(
                &WorkCounters {
                    peak_allocation_bytes: peak,
                    ..WorkCounters::default()
                },
                &self.budget,
            )
        });
        if let Err(error) = verified {
            drop(value);
            self.release(actual)?;
            return Err(error.into());
        }
        Ok((value, None))
    }

    pub(super) fn bytes(
        &mut self,
        domain: &[u8],
        fields: &[&[u8]],
    ) -> Result<Bytes, AuthorityStoreError> {
        let buffer = self.byte_buffer(domain, fields)?;
        #[cfg(not(target_arch = "wasm32"))]
        if self.is_native() {
            let mut buffer = buffer;
            let original = self
                .original
                .expect("native allocation has its original owner");
            // Filesystem payloads keep their existing intrinsic Bytes custody.
            let mut control = original.claim(
                std::mem::size_of::<ForkBytes>() as u64,
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
            let value = Bytes::from_owner(ForkBytes {
                bytes: buffer.0,
                _allocation: buffer.1.expect("native buffer custody"),
            });
            return Ok(value);
        }
        // Audited bytes 1.12 allocates a private sharing owner only for spare capacity.
        let owner = WorkCounters {
            allocation_operations: u64::from(buffer.0.capacity() != buffer.0.len()),
            ..WorkCounters::default()
        };
        self.admit(owner)?;
        let value = Bytes::from(buffer.0);
        self.charge(owner)?;
        Ok(value)
    }

    /// Request records retain the canonical SDK byte carrier and original guard.
    pub(super) fn stream_bytes(
        &mut self,
        domain: &[u8],
        fields: &[&[u8]],
    ) -> Result<acyclic_stream::OwnedBytes, AuthorityStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if self.is_native() {
            let mut buffer = self.byte_buffer(domain, fields)?;
            let result = acyclic_stream::OwnedBytes::from_vec_with_guard_in_place(
                &mut buffer.0,
                &mut buffer.1,
                self.native_meter(),
            );
            return result.map_err(|error| self.native_io_failure(error));
        }
        self.bytes(domain, fields).map(Into::into)
    }

    pub(super) fn byte_buffer(
        &mut self,
        domain: &[u8],
        fields: &[&[u8]],
    ) -> Result<(Vec<u8>, AuthorityAllocation), AuthorityStoreError> {
        let length = fields
            .iter()
            .try_fold(domain.len(), |total, field| total.checked_add(field.len()))
            .ok_or(crate::WorkError::Overflow)?;
        // Framed records encode; an empty domain copies a raw payload.
        let writes = WorkCounters {
            bytes_encoded: if domain.is_empty() { 0 } else { length as u64 },
            bytes_copied: length as u64,
            ..WorkCounters::default()
        };
        self.admit(writes)?;
        // Physical storage always retires before its original claim on failure.
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
        if self.is_native() {
            self.admit(writes)?;
        } else {
            self.admit(WorkCounters {
                bytes_encoded: writes.bytes_encoded,
                bytes_copied: writes.bytes_copied + length as u64,
                allocation_operations: 1,
                peak_allocation_bytes: self
                    .allocations
                    .live_bytes()
                    .checked_add(length as u64)
                    .ok_or(crate::WorkError::Overflow)?,
                ..WorkCounters::default()
            })?;
        }
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

    pub(super) fn stream_key(
        &mut self,
        domain: &[u8],
        identity: &[u8],
    ) -> Result<acyclic_stream::IdempotencyKey, AuthorityStoreError> {
        let bytes = self.stream_bytes(domain, &[&[0], identity])?;
        acyclic_stream::IdempotencyKey::new(bytes).map_err(|error| {
            if self.is_native() {
                AuthorityStoreError::NativeStream(error)
            } else {
                map_stream_error(error)
            }
        })
    }

    pub(super) fn operation_key(
        &mut self,
        authority: AuthorityId,
        operation: OperationId,
    ) -> Result<acyclic_stream::IdempotencyKey, AuthorityStoreError> {
        let delta = WorkCounters {
            bytes_copied: 32,
            ..WorkCounters::default()
        };
        self.admit(delta)?;
        let mut identity = [0_u8; 32];
        identity[..16].copy_from_slice(&authority.into_bytes());
        identity[16..].copy_from_slice(&operation.into_bytes());
        self.charge(delta)?;
        self.stream_key(b"operation", &identity)
    }

    pub(super) fn request_write_work(
        &self,
        request: &acyclic_stream::CommitRequest,
    ) -> Result<WorkCounters, AuthorityStoreError> {
        let mut delta = WorkCounters {
            backend_write_operations: 1,
            durability_operations: 1,
            ..WorkCounters::default()
        };
        if self.is_native() {
            delta.backend_write_operations = 0;
            delta.durability_operations = 0;
        }
        for mutation in &request.mutations {
            let records = match mutation {
                acyclic_stream::CommitMutation::Append { records, .. }
                | acyclic_stream::CommitMutation::Fork { records, .. } => records,
            };
            delta.authority_records_appended = delta
                .authority_records_appended
                .checked_add(records.len() as u64)
                .ok_or(crate::WorkError::Overflow)?;
            for record in records {
                delta.authority_bytes_written = delta
                    .authority_bytes_written
                    .checked_add(record.len() as u64)
                    .ok_or(crate::WorkError::Overflow)?;
            }
        }
        Ok(delta)
    }

    pub(super) fn push<T>(
        &mut self,
        values: &mut Vec<T>,
        value: T,
    ) -> Result<(), AuthorityStoreError> {
        self.check()?;
        if values.len() == values.capacity() {
            return Err(AuthorityStoreError::RejectedStatic(
                "fork request exceeded its admitted vector capacity",
            ));
        }
        let delta = WorkCounters {
            bytes_copied: std::mem::size_of::<T>() as u64,
            ..WorkCounters::default()
        };
        self.admit(delta)?;
        values.push(value);
        self.charge(delta)
    }

    pub(super) fn append_record(
        &mut self,
        mutations: &mut Vec<acyclic_stream::CommitMutation>,
        path: acyclic_stream::StreamPath,
        value: acyclic_stream::OwnedBytes,
    ) -> Result<(), AuthorityStoreError> {
        let mut records = self.vec(1)?;
        self.push(&mut records, value)?;
        self.push(
            mutations,
            acyclic_stream::CommitMutation::Append { path, records },
        )
    }

    pub(super) fn decode_epoch(
        &mut self,
        bytes: &[u8],
        domain: &[u8],
    ) -> Result<Epoch, AuthorityStoreError> {
        if !self.is_native() {
            return decode_epoch(bytes, domain, false);
        }
        let copies = if bytes.len() == domain.len().saturating_add(8) && bytes.starts_with(domain) {
            8
        } else {
            0
        };
        let delta = WorkCounters {
            bytes_copied: copies,
            ..WorkCounters::default()
        };
        self.admit(delta)?;
        let result = decode_epoch(bytes, domain, true);
        self.charge(delta)?;
        result
    }

    pub(super) fn observe_record(
        &mut self,
        step: crate::kernel::ReadAllocationStep,
        invalid: &'static str,
    ) -> Result<(), AuthorityStoreError> {
        match step {
            crate::kernel::ReadAllocationStep::BeforeCopy(count) => self.admit(WorkCounters {
                bytes_copied: count,
                ..WorkCounters::UNCHARGED
            }),
            crate::kernel::ReadAllocationStep::Copied(count) => {
                self.record_completed(WorkCounters {
                    bytes_copied: count,
                    ..WorkCounters::UNCHARGED
                })
            }
            crate::kernel::ReadAllocationStep::BeforeHash(count) => self.check().and_then(|()| {
                self.admit(WorkCounters {
                    bytes_hashed: count,
                    ..WorkCounters::UNCHARGED
                })
            }),
            crate::kernel::ReadAllocationStep::Hashed(count) => self
                .record_completed(WorkCounters {
                    bytes_hashed: count,
                    ..WorkCounters::UNCHARGED
                })
                .and_then(|()| self.check()),
            _ => Err(authority_corrupt(invalid, self)),
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
pub(super) struct ForkBytes {
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
