//! Filesystem storage bindings over the canonical public service providers.

use acyclic_stream::ProviderTask;
use crate::cancellation::CancellationToken;
use crate::foundation::{
    AuthorityId, Digest, DurableCommit, Epoch, GenerationId, Head, OperationId, ProposedCommit,
    Sequence, authority_commit_digest,
};
use crate::kernel::volume_authority_id;
use crate::performance::{OperationFailure, WorkBudget, WorkCounters};
use crate::storage::{
    AppendOutcome as FsAppendOutcome, AuthorityReceipt, AuthorityResult, AuthorityStoreError,
    CreateAuthorityOutcome, FenceOutcome, ObjectId, PublicationPermit, PublicationReservation,
    ReplayLimit, ReservationOutcome,
};
use crate::streams_record::StreamsDurableRecord;
use crate::{AsyncAuthorityStore, WorkspaceForkCommit, WorkspaceForkOutcome};
use bytes::Bytes;
use futures::StreamExt;
use std::sync::Arc;

const STREAM_RECORD_LIMIT: u64 = acyclic_stream::MAX_RECORD_BYTES as u64;
const GENESIS_DOMAIN: &[u8] = b"acyclic-fs-stream-genesis-v1\0";
const EPOCH_DOMAIN: &[u8] = b"acyclic-fs-stream-epoch-v1\0";
const LINEAGE_DOMAIN: &[u8] = b"acyclic-fs-stream-lineage-v1\0";
const LINEAGE_TAIL_DOMAIN: &[u8] = b"acyclic-fs-stream-lineage-tail-v1\0";
const AUTHORITY_MARKER: &[u8] = b"acyclic-fs-authority-v1\0";
const AUTHORITY_RETIRED: &[u8] = b"acyclic-fs-authority-retired-v1\0";
const PUBLICATION_GATE_FREE: &[u8] = b"acyclic-fs-publication-gate-v1\0free";
const PUBLICATION_GATE_ACTIVE: &[u8] = b"acyclic-fs-publication-gate-v1\0active\0";

#[derive(Debug, thiserror::Error)]
#[error("{error}")]
struct ForkFailure<E = AuthorityStoreError> {
    error: E,
    work: WorkCounters,
}

impl<E> ForkFailure<E> {
    fn new(error: E, work: WorkCounters) -> Self { Self { error, work } }
    fn before_work(error: E) -> Self { Self::new(error, WorkCounters::default()) }
}

impl<E> From<OperationFailure<E>> for ForkFailure<E> {
    fn from(failure: OperationFailure<E>) -> Self { Self::new(failure.error, *failure.work) }
}

impl<E> From<ForkFailure<E>> for OperationFailure<E> {
    fn from(failure: ForkFailure<E>) -> Self { Self::new(failure.error, failure.work) }
}

impl From<ForkFailure> for OperationFailure<crate::FsError> {
    fn from(failure: ForkFailure) -> Self { Self::new(failure.error.into(), failure.work) }
}

/// Admission and actual completed-work custody for native fork request production.
struct ForkWork<'a> {
    work: WorkCounters,
    budget: WorkBudget,
    cancellation: &'a CancellationToken,
    allocations: crate::kernel::allocation::AllocationLedger,
    /// Admission-only upper witness for retained opaque provider payloads.
    opaque_live_upper_bytes: u64,
    #[cfg(not(target_arch = "wasm32"))]
    original: Option<&'a crate::NativeMaterializationWork>,
    #[cfg(not(target_arch = "wasm32"))]
    request_allocation: Option<crate::NativeAllocation>,
}

impl<'a> ForkWork<'a> {
    fn new(budget: WorkBudget, cancellation: &'a CancellationToken) -> Self {
        Self {
            work: WorkCounters::default(), budget, cancellation,
            allocations: Default::default(), opaque_live_upper_bytes: 0,
            #[cfg(not(target_arch = "wasm32"))]
            original: None,
            #[cfg(not(target_arch = "wasm32"))]
            request_allocation: None,
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native(
        original: &'a crate::NativeMaterializationWork, cancellation: &'a CancellationToken,
    ) -> Result<Self, AuthorityStoreError> {
        original.admit(WorkCounters::UNCHARGED)?;
        cancellation.check().map_err(|_| AuthorityStoreError::Cancelled)?;
        // The standalone fields are never consulted by original funding.
        Ok(Self {
            work: original.receipt(), budget: WorkBudget::default(), cancellation,
            allocations: Default::default(), opaque_live_upper_bytes: 0,
            original: Some(original), request_allocation: None,
        })
    }

    fn is_native(&self) -> bool {
        #[cfg(not(target_arch = "wasm32"))]
        { self.original.is_some() }
        #[cfg(target_arch = "wasm32")]
        { false }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native_work(&self) -> Option<&'a crate::NativeMaterializationWork> {
        self.original
    }

    fn spent(&self) -> WorkCounters {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = self.original { return original.receipt(); }
        self.work
    }

    fn record_completed(&mut self, delta: WorkCounters) -> Result<(), ForkFailure> {
        self.charge(delta)?;
        self.check()
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native_meter(&self) -> Arc<dyn acyclic_native_runtime::NativeReadMeter> {
        let original = self.original.expect("native SDK boundary requires original funding");
        let meter = Arc::clone(original.native_read_meter());
        meter
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native_io_failure(&self, error: std::io::Error) -> ForkFailure {
        let original = self.original.expect("native I/O relay requires original funding");
        ForkFailure::new(AuthorityStoreError::NativeOriginal(original.runtime_error(error)), original.receipt())
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn result<T>(
        &mut self, result: acyclic_stream::NativeStreamResult<T>,
    ) -> Result<acyclic_stream::NativeStreamOutput<T>, ForkFailure> {
        let original = self.original.expect("native SDK relay requires original funding");
        let result = result.map_err(|error| ForkFailure::new(
            AuthorityStoreError::NativeOriginal(original.runtime_error(error)), original.receipt(),
        ))?.map_err(AuthorityStoreError::from);
        self.work = original.receipt();
        result.map_err(|error| ForkFailure::new(error, original.receipt()))
    }

    fn check(&self) -> Result<(), ForkFailure> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = self.original {
            self.cancellation.check().map_err(|_| ForkFailure::new(AuthorityStoreError::Cancelled, original.receipt()))?;
            return original.admit(WorkCounters::UNCHARGED)
                .map_err(|error| ForkFailure::new(error.into(), original.receipt()));
        }
        self.cancellation.check()
            .map_err(|_| ForkFailure::new(AuthorityStoreError::Cancelled, self.work))?;
        self.work.verify(self.budget)
            .map_err(|error| ForkFailure::new(error.into(), self.work))?;
        let peak = self.allocations.live_bytes().checked_add(self.opaque_live_upper_bytes)
            .ok_or_else(|| ForkFailure::new(crate::WorkError::Overflow.into(), self.work))?;
        self.work.admit(&WorkCounters { peak_allocation_bytes: peak, ..WorkCounters::default() }, &self.budget)
            .map_err(|error| ForkFailure::new(error.into(), self.work))
    }

    fn admit(&self, delta: WorkCounters) -> Result<(), ForkFailure> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = self.original {
            self.check()?;
            return original.admit(delta)
                .map_err(|error| ForkFailure::new(error.into(), original.receipt()));
        }
        self.check()?;
        let mut admitted = delta;
        admitted.peak_allocation_bytes = delta.peak_allocation_bytes.max(self.allocations.live_bytes())
            .checked_add(self.opaque_live_upper_bytes)
            .ok_or_else(|| ForkFailure::new(crate::WorkError::Overflow.into(), self.work))?;
        self.work.admit(&admitted, &self.budget)
            .map_err(|error| ForkFailure::new(error.into(), self.work))
    }

    fn charge(&mut self, delta: WorkCounters) -> Result<(), ForkFailure> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = self.original {
            let result = original.record_admitted(delta);
            self.work = original.receipt();
            return result.map_err(|error| ForkFailure::new(error.into(), self.work));
        }
        self.work.try_add_assign(&delta)
            .map_err(|error| ForkFailure::new(error.into(), self.work))?;
        self.check()
    }

    fn release(&mut self, bytes: u64) -> Result<(), ForkFailure> {
        if self.is_native() { return Ok(()); }
        self.allocations.release(bytes)
            .map_err(|error| ForkFailure::new(
                error.into_work_or(AuthorityStoreError::RejectedStatic("fork allocation custody mismatch")), self.work))
    }

    fn release_request(&mut self) -> Result<(), ForkFailure> {
        #[cfg(not(target_arch = "wasm32"))]
        if self.is_native() {
            drop(self.request_allocation.take());
            self.work = self.spent();
            return Ok(());
        }
        self.release(self.allocations.live_bytes())
    }

    fn hold_opaque(&mut self, bytes: u64) -> Result<(), ForkFailure> {
        if self.is_native() { return self.check(); }
        let next = self.opaque_live_upper_bytes.checked_add(bytes)
            .ok_or_else(|| ForkFailure::new(crate::WorkError::Overflow.into(), self.work))?;
        let peak = self.allocations.live_bytes().checked_add(next)
            .ok_or_else(|| ForkFailure::new(crate::WorkError::Overflow.into(), self.work))?;
        self.work.admit(&WorkCounters { peak_allocation_bytes: peak, ..WorkCounters::default() }, &self.budget)
            .map_err(|error| ForkFailure::new(error.into(), self.work))?;
        self.opaque_live_upper_bytes = next;
        Ok(())
    }

    fn release_opaque(&mut self, bytes: u64) -> Result<(), ForkFailure> {
        if self.is_native() { return Ok(()); }
        self.opaque_live_upper_bytes = self.opaque_live_upper_bytes.checked_sub(bytes)
            .ok_or_else(|| ForkFailure::new(crate::WorkError::Overflow.into(), self.work))?;
        Ok(())
    }

    fn vec<T>(&mut self, capacity: usize) -> Result<Vec<T>, ForkFailure> {
        let (value, allocation) = self.vec_owned(capacity)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(mut allocation) = allocation {
            if let Some(request) = &mut self.request_allocation {
                if let Err(error) = request.absorb(&mut allocation) {
                    drop(value);
                    return Err(ForkFailure::new(error.into(), self.spent()));
                }
            } else {
                self.request_allocation = Some(allocation);
            }
        }
        #[cfg(target_arch = "wasm32")]
        let _ = allocation;
        Ok(value)
    }

    fn vec_owned<T>(&mut self, capacity: usize) -> Result<(Vec<T>, ForkAllocation), ForkFailure> {
        self.check()?;
        let bytes = capacity.checked_mul(std::mem::size_of::<T>())
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(|| ForkFailure::new(crate::WorkError::Overflow.into(), self.spent()))?;
        #[cfg(not(target_arch = "wasm32"))]
        let allocation = if let Some(original) = self.original {
            original.admit(WorkCounters::UNCHARGED)
                .map_err(|error| ForkFailure::new(error.into(), original.receipt()))?;
            self.cancellation.check().map_err(|_| ForkFailure::new(AuthorityStoreError::Cancelled, original.receipt()))?;
            Some(original.claim_allocation(bytes, u64::from(bytes != 0))
                .map_err(|error| ForkFailure::new(error.into(), original.receipt()))?)
        } else { None };
        #[cfg(target_arch = "wasm32")]
        let allocation: ForkAllocation = None;
        if !self.is_native() {
            if self.opaque_live_upper_bytes != 0 {
                let peak = self.allocations.live_bytes().checked_add(bytes)
                    .and_then(|live| live.checked_add(self.opaque_live_upper_bytes))
                    .ok_or_else(|| ForkFailure::new(crate::WorkError::Overflow.into(), self.work))?;
                self.work.admit(&WorkCounters { peak_allocation_bytes: peak, ..WorkCounters::default() }, &self.budget)
                    .map_err(|error| ForkFailure::new(error.into(), self.work))?;
            }
            self.allocations.claim_bytes(bytes, u64::from(bytes != 0), &mut self.work, self.budget)
                .map_err(|error| ForkFailure::new(error.into_work_or(AuthorityStoreError::RejectedStatic("fork allocation failed")), self.work))?;
        }
        let mut value = Vec::<T>::new();
        if value.try_reserve_exact(capacity).is_err() {
            drop(value);
            drop(allocation);
            self.release(bytes)?;
            return Err(ForkFailure::new(AuthorityStoreError::RejectedStatic("fork allocation failed"), self.spent()));
        }
        let actual = value.capacity().checked_mul(std::mem::size_of::<T>())
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(|| ForkFailure::new(crate::WorkError::Overflow.into(), self.spent()))?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = self.original {
            let mut allocation = allocation.expect("native vector custody");
            let verified = allocation.reconcile_capacity(actual).map_err(AuthorityStoreError::from)
                .and_then(|()| self.cancellation.check().map_err(|_| AuthorityStoreError::Cancelled));
            if let Err(error) = verified {
                drop(value);
                drop(allocation);
                return Err(ForkFailure::new(error, original.receipt()));
            }
            self.work = original.receipt();
            return Ok((value, Some(allocation)));
        }
        let excess = actual.checked_sub(bytes)
            .ok_or_else(|| ForkFailure::new(crate::WorkError::Overflow.into(), self.work))?;
        self.allocations.record_owned_bytes(excess, &mut self.work)
            .map_err(|error| ForkFailure::new(error.into_work_or(AuthorityStoreError::RejectedStatic("fork allocation failed")), self.work))?;
        let verified = self.work.verify(self.budget).and_then(|()| {
            if self.opaque_live_upper_bytes == 0 { return Ok(()); }
            let peak = self.allocations.live_bytes().checked_add(self.opaque_live_upper_bytes).ok_or(crate::WorkError::Overflow)?;
            self.work.admit(&WorkCounters { peak_allocation_bytes: peak, ..WorkCounters::default() }, &self.budget)
        });
        if let Err(error) = verified {
            drop(value);
            self.release(actual)?;
            return Err(ForkFailure::new(error.into(), self.work));
        }
        Ok((value, None))
    }

    fn bytes(&mut self, domain: &[u8], fields: &[&[u8]])
        -> Result<Bytes, ForkFailure> {
        let buffer = self.byte_buffer(domain, fields)?;
        #[cfg(not(target_arch = "wasm32"))]
        if self.is_native() {
            let original = self.original.expect("native allocation has its original owner");
            // Filesystem payloads keep their existing intrinsic Bytes custody.
            let control = original.claim(std::mem::size_of::<ForkBytes>() as u64)
                .map_err(|error| ForkFailure::new(error.into(), original.receipt()))?;
            let value = Bytes::from_owner(ForkBytes {
                bytes: buffer.0, _allocation: buffer.1.expect("native buffer has concrete custody"), _control: control,
            });
            self.work = original.receipt();
            return Ok(value);
        }
        // Audited bytes 1.12 allocates a private sharing owner only for spare capacity.
        let owner = WorkCounters { allocation_operations: u64::from(buffer.0.capacity() != buffer.0.len()), ..WorkCounters::default() };
        self.admit(owner)?;
        let value = Bytes::from(buffer.0);
        self.charge(owner)?;
        Ok(value)
    }

    /// Request records retain the canonical SDK byte carrier and original guard.
    fn stream_bytes(&mut self, domain: &[u8], fields: &[&[u8]])
        -> Result<acyclic_stream::OwnedBytes, ForkFailure> {
        #[cfg(not(target_arch = "wasm32"))]
        if self.is_native() {
            let mut buffer = self.byte_buffer(domain, fields)?;
            let result = acyclic_stream::OwnedBytes::from_vec_with_guard_in_place(
                &mut buffer.0, &mut buffer.1, self.native_meter(),
            );
            self.work = self.spent();
            return result.map_err(|error| self.native_io_failure(error));
        }
        self.bytes(domain, fields).map(Into::into)
    }

    fn byte_buffer(&mut self, domain: &[u8], fields: &[&[u8]])
        -> Result<(Vec<u8>, ForkAllocation), ForkFailure> {
        let length = fields.iter().try_fold(domain.len(), |total, field| total.checked_add(field.len()))
            .ok_or_else(|| ForkFailure::new(crate::WorkError::Overflow.into(), self.spent()))?;
        // Framed records encode; an empty domain copies a raw payload.
        let writes = WorkCounters {
            bytes_encoded: if domain.is_empty() { 0 } else { length as u64 },
            bytes_copied: length as u64, ..WorkCounters::default()
        };
        self.admit(writes)?;
        // Physical storage always retires before its original claim on failure.
        let mut buffer = self.vec_owned::<u8>(length)?;
        buffer.0.extend_from_slice(domain);
        for field in fields { buffer.0.extend_from_slice(field); }
        self.charge(writes)?;
        Ok(buffer)
    }

    fn path(&mut self, authority: AuthorityId, suffix: &str, generation: Option<GenerationId>)
        -> Result<acyclic_stream::StreamPath, ForkFailure> {
        const PREFIX: &[u8] = b"fs/authorities/";
        let length = PREFIX.len().checked_add(32).and_then(|n| n.checked_add(suffix.len()))
            .and_then(|n| n.checked_add(if generation.is_some() { 64 } else { 0 }))
            .filter(|n| *n <= acyclic_stream::MAX_PATH_BYTES)
            .ok_or_else(|| ForkFailure::new(AuthorityStoreError::RejectedStatic("fork path is too long"), self.spent()))?;
        let writes = WorkCounters { bytes_encoded: length as u64, bytes_copied: (PREFIX.len() + suffix.len()) as u64, ..WorkCounters::default() };
        if self.is_native() {
            self.admit(writes)?;
        } else {
            self.admit(WorkCounters {
                bytes_encoded: writes.bytes_encoded,
                bytes_copied: writes.bytes_copied + length as u64,
                allocation_operations: 1,
                peak_allocation_bytes: self.allocations.live_bytes().checked_add(length as u64)
                    .ok_or_else(|| ForkFailure::new(crate::WorkError::Overflow.into(), self.work))?,
                ..WorkCounters::default()
            })?;
        }
        let mut text = [0_u8; acyclic_stream::MAX_PATH_BYTES];
        text[..PREFIX.len()].copy_from_slice(PREFIX);
        let cursor = PREFIX.len() + 32;
        hex::encode_to_slice(authority.into_bytes(), &mut text[PREFIX.len()..cursor]).expect("authority hex width");
        text[cursor..cursor + suffix.len()].copy_from_slice(suffix.as_bytes());
        if let Some(generation) = generation {
            hex::encode_to_slice(generation.digest().as_bytes(), &mut text[cursor + suffix.len()..length])
                .expect("generation hex width");
        }
        self.charge(writes)?;
        let text = std::str::from_utf8(&text[..length]).map_err(|_| ForkFailure::new(
            AuthorityStoreError::RejectedStatic("fork path is not UTF-8"), self.spent()))?;
        self.own_path(text)
    }

    fn own_path(&mut self, text: &str) -> Result<acyclic_stream::StreamPath, ForkFailure> {
        let length = text.len();
        #[cfg(not(target_arch = "wasm32"))]
        if self.is_native() {
            let result = acyclic_stream::StreamPath::new_native(text, &self.native_meter());
            self.work = self.spent();
            return result.map_err(|error| self.native_io_failure(error))?
                .map_err(|error| ForkFailure::new(AuthorityStoreError::NativeStream(error), self.spent()));
        }
        self.admit(WorkCounters {
            allocation_operations: 1,
            peak_allocation_bytes: self.allocations.live_bytes().checked_add(length as u64)
                .ok_or_else(|| ForkFailure::new(crate::WorkError::Overflow.into(), self.work))?,
            bytes_copied: length as u64, ..WorkCounters::default()
        })?;
        let path = acyclic_stream::StreamPath::new(text)
            .map_err(|error| ForkFailure::new(map_stream_error(error), self.work))?;
        self.allocations.claim_bytes(length as u64, 1, &mut self.work, WorkBudget::UNBOUNDED)
            .map_err(|error| ForkFailure::new(
                error.into_work_or(AuthorityStoreError::Rejected("fork allocation failed".to_owned())), self.work))?;
        self.charge(WorkCounters { bytes_copied: length as u64, ..WorkCounters::default() })?;
        Ok(path)
    }

    fn lease_path(&mut self, workspace: crate::WorkspaceId, lease: crate::OperationLeaseId, expiry: u64)
        -> Result<acyclic_stream::StreamPath, ForkFailure> {
        let mut text = [0_u8; acyclic_stream::MAX_PATH_BYTES];
        let length = crate::operation_window::stream_lease_path_encode(
            workspace, lease, expiry, &mut text, Some(&mut |completed, encoded, copied| {
                let work = WorkCounters { bytes_encoded: encoded, bytes_copied: copied, ..WorkCounters::UNCHARGED };
                if completed { self.charge(work) } else { self.admit(work) }
            }),
        )?;
        let text = std::str::from_utf8(&text[..length])
            .map_err(|_| authority_corrupt("lease path is not UTF-8", self))?;
        self.own_path(text)
    }

    fn clone_path(&self, path: &acyclic_stream::StreamPath)
        -> Result<acyclic_stream::StreamPath, ForkFailure> {
        // StreamPath owns Arc<str>; cloning shares the already admitted text.
        self.check()?;
        Ok(path.clone())
    }

    fn stream_key(&mut self, domain: &[u8], identity: &[u8])
        -> Result<acyclic_stream::IdempotencyKey, ForkFailure> {
        let bytes = self.stream_bytes(domain, &[&[0], identity])?;
        acyclic_stream::IdempotencyKey::new(bytes)
            .map_err(|error| ForkFailure::new(
                if self.is_native() { AuthorityStoreError::NativeStream(error) } else { map_stream_error(error) },
                self.spent()))
    }

    fn operation_key(&mut self, authority: AuthorityId, operation: OperationId)
        -> Result<acyclic_stream::IdempotencyKey, ForkFailure> {
        let delta = WorkCounters { bytes_copied: 32, ..WorkCounters::default() };
        self.admit(delta)?;
        let mut identity = [0_u8; 32];
        identity[..16].copy_from_slice(&authority.into_bytes());
        identity[16..].copy_from_slice(&operation.into_bytes());
        self.charge(delta)?;
        self.stream_key(b"operation", &identity)
    }

    fn request_write_work(&self, request: &acyclic_stream::CommitRequest)
        -> Result<WorkCounters, ForkFailure> {
        let mut delta = WorkCounters { backend_write_operations: 1, durability_operations: 1, ..WorkCounters::default() };
        if self.is_native() {
            delta.backend_write_operations = 0;
            delta.durability_operations = 0;
        }
        for mutation in &request.mutations {
            let records = match mutation {
                acyclic_stream::CommitMutation::Append { records, .. }
                | acyclic_stream::CommitMutation::Fork { records, .. } => records,
            };
            delta.authority_records_appended = delta.authority_records_appended.checked_add(records.len() as u64)
                .ok_or_else(|| ForkFailure::new(crate::WorkError::Overflow.into(), self.work))?;
            for record in records {
                delta.authority_bytes_written = delta.authority_bytes_written.checked_add(record.len() as u64)
                    .ok_or_else(|| ForkFailure::new(crate::WorkError::Overflow.into(), self.work))?;
            }
        }
        Ok(delta)
    }

    fn remaining(&self) -> Result<WorkBudget, ForkFailure> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = self.original {
            return original.remaining().map_err(|error| ForkFailure::new(error.into(), original.receipt()));
        }
        self.check()?;
        let mut budget = self.work.remaining(self.budget)
            .map_err(|error| ForkFailure::new(error.into(), self.work))?;
        budget.peak_allocation_bytes = self.budget.peak_allocation_bytes
            .checked_sub(self.allocations.live_bytes()).and_then(|bytes| bytes.checked_sub(self.opaque_live_upper_bytes))
            .ok_or_else(|| ForkFailure::new(crate::WorkError::Overflow.into(), self.work))?;
        Ok(budget)
    }

    fn merge_child(&mut self, mut child: WorkCounters) -> Result<(), ForkFailure> {
        #[cfg(not(target_arch = "wasm32"))]
        if self.is_native() {
            return Err(ForkFailure::new(AuthorityStoreError::NativeOriginalUnsupported, self.spent()));
        }
        child.peak_allocation_bytes = self.allocations.live_bytes().checked_add(child.peak_allocation_bytes)
            .ok_or_else(|| ForkFailure::new(crate::WorkError::Overflow.into(), self.work))?;
        self.charge(child)
    }

    fn push<T>(&mut self, values: &mut Vec<T>, value: T)
        -> Result<(), ForkFailure> {
        self.check()?;
        if values.len() == values.capacity() {
            return Err(ForkFailure::new(AuthorityStoreError::RejectedStatic(
                "fork request exceeded its admitted vector capacity"), self.spent()));
        }
        let delta = WorkCounters { bytes_copied: std::mem::size_of::<T>() as u64, ..WorkCounters::default() };
        self.admit(delta)?;
        values.push(value);
        self.charge(delta)
    }

    fn decode_epoch(&mut self, bytes: &[u8], domain: &[u8])
        -> Result<Epoch, ForkFailure> {
        if !self.is_native() {
            return decode_epoch(bytes, domain, false).map_err(|error| ForkFailure::new(error, self.work));
        }
        let copies = if bytes.len() == domain.len().saturating_add(8) && bytes.starts_with(domain) { 8 } else { 0 };
        let delta = WorkCounters { bytes_copied: copies, ..WorkCounters::default() };
        self.admit(delta)?;
        let result = decode_epoch(bytes, domain, true);
        self.charge(delta)?;
        result.map_err(|error| ForkFailure::new(error, self.spent()))
    }

    fn observe_record(&mut self, step: crate::kernel::ReadAllocationStep, invalid: &'static str)
        -> Result<(), ForkFailure> {
        match step {
                crate::kernel::ReadAllocationStep::BeforeCopy(count) =>
                    self.admit(WorkCounters { bytes_copied: count, ..WorkCounters::UNCHARGED }),
                crate::kernel::ReadAllocationStep::Copied(count) =>
                    self.record_completed(WorkCounters { bytes_copied: count, ..WorkCounters::UNCHARGED }),
                crate::kernel::ReadAllocationStep::BeforeHash(count) =>
                    self.check().and_then(|()| self.admit(WorkCounters { bytes_hashed: count, ..WorkCounters::UNCHARGED })),
                crate::kernel::ReadAllocationStep::Hashed(count) =>
                    self.record_completed(WorkCounters { bytes_hashed: count, ..WorkCounters::UNCHARGED })
                        .and_then(|()| self.check()),
                _ => Err(authority_corrupt(invalid, self)),
            }
    }

    fn decode_durable<'record>(&mut self, bytes: &'record [u8])
        -> Result<crate::streams_record::BorrowedStreamsDurableRecord<'record>, ForkFailure> {
        let result = StreamsDurableRecord::decode_borrowed_with(bytes, STREAM_RECORD_LIMIT,
            |step| self.observe_record(step, "durable envelope emitted an allocating decoder step"))?;
        result.map_err(|error| ForkFailure::new(
            if self.is_native() { AuthorityStoreError::DurableCodec(error) } else { AuthorityStoreError::Corrupt(error.to_string()) },
            self.spent()))
    }

    fn durable_digest(&mut self, authority: AuthorityId,
        record: &crate::streams_record::BorrowedStreamsDurableRecord<'_>,
    ) -> Result<Digest, ForkFailure> {
        let hashing = WorkCounters {
            bytes_hashed: crate::foundation::AUTHORITY_COMMIT_DIGEST_ENVELOPE_BYTES
                .checked_add(record.payload.len() as u64)
                .ok_or_else(|| ForkFailure::new(crate::WorkError::Overflow.into(), self.spent()))?,
            ..WorkCounters::UNCHARGED
        };
        self.admit(hashing)?;
        let digest = authority_commit_digest(authority, record.epoch, record.sequence,
            record.operation_id, record.fingerprint, record.previous_digest, record.payload);
        self.charge(hashing)?;
        Ok(digest)
    }

    fn decode_fixed<const N: usize>(
        &mut self, value: &[u8], domain: &[u8], invalid: &'static str,
    ) -> Result<[u8; N], ForkFailure> {
        let bytes = record_bytes::<N>(value, domain).ok_or_else(|| authority_corrupt(invalid, self))?;
        let copied = WorkCounters { bytes_copied: N as u64, ..WorkCounters::UNCHARGED };
        self.admit(copied)?;
        let bytes = *bytes;
        self.charge(copied)?;
        Ok(bytes)
    }

    fn decode_gate(&mut self, value: &[u8]) -> Result<Option<OperationId>, ForkFailure> {
        if value == PUBLICATION_GATE_FREE { return Ok(None); }
        self.decode_fixed::<16>(value, PUBLICATION_GATE_ACTIVE, "invalid publication gate record")
            .map(|bytes| Some(OperationId::from_bytes(bytes)))
    }

    fn finish<T>(&self, value: T) -> Result<AuthorityReceipt<T>, ForkFailure> {
        self.check()?;
        Ok(AuthorityReceipt { value, work: self.spent() })
    }
}

#[cfg(not(target_arch = "wasm32"))]
type ForkAllocation = Option<crate::NativeAllocation>;
#[cfg(target_arch = "wasm32")]
type ForkAllocation = Option<()>;

#[cfg(not(target_arch = "wasm32"))]
struct ForkBytes {
    bytes: Vec<u8>,
    _allocation: crate::NativeAllocation,
    _control: crate::NativeAllocation,
}

#[cfg(not(target_arch = "wasm32"))]
impl AsRef<[u8]> for ForkBytes {
    fn as_ref(&self) -> &[u8] { &self.bytes }
}

fn fork_fs_failure<E: From<crate::FsError>>(failure: impl Into<ForkFailure>) -> ForkFailure<E> {
    let failure = failure.into();
    ForkFailure::new(crate::FsError::from(failure.error).into(), failure.work)
}

fn project_fork_authority_failure(failure: impl Into<ForkFailure<crate::FsError>>) -> ForkFailure {
    let failure = failure.into();
    let error = match failure.error {
        crate::FsError::Authority(error) => error,
        #[cfg(not(target_arch = "wasm32"))]
        crate::FsError::NativeKernel(error) => AuthorityStoreError::NativeOriginal(error),
        crate::FsError::Work(error) => error.into(),
        crate::FsError::Cancelled(_) => AuthorityStoreError::Cancelled,
        crate::FsError::WorkspaceDeleted => AuthorityStoreError::Retired,
        error => AuthorityStoreError::Corrupt(error.to_string()),
    };
    ForkFailure::new(error, failure.work)
}

trait ForkPublicationGuard: ProviderTask {
    type Error: From<crate::FsError> + Send;
    fn seal(&mut self, meter: &mut ForkWork<'_>)
        -> impl Future<Output = Result<(), ForkFailure<Self::Error>>> + ProviderTask;
    fn steps<S: AsyncAuthorityStore + ?Sized>(
        &mut self, store: &S, fork: WorkspaceForkCommit, meter: &mut ForkWork<'_>,
    ) -> impl Future<Output = Result<crate::FsReceipt<WorkspaceForkOutcome>, ForkFailure<Self::Error>>> + acyclic_stream::ProviderTask;
    fn commit<P: acyclic_stream::StreamProvider>(
        &mut self, store: &StreamAuthorityStore<P>, request: acyclic_stream::CommitRequest,
        meter: &mut ForkWork<'_>,
    ) -> impl Future<Output = Result<AuthorityProviderOutput<acyclic_stream::CommitOutcome>, AppendFailure<Self::Error>>> + ProviderTask {
        async move {
            self.seal(meter).await.map_err(|failure| AppendFailure::Owner(failure.error))?;
            store.commit(request, None, meter,
                &mut None::<&mut fn() -> std::future::Ready<Result<(), Self::Error>>>).await
        }
    }
}

impl<F, Fut> ForkPublicationGuard for Option<&mut F>
where F: FnMut(WorkBudget) -> Fut + ProviderTask, Fut: Future<Output = crate::FsResult<()>> + ProviderTask {
    type Error = crate::FsError;
    async fn seal(&mut self, meter: &mut ForkWork<'_>) -> Result<(), ForkFailure<crate::FsError>> {
        meter.check().map_err(fork_fs_failure)?;
        if let Some(guard) = self.as_deref_mut() {
            let result = guard(meter.remaining().map_err(fork_fs_failure)?).await;
            match result {
                Ok(receipt) => meter.merge_child(receipt.work).map_err(fork_fs_failure)?,
                Err(failure) => {
                    let failure = ForkFailure::from(failure);
                    meter.merge_child(failure.work).map_err(fork_fs_failure)?;
                    return Err(ForkFailure::new(failure.error, meter.spent()));
                }
            }
        }
        meter.check().map_err(fork_fs_failure)
    }

    async fn steps<S: AsyncAuthorityStore + ?Sized>(
        &mut self, store: &S, fork: WorkspaceForkCommit, meter: &mut ForkWork<'_>,
    ) -> Result<crate::FsReceipt<WorkspaceForkOutcome>, ForkFailure<crate::FsError>> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = meter.native_work() {
            let value = crate::async_storage::commit_workspace_fork_in_steps_with_native_work(
                store, fork, original, meter.cancellation,
            ).await.map_err(|error| ForkFailure::new(error.into(), meter.spent()))?;
            return Ok(crate::FsReceipt { value, work: meter.spent() });
        }
        let child_budget = meter.remaining().map_err(fork_fs_failure)?;
        let result = match self.as_deref_mut() {
            Some(guard) => crate::async_storage::commit_workspace_fork_in_steps_guarded(
                store, fork, PublicationPermit::Unrestricted, child_budget, meter.cancellation, guard,
            ).await.map_err(ForkFailure::from),
            None => crate::async_storage::commit_workspace_fork_in_steps(
                store, fork, child_budget, meter.cancellation,
            ).await.map(|receipt| crate::FsReceipt { value: receipt.value, work: receipt.work }).map_err(fork_fs_failure),
        };
        match result {
            Ok(receipt) => {
                meter.merge_child(receipt.work).map_err(fork_fs_failure)?;
                Ok(crate::FsReceipt { value: receipt.value, work: meter.spent() })
            }
            Err(failure) => {
                meter.merge_child(failure.work).map_err(fork_fs_failure)?;
                Err(ForkFailure::new(failure.error, meter.spent()))
            }
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl<E, F, Fut> ForkPublicationGuard for OwnedAuthorityPublicationGuard<F>
where E: From<crate::FsError> + Send + 'static,
    F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static {
    type Error = E;
    async fn seal(&mut self, meter: &mut ForkWork<'_>) -> Result<(), ForkFailure<E>> {
        meter.check().map_err(fork_fs_failure)?;
        (self.0)(meter.native_work().expect("native fork funding")).await
            .map_err(|error| ForkFailure::new(error, meter.spent()))?;
        meter.check().map_err(fork_fs_failure)
    }

    async fn steps<S: AsyncAuthorityStore + ?Sized>(
        &mut self, store: &S, fork: WorkspaceForkCommit, meter: &mut ForkWork<'_>,
    ) -> Result<crate::FsReceipt<WorkspaceForkOutcome>, ForkFailure<E>> {
        // Ordered recovery retains the exact same original ledger and owner.
        let value = crate::async_storage::commit_workspace_fork_in_steps_guarded_with_native_work(
            store, fork, PublicationPermit::Unrestricted, meter.native_work().expect("native fork funding"), meter.cancellation, &mut self.0,
        ).await.map_err(|error| ForkFailure::new(error, meter.spent()))?;
        Ok(crate::FsReceipt { value, work: meter.spent() })
    }

    async fn commit<P: acyclic_stream::StreamProvider>(
        &mut self, store: &StreamAuthorityStore<P>, request: acyclic_stream::CommitRequest,
        meter: &mut ForkWork<'_>,
    ) -> Result<AuthorityProviderOutput<acyclic_stream::CommitOutcome>, AppendFailure<E>> {
        let original = meter.native_work().expect("native fork funding").clone();
        let mut callback = self.0.clone();
        let mut owner = OwnedAuthorityPublicationGuard(move || callback(&original));
        store.commit(request, None, meter, &mut owner).await
    }
}

/// Provider failures do not expose whether a lost acknowledgement made records durable.
fn commit_work(
    submitted: WorkCounters,
    result: &Result<acyclic_stream::CommitOutcome, acyclic_stream::StreamError>,
) -> WorkCounters {
    if matches!(result, Ok(acyclic_stream::CommitOutcome::Committed(_))) {
        submitted
    } else {
        WorkCounters { backend_write_operations: 1, ..WorkCounters::default() }
    }
}

/// Filesystem authority over native hierarchical Streams.
#[derive(Clone)]
pub struct StreamAuthorityStore<P> {
    provider: Arc<P>,
}

impl<P> StreamAuthorityStore<P> {
    /// Binds one authenticated public Stream provider.
    #[must_use]
    pub fn new(provider: Arc<P>) -> Self {
        Self { provider }
    }

    /// Returns the shared public Stream provider.
    #[must_use]
    pub fn provider(&self) -> Arc<P> { Arc::clone(&self.provider) }
}

#[derive(Clone, Copy)]
struct AuthoritySnapshot {
    // Validation binds the Stream record tail to head.sequence + 1.
    head: Head,
    epoch_tail: u64,
}

// Private read-only capability: retirement bypass is available only to accounting.
struct AccountingAuthority<'a, P>(&'a StreamAuthorityStore<P>);

impl<P: acyclic_stream::StreamProvider> AsyncAuthorityStore for AccountingAuthority<'_, P> {
    async fn fork_generation_authority(
        &self, _source: crate::GenerationForkSource, _destination: AuthorityId,
        _operation: OperationId, _budget: WorkBudget, _cancellation: &CancellationToken,
    ) -> AuthorityResult<CreateAuthorityOutcome> {
        Err(OperationFailure::before_work(AuthorityStoreError::Retired))
    }

    async fn create_authority(
        &self, _authority: AuthorityId, _epoch: Epoch, _budget: WorkBudget,
        _cancellation: &CancellationToken,
    ) -> AuthorityResult<CreateAuthorityOutcome> {
        Err(OperationFailure::before_work(AuthorityStoreError::Retired))
    }

    async fn head(
        &self, authority: AuthorityId, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> AuthorityResult<Head> {
        cancellation.check().map_err(|_| OperationFailure::before_work(AuthorityStoreError::Cancelled))?;
        let snapshot = self.0.historical_snapshot(authority).await
            .map_err(OperationFailure::before_work)?;
        authority_success(snapshot.head, authority_read_work(2), budget).map_err(Into::into)
    }

    async fn compare_and_append(
        &self, _authority: AuthorityId, _epoch: Epoch, _expected: Head,
        _commit: ProposedCommit, _budget: WorkBudget, _cancellation: &CancellationToken,
    ) -> AuthorityResult<FsAppendOutcome> {
        Err(OperationFailure::before_work(AuthorityStoreError::Retired))
    }

    async fn replay(
        &self, authority: AuthorityId, after: Sequence, limit: ReplayLimit,
        budget: WorkBudget, cancellation: &CancellationToken,
    ) -> AuthorityResult<Vec<DurableCommit>> {
        let mut meter = ForkWork::new(budget, cancellation);
        let (value, _) = self.0.authority_replay(authority, after, limit, &mut meter, ReadKind::Accounting).await?;
        authority_success(value, meter.work, budget).map_err(Into::into)
    }

    async fn fence(
        &self, _authority: AuthorityId, _expected: Head, _budget: WorkBudget,
        _cancellation: &CancellationToken,
    ) -> AuthorityResult<FenceOutcome> {
        Err(OperationFailure::before_work(AuthorityStoreError::Retired))
    }

    async fn find_operation(
        &self, _authority: AuthorityId, _operation: OperationId, _budget: WorkBudget,
        _cancellation: &CancellationToken,
    ) -> AuthorityResult<Option<DurableCommit>> {
        Err(OperationFailure::before_work(AuthorityStoreError::Retired))
    }
}

impl<P: acyclic_stream::StreamProvider> StreamAuthorityStore<P> {
    /// Reads a bounded historical accounting page, including retired authorities.
    pub async fn accounting_page<O: crate::AsyncObjectStore>(
        &self,
        objects: &O,
        authority_id: AuthorityId,
        cursor: crate::accounting::AccountingCursor,
        maximum_records: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<crate::accounting::AccountingPage, crate::accounting::AccountingError> {
        let historical = AccountingAuthority(self);
        let head = historical.head(authority_id, budget, cancellation).await
            .map_err(|failure| crate::accounting::AccountingError::Authority(failure.error.to_string()))?;
        let remaining = head.work.remaining(budget)
            .map_err(crate::accounting::AccountingError::Work)?;
        let mut page = crate::accounting::accounting_page_at_head(
            &historical, objects, authority_id, cursor, maximum_records,
            head.value, remaining, cancellation,
        ).await?;
        page.work = head.work.checked_add(page.work)
            .map_err(crate::accounting::AccountingError::Work)?;
        Ok(page)
    }

}

enum AppendFailure<E> {
    Authority(ForkFailure),
    Owner(E),
}

impl<E> From<ForkFailure> for AppendFailure<E> {
    fn from(value: ForkFailure) -> Self { Self::Authority(value) }
}

#[cfg(not(target_arch = "wasm32"))]
impl<E> From<acyclic_stream::NativeStreamFailure> for AppendFailure<E> {
    fn from(error: acyclic_stream::NativeStreamFailure) -> Self {
        Self::Authority(ForkFailure::before_work(error.into()))
    }
}

trait AuthorityPublicationGuard: Send {
    type Error: Send;
    fn seal(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;
    #[cfg(not(target_arch = "wasm32"))]
    fn native_commit<P: acyclic_stream::StreamProvider>(
        &mut self, store: &StreamAuthorityStore<P>, request: acyclic_stream::CommitRequest,
        deadline: Option<u64>, meter: &mut ForkWork<'_>,
    ) -> impl Future<Output = Result<AuthorityProviderOutput<acyclic_stream::CommitOutcome>, AppendFailure<Self::Error>>> + Send;
}

impl<E, F, Fut> AuthorityPublicationGuard for Option<&mut F>
where E: Send, F: FnMut() -> Fut + Send, Fut: Future<Output = Result<(), E>> + Send {
    type Error = E;
    async fn seal(&mut self) -> Result<(), E> {
        if let Some(guard) = self { guard().await?; }
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn native_commit<P: acyclic_stream::StreamProvider>(
        &mut self, store: &StreamAuthorityStore<P>, request: acyclic_stream::CommitRequest,
        deadline: Option<u64>, meter: &mut ForkWork<'_>,
    ) -> Result<AuthorityProviderOutput<acyclic_stream::CommitOutcome>, AppendFailure<E>> {
        // Borrowed ordinary callbacks cannot authorize accepted native work.
        if self.is_some() {
            return Err(ForkFailure::new(AuthorityStoreError::NativeOriginalUnsupported, meter.spent()).into());
        }
        let result = match deadline {
            Some(deadline) => store.provider.native_commit_before(request, deadline, meter.native_meter()).await,
            None => store.provider.native_commit(request, meter.native_meter()).await,
        };
        meter.result(result).map(AuthorityProviderOutput::Native).map_err(Into::into)
    }
}

#[cfg(not(target_arch = "wasm32"))]
struct OwnedAuthorityPublicationGuard<F>(F);

#[cfg(not(target_arch = "wasm32"))]
impl<E, F, Fut> AuthorityPublicationGuard for OwnedAuthorityPublicationGuard<F>
where E: Send + 'static, F: FnMut() -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static {
    type Error = E;
    async fn seal(&mut self) -> Result<(), E> { (self.0)().await }

    async fn native_commit<P: acyclic_stream::StreamProvider>(
        &mut self, store: &StreamAuthorityStore<P>, request: acyclic_stream::CommitRequest,
        deadline: Option<u64>, meter: &mut ForkWork<'_>,
    ) -> Result<AuthorityProviderOutput<acyclic_stream::CommitOutcome>, AppendFailure<E>> {
        let mut callback = self.0.clone();
        let before = move || {
            let future = callback();
            async move { future.await.map_err(AppendFailure::Owner) }
        };
        let result = store.provider.native_commit_guarded(
            request, deadline, meter.native_meter(), before,
        ).await;
        match result {
            Err(error) => meter.result(Err(error)).map(AuthorityProviderOutput::Native).map_err(Into::into),
            Ok(Err(AppendFailure::Authority(mut failure))) => {
                meter.work = meter.spent();
                failure.work = meter.work;
                Err(AppendFailure::Authority(failure))
            }
            Ok(Err(AppendFailure::Owner(error))) => Err(AppendFailure::Owner(error)),
            Ok(Ok(outcome)) => Ok(AuthorityProviderOutput::Native(outcome)),
        }
    }
}

fn authority_corrupt(message: &'static str, meter: &ForkWork<'_>) -> ForkFailure {
    let error = if meter.is_native() {
        AuthorityStoreError::CorruptStatic(message)
    } else {
        AuthorityStoreError::Corrupt(message.to_owned())
    };
    ForkFailure::new(error, meter.spent())
}

fn authority_stream_error(error: &AuthorityStoreError) -> Option<&acyclic_stream::StreamError> {
    match error {
        AuthorityStoreError::NativeStream(error) => Some(error),
        #[cfg(not(target_arch = "wasm32"))]
        AuthorityStoreError::NativeStreamFailure(error) => Some(error.source()),
        _ => None,
    }
}

fn authority_missing(error: &AuthorityStoreError) -> bool {
    matches!(error, AuthorityStoreError::Missing)
        || matches!(authority_stream_error(error), Some(acyclic_stream::StreamError::NotFound))
}

fn authority_decode_borrowed<'a>(
    authority: AuthorityId, encoded: &'a [u8], meter: &mut ForkWork<'_>,
) -> Result<crate::streams_record::BorrowedStreamsDurableRecord<'a>, ForkFailure> {
    let borrowed = meter.decode_durable(encoded)?;
    let digest = meter.durable_digest(authority, &borrowed)?;
    if digest != borrowed.digest {
        return Err(authority_corrupt("Stream authority record digest mismatch", meter));
    }
    Ok(borrowed)
}

fn authority_copy_durable(
    borrowed: crate::streams_record::BorrowedStreamsDurableRecord<'_>,
    committed_at_micros: u64, meter: &mut ForkWork<'_>,
) -> Result<DurableCommit, ForkFailure> {
    let payload = meter.bytes(&[], &[borrowed.payload])?;
    Ok(DurableCommit {
        epoch: borrowed.epoch, sequence: borrowed.sequence, operation_id: borrowed.operation_id,
        fingerprint: borrowed.fingerprint, previous_digest: borrowed.previous_digest,
        digest: borrowed.digest, payload,
        settled_at_micros: (committed_at_micros != 0).then_some(committed_at_micros),
    })
}

enum AuthorityProviderOutput<T> {
    Plain(T),
    #[cfg(not(target_arch = "wasm32"))]
    Native(acyclic_stream::NativeStreamOutput<T>),
}

impl<T> std::ops::Deref for AuthorityProviderOutput<T> {
    type Target = T;
    fn deref(&self) -> &T {
        match self {
            Self::Plain(value) => value,
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native(value) => value.value(),
        }
    }
}

enum AuthorityRead {
    Plain(acyclic_stream::RecordStream),
    #[cfg(not(target_arch = "wasm32"))]
    Native(acyclic_stream::NativeStreamOutput<acyclic_stream::NativeRecordStream>),
}

#[derive(Clone, Copy, PartialEq)]
enum ReadKind { Exact, Replay, Accounting }

impl AuthorityRead {
    async fn next(&mut self, meter: &mut ForkWork<'_>, kind: ReadKind)
        -> Result<Option<AuthorityProviderOutput<acyclic_stream::Record>>, ForkFailure> {
        if kind == ReadKind::Replay { meter.check()?; }
        if kind == ReadKind::Accounting {
            meter.cancellation.check().map_err(|_| ForkFailure::new(AuthorityStoreError::Cancelled, meter.work))?;
        }
        let value = match self {
            Self::Plain(stream) => stream.next().await
                .map(|record| record.map(AuthorityProviderOutput::Plain)
                    .map_err(|error| ForkFailure::new(map_stream_error(error), meter.spent())))
                .transpose()?,
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native(stream) => stream.value_mut().next().await
                .map(|record| meter.result(record).map(AuthorityProviderOutput::Native))
                .transpose()?,
        };
        if kind == ReadKind::Replay { meter.check()?; }
        if let Some(record) = &value {
            let delta = WorkCounters {
                authority_records_read: 1, authority_bytes_read: record.value.len() as u64,
                ..WorkCounters::default()
            };
            if kind != ReadKind::Replay && !meter.is_native() {
                meter.work = meter.work.checked_add(delta)
                    .map_err(|error| ForkFailure::new(error.into(), meter.work))?;
                meter.cancellation.check()
                    .map_err(|_| ForkFailure::new(AuthorityStoreError::Cancelled, meter.work))?;
                if kind == ReadKind::Exact { admit_authority(meter.work, meter.budget)?; }
            } else {
                meter.admit(delta)?;
                meter.charge(delta)?;
            }
        }
        Ok(value)
    }
}

impl<P: acyclic_stream::StreamProvider> StreamAuthorityStore<P> {
    async fn authority_read(
        &self, request: acyclic_stream::ReadRequest, meter: &mut ForkWork<'_>, kind: ReadKind,
    ) -> Result<AuthorityRead, ForkFailure> {
        if kind == ReadKind::Replay || meter.is_native() { meter.check()?; }
        let admission = WorkCounters {
            authority_records_read: u64::from(request.limit),
            authority_bytes_read: STREAM_RECORD_LIMIT.saturating_mul(u64::from(request.limit)),
            backend_read_operations: u64::from(!meter.is_native()), ..WorkCounters::default()
        };
        #[cfg(not(target_arch = "wasm32"))]
        if meter.is_native() {
            if kind == ReadKind::Exact { meter.admit(admission)?; }
            let result = self.provider.native_read(request, meter.native_meter()).await;
            let stream = meter.result(result).map(AuthorityRead::Native)?;
            if kind == ReadKind::Exact { meter.check()?; }
            return Ok(stream);
        }
        if kind != ReadKind::Replay {
            if kind == ReadKind::Exact {
                let peak = meter.allocations.live_bytes().checked_add(STREAM_RECORD_LIMIT)
                .ok_or_else(|| ForkFailure::new(crate::WorkError::Overflow.into(), meter.work))?;
                meter.admit(WorkCounters { peak_allocation_bytes: peak, ..WorkCounters::default() })?;
            }
            admit_snapshot_read(meter.work, admission, meter.budget, meter.cancellation)?;
            meter.work.backend_read_operations += 1;
        } else {
            meter.admit(admission)?;
            meter.charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::default() })?;
        }
        let result = self.provider.read(request).await.map(AuthorityRead::Plain)
            .map_err(|error| ForkFailure::new(map_stream_error(error), meter.spent()));
        if kind != ReadKind::Replay {
            let stream = result?;
            if kind == ReadKind::Exact {
                meter.cancellation.check()
                    .map_err(|_| ForkFailure::new(AuthorityStoreError::Cancelled, meter.work))?;
            }
            Ok(stream)
        } else {
            meter.check()?;
            result
        }
    }

    async fn authority_inspect(
        &self, key: acyclic_stream::IdempotencyKey, meter: &mut ForkWork<'_>,
    ) -> Result<AuthorityProviderOutput<Option<acyclic_stream::IdempotencyObservation>>, ForkFailure> {
        meter.check()?;
        #[cfg(not(target_arch = "wasm32"))]
        if meter.is_native() {
            let result = self.provider.native_inspect_idempotency(key, meter.native_meter()).await;
            let observation = meter.result(result)?;
            meter.check()?;
            return Ok(AuthorityProviderOutput::Native(observation));
        }
        meter.charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::default() })?;
        let result = self.provider.inspect_idempotency(key).await
            .map(AuthorityProviderOutput::Plain)
            .map_err(|error| ForkFailure::new(map_stream_error(error), meter.spent()));
        meter.check()?;
        result
    }

    async fn commit<G: AuthorityPublicationGuard>(
        &self, request: acyclic_stream::CommitRequest, deadline: Option<u64>,
        meter: &mut ForkWork<'_>, guard: &mut G,
    ) -> Result<AuthorityProviderOutput<acyclic_stream::CommitOutcome>, AppendFailure<G::Error>> {
        let submitted = meter.request_write_work(&request)?;
        meter.admit(submitted)?;
        #[cfg(not(target_arch = "wasm32"))]
        if meter.is_native() {
            let outcome = guard.native_commit(self, request, deadline, meter).await?;
            meter.release_request()?;
            if matches!(&*outcome, acyclic_stream::CommitOutcome::Committed(_)) {
                meter.charge(submitted)?;
            }
            meter.check()?;
            return Ok(outcome);
        }
        meter.check()?;
        guard.seal().await.map_err(AppendFailure::Owner)?;
        meter.check()?;
        meter.admit(submitted)?;
        let result = match deadline {
            Some(deadline) => self.provider.commit_before(request, deadline).await,
            None => self.provider.commit(request).await,
        };
        meter.charge(commit_work(submitted, &result))?;
        meter.check()?;
        result.map(AuthorityProviderOutput::Plain).map_err(|error|
            AppendFailure::Authority(ForkFailure::new(AuthorityStoreError::NativeStream(error), meter.spent())))
    }

    async fn authority_replay(
        &self, authority: AuthorityId, after: Sequence, limit: ReplayLimit, meter: &mut ForkWork<'_>, kind: ReadKind,
    ) -> Result<(Vec<DurableCommit>, ForkAllocation), ForkFailure> {
        if kind == ReadKind::Accounting {
            meter.cancellation.check().map_err(|_| ForkFailure::before_work(AuthorityStoreError::Cancelled))?;
        }
        if limit.records == 0 || limit.payload_bytes == 0 {
            return Err(ForkFailure::new(AuthorityStoreError::InvalidReplayLimit, meter.spent()));
        }
        let mut output = if kind == ReadKind::Accounting { (Vec::new(), None) } else {
            meter.check()?;
            self.authority_require_live(authority, meter).await?;
            meter.vec_owned((limit.records as usize).min(acyclic_stream::MAX_ITEMS))?
        };
        let path = if kind == ReadKind::Accounting { stream_path(authority, "/records").map_err(ForkFailure::before_work)? }
            else { meter.path(authority, "/records", None)? };
        let mut stream = self.authority_read(acyclic_stream::ReadRequest {
            path, from: after.get().saturating_add(1), limit: limit.records,
        }, meter, kind).await?;
        let mut payload_bytes = 0_u64;
        for _ in 0..limit.records {
            let Some(record) = stream.next(meter, kind).await? else { break };
            let owned = if kind == ReadKind::Accounting {
                Some(decode_durable(authority, &record).map_err(|error| ForkFailure::new(error, meter.work))?)
            } else { None };
            let borrowed = if kind == ReadKind::Accounting { None } else { Some(authority_decode_borrowed(authority, &record.value, meter)?) };
            let bytes = owned.as_ref().map_or_else(|| borrowed.as_ref().expect("live replay borrows its record").payload.len(), |record| record.payload.len());
            payload_bytes = payload_bytes.checked_add(bytes as u64)
                .ok_or_else(|| ForkFailure::new(AuthorityStoreError::ReplayRecordTooLarge {
                    observed: u64::MAX, maximum: limit.payload_bytes,
                }, meter.spent()))?;
            if payload_bytes > limit.payload_bytes {
                if output.0.is_empty() {
                    return Err(ForkFailure::new(AuthorityStoreError::ReplayRecordTooLarge {
                        observed: payload_bytes, maximum: limit.payload_bytes,
                    }, meter.spent()));
                }
                break;
            }
            let durable = match owned {
                Some(durable) => durable,
                None => authority_copy_durable(borrowed.expect("live replay borrows its record"), record.committed_at_micros, meter)?,
            };
            if kind == ReadKind::Accounting { output.0.push(durable); } else { meter.push(&mut output.0, durable)?; }
        }
        if kind != ReadKind::Accounting { meter.check()?; }
        Ok(output)
    }

    async fn authority_fence(
        &self, authority: AuthorityId, expected: Head, meter: &mut ForkWork<'_>,
    ) -> Result<FenceOutcome, ForkFailure> {
        meter.check()?;
        let snapshot = self.fork_snapshot(authority, meter).await?;
        if snapshot.head != expected {
            return Ok(FenceOutcome::Conflict { actual: snapshot.head });
        }
        let epoch = expected.epoch.get().checked_add(1).and_then(|next| Epoch::new(next).ok())
            .ok_or_else(|| ForkFailure::new(AuthorityStoreError::EpochExhausted, meter.spent()))?;
        let records = meter.path(authority, "/records", None)?;
        let epochs = meter.path(authority, "/epochs", None)?;
        let retirement = meter.path(authority, "/retirement", None)?;
        let copying = WorkCounters { bytes_copied: 64, ..WorkCounters::default() };
        meter.admit(copying)?;
        let mut identity = [0_u8; 64];
        identity[..16].copy_from_slice(&authority.into_bytes());
        identity[16..24].copy_from_slice(&expected.epoch.get().to_le_bytes());
        identity[24..32].copy_from_slice(&expected.sequence.get().to_le_bytes());
        identity[32..].copy_from_slice(expected.digest.as_bytes());
        meter.charge(copying)?;
        let idempotency_key = meter.stream_key(b"fence", &identity)?;
        let mut conditions = meter.vec(3)?;
        meter.push(&mut conditions, acyclic_stream::CommitCondition::Absent { path: retirement })?;
        meter.push(&mut conditions, acyclic_stream::CommitCondition::Tail {
            path: records, expected: (snapshot.head.sequence.get() + 1),
        })?;
        meter.push(&mut conditions, acyclic_stream::CommitCondition::Tail {
            path: meter.clone_path(&epochs)?, expected: snapshot.epoch_tail,
        })?;
        let mut epoch_records = meter.vec(1)?;
        let encoded = meter.stream_bytes(EPOCH_DOMAIN, &[&epoch.get().to_le_bytes()])?;
        meter.push(&mut epoch_records, encoded)?;
        let mut mutations = meter.vec(1)?;
        meter.push(&mut mutations, acyclic_stream::CommitMutation::Append { path: epochs, records: epoch_records })?;
        let outcome = self.commit(acyclic_stream::CommitRequest { conditions, mutations, idempotency_key },
            None, meter, &mut None::<&mut fn() -> std::future::Ready<Result<(), ForkFailure>>>)
            .await.map_err(|failure| match failure {
                AppendFailure::Authority(failure) | AppendFailure::Owner(failure) => failure,
            })?;
        match &*outcome {
            acyclic_stream::CommitOutcome::Committed(_) => Ok(FenceOutcome::Advanced(Head { epoch, ..expected })),
            acyclic_stream::CommitOutcome::Conflict(_) => {
                let actual = self.fork_snapshot(authority, meter).await?.head;
                Ok(FenceOutcome::Conflict { actual })
            }
        }
    }

    async fn authority_find_operation(
        &self, authority: AuthorityId, operation: OperationId, meter: &mut ForkWork<'_>,
    ) -> Result<(Option<DurableCommit>, ForkAllocation), ForkFailure> {
        meter.check()?;
        self.authority_require_live(authority, meter).await?;
        let (_, allocation) = meter.vec_owned::<u8>(0)?;
        let mut output = (None, allocation);
        let key = meter.operation_key(authority, operation)?;
        let observation = self.authority_inspect(key, meter).await?;
        let Some(observation) = &*observation else { return Ok(output) };
        let acyclic_stream::IdempotencyOutcome::Commit(outcome) = &observation.outcome else {
            return Err(authority_corrupt("operation identity retained another Stream mutation family", meter));
        };
        let acyclic_stream::CommitOutcome::Committed(envelope) = outcome else { return Ok(output) };
        let records = meter.path(authority, "/records", None)?;
        for mutation in &envelope.mutations {
            let acyclic_stream::CommittedMutation::Append(append) = mutation else { continue };
            if append.path != records { continue; }
            let record = match append.records.as_slice() {
                [record] => record,
                [genesis, record] => match meter.decode_epoch(&genesis.value, GENESIS_DOMAIN) {
                    Ok(_) => record,
                    #[cfg(not(target_arch = "wasm32"))]
                    Err(failure) if matches!(&failure.error, AuthorityStoreError::NativeOriginal(_) | AuthorityStoreError::Cancelled) => {
                        return Err(failure);
                    }
                    Err(_) => return Err(authority_corrupt("operation appended an invalid authority record count", meter)),
                },
                _ => return Err(authority_corrupt("operation appended an invalid authority record count", meter)),
            };
            let reading = WorkCounters {
                authority_records_read: 1, authority_bytes_read: record.value.len() as u64,
                ..WorkCounters::default()
            };
            meter.admit(reading)?;
            meter.charge(reading)?;
            let borrowed = authority_decode_borrowed(authority, &record.value, meter)?;
            if borrowed.operation_id != operation || output.0.is_some() {
                return Err(authority_corrupt("operation envelope does not bind its authority operation", meter));
            }
            output.0 = Some(authority_copy_durable(borrowed, record.committed_at_micros, meter)?);
        }
        if output.0.is_none() {
            return Err(authority_corrupt("operation envelope omits its authority record", meter));
        }
        meter.check()?;
        Ok(output)
    }

    /// Lists up to `maximum` exact filesystem authority identities, in order.
    pub async fn authorities(&self, maximum: u32) -> Result<Vec<AuthorityId>, AuthorityStoreError> {
        /// Restarts tolerated when authorities come and go mid-listing.
        const RESTARTS: usize = 16;
        let parent = acyclic_stream::StreamPath::new("fs/authorities").map_err(map_stream_error)?;
        let page = u32::try_from(acyclic_stream::MAX_ITEMS).unwrap_or(u32::MAX);
        'listing: for _ in 0..RESTARTS {
            let mut authorities = Vec::new();
            let mut after = None;
            let mut hierarchy_version = None;
            loop {
                let listed = match self
                    .provider
                    .children_page(acyclic_stream::ChildrenPageRequest {
                        parent: Some(parent.clone()),
                        after: after.take(),
                        hierarchy_version,
                        limit: page,
                    })
                    .await
                {
                    Ok(listed) => listed,
                    Err(acyclic_stream::StreamError::HierarchyChanged) => continue 'listing,
                    Err(error) => return Err(map_stream_error(error)),
                };
                for child in &listed.children {
                    let authority = authority_child(&child.path)?;
                    if !self.is_retired(authority).await? {
                        authorities.push(authority);
                    }
                }
                if authorities.len() > maximum as usize {
                    return Err(AuthorityStoreError::Rejected(
                        "authority listing bound exceeded".to_owned(),
                    ));
                }
                let Some(next) = listed.next_after else {
                    return Ok(authorities);
                };
                after = Some(next);
                hierarchy_version = Some(listed.hierarchy_version);
            }
        }
        Err(AuthorityStoreError::Rejected(
            "authorities kept changing while they were listed".to_owned(),
        ))
    }

    async fn historical_snapshot(
        &self,
        authority_id: AuthorityId,
    ) -> Result<AuthoritySnapshot, AuthorityStoreError> {
        let records = stream_path(authority_id, "/records")?;
        let epochs = stream_path(authority_id, "/epochs")?;
        let record_tail = self
            .provider
            .tail(records.clone())
            .await
            .map_err(map_stream_error)?;
        let epoch_tail = self
            .provider
            .tail(epochs.clone())
            .await
            .map_err(map_stream_error)?;
        if record_tail == 0 || epoch_tail == 0 {
            return Err(AuthorityStoreError::Corrupt(
                "authority genesis paths are empty".to_owned(),
            ));
        }
        let epoch_record = read_one(self.provider.as_ref(), epochs, epoch_tail - 1).await?;
        let epoch = decode_epoch(&epoch_record.value, EPOCH_DOMAIN, false)?;
        let head = if record_tail == 1 {
            let genesis = read_one(self.provider.as_ref(), records, 0).await?;
            let _genesis_epoch = decode_epoch(&genesis.value, GENESIS_DOMAIN, false)?;
            Head::genesis(epoch)
        } else {
            let record = read_one(self.provider.as_ref(), records, record_tail - 1).await?;
            let durable = decode_durable(authority_id, &record)?;
            if durable.sequence.get().saturating_add(1) != record_tail {
                return Err(AuthorityStoreError::Corrupt(
                    "Stream sequence does not match filesystem authority sequence".to_owned(),
                ));
            }
            Head {
                epoch,
                sequence: durable.sequence,
                digest: durable.digest,
            }
        };
        Ok(AuthoritySnapshot {
            head,
            epoch_tail,
        })
    }

    async fn is_retired(&self, authority_id: AuthorityId) -> Result<bool, AuthorityStoreError> {
        let path = stream_path(authority_id, "/retirement")?;
        match self.provider.tail(path.clone()).await {
            Err(acyclic_stream::StreamError::NotFound) => Ok(false),
            Ok(1) => {
                let marker = read_one(self.provider.as_ref(), path, 0).await?;
                if marker.value.as_ref() != AUTHORITY_RETIRED {
                    return Err(AuthorityStoreError::Corrupt(
                        "filesystem retirement marker is invalid".to_owned(),
                    ));
                }
                Ok(true)
            }
            Ok(_) => Err(AuthorityStoreError::Corrupt(
                "filesystem retirement marker has an invalid tail".to_owned(),
            )),
            Err(error) => Err(map_stream_error(error)),
        }
    }

    async fn record(
        &self, path: acyclic_stream::StreamPath, from: u64, meter: &mut ForkWork<'_>,
    ) -> Result<AuthorityProviderOutput<acyclic_stream::Record>, ForkFailure> {
        let mut stream = self.authority_read(acyclic_stream::ReadRequest { path, from, limit: 1 }, meter, ReadKind::Exact).await?;
        let record = stream.next(meter, ReadKind::Exact).await?.ok_or_else(|| {
            let error = if meter.is_native() {
                AuthorityStoreError::CorruptStatic("Stream authority path is missing its record")
            } else { AuthorityStoreError::Corrupt("Stream returned an empty exact read".to_owned()) };
            ForkFailure::new(error, meter.spent())
        })?;
        if !meter.is_native() && record.sequence != from {
            return Err(authority_corrupt("Stream returned a non-contiguous record", meter));
        }
        Ok(record)
    }

    async fn tail(
            &self,
            path: acyclic_stream::StreamPath,
            meter: &mut ForkWork<'_>,
        ) -> Result<u64, ForkFailure> {
            #[cfg(not(target_arch = "wasm32"))]
            if meter.is_native() {
                meter.check()?;
                let result = self.provider.native_tail(path, meter.native_meter()).await;
                let tail = meter.result(result)?;
                return Ok(*tail.value());
            }
            let budget = meter.budget;
            let cancellation = meter.cancellation;
            let work = &mut meter.work;
            let next = admit_snapshot_read(
                *work,
                WorkCounters {
                    backend_read_operations: 1,
                    ..WorkCounters::default()
                },
                budget,
                cancellation,
            )?;
            *work = next;
            let result = self.provider.tail(path).await;
            cancellation
                .check()
                .map_err(|_| ForkFailure::new(AuthorityStoreError::Cancelled, *work))?;
            result.map_err(|error| ForkFailure::new(map_stream_error(error), *work))
        }

    async fn fork_snapshot(
            &self,
            authority: AuthorityId,
            meter: &mut ForkWork<'_>,
        ) -> Result<AuthoritySnapshot, ForkFailure> {
            let baseline = if meter.is_native() { 0 } else { meter.allocations.live_bytes() };
            let result = async {
            self.authority_require_live(authority, meter).await?;
            let records = meter.path(authority, "/records", None)?;
            let epochs = meter.path(authority, "/epochs", None)?;
            let record_tail = self.tail(meter.clone_path(&records)?, meter).await?;
            let epoch_tail = self.tail(meter.clone_path(&epochs)?, meter).await?;
            if record_tail == 0 || epoch_tail == 0 {
                return Err(ForkFailure::new(AuthorityStoreError::CorruptStatic("authority genesis paths are empty"), meter.spent()));
            }
            let epoch_record = self.record(meter.clone_path(&epochs)?, epoch_tail - 1, meter).await?;
            let epoch = meter.decode_epoch(&epoch_record.value, EPOCH_DOMAIN)?;
            drop(epoch_record);
            let epochs_bytes = epochs.as_str().len() as u64;
            drop(epochs);
            meter.release(epochs_bytes)?;
            let record = self.record(meter.clone_path(&records)?, record_tail - 1, meter).await?;
            let head = if record_tail == 1 {
                meter.decode_epoch(&record.value, GENESIS_DOMAIN)?;
                Head::genesis(epoch)
            } else {
                let durable = meter.decode_durable(&record.value)?;
                let digest = meter.durable_digest(authority, &durable)?;
                if digest != durable.digest || durable.sequence.get().checked_add(1) != Some(record_tail) {
                    return Err(ForkFailure::new(AuthorityStoreError::CorruptStatic("Stream authority record or sequence mismatch"), meter.spent()));
                }
                Head { epoch, sequence: durable.sequence, digest: durable.digest }
            };
            drop(record);
            let records_bytes = records.as_str().len() as u64;
            drop(records);
            meter.release(records_bytes)?;
            Ok(AuthoritySnapshot { head, epoch_tail })
            }.await;
            if result.is_err() && !meter.is_native() {
                let remaining = meter.allocations.live_bytes().checked_sub(baseline)
                    .ok_or_else(|| ForkFailure::new(crate::WorkError::Overflow.into(), meter.work))?;
                meter.release(remaining)?;
            }
            result
        }

    async fn authority_require_live(
        &self, authority: AuthorityId, meter: &mut ForkWork<'_>,
    ) -> Result<(), ForkFailure> {
        let retirement = meter.path(authority, "/retirement", None)?;
        let retired = match self.tail(meter.clone_path(&retirement)?, meter).await {
            Err(failure) if matches!(failure.error, AuthorityStoreError::Missing) => false,
            Ok(1) => {
                let marker = self.record(meter.clone_path(&retirement)?, 0, meter).await?;
                if marker.value.as_ref() != AUTHORITY_RETIRED {
                    return Err(ForkFailure::new(AuthorityStoreError::CorruptStatic("filesystem retirement marker is invalid"), meter.spent()));
                }
                true
            }
            Ok(_) => return Err(ForkFailure::new(AuthorityStoreError::CorruptStatic("filesystem retirement marker has an invalid tail"), meter.spent())),
            Err(failure) => return Err(failure),
        };
        let retirement_bytes = retirement.as_str().len() as u64;
        drop(retirement);
        meter.release(retirement_bytes)?;
        if retired { return Err(ForkFailure::new(AuthorityStoreError::Retired, meter.work)); }
        Ok(())
    }

    async fn fork_gate(
            &self,
            authority: AuthorityId,
            meter: &mut ForkWork<'_>,
        ) -> Result<(), ForkFailure> {
            let gate = meter.path(authority, "/publication-gate", None)?;
            let tail = self.tail(meter.clone_path(&gate)?, meter).await?;
            if tail == 0 {
                return Err(ForkFailure::new(AuthorityStoreError::Corrupt("authority publication gate is empty".to_owned()), meter.work));
            }
            let record = self.record(meter.clone_path(&gate)?, tail - 1, meter).await?;
            decode_publication_gate(&record.value).map_err(|error| ForkFailure::new(error, meter.work))?;
            drop(record);
            let bytes = gate.as_str().len() as u64;
            drop(gate);
            meter.release(bytes)
        }

    async fn existing_fork_destination(
            &self,
            destination_authority: AuthorityId,
            source_generation: GenerationId,
            forked_at: u64,
            meter: &mut ForkWork<'_>,
        ) -> Result<Option<AuthoritySnapshot>, ForkFailure> {
            let snapshot = match self.fork_snapshot(destination_authority, meter).await {
                Ok(snapshot) => snapshot,
                Err(failure) if authority_missing(&failure.error) => return Ok(None),
                Err(failure) => return Err(failure),
            };
            let locator = meter.path(destination_authority, "/generations/", Some(source_generation))?;
            let locator_bytes = locator.as_str().len() as u64;
            let record = self.record(locator, 0, meter).await?;
            meter.release(locator_bytes)?;
            let located_tail = decode_lineage_tail(&record.value).map_err(|error| ForkFailure::new(error, meter.work))?;
            drop(record);
            if located_tail == 0 {
                return Err(authority_corrupt("destination generation locator selected an empty lineage", meter));
            }
            let lineage = meter.path(destination_authority, "/lineage", None)?;
            let lineage_bytes = lineage.as_str().len() as u64;
            let selected = self.record(lineage, located_tail - 1, meter).await?;
            meter.release(lineage_bytes)?;
            let generation = decode_lineage_generation(&selected.value).map_err(|error| ForkFailure::new(error, meter.work))?;
            if located_tail == forked_at && generation == source_generation {
                Ok(Some(snapshot))
            } else {
                Err(ForkFailure::new(AuthorityStoreError::RejectedStatic("destination generation fork conflicts with existing state"), meter.spent()))
            }
        }

    /// Resolves the original source identity with bounded paths and exact reads.
    async fn resolve_source_fork_point(
            &self,
            source_authority: AuthorityId,
            source_generation: GenerationId,
            meter: &mut ForkWork<'_>,
        ) -> Result<(acyclic_stream::StreamPath, u64), ForkFailure> {
            let source_lineage = meter.path(source_authority, "/lineage", None)?;
            let locator = meter.path(source_authority, "/generations/", Some(source_generation))?;
            let locator_bytes = locator.as_str().len() as u64;
            let record = self.record(locator, 0, meter).await.map_err(|failure| {
                if authority_missing(&failure.error) && !meter.is_native() {
                    ForkFailure::new(AuthorityStoreError::RejectedStatic("source authority has no generation lineage locator"), failure.work)
                } else { failure }
            })?;
            meter.release(locator_bytes)?;
            let forked_at = decode_lineage_tail(&record.value).map_err(|error| ForkFailure::new(error, meter.work))?;
            drop(record);
            if forked_at == 0 {
                return Err(authority_corrupt("generation locator selected an empty lineage", meter));
            }
            let selected = self.record(meter.clone_path(&source_lineage)?, forked_at - 1, meter).await?;
            let selected_generation = decode_lineage_generation(&selected.value).map_err(|error| ForkFailure::new(error, meter.work))?;
            if selected_generation != source_generation {
                return Err(authority_corrupt("generation locator and lineage disagree", meter));
            }
            Ok((source_lineage, forked_at))
        }
}

impl<P: acyclic_stream::StreamProvider> StreamAuthorityStore<P> {

    async fn first_record<'a, G: ForkPublicationGuard>(
        &self, authority: AuthorityId, commit: ProposedCommit,
        mut meter: ForkWork<'a>, mut guard: G,
    ) -> Result<crate::FsReceipt<bool>, ForkFailure<G::Error>> {
        meter.check().map_err(fork_fs_failure)?;
        let idempotency_key = meter.operation_key(authority, commit.operation_id).map_err(fork_fs_failure)?;
        let mut request = acyclic_stream::CommitRequest {
            conditions: meter.vec(7).map_err(fork_fs_failure)?,
            mutations: meter.vec(7).map_err(fork_fs_failure)?,
            idempotency_key,
        };
        first_record_commit(&mut request, authority, &commit, None, &mut meter).map_err(fork_fs_failure)?;
        let writes = meter.request_write_work(&request).map_err(fork_fs_failure)?;
        meter.admit(writes).map_err(fork_fs_failure)?;
        let result = guard.commit(self, request, &mut meter).await;
        if !meter.is_native() { meter.release_request().map_err(fork_fs_failure)?; }
        match result {
            Ok(outcome) => match &*outcome {
                acyclic_stream::CommitOutcome::Committed(_) => {
                    // An ACK confirms this operation, including keyed replay.
                    guard.seal(&mut meter).await?;
                    return Ok(crate::FsReceipt { value: true, work: meter.spent() });
                }
                acyclic_stream::CommitOutcome::Conflict(_) => {}
            },
            Err(AppendFailure::Authority(failure)) if matches!(authority_stream_error(&failure.error),
                Some(acyclic_stream::StreamError::IdempotencyMismatch)) =>
                return Ok(crate::FsReceipt { value: false, work: meter.spent() }),
            Err(AppendFailure::Authority(failure)) => return Err(fork_fs_failure(failure)),
            Err(AppendFailure::Owner(error)) => return Err(ForkFailure::new(error, meter.spent())),
        }
        let records = meter.path(authority, "/records", None).map_err(fork_fs_failure)?;
        let path_bytes = records.as_str().len() as u64;
        let tail = self.tail(meter.clone_path(&records).map_err(fork_fs_failure)?, &mut meter).await;
        let value = match tail {
            Ok(tail) if tail >= 2 => {
                let record = self.record(records, 1, &mut meter).await.map_err(fork_fs_failure)?;
                let first = meter.decode_durable(&record.value).map_err(fork_fs_failure)?;
                let digest = meter.durable_digest(authority, &first).map_err(fork_fs_failure)?;
                if digest != first.digest {
                    return Err(ForkFailure::new(crate::FsError::from(AuthorityStoreError::CorruptStatic(
                        "Stream durable record digest mismatch")).into(), meter.spent()));
                }
                first.sequence == Sequence::new(1) && first.operation_id == commit.operation_id
                    && first.fingerprint == commit.fingerprint
            }
            Ok(_) => { drop(records); false }
            Err(failure) if authority_missing(&failure.error) => { drop(records); false }
            Err(failure) => return Err(fork_fs_failure(failure)),
        };
        meter.release(path_bytes).map_err(fork_fs_failure)?;
        if value { guard.seal(&mut meter).await?; }
        Ok(crate::FsReceipt { value, work: meter.spent() })
    }

    async fn workspace_fork<'a, G: ForkPublicationGuard>(
            &self, fork: WorkspaceForkCommit, permit: PublicationPermit,
            mut meter: ForkWork<'a>, mut guard: G,
        ) -> Result<crate::FsReceipt<WorkspaceForkOutcome>, ForkFailure<G::Error>> {
            if !matches!(permit, PublicationPermit::Unrestricted) {
                return Err(ForkFailure::before_work(crate::FsError::from(AuthorityStoreError::RejectedStatic(
                    "workspace fork does not support a publication reservation")).into()));
            }
            let request = async {
                let meter = &mut meter;
                let idempotency_key = meter.operation_key(fork.destination, fork.creation.operation_id)?;
                let mut request = acyclic_stream::CommitRequest {
                    conditions: meter.vec(14)?,
                    mutations: meter.vec(14)?,
                    idempotency_key,
                };
                let lineage = match fork.lineage {
                    Some(source) if source.lineage == crate::GenerationFork::PublishedPrefix => {
                        let (source_lineage, forked_at) = self
                            .resolve_source_fork_point(source.authority, source.generation, meter).await?;
                        Some(ForkedLineage { source_lineage, forked_at, source_generation: source.generation })
                    }
                    Some(_) | None => None,
                };
                first_record_commit(&mut request, fork.retention, &fork.retained, None, meter)?;
                first_record_commit(&mut request, fork.destination, &fork.creation, lineage, meter)?;
                Ok::<_, ForkFailure>(request)
            }.await.map_err(fork_fs_failure)?;
            let writes = meter.request_write_work(&request).map_err(fork_fs_failure)?;
            meter.admit(writes).map_err(fork_fs_failure)?;
            let result = match guard.commit(self, request, &mut meter).await {
                Ok(output) => Some(output),
                Err(AppendFailure::Authority(failure)) if matches!(authority_stream_error(&failure.error),
                    Some(acyclic_stream::StreamError::IdempotencyMismatch)) => None,
                Err(AppendFailure::Authority(failure)) => return Err(fork_fs_failure(failure)),
                Err(AppendFailure::Owner(error)) => return Err(ForkFailure::new(error, meter.spent())),
            };
            if !meter.is_native() { meter.release_request().map_err(fork_fs_failure)?; }
            match result.as_ref().map(std::ops::Deref::deref) {
                Some(acyclic_stream::CommitOutcome::Committed(_)) => {
                    guard.seal(&mut meter).await?;
                    return Ok(crate::FsReceipt { value: WorkspaceForkOutcome::Committed, work: meter.spent() });
                }
                Some(acyclic_stream::CommitOutcome::Conflict(_)) | None => {}
            }
            guard.steps(self, fork, &mut meter).await
        }

    async fn create<'a, G: ForkPublicationGuard>(
            &self, authority: AuthorityId, epoch: Epoch,
            mut meter: ForkWork<'a>, mut guard: G,
        ) -> Result<crate::FsReceipt<CreateAuthorityOutcome>, ForkFailure<G::Error>> {
            match self.fork_snapshot(authority, &mut meter).await {
                Ok(snapshot) => {
                    self.fork_gate(authority, &mut meter).await.map_err(fork_fs_failure)?;
                    guard.seal(&mut meter).await?;
                    return Ok(crate::FsReceipt { value: CreateAuthorityOutcome::Existing(snapshot.head), work: meter.spent() });
                }
                Err(failure) if authority_missing(&failure.error) => {}
                Err(failure) => return Err(fork_fs_failure(failure)),
            }
            let idempotency_key = meter.stream_key(b"create", &authority.into_bytes()).map_err(fork_fs_failure)?;
            let mut conditions = meter.vec(4).map_err(fork_fs_failure)?;
            let mut mutations = meter.vec(4).map_err(fork_fs_failure)?;
            for (suffix, domain, static_value) in [
                ("", None, Some(AUTHORITY_MARKER)),
                ("/records", Some(GENESIS_DOMAIN), None),
                ("/epochs", Some(EPOCH_DOMAIN), None),
                ("/publication-gate", None, Some(PUBLICATION_GATE_FREE)),
            ] {
                let path = meter.path(authority, suffix, None).map_err(fork_fs_failure)?;
                let condition = acyclic_stream::CommitCondition::Absent { path: meter.clone_path(&path).map_err(fork_fs_failure)? };
                meter.push(&mut conditions, condition).map_err(fork_fs_failure)?;
                let mut records = meter.vec(1).map_err(fork_fs_failure)?;
                let value = if let Some(value) = static_value { acyclic_stream::OwnedBytes::from_static(value) }
                    else { meter.stream_bytes(domain.expect("epoch record domain"), &[&epoch.get().to_le_bytes()]).map_err(fork_fs_failure)? };
                meter.push(&mut records, value).map_err(fork_fs_failure)?;
                meter.push(&mut mutations, acyclic_stream::CommitMutation::Append { path, records }).map_err(fork_fs_failure)?;
            }
            let request = acyclic_stream::CommitRequest { conditions, mutations, idempotency_key };
            let writes = meter.request_write_work(&request).map_err(fork_fs_failure)?;
            meter.admit(writes).map_err(fork_fs_failure)?;
            let result = guard.commit(self, request, &mut meter).await.map_err(|failure| match failure {
                AppendFailure::Authority(failure) => fork_fs_failure(failure),
                AppendFailure::Owner(error) => ForkFailure::new(error, meter.spent()),
            })?;
            if !meter.is_native() { meter.release_request().map_err(fork_fs_failure)?; }
            let value = match &*result {
                acyclic_stream::CommitOutcome::Committed(_) => {
                    guard.seal(&mut meter).await?;
                    CreateAuthorityOutcome::Created(Head::genesis(epoch))
                }
                acyclic_stream::CommitOutcome::Conflict(_) => {
                    let snapshot = self.fork_snapshot(authority, &mut meter).await.map_err(fork_fs_failure)?;
                    self.fork_gate(authority, &mut meter).await.map_err(fork_fs_failure)?;
                    guard.seal(&mut meter).await?;
                    CreateAuthorityOutcome::Existing(snapshot.head)
                }
            };
            Ok(crate::FsReceipt { value, work: meter.spent() })
        }
}

#[cfg(test)]
impl<P: acyclic_stream::StreamProvider> StreamAuthorityStore<P> {
    /// Every generation in one authority's lineage, oldest first.
    pub(crate) async fn generation_lineage(
        &self,
        authority: AuthorityId,
    ) -> Result<Vec<GenerationId>, AuthorityStoreError> {
        let path = ForkWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .path(authority, "/lineage", None).map_err(|failure| failure.error)?;
        let tail = self
            .provider
            .tail(path.clone())
            .await
            .map_err(map_stream_error)?;
        let mut lineage = Vec::new();
        for sequence in 0..tail {
            let record = read_one(self.provider.as_ref(), path.clone(), sequence).await?;
            lineage.push(ForkWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
                .decode_fixed::<32>(&record.value, LINEAGE_DOMAIN, "invalid Stream generation lineage record").map(|bytes| GenerationId::new(Digest::from_bytes(bytes))).map_err(|failure| failure.error)?);
        }
        Ok(lineage)
    }

    /// The lineage length at which one generation is located.
    pub(crate) async fn generation_locator(
        &self,
        authority: AuthorityId,
        generation: GenerationId,
    ) -> Result<u64, AuthorityStoreError> {
        let record = read_one(
            self.provider.as_ref(),
            ForkWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
                .path(authority, "/generations/", Some(generation)).map_err(|failure| failure.error)?,
            0,
        )
        .await?;
        decode_lineage_tail(&record.value)
    }
}

/// The source lineage prefix a forked destination authority starts from.
struct ForkedLineage {
    source_lineage: acyclic_stream::StreamPath,
    forked_at: u64,
    source_generation: GenerationId,
}

/// Adds the authority creation and first durable record to one transaction.
fn first_record_commit(
    request: &mut acyclic_stream::CommitRequest,
    authority: AuthorityId,
    commit: &ProposedCommit,
    lineage: Option<ForkedLineage>,
    meter: &mut ForkWork<'_>,
) -> Result<(), ForkFailure> {
    use acyclic_stream::CommitCondition::Absent;
    use acyclic_stream::CommitMutation::Append;
    let epoch = Epoch::GENESIS;
    let sequence = Sequence::new(1);
    let previous = Head::genesis(epoch);
    let mut durable = crate::streams_record::BorrowedStreamsDurableRecord {
        epoch, sequence, operation_id: commit.operation_id, fingerprint: commit.fingerprint,
        previous_digest: previous.digest, digest: Digest::from_bytes([0; 32]), payload: &commit.payload,
    };
    durable.digest = meter.durable_digest(authority, &durable)?;
    let encoded = StreamsDurableRecord::encode_borrowed_with(&durable, STREAM_RECORD_LIMIT, |domain, fields| meter.stream_bytes(domain, fields))
        .map_err(|error| ForkFailure::new(if meter.is_native() { AuthorityStoreError::DurableCodec(error) } else { AuthorityStoreError::Rejected(error.to_string()) }, meter.spent()))??;
    let root = meter.path(authority, "", None)?;
    let records = meter.path(authority, "/records", None)?;
    let epochs = meter.path(authority, "/epochs", None)?;
    let gate = meter.path(authority, "/publication-gate", None)?;
    for path in [&root, &records, &epochs, &gate] {
        let condition = Absent { path: meter.clone_path(path)? };
        meter.push(&mut request.conditions, condition)?;
    }
    let mut marker_records = meter.vec(1)?;
    meter.push(&mut marker_records, acyclic_stream::OwnedBytes::from_static(AUTHORITY_MARKER))?;
    meter.push(&mut request.mutations, Append { path: root, records: marker_records })?;
    let generation = generation_from_payload(authority, sequence, durable.payload, meter)?;
    lineage_commit(request, authority, lineage, generation, meter)?;
    let mut record_values = meter.vec(2)?;
    let genesis = meter.stream_bytes(GENESIS_DOMAIN, &[&epoch.get().to_le_bytes()])?;
    meter.push(&mut record_values, genesis)?;
    meter.push(&mut record_values, encoded)?;
    meter.push(&mut request.mutations, Append { path: records, records: record_values })?;
    let mut epoch_values = meter.vec(1)?;
    let epoch_record = meter.stream_bytes(EPOCH_DOMAIN, &[&epoch.get().to_le_bytes()])?;
    meter.push(&mut epoch_values, epoch_record)?;
    meter.push(&mut request.mutations, Append { path: epochs, records: epoch_values })?;
    let mut gate_values = meter.vec(1)?;
    meter.push(&mut gate_values, acyclic_stream::OwnedBytes::from_static(PUBLICATION_GATE_FREE))?;
    meter.push(&mut request.mutations, Append { path: gate, records: gate_values })?;
    Ok(())
}

/// Adds the forked prefix and newly published generation to the authority lineage.
fn lineage_commit(
    request: &mut acyclic_stream::CommitRequest,
    authority: AuthorityId,
    lineage: Option<ForkedLineage>,
    generation: Option<GenerationId>,
    meter: &mut ForkWork<'_>,
) -> Result<(), ForkFailure> {
    use acyclic_stream::CommitCondition::Absent;
    use acyclic_stream::CommitMutation::{Append, Fork};
    if lineage.is_none() && generation.is_none() {
        return Ok(());
    }
    let lineage_path = meter.path(authority, "/lineage", None)?;
    let condition = Absent { path: meter.clone_path(&lineage_path)? };
    meter.push(&mut request.conditions, condition)?;
    let mut entries = meter.vec(usize::from(generation.is_some()))?;
    if let Some(generation) = generation {
        let entry = meter.stream_bytes(LINEAGE_DOMAIN, &[generation.digest().as_bytes()])?;
        meter.push(&mut entries, entry)?;
    }
    let mut tail = 0;
    let mut forked_locator = None;
    match lineage {
        Some(forked) => {
            let locator = meter.path(authority, "/generations/", Some(forked.source_generation))?;
            let condition = Absent { path: meter.clone_path(&locator)? };
            meter.push(&mut request.conditions, condition)?;
            meter.push(&mut request.mutations, Fork {
                source: forked.source_lineage,
                destination: lineage_path,
                at_tail: forked.forked_at,
                records: entries,
            })?;
            let mut locator_values = meter.vec(1)?;
            let locator_record = meter.stream_bytes(LINEAGE_TAIL_DOMAIN, &[&forked.forked_at.to_le_bytes()])?;
            meter.push(&mut locator_values, locator_record)?;
            let append = Append { path: meter.clone_path(&locator)?, records: locator_values };
            meter.push(&mut request.mutations, append)?;
            tail = forked.forked_at;
            forked_locator = Some(locator);
        }
        None => meter.push(&mut request.mutations, Append { path: lineage_path, records: entries })?,
    }
    if let Some(generation) = generation {
        let locator = meter.path(authority, "/generations/", Some(generation))?;
        if forked_locator.as_ref() == Some(&locator) {
            return Err(ForkFailure::new(AuthorityStoreError::Rejected(
                "a fork cannot publish its source generation again".to_owned(),
            ), meter.work));
        }
        let locator_record = meter.stream_bytes(LINEAGE_TAIL_DOMAIN, &[&tail.saturating_add(1).to_le_bytes()])?;
        let condition = Absent { path: meter.clone_path(&locator)? };
        meter.push(&mut request.conditions, condition)?;
        let mut locator_values = meter.vec(1)?;
        meter.push(&mut locator_values, locator_record)?;
        meter.push(&mut request.mutations, Append { path: locator, records: locator_values })?;
    }
    Ok(())
}

async fn seal_append_owner<G: AuthorityPublicationGuard>(
    meter: &ForkWork<'_>, guard: &mut G,
) -> Result<(), AppendFailure<G::Error>> {
    meter.check()?;
    guard.seal().await.map_err(AppendFailure::Owner)?;
    meter.check()?;
    Ok(())
}

impl<P: acyclic_stream::StreamProvider> StreamAuthorityStore<P> {
    /// Builds and publishes one guarded authority transaction.
    async fn append<G: AuthorityPublicationGuard>(
        &self, request: crate::GuardedAppend, mut meter: ForkWork<'_>, mut guard: G,
    ) -> Result<AuthorityReceipt<FsAppendOutcome>, AppendFailure<G::Error>> {
        use acyclic_stream::{CommitCondition, CommitMutation, CommitOutcome, StreamError};
        let crate::GuardedAppend { authority_id, epoch, expected, commit, permit } = request;
        if let Some(existing) = self.authority_find_operation(authority_id, commit.operation_id, &mut meter).await?.0 {
            let value = if existing.fingerprint == commit.fingerprint {
                FsAppendOutcome::AlreadyCommitted(existing)
            } else {
                FsAppendOutcome::IdempotencyConflict { committed_fingerprint: existing.fingerprint }
            };
            seal_append_owner(&meter, &mut guard).await?;
            return meter.finish(value).map_err(Into::into);
        }
        let snapshot = self.fork_snapshot(authority_id, &mut meter).await?;
        if snapshot.head.epoch != epoch {
            return meter.finish(FsAppendOutcome::Fenced { actual_epoch: snapshot.head.epoch }).map_err(Into::into);
        }
        if snapshot.head != expected {
            return meter.finish(FsAppendOutcome::Conflict { actual: snapshot.head }).map_err(Into::into);
        }
        let (gate, gate_tail, active_reservation) = self.reservation_gate(authority_id, &mut meter).await?;
        let admitted = match permit {
            PublicationPermit::Reservation { operation_id, gate_tail: permitted_tail, expected: reserved_head } =>
                permitted_tail == gate_tail && active_reservation == Some(OperationId::from_bytes(operation_id)) && reserved_head == expected,
            PublicationPermit::Unrestricted => active_reservation.is_none(),
            PublicationPermit::Lease { authority_id: permitted_authority, workspace_id, .. } => {
                let workspace = crate::WorkspaceId::from_bytes(workspace_id);
                active_reservation.is_none() && AuthorityId::from_bytes(permitted_authority) == authority_id
                    && crate::kernel::volume_authority_id(workspace.volume_id()) == authority_id
            }
        };
        if !admitted {
            return meter.finish(FsAppendOutcome::Fenced { actual_epoch: snapshot.head.epoch }).map_err(Into::into);
        }
        let sequence = expected.sequence.checked_next()
            .map_err(|_| ForkFailure::new(AuthorityStoreError::SequenceExhausted, meter.spent()))?;
        let hash = WorkCounters {
            bytes_hashed: crate::foundation::AUTHORITY_COMMIT_DIGEST_ENVELOPE_BYTES
                .checked_add(commit.payload.len() as u64)
                .ok_or_else(|| ForkFailure::new(crate::WorkError::Overflow.into(), meter.spent()))?,
            ..WorkCounters::UNCHARGED
        };
        meter.admit(hash)?;
        let digest = authority_commit_digest(authority_id, epoch, sequence, commit.operation_id,
            commit.fingerprint, expected.digest, &commit.payload);
        meter.charge(hash)?;
        let mut durable = DurableCommit {
            epoch, sequence, operation_id: commit.operation_id, fingerprint: commit.fingerprint,
            previous_digest: expected.digest, digest, payload: commit.payload,
            settled_at_micros: None,
        };
        let borrowed = crate::streams_record::BorrowedStreamsDurableRecord {
            epoch, sequence, operation_id: durable.operation_id, fingerprint: durable.fingerprint,
            previous_digest: durable.previous_digest, digest: durable.digest, payload: &durable.payload,
        };
        let encoded = StreamsDurableRecord::encode_borrowed_with(&borrowed, STREAM_RECORD_LIMIT,
            |domain, fields| meter.stream_bytes(domain, fields))
            .map_err(|error| ForkFailure::new(AuthorityStoreError::DurableCodec(error), meter.spent()))??;
        let records = meter.path(authority_id, "/records", None)?;
        let epochs = meter.path(authority_id, "/epochs", None)?;
        let retirement = meter.path(authority_id, "/retirement", None)?;
        let mut conditions = meter.vec(7)?;
        for condition in [
            CommitCondition::Absent { path: retirement },
            CommitCondition::Tail { path: records.clone(), expected: (snapshot.head.sequence.get() + 1) },
            CommitCondition::Tail { path: epochs, expected: snapshot.epoch_tail },
            CommitCondition::Tail { path: gate.clone(), expected: gate_tail },
        ] { meter.push(&mut conditions, condition)?; }
        let mut lease_path = None;
        let mut deadline = None;
        if let PublicationPermit::Lease { workspace_id, lease_id, expires_at_millis, .. } = permit {
            let path = meter.lease_path(crate::WorkspaceId::from_bytes(workspace_id),
                crate::OperationLeaseId::from_bytes(lease_id), expires_at_millis)?;
            meter.push(&mut conditions, CommitCondition::Tail { path: path.clone(), expected: 1 })?;
            lease_path = Some(path);
            deadline = Some(expires_at_millis);
        }
        let mut mutations = meter.vec(3)?;
        let mut record_values = meter.vec(1)?;
        meter.push(&mut record_values, encoded)?;
        meter.push(&mut mutations, CommitMutation::Append { path: records.clone(), records: record_values })?;
        if let Some(generation) = generation_from_payload(authority_id, sequence, &durable.payload, &mut meter)? {
            let lineage = meter.path(authority_id, "/lineage", None)?;
            let locator = meter.path(authority_id, "/generations/", Some(generation))?;
            let tail = self.tail(locator.clone(), &mut meter).await;
            match tail {
                Ok(1) => {
                    let located = self.record(locator, 0, &mut meter).await?;
                    let located_tail = meter.decode_fixed::<8>(&located.value, LINEAGE_TAIL_DOMAIN, "invalid Stream generation locator").map(u64::from_le_bytes)?;
                    if located_tail == 0 {
                        return Err(authority_corrupt("generation locator selects an empty lineage", &meter).into());
                    }
                    let entry = self.record(lineage, located_tail - 1, &mut meter).await?;
                    if meter.decode_fixed::<32>(&entry.value, LINEAGE_DOMAIN, "invalid Stream generation lineage record").map(|bytes| GenerationId::new(Digest::from_bytes(bytes)))? != generation {
                        return Err(authority_corrupt("generation locator selected another generation", &meter).into());
                    }
                }
                Err(failure) if authority_missing(&failure.error) => {
                    let lineage_tail = match self.tail(lineage.clone(), &mut meter).await {
                        Ok(tail) => tail,
                        Err(failure) if authority_missing(&failure.error) => 0,
                        Err(failure) => return Err(failure.into()),
                    };
                    let condition = if lineage_tail == 0 { CommitCondition::Absent { path: lineage.clone() } }
                        else { CommitCondition::Tail { path: lineage.clone(), expected: lineage_tail } };
                    meter.push(&mut conditions, condition)?;
                    meter.push(&mut conditions, CommitCondition::Absent { path: locator.clone() })?;
                    let mut entries = meter.vec(1)?;
                    let entry = meter.stream_bytes(LINEAGE_DOMAIN, &[generation.digest().as_bytes()])?;
                    meter.push(&mut entries, entry)?;
                    meter.push(&mut mutations, CommitMutation::Append { path: lineage, records: entries })?;
                    let mut locations = meter.vec(1)?;
                    let location = meter.stream_bytes(LINEAGE_TAIL_DOMAIN, &[&lineage_tail.saturating_add(1).to_le_bytes()])?;
                    meter.push(&mut locations, location)?;
                    meter.push(&mut mutations, CommitMutation::Append { path: locator, records: locations })?;
                }
                Ok(_) => return Err(authority_corrupt("generation locator contains an invalid record count", &meter).into()),
                Err(failure) => return Err(failure.into()),
            }
        }
        let key = meter.operation_key(authority_id, durable.operation_id)?;
        let request = acyclic_stream::CommitRequest { conditions, mutations, idempotency_key: key };
        let result = self.commit(request, deadline, &mut meter, &mut guard).await;
        if !meter.is_native() {
            let retained_paths = (gate.as_str().len() as u64)
                .checked_add(records.as_str().len() as u64)
                .and_then(|bytes| bytes.checked_add(lease_path.as_ref().map_or(0, |path| path.as_str().len() as u64)))
                .ok_or_else(|| ForkFailure::new(crate::WorkError::Overflow.into(), meter.spent()))?;
            let retired = meter.allocations.live_bytes().checked_sub(retained_paths)
                .ok_or_else(|| authority_corrupt("append path custody exceeds live request claims", &meter))?;
            meter.release(retired)?;
        }
        let value = match result {
            Ok(outcome) => match &*outcome {
                CommitOutcome::Committed(envelope) => {
                    let record = envelope.mutations.iter().find_map(|mutation| match mutation {
                        acyclic_stream::CommittedMutation::Append(append) if append.path == records =>
                            match append.records.as_slice() {
                                [record] if record.sequence == sequence.get()
                                    && record.commit_id == envelope.commit_id => Some(record),
                                _ => None,
                            },
                        _ => None,
                    }).ok_or_else(|| authority_corrupt("commit envelope omits its authority record", &meter))?;
                    durable.settled_at_micros = (record.committed_at_micros != 0)
                        .then_some(record.committed_at_micros);
                    FsAppendOutcome::Committed(durable)
                }
                CommitOutcome::Conflict(conflicts) => {
                    if let Some(existing) = self.authority_find_operation(authority_id, durable.operation_id, &mut meter).await?.0 {
                        let value = if existing.fingerprint == durable.fingerprint { FsAppendOutcome::AlreadyCommitted(existing) }
                            else { FsAppendOutcome::IdempotencyConflict { committed_fingerprint: existing.fingerprint } };
                        seal_append_owner(&meter, &mut guard).await?;
                        return meter.finish(value).map_err(Into::into);
                    }
                    let actual = self.fork_snapshot(authority_id, &mut meter).await?.head;
                    let permit_rejected = conflicts.iter().any(|conflict| {
                        let path = match conflict { acyclic_stream::CommitConflict::Tail { path, .. }
                            | acyclic_stream::CommitConflict::Exists { path } => path };
                        path == &gate || lease_path.as_ref().is_some_and(|lease| path == lease)
                    });
                    if permit_rejected || actual.epoch != epoch { FsAppendOutcome::Fenced { actual_epoch: actual.epoch } }
                        else { FsAppendOutcome::Conflict { actual } }
                }
            },
            Err(AppendFailure::Authority(failure)) if matches!(authority_stream_error(&failure.error), Some(StreamError::IdempotencyMismatch)) => {
                let existing = self.authority_find_operation(authority_id, durable.operation_id, &mut meter).await?.0
                    .ok_or_else(|| authority_corrupt("Stream idempotency mismatch has no operation record", &meter))?;
                seal_append_owner(&meter, &mut guard).await?;
                FsAppendOutcome::IdempotencyConflict { committed_fingerprint: existing.fingerprint }
            }
            Err(AppendFailure::Authority(failure)) if matches!(authority_stream_error(&failure.error), Some(StreamError::DeadlineElapsed)) =>
                FsAppendOutcome::Fenced { actual_epoch: epoch },
            Err(failure) => return Err(failure),
        };
        meter.finish(value).map_err(Into::into)
    }
}

impl<P: acyclic_stream::StreamProvider> AsyncAuthorityStore for StreamAuthorityStore<P> {
    async fn create_authority(
        &self, authority_id: AuthorityId, genesis_epoch: Epoch, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<CreateAuthorityOutcome> {
        self.create(authority_id, genesis_epoch, ForkWork::new(budget, cancellation),
            None::<&mut fn(WorkBudget) -> std::future::Ready<crate::FsResult<()>>>).await
            .map(|receipt| AuthorityReceipt { value: receipt.value, work: receipt.work })
            .map_err(|failure| project_fork_authority_failure(failure).into())
    }

    async fn create_authority_guarded<F, Fut>(
        &self, authority_id: AuthorityId, genesis_epoch: Epoch, budget: WorkBudget,
        cancellation: &CancellationToken, guard: &mut F,
    ) -> crate::FsResult<CreateAuthorityOutcome>
    where F: FnMut(WorkBudget) -> Fut + ProviderTask, Fut: Future<Output = crate::FsResult<()>> + ProviderTask {
        self.create(authority_id, genesis_epoch, ForkWork::new(budget, cancellation), Some(guard))
            .await.map_err(Into::into)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn create_authority_with_native_work(
        &self, authority: AuthorityId, epoch: Epoch,
        original: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<CreateAuthorityOutcome, AuthorityStoreError> {
        self.create(authority, epoch, ForkWork::native(original, cancellation)?,
            None::<&mut fn(WorkBudget) -> std::future::Ready<crate::FsResult<()>>>).await
            .map(|receipt| receipt.value)
            .map_err(|failure| project_fork_authority_failure(failure).error)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn create_authority_guarded_with_native_work<'a, E, F, Fut>(
        &self, authority: AuthorityId, epoch: Epoch,
        original: &'a crate::NativeMaterializationWork, cancellation: &CancellationToken, guard: &mut F,
    ) -> Result<CreateAuthorityOutcome, E>
    where E: From<crate::FsError> + Send + 'static,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static {
        self.create(authority, epoch, ForkWork::native(original, cancellation).map_err(crate::FsError::from)?,
            OwnedAuthorityPublicationGuard(guard.clone())).await
            .map(|receipt| receipt.value).map_err(|failure| failure.error)
    }

    async fn create_authority_with_first_record(
        &self, authority: AuthorityId, commit: ProposedCommit, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<bool> {
        self.first_record(authority, commit, ForkWork::new(budget, cancellation),
            None::<&mut fn(WorkBudget) -> std::future::Ready<crate::FsResult<()>>>).await
            .map(|receipt| AuthorityReceipt { value: receipt.value, work: receipt.work })
            .map_err(|failure| project_fork_authority_failure(failure).into())
    }

    async fn create_authority_with_first_record_guarded<F, Fut>(
        &self, authority: AuthorityId, commit: ProposedCommit, budget: WorkBudget,
        cancellation: &CancellationToken, guard: &mut F,
    ) -> crate::FsResult<bool>
    where F: FnMut(WorkBudget) -> Fut + ProviderTask, Fut: Future<Output = crate::FsResult<()>> + ProviderTask {
        self.first_record(authority, commit, ForkWork::new(budget, cancellation), Some(guard))
            .await.map_err(Into::into)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn create_authority_with_first_record_with_native_work(
        &self,
        authority: AuthorityId,
        commit: ProposedCommit,
        original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<bool, AuthorityStoreError> {
        let meter = ForkWork::native(original, cancellation)?;
        self.first_record(authority, commit, meter,
            None::<&mut fn(WorkBudget) -> std::future::Ready<crate::FsResult<()>>>).await
            .map(|receipt| receipt.value)
            .map_err(|failure| project_fork_authority_failure(failure).error)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn create_authority_with_first_record_guarded_with_native_work<'a, E, F, Fut>(
        &self, authority: AuthorityId, commit: ProposedCommit,
        original: &'a crate::NativeMaterializationWork, cancellation: &CancellationToken, guard: &mut F,
    ) -> Result<bool, E>
    where E: From<crate::FsError> + Send + 'static,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static {
        self.first_record(authority, commit, ForkWork::native(original, cancellation).map_err(crate::FsError::from)?,
            OwnedAuthorityPublicationGuard(guard.clone())).await
            .map(|receipt| receipt.value).map_err(|failure| failure.error)
    }

    async fn fork_generation_authority(
        &self,
        source: crate::GenerationForkSource,
        destination_authority: AuthorityId,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<CreateAuthorityOutcome> {
        self.fork_generation(
            source, destination_authority, operation_id, ForkWork::new(budget, cancellation),
            None::<&mut fn(WorkBudget) -> std::future::Ready<crate::FsResult<()>>>,
        ).await
            .map(|receipt| AuthorityReceipt { value: receipt.value, work: receipt.work })
            .map_err(|failure| project_fork_authority_failure(failure).into())
    }

    async fn fork_generation_authority_guarded<F, Fut>(
        &self,
        source: crate::GenerationForkSource,
        destination_authority: AuthorityId,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        guard: &mut F,
    ) -> crate::FsResult<CreateAuthorityOutcome>
    where
        F: FnMut(WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = crate::FsResult<()>> + ProviderTask,
    {
        self.fork_generation(
            source, destination_authority, operation_id, ForkWork::new(budget, cancellation), Some(guard),
        ).await.map_err(Into::into)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn fork_generation_authority_with_native_work(
        &self, source: crate::GenerationForkSource, destination: AuthorityId, operation: OperationId,
        original: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<CreateAuthorityOutcome, AuthorityStoreError> {
        self.fork_generation(source, destination, operation,
            ForkWork::native(original, cancellation)?,
            None::<&mut fn(WorkBudget) -> std::future::Ready<crate::FsResult<()>>>).await
            .map(|receipt| receipt.value)
            .map_err(|failure| project_fork_authority_failure(failure).error)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn fork_generation_authority_guarded_with_native_work<'a, E, F, Fut>(
        &self, source: crate::GenerationForkSource, destination: AuthorityId, operation: OperationId,
        original: &'a crate::NativeMaterializationWork, cancellation: &CancellationToken, guard: &mut F,
    ) -> Result<CreateAuthorityOutcome, E>
    where E: From<crate::FsError> + Send + 'static,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static {
        self.fork_generation(source, destination, operation, ForkWork::native(original, cancellation).map_err(crate::FsError::from)?,
            OwnedAuthorityPublicationGuard(guard.clone())).await
            .map(|receipt| receipt.value).map_err(|failure| failure.error)
    }

    async fn commit_workspace_fork(
        &self, fork: WorkspaceForkCommit, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> AuthorityResult<WorkspaceForkOutcome> {
        self.workspace_fork(fork, PublicationPermit::Unrestricted, ForkWork::new(budget, cancellation),
            None::<&mut fn(WorkBudget) -> std::future::Ready<crate::FsResult<()>>>).await
            .map(|receipt| AuthorityReceipt { value: receipt.value, work: receipt.work })
            .map_err(|failure| project_fork_authority_failure(failure).into())
    }

    async fn commit_workspace_fork_guarded<F, Fut>(
        &self, fork: WorkspaceForkCommit, permit: PublicationPermit, budget: WorkBudget,
        cancellation: &CancellationToken, guard: &mut F,
    ) -> crate::FsResult<WorkspaceForkOutcome>
    where F: FnMut(WorkBudget) -> Fut + ProviderTask, Fut: Future<Output = crate::FsResult<()>> + ProviderTask {
        self.workspace_fork(fork, permit, ForkWork::new(budget, cancellation), Some(guard)).await.map_err(Into::into)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn commit_workspace_fork_with_native_work(
        &self, fork: WorkspaceForkCommit, original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<WorkspaceForkOutcome, AuthorityStoreError> {
        self.workspace_fork(fork, PublicationPermit::Unrestricted,
            ForkWork::native(original, cancellation)?,
            None::<&mut fn(WorkBudget) -> std::future::Ready<crate::FsResult<()>>>).await
            .map(|receipt| receipt.value)
            .map_err(|failure| project_fork_authority_failure(failure).error)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn commit_workspace_fork_guarded_with_native_work<'a, E, F, Fut>(
        &self, fork: WorkspaceForkCommit, permit: PublicationPermit,
        original: &'a crate::NativeMaterializationWork, cancellation: &CancellationToken, guard: &mut F,
    ) -> Result<WorkspaceForkOutcome, E>
    where E: From<crate::FsError> + Send + 'static,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static {
        self.workspace_fork(fork, permit, ForkWork::native(original, cancellation).map_err(crate::FsError::from)?,
            OwnedAuthorityPublicationGuard(guard.clone())).await
            .map(|receipt| receipt.value).map_err(|failure| failure.error)
    }

    fn supports_generation_lineage_prefix(&self) -> bool {
        true
    }

    async fn head(
        &self,
        authority_id: AuthorityId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<Head> {
        let mut meter = ForkWork::new(budget, cancellation);
        let value = self.fork_snapshot(authority_id, &mut meter).await?.head;
        meter.finish(value).map_err(Into::into)
    }

    /// Retires filesystem authority metadata while preserving historical records.
    async fn retire_authority(
        &self,
        authority_id: AuthorityId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<()> {
        cancellation
            .check()
            .map_err(|_| OperationFailure::before_work(AuthorityStoreError::Cancelled))?;
        let root = stream_path(authority_id, "").map_err(OperationFailure::before_work)?;
        let path = stream_path(authority_id, "/retirement").map_err(OperationFailure::before_work)?;
        let mut work = authority_read_work(1);
        admit_authority(work, budget)?;
        match self.provider.tail(root).await {
            Err(acyclic_stream::StreamError::NotFound) => {
                return authority_success((), work, budget).map_err(Into::into);
            }
            Ok(_) => {}
            Err(error) => return Err(OperationFailure::new(map_stream_error(error), work)),
        }
        work = work
            .checked_add(authority_write_work(1, AUTHORITY_RETIRED.len() as u64))
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        admit_authority(work, budget)?;
        let request = acyclic_stream::AppendRequest {
            path,
            records: vec![acyclic_stream::OwnedBytes::from_static(AUTHORITY_RETIRED)],
            if_tail: Some(0),
            idempotency_key: Some(
                stream_key(b"retire-authority", &authority_id.into_bytes())
                    .map_err(OperationFailure::before_work)?,
            ),
        };
        match self.provider.append(request).await {
            Ok(acyclic_stream::AppendOutcome::Committed(_)) => authority_success((), work, budget).map_err(Into::into),
            Ok(acyclic_stream::AppendOutcome::TailConflict { .. }) => {
                work = work
                    .checked_add(authority_read_work(2))
                    .map_err(|error| OperationFailure::new(error.into(), work))?;
                admit_authority(work, budget)?;
                if self
                    .is_retired(authority_id)
                    .await
                    .map_err(|error| OperationFailure::new(error, work))?
                {
                    authority_success((), work, budget).map_err(Into::into)
                } else {
                    Err(OperationFailure::new(
                        AuthorityStoreError::Corrupt(
                            "filesystem retirement marker conflicted without retirement".to_owned(),
                        ),
                        work,
                    ))
                }
            }
            Err(error) => Err(OperationFailure::new(map_stream_error(error), work)),
        }
    }

    async fn compare_and_append(
        &self,
        authority_id: AuthorityId,
        epoch: Epoch,
        expected: Head,
        commit: ProposedCommit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<FsAppendOutcome> {
        self.compare_and_append_guarded(
            crate::GuardedAppend {
                authority_id,
                epoch,
                expected,
                commit,
                permit: PublicationPermit::Unrestricted,
            },
            budget,
            cancellation,
        )
        .await
    }

    async fn compare_and_append_guarded(
        &self, request: crate::GuardedAppend, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<FsAppendOutcome> {
        self.append(request, ForkWork::new(budget, cancellation),
            None::<&mut fn() -> std::future::Ready<Result<(), ForkFailure>>>).await
            .map_err(|failure| match failure {
                AppendFailure::Authority(error) | AppendFailure::Owner(error) => error.into(),
            })
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn compare_and_append_guarded_with_native_work(
        &self, request: crate::GuardedAppend, original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(FsAppendOutcome, crate::NativeAllocation), AuthorityStoreError> {
        let mut meter = ForkWork::native(original, cancellation)?;
        let (_, allocation) = meter.vec_owned::<u8>(0).map_err(|failure| failure.error)?;
        let allocation = allocation.ok_or(AuthorityStoreError::RejectedStatic("native append lost original output custody"))?;
        self.append(request, meter,
            None::<&mut fn() -> std::future::Ready<Result<(), AuthorityStoreError>>>).await
            .map(|receipt| (receipt.value, allocation))
            .map_err(|failure| match failure {
                AppendFailure::Authority(error) => error.error,
                AppendFailure::Owner(error) => error,
            })
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn compare_and_append_guarded_with_native_owner<F, Fut, E>(
        &self, request: crate::GuardedAppend, original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken, guard: &mut F,
    ) -> Result<(FsAppendOutcome, crate::NativeAllocation), E>
    where F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static, E: From<AuthorityStoreError> + Send + 'static {
        let mut meter = ForkWork::native(original, cancellation).map_err(E::from)?;
        let (_, allocation) = meter.vec_owned::<u8>(0).map_err(|failure| E::from(failure.error))?;
        let allocation = allocation.ok_or_else(|| E::from(
            AuthorityStoreError::RejectedStatic("native append lost original output custody")))?;
        let original = original.clone();
        let mut callback = guard.clone();
        let seal = OwnedAuthorityPublicationGuard(move || callback(&original));
        self.append(request, meter, seal).await
            .map(|receipt| (receipt.value, allocation))
            .map_err(|failure| match failure {
                AppendFailure::Authority(error) => E::from(error.error),
                AppendFailure::Owner(error) => error,
            })
    }

    async fn publication_reservation(
        &self, authority: AuthorityId, operation: OperationId, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<Option<PublicationReservation>> {
        self.reservation(authority, operation, ForkWork::new(budget, cancellation)).await
            .map_err(Into::into)
    }

    async fn reserve_publication(
        &self, authority: AuthorityId, expected: Head, operation: OperationId,
        budget: WorkBudget, cancellation: &CancellationToken,
    ) -> AuthorityResult<ReservationOutcome> {
        self.reserve(authority, expected, operation, ForkWork::new(budget, cancellation)).await
            .map_err(Into::into)
    }

    async fn release_publication(
        &self, reservation: PublicationReservation, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<()> {
        self.release(reservation, ForkWork::new(budget, cancellation)).await
            .map_err(Into::into)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn publication_reservation_with_native_work(
        &self, authority: AuthorityId, operation: OperationId,
        original: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<Option<PublicationReservation>, AuthorityStoreError> {
        self.reservation(authority, operation, ForkWork::native(original, cancellation)?).await
            .map(|receipt| receipt.value).map_err(|failure| failure.error)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn reserve_publication_with_native_work(
        &self, authority: AuthorityId, expected: Head, operation: OperationId,
        original: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<PublicationReservation, AuthorityStoreError> {
        match self.reserve(authority, expected, operation, ForkWork::native(original, cancellation)?).await
            .map_err(|failure| failure.error)?.value {
            ReservationOutcome::Reserved(proof) | ReservationOutcome::AlreadyReserved(proof) => Ok(proof),
            ReservationOutcome::Conflict { .. } => Err(AuthorityStoreError::RejectedStatic("publication reservation head or gate changed")),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn release_publication_with_native_work(
        &self, reservation: PublicationReservation,
        original: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<(), AuthorityStoreError> {
        self.release(reservation, ForkWork::native(original, cancellation)?).await
            .map(|receipt| receipt.value).map_err(|failure| failure.error)
    }

    async fn replay(
        &self,
        authority_id: AuthorityId,
        after: Sequence,
        limit: ReplayLimit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<Vec<DurableCommit>> {
        let mut meter = ForkWork::new(budget, cancellation);
        let output = self.authority_replay(authority_id, after, limit, &mut meter, ReadKind::Replay).await?;
        meter.finish(output.0).map_err(Into::into)
    }

    async fn fence(
        &self,
        authority_id: AuthorityId,
        expected: Head,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<FenceOutcome> {
        let mut meter = ForkWork::new(budget, cancellation);
        let value = self.authority_fence(authority_id, expected, &mut meter).await?;
        meter.finish(value).map_err(Into::into)
    }

    async fn find_operation(
        &self,
        authority_id: AuthorityId,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<Option<DurableCommit>> {
        let mut meter = ForkWork::new(budget, cancellation);
        let output = self.authority_find_operation(authority_id, operation_id, &mut meter).await?;
        meter.finish(output.0).map_err(Into::into)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn head_with_native_work(
        &self, authority: AuthorityId, work: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<Head, AuthorityStoreError> {
        let mut meter = ForkWork::native(work, cancellation)?;
        self.fork_snapshot(authority, &mut meter).await.map(|snapshot| snapshot.head).map_err(|failure| failure.error)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn replay_with_native_work(
        &self, authority: AuthorityId, after: Sequence, limit: ReplayLimit,
        work: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<(Vec<DurableCommit>, crate::NativeAllocation), AuthorityStoreError> {
        if limit.records == 0 || limit.payload_bytes == 0 {
            return Err(AuthorityStoreError::InvalidReplayLimit);
        }
        let mut meter = ForkWork::native(work, cancellation)?;
        let output = self.authority_replay(authority, after, limit, &mut meter, ReadKind::Replay).await
            .map_err(|failure| failure.error)?;
        Ok((output.0, output.1.expect("native output vector retains original custody")))
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn fence_with_native_work(
        &self, authority: AuthorityId, expected: Head, work: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<FenceOutcome, AuthorityStoreError> {
        let mut meter = ForkWork::native(work, cancellation)?;
        self.authority_fence(authority, expected, &mut meter).await.map_err(|failure| failure.error)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn find_operation_with_native_work(
        &self, authority: AuthorityId, operation: OperationId, work: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(Option<DurableCommit>, crate::NativeAllocation), AuthorityStoreError> {
        let mut meter = ForkWork::native(work, cancellation)?;
        let output = self.authority_find_operation(authority, operation, &mut meter).await
            .map_err(|failure| failure.error)?;
        Ok((output.0, output.1.expect("native output retains original custody")))
    }
}

/// The object an [`object_key`] names.
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub(crate) fn object_id_from_key(key: &str) -> Option<ObjectId> {
    let (tag, digest) = key.strip_prefix("fs/v1/")?.split_once('/')?;
    let kind = crate::storage::ObjectKind::from_canonical_tag(tag.parse().ok()?).ok()?;
    let digest = <[u8; 32]>::try_from(hex::decode(digest).ok()?).ok()?;
    let object = ObjectId {
        kind,
        digest: crate::foundation::Digest::from_bytes(digest),
    };
    (object_key(object) == key).then_some(object)
}

pub(crate) fn object_key(object_id: ObjectId) -> String {
    format!(
        "fs/v1/{}/{}",
        object_id.kind.canonical_tag(),
        hex::encode(object_id.digest.as_bytes())
    )
}

/// The authority a direct child of `fs/authorities` names.
fn authority_child(child: &acyclic_stream::StreamPath) -> Result<AuthorityId, AuthorityStoreError> {
    let encoded = child
        .as_str()
        .strip_prefix("fs/authorities/")
        .ok_or_else(|| {
            AuthorityStoreError::Corrupt("Stream authority child escaped its parent".to_owned())
        })?;
    if encoded.len() != 32 || encoded.contains('/') {
        return Err(AuthorityStoreError::Corrupt(
            "Stream authority child has a noncanonical identity".to_owned(),
        ));
    }
    let mut bytes = [0_u8; 16];
    hex::decode_to_slice(encoded, &mut bytes).map_err(|_| {
        AuthorityStoreError::Corrupt(
            "Stream authority child has a noncanonical identity".to_owned(),
        )
    })?;
    Ok(AuthorityId::from_bytes(bytes))
}

fn stream_path(authority: AuthorityId, suffix: &str) -> Result<acyclic_stream::StreamPath, AuthorityStoreError> {
    acyclic_stream::StreamPath::new(format!("fs/authorities/{}{suffix}", hex::encode(authority.into_bytes())))
        .map_err(map_stream_error)
}

fn decode_publication_gate(value: &[u8]) -> Result<Option<OperationId>, AuthorityStoreError> {
    if value == PUBLICATION_GATE_FREE { return Ok(None); }
    let bytes = record_bytes::<16>(value, PUBLICATION_GATE_ACTIVE)
        .ok_or_else(|| AuthorityStoreError::Corrupt("invalid publication gate record".to_owned()))?;
    Ok(Some(OperationId::from_bytes(*bytes)))
}

fn stream_key(
    domain: &[u8],
    identity: &[u8],
) -> Result<acyclic_stream::IdempotencyKey, AuthorityStoreError> {
    let mut value = Vec::with_capacity(
        domain
            .len()
            .saturating_add(identity.len())
            .saturating_add(1),
    );
    value.extend_from_slice(domain);
    value.push(0);
    value.extend_from_slice(identity);
    acyclic_stream::IdempotencyKey::new(Bytes::from(value)).map_err(map_stream_error)
}

fn reservation_identity(authority: AuthorityId, operation: OperationId, tail: u64) -> [u8; 40] {
    let mut identity = [0_u8; 40];
    identity[..16].copy_from_slice(&authority.into_bytes());
    identity[16..32].copy_from_slice(&operation.into_bytes());
    identity[32..].copy_from_slice(&tail.to_le_bytes());
    identity
}

/// The original acquisition/release request, with caller-transferred gate custody.
fn reservation_request(
    authority: AuthorityId,
    snapshot: Option<AuthoritySnapshot>,
    operation: OperationId,
    gate_tail: u64,
    gate: Option<acyclic_stream::StreamPath>,
    meter: &mut ForkWork<'_>,
) -> Result<acyclic_stream::CommitRequest, ForkFailure> {
    use acyclic_stream::{CommitCondition, CommitMutation};
    let gate = match gate {
        Some(gate) => gate,
        None => meter.path(authority, "/publication-gate", None)?,
    };
    let mut conditions = meter.vec::<CommitCondition>(if snapshot.is_some() { 4 } else { 2 })?;
    if let Some(snapshot) = snapshot {
        let epochs = meter.path(authority, "/epochs", None)?;
        meter.push(&mut conditions, CommitCondition::Tail { path: epochs, expected: snapshot.epoch_tail })?;
        let gate_condition = meter.clone_path(&gate)?;
        meter.push(&mut conditions, CommitCondition::Tail { path: gate_condition, expected: gate_tail })?;
        let records = meter.path(authority, "/records", None)?;
        meter.push(&mut conditions, CommitCondition::Tail { path: records, expected: (snapshot.head.sequence.get() + 1) })?;
    } else {
        let gate_condition = meter.clone_path(&gate)?;
        meter.push(&mut conditions, CommitCondition::Tail { path: gate_condition, expected: gate_tail })?;
    }
    let retirement = meter.path(authority, "/retirement", None)?;
    meter.push(&mut conditions, CommitCondition::Absent { path: retirement })?;
    let mut records = meter.vec::<acyclic_stream::OwnedBytes>(1)?;
    let body = if snapshot.is_none() { acyclic_stream::OwnedBytes::from_static(PUBLICATION_GATE_FREE) }
        else { meter.stream_bytes(PUBLICATION_GATE_ACTIVE, &[&operation.into_bytes()])? };
    meter.push(&mut records, body)?;
    let mut mutations = meter.vec::<CommitMutation>(1)?;
    meter.push(&mut mutations, CommitMutation::Append { path: gate, records })?;
    let copies = WorkCounters { bytes_copied: 40, ..WorkCounters::UNCHARGED };
    if meter.is_native() { meter.admit(copies)?; }
    let identity = reservation_identity(authority, operation, gate_tail);
    if meter.is_native() { meter.charge(copies)?; }
    let key = meter.stream_key(if snapshot.is_none() { b"release-publication" } else { b"reserve-publication" }, &identity)?;
    Ok(acyclic_stream::CommitRequest { conditions, mutations, idempotency_key: key })
}

/// Hashes the already canonical, fixed-shape request using the Stream producer.
fn reservation_request_digest(
    request: &acyclic_stream::CommitRequest,
    meter: &mut ForkWork<'_>,
) -> Result<[u8; 32], ForkFailure> {
    let mut bytes = (b"acyclic.stream.request.v1\0".len() + b"commit".len()) as u64;
    for condition in &request.conditions {
        let (path, scalar) = match condition {
            acyclic_stream::CommitCondition::Tail { path, .. } => (path, 8),
            acyclic_stream::CommitCondition::Absent { path } => (path, 0),
        };
        bytes = bytes.checked_add(1 + 8 + path.as_str().len() as u64 + scalar)
            .ok_or_else(|| ForkFailure::new(crate::WorkError::Overflow.into(), meter.work))?;
    }
    for mutation in &request.mutations {
        let acyclic_stream::CommitMutation::Append { path, records } = mutation else {
            return Err(authority_corrupt("reservation request contains a non-append mutation", meter));
        };
        bytes = bytes.checked_add(1 + 8 + path.as_str().len() as u64 + 8)
            .ok_or_else(|| ForkFailure::new(crate::WorkError::Overflow.into(), meter.work))?;
        for record in records {
            bytes = bytes.checked_add(8 + record.len() as u64)
                .ok_or_else(|| ForkFailure::new(crate::WorkError::Overflow.into(), meter.work))?;
        }
    }
    let hashing = WorkCounters { bytes_hashed: bytes, ..WorkCounters::default() };
    meter.admit(hashing)?;
    let digest = acyclic_stream::request::commit_digest(request);
    meter.charge(hashing)?;
    Ok(digest)
}

impl<P: acyclic_stream::StreamProvider> StreamAuthorityStore<P> {
    async fn reservation(
        &self, authority_id: AuthorityId, operation_id: OperationId, mut meter: ForkWork<'_>,
    ) -> Result<AuthorityReceipt<Option<PublicationReservation>>, ForkFailure> {
        meter.check()?;
        let (gate, gate_tail, active) = self.reservation_gate(authority_id, &mut meter).await?;
        let gate_bytes = gate.as_str().len() as u64;
        drop(gate);
        meter.release(gate_bytes)?;
        if active != Some(operation_id) { return meter.finish(None).map_err(Into::into); }
        let unavailable = |work| ForkFailure::new(
            AuthorityStoreError::ReservationOriginalHeadUnavailable { authority_id, operation_id, gate_tail }, work);
        let prior_gate = gate_tail.checked_sub(1).ok_or_else(|| unavailable(meter.work))?;
        let (observed, _retained_key_bytes) = self.reservation_observation(
            authority_id, operation_id, prior_gate, &mut meter,
        ).await?;
        let observation = observed.as_ref().ok_or_else(|| unavailable(meter.work))?;
        if !self.qualified_reservation_observation(observation, authority_id, operation_id, gate_tail, &mut meter)? {
            return Err(unavailable(meter.work));
        }
        let snapshot = self.fork_snapshot(authority_id, &mut meter).await?;
        let mut request = reservation_request(
            authority_id, Some(snapshot), operation_id, prior_gate, None, &mut meter,
        )?;
        let mut original_tails = None;
        'candidate: for record_tail in (1..=(snapshot.head.sequence.get() + 1)).rev() {
            for epoch_tail in (1..=snapshot.epoch_tail).rev() {
                let candidate_work = WorkCounters {
                    items_examined: 1, bytes_copied: if meter.is_native() { 16 } else { 0 },
                    ..WorkCounters::UNCHARGED
                };
                meter.admit(candidate_work)?;
                meter.charge(candidate_work)?;
                for condition in &mut request.conditions {
                    if let acyclic_stream::CommitCondition::Tail { path, expected } = condition {
                        if path.as_str().ends_with("/records") { *expected = record_tail; }
                        else if path.as_str().ends_with("/epochs") { *expected = epoch_tail; }
                    }
                }
                if reservation_request_digest(&request, &mut meter)? == observation.request_digest {
                    original_tails = Some((record_tail, epoch_tail));
                    break 'candidate;
                }
            }
        }
        let (record_tail, epoch_tail) = original_tails.ok_or_else(|| unavailable(meter.work))?;
        drop(request);
        drop(observed);
        meter.release_opaque(acyclic_stream::MAX_COMMAND_BYTES as u64)?;
        meter.release_request()?;
        let epochs = meter.path(authority_id, "/epochs", None)?;
        let epochs_bytes = epochs.as_str().len() as u64;
        let epoch_record = self.record(epochs, epoch_tail - 1, &mut meter).await
            .map_err(|failure| if authority_missing(&failure.error) && !meter.is_native() {
                unavailable(failure.work)
            } else { failure })?;
        meter.release(epochs_bytes)?;
        let epoch = meter.decode_epoch(&epoch_record.value, EPOCH_DOMAIN)?;
        drop(epoch_record);
        let records = meter.path(authority_id, "/records", None)?;
        let records_bytes = records.as_str().len() as u64;
        let record = self.record(records, record_tail - 1, &mut meter).await
            .map_err(|failure| if authority_missing(&failure.error) && !meter.is_native() {
                unavailable(failure.work)
            } else { failure })?;
        meter.release(records_bytes)?;
        let original_head = if record_tail == 1 {
            meter.decode_epoch(&record.value, GENESIS_DOMAIN)?;
            Head::genesis(epoch)
        } else {
            let durable = if meter.is_native() {
                meter.decode_durable(&record.value)?
            } else {
                StreamsDurableRecord::decode_borrowed(&record.value, STREAM_RECORD_LIMIT)
                    .map_err(|error| ForkFailure::new(AuthorityStoreError::Corrupt(error.to_string()), meter.work))?
            };
            let digest = meter.durable_digest(authority_id, &durable)?;
            if digest != durable.digest || durable.sequence.get().checked_add(1) != Some(record_tail) {
                return Err(unavailable(meter.work));
            }
            Head { epoch, sequence: durable.sequence, digest: durable.digest }
        };
        drop(record);
        // Reconfirm the exact existing gate slot after historical proof reads.
        let (gate, current_tail, active) = self.reservation_gate(authority_id, &mut meter).await?;
        let gate_bytes = gate.as_str().len() as u64;
        drop(gate);
        meter.release(gate_bytes)?;
        if current_tail != gate_tail || active != Some(operation_id) { return meter.finish(None).map_err(Into::into); }
        meter.finish(Some(PublicationReservation {
            authority_id, operation_id, expected: original_head, gate_tail,
        })).map_err(Into::into)
    }

    async fn reserve(
        &self, authority_id: AuthorityId, expected: Head, operation_id: OperationId,
        mut meter: ForkWork<'_>,
    ) -> Result<AuthorityReceipt<ReservationOutcome>, ForkFailure> {
        meter.check()?;
        let snapshot = self.fork_snapshot(authority_id, &mut meter).await?;
        if snapshot.head != expected {
            return meter.finish(ReservationOutcome::Conflict { actual: snapshot.head }).map_err(Into::into);
        }
        let (gate, gate_tail, active) = self.reservation_gate(authority_id, &mut meter).await?;
        if active == Some(operation_id) {
            let gate_bytes = gate.as_str().len() as u64;
            drop(gate);
            meter.release(gate_bytes)?;
            if !self.reservation_matches_original(
                authority_id, snapshot, operation_id, gate_tail, &mut meter,
            ).await? {
                return Err(ForkFailure::new(
                    AuthorityStoreError::ReservationOriginalHeadUnavailable {
                        authority_id, operation_id, gate_tail,
                    },
                    meter.work,
                ));
            }
            meter.release_request()?;
            return meter.finish(ReservationOutcome::AlreadyReserved(PublicationReservation {
                authority_id, operation_id, expected, gate_tail,
            })).map_err(Into::into);
        }
        if active.is_some() {
            drop(gate);
            meter.release_request()?;
            return meter.finish(ReservationOutcome::Conflict { actual: snapshot.head }).map_err(Into::into);
        }
        let next_gate_tail = gate_tail.checked_add(1)
            .ok_or_else(|| ForkFailure::new(crate::WorkError::Overflow.into(), meter.work))?;
        let request = reservation_request(
            authority_id, Some(snapshot), operation_id, gate_tail, Some(gate), &mut meter,
        )?;
        let result = self.reservation_commit(request, &mut meter).await?;
        match &*result {
            acyclic_stream::CommitOutcome::Committed(_) => {
                let value = ReservationOutcome::Reserved(PublicationReservation {
                    authority_id, operation_id, expected, gate_tail: next_gate_tail,
                });
                if meter.is_native() {
                    return Ok(AuthorityReceipt { value, work: meter.spent() });
                }
                meter.finish(value)
            }
            acyclic_stream::CommitOutcome::Conflict(_) => {
                let actual = self.fork_snapshot(authority_id, &mut meter).await?.head;
                meter.finish(ReservationOutcome::Conflict { actual })
            }
        }
    }

    async fn release(
        &self, reservation: PublicationReservation, mut meter: ForkWork<'_>,
    ) -> Result<AuthorityReceipt<()>, ForkFailure> {
        meter.check()?;
        let copies = WorkCounters { bytes_copied: 40, ..WorkCounters::UNCHARGED };
        meter.admit(copies)?;
        let identity = reservation_identity(reservation.authority_id, reservation.operation_id, reservation.gate_tail);
        meter.charge(copies)?;
        // Resolve a lost release acknowledgement before consulting a newer gate.
        let (observed, retained_bytes) = self.reservation_inspect(
            b"release-publication", &identity, &mut meter,
        ).await?;
        if let Some(observation) = observed.as_ref() {
            let retained_key = observation.idempotency_key.as_bytes();
            const DOMAIN: &[u8] = b"release-publication\0";
            if retained_key.len() != DOMAIN.len() + identity.len()
                || !retained_key.starts_with(DOMAIN) || retained_key[DOMAIN.len()..] != identity
            {
                return Err(authority_corrupt("publication release observation has another retry identity", &meter));
            }
            match &observation.outcome {
                acyclic_stream::IdempotencyOutcome::Commit(acyclic_stream::CommitOutcome::Committed(envelope)) => {
                    let request = reservation_request(
                        reservation.authority_id, None, reservation.operation_id,
                        reservation.gate_tail, None, &mut meter,
                    )?;
                    let digest = reservation_request_digest(&request, &mut meter)?;
                    let acyclic_stream::CommitMutation::Append { path: gate, .. } = &request.mutations[0]
                        else { unreachable!("release constructor produces one append"); };
                    let end = reservation.gate_tail.checked_add(1)
                        .ok_or_else(|| ForkFailure::new(crate::WorkError::Overflow.into(), meter.spent()))?;
                    let valid = observation.request_digest == digest
                        && matches!(envelope.mutations.as_slice(), [acyclic_stream::CommittedMutation::Append(append)]
                            if &append.path == gate && append.start == reservation.gate_tail
                                && append.end == end && append.tail == end
                                && matches!(append.records.as_slice(), [record]
                                    if record.sequence == reservation.gate_tail && record.commit_id == envelope.commit_id
                                        && record.value.as_ref() == PUBLICATION_GATE_FREE));
                    if !valid {
                        return Err(authority_corrupt("publication release observation does not bind the original gate release", &meter));
                    }
                    drop(request);
                }
                acyclic_stream::IdempotencyOutcome::Commit(acyclic_stream::CommitOutcome::Conflict(_)) =>
                    return Err(ForkFailure::new(AuthorityStoreError::RejectedStatic("publication reservation is no longer active"), meter.spent())),
                _ => return Err(authority_corrupt("publication release identity retained another Stream mutation family", &meter)),
            }
            drop(observed);
            meter.release(retained_bytes)?;
            meter.release_request()?;
            meter.release_opaque(acyclic_stream::MAX_COMMAND_BYTES as u64)?;
            return meter.finish(());
        }
        drop(observed);
        let (gate, current_tail, active) = self.reservation_gate(reservation.authority_id, &mut meter).await?;
        if current_tail != reservation.gate_tail || active != Some(reservation.operation_id) {
            return Err(ForkFailure::new(AuthorityStoreError::RejectedStatic(
                "publication reservation is no longer owned by this operation"), meter.spent()));
        }
        let request = reservation_request(
            reservation.authority_id, None, reservation.operation_id,
            reservation.gate_tail, Some(gate), &mut meter,
        )?;
        let result = self.reservation_commit(request, &mut meter).await?;
        match &*result {
            acyclic_stream::CommitOutcome::Committed(_) if meter.is_native() =>
                Ok(AuthorityReceipt { value: (), work: meter.spent() }),
            acyclic_stream::CommitOutcome::Committed(_) => meter.finish(()),
            acyclic_stream::CommitOutcome::Conflict(_) => Err(ForkFailure::new(
                AuthorityStoreError::RejectedStatic("publication reservation is no longer active"), meter.spent())),
        }
    }

    async fn reservation_commit(
        &self, request: acyclic_stream::CommitRequest, meter: &mut ForkWork<'_>,
    ) -> Result<AuthorityProviderOutput<acyclic_stream::CommitOutcome>, ForkFailure> {
        let writes = meter.request_write_work(&request)?;
        meter.admit(writes)?;
        meter.check()?;
        #[cfg(not(target_arch = "wasm32"))]
        if meter.is_native() {
            let result = self.provider.native_commit(request, meter.native_meter()).await;
            meter.release_request()?;
            let result = meter.result(result)?;
            if matches!(result.value(), acyclic_stream::CommitOutcome::Committed(_)) {
                meter.charge(writes)?;
            }
            // Return the acquired proof before any late cancellation/head check.
            return Ok(AuthorityProviderOutput::Native(result));
        }
        let result = self.provider.commit(request).await;
        meter.charge(commit_work(writes, &result))?;
        meter.release_request()?;
        result.map(AuthorityProviderOutput::Plain)
            .map_err(|error| ForkFailure::new(map_stream_error(error), meter.spent()))
    }

    async fn reservation_gate(
            &self,
            authority: AuthorityId,
            meter: &mut ForkWork<'_>,
        ) -> Result<(acyclic_stream::StreamPath, u64, Option<OperationId>), ForkFailure> {
            let gate = meter.path(authority, "/publication-gate", None)?;
            let tail = self.tail(meter.clone_path(&gate)?, meter).await?;
            if tail == 0 {
                return Err(authority_corrupt("authority publication gate is empty", meter));
            }
            let record = self.record(meter.clone_path(&gate)?, tail - 1, meter).await?;
            let active = meter.decode_gate(&record.value)?;
            Ok((gate, tail, active))
        }

    async fn reservation_observation(
        &self, authority: AuthorityId, operation: OperationId, prior_gate_tail: u64,
        meter: &mut ForkWork<'_>,
    ) -> Result<(AuthorityProviderOutput<Option<acyclic_stream::IdempotencyObservation>>, u64), ForkFailure> {
        let copies = WorkCounters { bytes_copied: 40, ..WorkCounters::UNCHARGED };
        if meter.is_native() { meter.admit(copies)?; }
        let identity = reservation_identity(authority, operation, prior_gate_tail);
        if meter.is_native() { meter.charge(copies)?; }
        self.reservation_inspect(b"reserve-publication", &identity, meter).await
    }

    async fn reservation_inspect(
        &self, domain: &[u8], identity: &[u8], meter: &mut ForkWork<'_>,
    ) -> Result<(AuthorityProviderOutput<Option<acyclic_stream::IdempotencyObservation>>, u64), ForkFailure> {
        let baseline = if meter.is_native() { 0 } else { meter.allocations.live_bytes() };
        let key = meter.stream_key(domain, identity)?;
        let key_bytes = if meter.is_native() { 0 } else {
            meter.allocations.live_bytes().checked_sub(baseline)
                .ok_or_else(|| ForkFailure::new(crate::WorkError::Overflow.into(), meter.spent()))?
        };
        let key_pointer = key.as_bytes().as_ptr() as usize;
        let ceiling = acyclic_stream::MAX_COMMAND_BYTES as u64;
        let record_ceiling = (acyclic_stream::MAX_ITEMS as u64).checked_mul(acyclic_stream::MAX_ITEMS as u64)
            .ok_or_else(|| ForkFailure::new(crate::WorkError::Overflow.into(), meter.spent()))?;
        let peak = if meter.is_native() { 0 } else {
            meter.allocations.live_bytes().checked_add(ceiling)
                .ok_or_else(|| ForkFailure::new(crate::WorkError::Overflow.into(), meter.spent()))?
        };
        meter.admit(WorkCounters {
            backend_read_operations: u64::from(!meter.is_native()),
            authority_records_read: record_ceiling, authority_bytes_read: ceiling,
            peak_allocation_bytes: peak, ..WorkCounters::UNCHARGED
        })?;
        let result = 'inspect: {
            #[cfg(not(target_arch = "wasm32"))]
            if meter.is_native() {
                let result = self.provider.native_inspect_idempotency(key, meter.native_meter()).await;
                break 'inspect meter.result(result).map(AuthorityProviderOutput::Native);
            }
            self.provider.inspect_idempotency(key).await.map(AuthorityProviderOutput::Plain)
                .map_err(|error| ForkFailure::new(map_stream_error(error), meter.spent()))
        };
        let mut actual = WorkCounters { backend_read_operations: u64::from(!meter.is_native()), ..WorkCounters::UNCHARGED };
        if let Ok(output) = &result
            && let Some(observation) = &**output
            && let acyclic_stream::IdempotencyOutcome::Commit(acyclic_stream::CommitOutcome::Committed(envelope)) = &observation.outcome
        {
            for mutation in &envelope.mutations {
                let records = match mutation {
                    acyclic_stream::CommittedMutation::Append(append) => &append.records,
                    acyclic_stream::CommittedMutation::Fork(fork) => &fork.records,
                };
                actual.authority_records_read = actual.authority_records_read.checked_add(records.len() as u64)
                    .ok_or_else(|| ForkFailure::new(crate::WorkError::Overflow.into(), meter.spent()))?;
                for record in records {
                    actual.authority_bytes_read = actual.authority_bytes_read.checked_add(record.value.len() as u64)
                        .ok_or_else(|| ForkFailure::new(crate::WorkError::Overflow.into(), meter.spent()))?;
                }
            }
        }
        meter.charge(actual)?;
        let result = result?;
        let retained_bytes = match &*result {
            Some(observation) if observation.idempotency_key.as_bytes().as_ptr() as usize == key_pointer => key_bytes,
            _ => { meter.release(key_bytes)?; 0 }
        };
        if result.is_some() { meter.hold_opaque(ceiling)?; }
        Ok((result, retained_bytes))
    }

    fn qualified_reservation_observation(
        &self, observation: &acyclic_stream::IdempotencyObservation,
        authority: AuthorityId, operation: OperationId, gate_tail: u64,
        meter: &mut ForkWork<'_>,
    ) -> Result<bool, ForkFailure> {
        let Some(prior) = gate_tail.checked_sub(1) else { return Ok(false); };
        let copies = WorkCounters { bytes_copied: 40, ..WorkCounters::UNCHARGED };
        if meter.is_native() { meter.admit(copies)?; }
        let identity = reservation_identity(authority, operation, prior);
        if meter.is_native() { meter.charge(copies)?; }
        let key = observation.idempotency_key.as_bytes();
        let domain = b"reserve-publication";
        if key.len() != domain.len() + 1 + identity.len() || !key.starts_with(domain)
            || key[domain.len()] != 0 || key[domain.len() + 1..] != identity {
            return Ok(false);
        }
        let acyclic_stream::IdempotencyOutcome::Commit(acyclic_stream::CommitOutcome::Committed(envelope)) = &observation.outcome else { return Ok(false); };
        let [acyclic_stream::CommittedMutation::Append(append)] = envelope.mutations.as_slice() else { return Ok(false); };
        let path = append.path.as_str();
        let prefix = b"fs/authorities/";
        let encoding = WorkCounters { bytes_encoded: 32, ..WorkCounters::UNCHARGED };
        if meter.is_native() { meter.admit(encoding)?; }
        let mut encoded_authority = [0_u8; 32];
        hex::encode_to_slice(authority.into_bytes(), &mut encoded_authority).expect("fixed authority hex width");
        if meter.is_native() { meter.charge(encoding)?; }
        if path.len() != prefix.len() + 32 + b"/publication-gate".len()
            || !path.as_bytes().starts_with(prefix)
            || path.as_bytes()[prefix.len()..prefix.len() + 32] != encoded_authority
            || !path.ends_with("/publication-gate")
            || append.start != prior || append.end != gate_tail || append.tail != gate_tail {
            return Ok(false);
        }
        let [record] = append.records.as_slice() else { return Ok(false); };
        Ok(record.sequence == prior && record.commit_id == envelope.commit_id
            && record.value.len() == PUBLICATION_GATE_ACTIVE.len() + 16
            && record.value.starts_with(PUBLICATION_GATE_ACTIVE)
            && record.value[PUBLICATION_GATE_ACTIVE.len()..] == operation.into_bytes())
    }

    async fn reservation_matches_original(
            &self,
            authority: AuthorityId,
            snapshot: AuthoritySnapshot,
            operation: OperationId,
            gate_tail: u64,
            meter: &mut ForkWork<'_>,
        ) -> Result<bool, ForkFailure> {
            let Some(prior) = gate_tail.checked_sub(1) else { return Ok(false); };
            let (observed, retained_bytes) = self.reservation_observation(authority, operation, prior, meter).await?;
            let Some(observation) = observed.as_ref() else { return Ok(false); };
            if !self.qualified_reservation_observation(observation, authority, operation, gate_tail, meter)? {
                drop(observed);
                meter.release(retained_bytes)?;
                meter.release_opaque(acyclic_stream::MAX_COMMAND_BYTES as u64)?;
                return Ok(false);
            }
            let baseline = if meter.is_native() { 0 } else { meter.allocations.live_bytes() };
            let request = reservation_request(authority, Some(snapshot), operation, prior, None, meter)?;
            let digest = reservation_request_digest(&request, meter)?;
            drop(request);
            if !meter.is_native() {
                let bytes = meter.allocations.live_bytes().checked_sub(baseline)
                    .ok_or_else(|| ForkFailure::new(crate::WorkError::Overflow.into(), meter.work))?;
                meter.release(bytes)?;
            }
            let matches = digest == observation.request_digest;
            drop(observed);
            meter.release(retained_bytes)?;
            meter.release_opaque(acyclic_stream::MAX_COMMAND_BYTES as u64)?;
            Ok(matches)
        }
}

impl<P: acyclic_stream::StreamProvider> StreamAuthorityStore<P> {
    async fn fork_generation<'a, G: ForkPublicationGuard>(
            &self,
            source: crate::GenerationForkSource,
            destination_authority: AuthorityId,
            operation_id: OperationId,
            mut meter: ForkWork<'a>,
            mut guard: G,
        ) -> Result<crate::FsReceipt<CreateAuthorityOutcome>, ForkFailure<G::Error>>
        {
            if source.lineage == crate::GenerationFork::Independent {
                return self.create(destination_authority, Epoch::GENESIS, meter, guard).await;
            }
            meter.check().map_err(fork_fs_failure)?;
            let (source_lineage, forked_at) = self.resolve_source_fork_point(source.authority, source.generation, &mut meter).await
                .map_err(fork_fs_failure)?;
            if let Some(snapshot) = self.existing_fork_destination(destination_authority, source.generation, forked_at, &mut meter).await
                .map_err(fork_fs_failure)?
            {
                self.fork_gate(destination_authority, &mut meter).await
                    .map_err(fork_fs_failure)?;
                guard.seal(&mut meter).await?;
                return Ok(crate::OperationReceipt { value: CreateAuthorityOutcome::Existing(snapshot.head), work: meter.spent() });
            }
            let request = (|| {
                let meter = &mut meter;
                let root = meter.path(destination_authority, "", None)?;
                let records_path = meter.path(destination_authority, "/records", None)?;
                let epochs_path = meter.path(destination_authority, "/epochs", None)?;
                let locator_path = meter.path(destination_authority, "/generations/", Some(source.generation))?;
                let lineage = meter.path(destination_authority, "/lineage", None)?;
                let gate_path = meter.path(destination_authority, "/publication-gate", None)?;
                use acyclic_stream::{CommitCondition, CommitMutation};
                let mut conditions = meter.vec::<CommitCondition>(6)?;
                for path in [&root, &records_path, &epochs_path,
                    &locator_path, &lineage, &gate_path] {
                    let path = meter.clone_path(path)?;
                    meter.push(&mut conditions, CommitCondition::Absent { path })?;
                }
                let mut mutations = meter.vec::<CommitMutation>(6)?;
                let mut marker = meter.vec::<acyclic_stream::OwnedBytes>(1)?;
                meter.push(&mut marker, acyclic_stream::OwnedBytes::from_static(AUTHORITY_MARKER))?;
                meter.push(&mut mutations, CommitMutation::Append { path: root, records: marker })?;
                meter.push(&mut mutations, CommitMutation::Fork {
                    source: source_lineage, destination: lineage,
                    at_tail: forked_at, records: Vec::new(),
                })?;
                let genesis = meter.stream_bytes(GENESIS_DOMAIN, &[&Epoch::GENESIS.get().to_le_bytes()])?;
                let mut records = meter.vec::<acyclic_stream::OwnedBytes>(1)?;
                meter.push(&mut records, genesis)?;
                meter.push(&mut mutations, CommitMutation::Append { path: records_path, records })?;
                let epoch = meter.stream_bytes(EPOCH_DOMAIN, &[&Epoch::GENESIS.get().to_le_bytes()])?;
                let mut epochs = meter.vec::<acyclic_stream::OwnedBytes>(1)?;
                meter.push(&mut epochs, epoch)?;
                meter.push(&mut mutations, CommitMutation::Append { path: epochs_path, records: epochs })?;
                let locator = meter.stream_bytes(LINEAGE_TAIL_DOMAIN, &[&forked_at.to_le_bytes()])?;
                let mut locators = meter.vec::<acyclic_stream::OwnedBytes>(1)?;
                meter.push(&mut locators, locator)?;
                meter.push(&mut mutations, CommitMutation::Append { path: locator_path, records: locators })?;
                let mut gate = meter.vec::<acyclic_stream::OwnedBytes>(1)?;
                meter.push(&mut gate, acyclic_stream::OwnedBytes::from_static(PUBLICATION_GATE_FREE))?;
                meter.push(&mut mutations, CommitMutation::Append { path: gate_path, records: gate })?;
                let idempotency_key = meter.stream_key(b"fork-authority", &operation_id.into_bytes())?;
                Ok::<_, ForkFailure>(acyclic_stream::CommitRequest { conditions, mutations, idempotency_key })
            })().map_err(fork_fs_failure)?;
            let write_work = meter.request_write_work(&request)
                .map_err(fork_fs_failure)?;
            meter.admit(write_work).map_err(fork_fs_failure)?;
            let committed = match guard.commit(self, request, &mut meter).await {
                Ok(output) => Some(output),
                Err(AppendFailure::Authority(failure)) if matches!(authority_stream_error(&failure.error),
                    Some(acyclic_stream::StreamError::IdempotencyMismatch)) => None,
                Err(AppendFailure::Authority(failure)) => return Err(fork_fs_failure(failure)),
                Err(AppendFailure::Owner(error)) => return Err(ForkFailure::new(error, meter.spent())),
            };
            if !meter.is_native() { meter.release_request().map_err(fork_fs_failure)?; }
            match committed.as_ref().map(std::ops::Deref::deref) {
                Some(acyclic_stream::CommitOutcome::Committed(_)) => {
                    guard.seal(&mut meter).await?;
                    Ok(crate::OperationReceipt {
                        value: CreateAuthorityOutcome::Created(Head::genesis(Epoch::GENESIS)), work: meter.spent(),
                    })
                }
                Some(acyclic_stream::CommitOutcome::Conflict(_)) | None => {
                    let snapshot = self.existing_fork_destination(destination_authority, source.generation, forked_at, &mut meter).await
                        .map_err(fork_fs_failure)?
                        .ok_or_else(|| ForkFailure::new(crate::FsError::from(AuthorityStoreError::RejectedStatic(
                            "generation fork conflicted without creating its destination")).into(), meter.spent()))?;
                    self.fork_gate(destination_authority, &mut meter).await
                        .map_err(fork_fs_failure)?;
                    guard.seal(&mut meter).await?;
                    Ok(crate::OperationReceipt { value: CreateAuthorityOutcome::Existing(snapshot.head), work: meter.spent() })
                }
            }
        }
}

impl From<crate::kernel::CanonicalDecodeError> for ForkFailure<crate::FsError> {
    fn from(error: crate::kernel::CanonicalDecodeError) -> Self { Self::before_work(error.into()) }
}

impl From<crate::kernel::VolumeCreatedError> for ForkFailure<crate::FsError> {
    fn from(error: crate::kernel::VolumeCreatedError) -> Self { Self::before_work(error.into()) }
}

fn generation_from_payload(
    authority_id: AuthorityId, sequence: Sequence, payload: &[u8], meter: &mut ForkWork<'_>,
) -> Result<Option<GenerationId>, ForkFailure> {
    meter.check()?;
    let examined = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
    meter.admit(examined)?;
    const CREATED: &[u8] = b"acyclic-fs-volume-created-v1\0";
    const PUBLISHED: &[u8] = b"acyclic-fs-publish-generation-v1\0";
    let created = payload.len() == CREATED.len() + 2 + 16 + crate::kernel::CONFIG_ENCODED_BYTES + 1 + 32
        && payload.starts_with(CREATED);
    meter.record_completed(examined)?;
    let published = payload.starts_with(PUBLISHED);
    if !created && !published { return Ok(None); }
    let mut observe = |step| meter.observe_record(step, "fixed authority facts emitted an allocating decoder step")
        .map_err(fork_fs_failure::<crate::FsError>);
    let fact = if created {
        crate::kernel::decode_volume_created_with_steps(payload, STREAM_RECORD_LIMIT, &mut observe)
            .map(|created| (created.volume_id, created.initial_generation_root.digest))
    } else {
        crate::kernel::decode_published_generation_with_steps(payload, STREAM_RECORD_LIMIT, &mut observe)
            .map(|published| (published.volume_id, published.generation_root.digest))
    };
    let (volume, digest) = match fact {
        Ok(fact) => fact,
        Err(failure) if matches!(failure.error, crate::FsError::Decode(_) | crate::FsError::VolumeCreated(_)) => return Ok(None),
        Err(failure) => return Err(project_fork_authority_failure(failure)),
    };
    if volume_authority_id(volume) != authority_id || created != (sequence == Sequence::new(1)) {
        return Err(authority_corrupt("generation fact identity or sequence does not match its authority", meter));
    }
    Ok(Some(GenerationId::new(digest)))
}

fn record_bytes<'a, const N: usize>(encoded: &'a [u8], domain: &[u8]) -> Option<&'a [u8; N]> {
    encoded.strip_prefix(domain)?.try_into().ok()
}

fn decode_lineage_generation(encoded: &[u8]) -> Result<GenerationId, AuthorityStoreError> {
    let bytes = record_bytes::<32>(encoded, LINEAGE_DOMAIN)
        .ok_or(AuthorityStoreError::CorruptStatic("invalid Stream generation-lineage record"))?;
    Ok(GenerationId::new(Digest::from_bytes(*bytes)))
}

fn decode_lineage_tail(encoded: &[u8]) -> Result<u64, AuthorityStoreError> {
    let bytes = record_bytes::<8>(encoded, LINEAGE_TAIL_DOMAIN)
        .ok_or(AuthorityStoreError::CorruptStatic("invalid Stream generation locator"))?;
    Ok(u64::from_le_bytes(*bytes))
}

fn decode_epoch(encoded: &[u8], domain: &[u8], native: bool) -> Result<Epoch, AuthorityStoreError> {
    let bytes = record_bytes::<8>(encoded, domain).ok_or_else(|| if native {
        AuthorityStoreError::CorruptStatic("invalid Stream epoch record")
    } else { AuthorityStoreError::Corrupt("invalid Stream epoch record".to_owned()) })?;
    Epoch::new(u64::from_le_bytes(*bytes)).map_err(|error| if native {
        AuthorityStoreError::CorruptStatic("zero Stream epoch")
    } else { AuthorityStoreError::Corrupt(error.to_string()) })
}

async fn read_one<P: acyclic_stream::StreamProvider>(
    provider: &P,
    path: acyclic_stream::StreamPath,
    from: u64,
) -> Result<acyclic_stream::Record, AuthorityStoreError> {
    let mut records = provider
        .read(acyclic_stream::ReadRequest {
            path,
            from,
            limit: 1,
        })
        .await
        .map_err(map_stream_error)?;
    let record = records
        .next()
        .await
        .ok_or_else(|| {
            AuthorityStoreError::Corrupt("Stream returned an empty exact read".to_owned())
        })?
        .map_err(map_stream_error)?;
    if record.sequence != from {
        return Err(AuthorityStoreError::Corrupt(
            "Stream returned a non-contiguous record".to_owned(),
        ));
    }
    Ok(record)
}

fn decode_durable(
    authority_id: AuthorityId,
    record: &acyclic_stream::Record,
) -> Result<DurableCommit, AuthorityStoreError> {
    let mut durable = StreamsDurableRecord::decode(&record.value, STREAM_RECORD_LIMIT)
        .map_err(|error| AuthorityStoreError::Corrupt(error.to_string()))?
        .0;
    if durable.sequence.get() != record.sequence {
        return Err(AuthorityStoreError::Corrupt(
            "Stream sequence does not match filesystem authority sequence".to_owned(),
        ));
    }
    durable.settled_at_micros =
        (record.committed_at_micros != 0).then_some(record.committed_at_micros);
    let expected = authority_commit_digest(
        authority_id,
        durable.epoch,
        durable.sequence,
        durable.operation_id,
        durable.fingerprint,
        durable.previous_digest,
        &durable.payload,
    );
    if expected != durable.digest {
        return Err(AuthorityStoreError::Corrupt(
            "Stream authority record digest mismatch".to_owned(),
        ));
    }
    Ok(durable)
}

fn map_stream_error(error: acyclic_stream::StreamError) -> AuthorityStoreError {
    match error {
        acyclic_stream::StreamError::NotFound => AuthorityStoreError::Missing,
        acyclic_stream::StreamError::Capacity => {
            AuthorityStoreError::Rejected("Stream capacity exhausted".to_owned())
        }
        other => AuthorityStoreError::Rejected(other.to_string()),
    }
}

fn admit_snapshot_read(
    work: WorkCounters,
    proposed: WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<WorkCounters, ForkFailure> {
    cancellation
        .check()
        .map_err(|_| ForkFailure::new(AuthorityStoreError::Cancelled, work))?;
    let next = work
        .checked_add(proposed)
        .map_err(|error| ForkFailure::new(error.into(), work))?;
    next.verify(budget)
        .map_err(|error| ForkFailure::new(error.into(), work))?;
    Ok(next)
}

fn authority_read_work(records: u64) -> WorkCounters {
    WorkCounters {
        authority_records_read: records,
        backend_read_operations: records,
        ..WorkCounters::default()
    }
}

fn authority_write_work(records: u64, payload_bytes: u64) -> WorkCounters {
    WorkCounters {
        authority_records_appended: records,
        authority_bytes_written: payload_bytes,
        backend_write_operations: 1,
        durability_operations: 1,
        ..WorkCounters::default()
    }
}

fn admit_authority(
    work: WorkCounters,
    budget: WorkBudget,
) -> Result<(), ForkFailure> {
    work.verify(budget)
        .map_err(|error| ForkFailure::new(error.into(), work))
}

fn authority_success<T>(value: T, work: WorkCounters, budget: WorkBudget) -> Result<AuthorityReceipt<T>, ForkFailure> {
    admit_authority(work, budget)?;
    Ok(AuthorityReceipt { value, work })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::{VolumeCreated, encode_publication_payload, encode_volume_created};
    use crate::model::{Lifecycle, VolumeConfig};
    use crate::storage::ObjectKind;
    #[tokio::test]
    async fn settlement_times_and_retired_accounting_use_original_stream_records() {
        let stream = Arc::new(acyclic_stream::MemoryStream::default());
        let authority = StreamAuthorityStore::new(Arc::clone(&stream));
        let authority_id = AuthorityId::from_bytes([0x71; 16]);
        let cancellation = CancellationToken::new();
        authority.create_authority(
            authority_id, Epoch::GENESIS, WorkBudget::UNBOUNDED, &cancellation,
        ).await.expect("create authority");
        let head = authority.head(authority_id, WorkBudget::UNBOUNDED, &cancellation)
            .await.expect("head").value;
        let proposed = ProposedCommit {
            operation_id: OperationId::from_bytes([0x72; 16]),
            fingerprint: Digest::from_bytes([0x73; 32]),
            payload: Bytes::from_static(b"accounting settlement"),
        };
        let outcome = authority.compare_and_append(
            authority_id, head.epoch, head, proposed.clone(),
            WorkBudget::UNBOUNDED, &cancellation,
        ).await.expect("append").value;
        let FsAppendOutcome::Committed(durable) = outcome else {
            panic!("expected committed record");
        };
        let record = read_one(stream.as_ref(), stream_path(authority_id, "/records").expect("path"), 1)
            .await.expect("exact authoritative record");
        assert_eq!(durable.settled_at_micros,
            (record.committed_at_micros != 0).then_some(record.committed_at_micros));
        let limit = ReplayLimit { records: 8, payload_bytes: 1_024 };
        assert_eq!(authority.replay(
            authority_id, Sequence::new(0), limit, WorkBudget::UNBOUNDED, &cancellation,
        ).await.expect("replay").value, vec![durable.clone()]);
        assert_eq!(authority.find_operation(
            authority_id, proposed.operation_id, WorkBudget::UNBOUNDED, &cancellation,
        ).await.expect("find").value, Some(durable.clone()));
        assert_eq!(authority.compare_and_append(
            authority_id, head.epoch, head, proposed.clone(),
            WorkBudget::UNBOUNDED, &cancellation,
        ).await.expect("retry").value, FsAppendOutcome::AlreadyCommitted(durable.clone()));

        let mut legacy = record.clone();
        legacy.committed_at_micros = 0;
        assert_eq!(decode_durable(authority_id, &legacy).expect("legacy").settled_at_micros, None);

        authority.retire_authority(authority_id, WorkBudget::UNBOUNDED, &cancellation)
            .await.expect("retire");
        assert!(matches!(authority.head(authority_id, WorkBudget::UNBOUNDED, &cancellation)
            .await.expect_err("ordinary head stays retired").error, AuthorityStoreError::Retired));
        assert!(matches!(authority.replay(
            authority_id, Sequence::new(0), limit, WorkBudget::UNBOUNDED, &cancellation,
        ).await.expect_err("ordinary replay stays retired").error, AuthorityStoreError::Retired));
        assert!(matches!(authority.find_operation(
            authority_id, proposed.operation_id, WorkBudget::UNBOUNDED, &cancellation,
        ).await.expect_err("ordinary operation lookup stays retired").error, AuthorityStoreError::Retired));
        assert!(matches!(authority.compare_and_append(
            authority_id, head.epoch, head, proposed, WorkBudget::UNBOUNDED, &cancellation,
        ).await.expect_err("ordinary retry stays retired").error, AuthorityStoreError::Retired));
        let accounting = AccountingAuthority(&authority);
        assert_eq!(accounting.replay(
            authority_id, Sequence::new(0), limit, WorkBudget::UNBOUNDED, &cancellation,
        ).await.expect("accounting retains retired history").value, vec![durable.clone()]);
        assert_eq!(accounting.head(authority_id, WorkBudget::UNBOUNDED, &cancellation)
            .await.expect("historical head").value.sequence, durable.sequence);
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native_authority_budget() -> WorkBudget {
        const N: u64 = 1 << 28;
        WorkBudget {
            authority_records_read: N, authority_records_appended: N,
            authority_bytes_read: N, authority_bytes_written: N,
            object_probes: N, backend_read_operations: N, backend_write_operations: N,
            durability_operations: N, page_reads: N, page_writes: N,
            object_bytes_read: N, object_bytes_written: N, bytes_hashed: N,
            bytes_copied: N, bytes_encoded: N, source_bytes_read: N,
            source_path_components: N, source_entries_visited: N, output_bytes: N,
            items_examined: N, items_returned: N, allocation_operations: N,
            peak_allocation_bytes: N, materializations: N,
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn original_append_owner_seals_after_planning_and_again_on_recovery()
        -> Result<(), Box<dyn std::error::Error>> {
        #[derive(Debug)]
        enum OwnerError { Authority, Rejected }
        impl From<AuthorityStoreError> for OwnerError {
            fn from(_: AuthorityStoreError) -> Self { Self::Authority }
        }
        let cancellation = CancellationToken::new();
        let store = StreamAuthorityStore::new(Arc::new(acyclic_stream::MemoryStream::default()));
        let authority = AuthorityId::from_bytes([0x86; 16]);
        store.create_authority(authority, Epoch::GENESIS, native_authority_budget(), &cancellation).await?;
        let original = crate::NativeMaterializationWork::new(native_authority_budget(), &cancellation)
            .map_err(|(error, _)| error)?;
        let expected = Head::genesis(Epoch::GENESIS);
        let request = crate::GuardedAppend { authority_id: authority, epoch: expected.epoch, expected,
            commit: ProposedCommit { operation_id: OperationId::from_bytes([0x87; 16]),
                fingerprint: Digest::from_bytes([0x88; 32]), payload: Bytes::from_static(b"owner-sealed") },
            permit: PublicationPermit::Unrestricted };
        let expected_root = original.clone();
        let mut reject = move |work: &crate::NativeMaterializationWork| {
            assert!(work.shares_ledger(&expected_root));
            assert!(work.receipt().bytes_hashed > 0);
            work.admit(WorkCounters::UNCHARGED).expect("unlocked original root");
            std::future::ready(Err(OwnerError::Rejected))
        };
        let failure = store.compare_and_append_guarded_with_native_owner(
            request.clone(), &original, &cancellation, &mut reject).await;
        assert!(matches!(failure, Err(OwnerError::Rejected)));
        assert_eq!(original.receipt().backend_write_operations, 0);
        assert_eq!(store.head(authority, native_authority_budget(), &cancellation).await?.value, expected);
        let (outcome, allocation) = store.compare_and_append_guarded_with_native_work(
            request.clone(), &original, &cancellation).await?;
        assert!(matches!(outcome, FsAppendOutcome::Committed(_)));
        drop(outcome);
        drop(allocation);
        let before = original.receipt();
        let failure = store.compare_and_append_guarded_with_native_owner(
            request, &original, &cancellation, &mut reject).await;
        assert!(matches!(failure, Err(OwnerError::Rejected)));
        assert_eq!(original.receipt().backend_write_operations, before.backend_write_operations);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn original_creation_denial_and_cancellation_publish_nothing()
        -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = CancellationToken::new();
        let store = StreamAuthorityStore::new(Arc::new(acyclic_stream::MemoryStream::default()));
        let authority = AuthorityId::from_bytes([0x89; 16]);
        let mut budget = native_authority_budget();
        budget.allocation_operations = 1;
        let original = crate::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(error, _)| error)?;
        let before = original.receipt();
        let first = ProposedCommit { operation_id: OperationId::from_bytes([0x8a; 16]),
            fingerprint: Digest::from_bytes([0x8b; 32]), payload: Bytes::from_static(b"denied") };
        let failure = store.create_authority_with_first_record_with_native_work(
            authority, first.clone(), &original, &cancellation).await;
        assert!(matches!(failure, Err(AuthorityStoreError::NativeOriginal(
            crate::NativeTreeMaterializationError::Work(_)))));
        assert_eq!(original.receipt().allocation_operations, before.allocation_operations);
        assert_eq!(original.receipt().backend_write_operations, 0);
        assert_eq!(original.receipt().authority_records_appended, 0);
        cancellation.cancel();
        let spent = original.receipt();
        let failure = store.create_authority_with_first_record_with_native_work(
            authority, first, &original, &cancellation).await;
        assert!(matches!(failure, Err(AuthorityStoreError::Cancelled)
            | Err(AuthorityStoreError::NativeOriginal(crate::NativeTreeMaterializationError::Cancelled))));
        assert_eq!(original.receipt(), spent);
        let fresh = CancellationToken::new();
        assert!(matches!(store.head(authority, native_authority_budget(), &fresh).await,
            Err(failure) if matches!(failure.error, AuthorityStoreError::Missing)));
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn original_authority_custom_defaults_never_fall_back_to_ordinary_io()
        -> Result<(), Box<dyn std::error::Error>> {
        use std::sync::atomic::Ordering;
        let cancellation = CancellationToken::new();
        let provider = Arc::new(ReleaseSpy::new(Arc::new(acyclic_stream::MemoryStream::default())));
        let store = StreamAuthorityStore::new(Arc::clone(&provider));
        let original = crate::NativeMaterializationWork::new(native_authority_budget(), &cancellation)
            .map_err(|(error, _)| error)?;
        let authority = AuthorityId::from_bytes([0x7a; 16]);
        assert!(matches!(&store.head_with_native_work(authority, &original, &cancellation).await,
            Err(AuthorityStoreError::NativeStreamFailure(error))
                if matches!(error.source(), acyclic_stream::StreamError::Unsupported)));
        assert!(matches!(&store.replay_with_native_work(authority, Sequence::GENESIS,
            ReplayLimit { records: 1, payload_bytes: 8 }, &original, &cancellation).await,
            Err(AuthorityStoreError::NativeStreamFailure(error))
                if matches!(error.source(), acyclic_stream::StreamError::Unsupported)));
        assert!(matches!(&store.fence_with_native_work(authority, Head::genesis(Epoch::GENESIS),
            &original, &cancellation).await, Err(AuthorityStoreError::NativeStreamFailure(error))
                if matches!(error.source(), acyclic_stream::StreamError::Unsupported)));
        assert!(matches!(&store.find_operation_with_native_work(authority, OperationId::from_bytes([0x7b; 16]),
            &original, &cancellation).await, Err(AuthorityStoreError::NativeStreamFailure(error))
                if matches!(error.source(), acyclic_stream::StreamError::Unsupported)));
        assert_eq!(provider.writes.load(Ordering::SeqCst), 0);
        assert_eq!(provider.reads.load(Ordering::SeqCst), 0);
        assert_eq!(original.receipt().backend_read_operations, 0);
        assert_eq!(original.receipt().backend_write_operations, 0);
        assert_eq!(original.receipt().authority_records_read, 0);
        original.receipt().verify(native_authority_budget())?;
        Ok(())
    }

    #[tokio::test]
    async fn authority_without_publication_gate_fails_closed() {
        let stream = Arc::new(acyclic_stream::MemoryStream::default());
        let authority_id = AuthorityId::from_bytes([0x31; 16]);
        let root = stream_path(authority_id, "").expect("root path");
        let records = stream_path(authority_id, "/records").expect("records path");
        let epochs = stream_path(authority_id, "/epochs").expect("epochs path");
        acyclic_stream::StreamProvider::commit(
            stream.as_ref(),
            acyclic_stream::CommitRequest {
                conditions: vec![
                    acyclic_stream::CommitCondition::Absent { path: root.clone() },
                    acyclic_stream::CommitCondition::Absent {
                        path: records.clone(),
                    },
                    acyclic_stream::CommitCondition::Absent {
                        path: epochs.clone(),
                    },
                ],
                mutations: vec![
                    acyclic_stream::CommitMutation::Append {
                        path: root,
                        records: vec![acyclic_stream::OwnedBytes::from_static(AUTHORITY_MARKER)],
                    },
                    acyclic_stream::CommitMutation::Append {
                        path: records,
                        records: vec![ForkWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()).bytes(GENESIS_DOMAIN, &[&Epoch::GENESIS.get().to_le_bytes()]).expect("epoch").into()],
                    },
                    acyclic_stream::CommitMutation::Append {
                        path: epochs,
                        records: vec![ForkWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()).bytes(EPOCH_DOMAIN, &[&Epoch::GENESIS.get().to_le_bytes()]).expect("epoch").into()],
                    },
                ],
                idempotency_key: stream_key(b"gateless-authority", &authority_id.into_bytes())
                    .expect("idempotency key"),
            },
        )
        .await
        .expect("seed gateless authority");

        let authority = StreamAuthorityStore::new(Arc::clone(&stream));
        let refused = authority
            .create_authority(
                authority_id,
                Epoch::GENESIS,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .expect_err("an authority without its publication gate is refused");
        assert!(
            matches!(refused.error, AuthorityStoreError::Corrupt(_)),
            "unexpected refusal: {:?}",
            refused.error
        );
        assert!(matches!(
            acyclic_stream::StreamProvider::tail(
                stream.as_ref(),
                ForkWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()).path(authority_id, "/publication-gate", None).expect("gate path"),
            )
            .await,
            Err(acyclic_stream::StreamError::NotFound)
        ));
    }

    #[tokio::test]
    async fn existing_authority_rejects_a_corrupt_publication_gate() {
        let stream = Arc::new(acyclic_stream::MemoryStream::default());
        let authority = StreamAuthorityStore::new(Arc::clone(&stream));
        let authority_id = AuthorityId::from_bytes([0x35; 16]);
        let cancellation = CancellationToken::new();
        authority
            .create_authority(
                authority_id,
                Epoch::GENESIS,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .expect("create authority");
        let gate = ForkWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()).path(authority_id, "/publication-gate", None).expect("gate path");
        acyclic_stream::StreamProvider::commit(
            stream.as_ref(),
            acyclic_stream::CommitRequest {
                conditions: vec![acyclic_stream::CommitCondition::Tail {
                    path: gate.clone(),
                    expected: 1,
                }],
                mutations: vec![acyclic_stream::CommitMutation::Append {
                    path: gate,
                    records: vec![acyclic_stream::OwnedBytes::from_static(b"invalid-publication-gate")],
                }],
                idempotency_key: stream_key(
                    b"corrupt-publication-gate",
                    &authority_id.into_bytes(),
                )
                .expect("corruption key"),
            },
        )
        .await
        .expect("append invalid gate record");

        let error = authority
            .create_authority(
                authority_id,
                Epoch::GENESIS,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .expect_err("corrupt gate must fail authority reopen");
        assert!(matches!(error.error, AuthorityStoreError::Corrupt(_)));
    }

    #[tokio::test]
    async fn publication_reservation_excludes_unreserved_writers()
        -> Result<(), Box<dyn std::error::Error>> {
        let (stream, reservation) = release_fixture().await?;
        let authority = StreamAuthorityStore::new(Arc::clone(&stream));
        let authority_id = reservation.authority_id;
        let head = reservation.expected;
        let operation_id = reservation.operation_id;
        let cancellation = CancellationToken::new();
        let retry = authority.reserve_publication(authority_id, head, operation_id,
            WorkBudget::UNBOUNDED, &cancellation).await?.value;
        assert!(matches!(retry, ReservationOutcome::AlreadyReserved(value) if value == reservation));
        let request = crate::GuardedAppend {
            authority_id, epoch: head.epoch, expected: head,
            commit: ProposedCommit {
                operation_id: OperationId::from_bytes([0x43; 16]),
                fingerprint: Digest::from_bytes([0x44; 32]),
                payload: Bytes::from_static(b"reserved write"),
            },
            permit: PublicationPermit::Unrestricted,
        };
        let ordinary = authority.compare_and_append_guarded(
            request.clone(), WorkBudget::UNBOUNDED, &cancellation).await?.value;
        assert!(matches!(ordinary, FsAppendOutcome::Fenced { .. }));
        let reserved = authority.compare_and_append_guarded(crate::GuardedAppend {
            permit: PublicationPermit::Reservation {
                operation_id: operation_id.into_bytes(), gate_tail: reservation.gate_tail,
                expected: reservation.expected,
            },
            ..request
        }, WorkBudget::UNBOUNDED, &cancellation).await?.value;
        assert!(matches!(reserved, FsAppendOutcome::Committed(_)));
        let changed = authority.head(authority_id, WorkBudget::UNBOUNDED, &cancellation).await?.value;
        let stale = authority.reserve_publication(authority_id, head, operation_id,
            WorkBudget::UNBOUNDED, &cancellation).await?.value;
        assert!(matches!(stale, ReservationOutcome::Conflict { actual } if actual == changed));
        authority.reserve_publication(authority_id, changed, operation_id, WorkBudget::UNBOUNDED, &cancellation)
            .await.expect_err("an active operation must not adopt a different source head");
        let denied_release = authority.release_publication(reservation, WorkBudget::default(), &cancellation)
            .await.expect_err("release must charge its idempotency read before doing work");
        assert!(matches!(denied_release.error, AuthorityStoreError::Work(_)));
        authority.release_publication(reservation, WorkBudget::UNBOUNDED, &cancellation).await?;
        authority.release_publication(reservation, WorkBudget::UNBOUNDED, &cancellation).await?;
        let current = authority.head(authority_id, WorkBudget::UNBOUNDED, &cancellation).await?.value;
        let reacquired = match authority.reserve_publication(authority_id, current, operation_id,
            WorkBudget::UNBOUNDED, &cancellation).await?.value {
            ReservationOutcome::Reserved(reservation) => reservation,
            outcome => panic!("unexpected reacquisition outcome: {outcome:?}"),
        };
        let forged = PublicationReservation {
            operation_id: OperationId::from_bytes([0x99; 16]), ..reacquired
        };
        assert!(authority.release_publication(forged, WorkBudget::UNBOUNDED, &cancellation).await.is_err());
        let gate = ForkWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .path(authority_id, "/publication-gate", None)?;
        let advanced = acyclic_stream::StreamProvider::commit(stream.as_ref(), acyclic_stream::CommitRequest {
            conditions: vec![acyclic_stream::CommitCondition::Tail {
                path: gate.clone(), expected: reacquired.gate_tail,
            }],
            mutations: vec![acyclic_stream::CommitMutation::Append {
                path: gate.clone(), records: vec![ForkWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .bytes(PUBLICATION_GATE_ACTIVE, &[&reacquired.operation_id.into_bytes()])?.into()],
            }],
            idempotency_key: stream_key(b"advance-active-gate", &authority_id.into_bytes())?,
        }).await?;
        assert!(matches!(advanced, acyclic_stream::CommitOutcome::Committed(_)));
        let release_key = ForkWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()).stream_key(
            b"release-publication", &[reacquired.authority_id.into_bytes().as_slice(),
                reacquired.operation_id.into_bytes().as_slice(), reacquired.gate_tail.to_le_bytes().as_slice()].concat())?;
        let retained_conflict = acyclic_stream::StreamProvider::commit(stream.as_ref(), acyclic_stream::CommitRequest {
            conditions: vec![acyclic_stream::CommitCondition::Tail {
                path: gate.clone(), expected: reacquired.gate_tail,
            }],
            mutations: vec![acyclic_stream::CommitMutation::Append {
                path: gate, records: vec![acyclic_stream::OwnedBytes::from_static(PUBLICATION_GATE_FREE)],
            }],
            idempotency_key: release_key,
        }).await?;
        assert!(matches!(retained_conflict, acyclic_stream::CommitOutcome::Conflict(_)));
        authority.release_publication(reacquired, WorkBudget::UNBOUNDED, &cancellation)
            .await.expect_err("retained release conflict must not become success");
        let gate = stream_path(authority_id, "/publication-gate")?;
        let tail = acyclic_stream::StreamProvider::tail(stream.as_ref(), gate.clone()).await?;
        let record = read_one(stream.as_ref(), gate, tail - 1).await?;
        assert_eq!(decode_publication_gate(&record.value)?, Some(reacquired.operation_id));
        Ok(())
    }

    struct ReleaseSpy {
        inner: Arc<acyclic_stream::MemoryStream>,
        reads: std::sync::atomic::AtomicU64,
        writes: std::sync::atomic::AtomicU64,
        fail_inspect: bool,
        cancel_inspect: Option<CancellationToken>,
        lose_ack: bool,
        corrupt_observation: u8,
    }

    impl ReleaseSpy {
        fn new(inner: Arc<acyclic_stream::MemoryStream>) -> Self {
            Self {
                inner, reads: std::sync::atomic::AtomicU64::new(0),
                writes: std::sync::atomic::AtomicU64::new(0),
                fail_inspect: false, cancel_inspect: None, lose_ack: false,
                corrupt_observation: 0,
            }
        }
    }

    #[async_trait::async_trait]
    impl acyclic_stream::StreamProvider for ReleaseSpy {
        async fn inspect_idempotency(&self, key: acyclic_stream::IdempotencyKey)
            -> Result<Option<acyclic_stream::IdempotencyObservation>, acyclic_stream::StreamError> {
            self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.fail_inspect { return Err(acyclic_stream::StreamError::Unavailable); }
            let mut result = self.inner.inspect_idempotency(key).await?;
            if let Some(observation) = result.as_mut() {
                if self.corrupt_observation == 1 {
                    observation.idempotency_key = stream_key(b"another-release", b"identity")
                        .map_err(|_| acyclic_stream::StreamError::InvalidArgument)?;
                }
                if let acyclic_stream::IdempotencyOutcome::Commit(
                    acyclic_stream::CommitOutcome::Committed(envelope),
                ) = &mut observation.outcome {
                    if let Some(acyclic_stream::CommittedMutation::Append(append)) = envelope.mutations.first_mut() {
                        match self.corrupt_observation {
                            2 => append.path = acyclic_stream::StreamPath::new("other/gate")?,
                            3 => append.start += 1,
                            4 => append.end += 1,
                            5 => append.tail += 1,
                            6 => append.records[0].sequence += 1,
                            7 => append.records[0].commit_id = acyclic_stream::CommitId::from_bytes([0xff; 32]),
                            8 => append.records[0].value = acyclic_stream::OwnedBytes::from_static(b"not-free"),
                            9 => append.records.push(append.records[0].clone()),
                            10 => observation.request_digest = [0; 32],
                            _ => {}
                        }
                    }
                }
            }
            if let Some(cancellation) = &self.cancel_inspect { cancellation.cancel(); }
            Ok(result)
        }
        async fn tail(&self, path: acyclic_stream::StreamPath) -> Result<u64, acyclic_stream::StreamError> {
            self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.tail(path).await
        }
        async fn read(&self, request: acyclic_stream::ReadRequest)
            -> Result<acyclic_stream::RecordStream, acyclic_stream::StreamError> {
            self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.read(request).await
        }
        async fn commit(&self, request: acyclic_stream::CommitRequest)
            -> Result<acyclic_stream::CommitOutcome, acyclic_stream::StreamError> {
            self.writes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let result = self.inner.commit(request).await?;
            if self.lose_ack { Err(acyclic_stream::StreamError::Unavailable) } else { Ok(result) }
        }
        async fn bounds(&self, path: acyclic_stream::StreamPath)
            -> Result<acyclic_stream::StreamBounds, acyclic_stream::StreamError> { self.inner.bounds(path).await }
        async fn append(&self, request: acyclic_stream::AppendRequest)
            -> Result<acyclic_stream::AppendOutcome, acyclic_stream::StreamError> { self.inner.append(request).await }
        async fn fork(&self, request: acyclic_stream::ForkRequest)
            -> Result<acyclic_stream::ForkReceipt, acyclic_stream::StreamError> { self.inner.fork(request).await }
        async fn follow(&self, path: acyclic_stream::StreamPath, from: u64)
            -> Result<acyclic_stream::RecordStream, acyclic_stream::StreamError> { self.inner.follow(path, from).await }
        async fn children(&self, request: acyclic_stream::ChildrenRequest)
            -> Result<acyclic_stream::ChildStream, acyclic_stream::StreamError> { self.inner.children(request).await }
        async fn read_commit(&self, id: acyclic_stream::CommitId)
            -> Result<acyclic_stream::CommittedEnvelope, acyclic_stream::StreamError> { self.inner.read_commit(id).await }
    }

    async fn release_fixture()
        -> Result<(Arc<acyclic_stream::MemoryStream>, PublicationReservation), Box<dyn std::error::Error>> {
        let inner = Arc::new(acyclic_stream::MemoryStream::default());
        let authority = StreamAuthorityStore::new(Arc::clone(&inner));
        let cancellation = CancellationToken::new();
        let id = AuthorityId::from_bytes([0xd1; 16]);
        authority.create_authority(id, Epoch::GENESIS, WorkBudget::UNBOUNDED, &cancellation).await?;
        let head = authority.head(id, WorkBudget::UNBOUNDED, &cancellation).await?.value;
        let outcome = authority.reserve_publication(id, head, OperationId::from_bytes([0xd2; 16]),
            WorkBudget::UNBOUNDED, &cancellation).await?.value;
        let ReservationOutcome::Reserved(reservation) = outcome else { return Err("reservation denied".into()); };
        Ok((inner, reservation))
    }

    #[tokio::test]
    async fn release_lost_ack_recovers_original_envelope_after_gate_advance()
        -> Result<(), Box<dyn std::error::Error>> {
        use std::sync::atomic::Ordering;
        let (inner, reservation) = release_fixture().await?;
        let mut spy = ReleaseSpy::new(Arc::clone(&inner));
        spy.lose_ack = true;
        let spy = Arc::new(spy);
        let failure = StreamAuthorityStore::new(Arc::clone(&spy))
            .release_publication(reservation, WorkBudget::UNBOUNDED, &CancellationToken::new()).await
            .expect_err("provider loses release acknowledgement");
        assert_eq!(spy.writes.load(Ordering::SeqCst), 1);
        assert_eq!(failure.work.durability_operations, 0);
        assert_eq!(failure.work.authority_bytes_written, 0);
        let authority = StreamAuthorityStore::new(Arc::clone(&inner));
        let new_operation = OperationId::from_bytes([0xd3; 16]);
        let outcome = authority.reserve_publication(reservation.authority_id, reservation.expected,
            new_operation, WorkBudget::UNBOUNDED, &CancellationToken::new()).await?.value;
        assert!(matches!(outcome, ReservationOutcome::Reserved(_)));
        let spy = Arc::new(ReleaseSpy::new(Arc::clone(&inner)));
        let receipt = StreamAuthorityStore::new(Arc::clone(&spy))
            .release_publication(reservation, WorkBudget::UNBOUNDED, &CancellationToken::new()).await?;
        assert_eq!(spy.writes.load(Ordering::SeqCst), 0);
        assert_eq!(receipt.work.authority_records_read, 1);
        assert_eq!(receipt.work.authority_bytes_read, PUBLICATION_GATE_FREE.len() as u64);
        assert_eq!(receipt.work.backend_write_operations, 0);
        let gate = stream_path(reservation.authority_id, "/publication-gate")?;
        let tail = acyclic_stream::StreamProvider::tail(inner.as_ref(), gate.clone()).await?;
        let record = read_one(inner.as_ref(), gate, tail - 1).await?;
        assert_eq!(decode_publication_gate(&record.value)?, Some(new_operation));
        for corrupt in 1..=10 {
            let mut spy = ReleaseSpy::new(Arc::clone(&inner));
            spy.corrupt_observation = corrupt;
            let spy = Arc::new(spy);
            let failure = StreamAuthorityStore::new(Arc::clone(&spy))
                .release_publication(reservation, WorkBudget::UNBOUNDED, &CancellationToken::new()).await
                .expect_err("retained observation must bind exact original release");
            assert!(matches!(failure.error, AuthorityStoreError::Corrupt(_)));
            assert_eq!(spy.writes.load(Ordering::SeqCst), 0);
        }
        let cancellation = CancellationToken::new();
        let mut spy = ReleaseSpy::new(inner);
        spy.cancel_inspect = Some(cancellation.clone());
        let failure = StreamAuthorityStore::new(Arc::new(spy))
            .release_publication(reservation, WorkBudget::UNBOUNDED, &cancellation).await
            .expect_err("cancellation retains the actual returned observation prefix");
        assert!(matches!(failure.error, AuthorityStoreError::Cancelled));
        assert_eq!(failure.work.authority_records_read, 1);
        assert_eq!(failure.work.authority_bytes_read, PUBLICATION_GATE_FREE.len() as u64);
        Ok(())
    }

    #[tokio::test]
    async fn release_rejects_committed_wrong_request_and_mutation_family()
        -> Result<(), Box<dyn std::error::Error>> {
        use std::sync::atomic::Ordering;
        for wrong_family in [false, true] {
            let (inner, reservation) = release_fixture().await?;
            let gate = ForkWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()).path(reservation.authority_id, "/publication-gate", None)?;
            let key = ForkWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()).stream_key(b"release-publication", &[reservation.authority_id.into_bytes().as_slice(), reservation.operation_id.into_bytes().as_slice(), reservation.gate_tail.to_le_bytes().as_slice()].concat())?;
            if wrong_family {
                let outcome = acyclic_stream::StreamProvider::append(inner.as_ref(), acyclic_stream::AppendRequest {
                    path: gate, records: vec![acyclic_stream::OwnedBytes::from_static(PUBLICATION_GATE_FREE)],
                    if_tail: Some(reservation.gate_tail), idempotency_key: Some(key),
                }).await?;
                assert!(matches!(outcome, acyclic_stream::AppendOutcome::Committed(_)));
            } else {
                let outcome = acyclic_stream::StreamProvider::commit(inner.as_ref(), acyclic_stream::CommitRequest {
                    conditions: vec![acyclic_stream::CommitCondition::Tail {
                        path: gate.clone(), expected: reservation.gate_tail,
                    }],
                    mutations: vec![acyclic_stream::CommitMutation::Append {
                        path: gate, records: vec![acyclic_stream::OwnedBytes::from_static(PUBLICATION_GATE_FREE)],
                    }],
                    idempotency_key: key,
                }).await?;
                assert!(matches!(outcome, acyclic_stream::CommitOutcome::Committed(_)));
            }
            let spy = Arc::new(ReleaseSpy::new(inner));
            let failure = StreamAuthorityStore::new(Arc::clone(&spy))
                .release_publication(reservation, WorkBudget::UNBOUNDED, &CancellationToken::new()).await
                .expect_err("arbitrary retained success must not release this reservation");
            assert!(matches!(failure.error, AuthorityStoreError::Corrupt(_)));
            assert_eq!(spy.writes.load(Ordering::SeqCst), 0);
        }
        let (inner, reservation) = release_fixture().await?;
        let receipt = StreamAuthorityStore::new(inner)
            .release_publication(reservation, WorkBudget::UNBOUNDED, &CancellationToken::new()).await?;
        assert_eq!(receipt.work.authority_bytes_written, PUBLICATION_GATE_FREE.len() as u64);
        Ok(())
    }

    #[test]
    fn generation_records_are_bound_to_their_authority_and_sequence()
    -> Result<(), Box<dyn std::error::Error>> {
        let volume_id = crate::foundation::VolumeId::from_bytes([0x71; 16]);
        let authority_id = volume_authority_id(volume_id);
        let root = ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: Digest::from_bytes([0x72; 32]),
        };
        let creation = encode_volume_created(VolumeCreated {
            volume_id,
            config: VolumeConfig::portable(Lifecycle::Ephemeral),
            initial_generation_root: root,
        })?;
        let publication = encode_publication_payload(volume_id, root);
        let generation = GenerationId::new(root.digest);
        let cancellation = CancellationToken::new();
        let mut meter = ForkWork::new(WorkBudget::UNBOUNDED, &cancellation);

        assert_eq!(
            generation_from_payload(authority_id, Sequence::new(1), &creation, &mut meter)?,
            Some(generation)
        );
        assert_eq!(
            generation_from_payload(authority_id, Sequence::new(2), &publication, &mut meter)?,
            Some(generation)
        );
        assert!(generation_from_payload(authority_id, Sequence::new(2), &creation, &mut meter).is_err());
        assert!(generation_from_payload(authority_id, Sequence::new(1), &publication, &mut meter).is_err());
        assert!(
            generation_from_payload(
                AuthorityId::from_bytes([0x73; 16]),
                Sequence::new(1),
                &creation,
                &mut meter,
            )
            .is_err()
        );
        assert!(
            generation_from_payload(
                AuthorityId::from_bytes([0x73; 16]),
                Sequence::new(2),
                &publication,
                &mut meter,
            )
            .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn generation_owner_guard_seals_after_planning_and_on_recovery()
    -> Result<(), Box<dyn std::error::Error>> {
        let stream = Arc::new(acyclic_stream::MemoryStream::default());
        let authority = StreamAuthorityStore::new(Arc::clone(&stream));
        let cancellation = CancellationToken::new();
        let volume = crate::foundation::VolumeId::from_bytes([0x91; 16]);
        let source_authority = volume_authority_id(volume);
        let root = ObjectId { kind: ObjectKind::GenerationRoot, digest: Digest::from_bytes([0x92; 32]) };
        let source_payload = encode_volume_created(VolumeCreated {
            volume_id: volume, config: VolumeConfig::portable(Lifecycle::Ephemeral), initial_generation_root: root,
        })?;
        authority.create_authority_with_first_record(source_authority, ProposedCommit {
            operation_id: OperationId::from_bytes([0x93; 16]),
            fingerprint: Digest::from_bytes([0x94; 32]), payload: Bytes::from(source_payload),
        }, WorkBudget::UNBOUNDED, &cancellation).await?;
        let source = crate::GenerationForkSource {
            authority: source_authority, generation: GenerationId::new(root.digest),
            lineage: crate::GenerationFork::PublishedPrefix,
        };
        let destination = AuthorityId::from_bytes([0x95; 16]);
        let operation = OperationId::from_bytes([0x96; 16]);
        let budget = WorkBudget { allocation_operations: 10_000, ..WorkBudget::UNBOUNDED };
        let mut guard = |remaining: WorkBudget| {
            assert!(remaining.allocation_operations < budget.allocation_operations);
            assert!(remaining.backend_read_operations < budget.backend_read_operations);
            std::future::ready(Err::<crate::FsReceipt<()>, _>(OperationFailure::new(
                crate::FsError::WorkspaceDeleted,
                WorkCounters { items_examined: 1, ..WorkCounters::default() },
            )))
        };
        let failed = authority.fork_generation_authority_guarded(
            source, destination, operation, budget, &cancellation, &mut guard,
        ).await.expect_err("deleted destination owner seals after preparation");
        assert!(matches!(failed.error, crate::FsError::WorkspaceDeleted));
        assert!(failed.work.backend_read_operations > 0);
        assert!(failed.work.allocation_operations > 0);
        assert_eq!(failed.work.backend_write_operations, 0);
        assert!(matches!(acyclic_stream::StreamProvider::tail(
            stream.as_ref(), stream_path(destination, "")?,
        ).await, Err(acyclic_stream::StreamError::NotFound)));
        authority.fork_generation_authority(
            source, destination, operation, budget, &cancellation,
        ).await?;
        let retried = authority.fork_generation_authority_guarded(
            source, destination, operation, budget, &cancellation, &mut guard,
        ).await.expect_err("existing destination requires a fresh recovery seal");
        assert!(matches!(retried.error, crate::FsError::WorkspaceDeleted));
        assert!(retried.work.backend_read_operations > failed.work.backend_read_operations);
        assert_eq!(retried.work.backend_write_operations, 0);
        Ok(())
    }
    #[tokio::test]
    async fn native_first_record_seals_prepared_request_and_ack_recovery()
    -> Result<(), Box<dyn std::error::Error>> {
        let stream = Arc::new(acyclic_stream::MemoryStream::default());
        let authority = StreamAuthorityStore::new(Arc::clone(&stream));
        let cancellation = CancellationToken::new();
        let destination = AuthorityId::from_bytes([0xa1; 16]);
        let proposed = || ProposedCommit {
            operation_id: OperationId::from_bytes([0xa2; 16]),
            fingerprint: Digest::from_bytes([0xa3; 32]),
            payload: Bytes::from_static(b"owned-canonical-first-record"),
        };
        let budget = WorkBudget { allocation_operations: 10_000, ..WorkBudget::UNBOUNDED };
        let mut reject_prepared = |remaining: WorkBudget| {
            assert!(remaining.allocation_operations < budget.allocation_operations);
            assert!(remaining.bytes_hashed < budget.bytes_hashed);
            std::future::ready(Err::<crate::FsReceipt<()>, _>(OperationFailure::before_work(crate::FsError::WorkspaceDeleted)))
        };
        let rejected = authority.create_authority_with_first_record_guarded(
            destination, proposed(), budget, &cancellation, &mut reject_prepared,
        ).await.expect_err("owner rejected the actual prepared canonical request");
        assert!(matches!(rejected.error, crate::FsError::WorkspaceDeleted));
        assert!(rejected.work.bytes_encoded > 0);
        assert!(rejected.work.bytes_hashed > 0);
        assert_eq!(rejected.work.backend_write_operations, 0);
        assert!(matches!(acyclic_stream::StreamProvider::tail(
            stream.as_ref(), stream_path(destination, "")?,
        ).await, Err(acyclic_stream::StreamError::NotFound)));
        assert!(authority.create_authority_with_first_record(
            destination, proposed(), budget, &cancellation,
        ).await?.value);
        let mut seals = 0;
        let mut reject_recovery = |_| {
            seals += 1;
            std::future::ready(if seals == 1 {
                Ok(crate::OperationReceipt { value: (), work: WorkCounters::default() })
            } else {
                Err(OperationFailure::before_work(crate::FsError::WorkspaceDeleted))
            })
        };
        let recovered = authority.create_authority_with_first_record_guarded(
            destination, proposed(), budget, &cancellation, &mut reject_recovery,
        ).await.expect_err("retained ACK also requires the current owner seal");
        assert!(matches!(recovered.error, crate::FsError::WorkspaceDeleted));
        assert!(recovered.work.authority_records_appended > 0);
        Ok(())
    }
    #[tokio::test]
    async fn readonly_reservation_recovers_original_head_after_authority_advance()
    -> Result<(), Box<dyn std::error::Error>> {
        let stream = Arc::new(acyclic_stream::MemoryStream::default());
        let authority = StreamAuthorityStore::new(Arc::clone(&stream));
        let cancellation = CancellationToken::new();
        let authority_id = AuthorityId::from_bytes([0xb1; 16]);
        let operation_id = OperationId::from_bytes([0xb2; 16]);
        authority.create_authority(authority_id, Epoch::GENESIS, WorkBudget::UNBOUNDED, &cancellation).await?;
        let original_head = authority.head(authority_id, WorkBudget::UNBOUNDED, &cancellation).await?.value;
        let reservation = match authority.reserve_publication(
            authority_id, original_head, operation_id, WorkBudget::UNBOUNDED, &cancellation,
        ).await?.value {
            ReservationOutcome::Reserved(reservation) => reservation,
            other => panic!("unexpected acquisition: {other:?}"),
        };
        authority.compare_and_append_guarded(crate::GuardedAppend {
            authority_id, epoch: original_head.epoch, expected: original_head,
            commit: ProposedCommit {
                operation_id: OperationId::from_bytes([0xb3; 16]),
                fingerprint: Digest::from_bytes([0xb4; 32]),
                payload: Bytes::from_static(b"original-reserved-operation-effect"),
            },
            permit: PublicationPermit::Reservation {
                operation_id: operation_id.into_bytes(), gate_tail: reservation.gate_tail, expected: original_head,
            },
        }, WorkBudget::UNBOUNDED, &cancellation).await?;
        let advanced_head = authority.head(authority_id, WorkBudget::UNBOUNDED, &cancellation).await?.value;
        assert_ne!(advanced_head, original_head);
        let spy = Arc::new(ReleaseSpy::new(Arc::clone(&stream)));
        let queried = StreamAuthorityStore::new(Arc::clone(&spy));
        let denied = queried.publication_reservation(
            authority_id, operation_id,
            WorkBudget { allocation_operations: 0, ..WorkBudget::UNBOUNDED }, &cancellation,
        ).await.expect_err("no path allocation admitted");
        assert!(matches!(denied.error, AuthorityStoreError::Work(_)));
        assert_eq!(spy.reads.load(std::sync::atomic::Ordering::SeqCst), 0);
        let recovered = queried.publication_reservation(
            authority_id, operation_id, WorkBudget::UNBOUNDED, &cancellation,
        ).await?;
        assert_eq!(recovered.value, Some(reservation));
        assert_eq!(recovered.value.expect("original evidence").expected, original_head);
        assert_eq!(recovered.work.backend_write_operations, 0);
        assert!(recovered.work.bytes_hashed > 0);
        assert_eq!(spy.writes.load(std::sync::atomic::Ordering::SeqCst), 0);
        let refused = queried.publication_reservation(
            authority_id, operation_id,
            WorkBudget { bytes_hashed: 0, ..WorkBudget::UNBOUNDED }, &cancellation,
        ).await.expect_err("query proof hashing refused after actual reads");
        assert!(matches!(refused.error, AuthorityStoreError::Work(_)));
        assert!(refused.work.backend_read_operations > 0);
        assert_eq!(refused.work.bytes_hashed, 0);
        let mut corrupted = ReleaseSpy::new(Arc::clone(&stream));
        corrupted.corrupt_observation = 10;
        let uncertain = StreamAuthorityStore::new(Arc::new(corrupted)).publication_reservation(
            authority_id, operation_id, WorkBudget::UNBOUNDED, &cancellation,
        ).await.expect_err("unqualified historical conditions cannot create a proof");
        assert!(matches!(uncertain.error, AuthorityStoreError::ReservationOriginalHeadUnavailable {
            authority_id: actual_authority, operation_id: actual_operation, gate_tail,
        } if actual_authority == authority_id && actual_operation == operation_id && gate_tail == reservation.gate_tail));
        assert_eq!(uncertain.work.backend_write_operations, 0);
        assert!(uncertain.work.bytes_hashed > 0);
        authority.release_publication(reservation, WorkBudget::UNBOUNDED, &cancellation).await?;
        let inactive = queried.publication_reservation(
            authority_id, operation_id, WorkBudget::UNBOUNDED, &cancellation,
        ).await?;
        assert_eq!(inactive.value, None);
        assert_eq!(inactive.work.backend_write_operations, 0);
        assert_eq!(spy.writes.load(std::sync::atomic::Ordering::SeqCst), 0);
        Ok(())
    }
}
