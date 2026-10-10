//! Finite per-attempt accounting over the Filesystem's measured engine APIs.
use super::*;
use crate::conversation::{FileDescriptor, FileRef, VolumeOwner};
use acyclic_fs::{CancellationToken, OperationFailure, OperationReceipt, WorkBudget, WorkCounters, WorkspaceError};
use std::sync::Mutex;

/// The pinned Filesystem creation contract interprets maximum_work as the
/// allowance for each SDK-observed non-byte counter, and maximum_bytes as the
/// allowance for each SDK-observed byte counter. Opaque provider work is not
/// invented as physical I/O; receipts aggregate across the complete attempt.
pub(crate) struct PreparationIo {
    ledger: PreparationLedger,
    pub(crate) cancellation: CancellationToken,
}

enum PreparationLedger {
    Owned { budget: WorkBudget, work: Mutex<(WorkCounters, u64)> },
    #[cfg(not(target_arch = "wasm32"))]
    Native { work: acyclic_fs::NativeMaterializationWork, custody: Mutex<acyclic_fs::NativeAllocation> },
}

/// A real logical quota reservation spanning an audited opaque serde decode.
/// It is not a claim about allocator calls, allocated bytes, or RSS.
pub(crate) struct DecodeReservation<'a> {
    io: &'a PreparationIo,
    bytes: u64,
}

impl DecodeReservation<'_> {
    pub(crate) fn substitute(&mut self, retained: u64) -> Result<()> {
        self.io.check_cancellation()?;
        if retained > self.bytes {
            return Err(Error::Invalid("audited decoded ownership exceeds reservation".into()));
        }
        self.io.release(self.bytes - retained)?;
        self.bytes = 0;
        Ok(())
    }
    /// Transfers an already admitted allowance to returned SDK-owned custody.
    pub(crate) fn into_retained(mut self) {
        self.bytes = 0;
    }
}

impl Drop for DecodeReservation<'_> {
    fn drop(&mut self) {
        self.io.release(self.bytes).expect("decode reservation belongs to its live ledger");
    }
}

/// Audited closed Harness DTOs only, including their Value/string/vector
/// containers. This is not an arbitrary DeserializeOwned heap sandbox.
pub(crate) trait PreparationRecord: serde::de::DeserializeOwned + Serialize {
    fn maximum_slot_bytes() -> u64;
}

pub(crate) fn preparation_slot_bytes<T>() -> u64 {
    [
        std::mem::size_of::<T>(),
        std::mem::size_of::<crate::model::ModelMessage>(),
        std::mem::size_of::<crate::model::ModelContentPart>(),
        std::mem::size_of::<crate::model::NativeConfigurationBinding>(),
        std::mem::size_of::<crate::tool::ToolDefinition>(),
        std::mem::size_of::<crate::conversation::Attachment>(),
        std::mem::size_of::<FileRef>(),
        std::mem::size_of::<serde_json::Value>(),
        std::mem::size_of::<(String, serde_json::Value)>(),
        std::mem::size_of::<(String, FileRef)>(),
        std::mem::size_of::<crate::workflow::WorkflowCommand>(),
        std::mem::size_of::<crate::workflow::WorkflowRecord>(),
        std::mem::size_of::<crate::workflow::MachineCheckpoint>(),
        std::mem::size_of::<crate::workflow::MachineTransition>(),
    ].into_iter().max().unwrap_or(0) as u64
}

macro_rules! audited_preparation_records {
    ($($record:ty),+ $(,)?) => {$(
        impl PreparationRecord for $record {
            fn maximum_slot_bytes() -> u64 { preparation_slot_bytes::<Self>() }
        }
    )+};
}

audited_preparation_records!(
    crate::context::Context,
    crate::context::CanonicalContextCheckpoint,
    crate::context::CompactionReference,
    crate::executor::ResponseProjection,
    crate::model::ModelEvent,
    crate::model::ModelRequest,
    crate::model::ModelContent,
    crate::model::ModelContentPart,
    crate::fork::InheritedConversationPrefix,
    serde_json::Value,
    crate::core::ExtensionConfiguration,
    FileRef,
    crate::workflow::WorkflowRecord,
    crate::workflow::WorkflowAdmission,
);

pub(crate) struct ReservedDecoded<'a, T> {
    pub(crate) value: T,
    _reservation: DecodeReservation<'a>,
}

impl<T> std::ops::Deref for ReservedDecoded<'_, T> {
    type Target = T;
    fn deref(&self) -> &T { &self.value }
}

impl<T> std::ops::DerefMut for ReservedDecoded<'_, T> {
    fn deref_mut(&mut self) -> &mut T { &mut self.value }
}

impl<T> ReservedDecoded<'_, T> {
    pub(crate) fn into_inner(self) -> T {
        let Self { value, _reservation } = self;
        drop(_reservation);
        value
    }

    pub(crate) fn into_retained(mut self) -> T {
        // Transfer actual held logical quota to this attempt's DTO custody.
        // It is conservative allowance, not an allocator-capacity assertion.
        self._reservation.bytes = 0;
        self.value
    }
}
impl PreparationIo {
    pub(crate) fn new(request: &crate::resource_preparation::ResourcePreparationRequest) -> Result<Self> {
        Self::new_with_cancellation(request, &CancellationToken::new())
    }

    pub(crate) fn new_with_cancellation(
        request: &crate::resource_preparation::ResourcePreparationRequest,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        request.validate()?;
        let units = request.maximum_work;
        let bytes = request.maximum_bytes;
        let budget = WorkBudget {
            authority_records_read: units, authority_records_appended: units,
            authority_bytes_read: bytes, authority_bytes_written: bytes,
            object_probes: units, backend_read_operations: units, backend_write_operations: units,
            durability_operations: units, page_reads: units, page_writes: units,
            object_bytes_read: bytes, object_bytes_written: bytes,
            bytes_hashed: bytes, bytes_copied: bytes, bytes_encoded: bytes,
            source_bytes_read: bytes, source_path_components: units, source_entries_visited: units,
            output_bytes: bytes, items_examined: units, items_returned: units,
            allocation_operations: units, peak_allocation_bytes: bytes, materializations: units,
        };
        Self::from_budget(budget, cancellation)
    }

    pub(crate) fn from_budget(budget: WorkBudget, cancellation: &CancellationToken) -> Result<Self> {
        if !budget.has_finite_allowances() {
            return Err(Error::Invalid("measured content requires finite work allowances".into()));
        }
        cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
        Ok(Self { ledger: PreparationLedger::Owned { budget, work: Mutex::new((WorkCounters::default(), 0)) }, cancellation: cancellation.clone() })
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn from_native_work(work: &acyclic_fs::NativeMaterializationWork) -> Result<Self> {
        work.remaining().map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let custody = work.retain_owned(0).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        Ok(Self {
            ledger: PreparationLedger::Native { work: work.clone(), custody: Mutex::new(custody) },
            cancellation: work.cancellation().clone(),
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn into_native_custody(self, retained: u64) -> Result<acyclic_fs::NativeAllocation> {
        match self.ledger {
            PreparationLedger::Native { custody, .. } => {
                let mut guard = custody.into_inner().unwrap_or_else(std::sync::PoisonError::into_inner);
                guard.substitute(retained).map_err(|cause| Error::NativeWork { cause, custody: None })?;
                Ok(guard)
            }
            PreparationLedger::Owned { .. } => Err(Error::Invalid("native custody requires the original native ledger".into())),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn original_native_work(&self) -> Option<&acyclic_fs::NativeMaterializationWork> {
        match &self.ledger {
            PreparationLedger::Native { work, .. } => Some(work),
            PreparationLedger::Owned { .. } => None,
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn absorb_native_custody(&self, incoming: acyclic_fs::NativeAllocation) -> Result<()> {
        match &self.ledger {
            PreparationLedger::Native { custody, .. } => custody.lock()
                .map_err(|_| Error::Storage("native preparation custody is poisoned".into()))?
                .absorb(incoming).map_err(|cause| Error::NativeWork { cause, custody: None }),
            PreparationLedger::Owned { .. } => Err(Error::Invalid("native custody belongs to another ledger".into())),
        }
    }

    pub(crate) fn check_cancellation(&self) -> Result<()> {
        match self.cancellation.check() {
            Ok(()) => Ok(()),
            Err(error) => {
                #[cfg(not(target_arch = "wasm32"))]
                if matches!(&self.ledger, PreparationLedger::Native { .. }) {
                    return Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, custody: None });
                }
                Err(Error::Storage(error.to_string()))
            }
        }
    }

    pub(crate) fn receipt(&self) -> WorkCounters {
        match &self.ledger {
            PreparationLedger::Owned { work, .. } => work.lock().unwrap_or_else(std::sync::PoisonError::into_inner).0,
            #[cfg(not(target_arch = "wasm32"))]
            PreparationLedger::Native { work, .. } => work.receipt(),
        }
    }
    /// This adapter's current live custody, not the whole native operation's.
    pub(crate) fn owned_bytes(&self) -> Result<u64> {
        match &self.ledger {
            PreparationLedger::Owned { work, .. } => work.lock().map(|state| state.1)
                .map_err(|_| Error::Storage("creation IO accounting is poisoned".into())),
            #[cfg(not(target_arch = "wasm32"))]
            PreparationLedger::Native { custody, .. } => custody.lock().map(|guard| guard.bytes())
                .map_err(|_| Error::Storage("native preparation custody is poisoned".into())),
        }
    }


    // Honest ordinary convenience uses the same engine without pretending its
    // allowance is finite. Measured public entry points use from_budget.
    pub(crate) fn for_unmeasured(cancellation: &CancellationToken) -> Result<Self> {
        cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
        Ok(Self {
            ledger: PreparationLedger::Owned { budget: WorkBudget::UNBOUNDED, work: Mutex::new((WorkCounters::default(), 0)) },
            cancellation: cancellation.clone(),
        })
    }

    pub(crate) fn preflight(&self, next: WorkCounters) -> Result<()> {
        self.check_cancellation()?;
        match &self.ledger {
            PreparationLedger::Owned { budget, .. } => self.receipt().checked_add(next).and_then(|work| work.verify(*budget))
                .map_err(|error| Error::Invalid(error.to_string())),
            #[cfg(not(target_arch = "wasm32"))]
            PreparationLedger::Native { work, .. } => work.admit(next).map_err(|cause| Error::NativeWork { cause, custody: None }),
        }
    }

    pub(crate) fn claim(&self, bytes: u64) -> Result<()> {
        self.check_cancellation()?;
        match &self.ledger {
            PreparationLedger::Owned { budget, work } => {
                let mut state = work.lock().map_err(|_| Error::Storage("creation IO accounting is poisoned".into()))?;
                let live = state.1.checked_add(bytes).ok_or_else(|| Error::Invalid("content ownership overflow".into()))?;
                let next = state.0.checked_add(WorkCounters { peak_allocation_bytes: live, ..Default::default() })
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                next.verify(*budget).map_err(|error| Error::Invalid(error.to_string()))?;
                *state = (next, live);
                Ok(())
            }
            #[cfg(not(target_arch = "wasm32"))]
            PreparationLedger::Native { custody, .. } => {
                let mut guard = custody.lock().map_err(|_| Error::Storage("native preparation custody is poisoned".into()))?;
                let live = guard.bytes().checked_add(bytes).ok_or_else(|| Error::Invalid("native preparation ownership overflow".into()))?;
                guard.resize_retained(live).map_err(|cause| Error::NativeWork { cause, custody: None })
            }
        }
    }

    pub(crate) fn allocation_attempt(&self, bytes: u64) -> Result<()> {
        let work = WorkCounters { allocation_operations: u64::from(bytes != 0), ..Default::default() };
        self.preflight(work)?;
        self.charge(work)
    }

    pub(crate) fn retain_owned(&self, bytes: u64) -> Result<()> {
        self.claim(bytes)
    }

    pub(crate) fn release(&self, bytes: u64) -> Result<()> {
        match &self.ledger {
            PreparationLedger::Owned { work, .. } => {
                let mut state = work.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                state.1 = state.1.checked_sub(bytes).ok_or_else(|| Error::Invalid("content ownership underflow".into()))?;
                Ok(())
            }
            #[cfg(not(target_arch = "wasm32"))]
            PreparationLedger::Native { custody, .. } => custody.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
                .release(bytes).map_err(|cause| Error::NativeWork { cause, custody: None }),
        }
    }

    pub(crate) fn reserve_decode(&self, bytes: u64) -> Result<DecodeReservation<'_>> {
        self.claim(bytes)?;
        Ok(DecodeReservation { io: self, bytes })
    }

    pub(crate) fn guard_owned(&self, bytes: u64) -> Result<DecodeReservation<'_>> {
        if bytes > self.owned_bytes()? { return Err(Error::Invalid("owned buffer exceeds retained quota".into())); }
        Ok(DecodeReservation { io: self, bytes })
    }

    pub(crate) fn remaining(&self) -> Result<WorkBudget> {
        self.check_cancellation()?;
        match &self.ledger {
            PreparationLedger::Owned { budget, work } => {
                let state = work.lock().map_err(|_| Error::Storage("creation IO accounting is poisoned".into()))?;
                let mut remaining = state.0.remaining(*budget).map_err(|error| Error::Invalid(error.to_string()))?;
                remaining.peak_allocation_bytes = remaining.peak_allocation_bytes.checked_sub(state.1)
                    .ok_or_else(|| Error::Invalid("content ownership exceeds allowance".into()))?;
                Ok(remaining)
            }
            #[cfg(not(target_arch = "wasm32"))]
            PreparationLedger::Native { work, .. } => work.remaining().map_err(|cause| Error::NativeWork { cause, custody: None }),
        }
    }

    pub(crate) fn charge(&self, next: WorkCounters) -> Result<()> {
        self.spent(next)?;
        match &self.ledger {
            PreparationLedger::Owned { budget, .. } => self.receipt().verify(*budget).map_err(|error| Error::Invalid(error.to_string())),
            #[cfg(not(target_arch = "wasm32"))]
            PreparationLedger::Native { work, .. } => work.remaining().map(|_| ()).map_err(|cause| Error::NativeWork { cause, custody: None }),
        }
    }

    pub(crate) fn spent(&self, mut next: WorkCounters) -> Result<()> {
        match &self.ledger {
            PreparationLedger::Owned { work, .. } => {
                let mut state = work.lock().map_err(|_| Error::Storage("creation IO accounting is poisoned".into()))?;
                next.peak_allocation_bytes = next.peak_allocation_bytes.checked_add(state.1)
                    .ok_or_else(|| Error::Invalid("content ownership overflow".into()))?;
                state.0 = state.0.checked_add(next).map_err(|error| Error::Invalid(error.to_string()))?;
                Ok(())
            }
            #[cfg(not(target_arch = "wasm32"))]
            PreparationLedger::Native { work, .. } => work.spent(next).map_err(|cause| Error::NativeWork { cause, custody: None }),
        }
    }

    pub(crate) fn receive<T>(&self, result: std::result::Result<OperationReceipt<T>, OperationFailure<WorkspaceError>>) -> Result<T> {
        match result {
            Ok(receipt) => { self.charge(receipt.work)?; Ok(receipt.value) }
            Err(failure) => {
                self.spent(*failure.work)?;
                #[cfg(not(target_arch = "wasm32"))]
                if matches!(&self.ledger, PreparationLedger::Native { .. }) {
                    return Err(match failure.error {
                        WorkspaceError::Work(cause) => Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Work(cause), custody: None },
                        WorkspaceError::Cancelled(_) => Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, custody: None },
                        cause => map_error(cause),
                    });
                }
                Err(map_error(failure.error))
            }
        }
    }

    pub(crate) fn receive_content<T>(&self, result: acyclic_fs::MeasuredResult<OperationReceipt<T>, Error>) -> Result<T> {
        match result {
            Ok(receipt) => { self.charge(receipt.work)?; Ok(receipt.value) }
            Err(failure) => { self.spent(*failure.work)?; Err(failure.error) }
        }
    }

    pub(crate) fn encode<T: Serialize>(&self, value: &T, maximum_bytes: u64) -> Result<Vec<u8>> {
        self.encode_json(value, maximum_bytes, false)
    }

    pub(crate) fn encode_json<T: Serialize>(&self, value: &T, maximum_bytes: u64, canonical: bool) -> Result<Vec<u8>> {
        let remaining = self.remaining()?;
        let bounds = crate::contract::JsonBounds {
            maximum_bytes: maximum_bytes.min(remaining.bytes_encoded),
            maximum_items: remaining.items_examined,
            maximum_depth: 256,
        };
        let mut charge = |next| self.codec_charge(next, true);
        if canonical {
            crate::contract::canonical_json_bytes_bounded(value, bounds, &mut charge)
        } else {
            crate::contract::json_bytes_bounded_trusted(value, bounds, &mut charge)
        }
    }

    pub(crate) fn encode_for_clone<T: Serialize>(&self, value: &T, maximum_bytes: u64) -> Result<Vec<u8>> {
        let remaining = self.remaining()?;
        let bounds = crate::contract::JsonBounds {
            maximum_bytes: maximum_bytes.min(remaining.bytes_encoded),
            maximum_items: remaining.items_examined, maximum_depth: 256,
        };
        let mut transferred = DecodeReservation { io: self, bytes: 0 };
        let encoded = crate::contract::canonical_json_bytes_bounded(value, bounds, &mut |next| {
            match next {
                crate::contract::JsonCodecCharge::Release { .. } => Ok(()),
                crate::contract::JsonCodecCharge::Retain { bytes } => {
                    self.codec_charge(next, true)?;
                    transferred.bytes = transferred.bytes.checked_add(bytes)
                        .ok_or_else(|| Error::Invalid("clone admission overflow".into()))?;
                    Ok(())
                }
                next => self.codec_charge(next, true),
            }
        })?;
        // The concrete Context clone consumes this admitted ownership. Failure
        // instead drops the guard without consulting cancellation or authority.
        transferred.bytes = 0;
        Ok(encoded)
    }

    pub(crate) fn capability<T: Serialize>(&self, value: &T, prefix: &str) -> Result<String> {
        let _encoded_hold;
        let encoded = self.encode_json(value, self.remaining()?.bytes_encoded, true)?;
        _encoded_hold = self.guard_owned(encoded.len() as u64)?;
        let hash = WorkCounters { bytes_hashed: encoded.len() as u64, ..Default::default() };
        self.preflight(hash)?;
        let digest = blake3::hash(&encoded);
        self.charge(hash)?;
        let length = prefix.len().checked_add(64)
            .ok_or_else(|| Error::Invalid("capability length overflow".into()))?;
        self.claim(length as u64)?;
        self.allocation_attempt(length as u64)?;
        let mut key = String::with_capacity(length);
        key.push_str(prefix);
        key.push_str(&digest.to_hex());
        Ok(key)
    }

    pub(crate) fn contains_capability<T: Serialize>(
        &self, grants: &crate::Capabilities, value: &T, prefix: &str,
    ) -> Result<bool> {
        let key_hold;
        let key = self.capability(value, prefix)?;
        key_hold = self.guard_owned(key.capacity() as u64)?;
        let granted = grants.contains(&key);
        drop(key);
        drop(key_hold);
        Ok(granted)
    }

    // Audited closed serde derives only: upload records and approval-only wires.
    // Their strings/key vectors occupy at most twice encoded input bytes plus
    // the concrete root body; opaque allocator nodes and RSS are excluded.
    pub(crate) fn decode_json<T: UploadRecord>(&self, bytes: &[u8]) -> Result<T> {
        let remaining = self.remaining()?;
        let owned = (bytes.len() as u64).checked_mul(2)
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<T>() as u64))
            .ok_or_else(|| Error::Invalid("upload decode ownership overflow".into()))?;
        let bounds = crate::contract::JsonBounds {
            maximum_bytes: bytes.len() as u64, maximum_items: remaining.items_examined, maximum_depth: 256,
        };
        let mut reservation = None;
        let value: T = crate::contract::json_from_slice_bounded_trusted(bytes, bounds, owned,
            &mut |next| match next {
                crate::contract::JsonCodecCharge::Admission { maximum_owned_bytes } => {
                    reservation = Some(self.reserve_decode(maximum_owned_bytes)?);
                    Ok(())
                }
                next => self.codec_charge(next, false),
            })?;
        reservation.as_mut().ok_or_else(|| Error::Invalid("trusted decode omitted admission".into()))?
            .substitute(value.logical_owned_bytes()?)?;
        Ok(value)
    }

    pub(crate) fn decode_preparation<T: PreparationRecord>(
        &self, bytes: &[u8],
    ) -> Result<ReservedDecoded<'_, T>> {
        let bounds = crate::contract::JsonBounds {
            maximum_bytes: bytes.len() as u64,
            maximum_items: self.remaining()?.items_examined,
            maximum_depth: 128,
        };
        let mut items = 0_u64;
        let mut reservation = None;
        let value = crate::contract::json_from_slice_bounded_trusted(bytes, bounds, 0, &mut |next| {
            match next {
                crate::contract::JsonCodecCharge::Work { items: count, .. } => {
                    items = items.checked_add(count)
                        .ok_or_else(|| Error::Invalid("preparation DTO item count overflow".into()))?;
                    self.codec_charge(next, false)
                }
                crate::contract::JsonCodecCharge::Admission { maximum_owned_bytes } => {
                    // Concrete slots cover initial/geometric Vec slack and the
                    // internally-tagged serde temporary representation. Strings
                    // and scratch are separate; allocator nodes/RSS excluded.
                    let owned = items.checked_mul(T::maximum_slot_bytes())
                        .and_then(|slots| slots.checked_mul(8))
                        .and_then(|slots| (bytes.len() as u64).checked_mul(4)
                            .and_then(|strings| slots.checked_add(strings)))
                        .and_then(|owned| owned.checked_add(maximum_owned_bytes))
                        .and_then(|owned| owned.checked_add(std::mem::size_of::<T>() as u64))
                        .ok_or_else(|| Error::Invalid("preparation DTO ownership overflow".into()))?;
                    reservation = Some(self.reserve_decode(owned)?);
                    Ok(())
                }
                next => self.codec_charge(next, false),
            }
        })?;
        let _reservation = reservation
            .ok_or_else(|| Error::Invalid("preparation DTO decode omitted reservation".into()))?;
        Ok(ReservedDecoded { value, _reservation })
    }

    pub(crate) fn codec_charge(&self, next: crate::contract::JsonCodecCharge, encode: bool) -> Result<()> {
        match next {
            crate::contract::JsonCodecCharge::Retain { bytes } => {
                // Codec reports finite logical ownership, not opaque allocator
                // operations. Never invent one allocation per logical node.
                self.retain_owned(bytes)
            }
            crate::contract::JsonCodecCharge::Release { bytes } => self.release(bytes),
            crate::contract::JsonCodecCharge::Admission { .. } => Err(Error::Invalid(
                "trusted decode admission requires a scoped reservation".into())),
            crate::contract::JsonCodecCharge::Copy { bytes } => {
                let work = WorkCounters { bytes_copied: bytes, ..Default::default() };
                self.preflight(work)?;
                self.charge(work)
            }
            crate::contract::JsonCodecCharge::Read { bytes } => {
                let work = WorkCounters { source_bytes_read: bytes, ..Default::default() };
                self.preflight(work)?;
                self.charge(work)
            }
            crate::contract::JsonCodecCharge::Work { bytes, items } => {
                let next = WorkCounters {
                    bytes_encoded: if encode { bytes } else { 0 },
                    source_bytes_read: if encode { 0 } else { bytes },
                    items_examined: items, ..Default::default()
                };
                self.preflight(next)?;
                self.charge(next)
            }
        }
    }
}

pub(crate) trait UploadRecord: serde::de::DeserializeOwned {
    fn logical_owned_bytes(&self) -> Result<u64>;
}

impl UploadRecord for FileRef {
    fn logical_owned_bytes(&self) -> Result<u64> {
        FileRef::logical_owned_bytes(self)
    }
}

impl UploadRecord for (FileRef, Option<crate::resources::GenerationRef>) {
    fn logical_owned_bytes(&self) -> Result<u64> {
        let mut bytes = self.0.logical_owned_bytes()?;
        if let Some(generation) = &self.1 {
            bytes = bytes.checked_add(generation.logical_owned_bytes()?)
                .ok_or_else(|| Error::Invalid("upload generation ownership overflow".into()))?;
        }
        Ok(bytes)
    }
}

pub(in crate::filesystem) fn strings(io: &PreparationIo, values: &[&str]) -> Result<()> {
    for value in values {
        io.claim(value.len() as u64)?;
    }
    Ok(())
}

pub(in crate::filesystem) fn file_ref(
    volume: &VolumeRef, path: &str, version: &str, descriptor: &FileDescriptor,
    display_name: &str, io: Option<&PreparationIo>,
) -> Result<FileRef> {
    if let Some(io) = io {
        let provider = volume.provider();
        strings(io, &[provider.namespace(), provider.family(), provider.version(), volume.id(),
            path, version, descriptor.media_type(), display_name])?;
        if let VolumeOwner::Project(owner) | VolumeOwner::Session(owner) = volume.owner() {
            strings(io, &[owner])?;
        }
    }
    FileRef::new(volume.clone(), path, version, descriptor.clone(), display_name)
}

pub(in crate::filesystem) fn clone_file(reference: &FileRef, io: Option<&PreparationIo>) -> Result<FileRef> {
    file_ref(reference.volume(), reference.path(), reference.version(), reference.descriptor(), reference.display_name(), io)
}

pub(in crate::filesystem) fn reference_key(reference: &FileRef, io: Option<&PreparationIo>) -> Result<String> {
    match io {
        Some(io) => io.capability(reference, "file:read:"),
        None => reference.read_capability(),
    }
}

pub(in crate::filesystem) fn item(io: Option<&PreparationIo>, logical_bytes: u64) -> Result<()> {
    if let Some(io) = io {
        let work = WorkCounters { items_examined: 1, ..Default::default() };
        io.preflight(work)?;
        io.charge(work)?;
        io.claim(logical_bytes)?;
    }
    Ok(())
}

pub(in crate::filesystem) async fn operation_generation<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>, reference: &WorkspaceRef, key: &IdempotencyKey, io: Option<&PreparationIo>,
) -> Result<Option<acyclic_fs::Generation<A, O>>> {
    match io {
        Some(io) => {
            let workspace = open(host, reference, io).await?;
            let hash = WorkCounters { bytes_hashed: key.as_str().len() as u64, ..Default::default() };
            io.preflight(hash)?;
            io.charge(hash)?;
            io.receive(workspace.operation_generation_measured(super::super::filesystem_key(key), io.remaining()?, &io.cancellation).await)
        }
        None => host.open(reference).await?.operation_generation(super::super::filesystem_key(key)).await.map_err(map_error),
    }
}

pub(in crate::filesystem) fn content_workspace(
    host_provider: &crate::resources::ProviderRef, volume: &VolumeRef,
    io: Option<&PreparationIo>,
) -> Result<WorkspaceRef> {
    let _name_hold;
    let name = volume_storage_name(volume, io)?;
    _name_hold = io.map(|io| io.guard_owned(name.len() as u64)).transpose()?;
    if let Some(io) = io {
        strings(io, &[host_provider.namespace(), host_provider.family(), host_provider.version()])?;
        io.claim(name.len() as u64)?;
    }
    workspace_ref(host_provider.clone(), &name)
}

pub(in crate::filesystem) fn volume_storage_name(
    volume: &VolumeRef, io: Option<&PreparationIo>,
) -> Result<String> {
    let Some(io) = io else { return volume.storage_name(); };
    let _encoded_hold;
    let encoded = io.encode_json(volume, io.remaining()?.bytes_encoded, true)?;
    _encoded_hold = io.guard_owned(encoded.len() as u64)?;
    let hash = WorkCounters { bytes_hashed: encoded.len() as u64, ..Default::default() };
    io.preflight(hash)?;
    let digest = blake3::hash(&encoded);
    io.charge(hash)?;
    io.claim(72)?;
    io.allocation_attempt(72)?;
    let mut name = String::with_capacity(72);
    name.push_str("harness-");
    name.push_str(&digest.to_hex());
    Ok(name)
}

pub(crate) async fn open<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>, reference: &WorkspaceRef, io: &PreparationIo,
) -> Result<acyclic_fs::Workspace<A, O>> {
    reference.validate()?;
    host.validate_provider(reference.as_resource().provider())?;
    let name = std::str::from_utf8(reference.as_resource().key())
        .map_err(|_| Error::Invalid("workspace reference key must be UTF-8".into()))?;
    io.receive(host.filesystem.open_workspace_measured(name, io.remaining()?, &io.cancellation).await)
}

pub(crate) async fn generation<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>, workspace: &acyclic_fs::Workspace<A, O>,
    reference: &crate::resources::GenerationRef, io: &PreparationIo,
) -> Result<acyclic_fs::Generation<A, O>> {
    reference.validate()?;
    host.validate_provider(reference.as_resource().provider())?;
    let bytes: [u8; 32] = reference.as_resource().key().try_into()
        .map_err(|_| Error::Invalid("generation reference key must be 32 bytes".into()))?;
    io.receive(workspace.generation_measured(
        acyclic_fs::GenerationId::new(acyclic_fs::Digest::from_bytes(bytes)),
        io.remaining()?, &io.cancellation,
    ).await)
}

pub(in crate::filesystem) async fn read<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>, reference: &WorkspaceRef, path: &str, maximum_bytes: u64,
    io: Option<&PreparationIo>,
) -> Result<bytes::Bytes> {
    let Some(io) = io else { return host.read(reference, None, path, maximum_bytes).await; };
    let workspace = open(host, reference, io).await?;
    io.receive(workspace.read_measured(path, maximum_bytes, io.remaining()?, &io.cancellation).await)
}

pub(in crate::filesystem) async fn resolve<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>, reference: &WorkspaceRef, io: Option<&PreparationIo>,
) -> Result<super::super::WorkspaceObservation> {
    let Some(io) = io else { return host.resolve(reference).await; };
    let workspace = open(host, reference, io).await?;
    let generation = io.receive(workspace.head_measured(io.remaining()?, &io.cancellation).await)?;
    let provider = reference.as_resource().provider();
    strings(io, &[provider.namespace(), provider.family(), provider.version()])?;
    io.claim(reference.as_resource().key().len() as u64)?;
    Ok(super::super::WorkspaceObservation {
        workspace: reference.clone(), generation: generation_ref(host, &generation, Some(io))?,
    })
}

pub(in crate::filesystem) async fn create<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>, name: &str, key: &IdempotencyKey, io: Option<&PreparationIo>,
) -> Result<()> {
    let Some(io) = io else {
        host.filesystem.create_workspace(name).await.map_err(map_error)?;
        return Ok(());
    };
    let operation = acyclic_fs::OperationId::from_bytes(
        super::super::filesystem_key(key).into_bytes(),
    );
    let lifecycle = if host.filesystem.capabilities().durable {
        acyclic_fs::Lifecycle::Durable
    } else {
        acyclic_fs::Lifecycle::Ephemeral
    };
    io.receive(host.filesystem.create_workspace_with_operation_measured(
        name, acyclic_fs::VolumeConfig::portable(lifecycle),
        operation, io.remaining()?, &io.cancellation,
    ).await)?;
    Ok(())
}

pub(in crate::filesystem) async fn apply<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>, reference: &WorkspaceRef,
    expected: Option<&crate::resources::GenerationRef>, mutations: &[WorkspaceMutation],
    key: &IdempotencyKey, io: Option<&PreparationIo>,
) -> Result<crate::resources::GenerationRef> {
    let Some(io) = io else { return host.apply(reference, expected, mutations, key).await; };
    let generation = apply_generation(host, reference, expected, mutations, key, io).await?;
    host.generation_ref(&generation)
}

pub(in crate::filesystem) async fn apply_generation<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>, reference: &WorkspaceRef,
    expected: Option<&crate::resources::GenerationRef>, mutations: &[WorkspaceMutation],
    key: &IdempotencyKey, io: &PreparationIo,
) -> Result<acyclic_fs::Generation<A, O>> {
    let workspace = open(host, reference, io).await?;
    let generation = match expected {
        Some(reference) => generation(host, &workspace, reference, io).await?,
        None => io.receive(workspace.head_measured(io.remaining()?, &io.cancellation).await)?,
    };
    let mut transaction = io.receive(workspace.begin_pinned_transaction_measured(
        &generation, super::super::filesystem_key(key), io.remaining()?, &io.cancellation,
    ).await)?;
    for mutation in mutations {
        match mutation {
            WorkspaceMutation::CreateDirectory { path } => {
                let result = transaction.create_dir_all_measured(path, io.remaining()?, &io.cancellation).await;
                match result {
                    Ok(work) => io.charge(work)?,
                    Err(failure) => { io.spent(*failure.work)?; return Err(map_error(failure.error)); }
                }
            }
            WorkspaceMutation::PutFile { path, bytes } => {
                io.claim(bytes.len() as u64)?;
                let copied = WorkCounters { bytes_copied: bytes.len() as u64, ..Default::default() };
                io.preflight(copied)?;
                io.allocation_attempt(bytes.len() as u64)?;
                let owned = bytes::Bytes::copy_from_slice(bytes);
                io.charge(copied)?;
                let result = transaction.put_measured(path, owned, io.remaining()?, &io.cancellation).await;
                // Candidate custody may retain the transferred buffer until commit.
                match result {
                    Ok(work) => io.charge(work)?,
                    Err(failure) => { io.spent(*failure.work)?; return Err(map_error(failure.error)); }
                }
            }
            WorkspaceMutation::Remove { .. } => return Err(Error::Unsupported("bounded publication does not remove content".into())),
        }
    }
    match io.receive(transaction.commit_with_permit_measured(
        acyclic_fs::PublicationPermit::Unrestricted, io.remaining()?, &io.cancellation,
    ).await)? {
        acyclic_fs::TransactionCommit::Committed(generation)
        | acyclic_fs::TransactionCommit::AlreadyCommitted(generation) => Ok(generation),
        acyclic_fs::TransactionCommit::Conflict { .. } => Err(Error::Conflict("workspace generation changed".into())),
        acyclic_fs::TransactionCommit::Fenced => Err(Error::Conflict("workspace writer was fenced".into())),
        acyclic_fs::TransactionCommit::IdempotencyConflict => Err(Error::Conflict("filesystem idempotency key belongs to another mutation".into())),
    }
}

pub(in crate::filesystem) fn generation_ref<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>, generation: &acyclic_fs::Generation<A, O>,
    io: Option<&PreparationIo>,
) -> Result<crate::resources::GenerationRef> {
    if let Some(io) = io {
        strings(io, &[host.provider.namespace(), host.provider.family(), host.provider.version()])?;
        io.claim(32)?;
    }
    host.generation_ref(generation)
}

pub(in crate::filesystem) async fn retain<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    _host: &FilesystemHost<A, O>, generation: &acyclic_fs::Generation<A, O>, io: &PreparationIo,
) -> Result<()> {
    io.claim(64)?;
    io.claim(("harness-generation-".len() + 64) as u64)?;
    let identity = format!("harness-generation-{}", hex::encode(generation.id().digest().into_bytes()));
    io.receive(generation.pin_measured(identity, io.remaining()?, &io.cancellation).await)?;
    Ok(())
}

pub(in crate::filesystem) async fn fork<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>, source: &VolumeRef, grant: &ContentGrant,
    source_generation: &crate::resources::GenerationRef, child: &VolumeRef,
    paths: Option<Vec<String>>, key: &IdempotencyKey, io: Option<&PreparationIo>,
) -> Result<super::super::WorkspaceObservation> {
    grant.require(source, VolumeOperation::Read)?;
    let Some(io) = io else { return host.fork_volume_pinned(source, source_generation, child, paths, key).await; };
    source.validate()?;
    child.validate()?;
    host.validate_provider(source.provider())?;
    host.validate_provider(child.provider())?;
    if source == child { return Err(Error::Invalid("volume fork requires a distinct destination".into())); }
    let source_ref = workspace_ref(host.provider.clone(), &source.storage_name()?)?;
    let workspace = open(host, &source_ref, io).await?;
    let source = generation(host, &workspace, source_generation, io).await?;
    retain(host, &source, io).await?;
    let child_name = child.storage_name()?;
    let key = super::super::filesystem_key(key);
    let options = acyclic_fs::ForkOptions::from_generation(source, key);
    let options = match paths { Some(paths) => options.inherit_paths(paths), None => options };
    let forked = io.receive(workspace.fork_measured(&child_name, options, io.remaining()?, &io.cancellation).await)?;
    let initial = io.receive(forked.operation_generation_measured(key, io.remaining()?, &io.cancellation).await)?
        .ok_or_else(|| Error::Storage("fork creation has no durable generation".into()))?;
    retain(host, &initial, io).await?;
    Ok(super::super::WorkspaceObservation {
        workspace: workspace_ref(host.provider.clone(), &child_name)?,
        generation: host.generation_ref(&initial)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conversation::{VolumeClass, VolumeOwner};
    use crate::core::{AggregateKind, AuthorityIssuer};
    use acyclic_fs::Fs;

    #[tokio::test]
    async fn exact_read_gate_precedes_backend_and_pinned_fork_never_writes_back() -> Result<()> {
        let provider = crate::resources::ProviderRef::new("source-read-control", "filesystem", "2")?;
        let host = FilesystemHost::new(Fs::memory(), provider.clone())?;
        let project = |name: &str| VolumeRef::new(provider.clone(), name, VolumeClass::Project,
            VolumeOwner::Project("source-read-control".into()));
        let source = project("selected")?;
        let caller = project("caller")?;
        let child = project("child")?;
        let denied_child = project("denied-child")?;
        let initial = host.create_volume(&source).await?;
        let caller_head = host.create_volume(&caller).await?;
        let selected_generation = host.apply(&initial.workspace, Some(&initial.generation),
            &[WorkspaceMutation::PutFile { path: "/value.txt".into(), bytes: b"selected".to_vec() }],
            &IdempotencyKey::new("selected-generation")?).await?;
        let wrong_generation = host.apply(&caller_head.workspace, Some(&caller_head.generation),
            &[WorkspaceMutation::PutFile { path: "/other.txt".into(), bytes: b"other-source".to_vec() }],
            &IdempotencyKey::new("other-source-generation")?).await?;
        let issuer = AuthorityIssuer::new("source-read-control", [41; 32], Authority {
            kind: AggregateKind::Conversation, id: "caller".into(),
        });
        let scope = issuer.root("exact-project-read", crate::Capabilities::new([
            source.capability(VolumeOperation::Read)?, caller.capability(VolumeOperation::Read)?,
        ]));
        let selected_read = ContentGrant::verify(&issuer.verifier(), &scope, &source, VolumeOperation::Read)?;
        let caller_read = ContentGrant::verify(&issuer.verifier(), &scope, &caller, VolumeOperation::Read)?;
        assert!(matches!(fork(&host, &source, &caller_read, &selected_generation, &denied_child,
            None, &IdempotencyKey::new("denied-source-read")?, None).await, Err(Error::Unauthorized(_))));
        let denied_ref = workspace_ref(provider.clone(), &denied_child.storage_name()?)?;
        assert!(host.resolve(&denied_ref).await.is_err());
        assert!(fork(&host, &source, &selected_read, &wrong_generation, &denied_child,
            None, &IdempotencyKey::new("wrong-generation")?, None).await.is_err());
        assert!(host.resolve(&denied_ref).await.is_err());
        let later_generation = host.apply(&initial.workspace, Some(&selected_generation),
            &[WorkspaceMutation::PutFile { path: "/value.txt".into(), bytes: b"later".to_vec() }],
            &IdempotencyKey::new("later-generation")?).await?;
        let forked = fork(&host, &source, &selected_read, &selected_generation, &child,
            None, &IdempotencyKey::new("pinned-selected-fork")?, None).await?;
        assert_eq!(host.read(&forked.workspace, Some(&forked.generation), "/value.txt", 32).await?,
            bytes::Bytes::from_static(b"selected"));
        host.apply(&forked.workspace, Some(&forked.generation),
            &[WorkspaceMutation::PutFile { path: "/value.txt".into(), bytes: b"child".to_vec() }],
            &IdempotencyKey::new("isolated-child-edit")?).await?;
        assert_eq!(host.resolve(&initial.workspace).await?.generation, later_generation);
        assert_eq!(host.read(&initial.workspace, None, "/value.txt", 32).await?,
            bytes::Bytes::from_static(b"later"));
        assert_eq!(host.read(&initial.workspace, Some(&selected_generation), "/value.txt", 32).await?,
            bytes::Bytes::from_static(b"selected"));
        Ok(())
    }
}
/// Borrows the original preparation ledger; there is no second meter.
pub(crate) struct PreparationHistoryIo<'a> {
    io: &'a PreparationIo,
}

impl PreparationIo {
    pub(crate) fn history_io(&self) -> PreparationHistoryIo<'_> {
        PreparationHistoryIo { io: self }
    }
}

impl PreparationHistoryIo<'_> {
    fn charged(&self, delta: WorkCounters) -> Result<()> {
        self.io.preflight(delta)?;
        self.io.charge(delta)
    }

    fn reserve<T>(&self, values: &mut Vec<T>, maximum: usize) -> Result<()> {
        if values.len() < values.capacity() { return Ok(()); }
        let old = values.capacity();
        let target = old.checked_mul(2).unwrap_or(maximum).max(4).min(maximum);
        if target <= old { return Err(Error::Invalid("history vector exceeds declared maximum".into())); }
        let bytes = target.checked_sub(old).and_then(|count| count.checked_mul(std::mem::size_of::<T>()))
            .ok_or_else(|| Error::Invalid("history vector ownership overflow".into()))? as u64;
        self.io.claim(bytes)?;
        self.io.allocation_attempt(bytes)?;
        values.try_reserve_exact(target - values.len()).map_err(|error| Error::Storage(error.to_string()))?;
        let actual = (values.capacity() - old).checked_mul(std::mem::size_of::<T>())
            .ok_or_else(|| Error::Invalid("history vector ownership overflow".into()))? as u64;
        if actual > bytes { self.io.claim(actual - bytes)?; }
        else { self.io.release(bytes - actual)?; }
        Ok(())
    }
}

impl crate::store::history::HistoryIo for PreparationHistoryIo<'_> {
    fn check_cancellation(&self) -> Result<()> {
        self.io.check_cancellation()
    }
    fn begin_call(&mut self) -> Result<()> {
        self.charged(WorkCounters { backend_read_operations: 1, ..Default::default() })
    }
    fn begin_write(&mut self) -> Result<()> {
        self.charged(WorkCounters { backend_write_operations: 1, ..Default::default() })
    }
    fn receive_record(&mut self, bytes: u64) -> Result<()> {
        // Preserve actual received work even if cancellation/quota now rejects it.
        self.io.charge(WorkCounters { authority_records_read: 1, bytes_read: bytes,
            items_examined: 1, ..Default::default() })?;
        self.io.retain_owned(bytes)
    }
    fn release_record(&mut self, bytes: u64) -> Result<()> { self.io.release(bytes) }
    fn retain_owned(&mut self, bytes: u64) -> Result<()> { self.io.retain_owned(bytes) }
    fn admit_owned(&self, bytes: u64) -> Result<()> {
        self.check_cancellation()?;
        if bytes > self.io.remaining()?.peak_allocation_bytes {
            return Err(Error::Invalid("history exceeds live logical byte allowance".into()));
        }
        Ok(())
    }
    fn release_owned(&mut self, bytes: u64) -> Result<()> { self.io.release(bytes) }
    fn clone_authority(&mut self, authority: &crate::core::Authority) -> Result<crate::core::Authority> {
        Ok(crate::core::Authority { kind: authority.kind, id: self.clone_text(&authority.id)? })
    }
    fn clone_text(&mut self, text: &str) -> Result<String> {
        self.io.claim(text.len() as u64)?;
        self.io.preflight(WorkCounters { bytes_copied: text.len() as u64, ..Default::default() })?;
        if !text.is_empty() { self.io.allocation_attempt(text.len() as u64)?; }
        let mut copy = String::with_capacity(text.len());
        copy.push_str(text);
        self.io.charge(WorkCounters { bytes_copied: text.len() as u64, ..Default::default() })?;
        Ok(copy)
    }
    fn reserve_events(&mut self, values: &mut Vec<crate::core::Event>, maximum: usize) -> Result<()> { self.reserve(values, maximum) }
    fn reserve_records(&mut self, values: &mut Vec<acyclic_stream::Record>, maximum: usize) -> Result<()> { self.reserve(values, maximum) }
    fn reserve_messages(&mut self, values: &mut Vec<crate::conversation::ConversationMessage>, maximum: usize) -> Result<()> { self.reserve(values, maximum) }
    fn reserve_values(&mut self, values: &mut Vec<serde_json::Value>, maximum: usize) -> Result<()> { self.reserve(values, maximum) }
    fn reserve_bytes(&mut self, values: &mut Vec<u8>, capacity: usize) -> Result<()> {
        if capacity <= values.capacity() { return Ok(()); }
        let old = values.capacity();
        self.io.claim((capacity - old) as u64)?;
        self.io.allocation_attempt((capacity - old) as u64)?;
        values.try_reserve_exact(capacity - values.len()).map_err(|error| Error::Storage(error.to_string()))?;
        if values.capacity() > capacity { self.io.claim((values.capacity() - capacity) as u64)?; }
        Ok(())
    }
    fn reserve_text(&mut self, text: &mut String, capacity: usize) -> Result<()> {
        if capacity <= text.capacity() { return Ok(()); }
        let old = text.capacity();
        self.io.claim((capacity - old) as u64)?;
        self.io.allocation_attempt((capacity - old) as u64)?;
        text.try_reserve_exact(capacity - text.len()).map_err(|error| Error::Storage(error.to_string()))?;
        if text.capacity() > capacity { self.io.claim((text.capacity() - capacity) as u64)?; }
        Ok(())
    }
    fn admit_allocation(&mut self, bytes: u64) -> Result<()> {
        self.admit_owned(bytes)?;
        if bytes != 0 { self.io.allocation_attempt(bytes)?; }
        Ok(())
    }
    fn copy_bytes(&mut self, bytes: u64) -> Result<()> {
        self.charged(WorkCounters { bytes_copied: bytes, ..Default::default() })
    }
    fn encoded_bytes(&mut self, bytes: u64) -> Result<()> {
        self.io.charge(WorkCounters { bytes_encoded: bytes, ..Default::default() })
    }
    fn admit_encoding(&self, bytes: u64) -> Result<()> {
        self.io.preflight(WorkCounters { bytes_encoded: bytes, ..Default::default() })
    }
    fn codec_charge(&mut self, charge: crate::contract::JsonCodecCharge) -> Result<()> {
        match charge {
            crate::contract::JsonCodecCharge::Admission { maximum_owned_bytes } => self.io.claim(maximum_owned_bytes),
            _ => self.io.codec_charge(charge, true),
        }
    }
    fn hashed_bytes(&mut self, bytes: u64) -> Result<()> {
        self.charged(WorkCounters { bytes_hashed: bytes, ..Default::default() })
    }
    fn admit_acknowledgment(&self, bytes: u64, count: u64) -> Result<()> {
        self.io.preflight(WorkCounters { authority_bytes_written: bytes, authority_records_appended: count, ..Default::default() })
    }
    fn acknowledged(&mut self, bytes: u64, count: u64) -> Result<()> {
        self.io.charge(WorkCounters { authority_bytes_written: bytes, authority_records_appended: count, ..Default::default() })
    }
    fn admit_output(&self, bytes: u64, count: u64) -> Result<()> {
        self.io.preflight(WorkCounters { output_bytes: bytes, items_returned: count, ..Default::default() })
    }
    fn stage_output(&mut self, bytes: u64, count: u64) -> Result<()> { self.admit_output(bytes, count) }
    fn return_output(&mut self, bytes: u64, count: u64) -> Result<()> {
        self.charged(WorkCounters { output_bytes: bytes, items_returned: count, ..Default::default() })
    }
    fn remaining(&self) -> Result<WorkBudget> { self.io.remaining() }
    fn cancellation(&self) -> &CancellationToken { &self.io.cancellation }
    fn receive_work(&mut self, mut work: WorkCounters) -> Result<()> {
        work.output_bytes = 0;
        work.items_returned = 0;
        self.io.spent(work)
    }
    fn verify_content<'a>(&'a mut self, verifier: &'a dyn crate::conversation::ContentResidencyVerifier,
        reference: &'a FileRef) -> crate::conversation::ContentFuture<'a, Result<()>> {
        Box::pin(async move {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = self.io.original_native_work() {
                return verifier.verify_with_native_work(reference,
                    reference.descriptor().byte_length(), work).await;
            }
            self.io.receive_content(verifier.verify_measured(reference, reference.descriptor().byte_length(),
                self.io.remaining()?, &self.io.cancellation).await)
        })
    }
    fn read_content<'a>(&'a mut self, verifier: &'a dyn crate::conversation::ContentResidencyVerifier,
        reference: &'a FileRef) -> crate::conversation::ContentFuture<'a, Result<Vec<u8>>> {
        Box::pin(async move {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = self.io.original_native_work() {
                let (bytes, custody) = self.io.native_call(verifier.read_with_native_work(reference,
                    reference.descriptor().byte_length(), work).await)?;
                self.io.absorb_native_custody(custody)?;
                return Ok(bytes);
            }
            let bytes = self.io.receive_content(verifier.read_measured(reference, reference.descriptor().byte_length(),
                self.io.remaining()?, &self.io.cancellation).await)?;
            self.io.retain_owned(bytes.capacity() as u64)?;
            Ok(bytes)
        })
    }
}

/// Absolute canonical private metadata path, admitted before its one buffer.
pub(crate) fn content_metadata_path(path: &str, io: &PreparationIo) -> Result<String> {
    super::super::content_metadata_path_io(path, true, Some(io))
}
