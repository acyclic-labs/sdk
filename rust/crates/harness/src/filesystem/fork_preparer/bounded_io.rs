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
    pub(crate) const fn bytes(&self) -> u64 { self.bytes }

    pub(crate) fn substitute(&mut self, retained: u64) -> Result<()> {
        self.io.check_cancellation()?;
        if retained > self.bytes {
            return Err(self.io.invalid("audited decoded ownership exceeds reservation"));
        }
        self.io.release(self.bytes - retained)?;
        self.bytes = retained;
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

/// The actual admitted typed-decode allowance. Native decoding owns its guard
/// directly until Serde succeeds, so a decoder error can move that same guard
/// without a fallible post-error transfer or another retention attempt.
struct TrustedDecodeReservation<'a> {
    io: &'a PreparationIo,
    scoped: Option<DecodeReservation<'a>>,
    diagnostic_bytes: u64,
    #[cfg(not(target_arch = "wasm32"))]
    native: Option<acyclic_fs::NativeAllocation>,
}

impl<'a> TrustedDecodeReservation<'a> {
    fn new(io: &'a PreparationIo) -> Self {
        Self { io, scoped: None, diagnostic_bytes: 0, #[cfg(not(target_arch = "wasm32"))] native: None }
    }

    fn reserve(&mut self, bytes: u64) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = self.io.original_native_work() {
            self.native = Some(work.retain_owned(bytes)
                .map_err(|cause| Error::NativeWork { cause, custody: None })?);
            return Ok(());
        }
        self.scoped = Some(self.io.reserve_decode(bytes)?);
        Ok(())
    }

    fn reserve_typed(
        &mut self, owned: u64, diagnostics: u64, native_code: Option<crate::wire::ErrorCode>,
    ) -> Result<()> {
        let bytes = owned.checked_add(diagnostics)
            .ok_or_else(|| decode_allowance_error("typed diagnostic reservation overflow", native_code))?;
        self.reserve(bytes)?;
        self.diagnostic_bytes = diagnostics;
        Ok(())
    }

    fn finish<T>(mut self, result: Result<T>) -> Result<ReservedDecoded<'a, T>> {
        let value = match result {
            Ok(value) => value,
            Err(error) => {
                #[cfg(not(target_arch = "wasm32"))]
                if let Error::NativeDecode { code, source, custody: None } = error {
                    return Err(Error::NativeDecode { code, source, custody: self.native.take() });
                }
                return Err(error);
            }
        };
        // Serde has returned successfully and its diagnostic temporaries have
        // physically dropped. Only payload custody survives this boundary.
        if self.diagnostic_bytes != 0 {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(guard) = self.native.as_mut() {
                let retained = guard.bytes() - self.diagnostic_bytes;
                if let Err(cause) = guard.substitute(retained) {
                    return Err(Error::NativeWork { cause, custody: self.native.take() });
                }
            }
            if let Some(guard) = self.scoped.as_mut() {
                guard.substitute(guard.bytes() - self.diagnostic_bytes)?;
            }
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(guard) = self.native.take() {
            let bytes = guard.bytes();
            self.io.absorb_native_custody(guard)?;
            self.scoped = Some(DecodeReservation { io: self.io, bytes });
        }
        let _reservation = self.scoped.take()
            .ok_or_else(|| self.io.invalid("trusted decode omitted admission"))?;
        Ok(ReservedDecoded { value, _reservation })
    }

    /// Known SDK serialization diagnostics use the existing audited logical
    /// allowance, not a measurement of Serde's private Message/ErrorImpl heap.
    #[cfg(not(target_arch = "wasm32"))]
    fn finish_serialization<T>(mut self, result: Result<T>) -> Result<T> {
        match result {
            Err(Error::NativeEncode { code, source, custody: None }) =>
                Err(Error::NativeEncode { code, source, custody: self.native.take() }),
            other => other,
        }
    }
}

/// Audited closed Harness DTOs only, including their Value/string/vector
/// containers. This is not an arbitrary DeserializeOwned heap sandbox.
pub(crate) trait PreparationRecord: serde::de::DeserializeOwned + Serialize {
    fn maximum_slot_bytes() -> u64;
    /// Audited static fields, discriminators, expected types, and custom error
    /// labels reachable by this closed visitor; include list punctuation.
    fn diagnostic_label_bytes() -> u64;
    /// Payload/scratch allowance before the audited visitor, including custom
    /// DTO reconstruction/cache temporaries and the closed codec diagnostic base
    /// exactly once. Typed formatter diagnostics are reserved separately from
    /// diagnostic_label_bytes(). Snapshot overrides remove their old diagnostic
    /// component and use the canonical admission, never a second source scan.
    fn decode_owned_allowance(
        shape: crate::contract::JsonValueShape, codec_scratch: u64, input_bytes: u64,
        native_code: Option<crate::wire::ErrorCode>,
    ) -> Result<u64> {
        preparation_decode_owned_allowance::<Self>(shape, codec_scratch, input_bytes, native_code)
    }
    /// Value-free private DTOs use None; Value-bearing records must override it.
    fn json_value_schema() -> crate::contract::JsonValueSchema {
        crate::contract::JsonValueSchema::None
    }
}

fn preparation_decode_owned_allowance<T: PreparationRecord>(
    shape: crate::contract::JsonValueShape, codec_scratch: u64, input_bytes: u64,
    native_code: Option<crate::wire::ErrorCode>,
) -> Result<u64> {
    let value_owned = match T::json_value_schema() {
        // Value-free visitors have scanner scratch but no full-Value base.
        // Value-bearing profiles below already include this closed codec base.
        crate::contract::JsonValueSchema::None => codec_scratch
            .checked_add(crate::contract::sdk_json_diagnostic_bytes())
            .ok_or_else(|| decode_allowance_error("preparation codec base overflow", native_code))?,
        _ => codec_scratch.max(crate::contract::json_value_decode_owned_bytes(shape)
            .map_err(|error| decode_allowance_with_code(error, native_code))?),
    };
    let nodes = shape.lexical_items().checked_add(shape.virtual_items())
        .ok_or_else(|| decode_allowance_error("preparation DTO item count overflow", native_code))?;
    let typed = match T::json_value_schema() {
        crate::contract::JsonValueSchema::Root => Some(0),
        _ => nodes.checked_mul(T::maximum_slot_bytes())
            .and_then(|bytes| bytes.checked_mul(8))
            .and_then(|bytes| input_bytes.checked_mul(4)
                .and_then(|strings| bytes.checked_add(strings))),
    };
    typed.and_then(|bytes| bytes.checked_add(value_owned))
        .and_then(|bytes| bytes.checked_add(std::mem::size_of::<T>() as u64))
        .ok_or_else(|| decode_allowance_error("preparation DTO ownership overflow", native_code))
}

fn decode_allowance_error(reason: &'static str, native_code: Option<crate::wire::ErrorCode>) -> Error {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(code) = native_code { return Error::NativeBoundary { code, reason }; }
    let _ = native_code;
    Error::Invalid(reason.into())
}

fn decode_allowance_with_code(error: Error, native_code: Option<crate::wire::ErrorCode>) -> Error {
    #[cfg(not(target_arch = "wasm32"))]
    if let (Some(code), Error::NativeBoundary { reason, .. }) = (native_code, &error) {
        return Error::NativeBoundary { code, reason: *reason };
    }
    let _ = native_code;
    error
}

pub(crate) fn preparation_slot_bytes<T>() -> u64 {
    let typed = [
        std::mem::size_of::<T>(),
        std::mem::size_of::<crate::model::ModelMessage>(),
        std::mem::size_of::<crate::model::ModelContentPart>(),
        std::mem::size_of::<crate::model::NativeConfigurationBinding>(),
        std::mem::size_of::<crate::model::ModelDataPart>(),
        std::mem::size_of::<crate::model::FileProjectionPolicy>(),
        std::mem::size_of::<crate::model::NativeMediaIntent>(),
        std::mem::size_of::<crate::model::NativeMediaPolicy>(),
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
    ].into_iter().max().unwrap_or(0) as u64;
    // Pinned Serde 229 Content storage is distinct from the eventual DTO.
    // The closed model graph has at most five simultaneous Content envelopes:
    // ModelContent -> ModelContentPart -> ToolResultContent -> ModelDataPart ->
    // NativeMediaIntent (or VolumeOwner). ModelDataPart is nonrecursive.
    // Each envelope permits the actual Content/pair vector's geometric
    // capacity. The decoder's 8*N multiplier covers these audited containers;
    // arbitrary DeserializeOwned implementations are not admitted by this API.
    let content = std::mem::size_of::<serde::__private229::de::Content<'static>>()
        .max(std::mem::size_of::<(
            serde::__private229::de::Content<'static>,
            serde::__private229::de::Content<'static>,
        )>()) as u64;
    typed + 2 * 5 * content
}

macro_rules! audited_preparation_records {
    ($($record:ty => $schema:ident),+ $(,)?) => {$(
        impl PreparationRecord for $record {
            fn maximum_slot_bytes() -> u64 { preparation_slot_bytes::<Self>() }
            fn diagnostic_label_bytes() -> u64 {
                crate::contract::preparation_serde_diagnostic_label_bytes()
            }
            fn json_value_schema() -> crate::contract::JsonValueSchema {
                crate::contract::JsonValueSchema::$schema
            }
        }
    )+};
}

audited_preparation_records!(
    crate::context::Context => Context,
    crate::context::CanonicalContextCheckpoint => None,
    crate::context::CompactionReference => None,
    crate::executor::ResponseProjection => ResponseProjection,
    crate::model::ModelEvent => ModelEvent,
    crate::model::ModelRequest => ModelRequest,
    crate::model::ModelContent => ModelContent,
    crate::model::ModelContentPart => ModelContentPart,
    crate::fork::InheritedConversationPrefix => None,
    crate::core::ExtensionConfiguration => None,
    FileRef => None,
    Vec<crate::conversation::Attachment> => None,
    crate::workflow::WorkflowRecord => WorkflowRecord,
    crate::workflow::WorkflowAdmission => WorkflowAdmission,
    crate::workflow::MachineCheckpoint => MachineCheckpoint,
    crate::workflow::WorkflowCommand => None,
);

impl PreparationRecord for crate::Outcome<serde_json::Value> {
    fn maximum_slot_bytes() -> u64 {
        // Outcome is externally tagged: only Succeeded reaches Value, Failed
        // owns one String, and Indeterminate carries one fixed operation ID.
        // It has no internally tagged/untagged Content buffering layers.
        [
            std::mem::size_of::<Self>(),
            std::mem::size_of::<serde_json::Value>(),
            std::mem::size_of::<(String, serde_json::Value)>(),
            std::mem::size_of::<String>(),
            std::mem::size_of::<crate::OperationId>(),
        ].into_iter().max().unwrap_or(0) as u64
    }
    fn diagnostic_label_bytes() -> u64 {
        crate::contract::preparation_serde_diagnostic_label_bytes()
    }
    fn json_value_schema() -> crate::contract::JsonValueSchema {
        crate::contract::JsonValueSchema::TaskOutcome
    }
}

// A raw Value has no closed SDK typed visitor or expected-field inventory.
// Its canonical full-Value decode allowance already covers diagnostics.
impl PreparationRecord for serde_json::Value {
    fn maximum_slot_bytes() -> u64 { preparation_slot_bytes::<Self>() }
    fn diagnostic_label_bytes() -> u64 { 0 }
    fn json_value_schema() -> crate::contract::JsonValueSchema {
        crate::contract::JsonValueSchema::Root
    }
}

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

impl<'a, T> ReservedDecoded<'a, T> {

    pub(crate) fn into_parts(self) -> (T, DecodeReservation<'a>) {
        (self.value, self._reservation)
    }

    pub(crate) fn substitute_owned(&mut self, bytes: u64) -> Result<()> {
        self._reservation.substitute(bytes)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn into_native_parts(mut self) -> Result<(T, acyclic_fs::NativeAllocation)> {
        let custody = self._reservation.io.take_native_custody(self._reservation.bytes)?;
        self._reservation.bytes = 0;
        Ok((self.value, custody))
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

    /// Moves genuine existing custody without consulting cancellation or quota.
    /// Recovery must observe those conditions while it still owns the lease.
    /// This does not admit growth, allocate a guard, or create another ledger.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn from_native_custody(custody: acyclic_fs::NativeAllocation) -> Self {
        let work = custody.work().clone();
        let cancellation = work.cancellation().clone();
        Self {
            ledger: PreparationLedger::Native { work, custody: Mutex::new(custody) },
            cancellation,
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn into_native_custody(self, retained: u64) -> Result<acyclic_fs::NativeAllocation> {
        match self.ledger {
            PreparationLedger::Native { custody, .. } => {
                let mut guard = custody.into_inner().unwrap_or_else(std::sync::PoisonError::into_inner);
                if let Err(cause) = guard.substitute(retained) {
                    return Err(Error::NativeWork { cause, custody: Some(guard) });
                }
                Ok(guard)
            }
            PreparationLedger::Owned { .. } => Err(Error::Invalid("native custody requires the original native ledger".into())),
        }
    }

    pub(crate) fn uses_native_work(&self) -> bool {
        match &self.ledger {
            PreparationLedger::Owned { .. } => false,
            #[cfg(not(target_arch = "wasm32"))]
            PreparationLedger::Native { .. } => true,
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
    pub(crate) fn absorb_native_custody(&self, mut incoming: acyclic_fs::NativeAllocation) -> Result<()> {
        match &self.ledger {
            PreparationLedger::Native { custody, .. } => {
                let mut held = custody.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                held.absorb(&mut incoming).map_err(|cause| Error::NativeWork {
                    cause, custody: Some(incoming),
                })
            }
            PreparationLedger::Owned { .. } => Err(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
                custody: Some(incoming),
            }),
        }
    }

    /// Move already admitted ownership; never retain it a second time.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn take_native_custody(&self, bytes: u64) -> Result<acyclic_fs::NativeAllocation> {
        match &self.ledger {
            PreparationLedger::Native { custody, .. } => custody.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .split_off(bytes).map_err(|cause| Error::NativeWork { cause, custody: None }),
            PreparationLedger::Owned { .. } => Err(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout, custody: None,
            }),
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
            PreparationLedger::Native { custody, .. } => Ok(custody.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner).bytes()),
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

    pub(crate) fn invalid(&self, reason: &'static str) -> Error {
        invalid(Some(self), reason)
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

    fn encoding_bounds(&self, maximum_bytes: u64) -> Result<crate::contract::JsonBounds> {
        let remaining = self.remaining()?;
        // A valid JSON value necessarily consumes a byte and a token. This is
        // admission only: actual codec work remains charged by its callbacks.
        // Keep an explicit invalid caller bound for the codec to diagnose.
        if maximum_bytes != 0 {
            self.preflight(WorkCounters { bytes_encoded: 1, items_examined: 1, ..Default::default() })?;
        }
        Ok(crate::contract::JsonBounds {
            maximum_bytes: maximum_bytes.min(remaining.bytes_encoded),
            maximum_items: remaining.items_examined,
            maximum_depth: 256,
        })
    }

    pub(crate) fn encode_json<T: Serialize>(&self, value: &T, maximum_bytes: u64, canonical: bool) -> Result<Vec<u8>> {
        let bounds = self.encoding_bounds(maximum_bytes)?;
        let mut charge = |next| self.codec_charge(next, true);
        self.encode_at_bounds(value, bounds, canonical, &mut charge)
    }

    fn encode_at_bounds<T: Serialize, F: FnMut(crate::contract::JsonCodecCharge) -> Result<()>>(
        &self, value: &T, bounds: crate::contract::JsonBounds, canonical: bool, charge: &mut F,
    ) -> Result<Vec<u8>> {
        #[cfg(not(target_arch = "wasm32"))]
        if self.original_native_work().is_some() {
            // Only closed SDK serializers are called by this producer. User
            // serializer work and arbitrary custom messages are not sandboxed.
            let mut diagnostics = TrustedDecodeReservation::new(self);
            diagnostics.reserve(crate::contract::sdk_json_diagnostic_bytes())?;
            let result = if canonical {
                crate::contract::canonical_json_bytes_bounded_native_trusted(
                    value, bounds, crate::wire::ErrorCode::Invalid, charge,
                )
            } else {
                crate::contract::json_bytes_bounded_native_trusted(
                    value, bounds, crate::wire::ErrorCode::Invalid, charge,
                )
            };
            return diagnostics.finish_serialization(result);
        }
        if canonical {
            crate::contract::canonical_json_bytes_bounded(value, bounds, charge)
        } else {
            crate::contract::json_bytes_bounded_trusted(value, bounds, charge)
        }
    }

    pub(crate) fn encode_for_clone<T: Serialize>(&self, value: &T, maximum_bytes: u64) -> Result<Vec<u8>> {
        let bounds = self.encoding_bounds(maximum_bytes)?;
        let mut transferred = DecodeReservation { io: self, bytes: 0 };
        let encoded = self.encode_at_bounds(value, bounds, true, &mut |next| {
            match next {
                crate::contract::JsonCodecCharge::Release { .. } => Ok(()),
                crate::contract::JsonCodecCharge::Retain { bytes } => {
                    self.codec_charge(next, true)?;
                    transferred.bytes = transferred.bytes.checked_add(bytes)
                        .ok_or_else(|| self.invalid("clone admission overflow"))?;
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
        _encoded_hold = self.guard_owned(encoded.capacity() as u64)?;
        let hash = WorkCounters { bytes_hashed: encoded.len() as u64, ..Default::default() };
        self.preflight(hash)?;
        let digest = blake3::hash(&encoded);
        self.charge(hash)?;
        let length = prefix.len().checked_add(64)
            .ok_or_else(|| self.invalid("capability length overflow"))?;
        self.claim(length as u64)?;
        let copying = WorkCounters { bytes_copied: length as u64, ..Default::default() };
        self.preflight(copying)?;
        self.allocation_attempt(length as u64)?;
        let mut key = String::with_capacity(length);
        key.push_str(prefix);
        key.push_str(&digest.to_hex());
        self.charge(copying)?;
        if key.capacity() > length { self.claim((key.capacity() - length) as u64)?; }
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

    /// Admit the actual ordinary History box before constructing it. Shared
    /// History observers release only this frame after the box physically drops;
    /// any returned DTO custody remains in the same IO ledger.
    fn history_future<'a, T, F, B>(
        &'a self, build: B,
    ) -> Result<crate::store::history::HistoryContentFuture<'a, T>>
    where
        F: Future<Output = Result<T>> + Send + 'a,
        B: FnOnce() -> F,
    {
        let bytes = std::mem::size_of::<F>() as u64;
        self.claim(bytes)?;
        if bytes != 0 { self.allocation_attempt(bytes)?; }
        Ok(crate::store::history::HistoryContentFuture::Ordinary {
            future: Box::pin(build()), frame_bytes: bytes,
        })
    }

    // Audited closed serde derives only: upload records and approval-only wires.
    // Their strings/key vectors occupy at most twice encoded input bytes plus
    // the concrete root body; opaque allocator nodes and RSS are excluded.
    pub(crate) fn decode_json<T: UploadRecord>(&self, bytes: &[u8]) -> Result<T> {
        let remaining = self.remaining()?;
        let native_code = self.uses_native_work().then_some(crate::wire::ErrorCode::Storage);
        let owned = (bytes.len() as u64).checked_mul(2)
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<T>() as u64))
            .ok_or_else(|| decode_allowance_error("upload decode ownership overflow", native_code))?;
        let bounds = crate::contract::JsonBounds {
            maximum_bytes: bytes.len() as u64, maximum_items: remaining.items_examined, maximum_depth: 256,
        };
        let mut reservation = TrustedDecodeReservation::new(self);
        let result: Result<T> = self.decode_trusted(bytes, bounds, owned,
            crate::wire::ErrorCode::Storage, T::json_value_schema(), &mut |next| match next {
                crate::contract::JsonCodecCharge::Admission { maximum_owned_bytes, value_shape } => {
                    let owned = match T::json_value_schema() {
                        crate::contract::JsonValueSchema::None => maximum_owned_bytes
                            .checked_add(crate::contract::sdk_json_diagnostic_bytes())
                            .ok_or_else(|| decode_allowance_error("upload codec base overflow", native_code))?,
                        _ => {
                            let scratch = maximum_owned_bytes.checked_sub(owned)
                                .ok_or_else(|| decode_allowance_error("upload decode scratch bound underflow", native_code))?;
                            owned.checked_add(scratch.max(
                                crate::contract::json_value_decode_owned_bytes(value_shape)
                                    .map_err(|error| decode_allowance_with_code(error, native_code))?,
                            )).ok_or_else(|| decode_allowance_error("upload decode ownership overflow", native_code))?
                        }
                    };
                    let diagnostics = crate::contract::json_typed_diagnostic_owned_bytes(
                        value_shape, T::diagnostic_label_bytes(), native_code,
                    )?;
                    reservation.reserve_typed(owned, diagnostics, native_code)
                }
                next => self.codec_charge(next, false),
            });
        let mut decoded = reservation.finish(result)?;
        decoded._reservation.substitute(decoded.value.logical_owned_bytes()?)?;
        Ok(decoded.into_retained())
    }

    fn decode_trusted<T: serde::de::DeserializeOwned, F: FnMut(crate::contract::JsonCodecCharge) -> Result<()>>(
        &self, bytes: &[u8], bounds: crate::contract::JsonBounds, maximum_owned_bytes: u64,
        code: crate::wire::ErrorCode, value_schema: crate::contract::JsonValueSchema,
        charge: &mut F,
    ) -> Result<T> {
        #[cfg(not(target_arch = "wasm32"))]
        if self.original_native_work().is_some() {
            return crate::contract::json_from_slice_bounded_native_trusted(
                bytes, bounds, maximum_owned_bytes, code, value_schema, charge,
            );
        }
        let _ = (code, value_schema);
        crate::contract::json_from_slice_bounded_trusted(bytes, bounds, maximum_owned_bytes, charge)
    }

    pub(crate) fn decode_preparation<T: PreparationRecord>(
        &self, bytes: &[u8],
    ) -> Result<ReservedDecoded<'_, T>> {
        self.decode_preparation_with_code(bytes, crate::wire::ErrorCode::Storage)
    }

    pub(crate) fn decode_preparation_with_code<T: PreparationRecord>(
        &self, bytes: &[u8], code: crate::wire::ErrorCode,
    ) -> Result<ReservedDecoded<'_, T>> {
        self.decode_preparation_seed_with_code(
            std::marker::PhantomData::<T>, bytes, code,
        )
    }

    pub(crate) fn decode_preparation_seed_with_code<'a, 'de, T, S>(
        &'a self, seed: S, bytes: &'de [u8], code: crate::wire::ErrorCode,
    ) -> Result<ReservedDecoded<'a, T>>
    where T: PreparationRecord, S: serde::de::DeserializeSeed<'de, Value = T>,
    {
        let bounds = crate::contract::JsonBounds {
            maximum_bytes: bytes.len() as u64,
            maximum_items: self.remaining()?.items_examined,
            maximum_depth: 128,
        };
        let mut reservation = TrustedDecodeReservation::new(self);
        let value_schema = T::json_value_schema();
        let native_code = self.uses_native_work().then_some(code);
        let mut charge = |next| {
            match next {
                crate::contract::JsonCodecCharge::Admission { maximum_owned_bytes, value_shape } => {
                    let owned = T::decode_owned_allowance(
                        value_shape, maximum_owned_bytes, bytes.len() as u64, native_code,
                    )?;
                    let diagnostics = if value_schema == crate::contract::JsonValueSchema::Root {
                        0
                    } else {
                        crate::contract::json_typed_diagnostic_owned_bytes(
                            value_shape, T::diagnostic_label_bytes(), native_code,
                        )?
                    };
                    reservation.reserve_typed(owned, diagnostics, native_code)
                }
                next => self.codec_charge(next, false),
            }
        };
        #[cfg(not(target_arch = "wasm32"))]
        if self.original_native_work().is_some() {
            let result = crate::contract::json_from_slice_seed_bounded_native_trusted(
                seed, bytes, bounds, 0, code, value_schema, &mut charge,
            );
            return reservation.finish(result);
        }
        let _ = code;
        let result = crate::contract::json_from_slice_seed_bounded_trusted(
            seed, bytes, bounds, 0, value_schema, &mut charge,
        );
        reservation.finish(result)
    }

    /// Builds the original serde JSON graph directly. Wire-size admission is
    /// distinct from bytes actually encoded: this path creates no wire buffer.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn value_json<T: Serialize>(
        &self, source: &T, maximum_bytes: u64,
    ) -> Result<serde_json::Value> {
        if self.original_native_work().is_none() {
            return Err(self.invalid("native JSON graph conversion requires original work"));
        }
        let bounds = crate::contract::JsonBounds {
            maximum_bytes,
            maximum_items: self.remaining()?.items_examined,
            maximum_depth: 256,
        };
        let root = std::mem::size_of::<serde_json::Value>() as u64;
        self.retain_owned(root)?;
        let root_hold = self.guard_owned(root)?;
        let mut diagnostics = TrustedDecodeReservation::new(self);
        diagnostics.reserve(crate::contract::sdk_json_diagnostic_bytes())?;
        let result = crate::contract::json_value_bounded_native_trusted(
            source, bounds, crate::wire::ErrorCode::Invalid,
            &mut |next| self.codec_charge(next, false),
        );
        let value = diagnostics.finish_serialization(result)?;
        root_hold.into_retained();
        Ok(value)
    }

    /// Checks the original JSON wire-size policy without building a graph or
    /// buffer. Resident strings are inspected before the serde sizing scan.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn check_json_byte_bound(
        &self, source: &serde_json::Value, maximum_bytes: u64,
    ) -> Result<()> {
        if self.original_native_work().is_none() {
            return Err(self.invalid("native JSON sizing requires original work"));
        }
        let bounds = crate::contract::JsonBounds {
            maximum_bytes,
            maximum_items: self.remaining()?.items_examined,
            maximum_depth: 256,
        };
        crate::contract::json_owned_value_shape_bounded(
            source, bounds, crate::contract::JsonValueSchema::None,
            &mut |next| self.codec_charge(next, false),
        )?;
        crate::contract::validate_json_byte_bound_native_trusted(
            source, bounds, crate::wire::ErrorCode::Invalid,
            &mut |next| self.codec_charge(next, false),
        )
    }

    /// The original borrowed Value visitor, restricted to audited SDK DTOs.
    /// No whole-Value clone, byte encoding, or parser round trip is performed.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn value_decode<T: UploadRecord>(
        &self, source: &serde_json::Value, code: crate::wire::ErrorCode,
    ) -> Result<T> {
        let mut decoded = self.value_decode_with_schema::<T>(
            source, code, T::json_value_schema(), preparation_slot_bytes::<T>(),
            T::diagnostic_label_bytes(), None,
        )?;
        let actual = (std::mem::size_of::<T>() as u64)
            .checked_add(decoded.value.logical_owned_bytes()?)
            .ok_or_else(|| decode_allowance_error("typed Value ownership overflow", Some(code)))?;
        decoded.substitute_owned(actual)?;
        Ok(decoded.into_retained())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn value_decode_preparation<T: PreparationRecord>(
        &self, source: &serde_json::Value, code: crate::wire::ErrorCode,
    ) -> Result<ReservedDecoded<'_, T>> {
        self.value_decode_with_schema(
            source, code, T::json_value_schema(), T::maximum_slot_bytes(),
            T::diagnostic_label_bytes(), Some(T::decode_owned_allowance),
        )
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn value_decode_with_schema<T: serde::de::DeserializeOwned>(
        &self, source: &serde_json::Value, code: crate::wire::ErrorCode,
        schema: crate::contract::JsonValueSchema, slot_bytes: u64,
        diagnostic_label_bytes: u64,
        profile: Option<fn(crate::contract::JsonValueShape, u64, u64, Option<crate::wire::ErrorCode>) -> Result<u64>>,
    ) -> Result<ReservedDecoded<'_, T>> {
        if self.original_native_work().is_none() {
            return Err(self.invalid("native Value decoding requires original work"));
        }
        let bounds = crate::contract::JsonBounds {
            maximum_bytes: u64::MAX,
            maximum_items: self.remaining()?.items_examined,
            maximum_depth: 128,
        };
        let mut reservation = TrustedDecodeReservation::new(self);
        let result = crate::contract::json_from_value_bounded_native_trusted(
            source, bounds, 0, code, schema, &mut |next| match next {
                crate::contract::JsonCodecCharge::Admission { value_shape, .. } => {
                    let allowance = match profile {
                        Some(profile) => profile(value_shape, 0, value_shape.input_bytes(), Some(code))?,
                        None => crate::contract::json_typed_value_decode_owned_bytes(
                            value_shape, slot_bytes,
                        ).map_err(|error| decode_allowance_with_code(error, Some(code)))?
                            .checked_add(std::mem::size_of::<T>() as u64)
                            .ok_or_else(|| decode_allowance_error("typed Value reservation overflow", Some(code)))?,
                    };
                    let diagnostics = if schema == crate::contract::JsonValueSchema::Root {
                        0
                    } else {
                        crate::contract::json_typed_diagnostic_owned_bytes(
                            value_shape, diagnostic_label_bytes, Some(code),
                        )?
                    };
                    reservation.reserve_typed(allowance, diagnostics, Some(code))
                }
                next => self.codec_charge(next, false),
            },
        );
        reservation.finish(result)
    }

    /// Preserves the consuming `serde_json::from_value` protocol. The old
    /// strings are moved, not cloned; their existing allocation stays live
    /// across the visitor and is combined with the preadmitted new temporaries.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn value_decode_owned<T: UploadRecord>(
        &self, value: serde_json::Value, allocation: acyclic_fs::NativeAllocation,
        code: crate::wire::ErrorCode,
    ) -> Result<(T, acyclic_fs::NativeAllocation)> {
        // Field order matters on every early error: Value physically drops
        // before its original guard, including cancellation before inspection.
        let mut input = (value, allocation);
        let work = self.original_native_work()
            .ok_or_else(|| self.invalid("owned Value decoding requires original work"))?;
        if !work.shares_ledger(input.1.work()) {
            return Err(self.invalid("owned Value belongs to a different work ledger"));
        }
        let bounds = crate::contract::JsonBounds {
            maximum_bytes: u64::MAX,
            maximum_items: self.remaining()?.items_examined,
            maximum_depth: 128,
        };
        let shape = crate::contract::json_owned_value_shape_bounded(
            &input.0, bounds, T::json_value_schema(),
            &mut |next| self.codec_charge(next, false),
        ).map_err(|error| decode_allowance_with_code(error, Some(code)))?;
        let nodes = shape.lexical_items().checked_add(shape.virtual_items())
            .ok_or_else(|| decode_allowance_error("owned Value item count overflow", Some(code)))?;
        let additional = crate::contract::json_owned_value_conversion_additional_bytes(shape)
            .map_err(|error| decode_allowance_with_code(error, Some(code)))?;
        // Only ModelContent's reachable untagged visitor clones ContentRef
        // strings. Tagged descendants consume those strings by visit_string.
        // This funds NEW temporary copies, not the existing moved input.
        let content_copy_bytes = match T::json_value_schema() {
            crate::contract::JsonValueSchema::ModelContent
            | crate::contract::JsonValueSchema::Context
            | crate::contract::JsonValueSchema::ResponseProjection
            | crate::contract::JsonValueSchema::ModelRequest => shape.input_bytes().checked_mul(2)
                .ok_or_else(|| decode_allowance_error("owned Content string reservation overflow", Some(code)))?,
            _ => 0,
        };
        let allowance = nodes.checked_mul(preparation_slot_bytes::<T>())
            .and_then(|bytes| bytes.checked_mul(8))
            .and_then(|bytes| bytes.checked_add(additional))
            .and_then(|bytes| bytes.checked_add(content_copy_bytes))
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<T>() as u64))
            .ok_or_else(|| decode_allowance_error("owned Value reservation overflow", Some(code)))?;
        let mut reservation = TrustedDecodeReservation::new(self);
        let diagnostics = crate::contract::json_typed_diagnostic_owned_bytes(
            shape, T::diagnostic_label_bytes(), Some(code),
        )?;
        reservation.reserve_typed(allowance, diagnostics, Some(code))?;
        let result = serde_json::from_value(input.0)
            .map_err(|source| Error::NativeDecode { code, source, custody: None });
        let mut output = reservation.finish(result)?.into_native_parts()?;
        if let Err(cause) = output.1.absorb(&mut input.1) {
            return Err(Error::NativeWork { cause, custody: Some(input.1) });
        }
        let actual = (std::mem::size_of::<T>() as u64)
            .checked_add(output.0.logical_owned_bytes()?)
            .ok_or_else(|| decode_allowance_error("owned typed Value ownership overflow", Some(code)))?;
        output.1.substitute(actual)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        Ok(output)
    }

    pub(crate) fn codec_charge(&self, next: crate::contract::JsonCodecCharge, encode: bool) -> Result<()> {
        match next {
            crate::contract::JsonCodecCharge::Retain { bytes } => {
                // Codec reports finite logical ownership, not opaque allocator
                // operations. Never invent one allocation per logical node.
                self.retain_owned(bytes)
            }
            crate::contract::JsonCodecCharge::Release { bytes } => self.release(bytes),
            crate::contract::JsonCodecCharge::CopyAdmission { bytes } =>
                self.preflight(WorkCounters { bytes_copied: bytes, ..Default::default() }),
            crate::contract::JsonCodecCharge::AllocationOperation { bytes } =>
                self.allocation_attempt(bytes),
            crate::contract::JsonCodecCharge::Admission { .. } => Err(self.invalid(
                "trusted decode admission requires a scoped reservation")),
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
            crate::contract::JsonCodecCharge::Inspect { items } => {
                let work = WorkCounters { items_examined: items, ..Default::default() };
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
    /// Deep owned capacities only. Direct Value conversion separately owns
    /// sizeof(Self); existing upload/staging DTO contracts remain deep-only.
    fn logical_owned_bytes(&self) -> Result<u64>;
    fn diagnostic_label_bytes() -> u64;
    fn json_value_schema() -> crate::contract::JsonValueSchema {
        crate::contract::JsonValueSchema::None
    }
}

impl UploadRecord for FileRef {
    fn diagnostic_label_bytes() -> u64 {
        crate::contract::preparation_serde_diagnostic_label_bytes()
    }
    fn logical_owned_bytes(&self) -> Result<u64> {
        FileRef::logical_owned_bytes(self)
    }
}

impl UploadRecord for (FileRef, Option<crate::resources::GenerationRef>) {
    fn diagnostic_label_bytes() -> u64 {
        crate::contract::preparation_serde_diagnostic_label_bytes()
    }
    fn logical_owned_bytes(&self) -> Result<u64> {
        let mut bytes = self.0.logical_owned_bytes()?;
        if let Some(generation) = &self.1 {
            bytes = bytes.checked_add(generation.logical_owned_bytes()?)
                .ok_or_else(|| Error::Invalid("upload generation ownership overflow".into()))?;
        }
        Ok(bytes)
    }
}

pub(in crate::filesystem) fn invalid(io: Option<&PreparationIo>, reason: &'static str) -> Error {
    #[cfg(not(target_arch = "wasm32"))]
    if io.and_then(PreparationIo::original_native_work).is_some() {
        return Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason };
    }
    let _ = io;
    Error::Invalid(reason.into())
}

pub(in crate::filesystem) fn validate_content_path(path: &str, io: Option<&PreparationIo>) -> Result<()> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(work) = io.and_then(PreparationIo::original_native_work) {
        return crate::conversation::validate_content_path_with_native_work(path, work);
    }
    crate::conversation::validate_content_path(path)
}

pub(in crate::filesystem) fn clone_text(value: &str, io: Option<&PreparationIo>) -> Result<String> {
    match io {
        Some(io) => crate::store::history::HistoryIo::clone_text(&mut io.history_io(), value),
        None => Ok(value.to_owned()),
    }
}

pub(in crate::filesystem) fn format_text(value: std::fmt::Arguments<'_>, io: Option<&PreparationIo>) -> Result<String> {
    crate::contract::preparation_format(value, io)
}

pub(in crate::filesystem) fn hex_key(bytes: &[u8], io: Option<&PreparationIo>) -> Result<String> {
    let Some(io) = io else { return Ok(hex::encode(bytes)); };
    let capacity = bytes.len().checked_mul(2).ok_or_else(|| invalid(Some(io), "hex key size overflow"))?;
    let copied = WorkCounters { bytes_copied: capacity as u64, ..Default::default() };
    io.claim(capacity as u64)?;
    io.preflight(copied)?;
    if capacity != 0 { io.allocation_attempt(capacity as u64)?; }
    let mut output = String::new();
    output.try_reserve_exact(capacity).map_err(Error::Allocation)?;
    for byte in bytes {
        output.push(b"0123456789abcdef"[(byte >> 4) as usize] as char);
        output.push(b"0123456789abcdef"[(byte & 15) as usize] as char);
    }
    if output.capacity() > capacity { io.claim((output.capacity() - capacity) as u64)?; }
    io.charge(copied)?;
    Ok(output)
}

pub(in crate::filesystem) fn conflict(io: Option<&PreparationIo>, reason: &'static str) -> Error {
    #[cfg(not(target_arch = "wasm32"))]
    if io.and_then(PreparationIo::original_native_work).is_some() {
        return Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict, reason };
    }
    let _ = io;
    Error::Conflict(reason.into())
}

pub(in crate::filesystem) fn unauthorized(io: Option<&PreparationIo>, reason: &'static str) -> Error {
    #[cfg(not(target_arch = "wasm32"))]
    if io.and_then(PreparationIo::original_native_work).is_some() {
        return Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, reason };
    }
    let _ = io;
    Error::Unauthorized(reason.into())
}

pub(in crate::filesystem) fn unsupported(io: Option<&PreparationIo>, reason: &'static str) -> Error {
    #[cfg(not(target_arch = "wasm32"))]
    if io.and_then(PreparationIo::original_native_work).is_some() {
        return Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported, reason };
    }
    let _ = io;
    Error::Unsupported(reason.into())
}

pub(in crate::filesystem) fn vector<T>(capacity: usize, io: Option<&PreparationIo>) -> Result<Vec<T>> {
    let bytes = capacity.checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| invalid(io, "vector capacity overflow"))?;
    if let Some(io) = io {
        io.claim(bytes as u64)?;
        if bytes != 0 { io.allocation_attempt(bytes as u64)?; }
    }
    let mut output = Vec::new();
    output.try_reserve_exact(capacity).map_err(Error::Allocation)?;
    if let Some(io) = io {
        let actual = output.capacity().checked_mul(std::mem::size_of::<T>())
            .ok_or_else(|| invalid(Some(io), "vector capacity overflow"))?;
        if actual > bytes { io.claim((actual - bytes) as u64)?; }
    }
    Ok(output)
}

pub(in crate::filesystem) fn clone_bytes(bytes: &[u8], io: Option<&PreparationIo>) -> Result<Vec<u8>> {
    let copied = WorkCounters { bytes_copied: bytes.len() as u64, ..Default::default() };
    if let Some(io) = io { io.preflight(copied)?; }
    let mut output = vector(bytes.len(), io)?;
    output.extend_from_slice(bytes);
    if let Some(io) = io { io.charge(copied)?; }
    Ok(output)
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
    FileRef::new_with_io(volume, path, version, descriptor, display_name, io)
}

pub(in crate::filesystem) fn clone_file(reference: &FileRef, io: Option<&PreparationIo>) -> Result<FileRef> {
    match io {
        Some(io) => reference.clone_with_io(io),
        None => Ok(reference.clone()),
    }
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
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = io.original_native_work() {
                let hash = WorkCounters {
                    bytes_hashed: key.as_str().len() as u64,
                    ..WorkCounters::UNCHARGED
                };
                original.admit(hash)
                    .map_err(|cause| Error::NativeWork { cause, custody: None })?;
                let operation = super::super::filesystem_key(key);
                original.spent(hash)
                    .map_err(|cause| Error::NativeWork { cause, custody: None })?;
                return workspace.operation_generation_with_native_work(
                    operation, original, &io.cancellation,
                ).await.map_err(|(source, custody)| Error::NativeWorkspace { source, custody });
            }
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
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
        // The generated hash name is already canonical ASCII. Transfer that
        // same paid string backing into the reference key without another copy.
        return WorkspaceRef::new(host_provider.clone_with_io(io)?, name.into_bytes(), None);
    }
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
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(original) = io.original_native_work() {
        let encoded = io.encode_json(volume, io.remaining()?.bytes_encoded, true)?;
        let hash = WorkCounters {
            bytes_hashed: encoded.len() as u64,
            ..WorkCounters::UNCHARGED
        };
        original.admit(hash)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let digest = blake3::hash(&encoded);
        original.spent(hash)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let mut custody = original.claim(72)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let mut name = String::new();
        name.try_reserve_exact(72).map_err(|source| Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::AllocationFailed(source),
            custody: None,
        })?;
        custody.reconcile_capacity(name.capacity() as u64)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let copy = WorkCounters {
            bytes_copied: 72, bytes_encoded: 64, ..WorkCounters::UNCHARGED
        };
        original.admit(copy)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        name.push_str("harness-");
        name.push_str(digest.to_hex().as_str());
        original.spent(copy)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        io.absorb_native_custody(custody)?;
        return Ok(name);
    }
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
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(original) = io.original_native_work() {
        original.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        // WorkspaceRef construction and deserialization validate its canonical
        // shape. Check this host binding without rebuilding owned diagnostics.
        if reference.as_resource().provider() != &host.provider {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "resource belongs to another Filesystem provider",
            });
        }
        let name = std::str::from_utf8(reference.as_resource().key())
            .map_err(|_| Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "workspace reference key must be UTF-8",
            })?;
        return host.filesystem.open_workspace_with_native_work(
            name, original, &io.cancellation,
        ).await.map_err(|(source, custody)| Error::NativeWorkspace { source, custody });
    }
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
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(original) = io.original_native_work() {
        original.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        if reference.as_resource().provider() != &host.provider {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "resource belongs to another Filesystem provider",
            });
        }
        let bytes: [u8; 32] = reference.as_resource().key().try_into()
            .map_err(|_| Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "generation reference key must be 32 bytes",
            })?;
        return workspace.generation_with_native_work(
            acyclic_fs::GenerationId::new(acyclic_fs::Digest::from_bytes(bytes)),
            original, &io.cancellation,
        ).await.map_err(|(source, custody)| Error::NativeWorkspace { source, custody });
    }
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
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(original) = io.original_native_work() {
        let generation = workspace.head_with_native_work(original, &io.cancellation)
            .await.map_err(|(source, custody)| Error::NativeWorkspace { source, custody })?;
        return generation.read_with_native_work(path, maximum_bytes, original, &io.cancellation)
            .await.map_err(|(source, custody)| Error::NativeWorkspace { source, custody });
    }
    io.receive(workspace.read_measured(path, maximum_bytes, io.remaining()?, &io.cancellation).await)
}

pub(in crate::filesystem) async fn resolve<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>, reference: &WorkspaceRef, io: Option<&PreparationIo>,
) -> Result<super::super::WorkspaceObservation> {
    let Some(io) = io else { return host.resolve(reference).await; };
    let workspace = open(host, reference, io).await?;
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(original) = io.original_native_work() {
        let generation = workspace.head_with_native_work(original, &io.cancellation)
            .await.map_err(|(source, custody)| Error::NativeWorkspace { source, custody })?;
        return Ok(super::super::WorkspaceObservation {
            workspace: reference.clone_with_io(io)?,
            generation: generation_ref(host, &generation, Some(io))?,
        });
    }
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
    let lifecycle = if host.filesystem.capabilities().durable {
        acyclic_fs::Lifecycle::Durable
    } else {
        acyclic_fs::Lifecycle::Ephemeral
    };
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(original) = io.original_native_work() {
        let hash_work = WorkCounters {
            bytes_hashed: key.as_str().len() as u64, ..WorkCounters::UNCHARGED
        };
        original.admit(hash_work)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let operation = acyclic_fs::OperationId::from_bytes(
            super::super::filesystem_key(key).into_bytes(),
        );
        original.spent(hash_work)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        host.filesystem.create_workspace_with_operation_with_native_work(
            name, acyclic_fs::VolumeConfig::portable(lifecycle),
            operation, original, &io.cancellation,
        ).await.map_err(|(source, custody)| Error::NativeWorkspace { source, custody })?;
        return Ok(());
    }
    let operation = acyclic_fs::OperationId::from_bytes(
        super::super::filesystem_key(key).into_bytes(),
    );
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
    generation_ref(host, &generation, Some(io))
}

pub(in crate::filesystem) async fn apply_generation<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>, reference: &WorkspaceRef,
    expected: Option<&crate::resources::GenerationRef>, mutations: &[WorkspaceMutation],
    key: &IdempotencyKey, io: &PreparationIo,
) -> Result<acyclic_fs::Generation<A, O>> {
    let workspace = open(host, reference, io).await?;
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(original) = io.original_native_work() {
        let pinned = match expected {
            Some(reference) => generation(host, &workspace, reference, io).await?,
            None => workspace.head_with_native_work(original, &io.cancellation)
                .await.map_err(|(source, custody)| Error::NativeWorkspace { source, custody })?,
        };
        let hashing = WorkCounters {
            bytes_hashed: key.0.len() as u64, ..WorkCounters::UNCHARGED
        };
        original.admit(hashing)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let operation = super::super::filesystem_key(key);
        original.spent(hashing)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        // The transaction may retain input Bytes through its candidate. Its
        // input claims are declared first, so all transaction data drops before
        // these original guards on success, failure or caller cancellation.
        let mut transferred: Option<acyclic_fs::NativeAllocation> = None;
        let mut transaction = workspace.begin_pinned_transaction_with_native_work(
            &pinned, operation, original, &io.cancellation,
        ).await.map_err(|(source, custody)| Error::NativeWorkspace { source, custody })?;
        for mutation in mutations {
            match mutation {
                WorkspaceMutation::CreateDirectory { path } => {
                    transaction.create_directories_with_native_work(
                        path, original, &io.cancellation,
                    ).await.map_err(|(source, custody)| Error::NativeWorkspace { source, custody })?;
                }
                WorkspaceMutation::PutFile { path, bytes } => {
                    let owned = if bytes.is_empty() {
                        bytes::Bytes::new()
                    } else {
                        let mut custody = original.claim(bytes.len() as u64)
                            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
                        let mut buffer = Vec::new();
                        buffer.try_reserve_exact(bytes.len()).map_err(|source| Error::NativeWork {
                            cause: acyclic_fs::NativeTreeMaterializationError::AllocationFailed(source),
                            custody: None,
                        })?;
                        custody.reconcile_capacity(buffer.capacity() as u64)
                            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
                        let copied = WorkCounters {
                            bytes_copied: bytes.len() as u64, ..WorkCounters::UNCHARGED
                        };
                        original.admit(copied)
                            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
                        buffer.extend_from_slice(bytes);
                        original.spent(copied)
                            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
                        if let Some(retained) = transferred.as_mut() {
                            retained.absorb(&mut custody)
                                .map_err(|cause| Error::NativeWork { cause, custody: None })?;
                        } else {
                            transferred = Some(custody);
                        }
                        bytes::Bytes::from(buffer)
                    };
                    transaction.write_with_native_work(
                        path, owned, original, &io.cancellation,
                    ).await.map_err(|(source, custody)| Error::NativeWorkspace { source, custody })?;
                }
                WorkspaceMutation::Remove { .. } => {
                    return Err(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Unsupported,
                        reason: "bounded publication does not remove content",
                    });
                }
            }
        }
        return match transaction.commit_with_permit_with_native_work(
            acyclic_fs::PublicationPermit::Unrestricted, original, &io.cancellation,
        ).await.map_err(|(source, custody)| Error::NativeWorkspace { source, custody })? {
            acyclic_fs::TransactionCommit::Committed(generation)
            | acyclic_fs::TransactionCommit::AlreadyCommitted(generation) => Ok(generation),
            acyclic_fs::TransactionCommit::Conflict { .. } => Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Conflict,
                reason: "workspace generation changed",
            }),
            acyclic_fs::TransactionCommit::Fenced => Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Conflict,
                reason: "workspace writer was fenced",
            }),
            acyclic_fs::TransactionCommit::IdempotencyConflict => Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Conflict,
                reason: "filesystem idempotency key belongs to another mutation",
            }),
        };
    }
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
    #[cfg(not(target_arch = "wasm32"))]
    if let Some((io, original)) = io.and_then(|io| {
        io.original_native_work().map(|original| (io, original))
    }) {
        let (reference, custody) = crate::resources::GenerationRef::new_with_native_work(
            &host.provider, &generation.id().digest().into_bytes(), None, original,
        )?;
        io.absorb_native_custody(custody)?;
        return Ok(reference);
    }
    if let Some(io) = io {
        strings(io, &[host.provider.namespace(), host.provider.family(), host.provider.version()])?;
        io.claim(32)?;
    }
    host.generation_ref(generation)
}

pub(in crate::filesystem) async fn retain<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    _host: &FilesystemHost<A, O>, generation: &acyclic_fs::Generation<A, O>, io: &PreparationIo,
) -> Result<()> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(original) = io.original_native_work() {
        let encoding = WorkCounters { bytes_encoded: 64, ..WorkCounters::UNCHARGED };
        original.admit(encoding)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let encoded = blake3::Hash::from_bytes(
            generation.id().digest().into_bytes(),
        ).to_hex();
        original.spent(encoding)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let owned = original.display(&format_args!(
            "harness-generation-{}", encoded.as_str(),
        )).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        // Declare custody before its data so rejection drops the actual string
        // first. The canonical SDK pin takes its own admitted retained copy.
        let custody = owned.1;
        let identity = owned.0;
        generation.pin_with_native_work(&identity, original, &io.cancellation)
            .await.map_err(|(source, custody)| Error::NativeWorkspace { source, custody })?;
        drop(identity);
        drop(custody);
        return Ok(());
    }
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
    let Some(io) = io else {
        grant.require(source, VolumeOperation::Read)?;
        return host.fork_volume_pinned(source, source_generation, child, paths, key).await;
    };
    grant.require_with_io(source, VolumeOperation::Read, Some(io))?;
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(original) = io.original_native_work() {
        source.validate_with_native_work(original)?;
        child.validate_with_native_work(original)?;
        if source.provider() != &host.provider || child.provider() != &host.provider
            || source == child
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "volume fork has a foreign or identical destination",
            });
        }
        let source_reference = content_workspace(&host.provider, source, Some(io))?;
        let workspace = open(host, &source_reference, io).await?;
        let pinned = generation(host, &workspace, source_generation, io).await?;
        retain(host, &pinned, io).await?;
        let child_name = volume_storage_name(child, Some(io))?;
        let hashing = WorkCounters {
            bytes_hashed: key.0.len() as u64, ..WorkCounters::UNCHARGED
        };
        original.admit(hashing)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let creation = super::super::filesystem_key(key);
        original.spent(hashing)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let options = acyclic_fs::ForkOptions::from_generation(pinned, creation);
        let options = match paths {
            Some(paths) => options.inherit_paths(paths),
            None => options,
        };
        let forked = workspace.fork_with_native_work(
            &child_name, options, original, &io.cancellation,
        ).await.map_err(|(source, custody)| Error::NativeWorkspace { source, custody })?;
        let initial = forked.operation_generation_with_native_work(
            creation, original, &io.cancellation,
        ).await.map_err(|(source, custody)| Error::NativeWorkspace { source, custody })?
            .ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Storage,
                reason: "fork creation has no durable original generation",
            })?;
        retain(host, &initial, io).await?;
        let provider = host.provider.clone_with_io(io)?;
        let reference = WorkspaceRef::new(provider, child_name.into_bytes(), None)?;
        return Ok(super::super::WorkspaceObservation {
            workspace: reference,
            generation: generation_ref(host, &initial, Some(io))?,
        });
    }
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
        values.try_reserve_exact(target - values.len()).map_err(Error::Allocation)?;
        let actual = (values.capacity() - old).checked_mul(std::mem::size_of::<T>())
            .ok_or_else(|| self.io.invalid("history vector ownership overflow"))? as u64;
        if actual > bytes { self.io.claim(actual - bytes)?; }
        else { self.io.release(bytes - actual)?; }
        Ok(())
    }
}

impl crate::store::history::HistoryIo for PreparationHistoryIo<'_> {
    #[cfg(not(target_arch = "wasm32"))]
    fn original_native_work(&self) -> Option<&acyclic_fs::NativeMaterializationWork> {
        self.io.original_native_work()
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn absorb_native_custody(&mut self, incoming: acyclic_fs::NativeAllocation) -> Result<()> {
        self.io.absorb_native_custody(incoming)
    }
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
        // Observe arrival before cancellation/quota. A rejected receipt owns
        // no new record claim and callers must not release one.
        self.io.spent(WorkCounters { authority_records_read: 1,
            authority_bytes_read: bytes, items_examined: 1,
            peak_allocation_bytes: bytes, ..Default::default() })?;
        self.io.remaining()?;
        self.io.retain_owned(bytes)
    }
    fn release_record(&mut self, bytes: u64) -> Result<()> { self.io.release(bytes) }
    fn retain_owned(&mut self, bytes: u64) -> Result<()> { self.io.retain_owned(bytes) }
    fn admit_owned(&self, bytes: u64) -> Result<()> {
        self.check_cancellation()?;
        if bytes > self.io.remaining()?.peak_allocation_bytes {
            return Err(self.io.invalid("history exceeds live logical byte allowance"));
        }
        Ok(())
    }
    fn release_owned(&mut self, bytes: u64) -> Result<()> { self.io.release(bytes) }
    fn decode_json_value(&mut self, bytes: &[u8]) -> Result<(serde_json::Value, u64)> {
        let decoded = self.io.decode_preparation::<serde_json::Value>(bytes)?;
        let held = decoded._reservation.bytes;
        Ok((decoded.into_retained(), held))
    }
    fn clone_authority(&mut self, authority: &crate::core::Authority) -> Result<crate::core::Authority> {
        Ok(crate::core::Authority { kind: authority.kind, id: self.clone_text(&authority.id)? })
    }
    fn clone_text(&mut self, text: &str) -> Result<String> {
        self.io.claim(text.len() as u64)?;
        self.io.preflight(WorkCounters { bytes_copied: text.len() as u64, ..Default::default() })?;
        if !text.is_empty() { self.io.allocation_attempt(text.len() as u64)?; }
        let mut copy = String::new();
        copy.try_reserve_exact(text.len()).map_err(Error::Allocation)?;
        if copy.capacity() > text.len() {
            self.io.claim((copy.capacity() - text.len()) as u64)?;
        }
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
        values.try_reserve_exact(capacity - values.len()).map_err(Error::Allocation)?;
        if values.capacity() > capacity { self.io.claim((values.capacity() - capacity) as u64)?; }
        Ok(())
    }
    fn reserve_text(&mut self, text: &mut String, capacity: usize) -> Result<()> {
        if capacity <= text.capacity() { return Ok(()); }
        let old = text.capacity();
        self.io.claim((capacity - old) as u64)?;
        self.io.allocation_attempt((capacity - old) as u64)?;
        text.try_reserve_exact(capacity - text.len()).map_err(Error::Allocation)?;
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
            crate::contract::JsonCodecCharge::Admission { maximum_owned_bytes, value_shape } =>
                self.io.claim(maximum_owned_bytes.max(
                    crate::contract::json_value_decode_owned_bytes(value_shape)?)),
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
        reference: &'a FileRef) -> Result<crate::store::history::HistoryContentFuture<'a, ()>> {
        let io = self.io;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.original_native_work() {
            return crate::NativeFutureBundle::new(work, || async move {
                verifier.verify_with_native_work(reference,
                    reference.descriptor().byte_length(), work)?.await
            }).map(crate::store::history::HistoryContentFuture::Native);
        }
        io.history_future(|| async move {
            io.receive_content(verifier.verify_measured(reference, reference.descriptor().byte_length(),
                io.remaining()?, &io.cancellation).await)
        })
    }
    fn read_content<'a>(&'a mut self, verifier: &'a dyn crate::conversation::ContentResidencyVerifier,
        reference: &'a FileRef) -> Result<crate::store::history::HistoryContentFuture<'a, acyclic_stream::OwnedBytes>> {
        let io = self.io;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.original_native_work() {
            return crate::NativeFutureBundle::new(work, || async move {
                verifier.read_with_native_work(reference,
                    reference.descriptor().byte_length(), work)?.await
            }).map(crate::store::history::HistoryContentFuture::Native);
        }
        io.history_future(|| async move {
            let bytes = io.receive_content(verifier.read_measured(reference, reference.descriptor().byte_length(),
                io.remaining()?, &io.cancellation).await)?;
            io.retain_owned(bytes.capacity() as u64)?;
            Ok(acyclic_stream::OwnedBytes::from(bytes))
        })
    }
}

/// Absolute canonical private metadata path, admitted before its one buffer.
pub(crate) fn content_metadata_path(path: &str, io: &PreparationIo) -> Result<String> {
    super::super::content_metadata_path_io(path, true, Some(io))
}
