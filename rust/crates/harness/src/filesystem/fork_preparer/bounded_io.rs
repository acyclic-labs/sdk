//! Finite per-attempt accounting over the Filesystem's measured engine APIs.
use super::*;
use crate::conversation::FileRef;
use acyclic_fs::{CancellationToken, OperationFailure, OperationReceipt, WorkBudget, WorkCounters, WorkspaceError};
use std::sync::Mutex;

/// Per-attempt filesystem work and allocation limits.
pub(crate) struct PreparationIo {
    ledger: PreparationLedger,
}

enum PreparationLedger {
    Owned { budget: WorkBudget, work: Mutex<(WorkCounters, u64, Option<OrdinaryPublicationBorrow>)>, cancellation: CancellationToken },
    #[cfg(not(target_arch = "wasm32"))]
    Native { work: acyclic_fs::NativeMaterializationWork, custody: Mutex<acyclic_fs::NativeAllocation> },
}

struct OrdinaryPublicationBorrow {
    allowance: WorkBudget,
    observed: WorkCounters,
    root_live: u64,
    sdk_live: u64,
}

fn observed_work(
    state: &(WorkCounters, u64, Option<OrdinaryPublicationBorrow>), mut next: WorkCounters,
) -> std::result::Result<(WorkCounters, Option<WorkCounters>), WorkspaceError> {
    let scoped = if let Some(scope) = &state.2 {
        let local_live = state.1.checked_sub(scope.root_live).ok_or(acyclic_fs::WorkError::Overflow)?;
        let mut local = next;
        local.peak_allocation_bytes = local.peak_allocation_bytes.checked_add(local_live)
            .ok_or(acyclic_fs::WorkError::Overflow)?;
        let observed = scope.observed.checked_add(local)?;
        next.peak_allocation_bytes = next.peak_allocation_bytes.checked_add(scope.sdk_live)
            .ok_or(acyclic_fs::WorkError::Overflow)?;
        Some(observed)
    } else { None };
    next.peak_allocation_bytes = next.peak_allocation_bytes.checked_add(state.1)
        .ok_or(acyclic_fs::WorkError::Overflow)?;
    Ok((state.0.checked_add(next)?, scoped))
}

struct OrdinaryPublicationScope<'a> {
    io: &'a PreparationIo,
}

impl Drop for OrdinaryPublicationScope<'_> {
    fn drop(&mut self) { self.io.finish_publication_borrow(); }
}

/// A real logical quota reservation spanning an audited opaque serde decode.
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
        #[cfg(not(target_arch = "wasm32"))]
        if self.io.uses_native_work() {
            drop(self.io.take_native_custody(self.bytes).expect("decode reservation belongs to its live ledger"));
            return;
        }
        self.io.release(self.bytes).expect("decode reservation belongs to its live ledger");
    }
}

fn finish_decode<'a, T>(io: &'a PreparationIo, result: Result<T>, reservation: Option<DecodeReservation<'a>>,
    diagnostics: u64) -> Result<ReservedDecoded<'a, T>> {
    #[cfg(not(target_arch = "wasm32"))]
    if io.uses_native_work() {
        let mut native = reservation.map(|mut reservation| {
            let custody = io.take_native_custody(reservation.bytes)?;
            reservation.bytes = 0;
            Ok::<_, Error>(custody)
        }).transpose()?;
        let value = result.map_err(|error| match error {
            Error::NativeDecode { code, source, custody: None } => Error::NativeDecode { code, source, custody: native.take() },
            error => error,
        })?;
        let mut guard = native.ok_or_else(|| io.invalid("trusted decode omitted admission"))?;
        if diagnostics != 0 && let Err(cause) = guard.substitute(guard.bytes() - diagnostics) {
            return Err(Error::NativeWork { cause, custody: Some(guard) });
        }
        let bytes = guard.bytes();
        io.absorb_native_custody(guard)?;
        return Ok(ReservedDecoded { value, _reservation: DecodeReservation { io, bytes } });
    }
    let value = result?;
    let mut reservation = reservation.ok_or_else(|| io.invalid("trusted decode omitted admission"))?;
    if diagnostics != 0 { reservation.substitute(reservation.bytes - diagnostics)?; }
    Ok(ReservedDecoded { value, _reservation: reservation })
}

#[cfg(not(target_arch = "wasm32"))]
fn finish_encoding<T>(mut diagnostics: DecodeReservation<'_>, result: Result<T>) -> Result<T> {
    match result {
        Err(Error::NativeEncode { code, source, custody: None }) => {
            let custody = diagnostics.io.take_native_custody(diagnostics.bytes)?;
            diagnostics.bytes = 0;
            Err(Error::NativeEncode { code, source, custody: Some(custody) })
        }
        other => other,
    }
}

/// Audited closed Harness DTOs only, including their Value/string/vector containers.
pub(crate) trait PreparationRecord: serde::de::DeserializeOwned + Serialize {
    fn maximum_slot_bytes() -> u64 { preparation_slot_bytes::<Self>() }
    /// Static diagnostic label bound.
    fn diagnostic_label_bytes() -> u64 { crate::contract::preparation_serde_diagnostic_label_bytes() }
    /// Owned-byte allowance for the closed DTO visitor.
    fn decode_owned_allowance(
        shape: crate::contract::JsonValueShape, codec_scratch: u64, input_bytes: u64,
        native_code: Option<crate::wire::ErrorCode>,
    ) -> Result<u64> {
        let value_owned = match Self::json_value_schema() {
            crate::contract::JsonValueSchema::None => codec_scratch
                .checked_add(crate::contract::sdk_json_diagnostic_bytes())
                .ok_or_else(|| decode_allowance_error("preparation codec base overflow", native_code))?,
            _ => codec_scratch.max(crate::contract::json_value_decode_owned_bytes(shape)
                .map_err(|error| decode_allowance_with_code(error, native_code))?),
        };
        let nodes = shape.lexical_items().checked_add(shape.virtual_items())
            .ok_or_else(|| decode_allowance_error("preparation DTO item count overflow", native_code))?;
        let typed = match Self::json_value_schema() {
            crate::contract::JsonValueSchema::Root => Some(0),
            _ => nodes.checked_mul(Self::maximum_slot_bytes())
                .and_then(|bytes| bytes.checked_mul(8))
                .and_then(|bytes| input_bytes.checked_mul(4)
                    .and_then(|strings| bytes.checked_add(strings))),
        };
        typed.and_then(|bytes| bytes.checked_add(value_owned))
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Self>() as u64))
            .ok_or_else(|| decode_allowance_error("preparation DTO ownership overflow", native_code))
    }
    /// Value-free private DTOs use None; Value-bearing records must override it.
    fn json_value_schema() -> crate::contract::JsonValueSchema {
        crate::contract::JsonValueSchema::None
    }
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
        [
            std::mem::size_of::<Self>(),
            std::mem::size_of::<serde_json::Value>(),
            std::mem::size_of::<(String, serde_json::Value)>(),
            std::mem::size_of::<String>(),
            std::mem::size_of::<crate::OperationId>(),
        ].into_iter().max().unwrap_or(0) as u64
    }
    fn json_value_schema() -> crate::contract::JsonValueSchema {
        crate::contract::JsonValueSchema::TaskOutcome
    }
}

impl PreparationRecord for serde_json::Value {
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
        self._reservation.bytes = 0;
        self.value
    }
}
pub(super) fn budget(units: u64, bytes: u64) -> WorkBudget {
    WorkBudget {
            authority_records_read: units, authority_records_appended: units,
            authority_bytes_read: bytes, authority_bytes_written: bytes,
            object_probes: units, backend_read_operations: units, backend_write_operations: units,
            durability_operations: units, page_reads: units, page_writes: units,
            object_bytes_read: bytes, object_bytes_written: bytes,
            bytes_hashed: bytes, bytes_copied: bytes, bytes_encoded: bytes,
            source_bytes_read: bytes, source_path_components: units, source_entries_visited: units,
            output_bytes: bytes, items_examined: units, items_returned: units,
            allocation_operations: units, peak_allocation_bytes: bytes, materializations: units,
    }
}

impl PreparationIo {
    pub(crate) fn new(
        request: &crate::resource_preparation::ResourcePreparationRequest,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        request.validate()?;
        Self::from_budget(budget(request.maximum_work, request.maximum_bytes), cancellation)
    }

    pub(crate) fn from_budget(budget: WorkBudget, cancellation: &CancellationToken) -> Result<Self> {
        if !budget.has_finite_allowances() {
            return Err(Error::Invalid("measured content requires finite work allowances".into()));
        }
        cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
        Ok(Self { ledger: PreparationLedger::Owned { budget, work: Mutex::new((WorkCounters::default(), 0, None)), cancellation: cancellation.clone() } })
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn from_native_work(work: &acyclic_fs::NativeMaterializationWork) -> Result<Self> {
        work.remaining().map_err(Error::from)?;
        let custody = work.retain_owned(0).map_err(Error::from)?;
        Ok(Self { ledger: PreparationLedger::Native { work: work.clone(), custody: Mutex::new(custody) } })
    }

    /// Moves genuine existing custody without consulting cancellation or quota.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn from_native_custody(custody: acyclic_fs::NativeAllocation) -> Self {
        let work = custody.work().clone();
        Self { ledger: PreparationLedger::Native { work, custody: Mutex::new(custody) } }
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
                .split_off(bytes).map_err(Error::from),
            PreparationLedger::Owned { .. } => Err(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout, custody: None,
            }),
        }
    }

    pub(crate) fn cancellation(&self) -> &CancellationToken {
        match &self.ledger {
            PreparationLedger::Owned { cancellation, .. } => cancellation,
            #[cfg(not(target_arch = "wasm32"))]
            PreparationLedger::Native { work, .. } => work.cancellation(),
        }
    }

    pub(crate) fn check_cancellation(&self) -> Result<()> {
        match self.cancellation().check() {
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

    pub(crate) fn for_unmeasured(cancellation: &CancellationToken) -> Result<Self> {
        cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
        Ok(Self {
            ledger: PreparationLedger::Owned { budget: WorkBudget::UNBOUNDED,
                work: Mutex::new((WorkCounters::default(), 0, None)), cancellation: cancellation.clone() },
        })
    }

    fn borrow_publication(&self, budget: WorkBudget, allowance: WorkBudget) -> Result<OrdinaryPublicationScope<'_>> {
        self.check_cancellation()?;
        match &self.ledger {
            PreparationLedger::Owned { work, .. } => {
                let mut state = work.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                if state.2.is_some() { return Err(Error::Invalid("nested ordinary publication accounting".into())); }
                let sdk_live = budget.peak_allocation_bytes.checked_sub(allowance.peak_allocation_bytes)
                    .ok_or_else(|| Error::from(WorkspaceError::from(acyclic_fs::WorkError::Overflow)))?;
                state.2 = Some(OrdinaryPublicationBorrow {
                    allowance, observed: WorkCounters::default(), root_live: state.1, sdk_live,
                });
                Ok(OrdinaryPublicationScope { io: self })
            }
            #[cfg(not(target_arch = "wasm32"))]
            PreparationLedger::Native { .. } => Err(Error::Invalid("ordinary publication requires ordinary accounting".into())),
        }
    }

    fn finish_publication_borrow(&self) -> WorkCounters {
        match &self.ledger {
            PreparationLedger::Owned { work, .. } => work.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner).2.take()
                .map_or_else(WorkCounters::default, |scope| scope.observed),
            #[cfg(not(target_arch = "wasm32"))]
            PreparationLedger::Native { .. } => WorkCounters::default(),
        }
    }

    pub(crate) fn preflight(&self, next: WorkCounters) -> Result<()> {
        self.check_cancellation()?;
        match &self.ledger {
            PreparationLedger::Owned { .. } => self.preflight_workspace(next).map_err(Error::from),
            #[cfg(not(target_arch = "wasm32"))]
            PreparationLedger::Native { work, .. } => work.admit(next).map_err(Error::from),
        }
    }

    fn preflight_workspace(&self, next: WorkCounters) -> std::result::Result<(), WorkspaceError> {
        self.cancellation().check()?;
        match &self.ledger {
            PreparationLedger::Owned { budget, work, .. } => {
                let state = work.lock().map_err(|_| WorkspaceError::Engine(
                    "creation IO accounting is poisoned".into()))?;
                let (recorded, scoped) = observed_work(&state, next)?;
                if let (Some(scope), Some(observed)) = (&state.2, scoped) { observed.verify(scope.allowance)?; }
                recorded.verify(*budget)?;
                Ok(())
            }
            #[cfg(not(target_arch = "wasm32"))]
            PreparationLedger::Native { work, .. } => work.admit(next)
                .map_err(|cause| WorkspaceError::Filesystem(acyclic_fs::FsError::NativeKernel(cause))),
        }
    }

    pub(crate) fn claim(&self, bytes: u64) -> Result<()> {
        self.check_cancellation()?;
        match &self.ledger {
            PreparationLedger::Owned { budget, work, .. } => {
                let mut state = work.lock().map_err(|_| Error::Storage("creation IO accounting is poisoned".into()))?;
                let live = state.1.checked_add(bytes).ok_or_else(|| Error::Invalid("content ownership overflow".into()))?;
                let mut peak = live;
                if let Some(scope) = &mut state.2 {
                    let local = live.checked_sub(scope.root_live)
                        .ok_or_else(|| Error::from(WorkspaceError::from(acyclic_fs::WorkError::Overflow)))?;
                    let observed = scope.observed.checked_add(WorkCounters { peak_allocation_bytes: local, ..Default::default() })
                        .map_err(|error| Error::from(WorkspaceError::from(error)))?;
                    observed.verify(scope.allowance).map_err(|error| Error::from(WorkspaceError::from(error)))?;
                    scope.observed = observed;
                    peak = peak.checked_add(scope.sdk_live)
                        .ok_or_else(|| Error::from(WorkspaceError::from(acyclic_fs::WorkError::Overflow)))?;
                }
                let next = state.0.checked_add(WorkCounters { peak_allocation_bytes: peak, ..Default::default() })
                    .map_err(|error| Error::from(WorkspaceError::from(error)))?;
                next.verify(*budget).map_err(|error| Error::from(WorkspaceError::from(error)))?;
                state.0 = next;
                state.1 = live;
                Ok(())
            }
            #[cfg(not(target_arch = "wasm32"))]
            PreparationLedger::Native { custody, .. } => {
                let mut guard = custody.lock().map_err(|_| Error::Storage("native preparation custody is poisoned".into()))?;
                let live = guard.bytes().checked_add(bytes).ok_or_else(|| Error::Invalid("native preparation ownership overflow".into()))?;
                guard.resize_retained(live).map_err(Error::from)
            }
        }
    }

    pub(crate) fn allocation_attempt(&self, bytes: u64) -> Result<()> {
        self.charged(WorkCounters { allocation_operations: u64::from(bytes != 0), ..Default::default() })
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
                .release(bytes).map_err(Error::from),
        }
    }

    fn reserve_typed(&self, owned: u64, diagnostics: u64, native_code: Option<crate::wire::ErrorCode>) -> Result<DecodeReservation<'_>> {
        let bytes = owned.checked_add(diagnostics)
            .ok_or_else(|| decode_allowance_error("typed diagnostic reservation overflow", native_code))?;
        self.reserve_decode(bytes)
    }

    pub(crate) fn reserve_decode(&self, bytes: u64) -> Result<DecodeReservation<'_>> {
        self.claim(bytes)?;
        Ok(DecodeReservation { io: self, bytes })
    }

    pub(crate) fn guard_owned(&self, bytes: u64) -> Result<DecodeReservation<'_>> {
        if bytes > self.owned_bytes()? { return Err(Error::Invalid("owned buffer exceeds retained quota".into())); }
        Ok(DecodeReservation { io: self, bytes })
    }

    pub(crate) fn guard_bytes(&self, bytes: &acyclic_stream::OwnedBytes) -> Result<Option<DecodeReservation<'_>>> {
        if self.uses_native_work() { return Ok(None); }
        self.guard_owned(bytes.retained_capacity().unwrap_or(0) as u64).map(Some)
    }

    pub(crate) fn invalid(&self, reason: &'static str) -> Error {
        failure(Some(self), crate::wire::ErrorCode::Invalid, reason)
    }

    pub(crate) fn remaining(&self) -> Result<WorkBudget> {
        self.check_cancellation()?;
        match &self.ledger {
            PreparationLedger::Owned { .. } => self.remaining_workspace().map_err(Error::from),
            #[cfg(not(target_arch = "wasm32"))]
            PreparationLedger::Native { work, .. } => work.remaining().map_err(Error::from),
        }
    }

    /// The same finite allowance, preserving the common SDK accounting cause.
    fn remaining_workspace(&self) -> std::result::Result<WorkBudget, WorkspaceError> {
        self.cancellation().check()?;
        match &self.ledger {
            PreparationLedger::Owned { budget, work, .. } => {
                let state = work.lock().map_err(|_| WorkspaceError::Engine(
                    "creation IO accounting is poisoned".into()))?;
                if let Some(scope) = &state.2 {
                    let local_live = state.1.checked_sub(scope.root_live)
                        .ok_or(acyclic_fs::WorkError::Overflow)?;
                    let mut remaining = scope.observed.remaining(scope.allowance)?;
                    remaining.peak_allocation_bytes = remaining.peak_allocation_bytes
                        .checked_sub(local_live).ok_or(acyclic_fs::WorkError::Overflow)?;
                    return Ok(remaining);
                }
                let mut remaining = state.0.remaining(*budget)?;
                remaining.peak_allocation_bytes = remaining.peak_allocation_bytes
                    .checked_sub(state.1).ok_or(acyclic_fs::WorkError::Overflow)?;
                Ok(remaining)
            }
            #[cfg(not(target_arch = "wasm32"))]
            PreparationLedger::Native { work, .. } => work.remaining()
                .map_err(|cause| WorkspaceError::Filesystem(acyclic_fs::FsError::NativeKernel(cause))),
        }
    }

    pub(crate) fn charge(&self, next: WorkCounters) -> Result<()> {
        self.spent(next)?;
        match &self.ledger {
            PreparationLedger::Owned { budget, .. } => self.receipt().verify(*budget).map_err(|error| Error::Invalid(error.to_string())),
            #[cfg(not(target_arch = "wasm32"))]
            PreparationLedger::Native { work, .. } => work.remaining().map(|_| ()).map_err(Error::from),
        }
    }

    pub(crate) fn spent(&self, next: WorkCounters) -> Result<()> {
        match &self.ledger {
            PreparationLedger::Owned { .. } => self.spent_workspace(next).map_err(Error::from),
            #[cfg(not(target_arch = "wasm32"))]
            PreparationLedger::Native { work, .. } => work.spent(next).map_err(Error::from),
        }
    }

    fn spent_workspace(&self, next: WorkCounters) -> std::result::Result<(), WorkspaceError> {
        match &self.ledger {
            PreparationLedger::Owned { work, .. } => {
                let mut state = work.lock().map_err(|_| WorkspaceError::Engine(
                    "creation IO accounting is poisoned".into()))?;
                let (recorded, scoped) = observed_work(&state, next)?;
                if let Some(observed) = scoped {
                    state.2.as_mut().expect("scoped prefix has its original scope").observed = observed;
                }
                state.0 = recorded;
                Ok(())
            }
            #[cfg(not(target_arch = "wasm32"))]
            PreparationLedger::Native { work, .. } => work.spent(next)
                .map_err(|cause| WorkspaceError::Filesystem(acyclic_fs::FsError::NativeKernel(cause))),
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
                Err(Error::from(failure.error))
            }
        }
    }

    pub(crate) fn receive_content<T>(&self, result: acyclic_fs::MeasuredResult<OperationReceipt<T>, Error>) -> Result<T> {
        match result {
            Ok(receipt) => { self.charge(receipt.work)?; Ok(receipt.value) }
            Err(failure) => { self.spent(*failure.work)?; Err(failure.error) }
        }
    }

    pub(crate) fn encode<T: Serialize>(&self, value: &T, maximum_bytes: u64, canonical: bool) -> Result<acyclic_stream::OwnedBytes> {
        let remaining = self.remaining()?;
        if maximum_bytes != 0 {
            self.preflight(WorkCounters { bytes_encoded: 1, items_examined: 1, ..Default::default() })?;
        }
        let bounds = crate::contract::JsonBounds {
            maximum_bytes: maximum_bytes.min(remaining.bytes_encoded),
            maximum_items: remaining.items_examined, maximum_depth: 256,
        };
        let mut charge = |next| self.codec_charge(next, true);
        let code = self.uses_native_work().then_some(crate::wire::ErrorCode::Invalid);
        #[cfg(not(target_arch = "wasm32"))]
        let diagnostics = if code.is_some() {
            Some(self.reserve_decode(crate::contract::sdk_json_diagnostic_bytes())?)
        } else { None };
        let result = if canonical {
            crate::contract::canonical_json_bytes_bounded(value, bounds, code, &mut charge)
        } else {
            crate::contract::json_bytes_bounded(value, bounds, code, &mut charge)
        };
        #[cfg(not(target_arch = "wasm32"))]
        let result = match diagnostics { Some(diagnostics) => finish_encoding(diagnostics, result), None => result };
        source_bytes_from_vec(result?, Some(self))
    }

    pub(crate) fn capability<T: Serialize>(&self, value: &T, prefix: &str) -> Result<String> {
        let _encoded_hold;
        let encoded = self.encode(value, self.remaining()?.bytes_encoded, true)?;
        _encoded_hold = self.guard_bytes(&encoded)?;
        let digest = digest(&encoded, Some(self))?;
        join_text(prefix, digest.to_hex().as_str(), self)
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

    /// Admit the actual ordinary History box before constructing it.
    pub(crate) fn history_future<'a, T, F, B>(
        &'a self, build: B,
    ) -> Result<crate::store::history::HistoryContentFuture<'a, T>>
    where
        F: Future<Output = Result<T>> + acyclic_stream::ProviderTask + 'a,
        B: FnOnce() -> F,
    {
        let bytes = std::mem::size_of::<F>() as u64;
        self.claim(bytes)?;
        if bytes != 0 { self.allocation_attempt(bytes)?; }
        Ok(crate::store::history::HistoryContentFuture::Ordinary {
            future: Box::pin(build()), frame_bytes: bytes,
        })
    }

    pub(crate) async fn verify_publication_lease(
        &self, lease: &dyn crate::resource_preparation::CreatedTaskLease,
    ) -> Result<()> {
        let future = lease.verify_with_io(self)?;
        let frame = match &future {
            crate::store::history::HistoryContentFuture::Ordinary { frame_bytes, .. } => *frame_bytes,
            #[cfg(not(target_arch = "wasm32"))]
            crate::store::history::HistoryContentFuture::Native(_) => 0,
        };
        let result = future.await;
        let released = if frame == 0 { Ok(()) } else { self.release(frame) };
        result.and(released)
    }

    pub(crate) fn decode_json<T: UploadRecord>(&self, bytes: &[u8]) -> Result<T> {
        let remaining = self.remaining()?;
        let native_code = self.uses_native_work().then_some(crate::wire::ErrorCode::Storage);
        let owned = (bytes.len() as u64).checked_mul(2)
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<T>() as u64))
            .ok_or_else(|| decode_allowance_error("upload decode ownership overflow", native_code))?;
        let bounds = crate::contract::JsonBounds {
            maximum_bytes: bytes.len() as u64, maximum_items: remaining.items_examined, maximum_depth: 256,
        };
        let mut reservation = None;
        let mut diagnostic_bytes = 0;
        let mut charge = |next| match next {
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
                    diagnostic_bytes = diagnostics;
                    reservation = Some(self.reserve_typed(owned, diagnostics, native_code)?);
                    Ok(())
                }
                next => self.codec_charge(next, false),
            };
        #[cfg(not(target_arch = "wasm32"))]
        let result: Result<T> = if self.original_native_work().is_some() {
            crate::contract::json_from_slice_bounded_native_trusted(bytes, bounds, owned,
                crate::wire::ErrorCode::Storage, T::json_value_schema(), &mut charge)
        } else { crate::contract::json_from_slice_bounded_trusted(bytes, bounds, owned, &mut charge) };
        #[cfg(target_arch = "wasm32")]
        let result: Result<T> = crate::contract::json_from_slice_bounded_trusted(bytes, bounds, owned, &mut charge);
        let mut decoded = finish_decode(self, result, reservation, diagnostic_bytes)?;
        decoded._reservation.substitute(decoded.value.logical_owned_bytes()?)?;
        Ok(decoded.into_retained())
    }

    pub(crate) fn decode_preparation<T: PreparationRecord>(
        &self, bytes: &[u8], code: crate::wire::ErrorCode,
    ) -> Result<ReservedDecoded<'_, T>> {
        self.decode_seed(
            std::marker::PhantomData::<T>, bytes, code,
        )
    }

    pub(crate) fn decode_seed<'a, 'de, T: PreparationRecord>(
        &'a self, seed: impl serde::de::DeserializeSeed<'de, Value = T>,
        bytes: &'de [u8], code: crate::wire::ErrorCode,
    ) -> Result<ReservedDecoded<'a, T>> {
        let bounds = crate::contract::JsonBounds {
            maximum_bytes: bytes.len() as u64,
            maximum_items: self.remaining()?.items_examined,
            maximum_depth: 128,
        };
        let mut reservation = None;
        let mut diagnostic_bytes = 0;
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
                    diagnostic_bytes = diagnostics;
                    reservation = Some(self.reserve_typed(owned, diagnostics, native_code)?);
                    Ok(())
                }
                next => self.codec_charge(next, false),
            }
        };
        #[cfg(not(target_arch = "wasm32"))]
        if self.original_native_work().is_some() {
            let result = crate::contract::json_from_slice_seed_bounded_native_trusted(
                seed, bytes, bounds, 0, code, value_schema, &mut charge,
            );
            return finish_decode(self, result, reservation, diagnostic_bytes);
        }
        let _ = code;
        let result = crate::contract::json_from_slice_seed_bounded_trusted(
            seed, bytes, bounds, 0, value_schema, &mut charge,
        );
        finish_decode(self, result, reservation, diagnostic_bytes)
    }

    /// Builds the original serde JSON graph directly.
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
        let diagnostics = self.reserve_decode(crate::contract::sdk_json_diagnostic_bytes())?;
        let result = crate::contract::json_value_bounded(
            source, bounds, Some(crate::wire::ErrorCode::Invalid),
            &mut |next| self.codec_charge(next, false),
        );
        let value = finish_encoding(diagnostics, result)?;
        root_hold.into_retained();
        Ok(value)
    }

    /// Checks the original JSON wire-size policy without building a graph or buffer.
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
        let mut reservation = None;
        let mut diagnostic_bytes = 0;
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
                    diagnostic_bytes = diagnostics;
                    reservation = Some(self.reserve_typed(allowance, diagnostics, Some(code))?);
                    Ok(())
                }
                next => self.codec_charge(next, false),
            },
        );
        finish_decode(self, result, reservation, diagnostic_bytes)
    }

    pub(crate) fn codec_charge(&self, next: crate::contract::JsonCodecCharge, encode: bool) -> Result<()> {
        match next {
            crate::contract::JsonCodecCharge::Retain { bytes } => self.claim(bytes),
            crate::contract::JsonCodecCharge::Release { bytes } => self.release(bytes),
            crate::contract::JsonCodecCharge::CopyAdmission { bytes } =>
                self.preflight(WorkCounters { bytes_copied: bytes, ..Default::default() }),
            crate::contract::JsonCodecCharge::AllocationOperation { bytes } =>
                self.allocation_attempt(bytes),
            crate::contract::JsonCodecCharge::Admission { .. } => Err(self.invalid(
                "trusted decode admission requires a scoped reservation")),
            crate::contract::JsonCodecCharge::Copy { bytes } =>
                self.charged(WorkCounters { bytes_copied: bytes, ..Default::default() }),
            crate::contract::JsonCodecCharge::Read { bytes } =>
                self.charged(WorkCounters { source_bytes_read: bytes, ..Default::default() }),
            crate::contract::JsonCodecCharge::Inspect { items } =>
                self.charged(WorkCounters { items_examined: items, ..Default::default() }),
            crate::contract::JsonCodecCharge::Work { bytes, items } => {
                let next = WorkCounters {
                    bytes_encoded: if encode { bytes } else { 0 },
                    source_bytes_read: if encode { 0 } else { bytes },
                    items_examined: items, ..Default::default()
                };
                self.charged(next)
            }
        }
    }
}

pub(crate) trait UploadRecord: serde::de::DeserializeOwned {
    /// Deep owned capacities only.
    fn logical_owned_bytes(&self) -> Result<u64>;
    fn diagnostic_label_bytes() -> u64 { crate::contract::preparation_serde_diagnostic_label_bytes() }
    fn json_value_schema() -> crate::contract::JsonValueSchema {
        crate::contract::JsonValueSchema::None
    }
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

pub(in crate::filesystem) fn failure(io: Option<&PreparationIo>, code: crate::wire::ErrorCode, reason: &'static str) -> Error {
    #[cfg(not(target_arch = "wasm32"))]
    if io.is_some_and(PreparationIo::uses_native_work) {
        return Error::NativeBoundary { code, reason };
    }
    match code {
        crate::wire::ErrorCode::Unauthorized => Error::Unauthorized(reason.into()),
        crate::wire::ErrorCode::Unsupported => Error::Unsupported(reason.into()),
        crate::wire::ErrorCode::Conflict => Error::Conflict(reason.into()),
        crate::wire::ErrorCode::Storage => Error::Storage(reason.into()),
        _ => Error::Invalid(reason.into()),
    }
}

pub(in crate::filesystem) fn validate_content_path(path: &str, io: Option<&PreparationIo>) -> Result<()> {
    crate::executor::with_history_io(io, |work| crate::conversation::validate_content_path(path, work))
}

pub(in crate::filesystem) fn clone_text(value: &str, io: Option<&PreparationIo>) -> Result<String> {
    match io {
        Some(io) => crate::store::history::HistoryIo::clone_text(&mut io.history_io(), value),
        None => Ok(value.to_owned()),
    }
}

pub(super) fn join_text(prefix: &str, suffix: &str, io: &PreparationIo) -> Result<String> {
    let length = prefix.len().checked_add(suffix.len()).ok_or_else(|| io.invalid("text size overflow"))?;
    let copy = WorkCounters { bytes_copied: length as u64, ..Default::default() };
    io.preflight(copy)?;
    io.claim(length as u64)?;
    io.allocation_attempt(length as u64)?;
    let mut value = String::new();
    value.try_reserve_exact(length).map_err(Error::Allocation)?;
    value.push_str(prefix);
    value.push_str(suffix);
    io.charge(copy)?;
    if value.capacity() > length { io.claim((value.capacity() - length) as u64)?; }
    Ok(value)
}

pub(in crate::filesystem) fn hex_key(bytes: &[u8], io: Option<&PreparationIo>) -> Result<String> {
    let Some(io) = io else { return Ok(hex::encode(bytes)); };
    let capacity = bytes.len().checked_mul(2).ok_or_else(|| failure(Some(io), crate::wire::ErrorCode::Invalid, "hex key size overflow"))?;
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

pub(in crate::filesystem) fn reserve<T>(values: &mut Vec<T>, capacity: usize, io: Option<&PreparationIo>) -> Result<()> {
    if capacity <= values.capacity() { return Ok(()); }
    let old = values.capacity();
    let bytes = (capacity - old).checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| failure(io, crate::wire::ErrorCode::Invalid, "vector capacity overflow"))? as u64;
    let copy = WorkCounters { bytes_copied: (values.len() * std::mem::size_of::<T>()) as u64, ..Default::default() };
    if let Some(io) = io {
        io.claim(bytes)?;
        io.preflight(copy)?;
        io.allocation_attempt(bytes)?;
    }
    values.try_reserve_exact(capacity - values.len()).map_err(Error::Allocation)?;
    if let Some(io) = io {
        io.claim(((values.capacity() - capacity) * std::mem::size_of::<T>()) as u64)?;
        io.charge(copy)?;
    }
    Ok(())
}

pub(in crate::filesystem) fn vector<T>(capacity: usize, io: Option<&PreparationIo>) -> Result<Vec<T>> {
    let mut output = Vec::new();
    reserve(&mut output, capacity, io)?;
    Ok(output)
}

pub(in crate::filesystem) fn clone_bytes(bytes: &[u8], io: Option<&PreparationIo>) -> Result<acyclic_stream::OwnedBytes> {
    let copied = WorkCounters { bytes_copied: bytes.len() as u64, ..Default::default() };
    if let Some(io) = io { io.preflight(copied)?; }
    let mut output = vector(bytes.len(), io)?;
    output.extend_from_slice(bytes);
    if let Some(io) = io { io.charge(copied)?; }
    source_bytes_from_vec(output, io)
}

pub(in crate::filesystem) fn source_bytes_from_vec(bytes: Vec<u8>, io: Option<&PreparationIo>) -> Result<acyclic_stream::OwnedBytes> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
        let original = io.original_native_work().expect("native source byte original");
        let custody = match io.take_native_custody(bytes.capacity() as u64) {
            Ok(custody) => custody,
            Err(error) => { drop(bytes); return Err(error); }
        };
        return acyclic_stream::OwnedBytes::from_vec_with_guard(bytes, custody, original.runtime_meter_owner())
            .map_err(|failure| {
                let error = failure.into_error();
                Error::NativeWork { cause: match error {
                    acyclic_stream::PreparedOwnedBytesError::Admission(error) => original.runtime_error(error),
                    acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized
                    => acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
                }, custody: None }
            });
    }
    let _ = io;
    Ok(bytes.into())
}

pub(in crate::filesystem) fn strings(io: &PreparationIo, values: &[&str]) -> Result<()> {
    for value in values {
        io.claim(value.len() as u64)?;
    }
    Ok(())
}

pub(in crate::filesystem) fn clone_file(reference: &FileRef, io: Option<&PreparationIo>) -> Result<FileRef> {
    match io {
        Some(io) => reference.copy(&mut Some(&mut io.history_io())),
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

macro_rules! filesystem_call {
    ($io:expr, $work:ident, $native:expr, $measured:expr) => {{
        let io = $io;
        #[cfg(not(target_arch = "wasm32"))]
        let result = if let Some($work) = io.original_native_work() {
            $native.await.map_err(native_workspace_error)
        } else { io.receive($measured.await) };
        #[cfg(target_arch = "wasm32")]
        let result = io.receive($measured.await);
        result
    }};
}
pub(super) use filesystem_call;

pub(super) fn digest(bytes: &[u8], io: Option<&PreparationIo>) -> Result<blake3::Hash> {
    let work = WorkCounters { bytes_hashed: bytes.len() as u64, ..Default::default() };
    if let Some(io) = io { io.preflight(work)?; }
    let digest = blake3::hash(bytes);
    if let Some(io) = io { io.charge(work)?; }
    Ok(digest)
}

pub(super) fn operation_key(key: &IdempotencyKey, io: Option<&PreparationIo>) -> Result<acyclic_fs::IdempotencyKey> {
    let hash = WorkCounters { bytes_hashed: key.as_str().len() as u64, ..Default::default() };
    if let Some(io) = io { io.preflight(hash)?; }
    let key = super::super::filesystem_key(key);
    if let Some(io) = io { io.charge(hash)?; }
    Ok(key)
}

pub(in crate::filesystem) async fn operation_generation<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>, reference: &WorkspaceRef, key: &IdempotencyKey, io: Option<&PreparationIo>,
) -> Result<Option<acyclic_fs::Generation<A, O>>> {
    let Some(io) = io else {
        return host.open(reference).await?.operation_generation(super::super::filesystem_key(key),
            WorkBudget::UNBOUNDED, &CancellationToken::new()).await
            .map(|receipt| receipt.value).map_err(|failure| map_error(failure.error));
    };
    let workspace = open(host, reference, io).await?;
    let operation = operation_key(key, Some(io))?;
    filesystem_call!(io, original,
        workspace.operation_generation_with_native_work(acyclic_fs::OperationId::from_bytes(operation.into_bytes()), original, io.cancellation()),
        workspace.operation_generation(operation, io.remaining()?, io.cancellation()))
}

pub(in crate::filesystem) fn content_workspace(
    host_provider: &crate::resources::ProviderRef, volume: &VolumeRef, io: Option<&PreparationIo>,
) -> Result<WorkspaceRef> {
    let name = volume_storage_name(volume, io)?;
    let provider = match io {
        Some(io) => host_provider.clone_with_io(io)?,
        None => host_provider.clone(),
    };
    WorkspaceRef::new(provider, name.into_bytes(), None)
}

pub(in crate::filesystem) fn volume_storage_name(volume: &VolumeRef, io: Option<&PreparationIo>) -> Result<String> {
    let Some(io) = io else { return volume.storage_name(&mut None); };
    let encoded = io.encode(volume, io.remaining()?.bytes_encoded, true)?;
    let hold = io.guard_bytes(&encoded)?;
    let digest = digest(&encoded, Some(io))?;
    let encoding = WorkCounters { bytes_encoded: if io.uses_native_work() { 64 } else { 0 }, ..Default::default() };
    io.preflight(encoding)?;
    let mut label = [0_u8; 72];
    label[..8].copy_from_slice(b"harness-");
    label[8..].copy_from_slice(digest.to_hex().as_bytes());
    io.charge(encoding)?;
    drop(encoded);
    drop(hold);
    clone_text(std::str::from_utf8(&label).expect("ASCII storage name"), Some(io))
}

pub(crate) async fn open<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>, reference: &WorkspaceRef, io: &PreparationIo,
) -> Result<acyclic_fs::Workspace<A, O>> {
    reference.validate_with_work(&mut Some(&mut io.history_io()))?;
    if reference.as_resource().provider() != &host.provider {
        return Err(failure(Some(io), crate::wire::ErrorCode::Invalid, "resource belongs to another Filesystem provider"));
    }
    let name = std::str::from_utf8(reference.as_resource().key())
        .map_err(|_| failure(Some(io), crate::wire::ErrorCode::Invalid, "workspace reference key must be UTF-8"))?;
    filesystem_call!(io, original,
        host.filesystem.open_workspace_with_native_work(name, original, io.cancellation()),
        host.filesystem.open_workspace(name, io.remaining()?, io.cancellation()))
}

pub(crate) async fn generation<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>, workspace: &acyclic_fs::Workspace<A, O>,
    reference: &crate::resources::GenerationRef, io: &PreparationIo,
) -> Result<acyclic_fs::Generation<A, O>> {
    reference.validate_with_work(&mut Some(&mut io.history_io()))?;
    if reference.as_resource().provider() != &host.provider {
        return Err(failure(Some(io), crate::wire::ErrorCode::Invalid, "resource belongs to another Filesystem provider"));
    }
    let bytes: [u8; 32] = reference.as_resource().key().try_into()
        .map_err(|_| failure(Some(io), crate::wire::ErrorCode::Invalid, "generation reference key must be 32 bytes"))?;
    let id = acyclic_fs::GenerationId::new(acyclic_fs::Digest::from_bytes(bytes));
    filesystem_call!(io, original,
        workspace.generation_with_native_work(id, original, io.cancellation()),
        workspace.generation(id, io.remaining()?, io.cancellation()))
}

pub(in crate::filesystem) async fn read<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>, reference: &WorkspaceRef, path: &str, maximum_bytes: u64,
    io: Option<&PreparationIo>,
) -> Result<acyclic_stream::OwnedBytes> {
    let Some(io) = io else { return host.read(reference, None, path, maximum_bytes).await; };
    let workspace = open(host, reference, io).await?;
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(original) = io.original_native_work() {
        let generation = workspace.head_with_native_work(original, io.cancellation())
            .await.map_err(native_workspace_error)?;
        return generation.read_with_native_work(path, maximum_bytes, original, io.cancellation())
            .await.map_err(native_workspace_error);
    }
    let bytes = io.receive(workspace.read(path, maximum_bytes, io.remaining()?, io.cancellation()).await)?;
    io.retain_owned(bytes.retained_capacity().unwrap_or(0) as u64)?;
    Ok(bytes)
}

pub(in crate::filesystem) async fn resolve<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>, reference: &WorkspaceRef, io: Option<&PreparationIo>,
) -> Result<super::super::WorkspaceObservation> {
    let Some(io) = io else { return host.resolve(reference).await; };
    let workspace = open(host, reference, io).await?;
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(original) = io.original_native_work() {
        let generation = workspace.head_with_native_work(original, io.cancellation())
            .await.map_err(native_workspace_error)?;
        return Ok(super::super::WorkspaceObservation {
            workspace: {
                let mut history = io.history_io();
                let mut observer = Some(&mut history as &mut dyn crate::store::history::HistoryIo);
                let mut quota = crate::wire_codec::CodecQuota::new(&mut observer, 0)?;
                let reference = reference.as_resource();
                let provider = crate::core::copy_provider(reference.provider(), &mut quota)?;
                let key = crate::core::copy_copy_vector(reference.key(), &mut quota)?;
                let version = reference.version().map(|value| crate::core::copy_text(value, &mut quota)).transpose()?;
                let resource = ResourceRef::new_with_work(reference.kind(), provider, key, version, quota.work())?;
                let workspace = WorkspaceRef::from_resource_with_work(resource, quota.work())?;
                quota.transfer();
                workspace
            },
            generation: generation_ref(host, &generation, Some(io))?,
        });
    }
    let generation = io.receive(workspace.head(io.remaining()?, io.cancellation()).await)?;
    let provider = reference.as_resource().provider();
    strings(io, &[provider.namespace(), provider.family(), provider.version()])?;
    io.claim(reference.as_resource().key().len() as u64)?;
    Ok(super::super::WorkspaceObservation {
        workspace: reference.clone(), generation: generation_ref(host, &generation, Some(io))?,
    })
}

pub(in crate::filesystem) async fn create<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>, name: &str, key: &IdempotencyKey, io: Option<&PreparationIo>, guarded: bool,
    preparation: Option<&crate::resource_preparation::TaskResourcePreparation>,
    #[cfg(not(target_arch = "wasm32"))]
    native_creation_guard: Option<&Arc<crate::resource_preparation::TaskResourcePreparation>>,
) -> Result<()> {
    let lifecycle = if host.filesystem.capabilities().durable { acyclic_fs::Lifecycle::Durable } else { acyclic_fs::Lifecycle::Ephemeral };
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(original) = io.and_then(PreparationIo::original_native_work) {
        let guard = if guarded {
            let guard = Arc::clone(native_creation_guard.ok_or(native_error(crate::wire::ErrorCode::Unauthorized,
                "task-bound native creation requires its original issued creation guard"))?);
            guard.verify_creation_with_native_work(original)?.await?;
            Some(guard)
        } else { None };
        let operation = acyclic_fs::OperationId::from_bytes(operation_key(key, io)?.into_bytes());
        if let Some(guard) = guard {
            let mut before = move |work: &acyclic_fs::NativeMaterializationWork| {
                let guard = Arc::clone(&guard);
                let work = work.clone();
                async move { guard.verify_creation_with_native_work(&work)?.await }
            };
            host.filesystem.create_workspace_with_operation_guarded_with_native_work(
                name, acyclic_fs::VolumeConfig::portable(lifecycle), operation,
                original, original.cancellation(), &mut before).await?;
        } else {
            host.filesystem.create_workspace_with_operation_with_native_work(
                name, acyclic_fs::VolumeConfig::portable(lifecycle), operation, original, original.cancellation())
                .await.map_err(native_workspace_error)?;
        }
        return Ok(());
    }
    if guarded && let Some(preparation) = preparation { preparation.verify_creation().await?; }
    match io {
        Some(io) => {
            let operation = acyclic_fs::OperationId::from_bytes(operation_key(key, Some(io))?.into_bytes());
            io.receive(host.filesystem.create_workspace_with_operation(
                name, acyclic_fs::VolumeConfig::portable(lifecycle), operation, io.remaining()?, io.cancellation()).await)?;
        }
        None => { host.filesystem.create_workspace(name, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| map_error(failure.error))?; }
    }
    if guarded && let Some(preparation) = preparation { preparation.verify_creation().await?; }
    Ok(())
}

pub(in crate::filesystem) async fn apply_generation_task_guarded<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>, reference: &WorkspaceRef,
    expected: Option<&crate::resources::GenerationRef>, mutations: &[WorkspaceMutation],
    key: &IdempotencyKey, io: Option<&PreparationIo>,
    preparation: Option<&crate::resource_preparation::TaskResourcePreparation>,
    #[cfg(not(target_arch = "wasm32"))]
    native_creation_guard: Option<&std::sync::Arc<crate::resource_preparation::TaskResourcePreparation>>,
) -> Result<acyclic_fs::Generation<A, O>> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
        let guard = std::sync::Arc::clone(native_creation_guard.ok_or(native_error(crate::wire::ErrorCode::Unauthorized, "task-bound native mutation requires its original issued creation guard"))?);
        let mut before = move |work: &acyclic_fs::NativeMaterializationWork| {
            let guard = Arc::clone(&guard);
            let work = work.clone();
            async move { guard.verify_creation_with_native_work(&work)?.await }
        };
        return apply_generation_guarded(
            host, reference, expected, mutations, key, io, &mut before,
        ).await;
    }
    let mut before = || async move {
        if let Some(preparation) = preparation { preparation.verify_creation().await?; }
        Ok(())
    };
    publish_generation(host, reference, expected, mutations, key, io, &mut before).await
}

pub(in crate::filesystem) async fn apply_generation<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>, reference: &WorkspaceRef,
    expected: Option<&crate::resources::GenerationRef>, mutations: &[WorkspaceMutation],
    key: &IdempotencyKey, io: &PreparationIo,
) -> Result<acyclic_fs::Generation<A, O>> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(original) = io.original_native_work() {
        let workspace = open(host, reference, io).await?;
        let mut before_publication = |_: &acyclic_fs::NativeMaterializationWork| std::future::ready(Ok(()));
        return apply_generation_native(
            host, &workspace, expected, mutations, key, io, original,
            &mut before_publication,
        ).await;
    }
    publish_generation(
        host, reference, expected, mutations, key, Some(io),
        &mut || std::future::ready(Ok(())),
    ).await
}

pub(in crate::filesystem) async fn publish_generation<A, O, F, Fut>(
    host: &FilesystemHost<A, O>, reference: &WorkspaceRef,
    expected: Option<&crate::resources::GenerationRef>, mutations: &[WorkspaceMutation],
    key: &IdempotencyKey, io: Option<&PreparationIo>, before_publication: &mut F,
) -> Result<acyclic_fs::Generation<A, O>>
where A: AsyncAuthorityStore, O: AsyncObjectStore,
    F: FnMut() -> Fut + Send, Fut: Future<Output = Result<()>> + Send,
{
    let Some(io) = io else {
        return host.apply_generation_guarded(reference, expected, mutations, key, before_publication).await;
    };
    let workspace = open(host, reference, io).await?;
    let generation = match expected {
        Some(reference) => generation(host, &workspace, reference, io).await?,
        None => io.receive(workspace.head(io.remaining()?, io.cancellation()).await)?,
    };
    let mut transaction = io.receive(workspace.begin_pinned_transaction_measured(
        &generation, super::super::filesystem_key(key), io.remaining()?, io.cancellation(),
    ).await)?;
    for mutation in mutations {
        match mutation {
            WorkspaceMutation::CreateDirectory { path } => {
                let result = transaction.create_dir_all_measured(path, io.remaining()?, io.cancellation()).await;
                match result {
                    Ok(work) => io.charge(work)?,
                    Err(failure) => { io.spent(*failure.work)?; return Err(Error::from(failure.error)); }
                }
            }
            WorkspaceMutation::PutFile { path, bytes } => {
                let result = transaction.write_measured(
                    path, bytes.clone(), io.remaining()?, io.cancellation(),
                ).await;
                match result {
                    Ok(work) => io.charge(work)?,
                    Err(failure) => { io.spent(*failure.work)?; return Err(Error::from(failure.error)); }
                }
            }
            WorkspaceMutation::Remove { .. } => return Err(Error::Unsupported("bounded publication does not remove content".into())),
        }
    }
    let budget = io.remaining()?;
    let callback_work = Mutex::new(WorkCounters::default());
    let committed = transaction.commit(None, acyclic_fs::PublicationPermit::Unrestricted, budget, io.cancellation(), |allowance| {
            let scope = io.borrow_publication(budget, allowance);
            let future = scope.as_ref().ok().map(|_| before_publication());
            let callback_work = &callback_work;
            async move {
                let scope = scope.map_err(OperationFailure::before_work)?;
                let result = match future {
                    Some(future) => future.await,
                    None => unreachable!("publication scope was admitted before creating the owner future"),
                };
                let observed = io.finish_publication_borrow();
                drop(scope);
                let mut total = callback_work.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                *total = total.checked_add(observed)
                    .map_err(|error| OperationFailure::new(Error::from(WorkspaceError::from(error)), observed))?;
                result.map(|value| OperationReceipt { value, work: observed })
                    .map_err(|error| OperationFailure::new(error, observed))
            }
        }).await;
    let callback_work = callback_work.into_inner().unwrap_or_else(std::sync::PoisonError::into_inner);
    let committed = match committed {
        Ok(mut receipt) => {
            receipt.work = exclude_publication_callback(receipt.work, callback_work)?;
            io.receive_content(Ok(receipt))?
        }
        Err(mut failure) => {
            *failure.work = exclude_publication_callback(*failure.work, callback_work)?;
            io.receive_content(Err(failure))?
        }
    };
    committed_generation(committed, io)
}

fn committed_generation<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    outcome: acyclic_fs::TransactionCommit<A, O>, io: &PreparationIo,
) -> Result<acyclic_fs::Generation<A, O>> {
    match outcome {
        acyclic_fs::TransactionCommit::Committed(generation)
        | acyclic_fs::TransactionCommit::AlreadyCommitted(generation) => Ok(generation),
        acyclic_fs::TransactionCommit::Conflict { .. } => Err(failure(Some(io), crate::wire::ErrorCode::Conflict, "workspace generation changed")),
        acyclic_fs::TransactionCommit::Fenced => Err(failure(Some(io), crate::wire::ErrorCode::Conflict, "workspace writer was fenced")),
        acyclic_fs::TransactionCommit::IdempotencyConflict => Err(failure(Some(io), crate::wire::ErrorCode::Conflict, "filesystem idempotency key belongs to another mutation")),
    }
}

fn exclude_publication_callback(mut work: WorkCounters, callback: WorkCounters) -> Result<WorkCounters> {
    macro_rules! exclude {
        ($($field:ident),*) => {
            $(work.$field = work.$field.checked_sub(callback.$field)
                .ok_or_else(|| Error::from(WorkspaceError::from(acyclic_fs::WorkError::Overflow)))?;)*
        };
    }
    exclude!(
        authority_records_read, authority_records_appended, authority_bytes_read, authority_bytes_written,
        object_probes, backend_read_operations, backend_write_operations, durability_operations,
        page_reads, page_writes, object_bytes_read, object_bytes_written, bytes_hashed, bytes_copied,
        bytes_encoded, source_bytes_read, source_path_components, source_entries_visited, output_bytes,
        items_examined, items_returned, allocation_operations, materializations
    );
    Ok(work)
}

#[cfg(not(target_arch = "wasm32"))]
pub(in crate::filesystem) async fn apply_generation_guarded<'a, A, O, F, Fut>(
    host: &FilesystemHost<A, O>, reference: &WorkspaceRef,
    expected: Option<&crate::resources::GenerationRef>, mutations: &[WorkspaceMutation],
    key: &IdempotencyKey, io: &'a PreparationIo, before_publication: &mut F,
) -> Result<acyclic_fs::Generation<A, O>>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    F: for<'m> FnMut(&'m acyclic_fs::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
    Fut: std::future::Future<Output = Result<()>> + Send + 'static,
{
    let original = io.original_native_work().ok_or(native_error(crate::wire::ErrorCode::Invalid, "guarded native publication requires its original accounting scope"))?;
    before_publication(original).await?;
    let workspace = open(host, reference, io).await?;
    apply_generation_native(
        host, &workspace, expected, mutations, key, io, original,
        before_publication,
    ).await
}

#[cfg(not(target_arch = "wasm32"))]
async fn apply_generation_native<'a, A, O, F, Fut>(
    host: &FilesystemHost<A, O>, workspace: &acyclic_fs::Workspace<A, O>,
    expected: Option<&crate::resources::GenerationRef>, mutations: &[WorkspaceMutation],
    key: &IdempotencyKey, io: &PreparationIo,
    original: &'a acyclic_fs::NativeMaterializationWork, before_publication: &mut F,
) -> Result<acyclic_fs::Generation<A, O>>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    F: for<'m> FnMut(&'m acyclic_fs::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
    Fut: std::future::Future<Output = Result<()>> + Send + 'static,
{
        let pinned = match expected {
            Some(reference) => generation(host, &workspace, reference, io).await?,
            None => workspace.head_with_native_work(original, io.cancellation())
                .await.map_err(native_workspace_error)?,
        };
        let operation = operation_key(key, Some(io))?;
        let mut transaction = workspace.begin_pinned_transaction_with_native_work(
            &pinned, operation, original, io.cancellation(),
        ).await.map_err(native_workspace_error)?;
        for mutation in mutations {
            before_publication(original).await?;
            match mutation {
                WorkspaceMutation::CreateDirectory { path } => {
                    transaction.create_directories_with_native_work(
                        path, original, io.cancellation(),
                    ).await.map_err(native_workspace_error)?;
                }
                WorkspaceMutation::PutFile { path, bytes } => {
                    transaction.write_with_native_work(
                        path, bytes.clone(), original, io.cancellation(),
                    ).await.map_err(native_workspace_error)?;
                }
                WorkspaceMutation::Remove { .. } => {
                    return Err(native_error(crate::wire::ErrorCode::Unsupported, "bounded publication does not remove content"));
                }
            }
        }
        committed_generation(transaction.commit_with_native_work(
            None, acyclic_fs::PublicationPermit::Unrestricted, original, io.cancellation(),
            before_publication,
        ).await?, io)

}

pub(in crate::filesystem) fn generation_ref<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>, generation: &acyclic_fs::Generation<A, O>,
    io: Option<&PreparationIo>,
) -> Result<crate::resources::GenerationRef> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(io) = io && let Some(original) = io.original_native_work() {
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

/// Pins the acknowledged immutable generation with its original typed cause.
pub(in crate::filesystem) async fn retain<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    generation: &acyclic_fs::Generation<A, O>, io: Option<&PreparationIo>,
) -> std::result::Result<(), acyclic_fs::LazyWorkspaceError> {
    use acyclic_fs::LazyWorkspaceError;
    let encoding = WorkCounters {
        bytes_encoded: 64, bytes_copied: 83, ..WorkCounters::default()
    };
    if let Some(io) = io {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = io.original_native_work() {
            original.admit(encoding)?;
        } else {
            io.preflight_workspace(encoding).map_err(LazyWorkspaceError::WorkspaceCause)?;
        }
        #[cfg(target_arch = "wasm32")]
        io.preflight_workspace(encoding).map_err(LazyWorkspaceError::WorkspaceCause)?;
    }
    let encoded = blake3::Hash::from_bytes(generation.id().digest().into_bytes()).to_hex();
    let mut label = [0_u8; 83];
    label[..19].copy_from_slice(b"harness-generation-");
    label[19..].copy_from_slice(encoded.as_bytes());
    let identity = std::str::from_utf8(&label).expect("fixed ASCII generation identity");
    let Some(io) = io else {
        return generation.pin(identity).await.map(|_| ())
            .map_err(LazyWorkspaceError::WorkspaceCause);
    };
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(original) = io.original_native_work() {
        original.spent(encoding)?;
        return generation.pin_with_native_work(identity, original, io.cancellation())
            .await.map(|_| ()).map_err(|(source, custody)| LazyWorkspaceError::NativeWorkspaceCause(
                acyclic_fs::lazy_workspace::NativeLazyWorkspaceCause { source, custody }));
    }
    io.spent_workspace(encoding).map_err(LazyWorkspaceError::WorkspaceCause)?;
    let result = generation.pin_measured(
        identity, io.remaining_workspace().map_err(LazyWorkspaceError::WorkspaceCause)?,
        io.cancellation(),
    ).await;
    match result {
        Ok(receipt) => {
            io.spent_workspace(receipt.work).map_err(LazyWorkspaceError::WorkspaceCause)?;
            io.preflight_workspace(WorkCounters::default())
                .map_err(LazyWorkspaceError::WorkspaceCause)?;
            drop(receipt.value);
            Ok(())
        }
        Err(failure) => {
            let source = failure.error;
            match io.spent_workspace(*failure.work) {
                Ok(()) => Err(LazyWorkspaceError::WorkspaceCause(source)),
                Err(accounting) => Err(LazyWorkspaceError::WorkspacePrefixFailure { source, accounting }),
            }
        }
    }
}

pub(in crate::filesystem) async fn fork<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>, source: &VolumeRef, grant: &ContentGrant,
    source_generation: &crate::resources::GenerationRef, child: &VolumeRef,
    paths: Option<Vec<String>>, key: &IdempotencyKey, io: Option<&PreparationIo>,
    preparation: Option<&crate::resource_preparation::TaskResourcePreparation>,
    #[cfg(not(target_arch = "wasm32"))]
    native_creation_guard: Option<&std::sync::Arc<crate::resource_preparation::TaskResourcePreparation>>,
) -> Result<super::super::WorkspaceObservation> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(original) = io.and_then(PreparationIo::original_native_work) {
        native_creation_guard.ok_or(native_error(crate::wire::ErrorCode::Unauthorized,
            "task-bound native fork requires its original issued creation guard"))?
            .verify_creation_with_native_work(original)?.await?;
    } else if let Some(preparation) = preparation { preparation.verify_creation().await?; }
    #[cfg(target_arch = "wasm32")]
    if let Some(preparation) = preparation { preparation.verify_creation().await?; }
    grant.require(source, VolumeOperation::Read, io)?;
    let observed = if let Some(io) = io {
        let mut history = io.history_io();
        source.validate(&mut Some(&mut history))?;
        child.validate(&mut Some(&mut history))?;
        if source.provider() != &host.provider || child.provider() != &host.provider || source == child {
            return Err(failure(Some(io), crate::wire::ErrorCode::Invalid, "volume fork has a foreign or identical destination"));
        }
        let source_ref = content_workspace(&host.provider, source, Some(io))?;
        let workspace = open(host, &source_ref, io).await?;
        let pinned = generation(host, &workspace, source_generation, io).await?;
        retain(&pinned, Some(io)).await?;
        let child_name = volume_storage_name(child, Some(io))?;
        let operation = operation_key(key, Some(io))?;
        let options = acyclic_fs::ForkOptions::from_generation(pinned, operation);
        let options = match paths { Some(paths) => options.inherit_paths(paths), None => options };
        #[cfg(not(target_arch = "wasm32"))]
        let forked = if let Some(original) = io.original_native_work() {
            let guard = Arc::clone(native_creation_guard.expect("native creation guard was verified"));
            let mut before = move |work: &acyclic_fs::NativeMaterializationWork| {
                let guard = Arc::clone(&guard);
                let work = work.clone();
                async move { guard.verify_creation_with_native_work(&work)?.await }
            };
            workspace.fork_with_permit_guarded_with_native_work(&child_name, options,
                acyclic_fs::PublicationPermit::Unrestricted, original, io.cancellation(), &mut before).await?
        } else {
            io.receive(workspace.fork(&child_name, options, acyclic_fs::PublicationPermit::Unrestricted, io.remaining()?, io.cancellation(), |_| std::future::ready(Ok(acyclic_fs::OperationReceipt { value: (), work: acyclic_fs::WorkCounters::default() }))).await)?
        };
        #[cfg(target_arch = "wasm32")]
        let forked = io.receive(workspace.fork(&child_name, options, acyclic_fs::PublicationPermit::Unrestricted, io.remaining()?, io.cancellation(), |_| std::future::ready(Ok(acyclic_fs::OperationReceipt { value: (), work: acyclic_fs::WorkCounters::default() }))).await)?;
        let initial = filesystem_call!(io, original,
            forked.operation_generation_with_native_work(acyclic_fs::OperationId::from_bytes(operation.into_bytes()), original, io.cancellation()),
            forked.operation_generation(operation, io.remaining()?, io.cancellation()))?;
        let initial = initial.ok_or_else(|| failure(Some(io), crate::wire::ErrorCode::Storage,
            "fork creation has no durable original generation"))?;
        retain(&initial, Some(io)).await?;
        super::super::WorkspaceObservation {
            workspace: WorkspaceRef::new(host.provider.clone_with_io(io)?, child_name.into_bytes(), None)?,
            generation: generation_ref(host, &initial, Some(io))?,
        }
    } else {
        host.fork_volume_pinned(source, source_generation, child, paths, key).await?
    };
    if !io.is_some_and(PreparationIo::uses_native_work)
        && let Some(preparation) = preparation { preparation.verify_creation().await?; }
    Ok(observed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conversation::{VolumeClass, VolumeOwner};
    use crate::core::{AggregateKind, AuthorityIssuer};
    use acyclic_fs::Fs;

    #[tokio::test]
    async fn finite_ordinary_owner_callback_retains_full_failure_and_real_sdk_prefix() -> Result<()> {
        let provider = crate::resources::ProviderRef::new("ordinary-owner-control", "filesystem", "2")?;
        let host = FilesystemHost::new(Fs::memory(), provider.clone())?;
        let volume = VolumeRef::new(provider, "owned", VolumeClass::Project,
            VolumeOwner::Project("ordinary-owner-control".into()))?;
        let initial = host.create_volume(&volume).await?;
        let mutations = [WorkspaceMutation::PutFile { path: "/file".into(), bytes: b"owned".to_vec() }];
        let cancellation = CancellationToken::new();
        let denied_io = PreparationIo::from_budget(budget(100_000, 16_000_000), &cancellation)?;
        let denied = publish_generation(
            &host, &initial.workspace, Some(&initial.generation), &mutations,
            &IdempotencyKey::new("expired-owner-publication")?, Some(&denied_io),
            &mut || async {
                let _checked = open(&host, &initial.workspace, &denied_io).await?;
                Err(Error::Indeterminate(crate::OperationId::from_bytes([113; 16])))
            },
        ).await;
        assert!(matches!(denied, Err(Error::Indeterminate(id)) if id == crate::OperationId::from_bytes([113; 16])));
        assert_eq!(host.resolve(&initial.workspace).await?.generation, initial.generation);
        assert!(denied_io.receipt().backend_read_operations > 0);
        assert!(denied_io.receipt().bytes_hashed > 0);
        let accepted_io = PreparationIo::from_budget(budget(100_000, 16_000_000), &cancellation)?;
        let accepted = publish_generation(
            &host, &initial.workspace, Some(&initial.generation), &mutations,
            &IdempotencyKey::new("live-owner-publication")?, Some(&accepted_io),
            &mut || async {
                let _checked = open(&host, &initial.workspace, &accepted_io).await?;
                Ok(())
            },
        ).await?;
        assert_eq!(accepted.read("/file", 32).await.map_err(map_error)?.as_ref(), b"owned");
        Ok(())
    }

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
            None, &IdempotencyKey::new("denied-source-read")?, None, None,
            #[cfg(not(target_arch = "wasm32"))]
            None).await, Err(Error::Unauthorized(_))));
        let denied_ref = workspace_ref(provider.clone(), &denied_child.storage_name()?)?;
        assert!(host.resolve(&denied_ref).await.is_err());
        assert!(fork(&host, &source, &selected_read, &wrong_generation, &denied_child,
            None, &IdempotencyKey::new("wrong-generation")?, None, None,
            #[cfg(not(target_arch = "wasm32"))]
            None).await.is_err());
        assert!(host.resolve(&denied_ref).await.is_err());
        let later_generation = host.apply(&initial.workspace, Some(&selected_generation),
            &[WorkspaceMutation::PutFile { path: "/value.txt".into(), bytes: b"later".to_vec() }],
            &IdempotencyKey::new("later-generation")?).await?;
        let forked = fork(&host, &source, &selected_read, &selected_generation, &child,
            None, &IdempotencyKey::new("pinned-selected-fork")?, None, None,
            #[cfg(not(target_arch = "wasm32"))]
            None).await?;
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
impl PreparationIo {
    pub(crate) fn history_io(&self) -> &Self { self }

    fn charged(&self, delta: WorkCounters) -> Result<()> {
        self.preflight(delta)?;
        self.charge(delta)
    }

    fn reserve_history<T>(&self, values: &mut Vec<T>, maximum: usize) -> Result<()> {
        if values.len() < values.capacity() { return Ok(()); }
        let target = values.capacity().checked_mul(2).unwrap_or(maximum).max(4).min(maximum);
        if target <= values.capacity() { return Err(self.invalid("history vector exceeds declared maximum")); }
        reserve(values, target, Some(self))
    }

}

impl crate::store::history::HistoryIo for &PreparationIo {
    #[cfg(not(target_arch = "wasm32"))]
    fn original_native_work(&self) -> Option<&acyclic_fs::NativeMaterializationWork> {
        PreparationIo::original_native_work(self)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn absorb_native_custody(&mut self, incoming: acyclic_fs::NativeAllocation) -> Result<()> {
        PreparationIo::absorb_native_custody(self, incoming)
    }
    fn check_cancellation(&self) -> Result<()> {
        PreparationIo::check_cancellation(self)
    }
    fn begin_call(&mut self) -> Result<()> {
        self.charged(WorkCounters { backend_read_operations: 1, ..Default::default() })
    }
    fn begin_write(&mut self) -> Result<()> {
        self.charged(WorkCounters { backend_write_operations: 1, ..Default::default() })
    }
    fn receive_record(&mut self, bytes: u64) -> Result<()> {
        self.spent(WorkCounters { authority_records_read: 1,
            authority_bytes_read: bytes, items_examined: 1,
            peak_allocation_bytes: bytes, ..Default::default() })?;
        self.remaining()?;
        self.retain_owned(bytes)
    }
    fn release_record(&mut self, bytes: u64) -> Result<()> { self.release(bytes) }
    fn retain_owned(&mut self, bytes: u64) -> Result<()> { PreparationIo::retain_owned(self, bytes) }
    fn admit_owned(&self, bytes: u64) -> Result<()> {
        self.check_cancellation()?;
        if bytes > self.remaining()?.peak_allocation_bytes {
            return Err(self.invalid("history exceeds live logical byte allowance"));
        }
        Ok(())
    }
    fn release_owned(&mut self, bytes: u64) -> Result<()> { self.release(bytes) }
    fn decode_json_value(&mut self, bytes: &[u8]) -> Result<(serde_json::Value, u64)> {
        let decoded = self.decode_preparation::<serde_json::Value>(bytes, crate::wire::ErrorCode::Storage)?;
        let held = decoded._reservation.bytes;
        Ok((decoded.into_retained(), held))
    }
    fn clone_authority(&mut self, authority: &crate::core::Authority) -> Result<crate::core::Authority> {
        Ok(crate::core::Authority { kind: authority.kind, id: self.clone_text(&authority.id)? })
    }
    fn clone_text(&mut self, text: &str) -> Result<String> { join_text("", text, self) }
    fn reserve_events(&mut self, values: &mut Vec<crate::core::Event>, maximum: usize) -> Result<()> { self.reserve_history(values, maximum) }
    fn reserve_records(&mut self, values: &mut Vec<crate::store::operations::ObservedRecord>, maximum: usize) -> Result<()> { self.reserve_history(values, maximum) }
    fn reserve_messages(&mut self, values: &mut Vec<crate::conversation::ConversationMessage>, maximum: usize) -> Result<()> { self.reserve_history(values, maximum) }
    fn reserve_values(&mut self, values: &mut Vec<serde_json::Value>, maximum: usize) -> Result<()> { self.reserve_history(values, maximum) }
    fn reserve_bytes(&mut self, values: &mut Vec<u8>, capacity: usize) -> Result<()> {
        if capacity <= values.capacity() { return Ok(()); }
        let old = values.capacity();
        self.claim((capacity - old) as u64)?;
        self.allocation_attempt((capacity - old) as u64)?;
        values.try_reserve_exact(capacity - values.len()).map_err(Error::Allocation)?;
        if values.capacity() > capacity { self.claim((values.capacity() - capacity) as u64)?; }
        Ok(())
    }
    fn reserve_text(&mut self, text: &mut String, capacity: usize) -> Result<()> {
        if capacity <= text.capacity() { return Ok(()); }
        let old = text.capacity();
        self.claim((capacity - old) as u64)?;
        self.allocation_attempt((capacity - old) as u64)?;
        text.try_reserve_exact(capacity - text.len()).map_err(Error::Allocation)?;
        if text.capacity() > capacity { self.claim((text.capacity() - capacity) as u64)?; }
        Ok(())
    }
    fn admit_allocation(&mut self, bytes: u64) -> Result<()> {
        self.admit_owned(bytes)?;
        if bytes != 0 { self.allocation_attempt(bytes)?; }
        Ok(())
    }
    fn admit_copy(&self, bytes: u64) -> Result<()> {
        self.preflight(WorkCounters { bytes_copied: bytes, ..Default::default() })
    }
    fn copy_bytes(&mut self, bytes: u64) -> Result<()> {
        self.charged(WorkCounters { bytes_copied: bytes, ..Default::default() })
    }
    fn encoded_bytes(&mut self, bytes: u64) -> Result<()> {
        self.charge(WorkCounters { bytes_encoded: bytes, ..Default::default() })
    }
    fn admit_encoding(&self, bytes: u64) -> Result<()> {
        self.preflight(WorkCounters { bytes_encoded: bytes, ..Default::default() })
    }
    fn codec_charge(&mut self, charge: crate::contract::JsonCodecCharge) -> Result<()> {
        match charge {
            crate::contract::JsonCodecCharge::Admission { maximum_owned_bytes, value_shape } =>
                self.claim(maximum_owned_bytes.max(
                    crate::contract::json_value_decode_owned_bytes(value_shape)?)),
            _ => PreparationIo::codec_charge(self, charge, true),
        }
    }
    fn hashed_bytes(&mut self, bytes: u64) -> Result<()> {
        self.charged(WorkCounters { bytes_hashed: bytes, ..Default::default() })
    }
    fn admit_acknowledgment(&self, bytes: u64, count: u64) -> Result<()> {
        self.preflight(WorkCounters { authority_bytes_written: bytes, authority_records_appended: count, ..Default::default() })
    }
    fn acknowledged(&mut self, bytes: u64, count: u64) -> Result<()> {
        self.charge(WorkCounters { authority_bytes_written: bytes, authority_records_appended: count, ..Default::default() })
    }
    fn admit_output(&self, bytes: u64, count: u64) -> Result<()> {
        self.preflight(WorkCounters { output_bytes: bytes, items_returned: count, ..Default::default() })
    }
    fn stage_output(&mut self, bytes: u64, count: u64) -> Result<()> { self.admit_output(bytes, count) }
    fn return_output(&mut self, bytes: u64, count: u64) -> Result<()> {
        self.charged(WorkCounters { output_bytes: bytes, items_returned: count, ..Default::default() })
    }
    fn remaining(&self) -> Result<WorkBudget> { PreparationIo::remaining(self) }
    fn cancellation(&self) -> &CancellationToken { PreparationIo::cancellation(self) }
    fn receive_work(&mut self, mut work: WorkCounters) -> Result<()> {
        work.output_bytes = 0;
        work.items_returned = 0;
        self.spent(work)
    }
    fn verify_content<'a>(&'a mut self, verifier: &'a dyn crate::conversation::ContentResidencyVerifier,
        reference: &'a FileRef) -> Result<crate::store::history::HistoryContentFuture<'a, ()>> {
        let io = *self;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.original_native_work() {
            return crate::NativeFutureBundle::new(work, || async move {
                verifier.verify(reference, reference.descriptor().byte_length(), Some(io))?.await
            }).map(crate::store::history::HistoryContentFuture::Native);
        }
        io.history_future(|| async move {
            verifier.verify(reference, reference.descriptor().byte_length(), Some(io))?.await
        })
    }
    fn read_content<'a>(&'a mut self, verifier: &'a dyn crate::conversation::ContentResidencyVerifier,
        reference: &'a FileRef) -> Result<crate::store::history::HistoryContentFuture<'a, (acyclic_stream::OwnedBytes, u64)>> {
        let io = *self;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.original_native_work() {
            return crate::NativeFutureBundle::new(work, || async move {
                let bytes = verifier.read(reference, reference.descriptor().byte_length(), Some(io))?.await?;
                Ok((bytes, 0))
            }).map(crate::store::history::HistoryContentFuture::Native);
        }
        io.history_future(|| async move {
            let bytes = verifier.read(reference, reference.descriptor().byte_length(), Some(io))?.await?;
            let retained = bytes.retained_capacity().unwrap_or(0) as u64;
            io.retain_owned(retained)?;
            Ok((bytes, retained))
        })
    }
}
