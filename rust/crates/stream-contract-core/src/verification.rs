//! The per-model-thread fault a Loom negative control injects. Production
//! paths in this crate and in `acyclic-stream` read the one registry, so a
//! control reaches a path wherever it lives.

use std::cell::Cell;

/// One injected production defect.
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Fault {
    None,
    MixedReceiptCut,
    BarrierAsFlushed,
    OmitAppendWake,
    RetainReplacedState,
    IgnoreHead,
    IgnoreAbsent,
    RetainConflictKey,
    RootOnlyFork,
    IgnoreForkExclusions,
    IgnorePageRecordBytes,
    OmitPoisonPublication,
    HoldPublicationDuringMaintenance,
    HoldStateDuringDurability,
    ReleasePendingPublication,
}

loom::thread_local! {
    static FAULT: Cell<Fault> = Cell::new(Fault::None);
}

/// Injects `fault` into this model thread.
pub fn set_fault(fault: Fault) {
    FAULT.with(|current| current.set(fault));
}

/// Whether this model thread injects `fault`.
pub fn fault_is(fault: Fault) -> bool {
    FAULT.with(|current| current.get() == fault)
}
