//! Fixtures may move the ledger, but only the meter can borrow its lock.
use acyclic_native_runtime::NativeCodecWork;
use std::sync::{LockResult, Mutex, MutexGuard};

#[derive(Default)]
pub(crate) struct CodecLedger(Mutex<NativeCodecWork>);

impl CodecLedger {
    pub(super) fn lock(&self) -> LockResult<MutexGuard<'_, NativeCodecWork>> {
        self.0.lock()
    }
}
