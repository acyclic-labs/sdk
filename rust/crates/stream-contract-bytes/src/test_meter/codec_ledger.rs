//! Fixtures may move the ledger, but only the meter can borrow its lock.
use acyclic_native_runtime::{
    InlineMutex as Mutex, InlineMutexGuard as MutexGuard, NativeCodecWork,
};
use std::sync::LockResult;

#[derive(Default)]
pub struct CodecLedger(Mutex<NativeCodecWork>);

impl CodecLedger {
    pub fn lock(&self) -> LockResult<MutexGuard<'_, NativeCodecWork>> {
        self.0.lock()
    }
}
