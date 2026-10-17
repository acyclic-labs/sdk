//! Compile with cfg only on this test target; model the production reader
//! without enabling unrelated Harness models in its library dependencies.
#![cfg(acyclic_harness_loom)]

mod sync {
    include!("../src/sync/arc.rs");
    include!("../src/sync/mutex.rs");
    include!("../src/sync/reader.rs");
    pub(crate) use Condvar as SyncCondvar;
    pub(crate) use Mutex as SyncMutex;
    pub(crate) use MutexGuard as SyncGuard;
}

#[path = "../src/client/native_input/reader.rs"]
mod reader;
