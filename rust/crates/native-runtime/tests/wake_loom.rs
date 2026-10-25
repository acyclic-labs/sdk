//! `cargo rustc -p acyclic-native-runtime --test wake_loom -- --cfg acyclic_native_runtime_loom`
#![cfg(acyclic_native_runtime_loom)]
#[path = "../src/wake.rs"]
mod wake;
use loom::sync::Arc;
use wake::NativeWake;

#[test]
fn native_wake_preserves_completion_at_every_wait_boundary() {
    loom::model(|| {
        let wake = Arc::new(NativeWake::default());
        let producer = Arc::clone(&wake);
        let thread = loom::thread::spawn(move || producer.notify());
        wake.wait(None);
        assert!(thread.join().is_ok());
    });
}

#[test]
fn native_wake_allows_inline_completion_under_the_owner_lock() {
    loom::model(|| {
        let owner = loom::sync::Mutex::new(());
        let wake = NativeWake::default();
        let state = owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        wake.notify();
        drop(state);
        wake.wait(None);
    });
}
