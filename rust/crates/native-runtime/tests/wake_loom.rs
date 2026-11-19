//! `ACYCLIC_LOOM=1 cargo test -p acyclic-native-runtime --test main --features model-checking wake_loom::`
#![cfg(loom)]
use crate::wake::NativeWake;
use loom::sync::Arc;

#[test]
fn native_wake_preserves_completion_at_every_wait_boundary() {
    loom::model(|| {
        let wake = Arc::new(NativeWake::default());
        wake.notify();
        wake.wait(None);
        let published = Arc::new(loom::sync::atomic::AtomicBool::new(false));
        let publication = Arc::clone(&published);
        let producer = Arc::clone(&wake);
        let thread = loom::thread::spawn(move || {
            publication.store(true, loom::sync::atomic::Ordering::Release);
            producer.notify();
        });
        wake.wait(None);
        assert!(
            published.load(loom::sync::atomic::Ordering::Acquire),
            "wake returned without its producer"
        );
        assert!(thread.join().is_ok());
    });
}

#[test]
fn native_wake_allows_inline_completion_under_the_owner_lock() {
    loom::model(|| {
        let owner = loom::sync::Mutex::new(());
        let wake = std::sync::Arc::new(NativeWake::default());
        let state = owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        std::task::Waker::from(std::sync::Arc::clone(&wake)).wake_by_ref();
        drop(state);
        wake.wait(None);
    });
}
