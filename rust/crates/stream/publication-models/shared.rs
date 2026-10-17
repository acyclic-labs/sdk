use crate::{StreamError, query_sync::SyncMutex};
use loom::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

fn initialization(broken: bool) {
    loom::model(move || {
        let slot = Arc::new(SyncMutex::new(None));
        let initialized = Arc::new(AtomicUsize::new(0));
        let start = |slot: Arc<SyncMutex<Option<usize>>>, initialized: Arc<AtomicUsize>| {
            loom::thread::spawn(move || {
                if broken {
                    if let Some(value) = *slot.lock().unwrap() {
                        return value;
                    }
                    let value = initialized.fetch_add(1, Ordering::SeqCst);
                    *slot.lock().unwrap() = Some(value);
                    value
                } else {
                    shared(&slot, || Ok(initialized.fetch_add(1, Ordering::SeqCst))).unwrap()
                }
            })
        };
        let one = start(Arc::clone(&slot), Arc::clone(&initialized));
        let two = start(Arc::clone(&slot), Arc::clone(&initialized));
        assert_eq!(
            one.join().unwrap(),
            two.join().unwrap(),
            "all follows must share one session client"
        );
        assert_eq!(
            initialized.load(Ordering::SeqCst),
            1,
            "all follows must share one session client"
        );
    });
}
#[test]
fn first_follows_initialize_one_shared_client() {
    initialization(false);
}
#[test]
#[should_panic(expected = "all follows must share one session client")]
fn initialization_negative_control_splits_the_cache_cut() {
    initialization(true);
}

#[test]
fn initialization_does_not_hide_failures_or_replace_the_shared_client() {
    loom::model(|| {
        let slot = SyncMutex::new(None);
        assert!(shared::<usize>(&slot, || Err(StreamError::Unavailable)).is_err());
        assert_eq!(shared(&slot, || Ok(7)).unwrap(), 7);
        assert_eq!(shared(&slot, || panic!("initialized twice")).unwrap(), 7);
    });
}
