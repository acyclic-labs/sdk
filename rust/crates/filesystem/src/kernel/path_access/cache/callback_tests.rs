use super::*;
#[cfg(loom)]
use loom::thread;
use std::sync::Arc;
#[cfg(not(loom))]
use std::thread;

fn id(key: u8) -> ObjectId {
    ObjectId {
        storage: None,
        kind: crate::ObjectKind::Blob,
        digest: crate::Digest::from_bytes([key; 32]),
    }
}

fn callbacks() {
    let cancellation = CancellationToken::new();
    let mut setup = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let cache = Arc::new(OperationReadCache::new(&(), 2, &mut setup, None).unwrap());
    let threads: Vec<_> = (0..2)
        .map(|key| {
            let cache = cache.clone();
            let cancellation = cancellation.clone();
            thread::spawn(move || {
                // Callbacks may hold the cursor, the highest outer native lock.
                let callback =
                    crate::lock_order::Mutex::<_, { crate::lock_order::CURSOR }>::new(());
                let _callback = callback.lock().unwrap();
                let mut accounting =
                    KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
                let bytes = OwnedBytes::from_static(b"entry");
                assert!(cache.admit(id(key), &bytes, 0, &mut accounting).unwrap());
                assert_eq!(
                    cache
                        .probe(id(key), &mut accounting)
                        .unwrap()
                        .unwrap()
                        .bytes,
                    bytes
                );
                assert_eq!(cache.lock_state().retained_owned_bytes, 0);
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    for key in 0..2 {
        assert_eq!(
            cache.probe(id(key), &mut setup).unwrap().unwrap().bytes,
            OwnedBytes::from_static(b"entry")
        );
    }
}

#[test]
fn source_callbacks_share_the_real_cache_without_lock_inversion() {
    crate::initialize_test_native_io();
    #[cfg(loom)]
    loom::model(callbacks);
    #[cfg(not(loom))]
    callbacks();
}
