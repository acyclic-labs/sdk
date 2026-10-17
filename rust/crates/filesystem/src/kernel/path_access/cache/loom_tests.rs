use super::*;
use std::sync::Arc;

fn id(value: u8) -> ObjectId {
    let mut bytes = [0; 32];
    bytes[0] = value;
    ObjectId {
        kind: crate::ObjectKind::Blob,
        digest: crate::Digest::from_bytes(bytes),
    }
}

fn grow_while_reading<const REPROBE: bool>() {
    crate::initialize_test_native_io();
    loom::model(|| {
        let cancellation = CancellationToken::new();
        let mut setup = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
        let cache = Arc::new(OperationReadCache::new(&(), 16, &mut setup, None).unwrap());
        let keys: Vec<_> = (0..=255)
            .map(id)
            .filter(|key| object_hash(*key) & 15 == 0)
            .take(8)
            .collect();
        assert_eq!(keys.len(), 8);
        for &key in &keys {
            assert!(
                cache
                    .admit(key, &OwnedBytes::from_static(b"old"), 0, &mut setup)
                    .unwrap()
            );
        }
        let fresh = (0..=255)
            .map(id)
            .find(|key| object_hash(*key) & 16 != 0 && !keys.contains(key))
            .unwrap();
        let writer_cache = cache.clone();
        let writer_cancel = cancellation.clone();
        let writer = loom::thread::spawn(move || {
            let mut work = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &writer_cancel);
            assert!(
                writer_cache
                    .admit_with_slot::<REPROBE>(
                        fresh,
                        &OwnedBytes::from_static(b"new"),
                        0,
                        &mut work
                    )
                    .unwrap()
            );
        });
        let reader_cache = cache.clone();
        let reader_cancel = cancellation.clone();
        let old = keys[0];
        let reader = loom::thread::spawn(move || {
            let mut work = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &reader_cancel);
            assert_eq!(
                reader_cache.probe(old, &mut work).unwrap().unwrap().bytes,
                OwnedBytes::from_static(b"old")
            );
        });
        writer.join().unwrap();
        reader.join().unwrap();
        for key in keys.into_iter().chain([fresh]) {
            assert!(
                cache.probe(key, &mut setup).unwrap().is_some(),
                "admitted key disappeared"
            );
        }
    });
}

#[test]
fn real_cache_growth_and_read_keep_every_admitted_key() {
    grow_while_reading::<true>();
}

#[test]
fn negative_control_rejects_reusing_a_slot_across_growth() {
    let failure = std::panic::catch_unwind(grow_while_reading::<false>)
        .expect_err("stale slot must be detected");
    let message = failure
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| failure.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(message.contains("admitted key disappeared"), "{message}");
}
