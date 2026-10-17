//! Two actors use the actual decoded-cache mutex; no backend I/O is modeled.
use super::*;

const _: fn(Mutex<()>) -> loom::sync::Mutex<()> = |lock| lock;
fn key(id: u8) -> DecodedCacheKey {
    DecodedCacheKey::new::<u64>(
        ObjectId {
            kind: crate::ObjectKind::FileTablePage,
            digest: crate::Digest::from_bytes([id; 32]),
        },
        Default::default(),
    )
}
fn value(id: u8) -> DecodedCacheValue {
    DecodedCacheValue {
        value: Arc::new(u64::from(id)),
        logical_bytes: 8,
        #[cfg(not(target_arch = "wasm32"))]
        native_custody: None,
    }
}
// Equivalent to the real admission at the model's one-entry/eight-byte bound,
// except the broken variant releases the guard between checking and inserting.
fn publish(cache: &CachedObjectStore<()>, id: u8, split: bool) {
    if split {
        let mut state = cache.lock_state().unwrap();
        if !state.decoded.is_empty() {
            evict_oldest(&mut state).unwrap();
        }
        drop(state);
        loom::thread::yield_now();
        cache
            .decoded_admit_bounded::<false>(key(id), value(id))
            .unwrap();
    } else {
        cache.decoded_admit(key(id), value(id)).unwrap();
    }
}
fn scenario(alias: bool, split: bool) {
    let cache = Arc::new(
        CachedObjectStore::new(
            (),
            ObjectCacheOptions {
                maximum_entries: 1,
                maximum_bytes: 8,
                maximum_in_flight: 1,
                maximum_waiters_per_object: 1,
            },
        )
        .unwrap(),
    );
    let writer = cache.clone();
    let peer = loom::thread::spawn(move || publish(&writer, 0, split));
    let observed = if alias {
        cache.decoded_get_matching::<false>(key(1))
    } else {
        cache.decoded_get(key(1))
    }
    .unwrap();
    let correct = observed.is_none_or(|v| *v.value.downcast::<u64>().unwrap() == 1);
    publish(&cache, 1, split);
    // Join before terminal observations so assertion-deletion leaves no live actor.
    peer.join().unwrap();
    let stats = cache.stats().unwrap();
    assert!(correct, "decoded cache crossed immutable identity");
    assert!(
        stats.resident_entries <= 1 && stats.resident_bytes <= 8,
        "decoded cache exceeded its admitted bound"
    );
}
#[test]
fn decoded_read_and_replacement_are_atomic() {
    loom::model(|| scenario(false, false));
}
fn detected(alias: bool, split: bool, target: &str) {
    let error = std::panic::catch_unwind(|| loom::model(move || scenario(alias, split)))
        .expect_err("cache mutation survived");
    let text = error
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| error.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(text.contains(target), "{text}");
}
#[test]
fn decoded_identity_negative_control() {
    detected(true, false, "decoded cache crossed immutable identity");
}
#[test]
fn decoded_bound_negative_control() {
    detected(false, true, "decoded cache exceeded its admitted bound");
}
