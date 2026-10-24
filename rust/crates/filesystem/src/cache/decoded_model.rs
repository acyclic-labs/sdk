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

fn relink_scenario(omit_touch: bool) {
    let cache = Arc::new(
        CachedObjectStore::new(
            (),
            ObjectCacheOptions {
                maximum_entries: 3,
                maximum_bytes: 32,
                maximum_in_flight: 1,
                maximum_waiters_per_object: 1,
            },
        )
        .unwrap(),
    );
    cache.decoded_admit(key(0), value(0)).unwrap();
    cache.decoded_admit(key(1), value(1)).unwrap();
    let canonical = key(2).object_id;
    cache
        .insert(canonical, OwnedBytes::from_static(b"c"))
        .unwrap();
    let writer = cache.clone();
    let peer = loom::thread::spawn(move || {
        let read = if omit_touch {
            writer.decoded_get_mode::<true, false>(key(0))
        } else {
            writer.decoded_get(key(0))
        }
        .unwrap();
        writer.decoded_admit(key(2), value(2)).unwrap();
        read.is_some_and(|value| *value.value.downcast_ref::<u64>().unwrap() == 0)
    });
    cache.cached_contains(canonical).unwrap();
    let read = cache.decoded_get(key(1)).unwrap();
    let correct = read.is_none_or(|value| *value.value.downcast_ref::<u64>().unwrap() == 1);
    let peer_correct = peer.join().unwrap();
    let actual = cache.lock_state().unwrap().recency_order();
    let first = RecencyKey::Decoded(key(0));
    let second = RecencyKey::Decoded(key(1));
    let third = RecencyKey::Canonical(canonical);
    let new = RecencyKey::Decoded(key(2));
    let legal = [
        [0, 1, 2, 3],
        [0, 2, 1, 3],
        [0, 2, 3, 1],
        [2, 0, 1, 3],
        [2, 0, 3, 1],
        [2, 3, 0, 1],
    ]
    .map(|schedule| {
        let mut order = vec![first, second, third];
        for event in schedule {
            let target = match event {
                0 => first,
                1 => new,
                2 => third,
                _ => second,
            };
            if let Some(index) = order.iter().position(|key| *key == target) {
                order.remove(index);
                order.push(target);
            } else if event == 1 {
                order.remove(0);
                order.push(target);
            }
        }
        order
    });
    assert!(correct && peer_correct, "recency read crossed identity");
    assert!(
        legal.contains(&actual),
        "recency is not a serial LRU history"
    );
}

#[test]
fn mixed_hits_and_eviction_preserve_serial_recency() {
    loom::model(|| relink_scenario(false));
}

#[test]
#[should_panic(expected = "recency is not a serial LRU history")]
fn omitted_touch_fails_the_same_serial_recency_model() {
    loom::model(|| relink_scenario(true));
}
