use super::*;

fn store(entries: u32, fills: u32) -> CachedObjectStore<()> {
    CachedObjectStore::new(
        (),
        ObjectCacheOptions {
            maximum_entries: entries,
            maximum_bytes: 16,
            maximum_in_flight: fills,
            maximum_waiters_per_object: 2,
        },
    )
    .unwrap()
}

fn object(id: u8) -> ObjectId {
    ObjectId {
        storage: None,
        kind: crate::ObjectKind::FileTablePage,
        digest: crate::Digest::from_bytes([id; 32]),
    }
}

fn decoded() -> DecodedCacheValue {
    DecodedCacheValue {
        value: Arc::new(7_u64),
        logical_bytes: 8,
        #[cfg(not(target_arch = "wasm32"))]
        native_custody: None,
    }
}

fn isolated(alias: bool) {
    let first = store(4, 2);
    let mut second = first.share_cache_with(()).unwrap();
    if alias {
        second.shared.id = first.shared.id;
    }
    first
        .insert(object(1), OwnedBytes::from_static(b"one"))
        .unwrap();
    let key = DecodedCacheKey::new::<u64>(object(1), Default::default());
    first.decoded_admit(key, decoded()).unwrap();
    assert!(first.clone().cached_contains(object(1)).unwrap());
    assert!(
        !second.cached_contains(object(1)).unwrap(),
        "namespace alias"
    );
    assert!(second.decoded_get(key).unwrap().is_none(), "decoded alias");
    second
        .insert(object(2), OwnedBytes::from_static(b"two"))
        .unwrap();
    first.clear().unwrap();
    assert!(!first.cached_contains(object(1)).unwrap());
    assert!(first.decoded_get(key).unwrap().is_none());
    assert!(second.cached_contains(object(2)).unwrap());
    let stats = first.stats().unwrap();
    assert_eq!(stats.resident_entries, 1);
    assert_eq!(stats.resident_bytes, 3);
    assert_eq!(stats.resident_decoded_bytes, 0);
}

#[test]
fn namespaces_isolate_bytes_decoders_and_clear() {
    isolated(false);
}

#[test]
fn namespace_alias_negative_control() {
    let failure = std::panic::catch_unwind(|| isolated(true)).unwrap_err();
    let message = failure
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| failure.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(message.contains("namespace alias"));
}

#[test]
fn pooled_eviction_preserves_an_active_payload() {
    let first = store(1, 1);
    let second = first.share_cache_with(()).unwrap();
    first
        .insert(object(1), OwnedBytes::from_static(b"one"))
        .unwrap();
    let ReadPlan::Hit(held) = first
        .read_plan(
            object(1),
            #[cfg(not(target_arch = "wasm32"))]
            None,
        )
        .unwrap()
    else {
        panic!("missing resident object")
    };
    second
        .insert(object(2), OwnedBytes::from_static(b"two"))
        .unwrap();
    assert!(!first.cached_contains(object(1)).unwrap());
    assert_eq!(held.as_ref(), b"one");
    assert_eq!(second.stats().unwrap().evictions, 1);
    first.clear().unwrap();
    assert!(second.cached_contains(object(2)).unwrap());
}

fn plan(cache: &CachedObjectStore<()>) -> Result<ReadPlan, ObjectStoreError> {
    cache.read_plan(
        object(1),
        #[cfg(not(target_arch = "wasm32"))]
        None,
    )
}

#[test]
fn fills_share_capacity_and_only_coalesce_within_their_namespace() {
    let first = store(2, 1);
    let second = first.share_cache_with(()).unwrap();
    let ReadPlan::Leader(flight) = plan(&first).unwrap() else {
        panic!("no leader")
    };
    let ReadPlan::Follower(follower) = plan(&first.clone()).unwrap() else {
        panic!("no follower")
    };
    assert!(Arc::ptr_eq(&flight, &follower));
    assert!(matches!(plan(&second), Err(ObjectStoreError::Rejected(_))));
    first.clear().unwrap();
    assert_eq!(second.stats().unwrap().in_flight, 1);
    ActiveFlightGuard {
        store: &first,
        object_id: object(1),
        flight,
    }
    .finish(None)
    .unwrap();
    let ReadPlan::Leader(flight) = plan(&second).unwrap() else {
        panic!("pool slot not released")
    };
    ActiveFlightGuard {
        store: &second,
        object_id: object(1),
        flight,
    }
    .finish(None)
    .unwrap();
    assert_eq!(first.stats().unwrap().in_flight, 0);
}
