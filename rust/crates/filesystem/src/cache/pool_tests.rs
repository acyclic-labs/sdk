use super::*;

fn store(entries: u32) -> CachedObjectStore<()> {
    CachedObjectStore::new(
        (),
        ObjectCacheOptions {
            maximum_entries: entries,
            maximum_bytes: 16,
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
    let first = store(4);
    let mut second = first.share_cache_with(()).unwrap();
    if alias {
        Arc::get_mut(&mut second.shared).unwrap().id = first.shared.id;
    }
    first
        .insert(
            object(1),
            OwnedBytes::from_static(b"one"),
            first.shared.fill_epoch(),
        )
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
        .insert(
            object(2),
            OwnedBytes::from_static(b"two"),
            second.shared.fill_epoch(),
        )
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
    let first = store(1);
    let second = first.share_cache_with(()).unwrap();
    first
        .insert(
            object(1),
            OwnedBytes::from_static(b"one"),
            first.shared.fill_epoch(),
        )
        .unwrap();
    let ReadPlan::Hit(held) = first
        .read_plan(
            object(1),
            &mut crate::kernel::KernelAccounting::standalone(
                crate::performance::BOUNDARY_BUDGET,
                &CancellationToken::new(),
            ),
        )
        .unwrap()
    else {
        panic!("missing resident object")
    };
    second
        .insert(
            object(2),
            OwnedBytes::from_static(b"two"),
            second.shared.fill_epoch(),
        )
        .unwrap();
    assert!(!first.cached_contains(object(1)).unwrap());
    assert_eq!(held.as_ref(), b"one");
    assert_eq!(second.stats().unwrap().evictions, 1);
    first.clear().unwrap();
    assert!(second.cached_contains(object(2)).unwrap());
}

fn plan(cache: &CachedObjectStore<()>) -> Result<ReadPlan<'_, ()>, ObjectStoreError> {
    cache.read_plan(
        object(1),
        &mut crate::kernel::KernelAccounting::standalone(
            crate::performance::BOUNDARY_BUDGET,
            &CancellationToken::new(),
        ),
    )
}

#[test]
fn fills_coalesce_only_within_their_namespace() {
    let first = store(2);
    let second = first.share_cache_with(()).unwrap();
    let ReadPlan::Leader(flight) = plan(&first).unwrap() else {
        panic!("no leader")
    };
    let clone = first.clone();
    let ReadPlan::Follower(follower) = plan(&clone).unwrap() else {
        panic!("no follower")
    };
    assert!(flights::same_flight(
        flight.flight.as_ref().unwrap(),
        &follower
    ));
    let ReadPlan::Leader(second_flight) = plan(&second).unwrap() else {
        panic!("namespace did not receive its own flight")
    };
    first.clear().unwrap();
    assert_eq!(second.stats().unwrap().in_flight, 2);
    flight.finish(None).unwrap();
    assert_eq!(second.stats().unwrap().in_flight, 1);
    second_flight.finish(None).unwrap();
    assert_eq!(first.stats().unwrap().in_flight, 0);
}

enum Retirement {
    Clear,
    Eviction,
    Locked,
}

fn drops_after_unlock(retirement: Retirement) -> bool {
    use std::sync::atomic::{AtomicBool, Ordering};

    struct ObserveDrop {
        pool: std::sync::Weak<Shared>,
        unlocked: Arc<AtomicBool>,
    }
    impl Drop for ObserveDrop {
        fn drop(&mut self) {
            self.unlocked.store(
                self.pool
                    .upgrade()
                    .is_some_and(|pool| pool.state.try_lock().is_ok()),
                Ordering::SeqCst,
            );
        }
    }

    let cache = store(1);
    let unlocked = Arc::new(AtomicBool::new(false));
    let value = DecodedCacheValue {
        value: Arc::new(ObserveDrop {
            pool: Arc::downgrade(&cache.shared.pool),
            unlocked: unlocked.clone(),
        }),
        ..decoded()
    };
    drop(
        cache
            .decoded_admit(
                DecodedCacheKey::new::<ObserveDrop>(object(1), Default::default()),
                value,
            )
            .unwrap(),
    );
    match retirement {
        Retirement::Locked => {
            // Restore the old destruction boundary: the observer must detect it.
            let mut state = cache.lock_state().unwrap();
            drop(cache.shared.clear(&mut state).unwrap());
        }
        Retirement::Clear => cache.clear().unwrap(),
        Retirement::Eviction => {
            cache
                .insert(
                    object(2),
                    OwnedBytes::from_static(b"two"),
                    cache.shared.fill_epoch(),
                )
                .unwrap();
        }
    }
    unlocked.load(Ordering::SeqCst)
}

fn reentrant_eviction_preserves_new_admission() {
    struct Reenter {
        namespace: std::sync::Weak<Namespace>,
        target: ObjectId,
    }
    impl Drop for Reenter {
        fn drop(&mut self) {
            let store = CachedObjectStore {
                inner: (),
                shared: self.namespace.upgrade().unwrap(),
            };
            assert!(
                store
                    .insert(
                        self.target,
                        OwnedBytes::from_static(b"new"),
                        store.shared.fill_epoch()
                    )
                    .unwrap()
            );
        }
    }

    for same_key in [false, true] {
        let cache = store(1);
        let requested = object(3);
        let reentered = if same_key { requested } else { object(2) };
        let value = DecodedCacheValue {
            value: Arc::new(Reenter {
                namespace: Arc::downgrade(&cache.shared),
                target: reentered,
            }),
            ..decoded()
        };
        drop(
            cache
                .decoded_admit(
                    DecodedCacheKey::new::<Reenter>(object(1), Default::default()),
                    value,
                )
                .unwrap(),
        );
        assert_eq!(
            cache
                .insert(
                    requested,
                    OwnedBytes::from_static(b"new"),
                    cache.shared.fill_epoch()
                )
                .unwrap(),
            same_key
        );
        assert!(cache.cached_contains(reentered).unwrap());
        assert_eq!(cache.stats().unwrap().resident_entries, 1);
        assert_eq!(cache.stats().unwrap().resident_bytes, 3);
        assert_eq!(cache.stats().unwrap().evictions, 1);
    }
}

#[test]
fn namespace_retirement_preserves_peers_leases_and_unlocked_destruction() {
    assert!(drops_after_unlock(Retirement::Clear));
    assert!(drops_after_unlock(Retirement::Eviction));
    reentrant_eviction_preserves_new_admission();
    assert!(
        !drops_after_unlock(Retirement::Locked),
        "drop observer missed the held lock"
    );
    let first = store(4);
    let second = first.share_cache_with(()).unwrap();
    first
        .insert(
            object(1),
            OwnedBytes::from_static(b"one"),
            first.shared.fill_epoch(),
        )
        .unwrap();
    second
        .insert(
            object(2),
            OwnedBytes::from_static(b"two"),
            second.shared.fill_epoch(),
        )
        .unwrap();
    let key = DecodedCacheKey::new::<u64>(object(1), Default::default());
    drop(first.decoded_admit(key, decoded()).unwrap());
    let clone = first.clone();
    drop(first);
    assert_eq!(second.stats().unwrap().resident_entries, 3);
    let held = clone.decoded_get(key).unwrap().unwrap();
    drop(clone);
    assert_eq!(second.stats().unwrap().resident_entries, 1);
    assert_eq!(second.stats().unwrap().resident_bytes, 3);
    assert!(second.cached_contains(object(2)).unwrap());
    assert_eq!(*held.value.downcast::<u64>().unwrap(), 7);
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn cancelled_flight_settlement_releases_followers_and_original_backing() {
    use crate::kernel::KernelAccounting;

    for native in [false, true] {
        let cache = store(2);
        let cancel = CancellationToken::new();
        let budget = crate::performance::BOUNDARY_BUDGET;
        let original = crate::NativeMaterializationWork::new(budget, &cancel).unwrap();
        let mut accounting = if native {
            KernelAccounting::native(&original, &cancel)
        } else {
            KernelAccounting::standalone(budget, &cancel)
        };
        let baseline = if native {
            original.live_allocation_bytes()
        } else {
            // The first retained flight promotes this caller's ledger. That
            // original control remains funded while the caller owns it.
            crate::arc_layout::allocation_bytes(std::alloc::Layout::new::<
                std::sync::Mutex<crate::kernel::allocation::SharedAllocationBytes>,
            >())
            .unwrap() as u64
        };
        let mut follower = None;
        // Inject cancellation after the actual shared installer and before the
        // actual original-ledger settlement. No backend or wait is replaced.
        let result = flights::insertion_work(&mut accounting, |insertion, accounting| {
            let leader =
                cache.install_read_plan(cache.shared.key(object(1)), insertion, accounting)?;
            let ReadPlan::Follower(joined) = plan(&cache)? else {
                panic!("installed producer was not shared")
            };
            follower = Some(joined);
            cancel.cancel();
            Ok(leader)
        });
        if native {
            assert!(matches!(
                result,
                Err(ObjectStoreError::Native(
                    crate::NativeTreeMaterializationError::Cancelled
                ))
            ));
        } else {
            assert!(matches!(result, Err(ObjectStoreError::Cancelled)));
        }
        assert_eq!(cache.stats().unwrap().in_flight, 0);
        let follower = follower.unwrap();
        {
            let mut read = pin!(follower.read());
            assert!(matches!(
                read.as_mut().poll(&mut Context::from_waker(Waker::noop())),
                Poll::Ready(Err(ObjectStoreError::SharedReadUnavailable))
            ));
        }
        drop(follower);
        if !native {
            let ledger = accounting.standalone_parts().unwrap().0;
            let crate::kernel::allocation::LedgerState::Shared(control) = &ledger.state else {
                panic!("original flight did not retain its caller's ledger")
            };
            assert_eq!(
                Arc::strong_count(control),
                1,
                "flight funding still retained"
            );
        }
        assert_eq!(
            if native {
                original.live_allocation_bytes()
            } else {
                accounting.standalone_parts().unwrap().0.live_bytes()
            },
            baseline
        );
        // A fresh original caller obtains a producer, never an abandoned wait.
        assert!(matches!(plan(&cache).unwrap(), ReadPlan::Leader(_)));
        assert_eq!(cache.stats().unwrap().in_flight, 0);
    }
}
