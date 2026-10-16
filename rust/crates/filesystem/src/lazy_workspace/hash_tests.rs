use super::*;
use proptest::prelude::*;

fn measured<T: MemoryLazyNode>(
    value: &T,
    context: u8,
    limit: u64,
    recheck: bool,
    cancel: u8,
    legacy: bool,
) -> (Result<[u8; 32], String>, WorkCounters, u64, String) {
    let cancellation = CancellationToken::new();
    let explicit_cancellation = CancellationToken::new();
    let budget = WorkBudget {
        bytes_hashed: limit,
        ..crate::performance::BOUNDARY_BUDGET
    };
    let original = crate::NativeMaterializationWork::new(budget, &cancellation)
        .map_err(|(error, _)| error)
        .unwrap();
    let work = match context {
        0 => original.clone(),
        1 => original.native_volume_work(budget).unwrap(),
        _ => original.invocation_view().unwrap().0,
    };
    let work = if recheck {
        work.recheck_admitted()
    } else {
        work
    };
    if cancel == 1 {
        cancellation.cancel();
    }
    if cancel == 2 {
        explicit_cancellation.cancel();
    }
    if cancel == 3 {
        work.cancellation().cancel();
    }
    let mut accounting = KernelAccounting::native(&work, &explicit_cancellation);
    let result = if legacy {
        hash_with_charge(value, Charge::Accounting(&mut accounting))
    } else {
        hash_lazy_value(value, &mut accounting)
    };
    {
        let state = original.state.lock();
        assert!(state.native_resource.is_none());
        assert!(state.native_invocation.is_none());
        assert!(state.capture_custody.is_none());
        assert!(!state.recheck_items);
    }
    (
        result.map_err(|error| format!("{error:?}")),
        original.receipt(),
        original.live_allocation_bytes(),
        format!("{:?}", work.remaining()),
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]
    #[test]
    fn node_hash_preserves_each_charge_and_error(
        path in proptest::collection::vec(any::<char>(), 0..128),
        left in any::<[u8; 32]>(),
        right in any::<[u8; 32]>(),
        removed in proptest::option::of(any::<[u8; 32]>()),
        priority in any::<u64>(),
        file_id in any::<[u8; 16]>(),
        record in proptest::collection::vec(any::<u8>(), 0..128),
        context in 0_u8..3,
        limit in prop_oneof![0_u64..1024, Just(u64::MAX)],
        recheck in any::<bool>(),
        cancel in 0_u8..4,
    ) {
        let node = LazyOverlay::Node {
            path: path.into_iter().collect(),
            priority,
            change: LazyOverlayChange::Tombstone { removed: removed.map(SourceVersion) },
            left: LazyOverlayId::from_bytes(left),
            right: LazyOverlayId::from_bytes(right),
        };
        let actual = measured(&node, context, limit, recheck, cancel, false);
        prop_assert_eq!(&actual, &measured(&node, context, limit, recheck, cancel, true));
        if let Ok(digest) = &actual.0 {
            let expected = *blake3::hash(&serde_json::to_vec(&node).unwrap()).as_bytes();
            prop_assert_eq!(*digest, expected);
        }
        let node = LazyShadow::Node {
            file_id: FileId::from_bytes(file_id),
            priority,
            record,
            metadata: crate::WorkspaceMetadata {
                posix_mode: Some(priority as u32),
                posix_uid: None,
                posix_gid: None,
                posix_flags: Some(priority),
                windows_attributes: None,
                created_ns: Some(priority as i64),
                modified_ns: None,
                accessed_ns: None,
                changed_ns: None,
                has_named_attributes: false,
                has_acl: true,
                has_security_descriptor: false,
            },
            left: LazyShadowId::from_bytes(left),
            right: LazyShadowId::from_bytes(right),
        };
        let actual = measured(&node, context, limit, recheck, cancel, false);
        prop_assert_eq!(&actual, &measured(&node, context, limit, recheck, cancel, true));
        if let Ok(digest) = &actual.0 {
            let expected = *blake3::hash(&serde_json::to_vec(&node).unwrap()).as_bytes();
            prop_assert_eq!(*digest, expected);
        }
    }
}
