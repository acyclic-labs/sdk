use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // A lease is stable only for its exact task/operation/reservation; field
    // boundaries cannot alias even when concatenated text is identical.
    #[test]
    fn publication_lease_binds_the_complete_task_fence(
        task in any::<[u8; 16]>(), operation in any::<[u8; 16]>(), text in "[a-z]{0,32}",
    ) {
        let task = TaskId::from_bytes(task);
        let operation = OperationId::from_bytes(operation);
        let fence = crate::scheduler::LeaseFence { placement: text.clone(), reservation_id: "a".into() };
        let original = lease_identity(task, operation, &fence);
        prop_assert_eq!(original, lease_identity(task, operation, &fence));
        let mut changed = fence.clone();
        changed.reservation_id.push('b');
        prop_assert_ne!(original, lease_identity(task, operation, &changed));
        let moved_boundary = crate::scheduler::LeaseFence { placement: format!("{text}a"), reservation_id: "b".into() };
        prop_assert_ne!(lease_identity(task, operation, &changed), lease_identity(task, operation, &moved_boundary));
        let mut bytes = operation.into_bytes();
        bytes[0] ^= 1;
        prop_assert_ne!(original, lease_identity(task, OperationId::from_bytes(bytes), &fence));
        let mut bytes = task.into_bytes();
        bytes[0] ^= 1;
        prop_assert_ne!(original, lease_identity(TaskId::from_bytes(bytes), operation, &fence));
    }
}

struct Clock;
impl acyclic_stream::UnixMillisClock for Clock {
    fn now_unix_millis(&self) -> u64 {
        0
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // Window creation and authority publication independently encode the same
    // exact lease gate for every identity and every nonzero decimal expiry.
    #[test]
    fn lease_gate_encoders_agree_for_all_identities_and_expiries(
        workspace in any::<[u8; 16]>(), lease in any::<[u8; 16]>(), expiry in 1u64..=u64::MAX,
    ) {
        use acyclic_fs::{AsyncAuthorityStore, AuthorityWork, CancellationToken, WorkBudget};
        futures::executor::block_on(async {
            let provider = Arc::new(acyclic_stream::MemoryStream::new_with_clock(
                Default::default(), Arc::new(Clock),
            ));
            let authority = acyclic_fs::StreamAuthorityStore::new(provider.clone());
            let windows = acyclic_fs::OperationWindowCoordinator::new(
                acyclic_fs::StreamOperationWindowStore::new(provider),
            );
            let workspace = acyclic_fs::WorkspaceId::from_bytes(workspace);
            let authority_id = acyclic_fs::kernel::volume_authority_id(workspace.volume_id());
            let cancellation = CancellationToken::new();
            let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
            let acyclic_fs::CreateAuthorityOutcome::Created(head) = authority.create_authority::<()>(authority_id, acyclic_fs::Epoch::GENESIS, &mut work, None).await.unwrap() else {
                unreachable!("fresh authority");
            };
            // A zero lease is deliberately rejected by the window's schema.
            let mut lease = lease;
            lease[0] |= 1;
            let lease = windows.begin_with_lease_id(
                workspace, acyclic_fs::GenerationId::new(acyclic_fs::Digest::from_bytes([1; 32])),
                "task:reservation", 0, expiry, acyclic_fs::OperationLeaseId::from_bytes(lease),
            ).await.unwrap();
            let result = authority.compare_and_append(acyclic_fs::GuardedAppend {
                authority_id, epoch: head.epoch, expected: head,
                commit: acyclic_fs::ProposedCommit {
                    operation_id: acyclic_fs::OperationId::from_bytes([2; 16]),
                    fingerprint: acyclic_fs::Digest::from_bytes([3; 32]),
                    payload: bytes::Bytes::from_static(b"lease-codec-property"),
                },
                permit: lease.publication_permit(),
            }, &mut work, None::<&mut ()>).await.unwrap();
            prop_assert!(matches!(result, acyclic_fs::AppendOutcome::Committed(_)));
            Ok(())
        })?;
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // Reconciliation retries bind the command, phase and both generation cuts.
    #[test]
    fn reconciliation_identity_binds_every_input(
        operation in any::<[u8; 16]>(), from in any::<[u8; 32]>(), to in any::<[u8; 32]>(),
    ) {
        let generation = |bytes| acyclic_fs::GenerationId::new(acyclic_fs::Digest::from_bytes(bytes));
        let operation = OperationId::from_bytes(operation);
        let original = reconciliation_operation(operation, b"close", generation(from), generation(to));
        prop_assert_eq!(original, reconciliation_operation(operation, b"close", generation(from), generation(to)));
        prop_assert_ne!(original, reconciliation_operation(operation, b"recover", generation(from), generation(to)));
        let mut changed = operation.into_bytes(); changed[0] ^= 1;
        prop_assert_ne!(original, reconciliation_operation(OperationId::from_bytes(changed), b"close", generation(from), generation(to)));
        let mut changed = from; changed[0] ^= 1;
        prop_assert_ne!(original, reconciliation_operation(operation, b"close", generation(changed), generation(to)));
        let mut changed = to; changed[0] ^= 1;
        prop_assert_ne!(original, reconciliation_operation(operation, b"close", generation(from), generation(changed)));
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // The pending native acknowledgement survives exact Git-state round trips.
    #[test]
    fn reconciliation_marker_round_trip(
        workspace in any::<[u8; 16]>(), generation in any::<[u8; 32]>(),
        operation in any::<[u8; 16]>(), present in any::<bool>(),
    ) {
        let workspace = acyclic_fs::WorkspaceId::from_bytes(workspace);
        let mut state = acyclic_fs::GitCompatState::new("main", workspace);
        state.checkout_reconciliation = present.then_some((
            acyclic_fs::OperationId::from_bytes(operation), acyclic_fs::GitGenerationRef {
                workspace_id: workspace,
                generation: acyclic_fs::GenerationId::new(acyclic_fs::Digest::from_bytes(generation)),
            },
        ));
        let bytes = serde_json::to_vec(&state).unwrap();
        let recovered: acyclic_fs::GitCompatState = serde_json::from_slice(&bytes).unwrap();
        prop_assert_eq!(&recovered, &state);
        prop_assert_eq!(serde_json::to_vec(&recovered).unwrap(), bytes);
    }
}

#[test]
fn reconciliation_fuzz_seed_is_a_recoverable_git_state() {
    let seed =
        include_bytes!("../../../../harness-fuzz/corpus/records/checkout-reconciliation.json");
    let state: acyclic_fs::GitCompatState = serde_json::from_slice(seed).unwrap();
    assert_eq!(
        state.checkout_reconciliation.unwrap().1.workspace_id,
        state.branches["main"].workspace_id
    );
    let encoded = serde_json::to_vec(&state).unwrap();
    assert_eq!(
        serde_json::from_slice::<acyclic_fs::GitCompatState>(&encoded).unwrap(),
        state
    );
}
