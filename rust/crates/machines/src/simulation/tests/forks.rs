use super::*;

#[tokio::test]
#[allow(
    clippy::indexing_slicing,
    reason = "indices are preceded by an assert_eq!(children.len(), 2), proving them in-bounds"
)]
async fn checkpoint_fork_lifetimes_and_endpoints_are_independent() {
    let provider = SimulatedMachines::default();
    let create_key = IdempotencyKey::parse("00000000-0000-0000-0000-000000000008")
        .unwrap_or_else(|_| unreachable!());
    let Ok(MutationOutcome::Created(machine)) = provider.create(request(create_key)).await else {
        unreachable!();
    };
    let checkpoint_key = IdempotencyKey::parse("00000000-0000-0000-0000-000000000009")
        .unwrap_or_else(|_| unreachable!());
    let Ok(MutationOutcome::Checkpointed(checkpoint)) =
        provider.checkpoint(machine.id, checkpoint_key).await
    else {
        unreachable!();
    };
    let fork_key = IdempotencyKey::parse("00000000-0000-0000-0000-00000000000a")
        .unwrap_or_else(|_| unreachable!());
    let Ok(MutationOutcome::Forked(children)) = provider
        .fork(
            checkpoint.id,
            NonZeroU32::new(2).unwrap_or(NonZeroU32::MIN),
            fork_key,
        )
        .await
    else {
        unreachable!();
    };
    assert_eq!(children.len(), 2);
    assert_ne!(children[0].id, children[1].id);
    assert_ne!(children[0].id, machine.id);
    provider
        .destroy_checkpoint(
            checkpoint.id,
            IdempotencyKey::parse("00000000-0000-0000-0000-00000000000b")
                .unwrap_or_else(|_| unreachable!()),
        )
        .await
        .unwrap_or_else(|_| unreachable!());
    assert!(provider.inspect_machine(machine.id).await.is_ok());
    assert!(provider.inspect_machine(children[0].id).await.is_ok());
}

#[tokio::test]
async fn live_fork_children_inherit_the_contract_and_replay_exactly() {
    let provider = Arc::new(SimulatedMachines::default());
    let Ok(MutationOutcome::Created(parent)) = provider.create(request(key(1))).await else {
        unreachable!();
    };
    let parent_view = provider
        .inspect_machine(parent.id)
        .await
        .unwrap_or_else(|_| unreachable!());
    let first = provider.fork_machine(parent.id, two(), key(2)).await;
    let Ok(MutationOutcome::MachineForked {
        source,
        fidelity,
        children,
    }) = first.clone()
    else {
        unreachable!("live fork must succeed: {first:?}")
    };
    assert_eq!(source, parent.id);
    assert_eq!(fidelity, ForkFidelity::MemoryAndDisk);
    assert_eq!(children.len(), 2);
    let ids = children
        .iter()
        .map(|child| child.id)
        .collect::<BTreeSet<_>>();
    assert_eq!(ids.len(), 2);
    assert!(!ids.contains(&parent.id));
    for child in &children {
        assert_eq!(child.state, MachineState::Running);
        assert_eq!(child.contract, parent_view.contract);
        assert_eq!(child.last_checkpoint, None);
        assert_ne!(child.endpoints, parent_view.endpoints);
    }
    assert_eq!(provider.fork_machine(parent.id, two(), key(2)).await, first);
    assert!(matches!(
        provider
            .fork_machine(parent.id, NonZeroU32::MIN, key(2))
            .await,
        Err(ProviderError::Conflict(_))
    ));
    assert_eq!(
        provider
            .inspect_machine(parent.id)
            .await
            .map(|value| value.state),
        Ok(MachineState::Running)
    );

    let child = children.first().map(|value| value.id).unwrap_or_default();
    provider
        .inspect_machine(child)
        .await
        .unwrap_or_else(|_| unreachable!());
    let Ok(MutationOutcome::MachineForked {
        fidelity,
        children: grandchildren,
        ..
    }) = provider.fork_machine(child, NonZeroU32::MIN, key(3)).await
    else {
        unreachable!();
    };
    assert_eq!(fidelity, ForkFidelity::MemoryAndDisk);
    assert_eq!(grandchildren.len(), 1);
}

#[tokio::test]
async fn live_fork_sources_are_joined_after_their_children() {
    let provider = SimulatedMachines::default();
    let Ok(MutationOutcome::Created(parent)) = provider.create(request(key(0x10))).await else {
        unreachable!()
    };
    let Ok(MutationOutcome::MachineForked { children, .. }) =
        provider.fork_machine(parent.id, two(), key(0x11)).await
    else {
        unreachable!()
    };
    assert!(matches!(
        provider.destroy_machine(parent.id, key(0x12)).await,
        Err(ProviderError::Conflict(_))
    ));
    for (index, child) in (0x13_u8..).zip(&children) {
        assert_eq!(
            provider.destroy_machine(child.id, key(index)).await,
            Ok(MutationOutcome::MachineDestroyed(child.id))
        );
    }
    assert_eq!(
        provider.destroy_machine(parent.id, key(0x12)).await,
        Ok(MutationOutcome::MachineDestroyed(parent.id))
    );
}

#[tokio::test]
async fn live_fork_requires_a_running_source() {
    let provider = SimulatedMachines::default();
    let Ok(MutationOutcome::Created(parent)) = provider.create(request(key(0x20))).await else {
        unreachable!()
    };
    provider
        .suspend(parent.id, key(0x21))
        .await
        .unwrap_or_else(|_| unreachable!());
    assert!(matches!(
        provider.fork_machine(parent.id, two(), key(0x22)).await,
        Err(ProviderError::Conflict(_))
    ));
    assert!(matches!(
        provider
            .fork_machine(MachineId::new(), two(), key(0x23))
            .await,
        Err(ProviderError::NotFound(_))
    ));
    let too_many = NonZeroU32::new(MAX_FORK_CHILDREN + 1).unwrap_or(NonZeroU32::MAX);
    assert!(matches!(
        provider.fork_machine(parent.id, too_many, key(0x24)).await,
        Err(ProviderError::Invalid(_))
    ));
}

#[tokio::test]
async fn live_fork_fidelity_follows_the_declared_capability() {
    let disk_only = SimulatedMachines::with_capabilities(BTreeSet::from([Capability::DiskFork]));
    let Ok(MutationOutcome::Created(parent)) = disk_only.create(request(key(0x30))).await else {
        unreachable!()
    };
    assert_eq!(
        parent.contract.fork_fidelity(),
        Some(ForkFidelity::DiskOnly)
    );
    assert!(matches!(
        disk_only.fork_machine(parent.id, two(), key(0x31)).await,
        Ok(MutationOutcome::MachineForked {
            fidelity: ForkFidelity::DiskOnly,
            ..
        })
    ));

    let none = SimulatedMachines::with_capabilities(BTreeSet::new());
    let Ok(MutationOutcome::Created(parent)) = none.create(request(key(0x32))).await else {
        unreachable!()
    };
    assert_eq!(parent.contract.fork_fidelity(), None);
    assert!(matches!(
        none.fork_machine(parent.id, two(), key(0x33)).await,
        Err(ProviderError::Unsupported(_))
    ));
    assert!(none.recover(key(0x33)).await.is_err());

    let mut require = request(key(0x34));
    require.compatibility = CompatibilityPolicy::Require(BTreeSet::from([Capability::LiveFork]));
    assert!(matches!(
        none.create(require).await,
        Err(ProviderError::Unsupported(_))
    ));
}

#[test]
fn live_fork_takes_precedence_over_disk_fork() {
    assert_eq!(
        ForkFidelity::for_capabilities(&BTreeSet::from([
            Capability::DiskFork,
            Capability::LiveFork
        ])),
        Some(ForkFidelity::MemoryAndDisk)
    );
}
