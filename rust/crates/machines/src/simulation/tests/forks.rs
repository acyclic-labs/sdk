use super::*;

#[tokio::test]
async fn settled_fork_pages_keep_original_custody_and_credit_after_recovery()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::results::{ResultCursor, Settlement};
    let provider = SimulatedMachines::default();
    let MutationOutcome::Created(parent) = provider.create(request(key(0x40))).await? else {
        unreachable!("original create");
    };
    let count = NonZeroU32::new(1_025).unwrap_or(NonZeroU32::MIN);
    let outcome = provider.fork_machine(parent.id, count, key(0x41)).await?;
    let MutationOutcome::Forked { results, .. } = outcome else {
        unreachable!("original fork");
    };
    assert_eq!(results.count, u64::from(count.get()));
    let operation = provider.recover_operation(key(0x41)).await?;
    assert_eq!(results.operation, operation);
    let original = {
        let state = provider.state.lock().await;
        state
            .replays
            .get(&operation)
            .ok_or("original replay missing")?
            .results
            .as_ptr() as usize
    };
    let cursor = ResultCursor {
        cut: results,
        from: 0,
    };
    let credit = NonZeroU32::new(3).unwrap_or(NonZeroU32::MIN);
    let first = provider.read_results(cursor, credit).await?;
    assert_eq!(first.machines.len(), 3);
    assert_eq!(first.machines.capacity(), 3);
    let child = first
        .machines
        .first()
        .ok_or("original first child missing")?;
    provider.destroy_machine(child.id, key(0x42)).await?;
    assert_eq!(
        provider.inspect_machine(child.id).await?.state,
        MachineState::Destroyed
    );
    assert_eq!(provider.recover(key(0x41)).await?, outcome);
    assert_eq!(
        provider.fork_machine(parent.id, count, key(0x41)).await?,
        outcome
    );
    assert_eq!(provider.read_results(cursor, credit).await?, first);
    assert_eq!(child.state, MachineState::Running);
    assert_eq!(
        provider.destroy_machine(parent.id, key(0x43)).await,
        Err(ProviderError::FailedPrecondition(Precondition::LiveForks))
    );
    let mut next = first.next;
    let mut visited = first.machines.len();
    while let Some(cursor) = next {
        let page = provider.read_results(cursor, credit).await?;
        assert!(page.machines.len() <= 3);
        assert!(page.machines.capacity() <= 3);
        visited += page.machines.len();
        next = page.next;
    }
    assert_eq!(visited, 1_025);
    let end = provider
        .read_results(
            ResultCursor {
                from: results.count,
                ..cursor
            },
            credit,
        )
        .await?;
    assert!(end.machines.is_empty());
    assert!(end.next.is_none());
    let Settlement::NativeController(revision) = results.settlement else {
        unreachable!("native owner")
    };
    for foreign in [
        ResultCursor {
            from: results.count + 1,
            ..cursor
        },
        ResultCursor {
            cut: crate::results::ResultCut {
                count: results.count + 1,
                ..results
            },
            ..cursor
        },
        ResultCursor {
            cut: crate::results::ResultCut {
                settlement: Settlement::NativeController(
                    revision.checked_add(1).ok_or("revision overflow")?,
                ),
                ..results
            },
            ..cursor
        },
        ResultCursor {
            cut: crate::results::ResultCut {
                operation: provider.recover_operation(key(0x40)).await?,
                ..results
            },
            ..cursor
        },
    ] {
        assert!(provider.read_results(foreign, credit).await.is_err());
    }
    {
        let mut state = provider.state.lock().await;
        let replay = state
            .replays
            .get_mut(&operation)
            .ok_or("original replay lost")?;
        assert_eq!(replay.results.as_ptr() as usize, original);
        assert_eq!(replay.results.len(), 1_025);
        // Losing original backing cannot turn a retained cut into a shorter success.
        replay.results.pop();
    }
    assert!(provider.read_results(cursor, credit).await.is_err());
    assert_eq!(provider.recover(key(0x41)).await?, outcome);
    Ok(())
}

#[tokio::test]
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
    let Ok(MutationOutcome::Forked {
        provenance,
        results: children,
    }) = provider
        .fork(
            checkpoint.id,
            NonZeroU32::new(1_025).unwrap_or(NonZeroU32::MIN),
            fork_key,
        )
        .await
    else {
        unreachable!();
    };
    let children = provider
        .read_results(
            crate::results::ResultCursor {
                cut: children,
                from: 0,
            },
            NonZeroU32::new(1_025).unwrap_or(NonZeroU32::MIN),
        )
        .await
        .unwrap_or_else(|_| unreachable!())
        .machines;
    assert_eq!(
        provenance,
        Provenance {
            source: ForkSource::Checkpoint(checkpoint.id),
            inherited: ForkFidelity::MemoryAndDisk,
        }
    );
    assert_eq!(children.len(), 1_025);
    let mut selected = children.iter();
    let first = selected.next().unwrap_or_else(|| unreachable!());
    let second = selected.next().unwrap_or_else(|| unreachable!());
    assert_ne!(first.id, second.id);
    assert_ne!(first.id, machine.id);
    // The stored contract names the checkpoint, so every read agrees with the reply.
    let mut listed = std::collections::BTreeMap::new();
    let mut after = None;
    loop {
        let page = provider
            .list_machines(after, MAX_PAGE_SIZE)
            .await
            .unwrap_or_else(|_| unreachable!());
        for machine in page.machines {
            assert!(listed.insert(machine.id, machine).is_none());
        }
        let Some(next) = page.next else { break };
        assert!(after.is_none_or(|previous| next > previous));
        after = Some(next);
    }
    for child in &children {
        assert_eq!(child.contract.image, Image::Checkpoint(checkpoint.id));
        assert_eq!(provider.inspect_machine(child.id).await.as_ref(), Ok(child));
        assert_eq!(listed.get(&child.id), Some(child));
    }
    provider
        .destroy_checkpoint(
            checkpoint.id,
            IdempotencyKey::parse("00000000-0000-0000-0000-00000000000b")
                .unwrap_or_else(|_| unreachable!()),
        )
        .await
        .unwrap_or_else(|_| unreachable!());
    assert!(provider.inspect_machine(machine.id).await.is_ok());
    assert!(provider.inspect_machine(first.id).await.is_ok());
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
    let Ok(MutationOutcome::Forked {
        provenance,
        results: children,
    }) = first.clone()
    else {
        unreachable!("live fork must succeed: {first:?}")
    };
    let children = provider
        .read_results(
            crate::results::ResultCursor {
                cut: children,
                from: 0,
            },
            NonZeroU32::new(1_025).unwrap_or(NonZeroU32::MIN),
        )
        .await
        .unwrap_or_else(|_| unreachable!())
        .machines;
    assert_eq!(
        provenance,
        Provenance {
            source: ForkSource::Machine(parent.id),
            inherited: ForkFidelity::MemoryAndDisk,
        }
    );
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
    let Ok(MutationOutcome::Forked {
        provenance,
        results: grandchildren,
    }) = provider.fork_machine(child, NonZeroU32::MIN, key(3)).await
    else {
        unreachable!();
    };
    let grandchildren = provider
        .read_results(
            crate::results::ResultCursor {
                cut: grandchildren,
                from: 0,
            },
            NonZeroU32::new(1_025).unwrap_or(NonZeroU32::MIN),
        )
        .await
        .unwrap_or_else(|_| unreachable!())
        .machines;
    assert_eq!(provenance.inherited, ForkFidelity::MemoryAndDisk);
    assert_eq!(grandchildren.len(), 1);
}

#[tokio::test]
async fn live_fork_sources_are_joined_after_their_children() {
    let provider = SimulatedMachines::default();
    let Ok(MutationOutcome::Created(parent)) = provider.create(request(key(0x10))).await else {
        unreachable!()
    };
    let Ok(MutationOutcome::Forked {
        results: children, ..
    }) = provider.fork_machine(parent.id, two(), key(0x11)).await
    else {
        unreachable!()
    };
    let children = provider
        .read_results(
            crate::results::ResultCursor {
                cut: children,
                from: 0,
            },
            NonZeroU32::new(1_025).unwrap_or(NonZeroU32::MIN),
        )
        .await
        .unwrap_or_else(|_| unreachable!())
        .machines;
    assert_eq!(
        provider.destroy_machine(parent.id, key(0x12)).await,
        Err(ProviderError::FailedPrecondition(Precondition::LiveForks))
    );
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
async fn live_fork_forks_a_suspended_source_without_waking_it() {
    let provider = SimulatedMachines::default();
    let Ok(MutationOutcome::Created(parent)) = provider.create(request(key(0x20))).await else {
        unreachable!()
    };
    provider
        .suspend(parent.id, key(0x21))
        .await
        .unwrap_or_else(|_| unreachable!());
    let Ok(MutationOutcome::Forked {
        results: children, ..
    }) = provider.fork_machine(parent.id, two(), key(0x22)).await
    else {
        unreachable!()
    };
    let children = provider
        .read_results(
            crate::results::ResultCursor {
                cut: children,
                from: 0,
            },
            NonZeroU32::new(1_025).unwrap_or(NonZeroU32::MIN),
        )
        .await
        .unwrap_or_else(|_| unreachable!())
        .machines;
    assert!(
        children
            .iter()
            .all(|child| child.state == MachineState::Running)
    );
    assert_eq!(
        provider
            .inspect_machine(parent.id)
            .await
            .map(|value| value.state),
        Ok(MachineState::Suspended)
    );
    // A destroyed source forks nothing.
    for (index, child) in (0x25_u8..).zip(&children) {
        provider
            .destroy_machine(child.id, key(index))
            .await
            .unwrap_or_else(|_| unreachable!());
    }
    provider
        .destroy_machine(parent.id, key(0x27))
        .await
        .unwrap_or_else(|_| unreachable!());
    assert!(matches!(
        provider.fork_machine(parent.id, two(), key(0x28)).await,
        Err(ProviderError::Conflict(_))
    ));
    assert!(matches!(
        provider
            .fork_machine(MachineId::new(), two(), key(0x23))
            .await,
        Err(ProviderError::NotFound(_))
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
        Ok(MutationOutcome::Forked {
            provenance: Provenance {
                inherited: ForkFidelity::DiskOnly,
                ..
            },
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
