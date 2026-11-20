use super::*;

// These two cases, with the cursor property in tests/results.rs, carry the settled-result guarantees.
#[tokio::test]
async fn failed_fork_cuts_every_birth_and_cleanup_replays_after_recovery()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::results::{ResultCursor, cleanup};
    // Both physical fork intents use the same birth retention and cleanup path.
    for live in [false, true] {
        for k in 0..4 {
            let provider = SimulatedMachines::default();
            let MutationOutcome::Created(parent) = provider.create(request(key(0x60))).await?
            else {
                unreachable!("original create");
            };
            let MutationOutcome::Checkpointed(checkpoint) =
                provider.checkpoint(parent.id, key(0x61)).await?
            else {
                unreachable!("original checkpoint");
            };
            provider.fail_fork_after(k).await;
            let count = NonZeroU32::new(4).unwrap();
            let outcome = if live {
                provider.fork_machine(parent.id, count, key(0x62)).await
            } else {
                provider.fork(checkpoint.id, count, key(0x62)).await
            };
            let Err(ProviderError::ForkFailed { phase, results }) = &outcome else {
                let leaked = provider.list_machines(None, 16).await?.machines.len() - 1;
                panic!("failed fork stranded {leaked} children without a cut: {outcome:?}");
            };
            assert_eq!(*phase, OperationPhase::Failed);
            assert_eq!(results.count, u64::from(k));
            assert_eq!(
                provider.inspect_operation(results.operation).await?.phase,
                *phase
            );
            // Lose the original result handle. The retained operation owns both
            // the same failure and its immutable child observations.
            let recovered = provider.clone().recover(key(0x62)).await;
            assert_eq!(recovered, outcome);
            let cursor = ResultCursor {
                cut: *results,
                from: 0,
            };
            let children = provider.read_results(cursor, count).await?.machines;
            let expected = (0..k)
                .map(|index| SimulatedMachines::machine(key(0x62), index))
                .collect::<Vec<_>>();
            assert_eq!(
                children.iter().map(|child| child.id).collect::<Vec<_>>(),
                expected
            );
            assert_eq!(
                provider.list_machines(None, 16).await?.machines.len(),
                usize::try_from(k)? + 1
            );
            cleanup(&provider, *results, NonZeroU32::MIN).await?;
            // A cleanup reply can also be lost. Identical derived destroy keys
            // recover the original destroys rather than dispatching new ones.
            cleanup(&provider.clone(), *results, two()).await?;
            for child in &children {
                assert_eq!(
                    provider.inspect_machine(child.id).await?.state,
                    MachineState::Destroyed
                );
            }
            assert_eq!(
                provider.read_results(cursor, count).await?.machines,
                children
            );
            provider.destroy_machine(parent.id, key(0x63)).await?;
        }
    }
    Ok(())
}

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
