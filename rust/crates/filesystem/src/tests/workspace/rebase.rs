use super::*;

#[tokio::test]
async fn live_rebase_advances_fork_lineage_and_preserves_independent_changes()
-> Result<(), Box<dyn Error>> {
    #[cfg(not(target_arch = "wasm32"))]
    crate::initialize_test_native_io();
    let fs = Fs::memory();
    let main = unbounded!(fs, create_workspace("main-rebase"))?;
    main.write_text("/base", "base").await?;
    let agent = fork_workspace(
        &main,
        "agent-rebase",
        ForkOptions::from_generation(unbounded!(main, head())?, IdempotencyKey::new()),
    )
    .await?;
    agent.write_text("/agent", "local").await?;
    main.write_text("/upstream", "first").await?;

    let before = unbounded!(agent, head())?.id();
    let cancellation = CancellationToken::new();
    let refused = agent
        .live_rebase(
            IdempotencyKey::new(),
            Some(128),
            Some(128),
            Some(32),
            WorkBudget {
                backend_read_operations: 0,
                ..WorkBudget::UNBOUNDED
            },
            &cancellation,
        )
        .await;
    assert!(
        matches!(
            refused,
            Err(crate::OperationFailure {
                error: WorkspaceError::Work(crate::WorkError::BudgetExceeded {
                    counter: crate::WorkCounter::BackendReadOperations,
                    ..
                }),
                ..
            })
        ),
        "live rebase bypassed original read admission"
    );
    assert_eq!(unbounded!(agent, head())?.id(), before);
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let failure = agent
        .live_rebase(
            IdempotencyKey::new(),
            Some(128),
            Some(128),
            Some(32),
            WorkBudget::UNBOUNDED,
            &cancelled,
        )
        .await
        .err()
        .ok_or("cancelled live rebase completed")?;
    assert!(matches!(failure.error, WorkspaceError::Cancelled(_)));
    assert_eq!(*failure.work().unwrap(), crate::WorkCounters::UNCHARGED);

    for reads in [1, 2] {
        let source = crate::workspace::Prefunded {
            budget: WorkBudget {
                backend_read_operations: reads,
                ..WorkBudget::UNBOUNDED
            },
            held: std::sync::Mutex::new(None),
        };
        let (result, spent, live, baseline) = agent
            .live_rebase(
                IdempotencyKey::new(),
                Some(128),
                Some(128),
                Some(32),
                &source,
                &cancellation,
            )
            .await;
        assert!(matches!(
            result,
            Err(WorkspaceError::Work(crate::WorkError::BudgetExceeded {
                counter: crate::WorkCounter::BackendReadOperations,
                ..
            }))
        ));
        assert_eq!(spent.backend_read_operations, reads);
        assert_eq!(
            live, baseline,
            "refused live rebase retained original scratch"
        );
        assert_eq!(unbounded!(agent, head())?.id(), before);
    }

    let first_key = IdempotencyKey::new();
    let source = crate::workspace::Prefunded {
        budget: WorkBudget::UNBOUNDED,
        held: std::sync::Mutex::new(None),
    };
    let (result, spent, live, baseline) = agent
        .live_rebase(
            first_key,
            Some(128),
            Some(128),
            Some(32),
            &source,
            &cancellation,
        )
        .await;
    let WorkspaceRebase::Rebased(first) = result? else {
        return Err("first live rebase did not publish".into());
    };
    assert!(spent.backend_read_operations > 2);
    assert!(spent.backend_write_operations > 0);
    assert!(spent.authority_records_appended > 0);
    assert_eq!(live, baseline, "live rebase retained temporary scratch");
    assert_eq!(
        unbounded!(first, read("/agent", 16))?,
        OwnedBytes::from_static(b"local")
    );
    assert_eq!(
        unbounded!(first, read("/upstream", 16))?,
        OwnedBytes::from_static(b"first")
    );
    assert!(matches!(
        unbounded!(
            agent,
            live_rebase(first_key, Some(128), Some(128), Some(32))
        )?,
        WorkspaceRebase::Current(_)
    ));

    main.write_text("/later", "second").await?;
    #[cfg(target_arch = "wasm32")]
    assert!(matches!(
        unbounded!(
            agent,
            live_rebase(IdempotencyKey::new(), Some(128), Some(128), Some(32))
        )?,
        WorkspaceRebase::Rebased(_)
    ));
    #[cfg(not(target_arch = "wasm32"))]
    let (original, held, initial, baseline) = {
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let initial = original.live_allocation_bytes().unwrap();
        let held = crate::kernel::allocation::retained::Retained::<Vec<u8>>::allocate(
            4096,
            &mut crate::kernel::allocation::KernelAccounting::native(&original, &cancellation),
        )?;
        let baseline = original.live_allocation_bytes().unwrap();
        assert!(matches!(
            agent
                .live_rebase(
                    IdempotencyKey::new(),
                    Some(128),
                    Some(128),
                    Some(32),
                    &original,
                    &cancellation,
                )
                .await?,
            WorkspaceRebase::Rebased(_)
        ));
        assert!(original.receipt().unwrap().backend_read_operations > 2);
        assert!(original.receipt().unwrap().backend_write_operations > 0);
        assert!(original.receipt().unwrap().authority_records_appended > 0);
        assert!(original.live_allocation_bytes().unwrap() > baseline);
        (original, held, initial, baseline)
    };
    assert_eq!(
        unbounded!(agent, read("/later", 16))?,
        OwnedBytes::from_static(b"second")
    );
    assert_eq!(
        unbounded!(agent, read("/agent", 16))?,
        OwnedBytes::from_static(b"local")
    );
    #[cfg(not(target_arch = "wasm32"))]
    {
        drop((first, agent, main, fs));
        assert_eq!(original.live_allocation_bytes().unwrap(), baseline);
        drop(held);
        assert_eq!(original.live_allocation_bytes().unwrap(), initial);
    }
    Ok(())
}
