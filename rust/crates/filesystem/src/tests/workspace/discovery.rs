use super::*;

#[tokio::test]
async fn bounded_direct_paths_retain_all_admitted_aliases() -> Result<(), Box<dyn Error>> {
    #[cfg(not(target_arch = "wasm32"))]
    crate::initialize_test_native_io();
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("bounded-direct-paths"))?;
    let mut transaction = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    transaction.write_text("/a", "body").await?;
    unbounded!(transaction, hard_link("/a", "/b"))?;
    assert!(matches!(
        commit(&mut transaction).await?,
        TransactionCommit::Committed(_)
    ));
    let generation = unbounded!(workspace, head())?;
    let id = unbounded!(generation, stat("/a"))?.file_id;
    let cancellation = CancellationToken::new();
    let budget = crate::performance::BOUNDARY_BUDGET;
    let refused = generation
        .paths_for_file_ids(
            [id],
            Some(16),
            WorkBudget {
                backend_read_operations: 0,
                ..budget
            },
            &cancellation,
        )
        .await;
    assert!(
        matches!(
            refused,
            Err(crate::OperationFailure {
                error: WorkspaceError::Work(crate::WorkError::BudgetExceeded {
                    counter: "backend_read_operations",
                    ..
                }),
                ..
            })
        ),
        "reverse paths ignored caller read admission"
    );
    let complete = generation
        .paths_for_file_ids([id], Some(16), budget, &cancellation)
        .await?
        .value;
    assert_eq!(
        complete.entries(),
        &[(id, vec!["/a".to_owned(), "/b".to_owned()])]
    );
    assert!(complete.is_complete());
    let bounded = generation
        .paths_for_file_ids([id], Some(1), budget, &cancellation)
        .await?
        .value;
    assert_eq!(bounded.entries()[0].1.len(), 1);
    assert!(!bounded.is_complete());
    for admitted_reads in [0, budget.backend_read_operations] {
        let source = crate::workspace::accounting_tests::Prefunded {
            budget: WorkBudget {
                backend_read_operations: admitted_reads,
                ..budget
            },
            held: std::sync::Mutex::new(None),
        };
        let (result, spent, live, baseline) = generation
            .paths_for_file_ids([id, id], Some(16), &source, &cancellation)
            .await;
        spent.verify(source.budget)?;
        if admitted_reads == 0 {
            assert!(result.is_err());
            assert_eq!(live, baseline, "refused paths retired independent backing");
        } else {
            let result = result?;
            assert_eq!(result.entries(), complete.entries());
            assert!(spent.backend_read_operations > 0);
            assert!(spent.bytes_encoded >= 4);
            assert!(
                live > baseline,
                "reverse paths returned without original backing"
            );
        }
    }
    {
        let mut original = crate::AuthorityWork::new(budget, &cancellation);
        let mut accounting = original.accounting();
        let independent = crate::kernel::Owned::<Vec<u8>>::allocate(73, &mut accounting)?;
        let independent = crate::kernel::allocation::retained::Retained::from_owned(
            independent,
            &mut accounting,
        )?;
        let baseline = accounting.standalone_parts().unwrap().0.live_bytes();
        let returned = generation
            .paths_for_file_ids_with_accounting([id], Some(16), &mut accounting, &cancellation)
            .await?;
        assert_eq!(returned.entries(), complete.entries());
        assert!(accounting.standalone_parts().unwrap().0.live_bytes() > baseline);
        drop(returned);
        assert_eq!(
            accounting.standalone_parts().unwrap().0.live_bytes(),
            baseline,
            "ordinary paths did not retire exactly their own backing"
        );
        drop(independent);
        accounting.spent().unwrap().verify(budget)?;
    }
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert!(matches!(
        generation
            .paths_for_file_ids([id], Some(16), budget, &cancelled)
            .await,
        Err(crate::OperationFailure {
            error: WorkspaceError::Cancelled(_),
            ..
        })
    ));
    assert!(
        generation
            .paths_for_file_ids(
                [id],
                Some(16),
                WorkBudget {
                    output_bytes: 0,
                    ..budget
                },
                &cancellation
            )
            .await
            .is_err()
    );
    #[cfg(not(target_arch = "wasm32"))]
    {
        let original = crate::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(error, _)| error)?;
        let birth = original.live_allocation_bytes().unwrap();
        let mut work = crate::AuthorityWork::native(&original, &cancellation);
        let independent = crate::kernel::Owned::<Vec<u8>>::allocate(73, &mut work.accounting())?;
        let baseline = original.live_allocation_bytes().unwrap();
        let returned = generation
            .paths_for_file_ids([id], Some(16), &original, &cancellation)
            .await?;
        assert_eq!(returned.entries(), complete.entries());
        assert!(original.live_allocation_bytes().unwrap() > baseline);
        drop(returned);
        assert_eq!(
            original.live_allocation_bytes().unwrap(),
            baseline,
            "paths did not retire exactly their own backing"
        );
        independent.retire(&mut work.accounting())?;
        assert_eq!(original.live_allocation_bytes().unwrap(), birth);
        original.receipt().unwrap().verify(budget)?;
    }
    Ok(())
}
