use super::*;

#[tokio::test]
async fn fresh_invocation_appends_after_cached_invocation_cancellation() -> Result<()> {
    for cancel_before_next in [false, true] {
        let journal = fixture().await?;
        let operation = OperationId::from_bytes([1; 16]);
        let event = ExecutionEvent::Started {
            request_digest: [1; 32],
        };
        let root = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(10_000_000, 1 << 28),
            &root,
        )
        .unwrap();
        let (first, cancel_first) = work.invocation_view().unwrap();
        let io = PreparationIo::from_native_work(&first)?;
        journal
            .append(operation, "start", &event, Some(&io))?
            .await?;
        journal
            .append(operation, "start", &event, Some(&io))?
            .await?;
        if cancel_before_next {
            cancel_first.cancel();
        }
        let (next, cancel_next) = work.invocation_view().unwrap();
        assert!(first.shares_allocation_context(&next));
        assert!(!first.shares_invocation_context(&next));
        cancel_first.cancel();

        // The old allocation-only match keeps this funding. Exercise the same
        // production replay with it to prove the control rejects fresh work.
        {
            let mut cache = journal.verified.lock().await;
            assert_eq!(cache.summary.tail, 1);
            let cache::VerifiedSummary { summary, funding } = &mut *cache;
            let rejected = journal
                .refresh_summary(operation, summary, funding.as_ref())
                .await;
            assert_eq!(
                rejected.unwrap_err().code(),
                crate::wire::ErrorCode::Cancelled
            );
        }

        let next_io = PreparationIo::from_native_work(&next)?;
        journal
            .append(operation, "start", &event, Some(&next_io))?
            .await?;
        assert_eq!(journal.replay(operation, 0, 2, None)?.await?.len(), 1);
        {
            let cache = journal.verified.lock().await;
            let retained = cache
                .funding
                .as_ref()
                .and_then(PreparationIo::original_native_work)
                .expect("native cache funding");
            assert!(retained.shares_invocation_context(&next));
            assert_eq!(cache.summary.tail, 1);
        }
        cancel_next.cancel();
        let rejected = async {
            journal
                .append(operation, "start", &event, Some(&next_io))?
                .await
        }
        .await;
        assert_eq!(
            rejected.unwrap_err().code(),
            crate::wire::ErrorCode::Cancelled
        );
        drop((journal, io, next_io, first, next, cancel_first, cancel_next));
        assert_eq!(
            work.live_allocation_bytes(),
            acyclic_fs::NativeMaterializationWork::accounting_state_bytes()
        );
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn cached_funding_matches_exact_invocation(a in 0usize..4, b in 0usize..4) {
        let cancellation = acyclic_fs::CancellationToken::new();
        let root = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(1_000_000, 1 << 24), &cancellation,
        ).unwrap();
        let (first, _) = root.invocation_view().unwrap();
        let (second, _) = root.invocation_view().unwrap();
        let views = [root, first.clone(), first, second];
        let same = a == b || (a == 1 && b == 2) || (a == 2 && b == 1);
        prop_assert_eq!(views[a].shares_invocation_context(&views[b]), same);
        let io = PreparationIo::from_native_work(&views[a]).unwrap();
        let next = PreparationIo::from_native_work(&views[b]).unwrap();
        let operation = OperationId::from_bytes([1; 16]);
        let mut cache = cache::VerifiedSummary::default();
        cache.bind(operation, Some(&io)).unwrap();
        cache.summary.operation = Some(operation);
        cache.summary.tail = 1;
        cache.bind(operation, Some(&next)).unwrap();
        prop_assert_eq!(cache.summary.tail, u64::from(same));
        if !same {
            prop_assert!(views[a].shares_allocation_context(&views[b]),
                "allocation-only negative control must conflate invocations");
        }
    }
}
