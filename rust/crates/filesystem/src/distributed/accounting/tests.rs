use super::*;
use acyclic_stream::{
    MemoryStream,
    testing::{Counted, RequestClass},
};

type TestResult = Result<(), Box<dyn std::error::Error>>;
const BUDGET: WorkBudget = crate::performance::BOUNDARY_BUDGET;
const AUTHORITY: AuthorityId = AuthorityId::from_bytes([92; 16]);

#[tokio::test]
async fn historical_reads_preserve_the_complete_original_receipt() -> TestResult {
    crate::initialize_test_native_io();
    let provider = Arc::new(Counted::new(MemoryStream::default()));
    let store = StreamAuthorityStore::new(provider.clone());
    let cancel = CancellationToken::new();
    store
        .create_first_record(
            AUTHORITY,
            ProposedCommit {
                publication: None,
                operation_id: OperationId::from_bytes([93; 16]),
                fingerprint: Digest::from_bytes([94; 32]),
                payload: Bytes::from_static(b"retained history"),
            },
            &mut AuthorityWork::new(BUDGET, &cancel),
            None::<&mut ()>,
        )
        .await?;
    store
        .retire_authority(AUTHORITY, &mut AuthorityWork::new(BUDGET, &cancel))
        .await?;
    let historical = AccountingAuthority(&store);
    let limit = ReplayLimit {
        records: 1,
        payload_bytes: STREAM_RECORD_LIMIT,
    };
    for native in [false, true] {
        let cancel = CancellationToken::new();
        let expected_owner =
            crate::NativeMaterializationWork::new(BUDGET, &cancel).map_err(|(error, _)| error)?;
        let actual_owner =
            crate::NativeMaterializationWork::new(BUDGET, &cancel).map_err(|(error, _)| error)?;
        let mut expected = if native {
            AuthorityWork::native(&expected_owner, &cancel)
        } else {
            AuthorityWork::new(BUDGET, &cancel)
        };
        let mut actual = if native {
            AuthorityWork::native(&actual_owner, &cancel)
        } else {
            AuthorityWork::new(BUDGET, &cancel)
        };
        let expected_independent = crate::kernel::allocation::retained::RetainedAllocation::claim(
            73,
            1,
            &mut expected.accounting(),
        )?;
        let actual_independent = crate::kernel::allocation::retained::RetainedAllocation::claim(
            73,
            1,
            &mut actual.accounting(),
        )?;
        let expected_head = {
            let mut scope = expected.scope()?;
            let snapshot = store.snapshot(AUTHORITY, &mut scope).await?;
            scope.finish(snapshot.head)?
        };
        let before = provider.requests();
        let head = historical.head(AUTHORITY, &mut actual).await?;
        assert_eq!(head, expected_head);
        let calls = provider.requests().since(before);
        assert_eq!(calls.class(RequestClass::Head), 1);
        assert_eq!(calls.auxiliary(Some(RequestClass::Head)), Some(0));
        assert_eq!(
            actual.spent(),
            expected.spent(),
            "historical head lost original completed work"
        );
        let encoded_before = actual.spent().bytes_encoded;
        let live = |work: &AuthorityWork<'_>| {
            if native {
                work.original.unwrap().live_allocation_bytes()
            } else {
                work.allocations.live_bytes()
            }
        };
        let before_replay = live(&actual);
        let (expected_records, expected_allocation) = {
            let mut scope = expected.scope()?;
            let value = store
                .read_records(AUTHORITY, Sequence::GENESIS, limit, &mut scope)
                .await?;
            scope.finish(value)?
        };
        let before = provider.requests();
        let (records, allocation) = historical
            .replay(AUTHORITY, Sequence::GENESIS, limit, &mut actual)
            .await?;
        let calls = provider.requests().since(before);
        assert_eq!(calls.class(RequestClass::Data), 1);
        assert_eq!(calls.auxiliary(Some(RequestClass::Data)), Some(0));
        assert_eq!(records, expected_records);
        eprintln!(
            "HISTORICAL_REPLAY_RECEIPT native={native} codec_encoded_delta={} work={:?}",
            actual.spent().bytes_encoded - encoded_before,
            actual.spent()
        );
        assert_eq!(
            actual.spent(),
            expected.spent(),
            "historical replay lost original completed work"
        );
        let container_bytes = (records.capacity() * std::mem::size_of::<DurableCommit>()) as u64;
        let allocation = allocation.map_or(
            crate::kernel::allocation::KernelOwnedAllocation::Standalone,
            crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal,
        );
        let records = crate::kernel::allocation::retained::Retained::from_owned(
            crate::kernel::Owned::new(records, allocation, container_bytes),
            &mut actual.accounting(),
        )?;
        let alias = records
            .value
            .first()
            .ok_or("historical replay omitted its physical record")?
            .payload
            .clone();
        assert!(
            live(&actual) >= before_replay + alias.len() as u64,
            "historical replay retired funding before its returned payload"
        );
        let before_container_retirement = live(&actual);
        drop((records, expected_records, expected_allocation));
        assert_eq!(
            live(&actual),
            before_container_retirement - container_bytes,
            "replay container did not retire its exact original capacity"
        );
        actual.spent().verify(BUDGET)?;
        cancel.cancel();
        assert!(
            live(&actual) >= before_replay + alias.len() as u64,
            "historical replay retired funding before its last payload alias"
        );
        drop(alias);
        assert_eq!(
            live(&actual),
            before_replay,
            "last payload alias did not retire its original backing"
        );
        drop((actual_independent, expected_independent));
    }
    for counter in ["allocation_operations", "bytes_hashed", "bytes_copied"] {
        let budget = match counter {
            "allocation_operations" => WorkBudget {
                allocation_operations: 0,
                ..BUDGET
            },
            "bytes_hashed" => WorkBudget {
                bytes_hashed: 0,
                ..BUDGET
            },
            _ => WorkBudget {
                bytes_copied: 0,
                ..BUDGET
            },
        };
        let mut work = AuthorityWork::new(budget, &cancel);
        let result = historical
            .replay(AUTHORITY, Sequence::GENESIS, limit, &mut work)
            .await;
        assert!(
            matches!(result, Err(AuthorityStoreError::Work(crate::WorkError::BudgetExceeded {
                counter: denied, ..
            })) if denied == counter),
            "historical replay did not preserve the denied {counter} cause"
        );
        let spent = work.spent();
        let denied = match counter {
            "allocation_operations" => spent.allocation_operations,
            "bytes_hashed" => spent.bytes_hashed,
            _ => spent.bytes_copied,
        };
        assert_eq!(denied, 0, "physical work preceded {counter} admission");
    }
    #[cfg(feature = "local")]
    {
        let directory = tempfile::tempdir()?;
        let provider = Arc::new(Counted::new(
            acyclic_stream::LocalStream::open(directory.path(), Default::default()).await?,
        ));
        let local = StreamAuthorityStore::new(provider.clone());
        let historical = AccountingAuthority(&local);
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(BUDGET, &cancellation)
            .map_err(|(error, _)| error)?;
        let mut work = AuthorityWork::native(&original, &cancellation);
        let (entered, observed) = tokio::sync::oneshot::channel();
        let (release, released) = tokio::sync::oneshot::channel();
        let mut publication =
            std::pin::pin!(provider.inner().test_publication_barrier(entered, released));
        assert!(futures::poll!(publication.as_mut()).is_pending());
        observed.await?;
        let before = provider.requests();
        let mut head = Box::pin(historical.head(AUTHORITY, &mut work));
        assert!(futures::poll!(head.as_mut()).is_pending());
        assert_eq!(
            provider.requests().since(before).class(RequestClass::Head),
            1
        );
        // Cancellation reaches the actual native provider after admission,
        // while its coherent head capture waits on the original publication.
        cancellation.cancel();
        release.send(()).map_err(|_| "publication owner exited")?;
        publication.await;
        assert!(matches!(head.await, Err(AuthorityStoreError::Cancelled)));
        assert_eq!(original.receipt().authority_records_read, 0);
        drop(work);
        assert_eq!(
            original.live_allocation_bytes(),
            crate::NativeMaterializationWork::accounting_state_bytes()
        );
    }
    cancel.cancel();
    let mut work = AuthorityWork::new(BUDGET, &cancel);
    let before = provider.requests();
    assert!(matches!(
        historical.head(AUTHORITY, &mut work).await,
        Err(AuthorityStoreError::Cancelled)
    ));
    assert_eq!(provider.requests(), before);
    assert_eq!(work.spent(), WorkCounters::UNCHARGED);
    Ok(())
}
