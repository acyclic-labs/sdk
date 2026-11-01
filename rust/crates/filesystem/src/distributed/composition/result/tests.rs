use super::*;
use acyclic_stream::{IdempotencyKey, MemoryStream, OwnedBytes};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn native_result_custody_survives_semantic_confirmation_and_retirement() -> TestResult {
    for decision in ["committed", "malformed", "conflict", "foreign"] {
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(
            crate::performance::BOUNDARY_BUDGET,
            &cancellation,
        )
        .map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let provider = Arc::new(MemoryStream::default());
        let store = StreamAuthorityStore::new(provider.clone());
        let collection = Arc::new(crate::collection::Collection::default());
        let mut work = AuthorityWork::native(&original, &cancellation);
        let mut commit = FilesystemCommit::new(CommitRequest {
            conditions: Vec::new(),
            mutations: Vec::new(),
            idempotency_key: IdempotencyKey::new(OwnedBytes::from_static(b"retained-result"))?,
            deadline_unix_millis: None,
        });
        let path = StreamPath::new("state/retained-result")?;
        let observed =
            store.authorized_observation(CommitCondition::Absent { path: path.clone() })?;
        store.contribute_record(
            &mut commit,
            &observed,
            &crate::distributed::state_record::StateRecord::Companion {
                kind: crate::distributed::state_record::CompanionKind::OperationOutcome,
                payload: OwnedBytes::from_static(b"original result"),
            },
            collection.unswept().await.into_hold(),
            &mut work,
        )?;

        // A later caller contribution transfers its actual capacity to the
        // original commit without retiring the caller's cached read context.
        let controls = work.vec::<u8>(19)?;
        let meter = work.native_meter()?;
        let before_transfer = original.receipt();
        commit.retain_native_custody(work.request_allocation.as_mut().unwrap(), &original)?;
        assert_eq!(work.request_allocation.as_ref().unwrap().bytes(), 0);
        assert_eq!(original.receipt(), before_transfer);
        assert!(Arc::ptr_eq(&meter, &work.native_meter()?));

        // Same ledger is insufficient: a distinct invocation is not custody
        // for this request. Both paid guards and the original bytes survive.
        let (foreign_work, foreign_cancellation) = original.invocation_view()?;
        let mut foreign = AuthorityWork::native(&foreign_work, &foreign_cancellation);
        let foreign_controls = foreign.vec::<u8>(23)?;
        let paid = foreign.request_allocation.as_ref().unwrap().bytes();
        let live = original.live_allocation_bytes();
        let request_before = commit.request().clone();
        assert!(
            commit
                .retain_native_custody(foreign.request_allocation.as_mut().unwrap(), &original)
                .is_err()
        );
        assert_eq!(foreign.request_allocation.as_ref().unwrap().bytes(), paid);
        assert_eq!(original.live_allocation_bytes(), live);
        assert_eq!(commit.request(), &request_before);
        drop(request_before);
        drop(foreign_controls);
        drop(foreign);
        drop(foreign_work);
        drop(foreign_cancellation);

        let retained_construction = commit.allocation.as_ref().unwrap().bytes();
        store.prepare_records(&mut commit, std::iter::empty(), &[], None, &mut work)?;
        assert!(commit.allocation.as_ref().unwrap().bytes() >= retained_construction);
        assert!(
            commit
                .retain_native_custody(work.request_allocation.as_mut().unwrap(), &original)
                .is_err()
        );
        let exact_request = commit.request().clone();
        if decision == "conflict" {
            let mut winner = exact_request.clone();
            winner.idempotency_key = IdempotencyKey::unique();
            assert!(matches!(
                provider.commit(winner).await?,
                CommitOutcome::Committed(_)
            ));
        }
        let request = work.copy_commit(commit.request())?;
        let mut output = provider
            .native_commit(request, None, work.native_meter()?)
            .await??;
        work.release_request()?;
        if decision == "malformed" {
            let CommitOutcome::Committed(receipt) = output.value_mut() else {
                return Err("actual native publication did not commit".into());
            };
            receipt.appends.clear();
        }
        let (outcome, allocation) = output.into_parts();
        let result_bytes = allocation.bytes();
        assert!(result_bytes > 0);
        let pointer = match &outcome {
            CommitOutcome::Committed(receipt) => receipt.appends.as_ptr() as usize,
            CommitOutcome::Conflict(conflicts) => conflicts.as_ptr() as usize,
        };
        let prepared = store.prepare_native_settlement(
            commit,
            acyclic_stream::NativeStreamOutput::new(outcome, allocation),
        );
        drop(controls);
        drop(work);
        drop(meter);
        cancellation.cancel();
        let mut collecting = Box::pin(collection.begin());
        match prepared {
            Ok(prepared) => {
                assert_eq!(prepared.request(), &exact_request);
                match prepared.outcome() {
                    CommitOutcome::Committed(receipt) => {
                        assert_ne!(decision, "conflict");
                        assert_eq!(receipt.appends.as_ptr() as usize, pointer);
                    }
                    CommitOutcome::Conflict(conflicts) => {
                        assert_eq!(decision, "conflict");
                        assert!(!conflicts.is_empty());
                        assert_eq!(conflicts.as_ptr() as usize, pointer);
                    }
                }
                assert!(futures::poll!(collecting.as_mut()).is_pending());
                let result = if decision == "foreign" {
                    let other = StreamAuthorityStore::new(Arc::new(MemoryStream::default()));
                    other.finish_settlement(prepared)?
                } else {
                    store.finish_settlement(prepared)?
                };
                if decision == "foreign" {
                    let FilesystemCommitOutcome::Unknown { commit, .. } = result else {
                        return Err("foreign store consumed a validated original result".into());
                    };
                    assert_eq!(commit.request(), &exact_request);
                    assert!(futures::poll!(collecting.as_mut()).is_pending());
                    drop(commit);
                } else if decision == "conflict" {
                    let FilesystemCommitOutcome::Rejected(conflicts) = result else {
                        return Err("original conditional decision was discarded".into());
                    };
                    assert_eq!(conflicts.as_ptr() as usize, pointer);
                    let retained = original.live_allocation_bytes();
                    assert!(retained >= baseline + result_bytes);
                    drop(conflicts);
                    assert!(original.live_allocation_bytes() <= retained - result_bytes);
                } else {
                    let FilesystemCommitOutcome::Committed(receipt) = result else {
                        return Err("original committed receipt was discarded".into());
                    };
                    assert_eq!(receipt.appends.as_ptr() as usize, pointer);
                    assert!(original.live_allocation_bytes() >= baseline + result_bytes);
                    let retained = original.live_allocation_bytes();
                    drop(receipt);
                    assert!(original.live_allocation_bytes() <= retained - result_bytes);
                }
            }
            Err(FilesystemCommitOutcome::Unknown { commit, .. }) if decision == "malformed" => {
                assert_eq!(commit.request(), &exact_request);
                assert!(futures::poll!(collecting.as_mut()).is_pending());
                drop(commit);
            }
            _ => return Err("unexpected native settlement decision".into()),
        }
        assert!(futures::poll!(collecting.as_mut()).is_ready());
        drop(collecting);
        drop(exact_request);
        drop(store);
        drop(provider);
        assert_eq!(original.live_allocation_bytes(), baseline);
    }
    // The original cache holder may itself await this durable result. Accepted
    // completion must finish without waiting for that holder to release its lease.
    let (store, authority) = crate::distributed::append::group_tests::fixture().await?;
    let members = crate::distributed::append::group_tests::requests(authority, 1)?;
    let cancellation = CancellationToken::new();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let mut commit = FilesystemCommit::new(CommitRequest {
        conditions: Vec::new(),
        mutations: Vec::new(),
        idempotency_key: IdempotencyKey::new(OwnedBytes::from_static(b"held-cache-result"))?,
        deadline_unix_millis: None,
    });
    let FilesystemAppendPreparation::Ready(confirmation) = store
        .contribute_append_group(&mut commit, &members, &[], &mut work)
        .await?
    else {
        return Err("original authority append refused".into());
    };
    store.seal(&mut commit, &mut work)?;
    let actor = commit.contributions[0].actor.clone();
    let lease = actor.journal.lock().await;
    let outcome = store.provider().commit(commit.request().clone()).await?;
    let prepared = match store.prepare_settlement(commit, outcome) {
        Ok(prepared) => prepared,
        Err(_) => return Err("original authority receipt refused".into()),
    };
    let FilesystemCommitOutcome::Committed(receipt) = store.finish_settlement(prepared)? else {
        return Err("held cache blocked original completion".into());
    };
    drop(receipt);
    drop(lease);
    drop(confirmation);
    assert_eq!(store.head(authority, &mut work).await?.sequence.get(), 1);
    Ok(())
}
