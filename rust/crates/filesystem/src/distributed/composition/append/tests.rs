use super::*;
use acyclic_stream::testing::Counted;
use acyclic_stream::{AppendRecord, IdempotencyKey, MemoryStream, OwnedBytes};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn caller(key: &'static [u8]) -> Result<FilesystemCommit, acyclic_stream::StreamError> {
    let path = StreamPath::new("caller/observation")?;
    Ok(FilesystemCommit::new(CommitRequest {
        deadline_unix_millis: None,
        conditions: vec![CommitCondition::Absent { path: path.clone() }],
        mutations: vec![CommitMutation::Append {
            path,
            records: vec![AppendRecord::from(OwnedBytes::from_static(
                b"original result",
            ))],
        }],
        idempotency_key: IdempotencyKey::new(OwnedBytes::from_static(key))?,
    }))
}

#[tokio::test]
async fn original_append_group_and_caller_share_one_commit_and_retained_confirmation() -> TestResult
{
    use crate::distributed::append::group_tests::requests;

    for count in [1, 4] {
        let provider = Arc::new(Counted::new(MemoryStream::default()));
        let store = StreamAuthorityStore::new(provider.clone());
        let authority = AuthorityId::from_bytes([33; 16]);
        let cancellation = CancellationToken::new();
        let mut setup = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        store
            .create_authority(authority, Epoch::GENESIS, &mut setup, None::<&mut ()>)
            .await?;
        let members = requests(authority, count)?;
        let mut publication = caller(b"joined")?;
        let original_key = publication.request().idempotency_key.clone();
        let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let FilesystemAppendPreparation::Ready(mut prepared) = store
            .contribute_append_group(&mut publication, &members, &[], &mut work)
            .await?
        else {
            return Err("original append group was refused".into());
        };
        assert_eq!(publication.request().idempotency_key, original_key);
        assert_eq!(work.spent().backend_write_operations, 0);
        store.seal(&mut publication, &mut work)?;
        let exact_request = publication.request().clone();
        let before = provider.requests();
        let outcome = provider.commit(exact_request.clone()).await?;
        let CommitOutcome::Committed(envelope) = &outcome else {
            return Err("whole original contribution did not commit".into());
        };
        assert_eq!(provider.requests().since(before).data(), 1);

        // Removing the actual authority range cannot certify its members.
        let records = work.path(store.placement(), authority, "/records", None)?;
        let mut missing = envelope.clone();
        missing.appends.retain(|append| append.path != records);
        assert!(
            prepared
                .confirm(&store, &CommitOutcome::Committed(missing), &mut work)
                .await
                .is_err()
        );
        let foreign = StreamAuthorityStore::with_placement(
            provider.clone(),
            AuthorityPlacement::new("other")?,
        );
        assert!(
            prepared
                .confirm(&foreign, &outcome, &mut work)
                .await
                .is_err()
        );
        let AppendGroupOutcome::Published {
            receipts,
            allocation,
        } = prepared.confirm(&store, &outcome, &mut work).await?
        else {
            return Err("confirmed original members were rejected".into());
        };
        assert_eq!(receipts.len(), members.len());
        for (receipt, member) in receipts.iter().zip(&members) {
            let FsAppendOutcome::Committed(record) = receipt else {
                return Err("original member was not committed".into());
            };
            assert_eq!(record.operation_id, member.commit.operation_id);
            assert_eq!(record.previous_digest, member.expected.digest);
        }
        drop(receipts);
        drop(allocation);
        assert!(matches!(
            prepared.confirm(&store, &outcome, &mut work).await,
            Err(AuthorityStoreError::RejectedStatic(
                "append confirmation already delivered"
            ))
        ));

        // A compact receipt must certify EVERY original caller, authority,
        // locator and policy member before any holds or projections settle.
        for index in 0..envelope.appends.len() {
            let mut missing = envelope.clone();
            missing.appends.remove(index);
            let FilesystemCommitOutcome::Unknown {
                commit: retained, ..
            } = store
                .settle(publication, CommitOutcome::Committed(missing), &mut work)
                .await?
            else {
                return Err("incomplete receipt settled original publication custody".into());
            };
            assert_eq!(retained.request(), &exact_request);
            publication = retained;
        }
        let mut reordered = envelope.clone();
        reordered.appends.swap(0, 1);
        let FilesystemCommitOutcome::Unknown {
            commit: retained, ..
        } = store
            .settle(publication, CommitOutcome::Committed(reordered), &mut work)
            .await?
        else {
            return Err("reordered receipt settled original publication custody".into());
        };
        assert_eq!(retained.request(), &exact_request);
        publication = retained;

        // A refused continuation keeps the original possibly applied owner.
        let other = StreamAuthorityStore::new(Arc::new(MemoryStream::default()));
        let FilesystemCommitOutcome::Unknown {
            commit: retained, ..
        } = other
            .settle(publication, outcome.clone(), &mut work)
            .await?
        else {
            return Err("foreign owner settled the original publication".into());
        };
        assert_eq!(retained.request(), &exact_request);
        let FilesystemCommitOutcome::Committed(replayed) =
            store.submit(retained, &mut work).await?
        else {
            return Err("same-owner exact publication replay was refused".into());
        };
        assert_eq!(&*replayed, envelope);
        assert_eq!(
            store.head(authority, &mut work).await?.sequence.get(),
            u64::from(count)
        );
        assert_eq!(
            provider
                .tail(StreamPath::new("caller/observation")?)
                .await?,
            1
        );

        // The actual separate-publication path remains functionally correct,
        // but has two provider commits and fails the one-commit cost target.
        let (split, split_authority) = crate::distributed::append::group_tests::fixture().await?;
        let split_members = requests(split_authority, count)?;
        let mut split_work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        drop(
            split
                .compare_and_append_group(&split_members, &[], &mut split_work, None::<&mut ()>)
                .await?,
        );
        let split_caller = caller(b"split")?;
        assert!(matches!(
            split.submit(split_caller, &mut split_work).await?,
            FilesystemCommitOutcome::Committed(_)
        ));
        assert_eq!(split_work.spent().backend_write_operations, 2);
    }

    // Rejection and cancellation must not leave a publishable prefix, alter
    // the original request, or retain an authority lease needed by its producer.
    let (store, authority) = crate::distributed::append::group_tests::fixture().await?;
    let mut members = requests(authority, 4)?;
    members[2].expected.digest = Digest::ZERO;
    let cancellation = CancellationToken::new();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let mut publication = caller(b"rejected")?;
    let original = publication.request().clone();
    assert!(matches!(
        store
            .contribute_append_group(&mut publication, &members, &[], &mut work)
            .await?,
        FilesystemAppendPreparation::Rejected(AppendGroupOutcome::Rejected { index: 2, .. })
    ));
    assert_eq!(publication.request(), &original);
    assert!(publication.authorities().is_empty());
    assert_eq!(work.spent().backend_write_operations, 0);
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let mut cancelled_work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancelled);
    assert!(matches!(
        store
            .contribute_append_group(&mut publication, &members, &[], &mut cancelled_work)
            .await,
        Err(AuthorityStoreError::Cancelled)
    ));
    assert_eq!(publication.request(), &original);
    let members = requests(authority, 4)?;
    let prepared = store
        .contribute_append_group(&mut publication, &members, &[], &mut work)
        .await?;
    drop(prepared);
    drop(publication);
    let mut successor = caller(b"successor")?;
    let prepared = store
        .contribute_append_group(&mut successor, &members, &[], &mut work)
        .await?;
    assert!(matches!(prepared, FilesystemAppendPreparation::Ready(_)));
    drop(prepared);
    drop(successor);
    assert_eq!(
        store.head(authority, &mut work).await?,
        Head::genesis(Epoch::GENESIS)
    );

    // A caller fence is part of the original atomic request: a competing
    // caller publication cannot leave any of the prepared authority prefix.
    let mut loser = caller(b"conditional-loser")?;
    let FilesystemAppendPreparation::Ready(mut prepared) = store
        .contribute_append_group(&mut loser, &members, &[], &mut work)
        .await?
    else {
        return Err("conditional loser failed to prepare".into());
    };
    store.seal(&mut loser, &mut work)?;
    let winner = caller(b"conditional-winner")?;
    assert!(matches!(
        store.provider().commit(winner.request().clone()).await?,
        CommitOutcome::Committed(_)
    ));
    let FilesystemCommitOutcome::Rejected(conflicts) = store.submit(loser, &mut work).await? else {
        return Err("caller fence failed to reject the whole contribution".into());
    };
    let AppendGroupOutcome::Published {
        receipts,
        allocation,
    } = prepared
        .confirm(
            &store,
            &CommitOutcome::Conflict(conflicts.clone()),
            &mut work,
        )
        .await?
    else {
        return Err("conditional result did not retain member outcomes".into());
    };
    assert!(receipts.iter().all(|outcome| matches!(
        outcome,
        FsAppendOutcome::Conflict { actual } if *actual == Head::genesis(Epoch::GENESIS)
    )));
    drop(receipts);
    drop(allocation);
    assert_eq!(
        store.head(authority, &mut work).await?,
        Head::genesis(Epoch::GENESIS)
    );
    assert_eq!(
        store
            .provider()
            .tail(StreamPath::new("caller/observation")?)
            .await?,
        1
    );
    Ok(())
}
