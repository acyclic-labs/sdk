use super::*;
use acyclic_stream::MemoryStream;

type TestResult = Result<(), Box<dyn std::error::Error>>;

pub(in crate::distributed) fn requests(
    authority: AuthorityId,
    count: u8,
) -> Result<Vec<crate::GuardedAppend>, AuthorityStoreError> {
    let mut expected = Head::genesis(Epoch::GENESIS);
    let mut output = Vec::new();
    for id in 1..=count {
        let commit = ProposedCommit {
            operation_id: OperationId::from_bytes([id; 16]),
            fingerprint: Digest::from_bytes([id; 32]),
            payload: Bytes::from_static(b"opaque non-generation operation"),
            publication: None,
        };
        let sequence = expected
            .sequence
            .checked_next()
            .map_err(|_| AuthorityStoreError::SequenceExhausted)?;
        let digest = authority_commit_digest(
            authority,
            expected.epoch,
            sequence,
            commit.operation_id,
            commit.fingerprint,
            expected.digest,
            &commit.payload,
        );
        output.push(crate::GuardedAppend {
            authority_id: authority,
            epoch: expected.epoch,
            expected,
            commit,
            permit: PublicationPermit::Unrestricted,
        });
        expected = Head {
            epoch: expected.epoch,
            sequence,
            digest,
        };
    }
    Ok(output)
}

pub(in crate::distributed) async fn fixture()
-> Result<(StreamAuthorityStore<MemoryStream>, AuthorityId), Box<dyn std::error::Error>> {
    let store = StreamAuthorityStore::new(Arc::new(MemoryStream::default()));
    let authority = AuthorityId::from_bytes([33; 16]);
    let token = CancellationToken::new();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &token);
    store
        .create_authority(authority, Epoch::GENESIS, &mut work, None::<&mut ()>)
        .await?;
    Ok((store, authority))
}

#[tokio::test]
async fn atomic_group_has_one_append_and_distinct_recoverable_receipts() -> TestResult {
    let (store, authority) = fixture().await?;
    let requests = requests(authority, 4)?;
    let token = CancellationToken::new();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &token);
    let AppendGroupOutcome::Published {
        receipts,
        allocation,
    } = store
        .compare_and_append_group(&requests, &[], &mut work, None::<&mut ()>)
        .await?
    else {
        return Err("group was rejected".into());
    };
    assert_eq!(
        work.spent().backend_write_operations,
        1,
        "group used more than one stream transaction"
    );
    assert_eq!(receipts.len(), requests.len());
    for (receipt, request) in receipts.iter().zip(&requests) {
        let FsAppendOutcome::Committed(commit) = receipt else {
            return Err("member was not committed".into());
        };
        assert_eq!(commit.operation_id, request.commit.operation_id);
        assert_eq!(commit.previous_digest, request.expected.digest);
        let recovered = store
            .find_operation(authority, commit.operation_id, &mut work)
            .await?
            .ok_or("operation locator was not published atomically")?;
        assert_eq!(&recovered, commit);
    }
    drop(receipts);
    drop(allocation);
    // A lost group acknowledgement recovers an individual operation identity;
    // it cannot publish the same batch twice.
    let mut retry = AuthorityWork::new(WorkBudget::UNBOUNDED, &token);
    assert!(matches!(
        store
            .compare_and_append_group(&requests, &[], &mut retry, None::<&mut ()>)
            .await?,
        AppendGroupOutcome::Rejected {
            index: 0,
            outcome: FsAppendOutcome::AlreadyCommitted(_),
            ..
        }
    ));
    assert_eq!(retry.spent().backend_write_operations, 0);
    assert_eq!(store.head(authority, &mut work).await?.sequence.get(), 4);
    Ok(())
}

#[tokio::test]
async fn rejected_group_member_publishes_no_prefix() -> TestResult {
    let token = CancellationToken::new();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &token);
    check_rejected_prefix(&mut work).await
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_rejected_group_member_publishes_no_prefix() -> TestResult {
    crate::initialize_test_native_io();
    let token = CancellationToken::new();
    let original =
        crate::NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &token)
            .map_err(|(error, _)| error)?;
    let mut work = AuthorityWork::native(&original, &token);
    check_rejected_prefix(&mut work).await
}

async fn check_rejected_prefix(work: &mut AuthorityWork<'_>) -> TestResult {
    let (store, authority) = fixture().await?;
    let mut requests = requests(authority, 4)?;
    requests
        .get_mut(2)
        .ok_or("missing third member")?
        .expected
        .digest = Digest::ZERO;
    assert!(matches!(
        store
            .compare_and_append_group(&requests, &[], work, None::<&mut ()>)
            .await?,
        AppendGroupOutcome::Rejected {
            index: 2,
            outcome: FsAppendOutcome::Conflict { actual },
            observed,
        } if observed == Head::genesis(Epoch::GENESIS) && actual == observed
    ));
    assert_eq!(work.spent().backend_write_operations, 0);
    assert_eq!(
        store.head(authority, work).await?,
        Head::genesis(Epoch::GENESIS)
    );
    for request in &requests {
        assert!(
            store
                .find_operation(authority, request.commit.operation_id, work)
                .await?
                .is_none()
        );
    }
    Ok(())
}

#[tokio::test]
async fn duplicate_group_operation_is_rejected_before_publication() -> TestResult {
    let (store, authority) = fixture().await?;
    let mut requests = requests(authority, 2)?;
    let first = requests
        .first()
        .ok_or("missing first member")?
        .commit
        .operation_id;
    requests
        .get_mut(1)
        .ok_or("missing second member")?
        .commit
        .operation_id = first;
    let token = CancellationToken::new();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &token);
    assert!(
        store
            .compare_and_append_group(&requests, &[], &mut work, None::<&mut ()>)
            .await
            .is_err()
    );
    assert_eq!(work.spent().backend_write_operations, 0);
    assert_eq!(
        store.head(authority, &mut work).await?,
        Head::genesis(Epoch::GENESIS)
    );
    Ok(())
}

struct FenceAtPublication<'a> {
    store: &'a StreamAuthorityStore<MemoryStream>,
    authority: AuthorityId,
    head: Head,
}

impl AuthorityGuard for FenceAtPublication<'_> {
    type Error = AuthorityStoreError;
    async fn seal(
        &mut self,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), AuthorityError<Self::Error>> {
        self.store.fence(self.authority, self.head, work).await?;
        Ok(())
    }
}

#[tokio::test]
async fn group_writer_epoch_is_fenced_at_the_actual_atomic_append() -> TestResult {
    let (store, authority) = fixture().await?;
    let requests = requests(authority, 4)?;
    let token = CancellationToken::new();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &token);
    let mut guard = FenceAtPublication {
        store: &store,
        authority,
        head: Head::genesis(Epoch::GENESIS),
    };
    let AppendGroupOutcome::Published {
        receipts,
        allocation,
    } = store
        .compare_and_append_group(&requests, &[], &mut work, Some(&mut guard))
        .await?
    else {
        return Err("fence ran before publication preparation".into());
    };
    assert!(
        receipts
            .iter()
            .all(|outcome| matches!(outcome, FsAppendOutcome::Fenced { .. })),
        "a member escaped the atomic epoch fence"
    );
    drop(receipts);
    drop(allocation);
    let head = store.head(authority, &mut work).await?;
    assert_ne!(head.epoch, Epoch::GENESIS);
    assert_eq!(head.sequence, Sequence::GENESIS);
    for request in &requests {
        assert!(
            store
                .find_operation(authority, request.commit.operation_id, &mut work)
                .await?
                .is_none()
        );
    }
    Ok(())
}

#[tokio::test]
async fn group_checks_every_members_policy_cut_and_the_active_scope() -> TestResult {
    for active_scope in [false, true] {
        let (store, authority) = fixture().await?;
        let token = CancellationToken::new();
        let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &token);
        let source_a = AuthorityId::from_bytes([44; 16]);
        let source_b = AuthorityId::from_bytes([45; 16]);
        for source in [source_a, source_b] {
            store
                .create_authority(source, Epoch::GENESIS, &mut work, None::<&mut ()>)
                .await?;
        }
        let cut_a = <StreamAuthorityStore<MemoryStream> as AsyncAuthorityStore>::policy_cut(
            &store, source_a, &mut work,
        )
        .await?;
        let cut_b = <StreamAuthorityStore<MemoryStream> as AsyncAuthorityStore>::policy_cut(
            &store, source_b, &mut work,
        )
        .await?;
        let changed_source = requests(source_b, 1)?;
        store
            .compare_and_append(
                changed_source
                    .into_iter()
                    .next()
                    .ok_or("missing source edit")?,
                &mut work,
                None::<&mut ()>,
            )
            .await?;
        let members = requests(authority, 2)?;
        let outcome = if active_scope {
            cut_b
                .scope(store.compare_and_append_group(&members, &[], &mut work, None::<&mut ()>))
                .await?
        } else {
            store
                .compare_and_append_group(&members, &[cut_a, cut_b], &mut work, None::<&mut ()>)
                .await?
        };
        drop(outcome);
        assert_eq!(
            store.head(authority, &mut work).await?.sequence,
            Sequence::GENESIS,
            "a stale member policy cut escaped the shared publication transaction"
        );
        for member in members {
            assert!(
                store
                    .find_operation(authority, member.commit.operation_id, &mut work)
                    .await?
                    .is_none()
            );
        }
    }
    Ok(())
}
