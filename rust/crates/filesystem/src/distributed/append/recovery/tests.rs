use super::*;
use acyclic_stream::StreamProviderExt as _;
use acyclic_stream::{AppendRequest, MemoryStream, StreamProvider};

type TestResult = Result<(), Box<dyn std::error::Error>>;

struct OperationRace {
    store: StreamAuthorityStore<MemoryStream>,
    winners: Vec<crate::GuardedAppend>,
    omit_locator: bool,
    fired: bool,
}

impl AuthorityGuard for OperationRace {
    type Error = AuthorityStoreError;

    async fn seal(&mut self, _: &mut AuthorityWork<'_>) -> Result<(), AuthorityError<Self::Error>> {
        if self.fired {
            return Ok(());
        }
        self.fired = true;
        let token = CancellationToken::new();
        let mut work = AuthorityWork::new(crate::performance::BOUNDARY_BUDGET, &token);
        for (index, winner) in self.winners.iter().enumerate() {
            let Preparation::Ready(mut prepared) = self
                .store
                .prepare_append(
                    winner,
                    &AppendView {
                        cursor: None,
                        projection: None,
                        transaction: None,
                    },
                    &mut work,
                    None::<&mut ()>,
                    Publication,
                )
                .await?
            else {
                return Err(
                    AuthorityStoreError::RejectedStatic("race proposal was rejected").into(),
                );
            };
            if self.omit_locator && index == 1 {
                let path = work.operation_path(
                    &AuthorityPlacement::default(),
                    winner.authority_id,
                    winner.commit.operation_id,
                )?;
                prepared.request.mutations.retain(|mutation| !matches!(mutation, CommitMutation::Append { path: target, .. } if target == &path));
            }
            let result = self
                .store
                .provider()
                .commit(prepared.request)
                .await
                .map_err(AuthorityStoreError::NativeStream)?;
            if !matches!(result, CommitOutcome::Committed(_)) {
                return Err(AuthorityStoreError::RejectedStatic("race commit was rejected").into());
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn ordinary_and_native_receipt_recovery_require_atomic_locators() -> TestResult {
    crate::initialize_test_native_io();
    for native in [false, true] {
        for grouped in [false, true] {
            for omit_locator in [false, true] {
                let (store, authority) = crate::distributed::append::group_tests::fixture().await?;
                let winners = crate::distributed::append::group_tests::requests(authority, 2)?;
                let mut requests = crate::distributed::append::group_tests::requests(
                    authority,
                    if grouped { 2 } else { 1 },
                )?;
                let first = requests.first_mut().ok_or("missing proposal")?;
                first.commit = winners
                    .get(1)
                    .ok_or("missing winning operation")?
                    .commit
                    .clone();
                if let Some(second) = requests.get_mut(1) {
                    // Avoid a duplicate operation inside the losing group.
                    second.commit.operation_id = OperationId::from_bytes([99; 16]);
                }
                let mut head = Head::genesis(Epoch::GENESIS);
                for request in &mut requests {
                    request.expected = head;
                    head.sequence = head.sequence.checked_next()?;
                    head.digest = authority_commit_digest(
                        authority,
                        head.epoch,
                        head.sequence,
                        request.commit.operation_id,
                        request.commit.fingerprint,
                        request.expected.digest,
                        &request.commit.payload,
                    );
                }
                let token = CancellationToken::new();
                let original = crate::NativeMaterializationWork::new(
                    crate::performance::BOUNDARY_BUDGET,
                    &token,
                )
                .map_err(|(error, _)| error)?;
                let mut work = if native {
                    AuthorityWork::native(&original, &token)
                } else {
                    AuthorityWork::new(crate::performance::BOUNDARY_BUDGET, &token)
                };
                let mut race = OperationRace {
                    store: store.clone(),
                    winners,
                    omit_locator,
                    fired: false,
                };
                let outcome = if grouped {
                    store
                        .compare_and_append_group(&requests, &[], &mut work, Some(&mut race))
                        .await
                        .map(|outcome| match outcome {
                            AppendGroupOutcome::Published {
                                mut receipts,
                                allocation,
                            } => {
                                let first = if receipts.is_empty() {
                                    None
                                } else {
                                    Some(receipts.remove(0))
                                };
                                drop(receipts);
                                drop(allocation);
                                first
                            }
                            AppendGroupOutcome::Rejected { outcome, .. } => Some(outcome),
                        })
                } else {
                    store
                        .compare_and_append(requests.remove(0), &mut work, Some(&mut race))
                        .await
                        .map(Some)
                };
                assert!(race.fired);
                if omit_locator {
                    assert!(
                        matches!(
                            outcome,
                            Err(AuthorityError::Authority(
                                AuthorityStoreError::Corrupt(_)
                                    | AuthorityStoreError::CorruptStatic(_)
                            ))
                        ),
                        "missing locator admitted, native={native}, grouped={grouped}"
                    );
                } else {
                    assert!(
                        matches!(outcome, Ok(Some(FsAppendOutcome::AlreadyCommitted(_)))),
                        "valid recovery rejected, native={native}, grouped={grouped}: {outcome:?}"
                    );
                }
                original
                    .receipt()
                    .verify(crate::performance::BOUNDARY_BUDGET)?;
            }
        }
    }
    Ok(())
}

fn reply_cursor(head: Head) {
    assert_eq!(
        head.sequence,
        Sequence::new(6),
        "recovery must retain the rejected append's cursor"
    );
}

async fn history() -> Result<
    (
        StreamAuthorityStore<MemoryStream>,
        AuthorityId,
        AuthoritySnapshot,
    ),
    Box<dyn std::error::Error>,
> {
    crate::initialize_test_native_io();
    let (store, authority) = crate::distributed::append::group_tests::fixture().await?;
    let requests = crate::distributed::append::group_tests::requests(authority, 7)?;
    let token = CancellationToken::new();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &token);
    drop(
        store
            .compare_and_append_group(
                requests.get(..3).ok_or("missing base history")?,
                &[],
                &mut work,
                None::<&mut ()>,
            )
            .await?,
    );
    let anchor = store.fork_snapshot(authority, &mut work).await?;
    drop(
        store
            .compare_and_append_group(
                requests.get(3..).ok_or("missing advanced history")?,
                &[],
                &mut work,
                None::<&mut ()>,
            )
            .await?,
    );
    Ok((store, authority, anchor))
}

#[tokio::test]
async fn conflict_reply_recovers_only_its_missing_prefix_without_head_probes() -> TestResult {
    let (store, authority, anchor) = history().await?;
    let token = CancellationToken::new();
    // The reply observed sequence 6. Sequence 7 was published subsequently and
    // must not change that observation, even though it is already readable.
    let conflicts = [acyclic_stream::CommitConflict::Tail {
        path: stream_path(authority, "/records")?,
        expected: 4,
        actual: Some(7),
    }];
    for native in [false, true] {
        let original =
            crate::NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &token)
                .map_err(|(error, _)| error)?;
        let mut work = if native {
            AuthorityWork::native(&original, &token)
        } else {
            AuthorityWork::new(crate::performance::BOUNDARY_BUDGET, &token)
        };
        let recovered = store
            .recover_conflict_head(authority, anchor, &conflicts, &mut work, |_, _, _| Ok(()))
            .await?;
        reply_cursor(recovered);
        assert_eq!(recovered.epoch, Epoch::GENESIS);
        assert_eq!(
            work.spent().authority_records_read,
            3,
            "recovery re-read the known prefix"
        );
        assert_eq!(
            work.spent().backend_read_operations,
            1,
            "recovery probed an already known tail"
        );
        let mut expected_work = AuthorityWork::new(WorkBudget::UNBOUNDED, &token);
        let (prefix, allocation) = store
            .read_records(
                authority,
                Sequence::new(5),
                ReplayLimit {
                    records: 1,
                    payload_bytes: STREAM_RECORD_LIMIT,
                },
                &mut expected_work,
            )
            .await?;
        assert_eq!(
            recovered.digest,
            prefix.first().ok_or("missing reference record")?.digest
        );
        drop(prefix);
        drop(allocation);
    }
    Ok(())
}

async fn probing_control() -> TestResult {
    let (store, authority, _) = history().await?;
    let token = CancellationToken::new();
    let mut work = AuthorityWork::new(crate::performance::BOUNDARY_BUDGET, &token);
    // This is the actual previous production recovery entry point, using the
    // same store and cursor oracle. No alternative snapshot implementation.
    reply_cursor(store.fork_snapshot(authority, &mut work).await?.head);
    Ok(())
}

#[tokio::test]
#[should_panic(expected = "recovery must retain the rejected append's cursor")]
async fn probing_the_latest_head_negative_control() {
    let result = probing_control().await;
    assert!(
        result.is_ok(),
        "control failed before the cursor oracle: {result:?}"
    );
}

#[tokio::test]
async fn conflict_epoch_uses_its_reply_tail_without_a_probe() -> TestResult {
    let (store, authority, _) = history().await?;
    let token = CancellationToken::new();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &token);
    let anchor = store.fork_snapshot(authority, &mut work).await?;
    let advanced = store.fence(authority, anchor.head, &mut work).await?;
    let crate::FenceOutcome::Advanced(expected) = advanced else {
        return Err("fence failed".into());
    };
    let conflicts = [acyclic_stream::CommitConflict::Tail {
        path: stream_path(authority, "/epochs")?,
        expected: anchor.epoch_tail,
        actual: Some(anchor.epoch_tail + 1),
    }];
    let mut work = AuthorityWork::new(crate::performance::BOUNDARY_BUDGET, &token);
    assert_eq!(
        store
            .recover_conflict_head(authority, anchor, &conflicts, &mut work, |_, _, _| Ok(()))
            .await?,
        expected
    );
    assert_eq!(work.spent().backend_read_operations, 1);
    assert_eq!(work.spent().authority_records_read, 1);
    Ok(())
}

#[tokio::test]
async fn conflict_recovery_rejects_missing_regressed_and_misbound_tails() -> TestResult {
    let (store, authority, anchor) = history().await?;
    let token = CancellationToken::new();
    for (expected, actual) in [
        (4, None),
        (4, Some(3)),
        (4, Some(4)),
        (3, Some(7)),
        (4, Some(99)),
    ] {
        let conflicts = [acyclic_stream::CommitConflict::Tail {
            path: stream_path(authority, "/records")?,
            expected,
            actual,
        }];
        let mut work = AuthorityWork::new(crate::performance::BOUNDARY_BUDGET, &token);
        assert!(
            store
                .recover_conflict_head(authority, anchor, &conflicts, &mut work, |_, _, _| Ok(()))
                .await
                .is_err()
        );
    }
    let conflict = acyclic_stream::CommitConflict::Tail {
        path: stream_path(authority, "/records")?,
        expected: 4,
        actual: Some(7),
    };
    let mut work = AuthorityWork::new(crate::performance::BOUNDARY_BUDGET, &token);
    assert!(
        store
            .recover_conflict_head(
                authority,
                anchor,
                &[conflict.clone(), conflict],
                &mut work,
                |_, _, _| Ok(())
            )
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn conflict_recovery_authenticates_each_link_after_its_base() -> TestResult {
    let (store, authority, _) = history().await?;
    let token = CancellationToken::new();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &token);
    let anchor = store.fork_snapshot(authority, &mut work).await?;
    let operation_id = OperationId::from_bytes([99; 16]);
    let fingerprint = Digest::from_bytes([99; 32]);
    let sequence = Sequence::new(8);
    // Valid object authentication, invalid predecessor: checking only the final
    // record's own hash cannot detect this detached suffix.
    let digest = authority_commit_digest(
        authority,
        anchor.head.epoch,
        sequence,
        operation_id,
        fingerprint,
        Digest::ZERO,
        b"detached",
    );
    let encoded = StreamsDurableRecord::encode(
        &DurableCommit {
            epoch: anchor.head.epoch,
            sequence,
            operation_id,
            fingerprint,
            previous_digest: Digest::ZERO,
            digest,
            payload: Bytes::from_static(b"detached"),
            settled_at_micros: None,
        },
        STREAM_RECORD_LIMIT,
    )?;
    store
        .provider()
        .append(AppendRequest {
            path: stream_path(authority, "/records")?,
            records: vec![acyclic_stream::OwnedBytes::from(encoded.to_vec()).into()],
            if_tail: Some(8),
            idempotency_key: acyclic_stream::IdempotencyKey::unique(),
        })
        .await?;
    let conflicts = [acyclic_stream::CommitConflict::Tail {
        path: stream_path(authority, "/records")?,
        expected: 8,
        actual: Some(9),
    }];
    let mut work = AuthorityWork::new(crate::performance::BOUNDARY_BUDGET, &token);
    let result = store
        .recover_conflict_head(authority, anchor, &conflicts, &mut work, |_, _, _| Ok(()))
        .await;
    assert!(result.is_err(), "detached conflict suffix was accepted");
    Ok(())
}
