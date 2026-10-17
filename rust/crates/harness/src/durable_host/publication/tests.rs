//! The production retry loop against the atomic Streams transaction boundary.
use super::*;
use acyclic_stream::{
    AppendRequest, CommitCondition, CommitOutcome, IdempotencyKey, MemoryStream, ReadRequest,
    StreamProvider,
};
use futures::StreamExt as _;
use proptest::prelude::*;
use std::sync::Arc;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // A retry requires strictly newer coordinator state and no target conflict.
    #[test]
    fn only_coordinator_progress_can_be_retried(
        tail in 0u64..u64::MAX, actual in any::<Option<u64>>(),
        same_path in any::<bool>(), target_changed in any::<bool>(),
    ) {
        let owner = StreamPath::new("owner").unwrap();
        let mut conflicts = vec![CommitConflict::Tail {
            path: StreamPath::new(if same_path {"owner"} else {"journal"}).unwrap(),
            expected: tail, actual,
        }];
        if target_changed {conflicts.push(CommitConflict::Exists {path:StreamPath::new("journal").unwrap()});}
        prop_assert_eq!(advanced(&conflicts, &owner, tail),
            same_path && !target_changed && actual.is_some_and(|actual| actual > tail));
        prop_assert!(!advanced(&[], &owner, tail));
    }
}

#[test]
fn unrelated_coordinator_progress_rechecks_the_original_fence() {
    scenario::<true, true>(false);
    scenario::<true, true>(true);
}

#[test]
#[should_panic(expected = "unrelated progress rejected the journal")]
fn negative_control_detects_missing_coordinator_retry() {
    scenario::<false, true>(false);
}

#[test]
#[should_panic(expected = "stale owner published a journal")]
fn negative_control_detects_dropping_the_fence() {
    scenario::<true, false>(true);
}

fn scenario<const RETRY: bool, const FENCE: bool>(lose_owner: bool) {
    shuttle::check_dfs(
        move || {
            shuttle::future::block_on(async move {
                let stream = Arc::new(MemoryStream::default());
                let owner = StreamPath::new("owner").unwrap();
                let journal = StreamPath::new("journal").unwrap();
                stream
                    .append(AppendRequest {
                        path: owner.clone(),
                        records: vec![b"lease".to_vec().into()],
                        if_tail: None,
                        idempotency_key: None,
                    })
                    .await
                    .unwrap();
                let peer_stream = Arc::clone(&stream);
                let peer_owner = owner.clone();
                let peer = shuttle::future::spawn(async move {
                    shuttle::future::yield_now().await;
                    peer_stream
                        .append(AppendRequest {
                            path: peer_owner,
                            records: vec![if lose_owner {
                                b"lost".to_vec().into()
                            } else {
                                b"lease".to_vec().into()
                            }],
                            if_tail: None,
                            idempotency_key: None,
                        })
                        .await
                        .unwrap();
                });
                let result = retry::<RETRY, _, _, _>((), |()| async {
                    let tail = stream.tail(owner.clone()).await.unwrap();
                    let last = stream
                        .read(ReadRequest {
                            path: owner.clone(),
                            from: tail - 1,
                            limit: 1,
                        })
                        .await
                        .unwrap()
                        .next()
                        .await
                        .unwrap()
                        .unwrap();
                    if last.value.as_ref() == b"lost" {
                        return Err(crate::Error::Unauthorized("owner lost".into()));
                    }
                    shuttle::future::yield_now().await;
                    let mut work = None;
                    let request = crate::distributed::journal_request::<FENCE>(
                        IdempotencyKey::new(tail.to_le_bytes().to_vec()).unwrap(),
                        0,
                        CommitCondition::Tail {
                            path: owner.clone(),
                            expected: tail,
                        },
                        CommitCondition::Absent {
                            path: journal.clone(),
                        },
                        journal.clone(),
                        b"result".to_vec().into(),
                        &mut work,
                    )
                    .unwrap();
                    // No yield inside the provider's atomic transaction boundary.
                    let current = stream.tail(owner.clone()).await.unwrap();
                    let outcome = stream.commit(request.0).await.unwrap();
                    assert!(
                        !(lose_owner
                            && current > tail
                            && matches!(outcome, CommitOutcome::Committed(_))),
                        "stale owner published a journal"
                    );
                    match outcome {
                        CommitOutcome::Committed(_) => Ok((Some(true), ())),
                        CommitOutcome::Conflict(conflicts)
                            if advanced(&conflicts, &owner, tail) =>
                        {
                            Ok((None, ()))
                        }
                        CommitOutcome::Conflict(_) => Ok((Some(false), ())),
                    }
                })
                .await;
                peer.await.unwrap();
                if lose_owner {
                    assert!(matches!(
                        result,
                        Ok(true) | Err(crate::Error::Unauthorized(_))
                    ));
                } else {
                    assert!(
                        matches!(result, Ok(true)),
                        "unrelated progress rejected the journal"
                    );
                }
            })
        },
        None,
    );
}
