//! Record identity is independent of both the authority digest and its tail.
use super::*;
use acyclic_stream::StreamProvider;
type TestResult = Result<(), Box<dyn std::error::Error>>;
const CHANGED: acyclic_stream::CommitId = acyclic_stream::CommitId::from_bytes([0xa7; 32]);

fn work<'a>(
    native: bool,
    original: &'a crate::NativeMaterializationWork,
    cancel: &'a CancellationToken,
) -> AuthorityWork<'a> {
    if native {
        AuthorityWork::native(original, cancel)
    } else {
        AuthorityWork::new(crate::performance::BOUNDARY_BUDGET, cancel)
    }
}

#[tokio::test]
async fn authority_attempt_keys_bind_the_observed_record_identity() -> TestResult {
    crate::initialize_test_native_io();
    for native in [false, true] {
        let (store, authority) = super::super::group_tests::fixture().await?;
        let members = super::super::group_tests::requests(authority, 1)?;
        let cancel = CancellationToken::new();
        let original =
            crate::NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &cancel)
                .map_err(|(error, _)| error)?;
        let mut work = work(native, &original, &cancel);
        let anchor = store.fork_snapshot(authority, &mut work).await?;
        assert_ne!(anchor.record_commit_id, CHANGED);
        let member = members.first().ok_or("missing member")?;
        let mut keys = Vec::new();
        for record_commit_id in [anchor.record_commit_id, CHANGED] {
            let view = AppendView {
                cursor: Some(AppendCursor::Durable(AuthoritySnapshot {
                    record_commit_id,
                    ..anchor
                })),
                projection: None,
                transaction: None,
            };
            let Preparation::Ready(prepared) = store
                .prepare_append(member, &view, &mut work, None::<&mut ()>, Publication)
                .await?
            else {
                return Err("proposal was rejected before CAS".into());
            };
            keys.push(prepared.request.idempotency_key);
        }
        assert_ne!(
            keys.first(),
            keys.last(),
            "retry key omitted the expected record identity"
        );
    }
    Ok(())
}

#[tokio::test]
async fn authority_batches_reject_a_changed_record_identity_without_publishing() -> TestResult {
    crate::initialize_test_native_io();
    for native in [false, true] {
        for count in [1, 4] {
            let (store, authority) = super::super::group_tests::fixture().await?;
            let members = super::super::group_tests::requests(authority, count)?;
            let cancel = CancellationToken::new();
            let original =
                crate::NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &cancel)
                    .map_err(|(error, _)| error)?;
            let mut work = work(native, &original, &cancel);
            let anchor = store.fork_snapshot(authority, &mut work).await?;
            assert_ne!(anchor.record_commit_id, CHANGED);
            let GroupPreparation::Ready(mut group) = store
                .prepare_group(&members, &mut work, None::<&mut ()>, None)
                .await?
            else {
                return Err("group was rejected before CAS".into());
            };
            let records = work.path(&AuthorityPlacement::default(), authority, "/records", None)?;
            for condition in &mut group.transaction.conditions {
                if let CommitCondition::Head {
                    path, commit_id, ..
                } = condition
                    && path == &records
                {
                    assert_eq!(*commit_id, anchor.record_commit_id);
                    *commit_id = CHANGED;
                }
            }
            let outcome = store.provider().commit(group.transaction).await?;
            assert!(
                matches!(outcome, CommitOutcome::Conflict(ref conflicts) if
                conflicts.iter().any(|conflict| matches!(conflict,
                    acyclic_stream::CommitConflict::Head { path, expected, actual }
                    if path == &records && *expected == CHANGED && *actual == anchor.record_commit_id
                ))),
                "same-tail identity mismatch published an authority batch"
            );
            assert_eq!(
                store.fork_snapshot(authority, &mut work).await?.head,
                anchor.head
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn authority_conflict_recovery_rejects_a_lost_record_identity_without_a_probe() -> TestResult
{
    crate::initialize_test_native_io();
    for native in [false, true] {
        let (store, authority) = super::super::group_tests::fixture().await?;
        let cancel = CancellationToken::new();
        let original =
            crate::NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &cancel)
                .map_err(|(error, _)| error)?;
        let mut work = work(native, &original, &cancel);
        let anchor = store.fork_snapshot(authority, &mut work).await?;
        let conflicts = [acyclic_stream::CommitConflict::Head {
            path: work.path(&AuthorityPlacement::default(), authority, "/records", None)?,
            expected: anchor.record_commit_id,
            actual: CHANGED,
        }];
        let reads = work.spent().backend_read_operations;
        let recovered = store
            .recover_conflict_head(authority, anchor, &conflicts, &mut work, |_, _, _| Ok(()))
            .await;
        assert!(
            matches!(
                recovered,
                Err(AuthorityStoreError::CorruptStatic(_) | AuthorityStoreError::Corrupt(_))
            ),
            "recovery reused an authority cursor after losing its record identity"
        );
        assert_eq!(
            work.spent().backend_read_operations,
            reads,
            "identity rejection performed a head probe"
        );
    }
    Ok(())
}
