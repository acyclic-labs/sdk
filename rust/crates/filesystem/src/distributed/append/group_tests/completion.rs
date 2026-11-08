use super::*;
use tokio::sync::oneshot;

struct CommitBoundary {
    cancel: Option<CancellationToken>,
    accepted: Option<oneshot::Sender<()>>,
    cache_held: Option<oneshot::Receiver<()>>,
    truncate_receipt: bool,
}

impl AuthorityGuard for CommitBoundary {
    type Error = AuthorityStoreError;

    async fn seal(&mut self, _: &mut AuthorityWork<'_>) -> Result<(), AuthorityError<Self::Error>> {
        Ok(())
    }

    async fn native_commit<P: acyclic_stream::NativeStreamProvider>(
        &mut self,
        provider: &P,
        request: acyclic_stream::CommitRequest,
        deadline: Option<u64>,
        work: &mut AuthorityWork<'_>,
    ) -> acyclic_stream::NativeStreamResult<CommitOutcome, AuthorityError<Self::Error>> {
        let mut original_guard = ();
        let mut result = original_guard
            .native_commit(provider, request, deadline, work)
            .await;
        if self.truncate_receipt
            && let Ok(Ok(output)) = &mut result
            && let CommitOutcome::Committed(receipt) = output.value_mut()
        {
            // Corrupt the real provider's range, preserving its original owner.
            // A receipt for only the first member must not settle the group.
            for append in &mut receipt.appends {
                append.tail = append.tail.min(append.start + 1);
            }
        }
        if let Some(cancel) = &self.cancel {
            cancel.cancel();
        }
        if let Some(accepted) = self.accepted.take() {
            accepted
                .send(())
                .map_err(|_| std::io::Error::other("cache holder dropped before publication"))?;
        }
        if let Some(held) = self.cache_held.take() {
            held.await
                .map_err(|_| std::io::Error::other("cache holder lost its original lock"))?;
        }
        result
    }
}

#[tokio::test]
async fn native_committed_group_delivers_original_receipts_after_cancel_or_with_held_cache()
-> TestResult {
    for schedule in 0..3 {
        let hold_cache = schedule == 1;
        let (store, authority) = fixture().await?;
        let requests = requests(authority, 2)?;
        let token = CancellationToken::new();
        let original =
            crate::NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &token)
                .map_err(|(error, _)| error)?;
        let mut work = AuthorityWork::native(&original, &token);
        let actor = store.actor(authority, &mut work)?;
        let (accepted, accepted_rx) = oneshot::channel();
        let (cache_held, cache_held_rx) = oneshot::channel();
        let (release, release_rx) = oneshot::channel::<()>();
        let mut guard = CommitBoundary {
            cancel: (schedule == 0).then(|| token.clone()),
            accepted: hold_cache.then_some(accepted),
            cache_held: hold_cache.then_some(cache_held_rx),
            truncate_receipt: schedule == 2,
        };
        let publication = async {
            let result = store
                .compare_and_append_group(&requests, &[], &mut work, Some(&mut guard))
                .await;
            // Every terminal path retires the holder, including a refused one.
            drop(guard.accepted.take());
            drop(guard.cache_held.take());
            drop(release);
            result
        };
        let holder = async {
            if !hold_cache {
                return Ok(());
            }
            accepted_rx.await?;
            let holder_token = CancellationToken::new();
            let mut holder_work = AuthorityWork::new(WorkBudget::UNBOUNDED, &holder_token);
            let held = actor.lease(&mut holder_work).await?;
            cache_held.send(()).map_err(|_| "publication owner lost")?;
            let _ = release_rx.await;
            drop(held);
            Ok::<_, Box<dyn std::error::Error>>(())
        };
        // Both producers run on this current-thread executor. Delivery must
        // not wait for the cache owner that is waiting for that same delivery.
        let (result, held) = tokio::join!(publication, holder);
        held?;
        let observed = CancellationToken::new();
        let mut observer = AuthorityWork::new(WorkBudget::UNBOUNDED, &observed);
        assert_eq!(
            store.head(authority, &mut observer).await?.sequence.get(),
            2
        );
        if schedule == 2 {
            assert!(matches!(
                result,
                Err(AuthorityError::Authority(
                    AuthorityStoreError::CorruptStatic(
                        "commit envelope omits its authority record"
                    )
                ))
            ));
            continue;
        }
        let AppendGroupOutcome::Published {
            receipts,
            allocation,
        } = result?
        else {
            return Err("accepted native group lost its original receipts".into());
        };
        assert_eq!(receipts.len(), requests.len());
        for (outcome, request) in receipts.iter().zip(&requests) {
            let FsAppendOutcome::Committed(commit) = outcome else {
                return Err("accepted native member was replaced by a refusal".into());
            };
            assert_eq!(commit.operation_id, request.commit.operation_id);
            assert_eq!(commit.previous_digest, request.expected.digest);
            assert_eq!(
                store
                    .find_operation(authority, commit.operation_id, &mut observer)
                    .await?
                    .as_ref(),
                Some(commit)
            );
        }
        assert_eq!(token.is_cancelled(), schedule == 0);
        original
            .receipt()
            .verify(crate::performance::BOUNDARY_BUDGET)?;
        drop(receipts);
        drop(allocation);
    }
    Ok(())
}
