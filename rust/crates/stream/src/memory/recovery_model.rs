use super::*;
use futures::StreamExt as _;
use loom::thread;
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

impl UnixMillisClock for loom::sync::atomic::AtomicU64 {
    fn now_unix_millis(&self) -> u64 {
        self.load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// Owns the real async state machine on the heap, outside Loom's coroutine stack.
pub fn block_on<F: std::future::Future>(future: F) -> F::Output {
    loom::future::block_on(Box::pin(future))
}

/// Applies the production recovery path to the existing provider object.
pub async fn recover(
    provider: &MemoryStream,
    replacement: &MemoryStream,
) -> std::result::Result<(), StreamError> {
    provider
        .install_state(&replacement.encode_state().await)
        .await
}

fn seed(provider: &MemoryStream) -> Result<(StreamPath, CommitId)> {
    let path = StreamPath::new("model")?;
    let outcome = block_on(provider.append(AppendRequest {
        path: path.clone(),
        records: vec![Bytes::from_static(b"anchor")],
        if_tail: Some(0),
        idempotency_key: None,
    }))?;
    let AppendOutcome::Committed(receipt) = outcome else {
        return Err("seed conflicted".into());
    };
    Ok((path, receipt.commit_id))
}

/// Checks every schedule; actor and publication counts bound the state.
///
/// # Panics
/// Panics on a violated model invariant.
pub fn check(run: impl Fn() -> Result + Send + Sync + 'static) {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = None;
    builder.max_permutations = None;
    builder.max_duration = None;
    builder.check(move || {
        let result = run();
        assert!(result.is_ok(), "{result:?}");
    });
}

/// Exhausts one producer and checked subscriber through the real provider.
///
/// # Panics
/// Panics on a violated model invariant.
pub fn subscription_and_append_have_no_lost_wakeup() {
    check(|| {
        let provider = MemoryStream::default();
        let (path, anchor) = seed(&provider)?;
        let writer = thread::spawn({
            let provider = provider.clone();
            let path = path.clone();
            move || {
                block_on(provider.append(AppendRequest {
                    path,
                    records: vec![Bytes::from_static(b"next")],
                    if_tail: Some(1),
                    idempotency_key: None,
                }))
            }
        });
        block_on(async {
            let mut stream = provider.follow_checked(path, 1, anchor).await?;
            let record = stream.next().await.ok_or("follow ended")??;
            assert_eq!(record.sequence, 1);
            assert_eq!(record.value.as_slice(), b"next");
            Ok(()) as Result
        })?;
        writer.join().map_err(|_| "writer panicked")??;
        Ok(())
    });
}

/// Exhausts checked subscription against real state replacement.
///
/// # Panics
/// Panics on a violated model invariant.
pub fn subscription_and_recovery_never_wait_for_a_missing_anchor() {
    check(|| {
        let provider = MemoryStream::default();
        let (path, anchor) = seed(&provider)?;
        let empty = block_on(MemoryStream::default().encode_state());
        let recovery = thread::spawn({
            let provider = provider.clone();
            move || block_on(provider.install_state(&empty))
        });
        block_on(async {
            match provider.follow_checked(path, 1, anchor).await {
                Ok(mut stream) => assert!(matches!(
                    stream.next().await,
                    Some(Err(StreamError::NotFound
                        | StreamError::OutOfRange
                        | StreamError::PrefixNotRetained))
                )),
                Err(error) => assert!(matches!(
                    error,
                    StreamError::NotFound
                        | StreamError::OutOfRange
                        | StreamError::PrefixNotRetained
                )),
            }
        });
        recovery.join().map_err(|_| "recovery panicked")??;
        Ok(())
    });
}

/// Exhausts Head publication against real state replacement.
///
/// # Panics
/// Panics on a violated model invariant.
pub fn head_fence_and_recovery_cannot_publish_an_obsolete_generation() {
    check(|| {
        let provider = MemoryStream::default();
        let (path, anchor) = seed(&provider)?;
        let replacement = MemoryStream::default();
        block_on(replacement.append(AppendRequest {
            path: path.clone(),
            records: vec![Bytes::from_static(b"replaced")],
            if_tail: Some(0),
            idempotency_key: None,
        }))?;
        let bytes = block_on(replacement.encode_state());
        let recovery = thread::spawn({
            let provider = provider.clone();
            move || block_on(provider.install_state(&bytes))
        });
        let request = CommitRequest {
            conditions: vec![CommitCondition::Head {
                path: path.clone(),
                expected: 1,
                commit_id: anchor,
            }],
            mutations: vec![CommitMutation::Append {
                path: path.clone(),
                records: vec![Bytes::from_static(b"obsolete")],
            }],
            idempotency_key: IdempotencyKey::new(Bytes::from_static(b"write"))?,
        };
        block_on(provider.commit(request))?;
        recovery.join().map_err(|_| "recovery panicked")??;
        assert_eq!(block_on(provider.tail(path)), Ok(1));
        Ok(())
    });
}

/// Checks local visibility, journal sampling and push follow without a device acknowledgement.
pub fn local_accepted_queries_do_not_wait_for_durability() {
    crate::local::query_model::accepted_queries_and_publication_have_no_lost_wakeup_or_durability_wait();
}

/// Checks that a terminal local failure wakes a subscriber already waiting for data.
pub fn local_poison_wakes_followers() {
    crate::local::query_model::poison_wakes_checked_followers();
}
