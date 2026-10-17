//! Race two real, prepared claim publications at the provider IO boundary.
use super::*;
use acyclic_stream::{MemoryStream, StreamPath};
use loom::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

const _: fn(acyclic_stream::query_sync::SyncMutex<()>) -> loom::sync::Mutex<()> = |lock| lock;

fn schedule<const CLAIM: bool>() {
    let path = StreamPath::new("hook-effects-model").unwrap();
    // Seed a real encoded header, then bound the provider topology to one
    // journal. The actual task-volume index, codecs, capture and CAS remain.
    let seed = Arc::new(MemoryStream::default());
    loom::future::block_on(TaskVolume::open(seed.clone(), path.clone())).unwrap();
    let records = loom::future::block_on(async {
        use futures::TryStreamExt;
        seed.read(acyclic_stream::ReadRequest {
            path: path.clone(),
            from: 0,
            limit: 1,
        })
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap()
    });
    let provider = Arc::new(crate::task_volume::model_checking::Journal::new(
        path.clone(),
        records,
    ));
    let volumes: Vec<_> = (0..2)
        .map(|_| {
            Arc::new(
                loom::future::block_on(TaskVolume::open_existing(provider.clone(), path.clone()))
                    .unwrap(),
            )
        })
        .collect();
    let executions = Arc::new(AtomicUsize::new(0));
    let prepared: Vec<_> = volumes
        .into_iter()
        .map(|volume| {
            let executions = Arc::clone(&executions);
            let mut future = Box::pin(async move {
                run_once::<_, _, _, _, _, CLAIM>(
                    &volume,
                    (
                        TaskId::from_bytes([1; 16]),
                        OperationId::from_bytes([2; 16]),
                        "session-start",
                    ),
                    4096,
                    true,
                    || std::future::ready(Ok((Vec::new(), None))),
                    || async {
                        if !CLAIM {
                            // The broken variant skips the claim; park at the
                            // same pre-execution cut, then detect both launches.
                            crate::task_volume::model_checking::publication_ready().await;
                        }
                        executions.fetch_add(1, Ordering::SeqCst);
                        // An uncertain external effect leaves its claim. Its
                        // result cannot change permission to launch a peer.
                        Err(Error::Indeterminate(OperationId::from_bytes([2; 16])))
                    },
                )
                .await
            });
            crate::task_volume::model_checking::pause_next_publication();
            // Capture and preparation run through the production volume. Both
            // contenders have observed absence before either request is sent.
            // Other observations (claim/outcome already visible) are covered
            // by the retained-outcome property. No provider/index is replaced.
            assert!(matches!(
                future
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop())),
                Poll::Pending
            ));
            future
        })
        .collect();
    let workers: Vec<_> = prepared
        .into_iter()
        .map(|future| {
            loom::thread::spawn(move || {
                let result = loom::future::block_on(future);
                assert!(matches!(result, Err(Error::Indeterminate(_))));
            })
        })
        .collect();
    for worker in workers {
        worker.join().unwrap();
    }
    assert_eq!(
        executions.load(Ordering::SeqCst),
        1,
        "hook executed more than once"
    );
}

#[test]
fn competing_dispatches_claim_once_with_negative_control() {
    loom::model(schedule::<true>);
    let failure = std::panic::catch_unwind(|| loom::model(schedule::<false>))
        .expect_err("missing claim escaped the duplicate-dispatch detector");
    let text = failure
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| failure.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        text.contains("hook executed more than once"),
        "unexpected failure: {text}"
    );
}
