use super::*;
use shuttle::sync::{Arc as ModelArc, Mutex};
use std::{
    future::{Future, poll_fn},
    task::{Poll, Waker},
};

// Pin real filesystem content before entering Shuttle. The client still executes
// its real endpoint validation, wait and commit path.
struct Content {
    file: acyclic_harness::conversation::FileRef,
    bytes: acyclic_stream::OwnedBytes,
}

impl ContentResidencyVerifier for Content {
    fn read_private_path<'a, 'io>(
        &'a self,
        _volume: &'a VolumeRef,
        _prefix: &'a str,
        _path: &'a str,
        _generation: Option<&'a acyclic_harness::resources::GenerationRef>,
        maximum_bytes: Option<u64>,
        access: acyclic_harness::conversation::ContentAccess<'a, 'io>,
    ) -> Result<
        acyclic_harness::conversation::BoundedContentFuture<
            'a,
            Option<(
                acyclic_harness::conversation::FileRef,
                acyclic_harness::conversation::Custody,
                Option<acyclic_harness::conversation::ContentHold<'io>>,
            )>,
        >,
    >
    where
        'io: 'a,
    {
        let acyclic_harness::conversation::ContentAccess { ceiling, io } = access;

        assert!(io.is_none());
        assert!(ceiling.is_none());
        assert!(maximum_bytes.is_none_or(|maximum| self.bytes.len() as u64 <= maximum));
        acyclic_harness::conversation::BoundedContentFuture::new(io, || async {
            // This model reader has no preparation ledger; its bytes are unmetered.
            Ok(Some((
                self.file.clone(),
                acyclic_harness::conversation::Custody::Unmetered(self.bytes.clone()),
                None,
            )))
        })
    }
}

// One consumer and one provider completion; both cancellation choices explore
// all interleavings. Lock order: the provider gate is the sole model lock; it
// must be released before waking the consumer or performing provider I/O.
#[test]
fn follow_has_no_lost_wakeup_or_torn_publication_across_cancellation() -> Result<()> {
    run_model(true)
}

// A missing provider wake must be observable, rather than a model that silently
// runs dependency synchronization without exploring its wait/notify boundary.
#[test]
#[should_panic(expected = "deadlock! blocked tasks")]
fn follow_model_detects_a_dropped_provider_wakeup() {
    let result = run_model(false);
    assert!(result.is_ok(), "model fixture failed: {result:?}");
}

fn run_model(wake: bool) -> Result<()> {
    let (content, publication, original) = futures::executor::block_on(async {
        let fixture = Fixture::new().await?;
        let base = fixture.source("a.txt").await?;
        fixture.put("a.txt", "added\n", "add").await?;
        let current = fixture.source("a.txt").await?;
        let (file, bytes) = current.read(fixture.reader.as_ref(), 1024).await?;
        let view = DiffView::new(fixture.root.volume.clone(), base.generation.clone())?;
        let publication = DiffPublication {
            previous: base.generation,
            generation: current.generation.clone(),
            paths: vec![DiffPath {
                largest_file_bytes: None,
                matches_base: false,
                path: "a.txt".into(),
                base: None,
                current: Some(current),
            }],
        };
        let bytes = bytes.bytes().clone();
        Ok::<_, Error>((Content { file, bytes }, publication, view))
    })?;
    let content = Arc::new(content);
    for cancel in [false, true] {
        let content = content.clone();
        let publication = publication.clone();
        let original = original.clone();
        shuttle::check_dfs(
            move || {
                let gate = ModelArc::new(Mutex::new((false, None::<Waker>)));
                let producer = {
                    let gate = gate.clone();
                    shuttle::thread::spawn(move || {
                        let waker = {
                            let mut state = gate.lock().unwrap_or_else(|error| error.into_inner());
                            state.0 = true;
                            state.1.take()
                        };
                        if let Some(waker) = waker.filter(|_| wake) {
                            waker.wake();
                        }
                    })
                };
                let pause = || -> futures::future::BoxFuture<'static, ()> {
                    let gate = gate.clone();
                    Box::pin(poll_fn(move |cx| {
                        let mut state = gate.lock().unwrap_or_else(|error| error.into_inner());
                        if state.0 {
                            Poll::Ready(())
                        } else {
                            state.1 = Some(cx.waker().clone());
                            Poll::Pending
                        }
                    }))
                };
                let reader = Reader {
                    inner: content.as_ref(),
                    reads: AtomicUsize::new(0),
                    expected_limits: None,
                    pause_first: Some(&pause),
                };
                let mut view = original.clone();
                let source = futures::stream::iter([Ok(publication.clone())]);
                let mut follow = view.follow(Box::pin(source), &reader, limits());
                shuttle::future::block_on(Box::pin(async {
                    let first = if cancel {
                        poll_fn(|cx| {
                            let mut future = Box::pin(follow.next());
                            Poll::Ready(future.as_mut().poll(cx))
                        })
                        .await
                    } else {
                        Poll::Pending
                    };
                    let delivered = match first {
                        Poll::Ready(result) => result,
                        Poll::Pending => {
                            assert_eq!(follow.view(), &original);
                            follow.next().await
                        }
                    };
                    assert!(
                        matches!(&delivered, Some(Ok(paths)) if paths == &["a.txt"]),
                        "expected one diff publication for a.txt: {delivered:?}"
                    );
                    assert_eq!(follow.view().generation(), &publication.generation);
                    assert_eq!(follow.view().line_counts(), (1, 0));
                    assert_eq!(follow.view().files().len(), 1);
                    assert!(follow.next().await.is_none());
                }));
                assert!(producer.join().is_ok(), "provider model thread failed");
            },
            None,
        );
    }
    Ok(())
}
