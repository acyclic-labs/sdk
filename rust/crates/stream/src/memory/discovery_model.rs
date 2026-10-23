use super::*;
use crate::verification::block_on;
use loom::{
    sync::atomic::{AtomicUsize, Ordering},
    thread,
};
use std::task::{Context, Poll, Wake, Waker};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub(crate) struct Probe(pub(crate) AtomicUsize);
impl Wake for Probe {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
pub(crate) fn probe() -> (Arc<Probe>, Waker) {
    let probe = Arc::new(Probe(AtomicUsize::new(0)));
    let wake = Waker::from(Arc::clone(&probe));
    (probe, wake)
}
pub(crate) fn request() -> ChildrenRequest {
    ChildrenRequest {
        parent: Some(StreamPath::new("agents").expect("parent")),
        limit: 1,
    }
}
pub(crate) fn append(provider: &MemoryStream, path: &str) -> Result {
    block_on(provider.append(AppendRequest {
        path: StreamPath::new(path)?,
        records: vec![Bytes::from_static(b"child").into()],
        if_tail: None,
        idempotency_key: None,
    }))?;
    Ok(())
}

// Deliberately broken handoff: read first, then subscribe/acknowledge the watch.
// It uses the production index and watch shim, omitting only their atomic cut.
fn late_watch(provider: MemoryStream) -> ChildStream {
    stream::try_unfold(provider, |provider| async move {
        loop {
            let next = provider
                .state
                .read()
                .await
                .next_child(request().parent.as_ref(), None)?;
            if let Some(path) = next {
                return Ok(Some((Child { path }, provider)));
            }
            let mut changed = provider.child_changes(request().parent.as_ref()).await;
            changed.borrow_and_update();
            changed
                .changed()
                .await
                .map_err(|_| StreamError::Unavailable)?;
        }
    })
    .boxed()
}

/// Explores parent creation or switching from the hierarchy watch to its index.
pub fn registration_race(broken: bool, create_parent: bool) {
    loom::model(move || {
        let provider = MemoryStream::default();
        let children = if broken {
            late_watch(provider.clone())
        } else {
            block_on(provider.children(request())).expect("children")
        };
        if !create_parent {
            append(&provider, "agents").expect("parent publication");
        }
        let (probe, wake) = probe();
        let polling = thread::spawn(move || {
            let mut children = children;
            let first = children.as_mut().poll_next(&mut Context::from_waker(&wake));
            (children, first)
        });
        append(&provider, "agents/new").expect("publication");
        let (mut children, first) = polling.join().expect("poller");
        let result = if first.is_pending() {
            assert!(
                probe.0.load(Ordering::SeqCst) > 0,
                "discovery lost a publication wake"
            );
            block_on(children.next())
        } else {
            match first {
                Poll::Ready(value) => value,
                Poll::Pending => unreachable!(),
            }
        };
        assert_eq!(
            result.expect("ended").expect("child").path.as_str(),
            "agents/new"
        );
    });
}

/// Explores cancellation against publication without affecting another observer.
pub fn cancellation_race(broken: bool) {
    loom::model(move || {
        let provider = MemoryStream::default();
        append(&provider, "agents").expect("parent");
        let mut cancelled = block_on(provider.children(request())).expect("cancelled");
        let mut kept = block_on(provider.children(request())).expect("kept");
        let (_, cancelled_wake) = probe();
        let (probe, wake) = probe();
        assert!(
            cancelled
                .as_mut()
                .poll_next(&mut Context::from_waker(&cancelled_wake))
                .is_pending()
        );
        assert!(
            kept.as_mut()
                .poll_next(&mut Context::from_waker(&wake))
                .is_pending()
        );
        let cancelling = thread::spawn({
            let provider = provider.clone();
            move || {
                if broken {
                    // Broken cancellation destroys the shared parent notification.
                    block_on(async {
                        provider
                            .state
                            .write()
                            .await
                            .paths
                            .get_mut(&request().parent.expect("parent"))
                            .expect("parent state")
                            .children
                            .changed = watch::channel(()).0;
                    });
                }
                drop(cancelled);
            }
        });
        append(&provider, "agents/new").expect("publication");
        cancelling.join().expect("canceller");
        assert_eq!(
            block_on(async {
                provider
                    .state
                    .read()
                    .await
                    .child_index(request().parent.as_ref())
                    .expect("index")
                    .changed
                    .receiver_count()
            }),
            1,
            "cancellation destroyed an independent discovery subscription"
        );
        assert!(probe.0.load(Ordering::SeqCst) > 0);
        let result = block_on(kept.next());
        assert!(
            matches!(result, Some(Ok(child)) if child.path.as_str() == "agents/new"),
            "cancellation destroyed an independent discovery subscription"
        );
        drop(kept);
        assert_eq!(
            block_on(async {
                provider
                    .state
                    .read()
                    .await
                    .child_index(request().parent.as_ref())
                    .expect("index")
                    .changed
                    .receiver_count()
            }),
            0
        );
    });
}

/// Verifies accepted local visibility while device durability is outstanding.
pub fn local_accepted(broken: bool) {
    crate::local::discovery_model::accepted(broken);
}
/// Verifies provider failure wakes an already parked child observer.
pub fn local_failure(broken: bool) {
    crate::local::discovery_model::failure(broken);
}
