use super::*;
use crate::{
    discovery_verification::{probe, request},
    verification::block_on,
};
use loom::sync::atomic::Ordering;
use std::task::Context;

// Reproduce the former local adapter: finite visibility waits on durability and
// a parked parent watch has no subscription to the provider failure state.
async fn old_children(
    provider: &LocalStream,
    wait_durable: bool,
) -> Result<ChildStream, StreamError> {
    let children = provider.inner.provider.children(request());
    let children = if wait_durable {
        provider.visible(children).await??
    } else {
        provider.accepted(children).await??
    };
    Ok(stream::try_unfold(
        (provider.clone(), children),
        move |(provider, mut children)| async move {
            let child = children.next().await.transpose()?;
            if wait_durable {
                provider.visible(async {}).await?;
            } else {
                provider.accepted(async {}).await?;
            }
            Ok(child.map(|child| (child, (provider, children))))
        },
    )
    .boxed())
}

pub(crate) fn accepted(broken: bool) {
    loom::model(move || {
        let provider = query_model::fixture(false).expect("fixture");
        crate::discovery_verification::append(&provider.inner.provider, "agents/new")
            .expect("accepted state");
        {
            let mut journal = provider.inner.journal.lock().expect("journal");
            // Initial fixture cut: its accepted frame is awaiting device sync.
            journal.appended = 1;
            journal.required = 1;
            journal.syncing = true;
        }
        let (probe, wake) = probe();
        let mut open = Box::pin(async {
            if broken {
                old_children(&provider, true).await
            } else {
                provider.children(request()).await
            }
        });
        let mut context = Context::from_waker(&wake);
        let opened = open.as_mut().poll(&mut context);
        assert!(
            opened.is_ready(),
            "discovery waited for a device acknowledgement"
        );
        let mut children = match opened {
            std::task::Poll::Ready(result) => result.expect("children"),
            std::task::Poll::Pending => unreachable!(),
        };
        assert!(
            children.as_mut().poll_next(&mut context).is_ready(),
            "discovery delivery waited for a device acknowledgement"
        );
        assert_eq!(probe.0.load(Ordering::SeqCst), 0);
    });
}

pub(crate) fn failure(broken: bool) {
    loom::model(move || {
        let provider = query_model::fixture(false).expect("fixture");
        crate::discovery_verification::append(&provider.inner.provider, "agents")
            .expect("parent state");
        let (probe, wake) = probe();
        let polling = loom::thread::spawn({
            let provider = provider.clone();
            let wake = wake.clone();
            move || {
                let children = block_on(async {
                    if broken {
                        old_children(&provider, false).await
                    } else {
                        provider.children(request()).await
                    }
                });
                match children {
                    Ok(mut children) => {
                        let first = children.as_mut().poll_next(&mut Context::from_waker(&wake));
                        (Some(children), first)
                    }
                    Err(error) => (None, std::task::Poll::Ready(Some(Err(error)))),
                }
            }
        });
        provider.inner.poison();
        let (mut children, first) = polling.join().expect("poller");
        let result = if first.is_pending() {
            assert!(
                probe.0.load(Ordering::SeqCst) > 0,
                "provider failure stranded discovery"
            );
            children
                .as_mut()
                .expect("pending children")
                .as_mut()
                .poll_next(&mut Context::from_waker(&wake))
        } else {
            first
        };
        assert!(matches!(
            result,
            std::task::Poll::Ready(Some(Err(StreamError::Unavailable)))
        ));
    });
}
