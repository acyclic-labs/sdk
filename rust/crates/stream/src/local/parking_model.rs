use super::*;
use crate::verification::block_on;
use std::{mem::ManuallyDrop, task::Context};

// The consumer polls exactly once during a publication, then waits for a new
// writer without polling its stream. Every schedule must release the handed-off
// read permit. Lock order is the production publication -> provider -> watches;
// the result watch is published only after the operation releases all locks.
pub(crate) fn verify(retain_pending: bool, children: bool, memory: bool) {
    loom::model(move || {
        let mut provider = query_model::fixture(false).unwrap();
        Arc::get_mut(&mut provider.inner)
            .unwrap()
            .retain_pending_read = retain_pending;
        let keeper = ManuallyDrop::new(provider.clone());
        let path = StreamPath::new("watched").unwrap();
        block_on(provider.inner.provider.append(AppendRequest {
            path: path.clone(),
            records: vec![crate::OwnedBytes::from_static(b"seed").into()],
            if_tail: None,
            idempotency_key: None,
        }))
        .unwrap();
        let follow: futures::stream::BoxStream<'static, _> = if children {
            block_on(provider.children(ChildrenRequest {
                parent: None,
                limit: 1,
            }))
            .unwrap()
            .map(|child| child.map(|_| ()))
            .boxed()
        } else {
            block_on(provider.follow(path, 0, crate::FollowFilter::default()))
                .unwrap()
                .map(|record| record.map(|_| ()))
                .boxed()
        };
        let mut follow = ManuallyDrop::new(follow);
        let held = if memory {
            futures::future::Either::Left(block_on(provider.inner.provider.model_hold_writer()))
        } else {
            futures::future::Either::Right(block_on(provider.inner.publication.write()))
        };
        let (_, wake) = crate::discovery_verification::probe();
        assert!(
            follow
                .as_mut()
                .poll_next(&mut Context::from_waker(&wake))
                .is_pending()
        );
        // The gate reserves the queued reader's permit here even if the
        // consumer never polls again. A non-fair try-lock model misses this.
        drop(held);
        let writer = ManuallyDrop::new(provider.clone());
        let held = follow_model::park(writer.inner.publication.write());
        drop(held);
        // A later reader must also progress behind that writer.
        drop(follow_model::park(writer.inner.publication.read()));
        assert_eq!(block_on(follow.next()), Some(Ok(())));
        drop(ManuallyDrop::into_inner(follow));
        drop(ManuallyDrop::into_inner(writer));
        drop(ManuallyDrop::into_inner(keeper));
    });
}

// Every consumer departure from a real dispatched job on the publication lock.
pub(crate) fn abandon(
    departure: crate::snapshot::model::Departure,
    children: bool,
    mutant: crate::snapshot::model::Mutant,
) {
    loom::model(move || {
        crate::snapshot::model::set_mutant(mutant);
        let provider = query_model::fixture(false).unwrap();
        let keeper = ManuallyDrop::new(provider.clone());
        let path = StreamPath::new("watched").unwrap();
        block_on(provider.inner.provider.append(AppendRequest {
            path: path.clone(),
            records: vec![crate::OwnedBytes::from_static(b"seed").into()],
            if_tail: None,
            idempotency_key: None,
        }))
        .unwrap();
        let follow: futures::stream::BoxStream<'static, _> = if children {
            block_on(provider.children(ChildrenRequest {
                parent: None,
                limit: 1,
            }))
            .unwrap()
            .map(|child| child.map(|_| ()))
            .boxed()
        } else {
            block_on(provider.follow(path, 0, crate::FollowFilter::default()))
                .unwrap()
                .map(|record| record.map(|_| ()))
                .boxed()
        };
        let held = block_on(provider.inner.publication.write());
        crate::snapshot::model::depart(
            follow,
            held,
            || provider.inner.publication.try_write().is_ok(),
            departure,
        );
        drop(ManuallyDrop::into_inner(keeper));
    });
}
