use super::*;
use crate::{snapshot::park, verification::block_on};
use std::{
    mem::ManuallyDrop,
    task::{Context, Waker},
};

// Lock order: state -> path watch. Stop polling the real returned stream before
// handoff, then require a real append to finish before polling that stream again.
pub(crate) fn verify(retain_pending: bool, children: bool, checked: bool) {
    loom::model(move || {
        let mut provider = MemoryStream::default();
        provider.retain_pending_read = retain_pending;
        let keeper = ManuallyDrop::new(provider.clone());
        let path = StreamPath::new("watched").unwrap();
        let AppendOutcome::Committed(seed) = block_on(provider.append(AppendRequest {
            path: path.clone(),
            records: vec![Bytes::from_static(b"seed")],
            if_tail: None,
            idempotency_key: None,
        }))
        .unwrap() else {
            unreachable!()
        };
        let follow = if children {
            block_on(provider.children(ChildrenRequest {
                parent: None,
                limit: 1,
            }))
            .unwrap()
            .map(|child| {
                child.map(|child| {
                    assert_eq!(child.path.as_str(), "watched");
                })
            })
            .boxed()
        } else {
            let follow = if checked {
                block_on(provider.follow_checked(path.clone(), 1, seed.commit_id)).unwrap()
            } else {
                block_on(provider.follow(path.clone(), 0)).unwrap()
            };
            follow
                .map(move |record| {
                    record.map(|record| {
                        assert_eq!(record.sequence, u64::from(checked));
                        assert_eq!(
                            record.value.as_slice(),
                            if checked { &b"next"[..] } else { &b"seed"[..] }
                        );
                    })
                })
                .boxed()
        };
        let mut follow = ManuallyDrop::new(follow);
        let held = block_on(provider.state.write());
        assert!(
            follow
                .as_mut()
                .poll_next(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        drop(held);
        park(provider.append(AppendRequest {
            path,
            records: vec![Bytes::from_static(b"next")],
            if_tail: Some(1),
            idempotency_key: None,
        }))
        .unwrap();
        assert_eq!(block_on(follow.next()), Some(Ok(())));
        drop(ManuallyDrop::into_inner(follow));
        drop(ManuallyDrop::into_inner(keeper));
    });
}
