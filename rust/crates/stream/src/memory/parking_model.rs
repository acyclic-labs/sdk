use super::*;
use crate::verification::{block_on, park};
use futures::TryFutureExt as _;
use std::{
    mem::ManuallyDrop,
    task::{Context, Waker},
};

// Lock order: state -> path watch. Stop polling the real returned stream before
// handoff, then require a real append to finish before polling that stream again.
pub(crate) fn verify(retain_pending: bool, checked: bool) {
    loom::model(move || {
        let mut provider = MemoryStream::default();
        provider.retain_pending_read = retain_pending;
        let keeper = ManuallyDrop::new(provider.clone());
        let path = StreamPath::new("watched").unwrap();
        let AppendOutcome::Committed(seed) = block_on(provider.append(AppendRequest {
            path: path.clone(),
            records: vec![Bytes::from_static(b"seed").into()],
            if_tail: None,
            idempotency_key: crate::IdempotencyKey::unique(),
        }))
        .unwrap() else {
            unreachable!()
        };
        let follow = {
            let follow = if checked {
                block_on(
                    provider
                        .follow(crate::FollowRequest {
                            path: path.clone(),
                            from: 1,
                            preceding: Some(seed.commit_id),
                            filter: crate::FollowFilter::default(),
                            progress: false,
                        })
                        .map_ok(crate::records_only),
                )
                .unwrap()
            } else {
                block_on(
                    provider
                        .follow(crate::FollowRequest::new(path.clone(), 0))
                        .map_ok(crate::records_only),
                )
                .unwrap()
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
            records: vec![Bytes::from_static(b"next").into()],
            if_tail: Some(1),
            idempotency_key: crate::IdempotencyKey::unique(),
        }))
        .unwrap();
        assert_eq!(block_on(follow.next()), Some(Ok(())));
        drop(ManuallyDrop::into_inner(follow));
        drop(ManuallyDrop::into_inner(keeper));
    });
}

// Capture tasks at the real snapshot dispatch boundary, then destroy the worker
// future like executor shutdown. Race its drop (and optional consumer drop) with
// releasing the state writer, including fair handoff to a granted read.
pub(crate) fn shutdown(discard: bool, checked: bool, cancel: bool, poll_worker: bool) {
    loom::model(move || {
        let provider = MemoryStream::default();
        let keeper = ManuallyDrop::new(provider.clone());
        let path = StreamPath::new("watched").unwrap();
        let AppendOutcome::Committed(seed) = block_on(provider.append(AppendRequest {
            path: path.clone(),
            records: vec![Bytes::from_static(b"seed").into()],
            if_tail: None,
            idempotency_key: crate::IdempotencyKey::unique(),
        }))
        .unwrap() else {
            unreachable!()
        };
        let follow = {
            let follow = if checked {
                block_on(
                    provider
                        .follow(crate::FollowRequest {
                            path: path.clone(),
                            from: 1,
                            preceding: Some(seed.commit_id),
                            filter: crate::FollowFilter::default(),
                            progress: false,
                        })
                        .map_ok(crate::records_only),
                )
            } else {
                block_on(
                    provider
                        .follow(crate::FollowRequest::new(path.clone(), 0))
                        .map_ok(crate::records_only),
                )
            }
            .unwrap();
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
        crate::snapshot::model::capture(discard);
        assert!(
            follow
                .as_mut()
                .poll_next(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        let mut worker = crate::snapshot::model::take();
        if poll_worker {
            assert!(
                worker
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
        }
        let shutdown = loom::thread::spawn(move || drop(worker));
        let cancelled = if cancel {
            Some(loom::thread::spawn(move || {
                drop(ManuallyDrop::into_inner(follow))
            }))
        } else {
            drop(held);
            park(provider.append(AppendRequest {
                path: path.clone(),
                records: vec![Bytes::from_static(b"next").into()],
                if_tail: Some(1),
                idempotency_key: crate::IdempotencyKey::unique(),
            }))
            .unwrap();
            assert_eq!(park(follow.next()), Some(Ok(())));
            drop(ManuallyDrop::into_inner(follow));
            shutdown.join().unwrap();
            drop(ManuallyDrop::into_inner(keeper));
            return;
        };
        drop(held);
        park(provider.append(AppendRequest {
            path,
            records: vec![Bytes::from_static(b"next").into()],
            if_tail: Some(1),
            idempotency_key: crate::IdempotencyKey::unique(),
        }))
        .unwrap();
        shutdown.join().unwrap();
        cancelled.unwrap().join().unwrap();
        // Only the fixture and its unwind keeper may still own the state.
        assert_eq!(Arc::strong_count(&provider.state), 2);
        drop(ManuallyDrop::into_inner(keeper));
    });
}

// Every consumer departure from a real dispatched memory snapshot job.
pub(crate) fn abandon(
    departure: crate::snapshot::model::Departure,
    mutant: crate::snapshot::model::Mutant,
) {
    loom::model(move || {
        crate::snapshot::model::set_mutant(mutant);
        let provider = MemoryStream::default();
        let keeper = ManuallyDrop::new(provider.clone());
        let path = StreamPath::new("watched").unwrap();
        block_on(provider.append(AppendRequest {
            path: path.clone(),
            records: vec![Bytes::from_static(b"seed").into()],
            if_tail: None,
            idempotency_key: crate::IdempotencyKey::unique(),
        }))
        .unwrap();
        let follow = {
            block_on(
                provider
                    .follow(crate::FollowRequest::new(path, 0))
                    .map_ok(crate::records_only),
            )
            .unwrap()
            .map(|record| record.map(|_| ()))
            .boxed()
        };
        let held = block_on(provider.state.write());
        crate::snapshot::model::depart(
            follow,
            held,
            || provider.state.try_write().is_ok(),
            departure,
        );
        drop(ManuallyDrop::into_inner(keeper));
    });
}
