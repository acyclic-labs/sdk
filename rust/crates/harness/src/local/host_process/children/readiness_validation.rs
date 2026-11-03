//! Sequential schedules checked against the production queue and registrations.
//! This does not model concurrency in the OS process-event hub.
use super::{ChildWake, Ready};
use std::collections::BTreeSet;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::task::{Context, Wake, Waker};

#[derive(Default)]
struct Notification(AtomicUsize);

impl Wake for Notification {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

/// Each byte polls a waiter, fires a borrowed registration, consumes queued
/// keys, or fires an owned registration. The low bits select the participant.
pub(crate) fn verify(input: &[u8], members: usize) {
    let ready = Arc::new(Ready::default());
    let wakes = (0..members)
        .map(|index| {
            Waker::from(Arc::new(ChildWake {
                key: format!("child-{index}"),
                ready: Arc::clone(&ready),
            }))
        })
        .collect::<Vec<_>>();
    let notifications = [
        Arc::new(Notification::default()),
        Arc::new(Notification::default()),
    ];
    let waiters = notifications
        .each_ref()
        .map(|waiter| Waker::from(Arc::clone(waiter)));
    let mut pending = vec![false; members];
    let mut waiting = None;
    let mut notified = [0_usize; 2];
    let consume = |pending: &mut [bool]| {
        let expected = pending
            .iter_mut()
            .enumerate()
            .filter_map(|(index, pending)| {
                if std::mem::take(pending) {
                    Some(format!("child-{index}"))
                } else {
                    None
                }
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(
            ready.take(),
            expected,
            "native ready queue lost, invented or repeated a child notification"
        );
    };
    for byte in input {
        match byte >> 6 {
            0 => {
                let waiter = usize::from(byte & 1);
                let empty = !pending.iter().any(|pending| *pending);
                assert_eq!(
                    ready
                        .poll(&Context::from_waker(&waiters[waiter]))
                        .is_pending(),
                    empty,
                    "native ready queue parked with unconsumed notifications"
                );
                if empty {
                    waiting = Some(waiter);
                }
            }
            1 | 3 => {
                let member = usize::from(byte & 63) % members;
                pending[member] = true;
                if let Some(waiter) = waiting.take() {
                    notified[waiter] += 1;
                }
                if byte >> 6 == 1 {
                    wakes[member].wake_by_ref();
                } else {
                    wakes[member].clone().wake();
                }
            }
            _ => consume(&mut pending),
        }
        for (waiter, expected) in notifications.iter().zip(notified) {
            assert_eq!(
                waiter.0.load(Ordering::Relaxed),
                expected,
                "fired child registration did not wake the current waiter exactly once"
            );
        }
    }
    consume(&mut pending);
    consume(&mut pending);
}
