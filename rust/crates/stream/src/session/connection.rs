//! Conflated connection generations, independent of task record delivery.
use super::sync::{Arc, Mutex};
use crate::StreamError;
use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

/// Push observation of successful physical session handshakes.
#[derive(Clone)]
pub struct Connections(Arc<Mutex<State>>);
struct State {
    generation: Result<u64, StreamError>,
    next_id: u64,
    waiters: BTreeMap<u64, Waker>,
}
impl Connections {
    pub(super) fn new() -> Self {
        Self(Arc::new(Mutex::new(State {
            generation: Ok(0),
            next_id: 0,
            waiters: BTreeMap::new(),
        })))
    }
    pub(super) fn advance(&self) -> Result<(), StreamError> {
        self.publish(true)
    }
    fn publish(&self, wake: bool) -> Result<(), StreamError> {
        // Lock order: never nested with pool, cursor or signal locks.
        // Registration and generation observation are atomic. Wake after unlock.
        let (result, waiters) = {
            let mut state = self.0.lock().map_err(|_| StreamError::Unavailable)?;
            state.generation = state
                .generation
                .clone()
                .and_then(|generation| generation.checked_add(1).ok_or(StreamError::Capacity));
            (
                state.generation.as_ref().map(|_| ()).map_err(Clone::clone),
                std::mem::take(&mut state.waiters),
            )
        };
        if wake {
            dispatch(waiters);
        }
        result
    }
    pub(super) fn close(&self, error: StreamError) {
        let waiters = {
            let mut state = self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.generation.is_ok() {
                state.generation = Err(error);
            }
            std::mem::take(&mut state.waiters)
        };
        dispatch(waiters);
    }
    /// Waits for a newer generation, including a recovery already observed.
    /// Returns the terminal pool error if its original driver ends or is dropped.
    pub async fn changed(&self, after: u64) -> Result<u64, StreamError> {
        Change {
            connections: self,
            after,
            id: None,
        }
        .await
    }
}
fn dispatch(waiters: BTreeMap<u64, Waker>) {
    for waiter in waiters.into_values() {
        waiter.wake();
    }
}
struct Change<'a> {
    connections: &'a Connections,
    after: u64,
    id: Option<u64>,
}
impl Future for Change<'_> {
    type Output = Result<u64, StreamError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let connections = self.connections;
        let waker = cx.waker().clone();
        let mut state = connections.0.lock().map_err(|_| StreamError::Unavailable)?;
        let generation = state.generation.clone()?;
        if generation > self.after {
            return Poll::Ready(Ok(generation));
        }
        let id = if let Some(id) = self.id {
            id
        } else {
            if state.waiters.len() == crate::MAX_ITEMS {
                return Poll::Ready(Err(StreamError::Capacity));
            }
            let id = state.next_id;
            state.next_id = id.checked_add(1).ok_or(StreamError::Capacity)?;
            self.id = Some(id);
            id
        };
        let retired = state.waiters.insert(id, waker);
        drop(state);
        drop(retired);
        Poll::Pending
    }
}
impl Drop for Change<'_> {
    fn drop(&mut self) {
        let retired = self
            .id
            .and_then(|id| self.connections.0.lock().ok()?.waiters.remove(&id));
        drop(retired);
    }
}

#[cfg(loom)]
/// Explores handshake publication concurrent with a waiter parking or cancelling.
pub fn verify_connections(omit_wake: bool) {
    loom::model(move || {
        use futures::task::{ArcWake, waker};
        use loom::sync::atomic::{AtomicUsize, Ordering};
        struct Wake(AtomicUsize);
        impl ArcWake for Wake {
            fn wake_by_ref(value: &std::sync::Arc<Self>) {
                value.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let connections = Connections::new();
        let publishing = connections.clone();
        let wake = std::sync::Arc::new(Wake(AtomicUsize::new(0)));
        let writer = loom::thread::spawn(move || publishing.publish(!omit_wake).unwrap());
        let waker = waker(wake.clone());
        let mut cx = Context::from_waker(&waker);
        let mut change = Box::pin(connections.changed(0));
        let first = change.as_mut().poll(&mut cx);
        let parked = first.is_pending();
        writer.join().unwrap();
        if parked {
            assert!(
                wake.0.load(Ordering::SeqCst) > 0,
                "connection publication lost its wakeup"
            );
        }
        let result = if parked {
            change.as_mut().poll(&mut cx)
        } else {
            first
        };
        assert!(matches!(result, Poll::Ready(Ok(1))));
        drop(change);
        let mut cancelled = Box::pin(connections.changed(1));
        assert!(cancelled.as_mut().poll(&mut cx).is_pending());
        drop(cancelled);
        assert!(connections.0.lock().unwrap().waiters.is_empty());
    });
}

#[cfg(loom)]
/// Explores retirement against actual registration, cancellation and late waits.
pub fn verify_connection_retirement(omit_retirement: bool) {
    loom::model(move || {
        use futures::task::{ArcWake, waker};
        use loom::sync::atomic::{AtomicUsize, Ordering};
        struct Wake(AtomicUsize);
        impl ArcWake for Wake {
            fn wake_by_ref(value: &std::sync::Arc<Self>) {
                value.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let connections = Connections::new();
        let retiring = connections.clone();
        let wake = std::sync::Arc::new(Wake(AtomicUsize::new(0)));
        let waker = waker(wake.clone());
        let mut cx = Context::from_waker(&waker);
        let mut cancelled = Box::pin(connections.changed(0));
        assert!(cancelled.as_mut().poll(&mut cx).is_pending());
        drop(cancelled);
        let closer = loom::thread::spawn(move || {
            if !omit_retirement {
                retiring.close(StreamError::AccessDenied);
            }
        });
        let mut waiting = Box::pin(connections.changed(0));
        let first = waiting.as_mut().poll(&mut cx);
        closer.join().unwrap();
        if first.is_pending() {
            assert!(
                wake.0.load(Ordering::SeqCst) > 0,
                "connection retirement lost its terminal wakeup"
            );
            assert!(matches!(
                waiting.as_mut().poll(&mut cx),
                Poll::Ready(Err(StreamError::AccessDenied))
            ));
        } else {
            assert!(matches!(first, Poll::Ready(Err(StreamError::AccessDenied))));
        }
        drop(waiting);
        connections.close(StreamError::Unavailable);
        assert!(matches!(
            Box::pin(connections.changed(0)).as_mut().poll(&mut cx),
            Poll::Ready(Err(StreamError::AccessDenied))
        ));
        assert_eq!(connections.advance(), Err(StreamError::AccessDenied));
        assert!(connections.0.lock().unwrap().waiters.is_empty());

        // Counter exhaustion is also terminal and must release a parked wait.
        let exhausted = Connections::new();
        exhausted.0.lock().unwrap().generation = Ok(u64::MAX);
        let mut waiting = Box::pin(exhausted.changed(u64::MAX));
        assert!(waiting.as_mut().poll(&mut cx).is_pending());
        wake.0.store(0, Ordering::SeqCst);
        assert_eq!(exhausted.advance(), Err(StreamError::Capacity));
        assert!(
            wake.0.load(Ordering::SeqCst) > 0,
            "connection exhaustion lost its terminal wakeup"
        );
        assert!(matches!(
            waiting.as_mut().poll(&mut cx),
            Poll::Ready(Err(StreamError::Capacity))
        ));
    });
}
