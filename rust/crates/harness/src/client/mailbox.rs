//! One-slot ownership handoff, with the same locks and wakeups under Loom.
use crate::sync::{Arc, SyncMutex as Mutex};
use std::{
    collections::BTreeMap,
    future::{Future, poll_fn},
    pin::Pin,
    task::{Context, Poll, Waker},
};

struct State<T> {
    value: Option<T>,
    sender_closed: bool,
    receiver_closed: bool,
    next_id: u64,
    senders: BTreeMap<u64, Option<Waker>>,
    receiver: Option<Waker>,
    #[cfg(acyclic_harness_loom)]
    omit_wake: bool,
}
// Lock order: this lock is never nested. No guard crosses await or calls a waker.
pub(crate) struct Sender<T>(Arc<Mutex<State<T>>>);
pub(crate) struct Receiver<T>(Arc<Mutex<State<T>>>);
pub(crate) fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let state = Arc::new(Mutex::new(State {
        value: None,
        sender_closed: false,
        receiver_closed: false,
        next_id: 0,
        senders: BTreeMap::new(),
        receiver: None,
        #[cfg(acyclic_harness_loom)]
        omit_wake: false,
    }));
    (Sender(state.clone()), Receiver(state))
}
fn wake(waker: Option<Waker>) {
    if let Some(waker) = waker {
        waker.wake();
    }
}
impl<T> Sender<T> {
    pub(crate) fn close(&self) {
        let (value, senders, receiver) = {
            let mut state = self.0.lock().expect("mailbox lock");
            state.sender_closed = true;
            (
                state.value.take(),
                std::mem::take(&mut state.senders),
                state.receiver.take(),
            )
        };
        drop(value);
        for (_, sender) in senders {
            wake(sender);
        }
        wake(receiver);
    }
    pub(crate) fn is_closed(&self) -> bool {
        self.0.lock().expect("mailbox lock").receiver_closed
    }
    pub(crate) fn send(&self, value: T) -> Send<'_, T> {
        Send {
            sender: self,
            value: Some(value),
            id: None,
        }
    }
    /// A single-use reply never waits for its recipient to be polled.
    pub(crate) fn reply(self, value: T) -> Result<(), ()> {
        let notify = {
            let mut state = self.0.lock().expect("mailbox lock");
            if state.receiver_closed || state.value.is_some() || !state.senders.is_empty() {
                return Err(());
            }
            state.value = Some(value);
            state.receiver.take()
        };
        wake(notify);
        Ok(())
    }
}
impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let notify = {
            let mut state = self.0.lock().expect("mailbox lock");
            state.sender_closed = true;
            state.receiver.take()
        };
        wake(notify);
    }
}
pub(crate) struct Send<'a, T> {
    sender: &'a Sender<T>,
    value: Option<T>,
    id: Option<u64>,
}
impl<T: Unpin> Future for Send<'_, T> {
    type Output = Result<(), ()>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let caller = cx.waker().clone();
        let sender = self.sender;
        let (result, notify, retired) = {
            let mut state = sender.0.lock().expect("mailbox lock");
            if state.receiver_closed || state.sender_closed {
                return Poll::Ready(Err(()));
            }
            let first = state.senders.first_key_value().map(|(id, _)| *id);
            if state.value.is_none() && (first.is_none() || first == self.id) {
                let retired = self
                    .id
                    .take()
                    .and_then(|id| state.senders.remove(&id))
                    .flatten();
                state.value = self.value.take();
                (Poll::Ready(Ok(())), state.receiver.take(), retired)
            } else {
                let id = match self.id {
                    Some(id) => id,
                    None => {
                        let Some(next) = state.next_id.checked_add(1) else {
                            return Poll::Ready(Err(()));
                        };
                        let id = state.next_id;
                        state.next_id = next;
                        self.id = Some(id);
                        id
                    }
                };
                let retired = state.senders.insert(id, Some(caller)).flatten();
                (Poll::Pending, None, retired)
            }
        };
        drop(retired);
        wake(notify);
        result
    }
}
impl<T> Drop for Send<'_, T> {
    fn drop(&mut self) {
        let Some(id) = self.id else {
            return;
        };
        let (retired, notify) = {
            let mut state = self.sender.0.lock().expect("mailbox lock");
            let retired = state.senders.remove(&id);
            let notify = if state.value.is_none() {
                state
                    .senders
                    .first_entry()
                    .and_then(|mut entry| entry.get_mut().take())
            } else {
                None
            };
            (retired, notify)
        };
        drop(retired);
        wake(notify);
    }
}
impl<T> Receiver<T> {
    fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let caller = cx.waker().clone();
        let (value, notify, retired) = {
            let mut state = self.0.lock().expect("mailbox lock");
            match state.value.take() {
                Some(value) => (
                    Poll::Ready(Some(value)),
                    state
                        .senders
                        .first_entry()
                        .and_then(|mut entry| entry.get_mut().take()),
                    None,
                ),
                None if state.sender_closed => (Poll::Ready(None), None, None),
                None => (Poll::Pending, None, state.receiver.replace(caller)),
            }
        };
        drop(retired);
        #[cfg(acyclic_harness_loom)]
        {
            let omitted = self.0.lock().expect("mailbox lock").omit_wake;
            if !omitted {
                wake(notify);
            }
        }
        #[cfg(not(acyclic_harness_loom))]
        wake(notify);
        value
    }
    pub(crate) async fn recv(&mut self) -> Option<T> {
        poll_fn(|cx| self.poll_recv(cx)).await
    }
}
impl<T> Future for Receiver<T> {
    type Output = Result<T, ()>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.poll_recv(cx).map(|value| value.ok_or(()))
    }
}
impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let (value, senders, receiver) = {
            let mut state = self.0.lock().expect("mailbox lock");
            state.receiver_closed = true;
            (
                state.value.take(),
                std::mem::take(&mut state.senders),
                state.receiver.take(),
            )
        };
        drop(value);
        drop(receiver);
        for (_, sender) in senders {
            wake(sender);
        }
    }
}

#[cfg(acyclic_harness_loom)]
#[path = "mailbox_model.rs"]
mod model;
#[cfg(acyclic_harness_loom)]
pub use model::verify_mailbox;
