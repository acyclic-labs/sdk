//! Original JavaScript host stop ownership, projected into the shared context.

use super::{Context as OperationContext, Stopped};
use futures::channel::oneshot;
use std::{
    cell::RefCell,
    future::Future,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
};
use wasm_bindgen::{JsCast, prelude::*};
use web_sys::{AbortSignal, Event};

/// Bind the original host's absolute deadline and stop signal once. The host
/// must abort that signal on cancellation, deadline or producer loss. A reason
/// of `"deadline"` preserves expiry; all other reasons are redacted cancellation.
/// This registers one listener, starts no timer and supplies no capacity credit.
/// Dropping the last context removes the original registration.
pub fn context(
    deadline_unix_millis: u64,
    signal: AbortSignal,
) -> Result<OperationContext, JsValue> {
    let stopped = listen(signal)?;
    Ok(OperationContext::from_stopped(
        deadline_unix_millis,
        async move { stopped.await.unwrap_or(Stopped::Cancelled) },
    ))
}

// Credential source watches and invocation stops share one drop-owned listener.
pub(crate) fn listen(signal: AbortSignal) -> Result<SignalStop, JsValue> {
    let (send, receive) = oneshot::channel();
    let send = Rc::new(RefCell::new(Some(send)));
    let changed_send = send.clone();
    let stopped = signal.clone();
    let changed = Closure::new(move |_: Event| {
        if let Some(send) = changed_send.borrow_mut().take() {
            let _ = send.send(reason(&stopped));
        }
    });
    signal.add_event_listener_with_callback("abort", changed.as_ref().unchecked_ref())?;
    let registration = SignalStop {
        signal,
        changed,
        receive,
    };
    // Close the registration race and preserve an already-stopped invocation.
    if registration.signal.aborted()
        && let Some(send) = send.borrow_mut().take()
    {
        let _ = send.send(reason(&registration.signal));
    }
    Ok(registration)
}

fn reason(signal: &AbortSignal) -> Stopped {
    if signal.reason().as_string().as_deref() == Some("deadline") {
        Stopped::Deadline
    } else {
        Stopped::Cancelled
    }
}

pub(crate) struct SignalStop {
    signal: AbortSignal,
    changed: Closure<dyn FnMut(Event)>,
    receive: oneshot::Receiver<Stopped>,
}

impl Future for SignalStop {
    type Output = Result<Stopped, oneshot::Canceled>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.receive).poll(cx)
    }
}

impl Drop for SignalStop {
    fn drop(&mut self) {
        let _ = self
            .signal
            .remove_event_listener_with_callback("abort", self.changed.as_ref().unchecked_ref());
    }
}
