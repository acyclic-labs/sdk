//! Original JavaScript host stop ownership, projected into the shared context.

use super::{Context as OperationContext, StopFuture, Stopped};
use futures::channel::oneshot;
use js_sys::{Function, Promise, Reflect};
use std::{
    cell::RefCell,
    future::Future,
    pin::Pin,
    rc::Rc,
    sync::Arc,
    task::{Context, Poll},
};
use wasm_bindgen::{JsCast, prelude::*};
use web_sys::{AbortController, AbortSignal, Event};

/// Retain the original host's stop promise and optional clock projection.
/// An absent clock deliberately leaves the context event-only; no ambient
/// clock or replacement timer is introduced at this boundary.
pub fn from_promise(
    deadline_unix_millis: u64,
    stopped: Promise,
    clock: Option<JsValue>,
) -> Result<OperationContext, JsValue> {
    let stopped = stop(stopped);
    match clock {
        None => Ok(OperationContext::from_stopped(
            deadline_unix_millis,
            stopped,
        )),
        Some(owner) => {
            let method =
                |name: &str| Reflect::get(&owner, &JsValue::from_str(name))?.dyn_into::<Function>();
            let clock = Clock {
                now: method("nowUnixMilliseconds")?,
                expires: method("expiresAt")?,
                owner,
            };
            Ok(OperationContext::from_clock(
                deadline_unix_millis,
                stopped,
                Arc::new(clock),
            ))
        }
    }
}

struct Clock {
    owner: JsValue,
    now: Function,
    expires: Function,
}

impl super::Clock for Clock {
    fn now_unix_millis(&self) -> Result<u64, Stopped> {
        self.now
            .call0(&self.owner)
            .ok()
            .and_then(|value| value.dyn_into::<js_sys::BigInt>().ok())
            .and_then(|value| u64::try_from(value).ok())
            .ok_or(Stopped::Cancelled)
    }

    fn expires_at(&self, deadline: u64) -> Result<StopFuture, Stopped> {
        // Register eagerly with the retained custodian. Dropping this future
        // releases only this registration, never the original invocation.
        let controller = AbortController::new().map_err(|_| Stopped::Cancelled)?;
        let signal = controller.signal();
        let registration = Cancel(Some(controller));
        let promise = self
            .expires
            .call2(&self.owner, &js_sys::BigInt::from(deadline), &signal)
            .map_err(|_| Stopped::Cancelled)?
            .dyn_into::<Promise>()
            .map_err(|_| Stopped::Cancelled)?;
        Ok(Box::pin(async move {
            let _registration = registration;
            stop(promise).await
        }))
    }
}

async fn stop(promise: Promise) -> Stopped {
    match wasm_bindgen_futures::JsFuture::from(promise)
        .await
        .ok()
        .and_then(|value| value.as_string())
        .as_deref()
    {
        Some("deadline") => Stopped::Deadline,
        _ => Stopped::Cancelled,
    }
}

pub(crate) struct Cancel(pub(crate) Option<AbortController>);

impl Drop for Cancel {
    fn drop(&mut self) {
        if let Some(controller) = self.0.take() {
            controller.abort();
        }
    }
}

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
