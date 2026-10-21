//! The one owner of every provider exchange's waits. Each send through a resolved model, the
//! model request, its token count and a credential refresh alike, goes through [`Bounded`], so
//! no exchange can wait without a deadline:
//! - a total deadline from the send to the body's end;
//! - an idle deadline on the body, so a silent stream fails even while the total runs.
//!
//! Both are measured on the transport's monotonic clock ([`HttpTransport::now`]) on every poll.
//! The platform timer is only a wake-up source, never the measure: a late, early or starved
//! timer cannot extend a deadline, because the next poll compares the clock with it. Both fail
//! as a typed [`ModelTransportFailure::Timeout`], which redelivery treats like any retryable
//! failure: redelivered only while no output has escaped.
//!
//! A deadline is the longest tolerated wait: it expires once the clock passes it.
//!
//! [`timer`] is the one place this crate creates a platform timer, deadline wake-ups and retry
//! backoffs alike; on the native ledger it admits the timer before it exists.
//! `scripts/qualify-provider-stream.sh` rejects any other timer construction in the crate.
use crate::{HttpRequest, HttpResponse, HttpTransport, TimeoutSpec};
use acyclic_harness_core::{Error, ModelTransportFailure, Result};
use acyclic_stream::{BoxProviderFuture, BoxProviderStream};
use bytes::Bytes;
use futures::{FutureExt as _, Stream, StreamExt as _};
use std::{
    future::Future,
    pin::{Pin, pin},
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

#[cfg(not(target_arch = "wasm32"))]
type Work = acyclic_fs::NativeMaterializationWork;
/// The portable path has no ledger.
#[cfg(target_arch = "wasm32")]
type Work = ();

/// A transport whose every exchange is bounded by `timeouts`.
pub(crate) struct Bounded {
    inner: Arc<dyn HttpTransport>,
    timeouts: TimeoutSpec,
}

impl Bounded {
    pub(crate) fn new(inner: Arc<dyn HttpTransport>, timeouts: TimeoutSpec) -> Arc<Self> {
        Arc::new(Self { inner, timeouts })
    }

    /// Runs `send` within the total deadline and returns its response with the deadlines the
    /// body inherits.
    async fn exchange(
        &self,
        send: impl Future<Output = Result<HttpResponse>>,
        work: Option<&Work>,
    ) -> Result<(HttpResponse, Deadlines)> {
        let mut deadlines = Deadlines::start(self.inner.clone(), self.timeouts, work.is_some());
        let mut send = pin!(send);
        let response = futures::future::poll_fn(|context| {
            // The deadline is checked before the producer, so readiness never bypasses it.
            if let Some(expired) = deadlines.expired(false) {
                return Poll::Ready(Err(deadlines.timeout(expired)));
            }
            if let Poll::Ready(response) = send.as_mut().poll(context) {
                return Poll::Ready(response);
            }
            match deadlines.poll_wake(context, false, work) {
                Poll::Ready(Ok(expired)) => Poll::Ready(Err(deadlines.timeout(expired))),
                Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
                Poll::Pending => Poll::Pending,
            }
        })
        .await?;
        // Silence is measured from the headers.
        deadlines.progress = deadlines.transport.now();
        deadlines.wake = None;
        Ok((response, deadlines))
    }
}

impl HttpTransport for Bounded {
    fn send(&self, request: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        Box::pin(async move {
            let (mut response, deadlines) = self.exchange(self.inner.send(request), None).await?;
            response.body = Box::pin(Guarded::new(response.body, deadlines));
            Ok(response)
        })
    }

    fn delay(&self, duration: Duration) -> BoxProviderFuture<'static, ()> {
        self.inner.delay(duration)
    }

    fn now(&self) -> Duration {
        self.inner.now()
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn delay_with_native_work(
        &self,
        duration: Duration,
        work: &Work,
    ) -> Result<acyclic_harness_core::NativeFutureBundle<'static, ()>> {
        self.inner.delay_with_native_work(duration, work)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn supports_native(&self, work: &Work) -> Result<bool> {
        self.inner.supports_native(work)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn send_with_native_work<'a>(
        &'a self,
        request: HttpRequest,
        work: &'a Work,
    ) -> Result<acyclic_harness_core::NativeFutureBundle<'a, HttpResponse>> {
        let send = self.inner.send_with_native_work(request, work)?;
        acyclic_harness_core::NativeFutureBundle::new(work, || async move {
            let (mut response, deadlines) = self.exchange(send, Some(work)).await?;
            // The guard is funded on the original ledger before it exists, and its custody
            // funds the body's wake-up timers.
            let custody = work.claim(
                std::mem::size_of::<Guarded>() as u64,
                1,
                acyclic_fs::WorkCounters::UNCHARGED,
            )?;
            let mut guarded = Guarded::new(response.body, deadlines);
            guarded.custody = Some(custody);
            response.body = Box::pin(guarded);
            Ok(response)
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Expired {
    Idle,
    Total,
}

/// The typed timeout of an expired deadline, rendered like every other transport failure.
fn timeout(native: bool, expired: Expired, timeouts: TimeoutSpec) -> Error {
    match expired {
        Expired::Idle => crate::transport_failure(
            native,
            ModelTransportFailure::Timeout,
            format_args!("no response bytes for {} ms", timeouts.idle_ms),
        ),
        Expired::Total => crate::transport_failure(
            native,
            ModelTransportFailure::Timeout,
            format_args!("exchange exceeded {} ms", timeouts.total_ms),
        ),
    }
}

/// A platform timer: portable, or admitted on the native ledger.
pub(crate) enum Wake {
    Portable(BoxProviderFuture<'static, ()>),
    #[cfg(not(target_arch = "wasm32"))]
    Native(acyclic_harness_core::NativeFutureBundle<'static, ()>),
}

impl Future for Wake {
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Result<()>> {
        match self.get_mut() {
            Self::Portable(timer) => timer.poll_unpin(context).map(Ok),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native(timer) => timer.poll_unpin(context),
        }
    }
}

/// Creates the platform timer for `wait`, the crate's only one: admitted on `work`'s ledger
/// before it exists when there is one, else portable.
pub(crate) fn timer(
    transport: &dyn HttpTransport,
    wait: Duration,
    work: Option<&Work>,
) -> Result<Wake> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(work) = work {
        return transport
            .delay_with_native_work(wait, work)
            .map(Wake::Native);
    }
    let _ = work;
    Ok(Wake::Portable(transport.delay(wait)))
}

/// One exchange's deadlines on the transport's clock.
struct Deadlines {
    transport: Arc<dyn HttpTransport>,
    timeouts: TimeoutSpec,
    native: bool,
    /// When the exchange must have ended.
    total: Duration,
    /// The last progress: the headers, then each chunk.
    progress: Duration,
    wake: Option<Wake>,
}

impl Deadlines {
    fn start(transport: Arc<dyn HttpTransport>, timeouts: TimeoutSpec, native: bool) -> Self {
        let now = transport.now();
        Self {
            total: now.saturating_add(Duration::from_millis(timeouts.total_ms.into())),
            progress: now,
            transport,
            timeouts,
            native,
            wake: None,
        }
    }

    fn idle_at(&self) -> Duration {
        self.progress
            .saturating_add(Duration::from_millis(self.timeouts.idle_ms.into()))
    }

    /// The deadline the clock has passed, if any; `idle` only once the body is being read.
    fn expired(&self, idle: bool) -> Option<Expired> {
        let now = self.transport.now();
        if now > self.total {
            Some(Expired::Total)
        } else if idle && now > self.idle_at() {
            Some(Expired::Idle)
        } else {
            None
        }
    }

    fn timeout(&self, expired: Expired) -> Error {
        timeout(self.native, expired, self.timeouts)
    }

    /// Waits for a deadline: wakes on the timer, then measures on the clock. A timer that fires
    /// before the clock reaches the deadline is re-armed for the rest; one that keeps firing
    /// early yields instead of spinning.
    fn poll_wake(
        &mut self,
        context: &mut Context<'_>,
        idle: bool,
        work: Option<&Work>,
    ) -> Poll<Result<Expired>> {
        for _ in 0..2 {
            if let Some(expired) = self.expired(idle) {
                self.wake = None;
                return Poll::Ready(Ok(expired));
            }
            if self.wake.is_none() {
                let due = if idle {
                    self.total.min(self.idle_at())
                } else {
                    self.total
                };
                // Wake past the deadline, since at the deadline itself it has not expired: a
                // whole millisecond past, so a timer or clock of millisecond granularity, which
                // may drop the fraction, still passes it rather than re-arming forever.
                let wait = due.saturating_sub(self.transport.now()) + Duration::from_millis(1);
                self.wake = Some(timer(self.transport.as_ref(), wait, work)?);
            }
            match self.wake.as_mut().map(|wake| wake.poll_unpin(context)) {
                Some(Poll::Pending) | None => return Poll::Pending,
                Some(Poll::Ready(result)) => {
                    self.wake = None;
                    result?;
                }
            }
        }
        context.waker().wake_by_ref();
        Poll::Pending
    }
}

/// A response body under both deadlines.
struct Guarded {
    body: BoxProviderStream<'static, Result<Bytes>>,
    deadlines: Deadlines,
    ended: bool,
    /// Native-ledger custody of this guard's own allocation, whose ledger funds its timers.
    #[cfg(not(target_arch = "wasm32"))]
    custody: Option<acyclic_fs::NativeAllocation>,
}

impl Guarded {
    fn new(body: BoxProviderStream<'static, Result<Bytes>>, deadlines: Deadlines) -> Self {
        Self {
            body,
            deadlines,
            ended: false,
            #[cfg(not(target_arch = "wasm32"))]
            custody: None,
        }
    }

    fn end(&mut self, item: Result<Bytes>) -> Poll<Option<Result<Bytes>>> {
        self.ended = true;
        self.deadlines.wake = None;
        Poll::Ready(Some(item))
    }
}

impl Stream for Guarded {
    type Item = Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.ended {
            return Poll::Ready(None);
        }
        // Measure both deadlines before polling the producer. A late or starved timer must
        // not let a newly ready chunk (or EOF) erase the silence that already expired.
        if let Some(expired) = this.deadlines.expired(true) {
            let error = this.deadlines.timeout(expired);
            return this.end(Err(error));
        }
        if let Poll::Ready(item) = this.body.poll_next_unpin(context) {
            if matches!(&item, Some(Ok(bytes)) if !bytes.is_empty()) {
                this.deadlines.progress = this.deadlines.transport.now();
            }
            if item.is_none() {
                this.ended = true;
                this.deadlines.wake = None;
            }
            return Poll::Ready(item);
        }
        #[cfg(not(target_arch = "wasm32"))]
        let work = this
            .custody
            .as_ref()
            .map(acyclic_fs::NativeAllocation::work);
        #[cfg(target_arch = "wasm32")]
        let work = None;
        match this.deadlines.poll_wake(context, true, work) {
            Poll::Ready(Ok(expired)) => {
                let error = this.deadlines.timeout(expired);
                this.end(Err(error))
            }
            Poll::Ready(Err(error)) => this.end(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
#[path = "deadline_tests.rs"]
mod tests;
