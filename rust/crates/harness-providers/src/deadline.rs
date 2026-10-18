//! The one owner of every provider exchange's waits. Each send through a resolved model, the
//! model request, its token count and a credential refresh alike, goes through [`Bounded`], so
//! no exchange can wait without a deadline:
//! - a total deadline from the send to the body's end;
//! - an idle deadline on the body, so a silent stream fails even while the total runs.
//!
//! Both read the transport's own timer, which tests drive without wall time, and both fail as a
//! typed [`ModelTransportFailure::Timeout`]. Redelivery then treats a timeout like any other
//! retryable failure: redelivered only while no output has escaped.
use crate::{HttpRequest, HttpResponse, HttpTransport, TimeoutSpec};
use acyclic_harness::{Error, ModelTransportFailure, Result};
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

/// A transport whose every exchange is bounded by `timeouts`.
pub(crate) struct Bounded {
    inner: Arc<dyn HttpTransport>,
    timeouts: TimeoutSpec,
}

impl Bounded {
    pub(crate) fn new(inner: Arc<dyn HttpTransport>, timeouts: TimeoutSpec) -> Arc<Self> {
        Arc::new(Self { inner, timeouts })
    }

    fn idle(&self) -> Duration {
        Duration::from_millis(self.timeouts.idle_ms.into())
    }

    fn total(&self) -> Duration {
        Duration::from_millis(self.timeouts.total_ms.into())
    }

    /// Runs `send` within the total deadline, then hands the rest of it to the body.
    async fn exchange(
        &self,
        send: impl Future<Output = Result<HttpResponse>>,
        native: bool,
    ) -> Result<(HttpResponse, BoxProviderFuture<'static, ()>)> {
        let mut total = self.inner.delay(self.total());
        let mut send = pin!(send);
        let response = futures::future::poll_fn(|context| {
            if let Poll::Ready(result) = send.as_mut().poll(context) {
                return Poll::Ready(result);
            }
            match total.poll_unpin(context) {
                Poll::Ready(()) => Poll::Ready(Err(timeout(native, Expired::Total, self.timeouts))),
                Poll::Pending => Poll::Pending,
            }
        })
        .await?;
        Ok((response, total))
    }

    fn guard(
        &self,
        body: BoxProviderStream<'static, Result<Bytes>>,
        total: BoxProviderFuture<'static, ()>,
        native: bool,
    ) -> Guarded {
        Guarded {
            body,
            transport: self.inner.clone(),
            timeouts: self.timeouts,
            window: self.idle(),
            idle: None,
            total,
            progress: false,
            ended: false,
            native,
            #[cfg(not(target_arch = "wasm32"))]
            _custody: None,
        }
    }
}

impl HttpTransport for Bounded {
    fn send(&self, request: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        Box::pin(async move {
            let (mut response, total) = self.exchange(self.inner.send(request), false).await?;
            response.body = Box::pin(self.guard(response.body, total, false));
            Ok(response)
        })
    }

    fn delay(&self, duration: Duration) -> BoxProviderFuture<'static, ()> {
        self.inner.delay(duration)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn supports_native(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<bool> {
        self.inner.supports_native(work)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn send_with_native_work<'a>(
        &'a self,
        request: HttpRequest,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<acyclic_harness::NativeFutureBundle<'a, HttpResponse>> {
        let send = self.inner.send_with_native_work(request, work)?;
        acyclic_harness::NativeFutureBundle::new(work, || async move {
            let (mut response, total) = self.exchange(send, true).await?;
            // The guard is funded on the original ledger before it exists.
            let custody = work.claim(
                std::mem::size_of::<Guarded>() as u64,
                1,
                acyclic_fs::WorkCounters::UNCHARGED,
            )?;
            let mut guarded = self.guard(response.body, total, true);
            guarded._custody = Some(custody);
            response.body = Box::pin(guarded);
            Ok(response)
        })
    }
}

#[derive(Clone, Copy)]
enum Expired {
    Idle,
    Total,
}

/// The typed timeout of an expired deadline, rendered like every other transport failure.
fn timeout(native: bool, expired: Expired, timeouts: TimeoutSpec) -> Error {
    let cause = match expired {
        Expired::Idle => format!("no response bytes for {} ms", timeouts.idle_ms),
        Expired::Total => format!("exchange exceeded {} ms", timeouts.total_ms),
    };
    crate::transport_failure(native, ModelTransportFailure::Timeout, &cause)
}

/// A response body under both deadlines. One platform timer serves each idle window rather
/// than each chunk: a chunk only marks progress, and a window that expires after progress
/// re-arms. A body silent for two whole windows is therefore always reported, one silent for
/// less than one window never is.
struct Guarded {
    body: BoxProviderStream<'static, Result<Bytes>>,
    transport: Arc<dyn HttpTransport>,
    timeouts: TimeoutSpec,
    window: Duration,
    idle: Option<BoxProviderFuture<'static, ()>>,
    total: BoxProviderFuture<'static, ()>,
    progress: bool,
    ended: bool,
    native: bool,
    /// Native-ledger custody of this guard's own allocation.
    #[cfg(not(target_arch = "wasm32"))]
    _custody: Option<acyclic_fs::NativeAllocation>,
}

impl Guarded {
    fn expire(&mut self, expired: Expired) -> Poll<Option<Result<Bytes>>> {
        self.ended = true;
        self.idle = None;
        Poll::Ready(Some(Err(timeout(self.native, expired, self.timeouts))))
    }
}

impl Stream for Guarded {
    type Item = Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.ended {
            return Poll::Ready(None);
        }
        if let Poll::Ready(item) = this.body.poll_next_unpin(context) {
            this.progress = true;
            if item.is_none() {
                this.ended = true;
                this.idle = None;
            }
            return Poll::Ready(item);
        }
        if this.total.poll_unpin(context).is_ready() {
            return this.expire(Expired::Total);
        }
        loop {
            let (transport, window) = (&this.transport, this.window);
            let idle = this.idle.get_or_insert_with(|| transport.delay(window));
            if idle.poll_unpin(context).is_pending() {
                return Poll::Pending;
            }
            this.idle = None;
            if !std::mem::take(&mut this.progress) {
                return this.expire(Expired::Idle);
            }
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "deadline_tests.rs"]
mod tests;
