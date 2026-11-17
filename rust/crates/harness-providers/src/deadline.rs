//! Caller-selected request deadlines share one event from send through body completion.
//! Requests without a deadline use the original transport directly. Model redelivery
//! backoff and explicit request expiry use the same platform timer adapter.
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

/// Applies the caller-selected deadlines to each request.
pub(crate) struct Bounded {
    inner: Arc<dyn HttpTransport>,
    timeouts: TimeoutSpec,
}

impl Bounded {
    pub(crate) fn new(
        inner: Arc<dyn HttpTransport>,
        timeouts: TimeoutSpec,
    ) -> Arc<dyn HttpTransport> {
        if timeouts.total_ms.is_none() && timeouts.first_byte_ms.is_none() {
            return inner;
        }
        Arc::new(Self { inner, timeouts })
    }

    /// Starts the header deadline once; the response body keeps only the total
    /// deadline, measured from the same send.
    async fn exchange<F: Future<Output = Result<HttpResponse>>>(
        &self,
        send: impl FnOnce() -> Result<F>,
        work: Option<&Work>,
        input_bytes: u64,
    ) -> Result<(HttpResponse, Option<Deadline>)> {
        let now = self.inner.now();
        let first = self.timeouts.first_byte_deadline_ms(input_bytes);
        let total = self.timeouts.total_ms;
        let (due, first_byte, millis) = match (first, total) {
            (Some(first), Some(total)) => {
                let first_due = now.saturating_add(Duration::from_millis(first));
                let total_due = now.saturating_add(Duration::from_millis(total));
                if first_due <= total_due {
                    (Some(first_due), true, first)
                } else {
                    (Some(total_due), false, total)
                }
            }
            (Some(first), None) => (
                Some(now.saturating_add(Duration::from_millis(first))),
                true,
                first,
            ),
            (None, Some(total)) => (
                Some(now.saturating_add(Duration::from_millis(total))),
                false,
                total,
            ),
            (None, None) => (None, false, 0),
        };
        let mut deadline = due.map(|due| Deadline {
            transport: self.inner.clone(),
            due,
            millis,
            first_byte,
            native: work.is_some(),
            wake: None,
        });
        let mut send = pin!(async { send()?.await });
        let response = futures::future::poll_fn(|context| {
            // The deadline is checked before the producer, so readiness never bypasses it.
            if let Some(deadline) = &deadline
                && deadline.expired()
            {
                return Poll::Ready(Err(deadline.timeout()));
            }
            if let Poll::Ready(response) = send.as_mut().poll(context) {
                return Poll::Ready(match &deadline {
                    Some(deadline) if deadline.expired() => Err(deadline.timeout()),
                    _ => response,
                });
            }
            let Some(deadline) = &mut deadline else {
                return Poll::Pending;
            };
            match deadline.poll_wake(context, work) {
                Poll::Ready(Ok(())) => Poll::Ready(Err(deadline.timeout())),
                Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
                Poll::Pending => Poll::Pending,
            }
        })
        .await?;
        let total = if first_byte {
            total.map(|millis| Deadline::start(self.inner.clone(), millis, work.is_some()))
        } else {
            deadline
        };
        Ok((response, total))
    }
}

impl HttpTransport for Bounded {
    fn send(&self, request: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        Box::pin(async move {
            let input_bytes = request.body.len() as u64;
            let (mut response, deadline) = self
                .exchange(|| Ok(self.inner.send(request)), None, input_bytes)
                .await?;
            if let Some(deadline) = deadline {
                response.body = Box::pin(Guarded::new(response.body, deadline));
            }
            Ok(response)
        })
    }

    fn delay(&self, duration: Duration) -> BoxProviderFuture<'static, Result<()>> {
        self.inner.delay(duration)
    }

    fn now(&self) -> Duration {
        self.inner.now()
    }

    fn unix_ms(&self) -> Result<u64> {
        self.inner.unix_ms()
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn delay_with_native_work(
        &self,
        duration: Duration,
        work: &Work,
    ) -> Result<acyclic_fs::NativeFutureBundle<'static, (), acyclic_harness_core::Error>> {
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
    ) -> Result<acyclic_fs::NativeFutureBundle<'a, HttpResponse, acyclic_harness_core::Error>> {
        acyclic_fs::NativeFutureBundle::new(work, || async move {
            let input_bytes = request.body.len() as u64;
            let (mut response, deadline) = self
                .exchange(
                    || self.inner.send_with_native_work(request, work),
                    Some(work),
                    input_bytes,
                )
                .await?;
            if let Some(deadline) = deadline {
                // The guard is funded on the original ledger before it exists, and its custody
                // funds the body's wake-up timers.
                let custody = work.claim(
                    std::mem::size_of::<Guarded>() as u64,
                    1,
                    acyclic_fs_work::performance::WorkCounters::UNCHARGED,
                )?;
                let mut guarded = Guarded::new(response.body, deadline);
                guarded.custody = Some(custody);
                response.body = Box::pin(guarded);
            }
            Ok(response)
        })
    }
}

/// The typed failure of an explicitly selected request deadline.
fn timeout(native: bool, millis: u64, first_byte: bool) -> Error {
    if first_byte {
        acyclic_harness_provider_response::transport_failure(
            native,
            ModelTransportFailure::Timeout,
            format_args!("no response headers within {millis} ms"),
        )
    } else {
        acyclic_harness_provider_response::transport_failure(
            native,
            ModelTransportFailure::Timeout,
            format_args!("exchange exceeded {millis} ms"),
        )
    }
}

/// A platform timer: portable, or admitted on the native ledger.
pub(crate) enum Wake {
    Portable(BoxProviderFuture<'static, Result<()>>),
    #[cfg(not(target_arch = "wasm32"))]
    Native(acyclic_fs::NativeFutureBundle<'static, (), acyclic_harness_core::Error>),
}

impl Future for Wake {
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Result<()>> {
        match self.get_mut() {
            Self::Portable(timer) => timer.poll_unpin(context),
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

/// One request's fixed deadline on the transport's monotonic clock.
struct Deadline {
    transport: Arc<dyn HttpTransport>,
    millis: u64,
    first_byte: bool,
    native: bool,
    due: Duration,
    wake: Option<Wake>,
}

impl Deadline {
    fn start(transport: Arc<dyn HttpTransport>, millis: u64, native: bool) -> Self {
        Self {
            due: transport
                .now()
                .saturating_add(Duration::from_millis(millis)),
            transport,
            millis,
            first_byte: false,
            native,
            wake: None,
        }
    }

    fn expired(&self) -> bool {
        self.transport.now() >= self.due
    }

    fn timeout(&self) -> Error {
        timeout(self.native, self.millis, self.first_byte)
    }

    /// Arms one event, retained across headers and every body chunk. A broken
    /// platform timer returns an error rather than starting a readiness recheck.
    fn poll_wake(&mut self, context: &mut Context<'_>, work: Option<&Work>) -> Poll<Result<()>> {
        let now = self.transport.now();
        if now >= self.due {
            self.wake = None;
            return Poll::Ready(Ok(()));
        }
        let wake = match &mut self.wake {
            Some(wake) => wake,
            slot @ None => slot.insert(timer(
                self.transport.as_ref(),
                self.due.saturating_sub(now),
                work,
            )?),
        };
        match wake.poll_unpin(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                self.wake = None;
                result?;
                if self.expired() {
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Ready(Err(crate::invalid(
                        "request deadline timer completed before expiry",
                    )))
                }
            }
        }
    }
}

/// A response body retaining the request's original deadline event.
struct Guarded {
    body: Option<BoxProviderStream<'static, Result<Bytes>>>,
    deadline: Deadline,
    /// Native-ledger custody of this guard's own allocation, whose ledger funds its timers.
    #[cfg(not(target_arch = "wasm32"))]
    custody: Option<acyclic_fs::NativeAllocation>,
}

impl Guarded {
    fn new(body: BoxProviderStream<'static, Result<Bytes>>, deadline: Deadline) -> Self {
        Self {
            body: Some(body),
            deadline,
            #[cfg(not(target_arch = "wasm32"))]
            custody: None,
        }
    }

    fn end(&mut self, item: Result<Bytes>) -> Poll<Option<Result<Bytes>>> {
        self.body = None;
        self.deadline.wake = None;
        Poll::Ready(Some(item))
    }
}

impl Stream for Guarded {
    type Item = Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.body.is_none() {
            return Poll::Ready(None);
        }
        // The original deadline precedes ready bytes and EOF even if the timer
        // driver has not yet delivered its wake.
        if this.deadline.expired() {
            let error = this.deadline.timeout();
            return this.end(Err(error));
        }
        if let Poll::Ready(item) = this
            .body
            .as_mut()
            .expect("live response body")
            .poll_next_unpin(context)
        {
            if this.deadline.expired() {
                return this.end(Err(this.deadline.timeout()));
            }
            if !matches!(&item, Some(Ok(_))) {
                this.body = None;
                this.deadline.wake = None;
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
        match this.deadline.poll_wake(context, work) {
            Poll::Ready(Ok(())) => {
                let error = this.deadline.timeout();
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
