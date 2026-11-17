//! One admission per model at each provider endpoint, shared by one registry.
//!
//! A 429 with a provider delay closes that model's bucket until exactly that
//! instant. When it opens, the waiters that accumulated during the cooldown are
//! admitted one per share of the provider's window, so a shared refusal never
//! releases a herd into the first second of the next window. Request bytes are
//! never changed.
use crate::{HttpRequest, HttpResponse, HttpTransport, ResolvedModel};
use acyclic_harness_core::{
    Result,
    model::{ModelAdmissionCause, ModelWait},
};
use futures::{FutureExt as _, StreamExt as _};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    task::{Poll, Waker},
    time::Duration,
};

/// The model gates of one registry, keyed by provider, base URL, model and credential.
/// A bucket retains its gate while anyone holds it or its cooldown is pending,
/// so the cooldown outlives its permits.
#[derive(Default)]
pub(crate) struct Endpoints(Mutex<HashMap<[u8; 32], Arc<Gate>>>);

impl Endpoints {
    pub(crate) fn gate(&self, provider: &ResolvedModel) -> Arc<Gate> {
        let mut identity = blake3::Hasher::new();
        identity.update(b"acyclic.provider-admission.v1\0");
        for part in [
            provider.model.provider.as_str(),
            &provider.config.base_url,
            provider.model.name.as_str(),
        ] {
            identity.update(&(part.len() as u64).to_le_bytes());
            identity.update(part.as_bytes());
        }
        // Only a digest of the credential is retained.
        match (provider.credential_scope, provider.key.as_deref()) {
            (Some(scope), _) => identity.update(&[1]).update(&scope),
            (None, Some(key)) => identity
                .update(&[2])
                .update(blake3::hash(key.as_bytes()).as_bytes()),
            (None, None) => identity.update(&[0]),
        };
        let identity = *identity.finalize().as_bytes();
        let mut gates = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(gate) = gates.get(&identity) {
            return gate.clone();
        }
        let now = provider.transport.now();
        gates.retain(|_, gate| {
            Arc::strong_count(gate) > 1 || gate.lock().closed_until.is_some_and(|until| until > now)
        });
        let gate = Arc::new(Gate::default());
        gates.insert(identity, gate.clone());
        gate
    }
}

/// Original gate observations followed by the value they admitted.
pub(crate) enum Event<T> {
    Wait(ModelWait),
    Trace(acyclic_stream::trace::TraceRecord),
    Ready(T),
}

/// Sends one model exchange through its endpoint's admission. A successful response
/// leaves its permit in `held` until the caller finishes reading it. A refusal with a
/// provider delay leaves an opener there instead: the caller's next send follows its
/// own wait for that same delay, which is the event that reopens the endpoint.
pub(crate) fn send<'a>(
    provider: &'a ResolvedModel,
    request: HttpRequest,
    work: acyclic_fs::NativeWork<'a>,
    held: &'a mut Option<Permit>,
) -> impl futures::Stream<Item = Result<Event<HttpResponse>>> + acyclic_stream::ProviderTask + 'a {
    async_stream::try_stream! {
        if let Some(permit) = held.take() { permit.waited(provider.transport.now()); }
        let transport = provider.transport.as_ref();
        let admission = provider.admission.gate(provider).admit(transport, work);
        let mut admission = core::pin::pin!(admission);
        let mut permit = loop {
            match admission.next().await {
                Some(Ok(Event::Wait(wait))) => yield Event::Wait(wait),
                Some(Ok(Event::Trace(record))) => yield Event::Trace(record),
                Some(Ok(Event::Ready(permit))) => break permit,
                Some(Err(error)) => Err(error)?,
                None => Err(crate::invalid("endpoint admission ended without a permit"))?,
            }
        };
        #[cfg(not(target_arch = "wasm32"))]
        let trace_custody = work.map(|work| work.claim(
            (crate::http::phase::NAME.len() + crate::http::phase::GUARANTEE.len()) as u64,
            2, acyclic_fs_work::performance::WorkCounters::UNCHARGED,
        )).transpose()?;
        let phase = crate::http::phase::HeadersPhase::new();
        if let Some(phase) = &phase { yield Event::Trace(phase.started()); }
        #[cfg(not(target_arch = "wasm32"))]
        drop(trace_custody);
        let response = crate::http::send(transport, request, work)?.await?;
        let finished = phase.as_ref().and_then(crate::http::phase::HeadersPhase::finished);
        if permit.observe(response.status, response.retry_after, transport.now()) {
            *held = Some(permit);
        }
        if let Some(finished) = finished { yield Event::Trace(finished); }
        yield Event::Ready(response);
    }
}

#[derive(Default)]
pub(crate) struct Gate(Mutex<State>);

#[derive(Default)]
struct State {
    /// The provider's instant on the transport clock, while closed.
    closed_until: Option<Duration>,
    /// The provider's last delay, which paces the waiters it accumulated.
    window: Option<Duration>,
    /// One admission per interval while the cooldown's waiters drain.
    pace: Option<Pace>,
    next_ticket: u64,
    /// Waiters in arrival (ticket) order.
    waiters: VecDeque<(u64, Waiter)>,
}

struct Pace {
    next: Duration,
    interval: Duration,
}

struct Waiter {
    admitted: bool,
    waker: Option<Waker>,
}

/// Funds a queued waiter's node: the queue never holds more than four slots per
/// waiter, because it shrinks to half its capacity whenever a quarter is in use.
#[cfg(not(target_arch = "wasm32"))]
const QUEUED_BYTES: u64 = 4 * std::mem::size_of::<(u64, Waiter)>() as u64;

impl State {
    fn position(&self, ticket: u64) -> Option<usize> {
        self.waiters
            .binary_search_by_key(&ticket, |(ticket, _)| *ticket)
            .ok()
    }

    fn remove(&mut self, ticket: u64) -> Option<Waiter> {
        let waiter = self.waiters.remove(self.position(ticket)?)?.1;
        if self.waiters.is_empty() {
            self.pace = None;
            if self.closed_until.is_none() {
                self.window = None;
            }
        }
        if self.waiters.len() * 4 <= self.waiters.capacity() {
            self.waiters.shrink_to(self.waiters.len() * 2);
        }
        Some(waiter)
    }

    /// An admission is valid only in the gate state that granted it: closing
    /// revokes grants whose waiter has not resumed, and every
    /// waiter re-evaluates (arming the provider's instant when closed).
    fn revoke(&mut self) {
        self.pace = None;
        for (_, waiter) in &mut self.waiters {
            waiter.admitted = false;
            if let Some(waker) = waiter.waker.take() {
                waker.wake();
            }
        }
    }

    fn cause(&self) -> Option<ModelAdmissionCause> {
        self.closed_until
            .map(|until| ModelAdmissionCause::Cooldown { until })
            .or_else(|| self.pace.as_ref().map(|_| ModelAdmissionCause::Credit))
    }

    /// Opens the gate at `now`, admitting at most one waiter per pacing interval.
    /// Returns the next instant this gate needs a wake-up, if any.
    fn open(&mut self, now: Duration) -> Option<Duration> {
        if let Some(until) = self.closed_until {
            if until > now {
                return Some(until);
            }
            self.closed_until = None;
            let pending = self
                .waiters
                .iter()
                .filter(|(_, waiter)| !waiter.admitted)
                .count();
            if pending > 1 {
                let interval =
                    self.window.unwrap_or_default() / u32::try_from(pending).unwrap_or(u32::MAX);
                if !interval.is_zero() {
                    self.pace = Some(Pace {
                        next: now,
                        interval,
                    });
                }
            }
        }
        if let Some(pace) = &mut self.pace {
            if now < pace.next {
                return Some(pace.next);
            }
            if let Some((_, waiter)) = self.waiters.iter_mut().find(|(_, waiter)| !waiter.admitted)
            {
                waiter.admitted = true;
                if let Some(waker) = waiter.waker.take() {
                    waker.wake();
                }
                if self.waiters.iter().all(|(_, waiter)| waiter.admitted) {
                    self.pace = None;
                    self.window = None;
                    return None;
                }
                pace.next = now.saturating_add(pace.interval);
                return Some(pace.next);
            }
            self.pace = None;
            self.window = None;
            return None;
        }
        for (_, waiter) in self
            .waiters
            .iter_mut()
            .filter(|(_, waiter)| !waiter.admitted)
        {
            waiter.admitted = true;
            if let Some(waker) = waiter.waker.take() {
                waker.wake();
            }
        }
        None
    }
}

impl Gate {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// How long this bucket withholds admission at `now`, if it does.
    pub(crate) fn saturation(&self, now: Duration) -> Option<Duration> {
        let state = self.lock();
        let closed = state
            .closed_until
            .filter(|until| *until > now)
            .map(|until| until.saturating_sub(now));
        let paced = state
            .pace
            .as_ref()
            .map(|pace| pace.next.saturating_sub(now));
        match (closed, paced) {
            (Some(closed), Some(paced)) => Some(closed.max(paced)),
            (closed, paced) => closed.or(paced),
        }
    }

    /// Queue funding is synchronous: an admission failure must release the
    /// state lock before the event stream can publish that failure.
    fn enqueue(
        &self,
        transport: &dyn HttpTransport,
        work: acyclic_fs::NativeWork<'_>,
    ) -> Result<
        Option<(
            u64,
            ModelAdmissionCause,
            Duration,
            acyclic_fs::NativeCustody,
        )>,
    > {
        let mut state = self.lock();
        state.open(transport.now());
        let Some(cause) = state.cause() else {
            return Ok(None);
        };
        #[cfg(not(target_arch = "wasm32"))]
        let custody = work
            .map(|work| {
                work.claim(
                    QUEUED_BYTES,
                    1,
                    acyclic_fs_work::performance::WorkCounters::UNCHARGED,
                )
            })
            .transpose()?;
        #[cfg(target_arch = "wasm32")]
        let custody = {
            let _ = work;
            None
        };
        let ticket = state.next_ticket;
        state.next_ticket += 1;
        let started = transport.now();
        state.waiters.push_back((
            ticket,
            Waiter {
                admitted: false,
                waker: None,
            },
        ));
        Ok(Some((ticket, cause, started, custody)))
    }

    fn admit<'a>(
        self: Arc<Self>,
        transport: &'a dyn HttpTransport,
        work: acyclic_fs::NativeWork<'a>,
    ) -> impl futures::Stream<Item = Result<Event<Permit>>> + acyclic_stream::ProviderTask + 'a
    {
        async_stream::try_stream! {
            let Some((ticket, mut cause, started, _custody)) = self.enqueue(transport, work)? else {
                yield Event::Ready(Permit::new(self));
                return;
            };
            let queued = Queued {
                gate: &self,
                ticket,
                #[cfg(not(target_arch = "wasm32"))]
                _custody,
            };
            yield Event::Wait(ModelWait::AdmissionStarted { cause });
            let mut timer: Option<(Duration, crate::deadline::Wake)> = None;
            loop {
                let (changed, at) = futures::future::poll_fn(|context| -> Poll<Result<_>> {
                    loop {
                        let mut state = self.lock();
                        let now = transport.now();
                        let next = state.open(now);
                        let Some(index) = state.position(ticket) else {
                            return Poll::Ready(Err(crate::invalid("admission waiter disappeared")));
                        };
                        if state.waiters[index].1.admitted {
                            state.remove(ticket);
                            return Poll::Ready(Ok((None, now)));
                        }
                        let Some(current) = state.cause() else {
                            continue;
                        };
                        if current != cause {
                            return Poll::Ready(Ok((Some(current), now)));
                        }
                        state.waiters[index].1.waker = Some(context.waker().clone());
                        let Some(until) = next else {
                            timer = None;
                            return Poll::Pending;
                        };
                        drop(state);
                        let wake = match &mut timer {
                            Some((at, wake)) if *at == until => wake,
                            slot => {
                                let wait = until.saturating_sub(transport.now());
                                &mut slot.insert((until, crate::deadline::timer(transport, wait, work)?)).1
                            }
                        };
                        if wake.poll_unpin(context)?.is_pending() { return Poll::Pending; }
                        timer = None;
                    }
                }).await?;
                let waited_for = at.saturating_sub(started);
                if let Some(next) = changed {
                    cause = next;
                    yield Event::Wait(ModelWait::AdmissionChanged { cause, waited_for });
                } else {
                    // Retain the original gate before yielding the finish.
                    let permit = Permit::new(self.clone());
                    drop(queued);
                    let admitted_at = transport.unix_ms()?;
                    yield Event::Wait(ModelWait::AdmissionFinished { waited_for, admitted_at });
                    yield Event::Ready(permit);
                    break;
                }
            }
        }
    }
}

/// A waiter abandoned before it resumes releases its queue node and custody.
struct Queued<'a> {
    gate: &'a Gate,
    ticket: u64,
    #[cfg(not(target_arch = "wasm32"))]
    _custody: acyclic_fs::NativeCustody,
}

impl Drop for Queued<'_> {
    fn drop(&mut self) {
        self.gate.lock().remove(self.ticket);
    }
}

/// One admitted request, held until its response is read; or, after a refusal with a
/// provider delay, the refused caller's claim to reopen the endpoint at that instant.
pub(crate) struct Permit {
    gate: Arc<Gate>,
    opens: Option<Duration>,
}

impl Permit {
    const fn new(gate: Arc<Gate>) -> Self {
        Self { gate, opens: None }
    }

    /// Keeps only an opener: an accepted response's permit ends before its caller waits.
    pub(crate) fn settle(held: &mut Option<Self>) {
        if held.as_ref().is_some_and(|permit| permit.opens.is_none()) {
            *held = None;
        }
    }

    /// The refused caller has waited out the provider's delay.
    fn waited(self, now: Duration) {
        if self.opens.is_some() {
            self.gate.lock().open(now);
        }
    }

    /// Learns from the response head. Returns whether the caller keeps this permit:
    /// in flight while accepted, or as the opener of a provider delay.
    fn observe(&mut self, status: u16, retry_after: Option<Duration>, now: Duration) -> bool {
        if (200..300).contains(&status) {
            return true;
        }
        if status == 429 {
            let mut state = self.gate.lock();
            if let Some(wait) = retry_after {
                let until = now.saturating_add(wait);
                if state.closed_until.is_none_or(|closed| closed < until) {
                    state.closed_until = Some(until);
                }
                state.window = Some(wait);
                state.pace = None;
                state.revoke();
                self.opens = Some(until);
            }
            return self.opens.is_some();
        }
        false
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "admission_tests.rs"]
mod tests;
