//! One admission per provider endpoint, shared by every request of one registry.
//!
//! A 429 with a provider delay closes the endpoint for every caller until exactly
//! that instant, and waiters are admitted in arrival order when it opens. In-flight
//! requests are bounded by credit: unbounded until the endpoint refuses one, then the
//! number the provider was serving when it refused. Credit grows by one after each
//! full credit of accepted requests, so a probe above the learned bound costs at most
//! one refusal. Request bytes are never changed.
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

/// The endpoint gates of one registry, keyed by provider, base URL and credential.
/// An endpoint retains its gate while anyone holds it or it carries provider-taught
/// state (a pending cooldown or learned credit), so that state outlives its permits.
#[derive(Default)]
pub(crate) struct Endpoints(Mutex<HashMap<[u8; 32], Arc<Gate>>>);

impl Endpoints {
    fn gate(&self, provider: &ResolvedModel) -> Arc<Gate> {
        let mut identity = blake3::Hasher::new();
        identity.update(b"acyclic.provider-admission.v1\0");
        for part in [provider.model.provider.as_str(), &provider.config.base_url] {
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
        gates.retain(|_, gate| Arc::strong_count(gate) > 1 || gate.lock().taught());
        let gate = Arc::new(Gate::default());
        gates.insert(identity, gate.clone());
        gate
    }
}

/// Original gate observations followed by the value they admitted.
pub(crate) enum Event<T> {
    Wait(ModelWait),
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
        if let Some(permit) = held.take() { permit.waited(); }
        let transport = provider.transport.as_ref();
        let admission = provider.admission.gate(provider).admit(transport, work);
        let mut admission = core::pin::pin!(admission);
        let mut permit = loop {
            match admission.next().await {
                Some(Ok(Event::Wait(wait))) => yield Event::Wait(wait),
                Some(Ok(Event::Ready(permit))) => break permit,
                Some(Err(error)) => Err(error)?,
                None => Err(crate::invalid("endpoint admission ended without a permit"))?,
            }
        };
        let response = crate::http::send(transport, request, work)?.await?;
        if permit.observe(response.status, response.retry_after, transport.now()) {
            *held = Some(permit);
        }
        yield Event::Ready(response);
    }
}

#[derive(Default)]
pub(crate) struct Gate(Mutex<State>);

#[derive(Default)]
struct State {
    /// Admitted permits, including admitted waiters that have not resumed yet.
    in_flight: u64,
    /// Permits whose request the provider accepted.
    accepted: u64,
    /// `None` until the provider refuses a request: no bound is assumed.
    credit: Option<u64>,
    /// Acceptances counted toward the next credit increase.
    successes: u64,
    /// The provider's instant on the transport clock, while closed.
    closed_until: Option<Duration>,
    next_ticket: u64,
    /// Waiters in arrival (ticket) order.
    waiters: VecDeque<(u64, Waiter)>,
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
    /// Whether the provider has taught this endpoint anything worth retaining.
    const fn taught(&self) -> bool {
        self.closed_until.is_some() || self.credit.is_some()
    }

    fn position(&self, ticket: u64) -> Option<usize> {
        self.waiters
            .binary_search_by_key(&ticket, |(ticket, _)| *ticket)
            .ok()
    }

    fn remove(&mut self, ticket: u64) -> Option<Waiter> {
        let waiter = self.waiters.remove(self.position(ticket)?)?.1;
        if self.waiters.len() * 4 <= self.waiters.capacity() {
            self.waiters.shrink_to(self.waiters.len() * 2);
        }
        Some(waiter)
    }

    /// An admission is valid only in the gate state that granted it: closing or
    /// lowering credit revokes grants whose waiter has not resumed, and every
    /// waiter re-evaluates (arming the provider's instant when closed).
    fn revoke(&mut self) {
        for (_, waiter) in &mut self.waiters {
            if std::mem::take(&mut waiter.admitted) {
                self.in_flight -= 1;
            }
            if let Some(waker) = waiter.waker.take() {
                waker.wake();
            }
        }
        self.wake();
    }

    fn cause(&self) -> ModelAdmissionCause {
        match self.closed_until {
            Some(until) => ModelAdmissionCause::Cooldown { until },
            None => ModelAdmissionCause::Credit,
        }
    }

    fn admits(&self) -> bool {
        self.closed_until.is_none() && self.credit.is_none_or(|credit| self.in_flight < credit)
    }

    /// Admits waiters in arrival order up to credit, only while open.
    fn wake(&mut self) {
        if self.closed_until.is_some() {
            return;
        }
        for (_, waiter) in self
            .waiters
            .iter_mut()
            .filter(|(_, waiter)| !waiter.admitted)
        {
            if self.credit.is_some_and(|credit| self.in_flight >= credit) {
                break;
            }
            waiter.admitted = true;
            self.in_flight += 1;
            if let Some(waker) = waiter.waker.take() {
                waker.wake();
            }
        }
    }
}

impl Gate {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
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
        if state
            .closed_until
            .is_some_and(|until| until <= transport.now())
        {
            state.closed_until = None;
        }
        if state.waiters.iter().all(|(_, waiter)| waiter.admitted) && state.admits() {
            state.in_flight += 1;
            return Ok(None);
        }
        #[cfg(not(target_arch = "wasm32"))]
        let custody = work
            .map(|work| work.claim(QUEUED_BYTES, 1, acyclic_fs::WorkCounters::UNCHARGED))
            .transpose()?;
        #[cfg(target_arch = "wasm32")]
        let custody = {
            let _ = work;
            None
        };
        let ticket = state.next_ticket;
        state.next_ticket += 1;
        let cause = state.cause();
        let started = transport.now();
        state.waiters.push_back((
            ticket,
            Waiter {
                admitted: false,
                waker: None,
            },
        ));
        state.wake();
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
                        let Some(index) = state.position(ticket) else {
                            return Poll::Ready(Err(crate::invalid("admission waiter disappeared")));
                        };
                        if state.waiters[index].1.admitted {
                            state.remove(ticket);
                            return Poll::Ready(Ok((None, transport.now())));
                        }
                        let current = state.cause();
                        if current != cause {
                            return Poll::Ready(Ok((Some(current), transport.now())));
                        }
                        state.waiters[index].1.waker = Some(context.waker().clone());
                        let Some(until) = state.closed_until else {
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
                        let mut state = self.lock();
                        if state.closed_until == Some(until) {
                            state.closed_until = None;
                            state.wake();
                        }
                    }
                }).await?;
                let waited_for = at.saturating_sub(started);
                if let Some(next) = changed {
                    cause = next;
                    yield Event::Wait(ModelWait::AdmissionChanged { cause, waited_for });
                } else {
                    // Own granted credit before yielding: cancellation while the
                    // finish is being observed must return that credit too.
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

/// A waiter abandoned before it resumes returns any admission it was given.
struct Queued<'a> {
    gate: &'a Gate,
    ticket: u64,
    #[cfg(not(target_arch = "wasm32"))]
    _custody: acyclic_fs::NativeCustody,
}

impl Drop for Queued<'_> {
    fn drop(&mut self) {
        let mut state = self.gate.lock();
        if state
            .remove(self.ticket)
            .is_some_and(|waiter| waiter.admitted)
        {
            state.in_flight -= 1;
            state.wake();
        }
    }
}

/// One admitted request, held until its response is read; or, after a refusal with a
/// provider delay, the refused caller's claim to reopen the endpoint at that instant.
pub(crate) struct Permit {
    gate: Arc<Gate>,
    accepted: bool,
    /// Counted in flight; a refusal stops counting before its caller waits.
    counted: bool,
    opens: Option<Duration>,
}

impl Permit {
    const fn new(gate: Arc<Gate>) -> Self {
        Self {
            gate,
            accepted: false,
            counted: true,
            opens: None,
        }
    }

    /// Keeps only an opener: an accepted response's permit ends before its caller waits.
    pub(crate) fn settle(held: &mut Option<Self>) {
        if held.as_ref().is_some_and(|permit| permit.opens.is_none()) {
            *held = None;
        }
    }

    /// The refused caller has waited out the provider's delay.
    fn waited(self) {
        if let Some(until) = self.opens {
            let mut state = self.gate.lock();
            if state.closed_until == Some(until) {
                state.closed_until = None;
                state.wake();
            }
        }
    }

    /// Learns from the response head. Returns whether the caller keeps this permit:
    /// in flight while accepted, or as the opener of a provider delay.
    fn observe(&mut self, status: u16, retry_after: Option<Duration>, now: Duration) -> bool {
        let mut state = self.gate.lock();
        if (200..300).contains(&status) {
            self.accepted = true;
            state.accepted += 1;
            if let Some(credit) = state.credit
                && state.closed_until.is_none()
                && state.in_flight >= credit
            {
                state.successes += 1;
                if state.successes >= credit {
                    state.successes = 0;
                    state.credit = Some(credit + 1);
                    state.wake();
                }
            }
            return true;
        }
        if status == 429 {
            // The provider was serving the accepted requests when it refused this one.
            let bound = state.accepted.max(1);
            state.credit = Some(state.credit.map_or(bound, |credit| credit.min(bound)));
            state.successes = 0;
            state.in_flight -= 1;
            self.counted = false;
            if let Some(wait) = retry_after {
                let until = now.saturating_add(wait);
                if state.closed_until.is_none_or(|closed| closed < until) {
                    state.closed_until = Some(until);
                }
                self.opens = Some(until);
            }
            state.revoke();
            return self.opens.is_some();
        }
        false
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        if !self.counted {
            return;
        }
        let mut state = self.gate.lock();
        state.in_flight -= 1;
        if self.accepted {
            state.accepted -= 1;
        }
        state.wake();
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "admission_tests.rs"]
mod tests;
