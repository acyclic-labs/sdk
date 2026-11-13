//! A provider that serves `K` concurrent requests and refuses the rest with a
//! Retry-After, driven by many agents on the paused clock.
use super::*;
use crate::Adapter;
use acyclic_harness_core::model::ModelProvider as _;
use acyclic_stream::BoxProviderFuture;
use futures::TryStreamExt as _;

#[path = "admission_observation_tests.rs"]
mod observations;

const AGENTS: usize = 60;
const CAPACITY: usize = 4;
const SERVICE: Duration = Duration::from_secs(10);
const RETRY_AFTER: Duration = Duration::from_secs(3);

struct Capacity {
    origin: tokio::time::Instant,
    active: Arc<Mutex<usize>>,
    /// Every request as (sent at, status).
    log: Mutex<Vec<(Duration, u16)>>,
}

/// Frees its provider slot when the response ends or is dropped.
struct Slot(Arc<Mutex<usize>>);

impl Drop for Slot {
    fn drop(&mut self) {
        *self.0.lock().unwrap() -= 1;
    }
}

impl HttpTransport for Capacity {
    fn send(&self, _: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        let now = self.now();
        let slot = {
            let mut active = self.active.lock().unwrap();
            (*active < CAPACITY).then(|| {
                *active += 1;
                Slot(self.active.clone())
            })
        };
        self.log
            .lock()
            .unwrap()
            .push((now, if slot.is_some() { 200 } else { 429 }));
        Box::pin(async move {
            Ok(match slot {
                Some(slot) => HttpResponse {
                    status: 200,
                    body: Box::pin(async_stream::stream! {
                        let _slot = slot;
                        tokio::time::sleep(SERVICE).await;
                        yield Ok(bytes::Bytes::from_static(crate::fixtures::CHAT.as_bytes()));
                    }),
                    request_id: None,
                    retry_after: None,
                },
                None => HttpResponse {
                    status: 429,
                    body: Box::pin(futures::stream::empty()),
                    request_id: None,
                    retry_after: Some(RETRY_AFTER),
                },
            })
        })
    }

    fn delay(&self, wait: Duration) -> BoxProviderFuture<'static, Result<()>> {
        Box::pin(async move {
            tokio::time::sleep(wait).await;
            Ok(())
        })
    }

    fn now(&self) -> Duration {
        self.origin.elapsed()
    }

    fn unix_ms(&self) -> Result<u64> {
        Ok(1_000_000 + u64::try_from(self.now().as_millis()).unwrap())
    }
}

/// Runs every agent to completion, each with its own selection. Agents share one
/// admission when `shared`, as selections of one registry do; otherwise each has its
/// own, which is the former per-agent Retry-After behaviour.
fn swarm(shared: bool) -> (Vec<(Duration, u16)>, Duration) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap();
    runtime.block_on(async {
        let transport = Arc::new(Capacity {
            origin: tokio::time::Instant::now(),
            active: Arc::default(),
            log: Mutex::default(),
        });
        let admission = Arc::<Endpoints>::default();
        let providers: Vec<_> = (0..AGENTS)
            .map(|_| {
                let mut provider = crate::tests::provider_with(
                    Adapter::Chat,
                    "https://provider.test/v1",
                    transport.clone(),
                );
                if shared {
                    provider.admission = admission.clone();
                }
                provider
            })
            .collect();
        let agents = providers.iter().map(|provider| async move {
            let (request, dispatch) = crate::tests::request(provider)?;
            let events: Vec<_> = provider.generate(&request, dispatch).try_collect().await?;
            assert!(matches!(
                events.last(),
                Some(acyclic_harness_core::model::ModelEvent::Completed { .. })
            ));
            Ok::<_, acyclic_harness_core::Error>(())
        });
        for agent in futures::future::join_all(agents).await {
            agent.unwrap();
        }
        let wall = transport.now();
        let log = std::mem::take(&mut *transport.log.lock().unwrap());
        (log, wall)
    })
}

/// Whether each refusal was the only one in its provider window, and no request
/// was sent before that window's instant.
fn one_refusal_per_window(log: &[(Duration, u16)]) -> bool {
    log.iter()
        .filter(|(_, status)| *status == 429)
        .all(|(at, _)| {
            log.iter()
                .filter(|(sent, _)| sent > at && *sent < *at + RETRY_AFTER)
                .count()
                == 0
                && log
                    .iter()
                    .filter(|(sent, status)| *status == 429 && sent == at)
                    .count()
                    == 1
        })
}

#[test]
fn one_endpoint_admission_refuses_once_per_window_and_serves_at_capacity() {
    let (log, wall) = swarm(true);
    let refusals = log.iter().filter(|(_, status)| *status == 429).count();
    assert_eq!(log.len() - refusals, AGENTS);
    assert!(one_refusal_per_window(&log), "{log:?}");
    // The provider serves `CAPACITY` requests per service time; nothing finishes sooner,
    // and admission adds no idle provider time.
    let capacity_bound = SERVICE * AGENTS.div_ceil(CAPACITY) as u32;
    assert_eq!(wall, capacity_bound);
    eprintln!("shared admission: {refusals} refusals, {wall:?} wall");

    // Negative control: per-agent admission, the former behaviour, herds into the
    // provider window after window.
    let (log, wall) = swarm(false);
    let refusals = log.iter().filter(|(_, status)| *status == 429).count();
    assert!(!one_refusal_per_window(&log));
    assert!(wall > capacity_bound);
    eprintln!("per-agent admission: {refusals} refusals, {wall:?} wall");
}

#[test]
fn queued_waiters_resume_in_arrival_order_and_cancelled_ones_return_credit() {
    futures::executor::block_on(async {
        let transport = Capacity {
            origin: tokio::time::Instant::now(),
            active: Arc::default(),
            log: Mutex::default(),
        };
        let gate = Arc::new(Gate::default());
        let mut first = gate.clone().admitted(&transport, None).await.unwrap();
        assert!(!first.observe(429, None, Duration::ZERO));
        drop(first);
        // The refusal learned credit 1; it was released, so one request is admitted.
        let held = gate.clone().admitted(&transport, None).await.unwrap();
        let mut second = Box::pin(gate.clone().admitted(&transport, None));
        let mut third = Box::pin(gate.clone().admitted(&transport, None));
        assert!(futures::poll!(second.as_mut()).is_pending());
        assert!(futures::poll!(third.as_mut()).is_pending());
        drop(held);
        // Dropping the admitted second waiter returns its credit to the third.
        assert!(futures::poll!(third.as_mut()).is_pending());
        drop(second);
        assert!(futures::poll!(third.as_mut()).is_ready());
    });
}

fn paused() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap()
}

fn capacity() -> Arc<Capacity> {
    Arc::new(Capacity {
        origin: tokio::time::Instant::now(),
        active: Arc::default(),
        log: Mutex::default(),
    })
}

#[test]
fn a_cooldown_outlives_every_permit_and_waiter_of_its_endpoint() {
    paused().block_on(async {
        let transport = capacity();
        let provider = crate::tests::provider_with(
            Adapter::Chat,
            "https://provider.test/v1",
            transport.clone(),
        );
        let gate = provider.admission.gate(&provider);
        let mut last = gate
            .clone()
            .admitted(transport.as_ref(), None)
            .await
            .unwrap();
        // A refusal with a provider delay keeps an opener, here abandoned unwaited.
        assert!(last.observe(429, Some(RETRY_AFTER), transport.now()));
        // The last request and every handle on the gate are gone; no waiter remains.
        let retained = Arc::downgrade(&gate);
        drop((last, gate));
        // Negative control: a registry that kept only a weak handle would lose the cooldown.
        assert!(
            retained.upgrade().is_some(),
            "the endpoint dropped its cooldown"
        );
        let started = transport.now();
        let mut next = Box::pin(
            provider
                .admission
                .gate(&provider)
                .admitted(transport.as_ref(), None),
        );
        assert!(futures::poll!(next.as_mut()).is_pending());
        drop(next.await.unwrap());
        assert_eq!(transport.now() - started, RETRY_AFTER);
    });
}

#[test]
fn a_grant_not_yet_resumed_is_revoked_when_the_endpoint_closes() {
    for revoked in [true, false] {
        paused().block_on(async {
            let transport = capacity();
            let gate = Arc::new(Gate::default());
            gate.lock().credit = Some(2);
            let first = gate
                .clone()
                .admitted(transport.as_ref(), None)
                .await
                .unwrap();
            let mut second = gate
                .clone()
                .admitted(transport.as_ref(), None)
                .await
                .unwrap();
            let mut waiter = Box::pin(gate.clone().admitted(transport.as_ref(), None));
            assert!(futures::poll!(waiter.as_mut()).is_pending());
            // Credit is granted to the waiter, which has not resumed yet.
            drop(first);
            assert!(gate.lock().waiters[0].1.admitted);
            if revoked {
                // Another request's refusal closes the endpoint before the waiter resumes.
                assert!(second.observe(429, Some(RETRY_AFTER), transport.now()));
                drop(second);
                assert!(futures::poll!(waiter.as_mut()).is_pending());
                let started = transport.now();
                drop(waiter.await.unwrap());
                assert_eq!(transport.now() - started, RETRY_AFTER);
            } else {
                // Negative control: the same grant resumes at once while the endpoint is open.
                assert!(futures::poll!(waiter.as_mut()).is_ready());
                drop(second);
            }
        });
    }
}

#[test]
fn a_queued_waiter_funds_its_node_on_its_work_until_it_resumes() {
    paused().block_on(async {
        let transport = capacity();
        let work = crate::tests::native_tests::work(&acyclic_fs::CancellationToken::new()).unwrap();
        let gate = Arc::new(Gate::default());
        gate.lock().credit = Some(1);
        let held = gate
            .clone()
            .admitted(transport.as_ref(), Some(&work))
            .await
            .unwrap();
        // An immediate admission queues nothing and funds nothing.
        let baseline = work.live_allocation_bytes();
        let mut waiter = Box::pin(gate.clone().admitted(transport.as_ref(), Some(&work)));
        assert!(futures::poll!(waiter.as_mut()).is_pending());
        assert_eq!(work.live_allocation_bytes(), baseline + QUEUED_BYTES);
        drop(held);
        drop(waiter.await.unwrap());
        assert_eq!(work.live_allocation_bytes(), baseline);
        assert!(gate.lock().waiters.capacity() == 0);
    });
}

#[test]
fn admission_span_starts_before_grant_and_retains_cause_changes() {
    paused().block_on(async {
        let transport = capacity();
        let gate = Arc::new(Gate::default());
        gate.lock().credit = Some(1);
        let mut held = gate
            .clone()
            .admitted(transport.as_ref(), None)
            .await
            .unwrap();
        let mut waiting = Box::pin(gate.clone().admit(transport.as_ref(), None));
        assert!(matches!(waiting.next().now_or_never(), Some(Some(Ok(Event::Wait(
            ModelWait::AdmissionStarted { cause: ModelAdmissionCause::Credit }
        ))))));
        // Negative control: a collector which buffers until admission cannot
        // produce the start above while this exact credit is still held.
        assert!(waiting.next().now_or_never().is_none());
        tokio::time::advance(Duration::from_secs(7)).await;
        assert!(held.observe(429, Some(RETRY_AFTER), transport.now()));
        let until = transport.now() + RETRY_AFTER;
        assert!(matches!(waiting.next().now_or_never(), Some(Some(Ok(Event::Wait(
            ModelWait::AdmissionChanged { cause: ModelAdmissionCause::Cooldown { until: at }, waited_for }
        )))) if at == until && waited_for == Duration::from_secs(7)));
        assert!(waiting.next().now_or_never().is_none());
        drop(held);
        tokio::time::advance(RETRY_AFTER).await;
        assert!(matches!(waiting.next().await, Some(Ok(Event::Wait(
            ModelWait::AdmissionFinished { waited_for, admitted_at: 1_010_000 }
        ))) if waited_for == Duration::from_secs(10)));
        assert!(matches!(waiting.next().await, Some(Ok(Event::Ready(_)))));
        assert_eq!(gate.lock().in_flight, 0);
    });
}

#[test]
fn cancelling_an_observed_admission_returns_original_queue_and_grant_credit() {
    for at_finish in [false, true] {
        paused().block_on(async {
            let transport = capacity();
            let work =
                crate::tests::native_tests::work(&acyclic_fs::CancellationToken::new()).unwrap();
            let gate = Arc::new(Gate::default());
            gate.lock().credit = Some(1);
            let held = gate
                .clone()
                .admitted(transport.as_ref(), Some(&work))
                .await
                .unwrap();
            let baseline = work.live_allocation_bytes();
            let mut waiting = Box::pin(gate.clone().admit(transport.as_ref(), Some(&work)));
            assert!(matches!(
                waiting.next().now_or_never(),
                Some(Some(Ok(Event::Wait(ModelWait::AdmissionStarted {
                    cause: ModelAdmissionCause::Credit
                }))))
            ));
            assert_eq!(work.live_allocation_bytes(), baseline + QUEUED_BYTES);
            assert!(waiting.next().now_or_never().is_none());
            if at_finish {
                drop(held);
                assert!(matches!(
                    waiting.next().await,
                    Some(Ok(Event::Wait(ModelWait::AdmissionFinished { .. })))
                ));
                assert_eq!(gate.lock().in_flight, 1);
                drop(waiting);
            } else {
                // An interrupted span keeps its original start and has no finish.
                drop(waiting);
                assert_eq!(gate.lock().in_flight, 1);
                drop(held);
            }
            assert_eq!(work.live_allocation_bytes(), baseline);
            assert_eq!(gate.lock().in_flight, 0);
            assert!(gate.lock().waiters.is_empty());
            // The next caller has immediate credit rather than a leaked wait.
            let mut next = Box::pin(gate.clone().admit(transport.as_ref(), Some(&work)));
            assert!(matches!(
                next.next().now_or_never(),
                Some(Some(Ok(Event::Ready(_))))
            ));
        });
    }
}

// Queue scheduling tests consume the same observations as every other caller.
// Tests of observation order below inspect the stream directly.
impl Gate {
    async fn admitted(
        self: Arc<Self>,
        transport: &dyn HttpTransport,
        work: acyclic_fs::NativeWork<'_>,
    ) -> Result<Permit> {
        let events = self.admit(transport, work);
        let mut events = core::pin::pin!(events);
        while let Some(event) = events.next().await {
            if let Event::Ready(permit) = event? {
                return Ok(permit);
            }
        }
        Err(crate::invalid("admission ended without its permit"))
    }
}
