//! Shared model cooldowns, driven through real requests on a paused clock.
use super::*;
use crate::Adapter;
use acyclic_harness_core::model::ModelProvider as _;
use acyclic_stream::BoxProviderFuture;

#[path = "admission_observation_tests.rs"]
mod observations;

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

    fn delay_with_native_work(
        &self,
        wait: Duration,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<acyclic_fs::NativeFutureBundle<'static, (), acyclic_harness_core::Error>> {
        acyclic_fs::NativeFutureBundle::new(work, move || async move {
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

/// A rate refusal is independent of how many earlier responses are still open.
struct RateRefusal {
    origin: tokio::time::Instant,
    refuse_at: usize,
    log: Mutex<Vec<(Duration, u16)>>,
}

impl HttpTransport for RateRefusal {
    fn send(&self, _: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        let mut log = self.log.lock().unwrap();
        let status = if log.len() == self.refuse_at {
            429
        } else {
            200
        };
        log.push((self.now(), status));
        Box::pin(async move {
            Ok(HttpResponse {
                status,
                // Keep every accepted response open: admission must not wait
                // for one of these bodies to complete after the cooldown.
                body: Box::pin(futures::stream::pending()),
                request_id: None,
                retry_after: (status == 429).then_some(RETRY_AFTER),
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

#[test]
fn rate_refusal_resumes_every_request_at_retry_after_with_responses_still_open() -> Result<()> {
    use acyclic_harness_core::model::ModelEvent;
    for accepted in [0, 3] {
        paused().block_on(async {
            let transport = Arc::new(RateRefusal {
                origin: tokio::time::Instant::now(),
                refuse_at: accepted,
                log: Mutex::default(),
            });
            let provider = crate::tests::provider_with(
                Adapter::Chat,
                "https://provider.test/v1",
                transport.clone(),
            );
            let (request, dispatch) = crate::tests::request(&provider)?;
            let mut active: Vec<_> = (0..accepted)
                .map(|_| provider.generate(&request, dispatch))
                .collect();
            for response in &mut active {
                assert!(response.next().now_or_never().is_none());
            }
            let mut refused = provider.generate(&request, dispatch);
            assert!(matches!(
                refused.next().now_or_never(),
                Some(Some(Ok(ModelEvent::Wait(ModelWait::Started { .. }))))
            ));
            let mut queued: Vec<_> = (0..10)
                .map(|_| provider.generate(&request, dispatch))
                .collect();
            for response in &mut queued {
                assert!(matches!(response.next().now_or_never(), Some(Some(Ok(
                    ModelEvent::Wait(ModelWait::AdmissionStarted {
                        cause: ModelAdmissionCause::Cooldown { until }
                    })
                ))) if until == RETRY_AFTER));
                assert!(response.next().now_or_never().is_none());
            }
            tokio::time::advance(RETRY_AFTER - Duration::from_nanos(1)).await;
            for response in &mut queued {
                assert!(response.next().now_or_never().is_none());
            }
            assert_eq!(transport.log.lock().unwrap().len(), accepted + 1);
            tokio::time::advance(Duration::from_nanos(1)).await;
            for response in &mut queued {
                assert!(matches!(response.next().now_or_never(), Some(Some(Ok(
                    ModelEvent::Wait(ModelWait::AdmissionFinished { waited_for, .. })
                ))) if waited_for == RETRY_AFTER));
                assert!(response.next().now_or_never().is_none());
            }
            let log = transport.log.lock().unwrap();
            assert_eq!(log.len(), accepted + 11);
            assert!(
                log[accepted + 1..]
                    .iter()
                    .all(|row| *row == (RETRY_AFTER, 200))
            );
            // The former accepted-at-429 credit admits only max(accepted, 1)
            // requests here while their response bodies remain open.
            Ok::<_, acyclic_harness_core::Error>(())
        })?;
    }
    Ok(())
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
fn one_models_retry_after_does_not_gate_another_models_http() -> Result<()> {
    use acyclic_harness_core::model::ModelEvent;

    paused().block_on(async {
        let transport = capacity();
        let fixture = crate::tests::provider_with(
            Adapter::Chat,
            "https://provider.test/v1",
            transport.clone(),
        );
        let mut specification = fixture.config.as_ref().clone();
        specification.models = ["limited", "independent"]
            .into_iter()
            .map(|name| {
                (
                    name.into(),
                    crate::ModelSpec {
                        tool_profile: None,
                        thinking_levels: false,
                        max_context_tokens: None,
                        capacity: fixture.capacity,
                        features: Default::default(),
                    },
                )
            })
            .collect();
        let mut registry = crate::Registry::with_transport(
            crate::Credentials::explicit(std::collections::BTreeMap::from([(
                "fixture".into(),
                "key".into(),
            )])),
            transport.clone(),
        );
        registry.register("fixture".into(), specification)?;
        let limited = registry.resolve("fixture/limited")?;
        let peer = registry.resolve("fixture/limited")?;
        let independent = registry.resolve("fixture/independent")?;
        // Existing provider work refuses the first model's real HTTP request.
        *transport.active.lock().unwrap() = CAPACITY;
        let (request, dispatch) = crate::tests::request(&limited)?;
        let mut refused = limited.generate(&request, dispatch);
        assert!(matches!(
            refused.next().now_or_never(),
            Some(Some(Ok(ModelEvent::Wait(ModelWait::Started { .. }))))
        ));
        *transport.active.lock().unwrap() = 0;
        let (peer_request, peer_dispatch) = crate::tests::request(&peer)?;
        let mut queued = peer.generate(&peer_request, peer_dispatch);
        // Separate selections of the same model still share its cooldown.
        assert!(matches!(
            queued.next().now_or_never(),
            Some(Some(Ok(ModelEvent::Wait(ModelWait::AdmissionStarted {
                cause: ModelAdmissionCause::Cooldown { until }
            })))) if until == RETRY_AFTER
        ));
        assert!(queued.next().now_or_never().is_none());
        let (other_request, other_dispatch) = crate::tests::request(&independent)?;
        let mut other = independent.generate(&other_request, other_dispatch);
        assert!(other.next().now_or_never().is_none());
        // The other model reached HTTP immediately; only its body is pending.
        // Omitting the model from the key instead yields AdmissionStarted here.
        assert_eq!(
            *transport.log.lock().unwrap(),
            [(Duration::ZERO, 429), (Duration::ZERO, 200)]
        );
        assert_eq!(transport.now(), Duration::ZERO);
        drop((other, queued, refused));
        assert_eq!(*transport.active.lock().unwrap(), 0);
        Ok(())
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
fn selecting_another_model_collects_only_expired_unused_cooldowns() {
    for expired in [false, true] {
        paused().block_on(async {
            let transport = capacity();
            let mut provider = crate::tests::provider_with(
                Adapter::Chat,
                "https://provider.test/v1",
                transport.clone(),
            );
            let gate = provider.admission.gate(&provider);
            let mut refused = gate
                .clone()
                .admitted(transport.as_ref(), None)
                .await
                .unwrap();
            assert!(refused.observe(429, Some(RETRY_AFTER), transport.now()));
            let retained = Arc::downgrade(&gate);
            drop((refused, gate));
            if expired {
                tokio::time::advance(RETRY_AFTER).await;
            }
            // A different bucket triggers collection without using the old
            // model again. A pending cooldown must survive the same collection.
            provider.model.name.push_str("-other");
            let _other = provider.admission.gate(&provider);
            assert_eq!(retained.upgrade().is_none(), expired);
        });
    }
}

#[test]
fn a_grant_not_yet_resumed_is_revoked_when_the_endpoint_closes() {
    for revoked in [true, false] {
        paused().block_on(async {
            let transport = capacity();
            let gate = Arc::new(Gate::default());
            let mut first = gate
                .clone()
                .admitted(transport.as_ref(), None)
                .await
                .unwrap();
            let mut second = gate
                .clone()
                .admitted(transport.as_ref(), None)
                .await
                .unwrap();
            assert!(first.observe(429, Some(RETRY_AFTER), transport.now()));
            let mut waiter = Box::pin(gate.clone().admitted(transport.as_ref(), None));
            assert!(futures::poll!(waiter.as_mut()).is_pending());
            tokio::time::advance(RETRY_AFTER).await;
            // A new caller opens the expired cooldown before this waiter resumes.
            let _fresh = gate
                .clone()
                .admitted(transport.as_ref(), None)
                .await
                .unwrap();
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
        let mut held = gate
            .clone()
            .admitted(transport.as_ref(), Some(&work))
            .await
            .unwrap();
        // An immediate admission queues nothing and funds nothing.
        let baseline = work.live_allocation_bytes();
        assert!(held.observe(429, Some(RETRY_AFTER), transport.now()));
        let mut waiter = Box::pin(gate.clone().admit(transport.as_ref(), Some(&work)));
        assert!(matches!(
            waiter.next().now_or_never(),
            Some(Some(Ok(Event::Wait(ModelWait::AdmissionStarted { .. }))))
        ));
        // The start exposes the original queue allocation before its native
        // timer is constructed on the next poll.
        assert_eq!(work.live_allocation_bytes(), baseline + QUEUED_BYTES);
        assert!(waiter.next().now_or_never().is_none());
        drop(held);
        assert!(matches!(
            waiter.next().await,
            Some(Ok(Event::Wait(ModelWait::AdmissionFinished { .. })))
        ));
        assert!(matches!(waiter.next().await, Some(Ok(Event::Ready(_)))));
        drop(waiter);
        assert_eq!(work.live_allocation_bytes(), baseline);
        assert!(gate.lock().waiters.capacity() == 0);
    });
}

#[test]
fn admission_span_starts_before_grant_and_retains_cause_changes() {
    paused().block_on(async {
        let transport = capacity();
        let gate = Arc::new(Gate::default());
        let mut held = gate
            .clone()
            .admitted(transport.as_ref(), None)
            .await
            .unwrap();
        let mut extending = gate.clone().admitted(transport.as_ref(), None).await.unwrap();
        assert!(held.observe(429, Some(RETRY_AFTER), transport.now()));
        let mut waiting = Box::pin(gate.clone().admit(transport.as_ref(), None));
        assert!(matches!(waiting.next().now_or_never(), Some(Some(Ok(Event::Wait(
            ModelWait::AdmissionStarted { cause: ModelAdmissionCause::Cooldown { until } }
        )))) if until == RETRY_AFTER));
        // Negative control: a collector which buffers until admission cannot
        // produce the start above while the cooldown is still closed.
        assert!(waiting.next().now_or_never().is_none());
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(extending.observe(429, Some(RETRY_AFTER), transport.now()));
        let until = transport.now() + RETRY_AFTER;
        assert!(matches!(waiting.next().now_or_never(), Some(Some(Ok(Event::Wait(
            ModelWait::AdmissionChanged { cause: ModelAdmissionCause::Cooldown { until: at }, waited_for }
        )))) if at == until && waited_for == Duration::from_secs(1)));
        assert!(waiting.next().now_or_never().is_none());
        drop(held);
        tokio::time::advance(RETRY_AFTER).await;
        assert!(matches!(waiting.next().await, Some(Ok(Event::Wait(
            ModelWait::AdmissionFinished { waited_for, admitted_at: 1_004_000 }
        ))) if waited_for == Duration::from_secs(4)));
        assert!(matches!(waiting.next().await, Some(Ok(Event::Ready(_)))));
    });
}

#[test]
fn cancelling_an_observed_admission_releases_original_queue_custody() {
    for at_finish in [false, true] {
        paused().block_on(async {
            let transport = capacity();
            let work =
                crate::tests::native_tests::work(&acyclic_fs::CancellationToken::new()).unwrap();
            let gate = Arc::new(Gate::default());
            let mut held = gate
                .clone()
                .admitted(transport.as_ref(), Some(&work))
                .await
                .unwrap();
            assert!(held.observe(429, Some(RETRY_AFTER), transport.now()));
            let baseline = work.live_allocation_bytes();
            let mut waiting = Box::pin(gate.clone().admit(transport.as_ref(), Some(&work)));
            assert!(matches!(
                waiting.next().now_or_never(),
                Some(Some(Ok(Event::Wait(ModelWait::AdmissionStarted {
                    cause: ModelAdmissionCause::Cooldown { until }
                }))))
             if until == RETRY_AFTER));
            assert_eq!(work.live_allocation_bytes(), baseline + QUEUED_BYTES);
            assert!(waiting.next().now_or_never().is_none());
            if at_finish {
                tokio::time::advance(RETRY_AFTER).await;
                held.waited();
                assert!(matches!(
                    waiting.next().await,
                    Some(Ok(Event::Wait(ModelWait::AdmissionFinished { .. })))
                ));
                drop(waiting);
            } else {
                // An interrupted span keeps its original start and has no finish.
                drop(waiting);
                assert_eq!(work.live_allocation_bytes(), baseline);
                tokio::time::advance(RETRY_AFTER).await;
                held.waited();
            }
            assert_eq!(work.live_allocation_bytes(), baseline);
            assert!(gate.lock().waiters.is_empty());
            // The next caller sees the expired cooldown without a leaked wait.
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
