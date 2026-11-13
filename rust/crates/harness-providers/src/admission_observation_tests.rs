//! The original gate's facts reach provider streams and an agent's journal
//! while its request is still queued, rather than after the request returns.
use super::*;
use acyclic_harness_core::model::ModelEvent;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

#[derive(Default)]
struct Transport {
    millis: AtomicU64,
    sent: AtomicUsize,
    counted: AtomicUsize,
}

impl Transport {
    fn response(&self, request: HttpRequest) -> HttpResponse {
        self.sent.fetch_add(1, Ordering::SeqCst);
        let body = if request.url.ends_with("/count_tokens") {
            self.counted.fetch_add(1, Ordering::SeqCst);
            br#"{"input_tokens":10}"#.as_slice()
        } else {
            b"data: {\"choices\":[{\"delta\":{\"content\":\"done\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n".as_slice()
        };
        HttpResponse {
            status: 200,
            body: Box::pin(futures::stream::iter([Ok(bytes::Bytes::from_static(body))])),
            request_id: None,
            retry_after: None,
        }
    }
}

impl HttpTransport for Transport {
    fn send(&self, request: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        Box::pin(async move { Ok(self.response(request)) })
    }

    fn supports_native(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<bool> {
        work.remaining()?;
        Ok(true)
    }

    fn send_with_native_work<'a>(
        &'a self,
        request: HttpRequest,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<acyclic_fs::NativeFutureBundle<'a, HttpResponse, acyclic_harness_core::Error>> {
        acyclic_fs::NativeFutureBundle::new(work, || async move {
            let response = self.response(request);
            HttpResponse::with_native_body(response.status, response.body, work)
        })
    }

    fn now(&self) -> Duration {
        Duration::from_millis(self.millis.load(Ordering::SeqCst))
    }

    fn unix_ms(&self) -> Result<u64> {
        Ok(1_000_000 + self.millis.load(Ordering::SeqCst))
    }

    fn delay(&self, _: Duration) -> BoxProviderFuture<'static, Result<()>> {
        Box::pin(std::future::pending())
    }

    fn delay_with_native_work(
        &self,
        _: Duration,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<acyclic_fs::NativeFutureBundle<'static, (), acyclic_harness_core::Error>> {
        acyclic_fs::NativeFutureBundle::new(work, std::future::pending)
    }
}

#[tokio::test]
async fn generation_and_count_publish_original_admission_on_both_provider_paths() -> Result<()> {
    for native in [false, true] {
        for count in [false, true] {
            let transport = Arc::<Transport>::default();
            let provider = crate::tests::provider_with(
                Adapter::Chat,
                "https://provider.test/v1",
                transport.clone(),
            );
            let gate = provider.admission.gate(&provider);
            let mut held = gate.clone().admitted(transport.as_ref(), None).await?;
            assert!(held.observe(429, Some(Duration::from_secs(5)), transport.now()));
            let (request, dispatch) = crate::tests::request(&provider)?;
            let work = crate::tests::native_tests::work(&acyclic_fs::CancellationToken::new())?;
            let baseline = work.live_allocation_bytes();
            // Returning a native stream must not await its first HTTP admission.
            let events = if native {
                let events = if count {
                    provider
                        .count_tokens_with_native_work(&request, 0, &work)?
                        .await?
                } else {
                    provider
                        .generate_with_native_work(&request, dispatch, &work)?
                        .await?
                };
                futures::future::Either::Right(events.map(|event| {
                    event.map(|(event, custody)| {
                        drop(custody);
                        event
                    })
                }))
            } else {
                futures::future::Either::Left(if count {
                    provider.count_tokens(&request, 0)
                } else {
                    provider.generate(&request, dispatch)
                })
            };
            let mut events = Box::pin(events);
            let started = events.next().await.transpose()?;
            assert!(matches!(
                started,
                Some(ModelEvent::Wait(ModelWait::AdmissionStarted {
                    cause: ModelAdmissionCause::Cooldown { until },
                }))
             if until == Duration::from_secs(5)));
            assert_eq!(transport.sent.load(Ordering::SeqCst), 0);
            assert!(events.next().now_or_never().is_none());
            transport.millis.store(5_000, Ordering::SeqCst);
            held.waited();
            assert!(
                matches!(events.next().await.transpose()?, Some(ModelEvent::Wait(
                ModelWait::AdmissionFinished { waited_for, admitted_at: 1_005_000 }
            )) if waited_for == Duration::from_secs(5))
            );
            assert!(matches!(
                events.next().await.transpose()?,
                Some(ModelEvent::Trace { .. })
            ));
            assert!(matches!(
                events.next().await.transpose()?,
                Some(ModelEvent::Trace { .. })
            ));
            let mut terminal = false;
            while let Some(event) = events.next().await {
                terminal |= matches!(
                    (count, event?),
                    (true, ModelEvent::Counted { .. }) | (false, ModelEvent::Completed { .. })
                );
            }
            assert!(terminal);
            assert_eq!(transport.sent.load(Ordering::SeqCst), 1);
            drop(events);
            assert_eq!(work.live_allocation_bytes(), baseline);
        }
    }
    Ok(())
}

#[tokio::test]
async fn agent_journals_admission_start_before_generation_or_token_count_is_admitted() -> Result<()>
{
    use acyclic_harness::{LocalHarness, local::agent_conversation::Conversation};
    use acyclic_harness_core::context::CompactionPolicy;
    for maximum in [None, std::num::NonZeroUsize::new(1)] {
        for count in [false, true] {
            let transport = Arc::<Transport>::default();
            let fixture = crate::tests::provider_with(
                Adapter::Chat,
                "https://provider.test/v1",
                transport.clone(),
            );
            // A dense first input requires the remote counter; its verified answer
            // fits the window, so generation can follow it without compaction.
            let capacity = acyclic_harness_core::context::ModelContextCapacity {
                context_tokens: 1_024,
                output_tokens: Some(128),
            };
            let mut specification = fixture.config.as_ref().clone();
            specification.authentication = crate::Authentication::Optional;
            // Exercise generation admission independently of the counter path.
            if !count {
                specification.token_counter = None;
            }
            // Register this mock Chat endpoint and counting behavior under
            // the root preset's model identity, with anonymous authentication.
            specification.models = std::collections::BTreeMap::from([(
                "morph-kimik31".into(),
                crate::ModelSpec {
                    tool_profile: None,
                    thinking_levels: false,
                    max_context_tokens: None,
                    capacity,
                    features: crate::ModelFeatures {
                        tools: true,
                        ..Default::default()
                    },
                },
            )]);
            let mut registry = crate::Registry::with_transport(
                crate::Credentials::explicit(std::collections::BTreeMap::new()),
                transport.clone(),
            );
            registry.register("morph".into(), specification)?;
            let provider = registry.resolve("morph/morph-kimik31")?;
            let gate = provider.admission.gate(&provider);
            let mut held = gate.clone().admitted(transport.as_ref(), None).await?;
            let mut extending = gate.clone().admitted(transport.as_ref(), None).await?;
            assert!(held.observe(429, Some(RETRY_AFTER), transport.now()));
            let provider = Arc::new(provider);
            let directory =
                tempfile::tempdir().map_err(|error| crate::invalid(error.to_string()))?;
            let policy = if count {
                CompactionPolicy::default()
            } else {
                CompactionPolicy::Disabled
            };
            let config = acyclic_harness::test_support::local_harness_config(
                directory.path(),
                (provider.model().clone(), provider),
            )?
            .compaction(policy)
            .limits(acyclic_harness_core::conversation::Limits {
                model_events_per_step: maximum,
                ..Default::default()
            });
            let harness = Arc::new(LocalHarness::open(config).await?);
            let conversation = Arc::new(Conversation::open(harness.connect().await?));
            let mut usage = conversation.usage_query();
            let input = "dense input ".repeat(1_000);
            let mut turn = Box::pin(acyclic_harness::test_support::prompt_settled(
                &harness,
                "admission",
                &input,
            ));
            while usage.waits().started == 0 {
                tokio::select! {
                    result = &mut turn => panic!("turn ended before its queued admission was journalled: {result:?}"),
                    result = usage.advance() => { result?; }
                }
            }
            // Negative control: buffering the original start until the gate opens
            // cannot expose this durable fact while sent remains zero.
            assert_eq!(usage.waits().started, 1);
            assert_eq!(usage.waits().completed, 0);
            assert_eq!(usage.waits().elapsed_ns(), None);
            assert_eq!(transport.sent.load(Ordering::SeqCst), 0);
            if maximum.is_some() {
                transport.millis.store(5_000, Ordering::SeqCst);
                held.waited();
                let outcome = turn.await?;
                assert!(
                    matches!(&outcome,
                        acyclic_harness::test_support::PromptOutcome::Failed(message)
                        if message.contains("model event limit exceeded")
                    ),
                    "wrong refusal cause: {outcome:?}"
                );
                let mut final_usage = conversation.usage_query();
                while !final_usage.synchronized() {
                    final_usage.advance().await?;
                }
                assert_eq!(final_usage.waits().started, 1);
                assert_eq!(final_usage.waits().completed, 0);
                assert_eq!(
                    transport.sent.load(Ordering::SeqCst),
                    0,
                    "a refused observation must not admit HTTP or a generation dispatch"
                );
                continue;
            }
            transport.millis.store(2_000, Ordering::SeqCst);
            assert!(extending.observe(429, Some(RETRY_AFTER), transport.now()));
            while usage.waits().changed == 0 {
                usage.advance().await?;
            }
            assert_eq!(usage.waits().changed, 1);
            assert_eq!(usage.waits().completed, 0);
            assert_eq!(usage.waits().elapsed_ns(), None);
            transport.millis.store(5_000, Ordering::SeqCst);
            extending.waited();
            assert!(matches!(
                turn.await?,
                acyclic_harness::test_support::PromptOutcome::Settled(_, _)
            ));
            while usage.waits().completed == 0 {
                usage.advance().await?;
            }
            assert_eq!(usage.waits().started, 1);
            assert_eq!(usage.waits().completed, 1);
            assert_eq!(usage.waits().elapsed_ns(), Some(5_000_000_000));
            // Settlement may include more than one model call. Match sends to
            // their actual durable calls, not an assumed host execution shape.
            let mut settled = conversation.usage_query();
            while !settled.synchronized() {
                settled.advance().await?;
            }
            let counted = transport.counted.load(Ordering::SeqCst);
            assert_eq!(counted > 0, count);
            assert!(settled.totals().completed > 0);
            assert_eq!(
                transport.sent.load(Ordering::SeqCst) as u64,
                settled.totals().completed + counted as u64,
                "HTTP generation sends must match completed journal calls"
            );
        }
    }
    Ok(())
}
