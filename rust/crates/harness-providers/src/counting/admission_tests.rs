use super::*;
use crate::counting::wire;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn dense_and_resumed_inputs_use_verified_counts_instead_of_planning_weights(
        text in prop::collection::vec(any::<char>(), 0..256),
        copies in 1usize..5, observed in 1u64..4096, framing in 0u32..32,
    ) {
        let dense: String = text.into_iter().collect();
        for (adapter, _) in fixtures() {
            let (mut provider, script) = provider(adapter, 200);
            Arc::make_mut(&mut provider.config).token_counter.as_mut().unwrap().framing_tokens = framing;
            let (original, _) = request(&provider).unwrap();
            let mut resumed = original.request().clone();
            resumed.messages[1].content = ModelContent::Text(dense.repeat(copies));
            let prepared = PreparedModelRequest::prepare(resumed, Limits::default(), None).unwrap();
            let names = crate::tool_names::Names::new(prepared.request());
            let origin = provider.reasoning_origin(provider.key.as_deref(), None);
            let projected = projection::body_named(&provider, prepared.request(), &names, &origin, &mut crate::files::FileText::default()).unwrap();
            let counted = wire::body(projected, &provider.model.name, adapter).unwrap();
            let actual = counted.len() as u64 + 512;
            let first = futures::executor::block_on(provider.count_tokens(&prepared, 0)).unwrap();
            prop_assert_eq!(first.input_tokens, actual + u64::from(framing));
            prop_assert!(first.input_tokens >= actual);
            // Calibration can alter only planning weights, including after resume.
            provider.estimator.observe(1_000, &json!({"usage":{
                "input_tokens":observed,"prompt_tokens":observed,"promptTokenCount":observed
            }}));
            let next = futures::executor::block_on(provider.count_tokens(&prepared, 0)).unwrap();
            prop_assert_eq!(next.input_tokens, first.input_tokens);
            prop_assert_eq!(next.request_digest, prepared.request_digest());
            if observed >= 1_000 {
                prop_assert!(next.fixed_tokens > first.fixed_tokens);
            }
            prop_assert!(script.sent.lock().unwrap().is_empty(), "counting must not dispatch inference");
        }
    }
}

#[test]
fn calibrated_weight_negative_control_cannot_establish_admission() -> Result<()> {
    let (provider, _) = provider(Adapter::Responses, 200);
    let (request, _) = request(&provider)?;
    let count = futures::executor::block_on(provider.count_tokens(&request, 0))?;
    let planning = u64::from(count.fixed_tokens)
        + count
            .message_tokens
            .iter()
            .map(|n| u64::from(*n))
            .sum::<u64>();
    assert!(
        planning < count.input_tokens,
        "the deliberately dense tokenizer defeats byte-ratio admission"
    );
    assert!(count.input_tokens >= 512);
    Ok(())
}

struct CountResponse {
    status: u16,
    body: &'static [u8],
    empty_chunks: usize,
}
impl HttpTransport for CountResponse {
    fn delay(
        &self,
        wait: std::time::Duration,
    ) -> acyclic_stream::BoxProviderFuture<'static, Result<()>> {
        crate::fixtures::delay(wait)
    }
    fn now(&self) -> std::time::Duration {
        crate::fixtures::now()
    }
    fn send(&self, _: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        Box::pin(async move {
            Ok(HttpResponse {
                request_id: None,
                retry_after: None,
                status: self.status,
                body: Box::pin(
                    futures::stream::repeat_with(|| Ok(Bytes::new()))
                        .take(self.empty_chunks)
                        .chain(futures::stream::iter([Ok(Bytes::from_static(self.body))])),
                ),
            })
        })
    }
}

#[tokio::test]
async fn count_endpoint_errors_are_sanitized_classified_and_bounded() -> Result<()> {
    for (status, code) in [
        (401, acyclic_harness_core::wire::ErrorCode::Unauthorized),
        (403, acyclic_harness_core::wire::ErrorCode::Unauthorized),
        (408, acyclic_harness_core::wire::ErrorCode::Unavailable),
        (429, acyclic_harness_core::wire::ErrorCode::Unavailable),
        (503, acyclic_harness_core::wire::ErrorCode::Unavailable),
        (400, acyclic_harness_core::wire::ErrorCode::Invalid),
    ] {
        let (mut provider, _) = provider(Adapter::Responses, 200);
        provider.transport = crate::deadline::Bounded::new(
            Arc::new(CountResponse {
                status,
                body: b"fixture-secret",
                empty_chunks: 0,
            }),
            crate::TimeoutSpec::DEFAULT,
        );
        let (request, _) = request(&provider)?;
        let error = provider.count_tokens(&request, 0).await.unwrap_err();
        assert_eq!(error.code(), code);
        assert!(error.to_string().contains(&status.to_string()));
        // Only a refusal carries its bounded body excerpt, as on dispatch.
        assert_eq!(
            error.to_string().contains("fixture-secret"),
            code == acyclic_harness_core::wire::ErrorCode::Unavailable
        );
    }
    for body in [
        b"{\"input_tokens\":-1}".as_slice(),
        b"fixture-secret",
        b"{\"input_tokens\":9007199254740992}",
    ] {
        let (mut provider, _) = provider(Adapter::Responses, 200);
        provider.transport = crate::deadline::Bounded::new(
            Arc::new(CountResponse {
                status: 200,
                body,
                empty_chunks: 0,
            }),
            crate::TimeoutSpec::DEFAULT,
        );
        let (request, _) = request(&provider)?;
        assert!(provider.count_tokens(&request, 0).await.is_err());
    }
    let (mut provider, _) = provider(Adapter::Responses, 200);
    Arc::make_mut(&mut provider.config).max_event_bytes = 8;
    provider.transport = crate::deadline::Bounded::new(
        Arc::new(CountResponse {
            status: 200,
            body: b"{\"input_tokens\":1}",
            empty_chunks: 0,
        }),
        crate::TimeoutSpec::DEFAULT,
    );
    let (request, _) = request(&provider)?;
    assert!(provider.count_tokens(&request, 0).await.is_err());
    let counted = b"{\"input_tokens\":1}";
    Arc::make_mut(&mut provider.config).max_event_bytes = counted.len();
    provider.transport = crate::deadline::Bounded::new(
        Arc::new(CountResponse {
            status: 200,
            body: counted,
            empty_chunks: counted.len() + 1,
        }),
        crate::TimeoutSpec::DEFAULT,
    );
    assert_eq!(provider.count_tokens(&request, 0).await?.input_tokens, 1);
    Ok(())
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(64))]
    #[test]
    fn count_body_failures_share_the_exchange_attempt_budget(
        failures in 0..6usize, budget in 0..5u8, timeout in proptest::prelude::any::<bool>(),
        partial in proptest::prelude::any::<bool>(), definitive in proptest::prelude::any::<bool>(),
        status in proptest::sample::select(vec![200u16, 201, 202, 400, 413, 408, 429, 503]),
    ) {
        struct CountBody {
            sent: std::sync::atomic::AtomicUsize,
            failures: usize, timeout: bool, partial: bool, definitive: bool, status: u16,
        }
        impl HttpTransport for CountBody {
            fn delay(&self, wait: std::time::Duration) -> BoxProviderFuture<'static, Result<()>> {
                crate::fixtures::delay(wait)
            }
            fn now(&self) -> std::time::Duration {
                crate::fixtures::now()
            }
            fn send(&self, _: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
                let attempt = self.sent.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Box::pin(async move {
                    let body: acyclic_stream::BoxProviderStream<'static, Result<Bytes>> = if attempt < self.failures {
                        let failure = if self.definitive { invalid("malformed count") } else {
                            crate::transport_failure(false, if self.timeout {
                                acyclic_harness_core::ModelTransportFailure::Timeout
                            } else { acyclic_harness_core::ModelTransportFailure::Interrupted }, "count body failed")
                        };
                        Box::pin(futures::stream::iter(self.partial.then(|| Ok(Bytes::from_static(b"{\"input_tokens\":"))))
                            .chain(futures::stream::once(async { Err(failure) })))
                    } else {
                        Box::pin(futures::stream::iter([Ok(Bytes::from_static(b"{\"input_tokens\":7}"))]))
                    };
                    let status = if attempt < self.failures { self.status } else { 200 };
                    Ok(HttpResponse { status, body, request_id: None, retry_after: None })
                })
            }
        }
        let (mut provider, _) = provider(Adapter::Responses, 200);
        Arc::make_mut(&mut provider.config).retry.attempts = budget;
        let transport = Arc::new(CountBody {
            sent: std::sync::atomic::AtomicUsize::new(0), failures, timeout, partial, definitive, status,
        });
        provider.transport = crate::deadline::Bounded::new(transport.clone(), crate::TimeoutSpec::DEFAULT);
        let (request, _) = request(&provider).unwrap();
        let result = futures::executor::block_on(provider.count_tokens(&request, 0));
        let final_failure = definitive || matches!(status, 400 | 413);
        let expected = if final_failure && failures > 0 { 1 } else { (failures + 1).min(usize::from(budget) + 1) };
        proptest::prop_assert_eq!(transport.sent.load(std::sync::atomic::Ordering::SeqCst), expected);
        proptest::prop_assert_eq!(result.is_ok(), failures == 0 || (!final_failure && failures <= usize::from(budget)));
        if let Ok(count) = result {
            proptest::prop_assert_eq!(count.input_tokens, 7);
            proptest::prop_assert_eq!(count.request_digest, request.request_digest());
        }
    }
}

/// Counts sends; a turn whose bound is within its ceiling must send none.
struct Sends(std::sync::atomic::AtomicUsize);
impl HttpTransport for Sends {
    fn delay(&self, wait: std::time::Duration) -> BoxProviderFuture<'static, Result<()>> {
        crate::fixtures::delay(wait)
    }
    fn now(&self) -> std::time::Duration {
        crate::fixtures::now()
    }
    fn send(&self, _: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::pin(async {
            Ok(HttpResponse {
                request_id: None,
                retry_after: None,
                status: 200,
                body: Box::pin(futures::stream::iter([Ok(Bytes::from_static(
                    b"{\"input_tokens\":7}",
                ))])),
            })
        })
    }
}

#[tokio::test]
async fn admission_within_the_ceiling_sends_no_count_request() -> Result<()> {
    for (adapter, _) in fixtures() {
        let (mut provider, _) = provider(adapter, 200);
        let framing = 3;
        Arc::make_mut(&mut provider.config)
            .token_counter
            .as_mut()
            .unwrap()
            .framing_tokens = framing;
        let sends = Arc::new(Sends(std::sync::atomic::AtomicUsize::new(0)));
        provider.transport =
            crate::deadline::Bounded::new(sends.clone(), crate::TimeoutSpec::DEFAULT);
        let sent = || sends.0.load(std::sync::atomic::Ordering::SeqCst);
        let (prepared, _) = request(&provider)?;
        let names = crate::tool_names::Names::new(prepared.request());
        let origin = provider.reasoning_origin(provider.key.as_deref(), None);
        let projected = projection::body_named(
            &provider,
            prepared.request(),
            &names,
            &origin,
            &mut crate::files::FileText::default(),
        )?;
        let body = wire::body(projected, &provider.model.name, adapter)?.len() as u64;
        // Unanchored: the exact body bounds every counted token.
        let bound = provider.count_tokens(&prepared, u64::MAX).await?;
        assert_eq!((bound.input_tokens, sent()), (body + u64::from(framing), 0));
        assert_eq!(bound.request_digest, prepared.request_digest());
        // Negative control: a ceiling the bound cannot prove sends the verified count.
        let counted = provider.count_tokens(&prepared, body).await?;
        assert_eq!((counted.input_tokens, sent()), (7 + u64::from(framing), 1));
        // Anchored on reported usage, a tighter ceiling is still proven locally.
        provider.usage.observe(
            adapter,
            &prepared,
            &json!({"usage":{"input_tokens":5,"prompt_tokens":5,"promptTokenCount":5}}),
        )?;
        let anchored = provider
            .count_tokens(&prepared, 5 + u64::from(framing))
            .await?;
        assert_eq!((anchored.input_tokens, sent()), (5 + u64::from(framing), 1));
        if adapter == Adapter::Anthropic {
            let mut request = prepared.request().clone();
            request.messages.push(ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Part(ModelContentPart::Notices {
                    notices: vec![acyclic_model_render::Notice {
                        source: "files".into(),
                        kind: "files.changed".into(),
                        body: json!({"paths":["&'".repeat(128)]}),
                        wake: false,
                        coalesce_key: None,
                    }],
                }),
            });
            for as_parts in [false, true] {
                if as_parts {
                    let content = &mut request.messages.last_mut().unwrap().content;
                    *content = ModelContent::Parts(content.parts().to_vec());
                }
                // Restore the old semantic-byte calculation as the negative:
                // it fits this ceiling but omits the renderer's XML expansion.
                let old_bound = 5
                    + serde_json::to_vec(request.messages.last().unwrap())
                        .unwrap()
                        .len() as u64
                    + u64::from(framing);
                let expanded =
                    PreparedModelRequest::prepare(request.clone(), Limits::default(), None)?;
                let names = crate::tool_names::Names::new(expanded.request());
                let projected = projection::body_named(
                    &provider,
                    expanded.request(),
                    &names,
                    &origin,
                    &mut crate::files::FileText::default(),
                )?;
                let rendered = wire::body(projected, &provider.model.name, adapter)?;
                let expanded_bound = rendered.len() as u64 + u64::from(framing);
                assert!(expanded_bound > old_bound);
                let before = sent();
                let verified = provider.count_tokens(&expanded, old_bound).await?;
                assert_eq!(
                    (verified.input_tokens, sent()),
                    (7 + u64::from(framing), before + 1)
                );
                // The exact expanded body still permits zero-HTTP admission.
                let local = provider.count_tokens(&expanded, expanded_bound).await?;
                assert_eq!((local.input_tokens, sent()), (expanded_bound, before + 1));
            }
        }
    }
    Ok(())
}
