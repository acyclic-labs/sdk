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
            let counted = wire::body(&projected, &provider.model.name, adapter);
            let actual = serde_json::to_vec(&counted).unwrap().len() as u64 + 512;
            let first = futures::executor::block_on(provider.count_tokens(&prepared)).unwrap();
            prop_assert_eq!(first.input_tokens, actual + u64::from(framing));
            prop_assert!(first.input_tokens >= actual);
            // Calibration can alter only planning weights, including after resume.
            provider.estimator.observe(1_000, &json!({"usage":{
                "input_tokens":observed,"prompt_tokens":observed,"promptTokenCount":observed
            }}));
            let next = futures::executor::block_on(provider.count_tokens(&prepared)).unwrap();
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
    let count = futures::executor::block_on(provider.count_tokens(&request))?;
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

#[test]
fn a_missing_counter_never_falls_back_to_an_estimate_or_dispatch() -> Result<()> {
    let (mut provider, script) = provider(Adapter::Chat, 200);
    Arc::make_mut(&mut provider.config).token_counter = None;
    let (request, _) = request(&provider)?;
    let error = futures::executor::block_on(provider.count_tokens(&request)).unwrap_err();
    assert_eq!(error.code(), acyclic_harness::wire::ErrorCode::Unsupported);
    assert!(script.sent.lock().unwrap().is_empty());
    Ok(())
}

struct CountResponse {
    status: u16,
    body: &'static [u8],
    empty_chunks: usize,
}
impl HttpTransport for CountResponse {
    fn delay(&self, _: std::time::Duration) -> acyclic_stream::BoxProviderFuture<'static, ()> {
        Box::pin(async {})
    }
    fn send(&self, _: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        Box::pin(async move {
            Ok(HttpResponse {
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
        (401, acyclic_harness::wire::ErrorCode::Unauthorized),
        (403, acyclic_harness::wire::ErrorCode::Unauthorized),
        (408, acyclic_harness::wire::ErrorCode::Unavailable),
        (429, acyclic_harness::wire::ErrorCode::Unavailable),
        (503, acyclic_harness::wire::ErrorCode::Unavailable),
        (400, acyclic_harness::wire::ErrorCode::Invalid),
    ] {
        let (mut provider, _) = provider(Adapter::Responses, 200);
        provider.transport = Arc::new(CountResponse {
            status,
            body: b"fixture-secret",
            empty_chunks: 0,
        });
        let (request, _) = request(&provider)?;
        let error = provider.count_tokens(&request).await.unwrap_err();
        assert_eq!(error.code(), code);
        assert!(error.to_string().contains(&status.to_string()));
        // Only a refusal carries its bounded body excerpt, as on dispatch.
        assert_eq!(
            error.to_string().contains("fixture-secret"),
            code == acyclic_harness::wire::ErrorCode::Unavailable
        );
    }
    for body in [
        b"{\"input_tokens\":-1}".as_slice(),
        b"fixture-secret",
        b"{\"input_tokens\":9007199254740992}",
    ] {
        let (mut provider, _) = provider(Adapter::Responses, 200);
        provider.transport = Arc::new(CountResponse {
            status: 200,
            body,
            empty_chunks: 0,
        });
        let (request, _) = request(&provider)?;
        assert!(provider.count_tokens(&request).await.is_err());
    }
    let (mut provider, _) = provider(Adapter::Responses, 200);
    Arc::make_mut(&mut provider.config).max_event_bytes = 8;
    provider.transport = Arc::new(CountResponse {
        status: 200,
        body: b"{\"input_tokens\":1}",
        empty_chunks: 0,
    });
    let (request, _) = request(&provider)?;
    assert!(provider.count_tokens(&request).await.is_err());
    provider.transport = Arc::new(CountResponse {
        status: 200,
        body: b"",
        empty_chunks: 9,
    });
    let error = provider.count_tokens(&request).await.unwrap_err();
    assert!(error.to_string().contains("item limit"));
    Ok(())
}
