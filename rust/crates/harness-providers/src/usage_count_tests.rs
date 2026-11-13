use super::*;
use acyclic_harness_core::model::{
    Model, ModelContent, ModelMessage, ModelRequest, ModelRole, PreparedModelRequest,
};
use proptest::prelude::*;
use serde_json::json;

#[tokio::test]
async fn cached_input_observations_preserve_total_through_forwarders()
-> acyclic_harness_core::Result<()> {
    use acyclic_harness_core::model::{
        Divergence, DivergenceSink, ModelEvent, ModelProvider, RecordedModel, Recording,
    };
    use futures::TryStreamExt as _;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    struct Quiet;
    impl DivergenceSink for Quiet {
        fn diverged(&self, _: Divergence) {}
    }
    struct Reply {
        body: String,
        sent: AtomicUsize,
    }
    impl crate::HttpTransport for Reply {
        fn send(
            &self,
            _: crate::HttpRequest,
        ) -> acyclic_stream::BoxProviderFuture<'_, acyclic_harness_core::Result<crate::HttpResponse>>
        {
            self.sent.fetch_add(1, Ordering::SeqCst);
            let body = self.body.clone();
            Box::pin(async move {
                Ok(crate::HttpResponse {
                    request_id: None,
                    retry_after: None,
                    status: 200,
                    body: crate::tests::chunks(body, 1),
                })
            })
        }
        fn delay(
            &self,
            duration: std::time::Duration,
        ) -> acyclic_stream::BoxProviderFuture<'static, acyclic_harness_core::Result<()>> {
            crate::fixtures::delay(duration)
        }
        fn now(&self) -> std::time::Duration {
            crate::fixtures::now()
        }
    }
    for (adapter, fixture) in crate::fixtures::fixtures() {
        let cases = if adapter == crate::Adapter::Anthropic {
            vec![
                (json!(11), Some(23)),
                (json!("invalid"), None),
                (json!(u64::MAX), None),
            ]
        } else {
            vec![(json!(4), Some(5))]
        };
        for (cached, total) in cases {
            let body = match adapter {
                crate::Adapter::Anthropic => fixture.replace("\"input_tokens\":5", &format!("\"input_tokens\":5,\"cache_read_input_tokens\":{cached},\"cache_creation_input_tokens\":7")),
                crate::Adapter::Responses => fixture.replace("\"input_tokens\":5", &format!("\"input_tokens\":5,\"input_tokens_details\":{{\"cached_tokens\":{cached}}}")),
                crate::Adapter::Chat => fixture.replace("\"prompt_tokens\":5", &format!("\"prompt_tokens\":5,\"prompt_tokens_details\":{{\"cached_tokens\":{cached}}}")),
                crate::Adapter::Gemini => fixture.replace("\"promptTokenCount\":5", &format!("\"promptTokenCount\":5,\"cachedContentTokenCount\":{cached}")),
            };
            let transport = Arc::new(Reply {
                body,
                sent: AtomicUsize::new(0),
            });
            let provider =
                crate::tests::provider_with(adapter, "http://127.0.0.1:1234/v1", transport.clone());
            let (request, dispatch) = crate::tests::request(&provider)?;
            let recorded = Arc::new(RecordedModel::new(
                Arc::new(provider),
                Arc::new(Recording::default()),
                Arc::new(Quiet),
            ));
            let wrapped = acyclic_stream::testing::Counted::new(recorded);
            let events: Vec<_> = wrapped.generate(&request, dispatch).try_collect().await?;
            let metadata = events
                .iter()
                .find_map(|event| match event {
                    ModelEvent::Completed { metadata } => Some(metadata),
                    _ => None,
                })
                .expect("the native stream completed");
            let raw = match adapter {
                crate::Adapter::Chat => &metadata["usage"]["prompt_tokens"],
                crate::Adapter::Gemini => &metadata["usage"]["promptTokenCount"],
                _ => &metadata["usage"]["input_tokens"],
            };
            assert_eq!(
                raw.as_u64(),
                Some(5),
                "billing counters must remain original"
            );
            assert_eq!(
                wrapped.reported_input_tokens(&request.request().model, metadata)?,
                total,
                "{adapter:?}"
            );
            assert_eq!(
                transport.sent.load(Ordering::SeqCst),
                1,
                "interpreting completed usage must not make another request"
            );
        }
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    #[test]
    fn usage_anchor_counts_new_input_and_rejects_unrelated_resume_prefixes(
        input in ".{1,128}", delta in ".{1,128}", tokens in 1u64..100_000,
        output in 0u64..u64::MAX,
    ) {
        let mut request = ModelRequest {
            summary: false,
            compose_tools: false,
            model: Model::new("openai".into(), "fixture".into(), "1".into()).unwrap(),
            messages: Vec::new(), tools: Vec::new(), sampling: Default::default(),
        };
        request.messages = vec![ModelMessage { role: ModelRole::User, content: ModelContent::Text(input) }];
        let first = PreparedModelRequest::prepare(request.clone(), Default::default(), None).unwrap();
        let counter = Counter::default();
        counter.observe(crate::Adapter::Responses, &first, &json!({"usage":{"input_tokens":tokens,"output_tokens":output}})).unwrap();
        prop_assert_eq!(counter.codex_estimate(&first).unwrap(), tokens);
        // Generated assistant content is new input exactly once, independent
        // of the output-token count from the response which produced it.
        request.messages.push(ModelMessage { role: ModelRole::Assistant, content: ModelContent::Text(delta) });
        let next = PreparedModelRequest::prepare(request.clone(), Default::default(), None).unwrap();
        let before = first.input_bytes().unwrap();
        let after = next.input_bytes().unwrap();
        prop_assert_eq!(counter.codex_estimate(&next).unwrap(), tokens + (after-before).div_ceil(3));
        request.messages[0].content = ModelContent::Text("unrelated resume".into());
        let resumed = PreparedModelRequest::prepare(request, Default::default(), None).unwrap();
        let total = resumed.input_bytes().unwrap();
        prop_assert_eq!(counter.codex_estimate(&resumed).unwrap(), total.div_ceil(3));
    }
    #[test]
    fn usage_bound_charges_every_appended_byte_and_never_anchors_a_changed_prefix(
        input in "\\S.{0,127}", delta in "\\S.{0,127}", tokens in 1u64..100_000,
        cache_read in 0u64..100_000,
    ) {
        let mut request = ModelRequest {
            summary: false,
            compose_tools: true,
            model: Model::new("anthropic".into(), "fixture".into(), "1".into()).unwrap(),
            messages: vec![ModelMessage { role: ModelRole::User, content: ModelContent::Text(input) }],
            tools: vec![acyclic_harness_core::tool::ToolDefinition {
                name: "extra".into(), revision: "1".into(), description: "Extra capability".into(),
                input_schema: json!({"type":"object"}),
                output_schema: json!({}), projection_schema: json!({}),
            }], sampling: Default::default(),
        };
        let first = PreparedModelRequest::prepare(request.clone(), Default::default(), None).unwrap();
        let counter = Counter::default();
        prop_assert_eq!(counter.bound(&first).unwrap(), None);
        // Anthropic reports cached input beside uncached input; both are input.
        counter.observe(crate::Adapter::Anthropic, &first,
            &json!({"usage":{"input_tokens":tokens,"cache_read_input_tokens":cache_read}})).unwrap();
        prop_assert_eq!(counter.bound(&first).unwrap(), Some(tokens + cache_read));
        // Every render option belongs to the observed input identity.
        for variant in 0..3 {
            let mut changed = request.clone();
            match variant {
                0 => changed.sampling.temperature = Some(0.5),
                1 => changed.summary = true,
                _ => changed.tools[0].description.push_str(" changed"),
            }
            let changed = PreparedModelRequest::prepare(changed, Default::default(), None).unwrap();
            prop_assert_eq!(counter.bound(&changed).unwrap(), None, "changed configuration reused observed input");
        }
        request.messages.push(ModelMessage { role: ModelRole::Assistant, content: ModelContent::Text(delta) });
        let next = PreparedModelRequest::prepare(request.clone(), Default::default(), None).unwrap();
        let before = first.input_bytes().unwrap();
        let after = next.input_bytes().unwrap();
        prop_assert_eq!(counter.bound(&next).unwrap(), Some(tokens + cache_read + (after - before)));
        // The same registered catalog exposes more native schemas when code
        // composition is disabled. Its old, smaller usage cannot admit it.
        prop_assert_eq!(next.request().model_tools().count(), 0);
        request.compose_tools = false;
        let exposed = PreparedModelRequest::prepare(request.clone(), Default::default(), None).unwrap();
        prop_assert_eq!(exposed.request().model_tools().count(), 1);
        prop_assert_eq!(counter.bound(&exposed).unwrap(), None);
        request.compose_tools = true;
        // Negative control: a rewritten prefix is never anchored by its old usage.
        request.messages[0].content = ModelContent::Text("unrelated resume".into());
        let resumed = PreparedModelRequest::prepare(request, Default::default(), None).unwrap();
        prop_assert_eq!(counter.bound(&resumed).unwrap(), None);
    }
    #[test]
    fn local_delta_never_rounds_down_or_discards_authoritative_usage(
        input in "\\S.{0,63}", delta in "\\S.{0,63}", tokens in any::<u64>(),
    ) {
        let mut request = ModelRequest {
            summary: false, compose_tools: false,
            model: Model::new("openai".into(), "fixture".into(), "1".into()).unwrap(),
            messages: vec![ModelMessage { role: ModelRole::User, content: ModelContent::Text(input) }],
            tools: Vec::new(), sampling: Default::default(),
        };
        let first = PreparedModelRequest::prepare(request.clone(), Default::default(), None).unwrap();
        let counter = Counter::default();
        prop_assert_eq!(counter.codex_estimate(&first).unwrap(), first.input_bytes().unwrap().div_ceil(3));
        counter.observe(crate::Adapter::Responses, &first, &json!({"usage":{"input_tokens":tokens}})).unwrap();
        request.messages.push(ModelMessage { role: ModelRole::Assistant, content: ModelContent::Text(delta) });
        let next = PreparedModelRequest::prepare(request, Default::default(), None).unwrap();
        let added = next.input_bytes().unwrap() - first.input_bytes().unwrap();
        let counted = counter.codex_estimate(&next).unwrap();
        prop_assert_eq!(counted, tokens.saturating_add(added.div_ceil(3)));
        prop_assert!(counted >= tokens);
        prop_assert!(counted == u64::MAX || counted.saturating_sub(tokens).saturating_mul(3) >= added);
    }
}
