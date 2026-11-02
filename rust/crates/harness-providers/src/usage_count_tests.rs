use super::*;
use acyclic_harness_core::model::{
    Model, ModelContent, ModelMessage, ModelRequest, ModelRole, PreparedModelRequest,
};
use proptest::prelude::*;
use serde_json::json;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    #[test]
    fn usage_anchor_counts_new_input_and_rejects_unrelated_resume_prefixes(
        input in ".{1,128}", delta in ".{1,128}", tokens in 1u64..100_000,
        output in 0u64..u64::MAX,
    ) {
        let mut request = ModelRequest {
            compose_tools: false,
            model: Model::new("openai".into(), "fixture".into(), "1".into()).unwrap(),
            messages: Vec::new(), tools: Vec::new(), sampling: Default::default(), max_output_tokens: None,
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
        let (_, before) = footprint(&first, 1).unwrap();
        let (_, after) = footprint(&next, 1).unwrap();
        prop_assert_eq!(counter.codex_estimate(&next).unwrap(), tokens + (after-before).div_ceil(3));
        request.messages[0].content = ModelContent::Text("unrelated resume".into());
        let resumed = PreparedModelRequest::prepare(request, Default::default(), None).unwrap();
        let (prefix, total) = footprint(&resumed, 1).unwrap();
        let anchor = counter.0.get().unwrap();
        if anchor.unwrap().digest != prefix {
            prop_assert_eq!(counter.codex_estimate(&resumed).unwrap(), total.div_ceil(3));
        }
    }
    #[test]
    fn usage_bound_charges_every_appended_byte_and_never_anchors_a_changed_prefix(
        input in ".{1,128}", delta in ".{1,128}", tokens in 1u64..100_000,
        cache_read in 0u64..100_000,
    ) {
        let mut request = ModelRequest {
            compose_tools: false,
            model: Model::new("anthropic".into(), "fixture".into(), "1".into()).unwrap(),
            messages: vec![ModelMessage { role: ModelRole::User, content: ModelContent::Text(input) }],
            tools: Vec::new(), sampling: Default::default(), max_output_tokens: None,
        };
        let first = PreparedModelRequest::prepare(request.clone(), Default::default(), None).unwrap();
        let counter = Counter::default();
        prop_assert_eq!(counter.bound(&first).unwrap(), None);
        // Anthropic reports cached input beside uncached input; both are input.
        counter.observe(crate::Adapter::Anthropic, &first,
            &json!({"usage":{"input_tokens":tokens,"cache_read_input_tokens":cache_read}})).unwrap();
        prop_assert_eq!(counter.bound(&first).unwrap(), Some(tokens + cache_read));
        request.messages.push(ModelMessage { role: ModelRole::Assistant, content: ModelContent::Text(delta) });
        let next = PreparedModelRequest::prepare(request.clone(), Default::default(), None).unwrap();
        let (_, before) = footprint(&first, 1).unwrap();
        let (_, after) = footprint(&next, 1).unwrap();
        prop_assert_eq!(counter.bound(&next).unwrap(), Some(tokens + cache_read + (after - before)));
        // Negative control: a rewritten prefix is never anchored by its old usage.
        request.messages[0].content = ModelContent::Text("unrelated resume".into());
        let resumed = PreparedModelRequest::prepare(request, Default::default(), None).unwrap();
        prop_assert_eq!(counter.bound(&resumed).unwrap(), None);
    }
    #[test]
    fn local_delta_never_rounds_down_or_discards_authoritative_usage(
        bytes in 0u64..u64::MAX/2, delta in 0u64..u64::MAX/2, tokens in 0u64..u64::MAX/2,
    ) {
        let snapshot = Usage { digest:[1;32], messages:1, bytes, tokens };
        let counted = estimate(Some(snapshot), [1;32], bytes+delta);
        prop_assert!(counted >= tokens);
        prop_assert!(counted.saturating_sub(tokens).saturating_mul(3) >= delta);
    }
}
