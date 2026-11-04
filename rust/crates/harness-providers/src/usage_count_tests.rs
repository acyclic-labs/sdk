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
            summary: false,
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
            }], sampling: Default::default(), max_output_tokens: None,
        };
        let first = PreparedModelRequest::prepare(request.clone(), Default::default(), None).unwrap();
        let counter = Counter::default();
        prop_assert_eq!(counter.bound(&first).unwrap(), None);
        // Anthropic reports cached input beside uncached input; both are input.
        counter.observe(crate::Adapter::Anthropic, &first,
            &json!({"usage":{"input_tokens":tokens,"cache_read_input_tokens":cache_read}})).unwrap();
        prop_assert_eq!(counter.bound(&first).unwrap(), Some(tokens + cache_read));
        // Every render option belongs to the observed input identity.
        for variant in 0..4 {
            let mut changed = request.clone();
            match variant {
                0 => changed.max_output_tokens = Some(7),
                1 => changed.sampling.temperature = Some(0.5),
                2 => changed.summary = true,
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
            tools: Vec::new(), sampling: Default::default(), max_output_tokens: None,
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
