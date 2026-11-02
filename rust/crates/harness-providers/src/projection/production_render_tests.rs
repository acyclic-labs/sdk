//! Cache boundaries through the original request producer, including its role fold.
use crate::{Adapter, ResolvedModel};
use acyclic_harness_core::model::{
    ModelContent, ModelMessage, ModelRequest, ModelRole, PreparedModelRequest,
};
use proptest::prelude::*;
use serde_json::{Value, json, value::RawValue};
use std::collections::BTreeMap;

fn encode(provider: &ResolvedModel, request: &ModelRequest) -> Vec<u8> {
    let prepared =
        PreparedModelRequest::prepare(request.clone(), Default::default(), None).unwrap();
    let names = crate::tool_names::Names::new(prepared.request());
    let origin = provider.reasoning_origin(provider.key.as_deref(), None);
    futures::executor::block_on(crate::dispatch::encode(
        provider, &prepared, &names, None, &origin,
    ))
    .unwrap()
    .bytes
}

fn conversation_field(family: Adapter) -> &'static str {
    match family {
        Adapter::Responses => "input",
        Adapter::Gemini => "contents",
        _ => "messages",
    }
}

// Borrow the exact array span from the production bytes. Parsing is an oracle
// here, never a second encoder or a production prefix-discovery path.
fn prefix(bytes: &[u8], family: Adapter) -> &[u8] {
    let root: BTreeMap<&str, &RawValue> = serde_json::from_slice(bytes).unwrap();
    let conversation = root[conversation_field(family)].get();
    assert!(conversation.starts_with('[') && conversation.ends_with(']'));
    let end = conversation.as_ptr() as usize - bytes.as_ptr() as usize + conversation.len() - 1;
    &bytes[..end]
}

fn text(role: ModelRole, value: String) -> ModelMessage {
    ModelMessage {
        role,
        content: ModelContent::Text(value),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    #[test]
    fn production_turn_boundaries_and_call_controls_preserve_cache_prefixes(
        first in ".{0,64}", next in ".{0,64}", output in 1_u32..1025,
        temperature in 0_u32..201, top in 1_u32..101,
    ) {
        let code = acyclic_harness_core::tools::catalog()
            .find(|tool| tool.name == "code").unwrap().definition().unwrap();
        for family in [Adapter::Chat, Adapter::Responses, Adapter::Anthropic, Adapter::Gemini] {
            let (provider, _) = crate::tests::provider(family, 200);
            let (prepared, _) = crate::tests::request(&provider).unwrap();
            let mut request = prepared.request().clone();
            request.tools.push(code.clone());
            request.messages[1] = text(ModelRole::User, format!("first:{first}"));
            let original = encode(&provider, &request);
            let frozen = prefix(&original, family);

            request.max_output_tokens = Some(output);
            request.sampling = acyclic_harness_core::model::ModelSampling {
                temperature: Some(f64::from(temperature) / 100.0),
                top_p: Some(f64::from(top) / 100.0),
                seed: matches!(family, Adapter::Chat | Adapter::Gemini).then_some(7),
            };
            let changed = encode(&provider, &request);
            prop_assert_eq!(prefix(&changed, family), frozen,
                "per-call controls must not enter the cacheable prefix");
            let root: BTreeMap<&str, &RawValue> = serde_json::from_slice(&changed).unwrap();
            let control = match family {
                Adapter::Chat => "max_completion_tokens",
                Adapter::Responses => "max_output_tokens",
                Adapter::Anthropic => "max_tokens",
                Adapter::Gemini => "generationConfig",
            };
            prop_assert!(root[control].get().as_ptr() as usize
                > root[conversation_field(family)].get().as_ptr() as usize);
            let value: Value = serde_json::from_slice(&changed).unwrap();
            let limit = if family == Adapter::Gemini {
                &value[control]["maxOutputTokens"]
            } else { &value[control] };
            prop_assert_eq!(limit, &json!(output));

            // A completed user turn remains an exact prefix when a subsequent
            // assistant/user pair is appended by the real semantic producer.
            request.messages.extend([
                text(ModelRole::Assistant, "accepted answer".into()),
                text(ModelRole::User, format!("next:{next}")),
            ]);
            let appended = encode(&provider, &request);
            prop_assert!(prefix(&appended, family).starts_with(frozen));
            request.messages[1] = text(ModelRole::User, format!("changed:{first}"));
            let mutated = encode(&provider, &request);
            prop_assert!(!prefix(&mutated, family).starts_with(frozen),
                "changing retained source must fail the same prefix oracle");

            // Semantic messages are not always frozen wire containers. These
            // actual same-role folds must invalidate a purported append edge.
            if matches!(family, Adapter::Anthropic | Adapter::Gemini | Adapter::Chat) {
                request.messages.truncate(2);
                let role = if family == Adapter::Chat { ModelRole::Assistant } else { ModelRole::User };
                if family == Adapter::Chat {
                    request.messages.push(text(role, format!("answer:{first}")));
                }
                let before_merge = encode(&provider, &request);
                request.messages.push(text(role, format!("merged:{next}")));
                let after_merge = encode(&provider, &request);
                prop_assert!(!prefix(&after_merge, family).starts_with(prefix(&before_merge, family)),
                    "merging into a wire container cannot claim a frozen-item append edge");
            }
        }
    }
}
