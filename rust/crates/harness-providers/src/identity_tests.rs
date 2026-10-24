use crate::{Adapter, Result, tests};
use acyclic_harness_core::{ConversationId, model::*};
use proptest::prelude::*;
use serde_json::json;
use std::sync::Arc;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn conversation_identity_owns_cache_fields_across_turns(id in any::<[u8;16]>()) {
        let conversation = ConversationId::from_bytes(id);
        for adapter in [Adapter::Responses, Adapter::Chat, Adapter::Anthropic, Adapter::Gemini] {
            let (mut provider, _) = tests::provider(adapter, 200);
            provider.model.options.as_object_mut().unwrap().insert("prompt_cache_key".into(), json!("untrusted-option"));
            let (prepared, _) = tests::request(&provider).unwrap();
            let mut request = prepared.request().clone();
            request.conversation_id = Some(conversation);
            let first = crate::projection::body(&provider, &request).unwrap();
            request.messages.push(ModelMessage { role: ModelRole::User, content: ModelContent::Text("next turn".into()) });
            let second = crate::projection::body(&provider, &request).unwrap();
            if matches!(adapter, Adapter::Responses | Adapter::Chat) {
                prop_assert_eq!(first.get("prompt_cache_key").unwrap(), &json!(conversation.to_string()));
                prop_assert_eq!(first.get("prompt_cache_key").unwrap(), second.get("prompt_cache_key").unwrap());
                request.conversation_id = Some(ConversationId::from_bytes(id.map(|byte| byte ^ 0xff)));
                let independent = crate::projection::body(&provider, &request).unwrap();
                prop_assert_ne!(first.get("prompt_cache_key").unwrap(), independent.get("prompt_cache_key").unwrap());
            } else {
                prop_assert!(first.get("prompt_cache_key").is_none());
            }
            request.conversation_id = None;
            prop_assert!(crate::projection::body(&provider, &request).unwrap().get("prompt_cache_key").is_none());
        }
    }

    #[test]
    fn known_account_survives_key_rotation_but_never_crosses_scope(key in "[a-z]{1,32}", other in "[A-Z]{1,32}") {
        let (mut provider, _) = tests::provider(Adapter::Responses, 200);
        let original = provider.reasoning_origin(Some(&key), None);
        prop_assert!(original.credential_scope.is_some());
        prop_assert_eq!(&original, &provider.reasoning_origin(Some(&other), None));
        let block = ReasoningBlock { kind: ReasoningKind::Encrypted, text: Some("readable".into()), origin: original.clone(), continuity: vec![1], outcome: ReasoningOutcome::PreservedOpaque };
        prop_assert!(matches!(crate::project_reasoning(&block, &provider.reasoning_origin(Some(&other), None)), crate::ProjectedReasoning::Native { .. }), "known account lost native continuity");
        prop_assert_ne!(original.credential_scope, provider.reasoning_origin(Some(&key), Some("other-account")).credential_scope);
        Arc::make_mut(&mut provider.config).base_url = "https://other.invalid/v1/".into();
        prop_assert_ne!(original.credential_scope, provider.reasoning_origin(Some(&key), None).credential_scope);
        Arc::make_mut(&mut provider.config).account_id = None;
        let unknown = provider.reasoning_origin(Some(&key), None);
        prop_assert!(unknown.credential_scope.is_none());
        let unknown_block = ReasoningBlock { origin: unknown.clone(), ..block };
        prop_assert!(matches!(crate::project_reasoning(&unknown_block, &unknown), crate::ProjectedReasoning::Readable { .. }), "unknown account did not fall back to readable context");
        let opaque = ReasoningBlock { text: None, ..unknown_block };
        prop_assert!(matches!(crate::project_reasoning(&opaque, &unknown), crate::ProjectedReasoning::Dropped { .. }), "unknown opaque continuity was not dropped");
    }
}

#[test]
fn unknown_account_is_not_authorized_by_a_matching_api_key() {
    let (mut provider, _) = tests::provider(Adapter::Responses, 200);
    Arc::make_mut(&mut provider.config).account_id = None;
    let origin = provider.reasoning_origin(Some("same-key"), None);
    assert!(origin.credential_scope.is_none());
    // The previous key-derived shape gives an unknown account a native scope.
    assert_ne!(
        origin.credential_scope,
        Some(*blake3::hash(b"same-key").as_bytes())
    );
}

#[test]
fn account_identity_is_validated_before_resolution() -> Result<()> {
    let (provider, _) = tests::provider(Adapter::Responses, 200);
    for account in ["", " ", "a\nb", "a\0b"] {
        let mut spec = (*provider.config).clone();
        spec.account_id = Some(account.into());
        assert!(crate::registry::validate_spec("fixture", &spec).is_err());
    }
    for name in [
        "session-id",
        "thread-id",
        "chatgpt-account-id",
        "x-client-request-id",
    ] {
        for name in [name.to_owned(), name.to_uppercase()] {
            let mut spec = (*provider.config).clone();
            spec.headers.insert(name, "injected".into());
            assert!(crate::registry::validate_spec("fixture", &spec).is_err());
        }
    }
    Ok(())
}
