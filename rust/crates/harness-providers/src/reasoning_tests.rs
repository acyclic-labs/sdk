use super::*;
use acyclic_harness_core::model::ReasoningKind;
use proptest::prelude::*;

const FAMILIES: [&str; 4] = ["anthropic", "responses", "gemini", "chat"];
const KINDS: [ReasoningKind; 6] = [
    ReasoningKind::Readable,
    ReasoningKind::Summary,
    ReasoningKind::Encrypted,
    ReasoningKind::Redacted,
    ReasoningKind::Signature,
    ReasoningKind::Opaque,
];

fn origin(provider: &str, scope: &str) -> ReasoningOrigin {
    ReasoningOrigin {
        provider: provider.into(),
        model: "source-model".into(),
        credential_scope: (!scope.is_empty()).then(|| *blake3::hash(scope.as_bytes()).as_bytes()),
    }
}

fn check_chat_native(text: &str) {
    use acyclic_harness_core::model::{ModelContent, ModelContentPart, ModelMessage, ModelRole};
    let escaped = serde_json::to_string(text).unwrap();
    let token = format!(
        "\"\\u0061{}\"",
        escaped
            .strip_prefix('"')
            .unwrap()
            .strip_suffix('"')
            .unwrap()
    );
    // Generated parser/renderer input, not a captured response fixture.
    let native = format!("{{ \"reasoning_content\" : {token} }}");
    let (provider, _) = crate::tests::provider(crate::Adapter::Chat, 200);
    let (prepared, _) = crate::tests::request(&provider).unwrap();
    let target = provider.reasoning_origin(provider.key.as_deref(), None);
    let block = ReasoningBlock {
        kind: ReasoningKind::Readable,
        text: Some("display-must-never-be-replayed".into()),
        origin: target.clone(),
        continuity: native.into_bytes(),
        outcome: ReasoningOutcome::PreservedReadable,
    };
    for split in [false, true] {
        let mut request = prepared.request().clone();
        for _ in 0..2 {
            let parts = vec![
                ModelContentPart::Reasoning {
                    block: block.clone(),
                },
                ModelContentPart::Text {
                    text: "answer".into(),
                },
            ];
            if split {
                request
                    .messages
                    .extend(parts.into_iter().map(|part| ModelMessage {
                        role: ModelRole::Assistant,
                        content: ModelContent::Part(part),
                    }));
            } else {
                request.messages.push(ModelMessage {
                    role: ModelRole::Assistant,
                    content: ModelContent::Parts(parts),
                });
            }
        }
        let names = crate::tool_names::Names::new(&request);
        let body = crate::projection::body_named(
            &provider,
            &request,
            &names,
            &target,
            &mut crate::files::FileText::default(),
        )
        .unwrap();
        let bytes = serde_json::to_vec(&body).unwrap();
        let wire = std::str::from_utf8(&bytes).unwrap();
        assert!(
            wire.contains(&token),
            "native JSON string token was re-encoded"
        );
        assert!(!wire.contains("display-must-never-be-replayed"));
        assert_eq!(
            wire.matches(&token).count(),
            2,
            "native blocks were merged or lost"
        );
        let body = serde_json::from_slice(&bytes).unwrap();
        assert!(crate::validate_input(crate::Adapter::Chat, &body).is_empty());
    }
}

#[test]
fn malformed_owned_native_continuity_refuses_before_request_encoding() {
    use acyclic_harness_core::model::{ModelContent, ModelContentPart, ModelMessage, ModelRole};
    for adapter in [
        crate::Adapter::Anthropic,
        crate::Adapter::Responses,
        crate::Adapter::Chat,
    ] {
        let (provider, _) = crate::tests::provider(adapter, 200);
        let (prepared, _) = crate::tests::request(&provider).unwrap();
        let target = provider.reasoning_origin(provider.key.as_deref(), None);
        let mut request = prepared.request().clone();
        request.messages.push(ModelMessage {
            role: ModelRole::Assistant,
            content: ModelContent::Part(ModelContentPart::Reasoning {
                block: ReasoningBlock {
                    kind: ReasoningKind::Readable,
                    text: Some("must-not-become-fallback-text".into()),
                    origin: target.clone(),
                    continuity: b"{}".to_vec(),
                    outcome: ReasoningOutcome::PreservedReadable,
                },
            }),
        });
        let names = crate::tool_names::Names::new(&request);
        let encode = |target: &ReasoningOrigin| {
            crate::projection::body_named(
                &provider,
                &request,
                &names,
                target,
                &mut crate::files::FileText::default(),
            )
        };
        assert!(
            encode(&target).is_err(),
            "owned malformed native data was lost"
        );
        let mut foreign = target;
        foreign.credential_scope = None;
        let body = serde_json::to_value(encode(&foreign).unwrap()).unwrap();
        assert!(crate::validate_input(adapter, &body).is_empty());
        assert!(!body.to_string().contains("must-not-become-fallback-text"));
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn native_continuity_is_exact_and_foreign_projection_never_forges_it(
        text in prop::option::of(".{0,96}"), bytes in prop::collection::vec(any::<u8>(), 0..128),
        kind in 0usize..6, known_scope in any::<bool>(),
    ) {
        check_chat_native(text.as_deref().unwrap_or_default());
        for source in FAMILIES {
            let block = ReasoningBlock {kind: KINDS[kind], text: text.clone(), origin: origin(source, if known_scope {"scope"} else {""}),
                continuity: bytes.clone(), outcome: ReasoningOutcome::PreservedOpaque};
            let encoded = serde_json::to_vec(&block).unwrap();
            let replay: ReasoningBlock = serde_json::from_slice(&encoded).unwrap();
            prop_assert_eq!(&replay, &block);
            for target in FAMILIES {
                let mut destination = origin(target, if known_scope {"scope"} else {""});
                // Switching models within one family/scope preserves continuation.
                destination.model = "different-model".into();
                let projected = project_reasoning(&replay, &destination);
                let native = source == target && known_scope && !bytes.is_empty();
                prop_assert_eq!(matches!(projected, ProjectedReasoning::Native {..}), native);
                if let ProjectedReasoning::Native {continuity, ..} = projected {
                    prop_assert_eq!(continuity, bytes.as_slice());
                } else {
                    prop_assert!(matches!(projected, ProjectedReasoning::Dropped {..}), "opaque foreign block must be dropped");
                }
                destination.credential_scope = Some(*blake3::hash(b"another-org").as_bytes());
                prop_assert!(!matches!(project_reasoning(&replay, &destination), ProjectedReasoning::Native {..}), "credential ownership must match");
            }
        }
    }
}

#[test]
fn provider_only_matching_negative_control_would_cross_credential_ownership() {
    let block = ReasoningBlock {
        kind: ReasoningKind::Encrypted,
        text: None,
        origin: origin("responses", "first-org"),
        continuity: b"org-bound encrypted state".to_vec(),
        outcome: ReasoningOutcome::PreservedOpaque,
    };
    let target = origin("responses", "second-org");
    let broken_native = block.origin.provider == target.provider;
    assert!(broken_native);
    assert_eq!(
        project_reasoning(&block, &target),
        ProjectedReasoning::Dropped {
            outcome: ReasoningOutcome::UnsupportedShape
        }
    );
}

#[test]
fn canonical_json_reencoding_negative_control_changes_native_continuity() {
    let original = br#"{ "type" : "reasoning", "id" : "r", "encrypted_content" : "opaque" }"#;
    let canonical =
        serde_json::to_vec(&serde_json::from_slice::<serde_json::Value>(original).unwrap())
            .unwrap();
    assert_ne!(original.as_slice(), canonical);
    let block = ReasoningBlock {
        kind: ReasoningKind::Encrypted,
        text: None,
        origin: origin("responses", "org"),
        continuity: original.to_vec(),
        outcome: ReasoningOutcome::PreservedOpaque,
    };
    assert_eq!(
        project_reasoning(&block, &origin("responses", "org")),
        ProjectedReasoning::Native {
            continuity: original,
            outcome: ReasoningOutcome::PreservedOpaque,
        }
    );
}

#[test]
fn missing_ownership_never_authorizes_native_replay() {
    let block = ReasoningBlock {
        kind: ReasoningKind::Signature,
        text: None,
        origin: origin("responses", ""),
        continuity: vec![0, 255],
        outcome: ReasoningOutcome::PreservedOpaque,
    };
    assert_eq!(
        project_reasoning(&block, &block.origin),
        ProjectedReasoning::Dropped {
            outcome: ReasoningOutcome::UnsupportedShape
        }
    );
}

#[test]
fn checked_in_reasoning_seed_round_trips_through_the_fuzzer_oracle() {
    crate::fuzzing::assert_stream_invariants(include_bytes!(
        "../fuzz/corpus/provider_stream/reasoning.json"
    ));
}
