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

#[test]
fn every_origin_target_and_readable_opaque_pair_has_an_explicit_projection() {
    for source in FAMILIES {
        for target in FAMILIES {
            for readable in [false, true] {
                let block = ReasoningBlock {
                    kind: ReasoningKind::Encrypted,
                    text: readable.then(|| "verified readable text".into()),
                    origin: origin(source, "org"),
                    continuity: b"exact native payload".to_vec(),
                    outcome: ReasoningOutcome::PreservedOpaque,
                };
                let projection = project_reasoning(&block, &origin(target, "org"));
                if source == target {
                    let ProjectedReasoning::Native { continuity, .. } = projection else {
                        panic!("native pair")
                    };
                    assert!(std::ptr::eq(continuity.as_ptr(), block.continuity.as_ptr()));
                    assert_eq!(continuity, block.continuity);
                } else if readable {
                    let ProjectedReasoning::Readable {
                        text,
                        origin,
                        outcome,
                    } = projection
                    else {
                        panic!("foreign readable pair")
                    };
                    assert_eq!(text, "verified readable text");
                    assert!(std::ptr::eq(origin, &block.origin));
                    assert_eq!(outcome, ReasoningOutcome::PreservedReadable);
                } else {
                    assert_eq!(
                        projection,
                        ProjectedReasoning::Dropped {
                            outcome: ReasoningOutcome::UnsupportedShape
                        }
                    );
                }
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn native_continuity_is_exact_and_foreign_projection_never_forges_it(
        text in prop::option::of(".{0,96}"), bytes in prop::collection::vec(any::<u8>(), 0..128),
        kind in 0usize..6, known_scope in any::<bool>(),
    ) {
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
                } else if let Some(text) = text.as_deref().filter(|text| !text.trim().is_empty()) {
                    let ProjectedReasoning::Readable {text: projected_text, ..} = projected else {panic!("readable text must survive")};
                    prop_assert_eq!(projected_text, text);
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
