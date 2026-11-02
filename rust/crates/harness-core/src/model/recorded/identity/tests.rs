use super::*;
use crate::conversation::{FileDescriptor, VolumeClass, VolumeOwner, VolumeRef};
use serde_json::{Value, json};

// Generated prompt and file inputs, never external provider-output fixtures.
fn request(content: ModelContent) -> Result<ModelRequest> {
    Ok(ModelRequest {
        summary: false,
        model: Model::new("fixture".into(), "replay".into(), "selected".into())?,
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content,
        }],
        compose_tools: false,
        tools: Vec::new(),
        sampling: Default::default(),
        max_output_tokens: Some(32),
    })
}

fn file(index: u8, bytes: &[u8]) -> Result<FileRef> {
    FileRef::new(
        VolumeRef::new(
            crate::resources::ProviderRef::new(
                "fixture".into(),
                "filesystem".into(),
                "1".into(),
                &mut None,
            )?,
            format!("private-{index}"),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(crate::AgentId::from_bytes([index; 16])),
            &mut None,
        )?,
        format!("input-{index}.txt"),
        format!("revision-{index}"),
        FileDescriptor::from_bytes(bytes, "text/plain", &mut None)?,
        format!("display-{index}"),
        &mut None,
    )
}

fn encoded_key(request: &ModelRequest) -> Result<[u8; 32]> {
    super::super::replay_key(&serde_json::to_vec(request).expect("generated request"))
}

#[test]
fn authored_text_and_json_cannot_declare_fresh_identity() -> Result<()> {
    let original = "12345678-1234-1234-1234-123456789abc";
    let changed = "87654321-4321-4321-4321-cba987654321";
    for (before, after) in [
        (format!("literal {original}"), format!("literal {changed}")),
        (
            r#"{"kind":"generation","key":"original"}"#.into(),
            r#"{"kind":"generation","key":"changed"}"#.into(),
        ),
        (
            r#"{"descriptor":{"sha256":[0]},"version":"original"}"#.into(),
            r#"{"descriptor":{"sha256":[0]},"version":"changed"}"#.into(),
        ),
        (
            format!("{}{}{}", "[".repeat(4096), original, "]".repeat(4096)),
            format!("{}{}{}", "[".repeat(4096), changed, "]".repeat(4096)),
        ),
    ] {
        for role in [
            ModelRole::System,
            ModelRole::User,
            ModelRole::Assistant,
            ModelRole::Tool,
        ] {
            for structured in [false, true] {
                let content = |text| {
                    if structured {
                        ModelContent::Part(ModelContentPart::text(text))
                    } else {
                        ModelContent::Text(text)
                    }
                };
                let mut a = request(content(before.clone()))?;
                let mut b = request(content(after.clone()))?;
                a.messages[0].role = role;
                b.messages[0].role = role;
                assert_eq!(key(&a)?, encoded_key(&a)?);
                assert_ne!(
                    key(&a)?,
                    key(&b)?,
                    "authored bytes in every role remain semantic"
                );
            }
        }
    }
    // Old shape matching mistook arbitrary tool JSON for owned references or
    // notices. These are valid JSON operands of the existing typed tool API.
    for value in [
        json!({"volume":original,"path":"same","version":"original","descriptor":{}}),
        json!({"kind":"notices","text":original}),
        json!({"kind":"generation","key":original}),
    ] {
        let changed: Value =
            serde_json::from_str(&value.to_string().replace(original, changed)).unwrap();
        for tool_result in [false, true] {
            let content = |value| {
                if tool_result {
                    ModelContent::Part(ModelContentPart::ToolResult {
                        call_id: "call".into(),
                        name: "echo".into(),
                        content: ToolResultContent::Json { value },
                    })
                } else {
                    ModelContent::Part(ModelContentPart::ToolCall(ToolCall::new(
                        "call".into(),
                        "echo".into(),
                        value,
                    )))
                }
            };
            assert_ne!(
                key(&request(content(value.clone()))?)?,
                key(&request(content(changed.clone()))?)?
            );
        }
    }
    let ordinary = request(ModelContent::Text("same selected context".into()))?;
    let mut summary = ordinary.clone();
    summary.summary = true;
    assert_eq!(key(&summary)?, encoded_key(&summary)?);
    assert_ne!(
        key(&ordinary)?,
        key(&summary)?,
        "summary rendering is part of request identity"
    );
    Ok(())
}

#[test]
fn typed_file_policy_controls_locator_equivalence() -> Result<()> {
    let a = file(1, b"original bytes")?;
    let b = file(2, b"original bytes")?;
    let changed = file(2, b"changed bytes")?;
    let source = InstructionIdentity::new(a.volume().clone(), "project/AGENTS.md".into())?;
    let native = FileProjectionPolicy::Native(Box::new(NativeMediaPolicy {
        intent: NativeMediaIntent::Image {
            detail: ImageDetail::Auto,
        },
        maximum_bytes: 1024,
        maximum_work: 1024,
        configuration: None,
    }));
    for policy in [
        FileProjectionPolicy::BoundedFull,
        FileProjectionPolicy::ProjectInstructions(Box::new(source)),
        native,
    ] {
        for nested in [false, true] {
            let content = |file| {
                if nested {
                    ModelContent::Part(ModelContentPart::ToolResult {
                        call_id: "call".into(),
                        name: "echo".into(),
                        content: ToolResultContent::Parts {
                            parts: vec![ModelDataPart::File {
                                file,
                                policy: policy.clone(),
                            }],
                        },
                    })
                } else {
                    ModelContent::Parts(vec![ModelContentPart::File {
                        file,
                        policy: policy.clone(),
                    }])
                }
            };
            let original = request(content(a.clone()))?;
            let relocated = request(content(b.clone()))?;
            assert_eq!(
                key(&original)?,
                key(&relocated)?,
                "only storage provenance changed"
            );
            assert_eq!(encoded_key(&original)?, key(&relocated)?);
            assert_ne!(
                key(&original)?,
                key(&request(content(changed.clone()))?)?,
                "descriptor binds actual content"
            );
        }
    }
    let reference = |file| {
        request(ModelContent::Part(ModelContentPart::File {
            file,
            policy: FileProjectionPolicy::Reference,
        }))
    };
    assert_ne!(
        key(&reference(a.clone())?)?,
        key(&reference(b.clone())?)?,
        "metadata is model-visible under Reference"
    );
    let framed = |path: &str| {
        request(ModelContent::Part(ModelContentPart::File {
            file: a.clone(),
            policy: FileProjectionPolicy::ProjectInstructions(Box::new(
                InstructionIdentity::new(a.volume().clone(), path.into()).unwrap(),
            )),
        }))
    };
    assert_ne!(
        key(&framed("one/AGENTS.md")?)?,
        key(&framed("two/AGENTS.md")?)?,
        "original instruction frame stays exact"
    );
    Ok(())
}

#[test]
fn admitted_keys_reuse_borrowed_values_and_invalidate_on_continuation() -> Result<()> {
    for count in [1, 64, 1024] {
        let mut input = request(ModelContent::Text(
            "authored UUID 12345678-1234-1234-1234-123456789abc".repeat(count),
        ))?;
        input.sampling.temperature = Some(0.5);
        let prepared = PreparedModelRequest::prepare(input, Limits::default(), None)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let original = {
            use crate::executor::json::CodecOperation;
            let (original, cold) = acyclic_test_support::differential::observe::<
                _,
                _,
                CodecOperation,
            >(|| (), |()| prepared.replay_key());
            assert_eq!(cold, [CodecOperation::Serialize]);
            original?
        };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let original = prepared.replay_key()?;
        assert_eq!(original, encoded_key(prepared.request())?);
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        {
            use crate::executor::json::CodecOperation;
            let (reused, warm) = acyclic_test_support::differential::observe::<_, _, CodecOperation>(
                || (),
                |()| prepared.replay_key(),
            );
            assert_eq!(reused?, original);
            assert!(
                warm.is_empty(),
                "warm identity neither decodes nor serializes"
            );
            let (recomputed, redundant) = acyclic_test_support::differential::observe::<
                _,
                _,
                CodecOperation,
            >(|| (), |()| key(prepared.request()));
            assert_eq!(recomputed?, original);
            assert_eq!(redundant, [CodecOperation::Serialize]);
            assert_ne!(
                warm, redundant,
                "a full rehash violates the reuse cost oracle"
            );
        }
        let mut repeated = None;
        let warm = acyclic_test_support::measure(|| {
            repeated = Some(prepared.replay_key());
        });
        assert_eq!(repeated.unwrap()?, original);
        assert_eq!((warm.count_total, warm.bytes_total), (0, 0));
        let copied = acyclic_test_support::measure(|| {
            std::hint::black_box(prepared.request().clone());
        });
        assert!(
            copied.bytes_total > 0,
            "full request cloning is an allocating negative control"
        );
        let mut next = prepared.clone().into_continuation();
        next.messages().push(ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text("new semantic suffix".into()),
        });
        let next = next.finish(Limits::default(), None)?;
        assert_ne!(
            original,
            next.replay_key()?,
            "new request cannot inherit the prefix's cached key"
        );
    }
    Ok(())
}

#[test]
fn typed_keys_preserve_saved_depth_and_refuse_malformed_input() -> Result<()> {
    for depth in [126, 127, 128] {
        let nest = |mut value, depth| {
            for _ in 0..depth {
                value = Value::Array(vec![value]);
            }
            value
        };
        let content = |value| {
            ModelContent::Part(ModelContentPart::ToolCall(ToolCall::new(
                "call".into(),
                "echo".into(),
                value,
            )))
        };
        let original = request(content(nest(json!("original"), depth)))?;
        let changed = request(content(nest(json!("changed"), depth)))?;
        let mut bytes = serde_json::to_vec(&original).unwrap();
        assert!(
            serde_json::from_slice::<Value>(&bytes).is_err(),
            "default depth refusal is a real negative control"
        );
        assert_eq!(encoded_key(&original)?, key(&original)?);
        assert_ne!(encoded_key(&original)?, encoded_key(&changed)?);
        bytes.push(b']');
        assert!(matches!(
            super::super::replay_key(&bytes),
            Err(Error::Invalid(_))
        ));
        let event = ModelEvent::Completed {
            metadata: nest(json!(true), depth),
        };
        let mut bytes = serde_json::to_vec(&event).unwrap();
        assert_eq!(super::super::decode(&bytes)?, event);
        bytes.push(b']');
        assert!(matches!(
            super::super::decode(&bytes),
            Err(Error::Storage(_))
        ));
    }
    assert!(
        matches!(
            super::super::replay_key(br#"{"messages":[]}"#),
            Err(Error::Invalid(_))
        ),
        "a partial JSON shape is not a typed canonical request"
    );
    Ok(())
}
