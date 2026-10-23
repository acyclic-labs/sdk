use super::*;
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};

#[cfg(not(target_arch = "wasm32"))]
#[path = "request_projection_tests.rs"]
mod projection_reuse;

#[test]
fn tool_call_constructor_preserves_the_existing_wire_shape_and_defaults_metadata() -> Result<()> {
    let call = ToolCall::new("call".into(), "echo".into(), json!({"value":1}));
    assert!(call.metadata.is_null());
    let event = ModelEvent::ToolCall(call.clone());
    let bytes = crate::contract::canonical_json_bytes(&event)?;
    let value: Value = serde_json::from_slice(&bytes).expect("fixture event JSON");
    assert_eq!(
        value,
        json!({"kind":"tool_call","call_id":"call","name":"echo","arguments":{"value":1}})
    );
    assert_eq!(
        serde_json::from_value::<ModelEvent>(value.clone()).expect("legacy event"),
        event
    );
    assert_eq!(
        serde_json::from_value::<ModelContentPart>(value).expect("legacy content"),
        ModelContentPart::ToolCall(call)
    );
    Ok(())
}

fn request() -> Result<ModelRequest> {
    Ok(ModelRequest {
        model: Model::new("mock".into(), "exact".into(), "pinned".into(), json!({}))?,
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text("é\0🦀\r\n".into()),
        }],
        tools: vec![crate::tool::ToolDefinition {
            name: "echo".into(),
            revision: "schema-2".into(),
            description: "Echo".into(),
            input_schema: json!({"type":"string"}),
            output_schema: json!({"type":"string"}),
            projection_schema: json!({"type":"object","properties":{"kind":{"const":"json"},"value":{"type":"string"}},
                "required":["kind","value"],"additionalProperties":false}),
        }],
        max_output_tokens: Some(32),
    })
}
fn exchange(request: &mut ModelRequest) {
    request.messages.extend([
        ModelMessage {
            role: ModelRole::Assistant,
            content: ModelContent::Part(ModelContentPart::ToolCall(crate::model::ToolCall::new(
                "call-1".into(),
                "echo".into(),
                json!("é"),
            ))),
        },
        ModelMessage {
            role: ModelRole::Tool,
            content: ModelContent::Part(ModelContentPart::ToolResult {
                call_id: "call-1".into(),
                name: "echo".into(),
                content: ToolResultContent::Json { value: json!("é") },
            }),
        },
    ]);
}

#[derive(Clone, Default)]
struct PrefixReader {
    bytes: BTreeMap<String, acyclic_stream::OwnedBytes>,
    grants: BTreeSet<String>,
}
impl crate::conversation::ContentResidencyVerifier for PrefixReader {
    fn read<'a>(
        &'a self,
        file: &'a FileRef,
        _: u64,
        _ceiling: Option<&'a crate::Capabilities>,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let capability = file.read_capability(&mut None)?;
            if !self.grants.contains(&capability) {
                return Err(Error::Unauthorized("prefix read is not granted".into()));
            }
            self.bytes
                .get(&capability)
                .cloned()
                .ok_or_else(|| Error::NotFound("prefix version is missing".into()))
        })
    }
}
fn store(
    reader: &mut PrefixReader,
    bytes: acyclic_stream::OwnedBytes,
    media_type: &str,
    index: u8,
) -> Result<FileRef> {
    use crate::conversation::{FileDescriptor, VolumeClass, VolumeOwner, VolumeRef};
    let file = FileRef::new(
        VolumeRef::new(
            crate::resources::ProviderRef::new(
                "test".into(),
                "filesystem".into(),
                "2".into(),
                &mut None,
            )?,
            "private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(crate::AgentId::from_bytes([1; 16])),
            &mut None,
        )?,
        format!(".system/inherited-conversation/model-prefix/{index}.json"),
        format!("generation-{index}"),
        FileDescriptor::from_bytes(&bytes, media_type, &mut None)?,
        "model-prefix.json".into(),
        &mut None,
    )?;
    reader.grants.insert(file.read_capability(&mut None)?);
    reader.bytes.insert(file.read_capability(&mut None)?, bytes);
    Ok(file)
}

#[test]
fn native_media_preserves_configuration_and_validates_bounds() -> Result<()> {
    let mut reader = PrefixReader::default();
    let media = store(&mut reader, vec![1, 2, 3].into(), "image/png", 90)?;
    let options = store(
        &mut reader,
        b"{\"detail\":\"high\"}".to_vec().into(),
        "application/json",
        91,
    )?;
    let binding = NativeConfigurationBinding {
        source: crate::core::EventReference {
            authority: crate::core::Authority {
                kind: crate::core::AggregateKind::Agent,
                id: "native-owner".into(),
            },
            revision: 1,
        },
        configuration: crate::core::ExtensionConfiguration {
            extension: crate::core::ExtensionDependency {
                name: "native.options".into(),
                version: 1,
            },
            schema_digest: [7; 32],
            content: options.clone(),
        },
        implementation_digest: [8; 32],
    };
    let policy = NativeMediaPolicy {
        intent: NativeMediaIntent::Image {
            detail: ImageDetail::High,
        },
        maximum_bytes: 64,
        maximum_work: 64,
        configuration: Some(binding.clone()),
    };
    let result = ModelContent::Part(ModelContentPart::ToolResult {
        call_id: "native-call".into(),
        name: "native.tool".into(),
        content: ToolResultContent::Parts {
            parts: vec![ModelDataPart::File {
                file: media.clone(),
                policy: FileProjectionPolicy::Native(Box::new(policy.clone())),
            }],
        },
    });
    result.validate_limits(Limits::default(), &mut None)?;
    assert!(
        result
            .validate_limits(
                Limits {
                    attachments: 0,
                    ..Limits::default()
                },
                &mut None
            )
            .is_err()
    );
    assert!(result.contains_native_media());
    assert_eq!(
        result.file_refs().collect::<Vec<_>>(),
        vec![&media, &options]
    );
    assert_eq!(
        result.native_configurations().collect::<Vec<_>>(),
        vec![&binding]
    );
    let bytes = crate::contract::canonical_json_bytes(&result)?;
    assert_eq!(
        crate::contract::json_from_slice::<ModelContent>(&bytes)
            .map_err(|error| Error::Invalid(error.to_string()))?,
        result
    );
    assert!(serde_json::from_value::<ToolResultContent>(json!({
        "kind":"parts", "parts":[{"kind":"tool_call","call_id":"nested","name":"native.tool","arguments":{}}]
    })).is_err());
    for value in [0, crate::conversation::MAX_EXACT_JS_INTEGER + 1] {
        assert!(
            NativeMediaPolicy {
                maximum_bytes: value,
                ..policy.clone()
            }
            .validate(&media, Limits::default(), &mut None)
            .is_err()
        );
        assert!(
            NativeMediaPolicy {
                maximum_work: value,
                ..policy.clone()
            }
            .validate(&media, Limits::default(), &mut None)
            .is_err()
        );
    }
    for (intent, invalid) in [
        (
            NativeMediaIntent::Audio {
                maximum_duration_ms: 10,
            },
            NativeMediaIntent::Audio {
                maximum_duration_ms: 0,
            },
        ),
        (
            NativeMediaIntent::Video {
                maximum_duration_ms: 10,
                maximum_frames: 2,
            },
            NativeMediaIntent::Video {
                maximum_duration_ms: 1,
                maximum_frames: u32::MAX,
            },
        ),
        (
            NativeMediaIntent::Document { maximum_pages: 2 },
            NativeMediaIntent::Document { maximum_pages: 0 },
        ),
    ] {
        NativeMediaPolicy {
            intent,
            ..policy.clone()
        }
        .validate(&media, Limits::default(), &mut None)?;
        assert!(
            NativeMediaPolicy {
                intent: invalid,
                ..policy.clone()
            }
            .validate(&media, Limits::default(), &mut None)
            .is_err()
        );
    }
    let mut original = request()?;
    original.messages[0].content = ModelContent::Part(ModelContentPart::File {
        file: media,
        policy: FileProjectionPolicy::Native(Box::new(policy)),
    });
    let high = PreparedModelRequest::prepare(original.clone(), Limits::default(), None)?;
    let ModelContent::Part(ModelContentPart::File {
        policy: FileProjectionPolicy::Native(policy),
        ..
    }) = &mut original.messages[0].content
    else {
        unreachable!()
    };
    policy.intent = NativeMediaIntent::Image {
        detail: ImageDetail::Low,
    };
    let low = PreparedModelRequest::prepare(original, Limits::default(), None)?;
    assert_ne!(low.request_digest(), high.request_digest());
    assert_eq!(
        PreparedModelRequest::decode(low.bytes(), Limits::default(), None)?.request(),
        low.request()
    );
    Ok(())
}

#[tokio::test]
async fn prefix_depth_three_reopens_and_authenticates_history() -> Result<()> {
    let limits = Limits::default();
    let mut reader = PrefixReader::default();
    let attachment = store(
        &mut reader,
        "attachment é".as_bytes().to_vec().into(),
        "text/plain",
        77,
    )?;
    let mut root = request()?;
    root.messages[0].content = ModelContent::Parts(vec![
        ModelContentPart::Text {
            text: "é\0🦀\r\n".into(),
        },
        ModelContentPart::File {
            file: attachment.clone(),
            policy: FileProjectionPolicy::Reference,
        },
    ]);
    exchange(&mut root);
    let mut prepared = PreparedModelRequest::prepare(root, limits, None)?;
    let mut head = store(
        &mut reader,
        ModelPrefix::select(&prepared, None)?.canonical_bytes(None)?,
        ModelPrefix::MEDIA_TYPE,
        0,
    )?;
    for depth in 1..=3 {
        let mut next = prepared.request().clone();
        next.messages.push(ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text(format!("child {depth}: é\0🦀; parent scratch retained")),
        });
        let next = PreparedModelRequest::prepare(next, limits, None)?;
        head = store(
            &mut reader,
            ModelPrefix::select(&next, Some((&head, &prepared)))?.canonical_bytes(None)?,
            ModelPrefix::MEDIA_TYPE,
            depth,
        )?;
        prepared = next;
    }
    let reopened = reader.clone();
    let mut local = request()?;
    local.model.revision = "changed".into();
    let inherited =
        PreparedModelRequest::inherit(local.clone(), &head, &reopened, limits, None).await?;
    let mut expected = local.clone();
    expected.messages = prepared.request().messages.clone();
    expected.messages.extend(local.messages.clone());
    assert_eq!(
        inherited.bytes(),
        PreparedModelRequest::prepare(expected, limits, None)?.bytes()
    );
    let mut incompatible = local.clone();
    incompatible.tools.clear();
    assert!(
        PreparedModelRequest::inherit(incompatible, &head, &reader, limits, None)
            .await
            .is_err()
    );
    assert!(
        PreparedModelRequest::inherit(
            local.clone(),
            &head,
            &reader,
            Limits {
                context_messages: prepared.request().messages.len(),
                ..limits
            },
            None
        )
        .await
        .is_err()
    );
    let mut denied = reopened;
    denied.grants.remove(&head.read_capability(&mut None)?);
    assert!(matches!(
        PreparedModelRequest::inherit(local.clone(), &head, &denied, limits, None).await,
        Err(Error::Unauthorized(_))
    ));
    let mut missing = reader.clone();
    missing
        .bytes
        .remove(&attachment.read_capability(&mut None)?);
    assert!(
        PreparedModelRequest::inherit(local.clone(), &head, &missing, limits, None)
            .await
            .is_err()
    );
    let mut corrupt = reader.clone();
    corrupt
        .bytes
        .insert(head.read_capability(&mut None)?, b"{}".to_vec().into());
    assert!(
        PreparedModelRequest::inherit(local.clone(), &head, &corrupt, limits, None)
            .await
            .is_err()
    );
    let mut changed = prepared.request().clone();
    changed.messages[0].content = ModelContent::Text("later parent mutation".into());
    let changed = PreparedModelRequest::prepare(changed, limits, None)?;
    assert!(ModelPrefix::select(&changed, Some((&head, &prepared))).is_err());
    let impostor = store(
        &mut reader,
        ModelPrefix::select(&changed, None)?.canonical_bytes(None)?,
        ModelPrefix::MEDIA_TYPE,
        88,
    )?;
    let mut descendant = prepared.request().clone();
    descendant.messages.push(ModelMessage {
        role: ModelRole::User,
        content: ModelContent::Text("next".into()),
    });
    let descendant = PreparedModelRequest::prepare(descendant, limits, None)?;
    let mislabeled = store(
        &mut reader,
        ModelPrefix::select(&descendant, Some((&impostor, &prepared)))?.canonical_bytes(None)?,
        ModelPrefix::MEDIA_TYPE,
        89,
    )?;
    assert!(matches!(
        PreparedModelRequest::inherit(local, &mislabeled, &reader, limits, None).await,
        Err(Error::Invalid(_))
    ));
    Ok(())
}

#[test]
fn projection_envelopes_enforce_the_render_bound() -> Result<()> {
    for content in [
        ToolResultContent::Json {
            value: json!("é\0🦀"),
        },
        ToolResultContent::Parts {
            parts: vec![ModelDataPart::Text {
                text: "é\0🦀".into(),
            }],
        },
    ] {
        let exact = crate::contract::canonical_json_bytes(&content)?.len() as u64;
        content.validate_limits(
            Limits {
                render_bytes: exact,
                ..Limits::default()
            },
            &mut None,
        )?;
        assert!(
            content
                .validate_limits(
                    Limits {
                        render_bytes: exact - 1,
                        ..Limits::default()
                    },
                    &mut None
                )
                .is_err()
        );
    }
    Ok(())
}

#[test]
fn malformed_exchanges_and_bounds_fail_closed() -> Result<()> {
    let mut valid = request()?;
    exchange(&mut valid);
    valid.max_output_tokens = None;
    let prepared = PreparedModelRequest::prepare(valid.clone(), Limits::default(), None)?;
    assert_eq!(
        PreparedModelRequest::decode(prepared.bytes(), Limits::default(), None)?.request(),
        &valid
    );
    for mutate in [
        (|request: &mut ModelRequest| {
            request.messages.pop();
        }) as fn(&mut ModelRequest),
        |request| request.messages.swap(1, 2),
        |request| request.messages[1].role = ModelRole::User,
        |request| {
            let ModelContent::Part(ModelContentPart::ToolResult { content, .. }) =
                &mut request.messages[2].content
            else {
                unreachable!()
            };
            *content = ToolResultContent::Json { value: json!(42) };
        },
        |request| request.tools.push(request.tools[0].clone()),
        |request| request.max_output_tokens = Some(0),
        |request| request.model.revision.clear(),
        |request| {
            let ModelContent::Part(ModelContentPart::ToolCall(crate::model::ToolCall {
                arguments,
                ..
            })) = &mut request.messages[1].content
            else {
                unreachable!()
            };
            *arguments = json!(false);
        },
    ] {
        let mut changed = valid.clone();
        mutate(&mut changed);
        assert!(PreparedModelRequest::prepare(changed, Limits::default(), None).is_err());
    }
    let mut oversized = request()?;
    oversized.model.options = json!({"large":"x".repeat(64 * 1024 * 1024 + 1)});
    assert!(PreparedModelRequest::prepare(oversized, Limits::default(), None).is_ok());
    Ok(())
}

#[test]
fn continuation_metadata_is_bounded_and_bound_to_the_prepared_request() -> Result<()> {
    let mut input = request()?;
    exchange(&mut input);
    let ModelContent::Part(ModelContentPart::ToolCall(crate::model::ToolCall { metadata, .. })) =
        &mut input.messages[1].content
    else {
        unreachable!()
    };
    *metadata = json!({"signature":"signed", "nested":[{"provider":"opaque"}]});
    let original = PreparedModelRequest::prepare(input.clone(), Limits::default(), None)?;
    let decoded = PreparedModelRequest::decode(original.bytes(), Limits::default(), None)?;
    assert_eq!(decoded.request(), &input);
    assert!(
        PreparedModelRequest::prepare(
            input.clone(),
            Limits {
                render_bytes: 16,
                ..Limits::default()
            },
            None
        )
        .is_err()
    );
    let ModelContent::Part(ModelContentPart::ToolCall(crate::model::ToolCall { metadata, .. })) =
        &mut input.messages[1].content
    else {
        unreachable!()
    };
    *metadata = json!({"signature":"different"});
    let changed = PreparedModelRequest::prepare(input, Limits::default(), None)?;
    assert_ne!(original.request_digest(), changed.request_digest());
    Ok(())
}

#[cfg(feature = "filesystem")]
#[test]
fn short_request_cannot_hide_excess_string_capacity() -> Result<()> {
    let cancellation = acyclic_fs::CancellationToken::new();
    for spare in [0, 16 * 1024 * 1024] {
        let mut input = request()?;
        input.model.provider.reserve(spare);
        let io = PreparationIo::from_budget(
            acyclic_fs::performance::work_budget(1_000_000, 8 * 1024 * 1024),
            &cancellation,
        )?;
        assert_eq!(
            PreparedModelRequest::prepare(input, Limits::default(), Some(&io)).is_ok(),
            spare == 0,
        );
    }
    Ok(())
}

proptest::proptest! {
    #[test]
    fn pinned_history_requires_the_exact_advertised_revision(revision in "[a-z0-9-]{1,24}", text in ".{0,48}") {
        let mut input = request()?;
        input.tools[0].revision = revision.clone();
        exchange(&mut input);
        let selector = format!("echo@{revision}");
        let ModelContent::Part(ModelContentPart::ToolCall(call)) = &mut input.messages[1].content else { unreachable!() };
        call.name = selector.clone();
        call.arguments = json!(text);
        let ModelContent::Part(ModelContentPart::ToolResult {name, content, ..}) = &mut input.messages[2].content else { unreachable!() };
        *name = selector;
        *content = ToolResultContent::Json {value:json!(text)};
        let prepared = PreparedModelRequest::prepare(input.clone(), Limits::default(), None)?;
        let decoded = PreparedModelRequest::decode(prepared.bytes(), Limits::default(), None)?;
        proptest::prop_assert_eq!(decoded.request(), &input);
        let mut wrong_revision = input.clone();
        wrong_revision.tools[0].revision.push('x');
        proptest::prop_assert!(PreparedModelRequest::prepare(wrong_revision, Limits::default(), None).is_err());
        let mut hidden = input.clone(); hidden.tools.clear();
        proptest::prop_assert!(PreparedModelRequest::prepare(hidden, Limits::default(), None).is_err());
        let ModelContent::Part(ModelContentPart::ToolResult {name, ..}) = &mut input.messages[2].content else { unreachable!() };
        *name = "echo".into();
        proptest::prop_assert!(PreparedModelRequest::prepare(input, Limits::default(), None).is_err());
    }
}

#[test]
fn pinned_request_fuzz_seed_is_a_valid_exact_exchange() -> Result<()> {
    let request: ModelRequest = serde_json::from_slice(include_bytes!(
        "../../../harness-fuzz/corpus/model_request/pinned"
    ))
    .expect("model request seed");
    let prepared = PreparedModelRequest::prepare(request.clone(), Limits::default(), None)?;
    assert_eq!(
        PreparedModelRequest::decode(prepared.bytes(), Limits::default(), None)?.request(),
        &request
    );
    Ok(())
}
