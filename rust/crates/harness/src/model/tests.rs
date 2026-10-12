use super::*;
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};

fn request() -> Result<ModelRequest> {
    Ok(ModelRequest { model: Model::new("mock".into(), "exact".into(), "pinned".into(), json!({}))?,
        messages: vec![ModelMessage { role: ModelRole::User, content: ModelContent::Text("é\0🦀\r\n".into()) }],
        tools: vec![crate::tool::ToolDefinition {
            name: "echo".into(), revision: "schema-2".into(), description: "Echo".into(),
            input_schema: json!({"type":"string"}), output_schema: json!({"type":"string"}),
            projection_schema: json!({"type":"object","properties":{"kind":{"const":"json"},"value":{"type":"string"}},
                "required":["kind","value"],"additionalProperties":false}),
        }], max_output_tokens: Some(32),
    })
}
fn exchange(request: &mut ModelRequest) {
    request.messages.extend([
        ModelMessage { role: ModelRole::Assistant, content: ModelContent::Part(ModelContentPart::ToolCall {
            call_id: "call-1".into(), name: "echo".into(), arguments: json!("é"),
        }) },
        ModelMessage { role: ModelRole::Tool, content: ModelContent::Part(ModelContentPart::ToolResult {
            call_id: "call-1".into(), name: "echo".into(), content: ToolResultContent::Json { value: json!("é") },
        }) },
    ]);
}

#[derive(Clone, Default)]
struct PrefixReader { bytes: BTreeMap<String, acyclic_stream::OwnedBytes>, grants: BTreeSet<String> }
impl crate::conversation::ContentResidencyVerifier for PrefixReader {
    fn read<'a>(&'a self, file: &'a FileRef, _: u64, io: Option<&'a PreparationIo>)
        -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let capability = file.read_capability()?;
            if !self.grants.contains(&capability) { return Err(Error::Unauthorized("prefix read is not granted".into())); }
            self.bytes.get(&capability).cloned().ok_or_else(|| Error::NotFound("prefix version is missing".into()))
        })
    }
}
fn store(reader: &mut PrefixReader, bytes: acyclic_stream::OwnedBytes, media_type: &str, index: u8) -> Result<FileRef> {
    use crate::conversation::{FileDescriptor, VolumeClass, VolumeOwner, VolumeRef};
    let file = FileRef::new(VolumeRef::new(crate::resources::ProviderRef::new("test".into(), "filesystem".into(), "2".into(), &mut None)?,
        "private".into(), VolumeClass::AgentPrivate, VolumeOwner::Agent(crate::AgentId::from_bytes([1; 16])), &mut None)?,
        format!(".system/inherited-conversation/model-prefix/{index}.json"), format!("generation-{index}"),
        FileDescriptor::from_bytes(&bytes, media_type, &mut None)?, "model-prefix.json".into(), &mut None)?;
    reader.grants.insert(file.read_capability()?);
    reader.bytes.insert(file.read_capability()?, bytes);
    Ok(file)
}

#[test]
fn native_data_preserves_options_nonrecursive_shape_and_exact_bounds() -> Result<()> {
    let mut reader = PrefixReader::default();
    let media = store(&mut reader, vec![1, 2, 3].into(), "image/png", 90)?;
    let options = store(&mut reader, b"{\"detail\":\"high\"}".to_vec().into(), "application/json", 91)?;
    let binding = NativeConfigurationBinding {
        source: crate::core::EventReference { authority: crate::core::Authority { kind: crate::core::AggregateKind::Agent, id: "native-owner".into() }, revision: 1 },
        configuration: crate::core::ExtensionConfiguration { extension: crate::core::ExtensionDependency { name: "native.options".into(), version: 1 },
            schema_digest: [7; 32], content: options.clone() }, implementation_digest: [8; 32],
    };
    let policy = NativeMediaPolicy { intent: NativeMediaIntent::Image { detail: ImageDetail::High }, maximum_bytes: 64, maximum_work: 64, configuration: Some(binding.clone()) };
    let file = ModelContentPart::File { file: media.clone(), policy: FileProjectionPolicy::Native(Box::new(policy.clone())) };
    let result = ModelContentPart::ToolResult { call_id: "native-call".into(), name: "native.tool".into(), content: ToolResultContent::Parts {
        parts: vec![ModelDataPart::Text { text: "before".into() }, ModelDataPart::File { file: media.clone(), policy: FileProjectionPolicy::Native(Box::new(policy)) },
            ModelDataPart::Text { text: "after".into() }],
    } };
    for content in [ModelContent::Part(file.clone()), ModelContent::Parts(vec![file]), ModelContent::Part(result.clone()), ModelContent::Parts(vec![result])] {
        content.validate_limits(Limits::default(), &mut None)?;
        assert!(content.validate_limits(Limits { attachments: 0, ..Limits::default() }, &mut None).is_err());
        assert!(content.contains_native_media());
        assert_eq!(content.file_refs().collect::<Vec<_>>(), vec![&media, &options]);
        assert_eq!(content.native_configurations().collect::<Vec<_>>(), vec![&binding]);
        let wire = serde_json::to_value(&content).map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(serde_json::from_value::<ModelContent>(wire).map_err(|error| Error::Invalid(error.to_string()))?, content);
    }
    let nested = json!({"kind":"parts","parts":[{"kind":"tool_call","call_id":"nested","name":"native.tool","arguments":{}}]});
    assert!(serde_json::from_value::<ToolResultContent>(nested).is_err());
    for intent in [NativeMediaIntent::Image { detail: ImageDetail::Auto }, NativeMediaIntent::Audio { maximum_duration_ms: 10 },
        NativeMediaIntent::Video { maximum_duration_ms: 10, maximum_frames: 2 }, NativeMediaIntent::Document { maximum_pages: 2 }]
    {
        let valid = NativeMediaPolicy { intent, maximum_bytes: 64, maximum_work: 64, configuration: None };
        valid.validate(&media, Limits::default(), &mut None)?;
        for value in [0, crate::conversation::MAX_EXACT_JS_INTEGER + 1, u64::MAX] {
            let mut invalid = valid.clone(); invalid.maximum_bytes = value;
            assert!(invalid.validate(&media, Limits::default(), &mut None).is_err());
            let mut invalid = valid.clone(); invalid.maximum_work = value;
            assert!(invalid.validate(&media, Limits::default(), &mut None).is_err());
        }
    }
    for intent in [NativeMediaIntent::Audio { maximum_duration_ms: 0 }, NativeMediaIntent::Video { maximum_duration_ms: 1, maximum_frames: u32::MAX },
        NativeMediaIntent::Document { maximum_pages: 0 }]
    {
        assert!(NativeMediaPolicy { intent, maximum_bytes: 64, maximum_work: 64, configuration: None }.validate(&media, Limits::default(), &mut None).is_err());
    }
    let mut original = request()?;
    original.messages[0].content = ModelContent::Part(ModelContentPart::File { file: media,
        policy: FileProjectionPolicy::Native(Box::new(NativeMediaPolicy { intent: NativeMediaIntent::Image { detail: ImageDetail::Low }, maximum_bytes: 64, maximum_work: 64, configuration: None })) });
    let low = PreparedModelRequest::prepare(original.clone(), Limits::default(), None)?;
    let ModelContent::Part(ModelContentPart::File { policy: FileProjectionPolicy::Native(policy), .. }) = &mut original.messages[0].content else { unreachable!() };
    policy.intent = NativeMediaIntent::Image { detail: ImageDetail::High };
    let high = PreparedModelRequest::prepare(original, Limits::default(), None)?;
    assert_ne!(low.request_digest(), high.request_digest());
    assert_eq!(PreparedModelRequest::decode(low.bytes(), Limits::default(), None)?.request(), low.request());
    Ok(())
}

#[tokio::test]
async fn shared_prefix_depth_three_reopens_and_rejects_mutation_or_missing_grants() -> Result<()> {
    let limits = Limits::default();
    let mut reader = PrefixReader::default();
    let attachment = store(&mut reader, "attachment é".as_bytes().to_vec().into(), "text/plain", 77)?;
    let mut root = request()?;
    root.messages[0].content = ModelContent::Parts(vec![ModelContentPart::Text { text: "é\0🦀\r\n".into() },
        ModelContentPart::File { file: attachment.clone(), policy: FileProjectionPolicy::Reference }]);
    exchange(&mut root);
    let mut prepared = PreparedModelRequest::prepare(root, limits, None)?;
    let mut head = store(&mut reader, ModelPrefix::select(&prepared, None)?.canonical_bytes(None)?, ModelPrefix::MEDIA_TYPE, 0)?;
    for depth in 1..=3 {
        let mut next = prepared.request().clone();
        next.messages.push(ModelMessage { role: ModelRole::User, content: ModelContent::Text(format!("child {depth}: é\0🦀; parent scratch retained")) });
        let next = PreparedModelRequest::prepare(next, limits, None)?;
        head = store(&mut reader, ModelPrefix::select(&next, Some((&head, &prepared)))?.canonical_bytes(None)?, ModelPrefix::MEDIA_TYPE, depth)?;
        prepared = next;
    }
    let reopened = reader.clone();
    let mut local = request()?;
    for child in ["a", "b"] {
        local.messages = vec![ModelMessage { role: ModelRole::User, content: ModelContent::Text(format!("notification; task {child}; identity; workspace; fresh scratch")) }];
        let inherited = PreparedModelRequest::inherit(local.clone(), &head, &reopened, limits, None).await?;
        let mut expected = prepared.request().clone(); expected.messages.extend(local.messages.clone());
        assert_eq!(inherited.bytes(), PreparedModelRequest::prepare(expected, limits, None)?.bytes());
    }
    let mut changed = local.clone(); changed.model.revision = "changed".into();
    let changed = PreparedModelRequest::inherit(changed, &head, &reader, limits, None).await?;
    assert_eq!(changed.request().model.revision, "changed");
    let mut expected = prepared.request().messages.clone(); expected.extend(local.messages.clone());
    assert_eq!(changed.request().messages, expected);
    let mut incompatible = local.clone(); incompatible.tools.clear();
    assert!(PreparedModelRequest::inherit(incompatible, &head, &reader, limits, None).await.is_err());
    let mut denied = reader.clone(); denied.grants.remove(&head.read_capability()?);
    assert!(matches!(PreparedModelRequest::inherit(local.clone(), &head, &denied, limits, None).await, Err(Error::Unauthorized(_))));
    let mut missing = reader.clone(); missing.bytes.remove(&attachment.read_capability()?);
    assert!(PreparedModelRequest::inherit(local.clone(), &head, &missing, limits, None).await.is_err());
    assert!(PreparedModelRequest::inherit(local.clone(), &head, &reader, Limits { context_messages: prepared.request().messages.len(), ..limits }, None).await.is_err());
    let mut corrupt = reader.clone(); corrupt.bytes.insert(head.read_capability()?, b"{}".to_vec().into());
    assert!(PreparedModelRequest::inherit(local, &head, &corrupt, limits, None).await.is_err());
    let mut changed_parent = prepared.request().clone(); changed_parent.messages[0].content = ModelContent::Text("later parent mutation".into());
    let changed_parent = PreparedModelRequest::prepare(changed_parent, limits, None)?;
    assert!(ModelPrefix::select(&changed_parent, Some((&head, &prepared))).is_err());
    let impostor = store(&mut reader, ModelPrefix::select(&changed_parent, None)?.canonical_bytes(None)?, ModelPrefix::MEDIA_TYPE, 88)?;
    let mut descendant = prepared.request().clone(); descendant.messages.push(ModelMessage { role: ModelRole::User, content: ModelContent::Text("next".into()) });
    let descendant = PreparedModelRequest::prepare(descendant, limits, None)?;
    let mislabeled = store(&mut reader, ModelPrefix::select(&descendant, Some((&impostor, &prepared)))?.canonical_bytes(None)?, ModelPrefix::MEDIA_TYPE, 89)?;
    assert!(matches!(PreparedModelRequest::inherit(request()?, &mislabeled, &reader, limits, None).await, Err(Error::Invalid(_))));
    Ok(())
}

#[test]
fn complete_tool_projection_envelopes_enforce_the_exact_render_bound() -> Result<()> {
    for content in [ToolResultContent::Json { value: json!("é\0🦀") }, ToolResultContent::Parts { parts: vec![ModelDataPart::Text { text: "é\0🦀".into() }] }] {
        let exact = crate::contract::canonical_json_bytes(&content)?.len() as u64;
        content.validate_limits(Limits { render_bytes: exact, ..Limits::default() }, &mut None)?;
        assert!(matches!(content.validate_limits(Limits { render_bytes: exact - 1, ..Limits::default() }, &mut None), Err(Error::Invalid(_))));
    }
    Ok(())
}

#[test]
fn malformed_exchanges_and_bounds_fail_closed() -> Result<()> {
    let base = request()?;
    let mut valid = base.clone(); exchange(&mut valid);
    PreparedModelRequest::prepare(valid.clone(), Limits::default(), None)?;
    let mut changed = valid.clone(); changed.max_output_tokens = None;
    let unconstrained = PreparedModelRequest::prepare(changed, Limits::default(), None)?;
    assert!(unconstrained.request().max_output_tokens.is_none());
    assert_eq!(PreparedModelRequest::decode(unconstrained.bytes(), Limits::default(), None)?.request().max_output_tokens, None);
    for mutate in [
        (|request: &mut ModelRequest| { request.messages.pop(); }) as fn(&mut ModelRequest),
        |request| request.messages.swap(1, 2), |request| request.messages[1].role = ModelRole::User,
        |request| { let ModelContent::Part(ModelContentPart::ToolResult { content, .. }) = &mut request.messages[2].content else { unreachable!() }; *content = ToolResultContent::Json { value: json!(42) }; },
        |request| request.tools.push(request.tools[0].clone()), |request| request.max_output_tokens = Some(0), |request| request.model.revision.clear(),
        |request| { let ModelContent::Part(ModelContentPart::ToolCall { arguments, .. }) = &mut request.messages[1].content else { unreachable!() }; *arguments = json!(false); },
    ] {
        let mut changed = valid.clone(); mutate(&mut changed);
        assert!(PreparedModelRequest::prepare(changed, Limits::default(), None).is_err());
    }
    let mut oversized = base;
    oversized.model.options = json!({"large":"x".repeat(64 * 1024 * 1024 + 1)});
    assert!(PreparedModelRequest::prepare(oversized, Limits::default(), None).is_ok());
    Ok(())
}
