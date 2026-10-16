use super::*;
use proptest::prelude::*;

fn pinned(mut request: ModelRequest, revision: &str) -> ModelRequest {
    exchange(&mut request);
    for message in &mut request.messages {
        if let ModelContent::Part(
            ModelContentPart::ToolCall { name, .. } | ModelContentPart::ToolResult { name, .. },
        ) = &mut message.content
        {
            *name = format!("echo@{revision}");
        }
    }
    request
}

fn value() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<i64>().prop_map(|number| json!(number)),
        proptest::collection::vec(any::<char>(), 0..16)
            .prop_map(|text| Value::String(text.into_iter().collect())),
    ]
    .prop_recursive(3, 24, 4, |inner| {
        prop_oneof![
            proptest::collection::vec(inner.clone(), 0..4).prop_map(Value::Array),
            proptest::collection::btree_map("[a-z]{0,6}", inner, 0..4)
                .prop_map(|fields| Value::Object(fields.into_iter().collect())),
        ]
    })
}

proptest! {
    // A pinned exchange resolves exactly its retained revision, independently of
    // advertisement. Canonical request and prefix codecs preserve arbitrary JSON.
    #[test]
    fn pinned_history_is_revision_bound_and_round_trips(
        revision in "[a-z]{1,8}", advertised in any::<bool>(),
        arguments in value(), result in value(),
    ) {
        let mut input = pinned(request().unwrap(), &revision);
        let mut definition = input.tools.remove(0);
        definition.revision = revision.clone();
        definition.input_schema = json!({});
        definition.projection_schema = crate::tool::json_projection_schema(json!({}));
        if advertised {
            input.tools.push(definition.clone());
        } else {
            input.history_tools.push(definition.clone());
        }
        let ModelContent::Part(ModelContentPart::ToolCall { arguments: slot, .. }) = &mut input.messages[1].content else { unreachable!() };
        *slot = arguments;
        let ModelContent::Part(ModelContentPart::ToolResult { content, .. }) = &mut input.messages[2].content else { unreachable!() };
        *content = ToolResultContent::Json { value: result };
        let limits = Limits::default();
        let prepared = PreparedModelRequest::prepare(input.clone(), limits, None).unwrap();
        let decoded = PreparedModelRequest::decode(prepared.bytes(), limits, None).unwrap();
        prop_assert_eq!(decoded.request(), &input);
        prop_assert_eq!(decoded.request_digest(), prepared.request_digest());
        prop_assert_eq!(decoded.request().tools.len(), usize::from(advertised));
        let prefix = ModelPrefix::select(&prepared, None).unwrap();
        let encoded = prefix.canonical_bytes(None).unwrap();
        let decoded: ModelPrefix = serde_json::from_slice(&encoded).unwrap();
        prop_assert_eq!(prefix, decoded);

        let mut unbound = input.clone();
        unbound.tools.clear();
        unbound.history_tools.clear();
        prop_assert!(PreparedModelRequest::prepare(unbound, limits, None).is_err());
        let mut wrong_revision = input.clone();
        for tool in wrong_revision.tools.iter_mut().chain(&mut wrong_revision.history_tools) {
            tool.revision.push_str("-other");
        }
        prop_assert!(PreparedModelRequest::prepare(wrong_revision, limits, None).is_err());
        let mut wrong_schema = input.clone();
        for tool in wrong_schema.tools.iter_mut().chain(&mut wrong_schema.history_tools) {
            tool.input_schema = json!(false);
        }
        prop_assert!(PreparedModelRequest::prepare(wrong_schema, limits, None).is_err());
        let mut wrong_projection = input.clone();
        for tool in wrong_projection.tools.iter_mut().chain(&mut wrong_projection.history_tools) {
            tool.projection_schema = json!(false);
        }
        prop_assert!(PreparedModelRequest::prepare(wrong_projection, limits, None).is_err());
        let mut conflicting = input;
        conflicting.tools = vec![definition.clone()];
        definition.description.push_str(" changed");
        conflicting.history_tools = vec![definition];
        prop_assert!(PreparedModelRequest::prepare(conflicting, limits, None).is_err());
    }
}

#[test]
fn retained_schemas_never_bind_unqualified_hidden_calls() -> Result<()> {
    let mut input = request()?;
    exchange(&mut input);
    input.history_tools = std::mem::take(&mut input.tools);
    assert!(PreparedModelRequest::prepare(input, Limits::default(), None).is_err());
    let mut input = pinned(request()?, "schema-2");
    input.history_tools = std::mem::take(&mut input.tools);
    input.history_tools.push(input.history_tools[0].clone());
    assert!(PreparedModelRequest::prepare(input, Limits::default(), None).is_err());
    Ok(())
}

#[test]
fn history_request_corpus_replays_canonical_admission() -> Result<()> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("fuzz/corpus/model_request");
    let (mut admitted, mut rejected, mut prefixes) = (0, 0, 0);
    for entry in std::fs::read_dir(root).unwrap() {
        let bytes = std::fs::read(entry.unwrap().path()).unwrap();
        if let Ok(input) = serde_json::from_slice::<ModelRequest>(&bytes) {
            match PreparedModelRequest::prepare(input, Limits::default(), None) {
                Ok(prepared) => {
                    let decoded =
                        PreparedModelRequest::decode(prepared.bytes(), Limits::default(), None)?;
                    assert_eq!(decoded.request(), prepared.request());
                    admitted += 1;
                }
                Err(_) => rejected += 1,
            }
        } else if let Ok(prefix) = serde_json::from_slice::<ModelPrefix>(&bytes) {
            let decoded: ModelPrefix = serde_json::from_slice(&prefix.canonical_bytes(None)?)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            assert_eq!(decoded, prefix);
            prefixes += 1;
        } else {
            rejected += 1;
        }
    }
    assert!(admitted > 0 && rejected > 0 && prefixes > 0);
    Ok(())
}

#[tokio::test]
async fn inherited_pinned_history_keeps_schemas_without_advertising_them() -> Result<()> {
    let limits = Limits::default();
    for hidden in [false, true] {
        let mut parent = pinned(request()?, "schema-2");
        if hidden {
            parent.history_tools = std::mem::take(&mut parent.tools);
        }
        let parent = PreparedModelRequest::prepare(parent, limits, None)?;
        let mut reader = PrefixReader::default();
        let prefix = store(
            &mut reader,
            ModelPrefix::select(&parent, None)?.canonical_bytes(None)?,
            ModelPrefix::MEDIA_TYPE,
            1,
        )?;
        let mut child = request()?;
        child.tools.clear();
        let inherited =
            PreparedModelRequest::inherit(child.clone(), &prefix, &reader, limits, None).await?;
        assert!(inherited.request().tools.is_empty());
        assert_eq!(inherited.request().history_tools.len(), 1);
        assert_eq!(
            &inherited.request().messages[..parent.request().messages.len()],
            &parent.request().messages
        );
        PreparedModelRequest::decode(inherited.bytes(), limits, None)?;
        let mut corrupted = inherited.request().clone();
        corrupted.history_tools.clear();
        assert!(PreparedModelRequest::prepare(corrupted, limits, None).is_err());
        child.history_tools = inherited.request().history_tools.clone();
        child.history_tools[0].description.push_str(" changed");
        assert!(
            PreparedModelRequest::inherit(child, &prefix, &reader, limits, None)
                .await
                .is_err()
        );
    }
    Ok(())
}

#[cfg(feature = "filesystem")]
#[test]
fn retained_tool_capacity_is_charged() -> Result<()> {
    let cancellation = acyclic_fs::CancellationToken::new();
    for spare in [0, 64 * 1024] {
        let mut input = pinned(request()?, "schema-2");
        input.history_tools = std::mem::take(&mut input.tools);
        input.history_tools.reserve(spare);
        let io = PreparationIo::from_budget(
            acyclic_fs::performance::work_budget(1_000_000, 8 * 1024 * 1024),
            &cancellation,
        )?;
        assert_eq!(
            PreparedModelRequest::prepare(input, Limits::default(), Some(&io)).is_ok(),
            spare == 0
        );
    }
    Ok(())
}
