use super::*;
use crate::{Adapter, ResolvedModel};
use acyclic_harness_core::{
    approvals::ApprovalPolicy,
    model::{ModelContent, ModelMessage, ModelRole, ModelSampling, PreparedModelRequest},
    profile::{AgentOverride, ModelProfile, ProjectLayer, RoleFile, resolve},
};
use proptest::prelude::*;
use serde::de::{MapAccess, Visitor};
use serde_json::{Map, json};
use std::fmt;

// The former map-based serialization as a semantic oracle: every root field,
// the declarations and the conversation in one map, whatever its key order.
fn former(body: &Request<'_>) -> Value {
    let mut map = Map::new();
    for (name, value) in body.fields.header().chain(body.fields.call_controls()) {
        map.insert(name.to_owned(), value.clone());
    }
    if let Some(tools) = body.fields.tools() {
        map.insert("tools".into(), serde_json::from_str(tools.get()).unwrap());
    }
    let mut messages = serde_json::to_value(&body.messages).unwrap();
    if let Some(instruction) = &body.summary {
        let instruction = instruction.as_ref();
        let contents = messages.as_array_mut().unwrap();
        if body.target.family == Adapter::Gemini
            && contents.last().is_some_and(|last| last["role"] == "user")
        {
            contents.last_mut().unwrap()["parts"]
                .as_array_mut()
                .unwrap()
                .push(json!({"text":instruction}));
        } else {
            contents.push(if body.target.family == Adapter::Gemini {
                json!({"role":"user","parts":[{"text":instruction}]})
            } else {
                json!({"role":"user","content":instruction})
            });
        }
        let (key, value) = match body.target.family {
            Adapter::Gemini => (
                "toolConfig",
                json!({"functionCallingConfig":{"mode":"NONE"}}),
            ),
            Adapter::Anthropic => ("tool_choice", json!({"type":"none"})),
            _ => ("tool_choice", json!("none")),
        };
        map.insert(key.into(), value);
    }
    map.insert(conversation(body.target.family).into(), messages);
    Value::Object(map)
}

fn conversation(family: Adapter) -> &'static str {
    match family {
        Adapter::Responses => "input",
        Adapter::Gemini => "contents",
        Adapter::Anthropic | Adapter::Chat => "messages",
    }
}

// Preserve root key order and reject duplicates instead of letting Value hide
// them. Apply the same decoder to Gemini's nested generationConfig object.
struct Object(Vec<(String, Box<RawValue>)>);
impl<'de> serde::Deserialize<'de> for Object {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Fields;
        impl<'de> Visitor<'de> for Fields {
            type Value = Object;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("unique object fields")
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Object, M::Error> {
                let mut fields: Vec<(String, Box<RawValue>)> = Vec::new();
                while let Some(entry) = map.next_entry::<String, Box<RawValue>>()? {
                    if fields.iter().any(|(name, _)| name == &entry.0) {
                        return Err(serde::de::Error::custom("duplicate field"));
                    }
                    fields.push(entry);
                }
                Ok(Object(fields))
            }
        }
        deserializer.deserialize_map(Fields)
    }
}

fn freeze_effort(provider: &ResolvedModel, fields: &mut Fields) {
    let mode = match provider.config.adapter {
        Adapter::Responses | Adapter::Chat => "effort",
        Adapter::Anthropic => "adaptive",
        Adapter::Gemini => "level",
    };
    let source = format!(
        "---\nversion: 1\ntools: []\nreasoning: {{mode: {mode}, effort: high}}\ncache: automatic\nbudget_tokens: 4096\nwatermark_tokens: 2000\n---\n"
    );
    let model = ModelProfile::parse(
        &source,
        "---\nversion: 1\nkey: fixture/model\n---\n",
        provider.capacity,
        "renderer-v1",
    )
    .unwrap();
    let role = RoleFile::parse("---\nversion: 1\nname: worker\nmodel: fixture/model\ncontext: fork\nmay_spawn: []\nmax_output_tokens: 64\n---\n").unwrap();
    let project = ProjectLayer::parse("---\nversion: 1\n---\n").unwrap();
    let approvals = ApprovalPolicy::default().bind().unwrap();
    let agent = AgentOverride::parse("---\nversion: 1\n---\n", &approvals).unwrap();
    let profile = resolve(&model, &role, &project, &agent).unwrap();
    let binding = profile
        .bind_model(&provider.model, provider.capacity)
        .unwrap();
    super::super::profile::project(provider.config.adapter, &binding, fields);
}

// Print one repeatable configuration-byte identity for the two feature builds.
// Empty conversation arrays isolate configuration from native replay bytes,
// which remain owned byte-for-byte by their original producers.
fn configuration_pin() {
    let probe: Map<String, Value> = [("z".into(), Value::Null), ("a".into(), Value::Null)]
        .into_iter()
        .collect();
    println!(
        "configuration_map_preserves_order={}",
        probe.keys().next().unwrap() == "z"
    );
    let mut digest = blake3::Hasher::new();
    for family in [
        Adapter::Responses,
        Adapter::Chat,
        Adapter::Anthropic,
        Adapter::Gemini,
    ] {
        let (provider, _) = crate::tests::provider(family, 200);
        let (request, _) = crate::tests::request(&provider).unwrap();
        let mut request = request.request().clone();
        request.sampling = ModelSampling {
            temperature: Some(0.25),
            top_p: Some(0.75),
            seed: matches!(family, Adapter::Chat | Adapter::Gemini).then_some(42),
        };
        let names = crate::tool_names::Names::new(&request);
        let origin = provider.reasoning_origin(provider.key.as_deref(), None);
        let mut body = super::super::body_named(
            &provider,
            &request,
            &names,
            &origin,
            &mut crate::files::FileText::default(),
        )
        .unwrap();
        freeze_effort(&provider, &mut body.fields);
        body.messages.clear();
        let bytes = body.encode().unwrap();
        println!(
            "configuration_wire={}",
            std::str::from_utf8(&bytes).unwrap()
        );
        digest.update(&(bytes.len() as u64).to_be_bytes());
        digest.update(&bytes);
    }
    println!("configuration_bytes_blake3={}", digest.finalize().to_hex());
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    #[test]
    fn deterministic_configuration_preserves_protocol_and_progress(
        text in ".{1,80}", temperature in 0_u32..201, top in 1_u32..101,
        output in 1_u32..1025, seed in any::<i32>(),
    ) {
        static PIN: std::sync::Once = std::sync::Once::new();
        PIN.call_once(configuration_pin);
        // Generated request inputs, not recordings of provider exchanges.
        for family in [Adapter::Responses, Adapter::Chat, Adapter::Anthropic, Adapter::Gemini] {
            let (provider, _) = crate::tests::provider(family, 200);
            let (prepared, _) = crate::tests::request(&provider).unwrap();
            for summary in [false, true] {
                let mut request = prepared.request().clone();
                request.messages[1].content = ModelContent::Text(text.clone());
                request.summary = summary;
                request.tools.push(acyclic_harness_core::tools::catalog().find(|tool| tool.name == "code").unwrap().definition().unwrap());
                if summary {
                    request.messages.push(ModelMessage { role: ModelRole::Assistant, content: ModelContent::Text("accepted reply".into()) });
                    request.messages.push(ModelMessage { role: ModelRole::User, content: ModelContent::Text("Summarize the selected context.".into()) });
                }
                for changed in [false, true] {
                    request.sampling = if changed { ModelSampling {
                        temperature: Some(f64::from(temperature) / 100.0),
                        top_p: Some(f64::from(top) / 100.0),
                        seed: matches!(family, Adapter::Chat | Adapter::Gemini).then_some(i64::from(seed)),
                    } } else { ModelSampling::default() };
                    request.max_output_tokens = Some(if changed { output } else { 1024 });
                    let admitted = PreparedModelRequest::prepare(request.clone(), Default::default(), None).unwrap();
                    let names = crate::tool_names::Names::new(admitted.request());
                    let origin = provider.reasoning_origin(provider.key.as_deref(), None);
                    let mut body = super::super::body_named(&provider, admitted.request(), &names, &origin, &mut crate::files::FileText::default()).unwrap();
                    // Use the original profile projector to compose the frozen
                    // effort with these independently admitted call controls.
                    freeze_effort(&provider, &mut body.fields);
                    let encoded = super::super::cache::encode(&body, &provider, &origin).unwrap();
                    let wire = &encoded.bytes;
                    let actual: Value = serde_json::from_slice(wire).unwrap();
                    prop_assert_eq!(&actual, &former(&body));
                    // The header, then the conversation, then the call trailer.
                    let header = body.header().unwrap();
                    let tail = body.tail().unwrap();
                    prop_assert!(wire.starts_with(header.bytes()) && wire.ends_with(tail.bytes()));
                    prop_assert_eq!(crate::validate_input(family, &actual), Vec::<&str>::new());
                    let object: Object = serde_json::from_slice(wire).unwrap();
                    let keys: Vec<_> = object.0.iter().map(|(key, _)| key.as_str()).collect();
                    let (limit, effort) = match family {
                        Adapter::Chat => ("max_completion_tokens", "/reasoning_effort"),
                        Adapter::Responses => ("max_output_tokens", "/reasoning/effort"),
                        Adapter::Anthropic => ("max_tokens", "/output_config/effort"),
                        Adapter::Gemini => ("maxOutputTokens", "/generationConfig/thinkingConfig/thinkingLevel"),
                    };
                    prop_assert_eq!(actual.pointer(effort), Some(&json!("high")));
                    let mut expected = match family {
                        Adapter::Responses => vec!["model", "stream", "include", "store", "prompt_cache_key", "reasoning"],
                        Adapter::Chat => vec!["model", "stream", "stream_options", "reasoning_effort"],
                        Adapter::Anthropic => vec!["model", "stream", "system", "cache_control", "thinking", "output_config"],
                        Adapter::Gemini => vec!["systemInstruction"],
                    };
                    let header_keys = expected.len();
                    if body.fields.tools().is_some() { expected.push("tools"); }
                    expected.push(conversation(family));
                    if family == Adapter::Gemini { expected.push("generationConfig"); }
                    else {
                        expected.push(limit);
                        if changed {
                            expected.extend(["temperature", "top_p"]);
                            if family == Adapter::Chat { expected.push("seed"); }
                        }
                    }
                    if summary { expected.push(if family == Adapter::Gemini { "toolConfig" } else { "tool_choice" }); }
                    prop_assert_eq!(&keys, &expected);
                    // Observation and direct dispatch select the same encoder.
                    prop_assert_eq!(wire, &body.encode().unwrap());
                    prop_assert!(actual["tools"].as_array().is_some_and(|tools| !tools.is_empty()));
                    if summary {
                        prop_assert!(matches!(body.summary, Some(std::borrow::Cow::Borrowed(_))));
                        let instruction = body.summary.as_deref().unwrap();
                        let messages = actual[conversation(family)].as_array().unwrap();
                        prop_assert_eq!(messages.len(), body.messages.len() + 1);
                        prop_assert_eq!(messages.last().unwrap()[if family == Adapter::Gemini { "parts" } else { "content" }].clone(),
                            if family == Adapter::Gemini { json!([{"text":instruction}]) } else { json!(instruction) });
                        let ordinary = acyclic_model_render::Tail::new(&body.fields).unwrap();
                        prop_assert!(acyclic_model_render::render_summary(&body.header().unwrap(), &body.messages, instruction, &ordinary, &mut Vec::new()).is_err());
                    }
                    let config = if family == Adapter::Gemini {
                        let raw = &object.0.iter().find(|(key, _)| key == "generationConfig").unwrap().1;
                        let _: Object = serde_json::from_str(raw.get()).unwrap();
                        &actual["generationConfig"]
                    } else {
                        &actual
                    };
                    prop_assert_eq!(&config[limit], &json!(request.max_output_tokens.unwrap()));
                    if changed {
                        prop_assert_eq!(&config["temperature"], &json!(request.sampling.temperature.unwrap()));
                        prop_assert_eq!(&config[if family == Adapter::Gemini { "topP" } else { "top_p" }], &json!(request.sampling.top_p.unwrap()));
                        if let Some(seed) = request.sampling.seed { prop_assert_eq!(&config["seed"], &json!(seed)); }
                    }
                    // A sorted-map root (the original default build) cannot
                    // satisfy the explicit protocol order, with values unchanged.
                    // Gemini's header has one slot, so only order of several
                    // slots can distinguish the two builds.
                    if header_keys > 1 {
                        let mut sorted: Map<_, _> = body.fields.header().map(|(name, value)| (name.to_owned(), value.clone())).collect();
                        sorted.sort_keys();
                        let restored: Vec<_> = sorted.keys().map(String::as_str).collect();
                        prop_assert_ne!(restored.as_slice(), &expected[..header_keys]);
                    }

                    if changed && matches!(family, Adapter::Anthropic | Adapter::Responses) {
                        let mut unsupported = request.clone();
                        unsupported.sampling.seed = Some(0);
                        prop_assert!(super::super::body(&provider, &unsupported).is_err());
                    }
                }
            }
        }
    }
}

#[test]
fn gemini_summary_extends_admitted_user_and_tool_result_sources() -> acyclic_harness_core::Result<()>
{
    use acyclic_harness_core::model::{ModelContentPart, ToolCall, ToolResultContent};
    let (provider, transport) = crate::tests::provider(Adapter::Gemini, 200);
    let (prepared, _) = crate::tests::request(&provider)?;
    for tool_result in [false, true] {
        let mut request = prepared.request().clone();
        request.summary = true;
        if tool_result {
            request.tools.push(
                acyclic_harness_core::tools::catalog()
                    .find(|tool| tool.name == "code")
                    .unwrap()
                    .definition()?,
            );
            request.messages.extend([
                ModelMessage {
                    role: ModelRole::Assistant,
                    content: ModelContent::Part(ModelContentPart::ToolCall(ToolCall::new(
                        "call".into(),
                        "code".into(),
                        json!({"code":"42"}),
                    ))),
                },
                ModelMessage {
                    role: ModelRole::Tool,
                    content: ModelContent::Part(ModelContentPart::ToolResult {
                        call_id: "call".into(),
                        name: "code".into(),
                        content: ToolResultContent::Json { value: json!(42) },
                    }),
                },
            ]);
        }
        request.messages.push(ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text("Summarize \"猫\".".into()),
        });
        let prepared = PreparedModelRequest::prepare(request, Default::default(), None)?;
        let request = prepared.request();
        let names = crate::tool_names::Names::new(request);
        let origin = provider.reasoning_origin(provider.key.as_deref(), None);
        let body = super::super::body_named(
            &provider,
            request,
            &names,
            &origin,
            &mut crate::files::FileText::default(),
        )?;
        let observed = super::super::cache::encode(&body, &provider, &origin)?;
        let bytes = body.encode()?;
        assert_eq!(bytes, observed.bytes);
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value, former(&body));
        assert!(crate::validate_input(Adapter::Gemini, &value).is_empty());
        let contents = value["contents"].as_array().unwrap();
        assert!(
            contents
                .windows(2)
                .all(|pair| pair[0]["role"] != pair[1]["role"])
        );
        assert_eq!(contents.len(), body.messages.len());
        let parts = contents.last().unwrap()["parts"].as_array().unwrap();
        assert_eq!(
            parts.last().unwrap(),
            &json!({"text": body.summary.as_deref().unwrap()})
        );
        if tool_result {
            assert!(parts[0].get("functionResponse").is_some());
        } else {
            assert_eq!(parts[0], json!({"text":"user"}));
        }
        assert_eq!(
            request.messages.last().unwrap().content,
            ModelContent::Text("Summarize \"猫\".".into())
        );
    }
    assert!(transport.sent.lock().unwrap().is_empty());
    Ok(())
}
