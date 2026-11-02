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
    map.insert(
        conversation(body.target.family).into(),
        serde_json::to_value(&body.messages).unwrap(),
    );
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

fn freeze_effort(provider: &ResolvedModel, fields: &mut Fields<'_>) {
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
                if summary {
                    request.messages.push(ModelMessage { role: ModelRole::Assistant, content: ModelContent::Text("accepted reply".into()) });
                    request.messages.push(ModelMessage { role: ModelRole::User, content: ModelContent::Text("Summarize the selected context.".into()) });
                } else {
                    request.tools.push(acyclic_harness_core::tools::catalog().find(|tool| tool.name == "code").unwrap().definition().unwrap());
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
                    prop_assert_eq!(&keys, &expected);
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
