use super::*;
use crate::{Adapter, ResolvedModel};
use acyclic_harness_core::{
    approvals::ApprovalPolicy,
    model::{ModelContent, ModelMessage, ModelRole, ModelSampling, PreparedModelRequest},
    profile::{AgentOverride, ModelProfile, ProjectLayer, RoleFile, resolve},
};
use proptest::prelude::*;
use serde::Serializer as _;
use serde::de::{MapAccess, Visitor};
use serde_json::{Map, json};
use std::{
    fmt,
    io::{self, Write},
};

// Restore the former map-based serialization as a semantic oracle. Root order
// comes from the selected serde_json map backend, as it did before this change.
struct Former<'a, 'body>(&'a Body<'body>);
impl Serialize for Former<'_, '_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        let fields: Map<_, _> = self
            .0
            .fields
            .iter()
            .map(|(name, value)| (name.to_owned(), value.clone()))
            .collect();
        for (name, value) in &fields {
            map.serialize_entry(name, value)?;
        }
        if let Some(tools) = &self.0.tools {
            map.serialize_entry("tools", tools)?;
        }
        map.serialize_entry(self.0.messages_field, &self.0.messages)?;
        map.end()
    }
}

struct Closed {
    writes: usize,
}
impl Write for Closed {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.writes += 1;
        if self.writes == 1 {
            Ok(bytes.len())
        } else {
            Err(io::ErrorKind::BrokenPipe.into())
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        panic!("serialization must not flush")
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
    fields.freeze();
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
    struct Configuration<'a, 'body>(&'a Body<'body>);
    impl Serialize for Configuration<'_, '_> {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            self.0
                .serialize_with_messages(serializer, None, None::<&str>, &[] as &[Value])
        }
    }
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
        let bytes = serde_json::to_vec(&Configuration(&body)).unwrap();
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
                    let former = serde_json::to_vec(&Former(&body)).unwrap();
                    prop_assert_eq!(&actual, &serde_json::from_slice::<Value>(&former).unwrap());
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
                    if family == Adapter::Gemini { expected.push("generationConfig"); }
                    else {
                        expected.push(limit);
                        if changed {
                            expected.extend(["temperature", "top_p"]);
                            if family == Adapter::Chat { expected.push("seed"); }
                        }
                    }
                    if body.tools.is_some() { expected.push("tools"); }
                    expected.push(body.messages_field);
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
                    let mut sorted: Map<_, _> = body.fields.iter().map(|(name, value)| (name.to_owned(), value.clone())).collect();
                    sorted.sort_keys();
                    let restored: Vec<_> = sorted.keys().map(String::as_str).collect();
                    prop_assert_ne!(restored.as_slice(), &expected[..expected.len() - usize::from(body.tools.is_some()) - 1]);

                    let mut serializer = serde_json::Serializer::new(io::sink());
                    let mut map = serializer.serialize_map(None).unwrap();
                    let cost = acyclic_test_support::measure(|| body.serialize_trailer(&mut map, None).unwrap());
                    map.end().unwrap();
                    prop_assert_eq!((cost.count_total, cost.bytes_total), (0, 0));
                    let copied = acyclic_test_support::measure(|| { std::hint::black_box(body.fields.clone()); });
                    prop_assert!(copied.count_total > 0);
                    let mut closed = Closed { writes: 0 };
                    let mut serializer = serde_json::Serializer::new(&mut closed);
                    let mut map = serializer.serialize_map(None).unwrap();
                    let refusal = body.serialize_trailer(&mut map, None).unwrap_err();
                    prop_assert_eq!(refusal.io_error_kind(), Some(io::ErrorKind::BrokenPipe));
                    prop_assert_eq!(closed.writes, 2);

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
