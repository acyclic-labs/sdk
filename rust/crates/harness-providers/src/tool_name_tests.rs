use super::*;
use crate::tool_names::Names;
use acyclic_harness::{
    model::{ModelContentPart, ToolCall, ToolResultContent},
    tool::ToolDefinition,
};
use proptest::prelude::*;
use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
};

fn definition(name: String) -> ToolDefinition {
    ToolDefinition {
        name,
        revision: "1".into(),
        description: "Read a file".into(),
        input_schema: json!({"type":"object"}),
        output_schema: json!({"type":"object"}),
        projection_schema: json!({"type":"object"}),
    }
}

fn valid(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && (name.as_bytes()[0].is_ascii_alphabetic() || name.starts_with('_'))
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

pub(super) fn assert_names(input: Vec<String>) {
    let (provider, _) = provider(Adapter::Responses, 200);
    let (prepared, _) = request(&provider).unwrap();
    let mut request = prepared.request().clone();
    request.tools = input
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(definition)
        .collect();
    Names::assert_round_trip(&request);
    let names = Names::new(&request);
    let mut seen = BTreeSet::new();
    for tool in &request.tools {
        let wire = names.wire(&tool.name).unwrap();
        assert!(valid(&wire), "target tool-name grammar");
        assert!(
            seen.insert(wire.to_string()),
            "distinct canonical names collided"
        );
        let mut call =
            ModelEvent::ToolCall(ToolCall::new("call".into(), wire.into_owned(), json!({})));
        names.restore(&mut call).unwrap();
        let ModelEvent::ToolCall(call) = call else {
            unreachable!()
        };
        assert_eq!(call.name, tool.name);
    }
    let mut permuted = request.clone();
    permuted.tools.reverse();
    let permuted_names = Names::new(&permuted);
    for tool in &request.tools {
        assert_eq!(
            names.wire(&tool.name).unwrap(),
            permuted_names.wire(&tool.name).unwrap()
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn arbitrary_canonical_names_have_collision_free_reversible_target_names(
        input in prop::collection::vec(".{0,128}", 0..24)
    ) { assert_names(input); }

    #[test]
    fn every_named_control_matches_its_projected_declaration(name in ".{1,96}") {
        for canonical in [name.as_str(), "harness_tool_0", "acyclic.read_file"] {
            for (adapter, _) in fixtures() {
                let (provider, _) = provider(adapter, 200);
                let (prepared, _) = request(&provider).unwrap();
                let mut request = prepared.request().clone();
                request.tools.push(definition(canonical.into()));
                if canonical != "A_anchor" {
                    request.tools.push(definition("A_anchor".into()));
                }
                let choices = match adapter {
                    Adapter::Anthropic => vec![json!({"tool_choice":{"type":"tool","name":canonical}})],
                    Adapter::Responses => vec![json!({"tool_choice":{"type":"function","name":canonical}}),
                        json!({"tool_choice":{"type":"allowed_tools","mode":"auto","tools":[{"type":"function","name":canonical}]}})],
                    Adapter::Chat => vec![json!({"tool_choice":{"type":"function","function":{"name":canonical}}}),
                        json!({"tool_choice":{"type":"allowed_tools","mode":"auto","tools":[{"type":"function","function":{"name":canonical}}]}})],
                    Adapter::Gemini => vec![json!({"toolConfig":{"functionCallingConfig":{"mode":"ANY","allowedFunctionNames":[canonical]}}})],
                };
                let names = Names::new(&request);
                let expected = names.wire(canonical).unwrap().into_owned();
                for options in choices {
                    let mut request = request.clone();
                    request.model.options = options;
                    let body = projection::body(&provider, &request).unwrap();
                    let (declaration, controls) = match adapter {
                        Adapter::Anthropic => (&body["tools"][0]["name"], vec![&body["tool_choice"]["name"]]),
                        Adapter::Responses => (&body["tools"][0]["name"], vec![if body["tool_choice"]["type"] == "allowed_tools" {
                            &body["tool_choice"]["tools"][0]["name"] } else { &body["tool_choice"]["name"] }]),
                        Adapter::Chat => (&body["tools"][0]["function"]["name"], vec![if body["tool_choice"]["type"] == "allowed_tools" {
                            &body["tool_choice"]["tools"][0]["function"]["name"] } else { &body["tool_choice"]["function"]["name"] }]),
                        Adapter::Gemini => (&body["tools"][0]["functionDeclarations"][0]["name"], vec![&body["toolConfig"]["functionCallingConfig"]["allowedFunctionNames"][0]]),
                    };
                    prop_assert_eq!(declaration, &expected);
                    for selected in controls { prop_assert_eq!(selected, declaration); }
                }
            }
        }
    }
}

#[test]
fn builtins_collisions_unicode_and_replayed_names_project_for_every_adapter() -> Result<()> {
    let canonical = [
        "acyclic.read_file",
        "acyclic_read_file",
        "harness_tool_0",
        "0start",
        "讀取",
        "echo",
    ];
    assert_names(canonical.into_iter().map(String::from).collect());
    for (adapter, _) in fixtures() {
        let (provider, _) = provider(adapter, 200);
        let (prepared, _) = request(&provider)?;
        let mut request = prepared.request().clone();
        request.tools = canonical
            .into_iter()
            .map(|name| definition(name.into()))
            .collect();
        request.messages.push(ModelMessage {
            role: ModelRole::Assistant,
            content: ModelContent::Parts(
                canonical
                    .iter()
                    .enumerate()
                    .map(|(i, name)| {
                        ModelContentPart::ToolCall(ToolCall::new(
                            format!("call_{i}"),
                            (*name).into(),
                            json!({}),
                        ))
                    })
                    .collect(),
            ),
        });
        request.messages.push(ModelMessage {
            role: ModelRole::Tool,
            content: ModelContent::Parts(
                canonical
                    .iter()
                    .enumerate()
                    .map(|(i, name)| ModelContentPart::ToolResult {
                        call_id: format!("call_{i}"),
                        name: (*name).into(),
                        content: ToolResultContent::Json { value: json!({}) },
                    })
                    .collect(),
            ),
        });
        let replay: ModelRequest =
            serde_json::from_slice(&serde_json::to_vec(&request).unwrap()).unwrap();
        let body = projection::body(&provider, &request)?;
        assert_eq!(body, projection::body(&provider, &replay)?);
        let names = Names::new(&request);
        for name in canonical {
            let wire = names.wire(name)?;
            let encoded = serde_json::to_string(&body).unwrap();
            assert!(encoded.contains(&format!("\"name\":\"{wire}\"")));
            if !valid(name) {
                assert!(!encoded.contains(&format!("\"name\":\"{name}\"")));
            }
        }
    }
    Ok(())
}

#[test]
fn undeclared_names_outside_the_provider_grammar_are_protocol_errors() {
    let (provider, _) = provider(Adapter::Responses, 200);
    let (prepared, _) = request(&provider).unwrap();
    let mut request = prepared.request().clone();
    request.tools.push(definition("acyclic.read_file".into()));
    let names = Names::new(&request);
    for wire in ["acyclic.read_file", "", "0start", "讀取", "mcp.server.tool"] {
        let mut event = ModelEvent::ToolCall(ToolCall::new("call".into(), wire.into(), json!({})));
        assert!(names.restore(&mut event).is_err(), "{wire}");
    }
    // A grammatical undeclared name passes through to tool admission.
    for wire in ["harness_tool_0", "harness_tool_99", "unknown"] {
        let mut event = ModelEvent::ToolCall(ToolCall::new("call".into(), wire.into(), json!({})));
        names.restore(&mut event).unwrap();
        let ModelEvent::ToolCall(call) = event else {
            unreachable!()
        };
        assert_eq!(call.name, wire);
    }
}

fn wires(tools: &[&str]) -> BTreeMap<String, String> {
    let (provider, _) = provider(Adapter::Responses, 200);
    let (prepared, _) = request(&provider).unwrap();
    let mut request = prepared.request().clone();
    request.tools = tools
        .iter()
        .map(|name| definition((*name).into()))
        .collect();
    let names = Names::new(&request);
    tools
        .iter()
        .map(|name| ((*name).to_owned(), names.wire(name).unwrap().into_owned()))
        .collect()
}

#[test]
fn mcp_tools_keep_server_qualified_names_independent_of_position() {
    let alone = wires(&["mcp.docs.search"]);
    assert_eq!(alone["mcp.docs.search"], "mcp__docs__search");
    // Default tools added ahead of the MCP tool, in any order, leave its name.
    for tools in [
        vec![
            "acyclic.read_file",
            "acyclic.write_file",
            "bash",
            "mcp.docs.search",
        ],
        vec![
            "mcp.docs.search",
            "acyclic.zz_new_default",
            "acyclic.read_file",
        ],
        vec!["0first", "讀取", "mcp.docs.search", "mcp.docs.fetch"],
    ] {
        assert_eq!(wires(&tools)["mcp.docs.search"], "mcp__docs__search");
    }
    let sanitized = wires(&["mcp.my server.do/it", "mcp.1srv.tool"]);
    assert_eq!(sanitized["mcp.my server.do/it"], "mcp__my_server__do_it");
    assert_eq!(sanitized["mcp.1srv.tool"], "mcp__1srv__tool");
}

#[test]
fn colliding_and_overlong_aliases_take_stable_digests() {
    // A declared name equal to an alias keeps its name; the alias moves.
    let collided = wires(&["mcp.a.b", "mcp__a__b", "mcp.a/b"]);
    assert_eq!(collided["mcp__a__b"], "mcp__a__b");
    assert!(collided["mcp.a.b"].starts_with("mcp__a__b_"));
    assert_ne!(collided["mcp.a.b"], "mcp__a__b");
    assert_ne!(collided["mcp.a.b"], collided["mcp.a/b"]);
    assert_eq!(collided, wires(&["mcp.a/b", "mcp__a__b", "mcp.a.b"]));
    let long = format!("mcp.server.{}", "t".repeat(80));
    let other = format!("mcp.server.{}x", "t".repeat(80));
    let digested = wires(&[&long, &other]);
    assert!(digested[&long].starts_with("mcp__server__ttt"));
    assert!(digested.values().all(|wire| valid(wire)));
    assert_ne!(digested[&long], digested[&other]);
    assert_eq!(wires(&[&long])[&long], digested[&long]);
}

#[test]
#[should_panic(expected = "positional aliases moved")]
fn negative_control_positional_aliases_move_when_a_tool_is_added() {
    let positional = |tools: &[&str]| {
        let mut sorted = tools.to_vec();
        sorted.sort_unstable();
        let index = sorted.iter().position(|name| *name == "mcp.docs.search");
        format!("harness_tool_{}", index.unwrap())
    };
    assert_eq!(
        positional(&["mcp.docs.search"]),
        positional(&["acyclic.read_file", "mcp.docs.search"]),
        "positional aliases moved"
    );
}

#[test]
#[should_panic(expected = "distinct canonical names collided")]
fn negative_control_rejects_lossy_punctuation_replacement() {
    let mut seen = BTreeSet::new();
    for name in ["acyclic.read_file", "acyclic_read_file"] {
        assert!(
            seen.insert(name.replace('.', "_")),
            "distinct canonical names collided"
        );
    }
}

#[tokio::test]
async fn every_adapter_restores_streamed_builtin_calls_with_native_custody() -> Result<()> {
    for (adapter, fixture) in [
        (
            Adapter::Chat,
            "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call\",\"function\":{\"name\":\"acyclic__read_file\",\"arguments\":\"{}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\ndata: [DONE]\n\n",
        ),
        (
            Adapter::Responses,
            "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"function_call\",\"call_id\":\"call\",\"name\":\"acyclic__read_file\",\"arguments\":\"{}\"}}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n",
        ),
        (
            Adapter::Anthropic,
            "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"call\",\"name\":\"acyclic__read_file\",\"input\":{}}}\n\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\ndata: {\"type\":\"message_stop\"}\n\n",
        ),
        (
            Adapter::Gemini,
            "data: {\"candidates\":[{\"content\":{\"parts\":[{\"functionCall\":{\"id\":\"call\",\"name\":\"acyclic__read_file\",\"args\":{}}}]},\"finishReason\":\"STOP\"}]}\n\n",
        ),
    ] {
        let (mut provider, _) = provider(adapter, 200);
        provider.transport = crate::deadline::Bounded::new(
            Arc::new(AliasFixture(native_tests::Fixture::new(fixture))),
            crate::TimeoutSpec::DEFAULT,
        );
        let (prepared, _) = request(&provider)?;
        let mut request = prepared.request().clone();
        request.tools.push(definition("acyclic.read_file".into()));
        let prepared = PreparedModelRequest::prepare(request, Limits::default(), None)?;
        let dispatch = ModelDispatch {
            request_digest: prepared.request_digest(),
            ..parser_dispatch()
        };
        let ordinary: Vec<_> = provider
            .generate(prepared.clone(), dispatch)
            .try_collect()
            .await?;
        assert!(ordinary.iter().any(
            |event| matches!(event, ModelEvent::ToolCall(call) if call.name == "acyclic.read_file")
        ));
        let work = native_tests::work(&acyclic_fs::CancellationToken::new())?;
        let baseline = work.live_allocation_bytes();
        let mut stream = provider
            .generate_with_native_work(&prepared, dispatch, &work)?
            .await?;
        let mut found = false;
        while let Some(event) = stream.next().await {
            let (event, custody) = event?;
            if let ModelEvent::ToolCall(call) = &event {
                assert_eq!(call.name, "acyclic.read_file");
                assert!(custody.bytes() >= event.logical_owned_bytes()?);
                found = true;
            }
        }
        drop(stream);
        assert!(found);
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
    Ok(())
}

struct AliasFixture(native_tests::Fixture);

impl HttpTransport for AliasFixture {
    fn delay(&self, wait: std::time::Duration) -> acyclic_stream::BoxProviderFuture<'static, ()> {
        crate::fixtures::delay(wait)
    }
    fn now(&self) -> std::time::Duration {
        crate::fixtures::now()
    }
    fn delay_with_native_work(
        &self,
        wait: std::time::Duration,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<acyclic_harness::NativeFutureBundle<'static, ()>> {
        crate::fixtures::native_delay(wait, work)
    }
    fn send(&self, _: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        Box::pin(async move {
            Ok(HttpResponse {
                retry_after: None,
                status: 200,
                body: chunks(&self.0.response, 1),
            })
        })
    }
    fn supports_native(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<bool> {
        self.0.supports_native(work)
    }
    fn send_with_native_work<'a>(
        &'a self,
        request: HttpRequest,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<acyclic_harness::NativeFutureBundle<'a, HttpResponse>> {
        self.0.send_with_native_work(request, work)
    }
}

#[test]
fn checked_in_tool_name_seed_exercises_the_fuzzer_oracle() {
    let seed = include_bytes!("../fuzz/corpus/provider_stream/tool-names.json");
    let request: ModelRequest = serde_json::from_slice(&seed[1..]).unwrap();
    assert_eq!(request.tools[0].name, "acyclic.read_file");
    Names::assert_round_trip(&request);
    crate::fuzzing::assert_stream_invariants(seed);
}

#[tokio::test]
async fn an_invalid_alias_rejects_the_complete_batch_before_publication() -> Result<()> {
    const BODY: &str = "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"first\",\"function\":{\"name\":\"acyclic__read_file\",\"arguments\":\"{}\"}},{\"index\":1,\"id\":\"second\",\"function\":{\"name\":\"bad.name\",\"arguments\":\"{}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\ndata: [DONE]\n\n";
    let (mut provider, _) = provider(Adapter::Chat, 200);
    provider.transport = crate::deadline::Bounded::new(
        Arc::new(AliasFixture(native_tests::Fixture::new(BODY))),
        crate::TimeoutSpec::DEFAULT,
    );
    let (prepared, _) = request(&provider)?;
    let mut request = prepared.request().clone();
    request.tools.push(definition("acyclic.read_file".into()));
    let prepared = PreparedModelRequest::prepare(request, Limits::default(), None)?;
    let dispatch = ModelDispatch {
        request_digest: prepared.request_digest(),
        ..parser_dispatch()
    };
    let mut ordinary = provider.generate(prepared.clone(), dispatch);
    assert!(ordinary.next().await.unwrap().is_err());
    assert!(ordinary.next().await.is_none());
    let work = native_tests::work(&acyclic_fs::CancellationToken::new())?;
    let mut native = provider
        .generate_with_native_work(&prepared, dispatch, &work)?
        .await?;
    assert!(native.next().await.unwrap().is_err());
    assert!(native.next().await.is_none());
    Ok(())
}

#[tokio::test]
async fn canonical_name_expansion_obeys_the_aggregate_parser_bound() -> Result<()> {
    let (mut provider, _) = provider(Adapter::Chat, 200);
    Arc::make_mut(&mut provider.config).max_event_bytes = 512;
    let (prepared, _) = request(&provider)?;
    let mut request = prepared.request().clone();
    request.tools = ['a', 'b']
        .into_iter()
        .map(|letter| definition(format!("namespace.{}", letter.to_string().repeat(200))))
        .collect();
    let names = Names::new(&request);
    let call = |id: &str, index: usize| {
        let name = names.wire(&request.tools[index].name).unwrap();
        format!(
            "{{\"index\":{index},\"id\":\"{id}\",\"function\":{{\"name\":\"{name}\",\"arguments\":\"{{}}\"}}}}"
        )
    };
    let body = format!(
        "data: {{\"choices\":[{{\"delta\":{{\"tool_calls\":[{},{}]}},\"finish_reason\":\"tool_calls\"}}]}}\n\ndata: [DONE]\n\n",
        call("first", 0),
        call("second", 1)
    );
    drop(names);
    // The fixture serves a static body for the test's lifetime.
    let body: &'static str = Box::leak(body.into_boxed_str());
    provider.transport = crate::deadline::Bounded::new(
        Arc::new(AliasFixture(native_tests::Fixture::new(body))),
        crate::TimeoutSpec::DEFAULT,
    );
    let prepared = PreparedModelRequest::prepare(request, Limits::default(), None)?;
    let dispatch = ModelDispatch {
        request_digest: prepared.request_digest(),
        ..parser_dispatch()
    };
    let mut ordinary = provider.generate(prepared.clone(), dispatch);
    let error = ordinary.next().await.unwrap().unwrap_err();
    assert!(error.to_string().contains("aggregate"));
    let work = native_tests::work(&acyclic_fs::CancellationToken::new())?;
    let mut native = provider
        .generate_with_native_work(&prepared, dispatch, &work)?
        .await?;
    let error = native.next().await.unwrap().unwrap_err();
    assert!(error.to_string().contains("aggregate"));
    Ok(())
}

pub(crate) fn request_with(tools: &[&str]) -> ModelRequest {
    let (provider, _) = provider(Adapter::Responses, 200);
    let (prepared, _) = request(&provider).unwrap();
    let mut request = prepared.request().clone();
    request.tools = tools
        .iter()
        .map(|name| definition((*name).into()))
        .collect();
    request
}

#[test]
fn aliases_are_exactly_sized_and_the_build_matches_its_claim() {
    let long = format!("mcp.server.{}", "t".repeat(80));
    let request = request_with(&[&long]);
    let (names, built) = Names::built(&request);
    let aliases: Vec<_> = names.aliases().collect();
    assert_eq!(aliases, [(64, 64)], "digest alias capacity is exact");
    // One 40-byte binding, the 64-byte digest buffer, the 64-byte alias,
    // and one wire index entry: no growth and no uncounted temporary.
    #[cfg(target_pointer_width = "64")]
    assert_eq!(
        built,
        crate::tool_names::Built {
            allocations: 4,
            bytes: 40 + 64 + 64 + 8,
            probes: 0,
            hashed: 116,
            wire_sorts: 1,
            wire_comparisons: 0,
        }
    );
    assert!(built.within(&Names::claim(1).unwrap()));
    // A readable alias needs neither the digest buffer nor any slack.
    let request = request_with(&["mcp.docs.search", "read"]);
    let (names, built) = Names::built(&request);
    assert_eq!(names.aliases().collect::<Vec<_>>(), [(17, 17)]);
    assert_eq!(built.allocations, 3);
    assert!(built.within(&Names::claim(2).unwrap()));
    for size in 0..64 {
        let names: Vec<String> = (0..size)
            .map(|index| format!("mcp.s{index}.{}", "x".repeat(index * 3)))
            .collect();
        let request = request_with(&names.iter().map(String::as_str).collect::<Vec<_>>());
        let (table, built) = Names::built(&request);
        assert!(table.aliases().all(|(length, capacity)| length == capacity));
        assert!(built.within(&Names::claim(size as u64).unwrap()));
    }
}

#[test]
fn negative_control_a_grown_alias_retains_slack_beyond_its_claim() {
    // The former builder reserved the readable prefix, then appended the digest.
    let mut grown = String::with_capacity(47);
    grown.push_str(&"t".repeat(47));
    grown.push_str(&"0".repeat(17));
    assert!(grown.capacity() > grown.len(), "growth left no slack");
    assert!(
        grown.capacity() > 64,
        "the per-alias claim would hold slack"
    );
}

/// The reviewer's input: an alias, its declared readable name, and the
/// alias's first `taken` salted digests, all declared as canonical names.
pub(crate) fn adversarial(taken: u32) -> Vec<String> {
    let mut tools = vec!["mcp.a.b".to_owned(), "mcp__a__b".to_owned()];
    tools.extend((1..=taken).map(|salt| Names::candidate("mcp.a.b", salt)));
    tools
}

#[test]
fn occupied_candidates_cost_one_probe_each() {
    let tools = adversarial(1024);
    let request = request_with(&tools.iter().map(String::as_str).collect::<Vec<_>>());
    let (names, built) = Names::built(&request);
    assert_eq!(
        built.probes, 1025,
        "one probe per occupied candidate plus the free one"
    );
    assert!(built.within(&Names::claim(tools.len() as u64).unwrap()));
    assert_eq!(
        names.wire("mcp.a.b").unwrap(),
        Names::candidate("mcp.a.b", 1025)
    );
    Names::assert_round_trip(&request);
}

#[test]
fn lookups_borrow_the_cached_alias() {
    let request = request_with(&["mcp.docs.search"]);
    let names = Names::new(&request);
    let first = names.wire("mcp.docs.search").unwrap();
    let second = names.wire("mcp.docs.search").unwrap();
    assert!(matches!(first, Cow::Borrowed(_)) && matches!(second, Cow::Borrowed(_)));
    assert_eq!(first.as_ptr(), second.as_ptr(), "lookups share one alias");
    // Negative control: an owned copy has its own storage.
    let copied = first.clone().into_owned();
    assert_ne!(copied.as_ptr(), second.as_ptr());
}
