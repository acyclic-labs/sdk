use super::*;
use acyclic_harness::{
    OperationId,
    model::{ModelContentPart, ToolResultContent},
};
use proptest::prelude::*;

const MEMBERS: [(&str, Adapter); 4] = [
    ("anthropic", Adapter::Anthropic),
    ("responses", Adapter::Responses),
    ("gemini", Adapter::Gemini),
    ("chat", Adapter::Chat),
];

struct Routing {
    sent: Mutex<Vec<HttpRequest>>,
}
impl HttpTransport for Routing {
    fn delay(&self, wait: std::time::Duration) -> acyclic_stream::BoxProviderFuture<'static, ()> {
        crate::fixtures::delay(wait)
    }
    fn now(&self) -> std::time::Duration {
        crate::fixtures::now()
    }
    fn send(&self, request: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        Box::pin(async move {
            let name = request
                .url
                .split('/')
                .nth(3)
                .ok_or_else(|| invalid("fixture route"))?;
            let fixtures: serde_json::Value =
                serde_json::from_str(include_str!("fixtures/alloy-turns.json")).unwrap();
            let body = fixtures[name]
                .as_str()
                .ok_or_else(|| invalid("fixture route"))?
                .to_owned();
            self.sent.lock().unwrap().push(request);
            Ok(HttpResponse {
                retry_after: None,
                status: 200,
                body: Box::pin(futures::stream::iter([Ok(Bytes::from(body))])),
            })
        })
    }
}
fn registry(capacities: &[u32]) -> (Registry, Arc<Routing>) {
    let transport = Arc::new(Routing {
        sent: Mutex::new(Vec::new()),
    });
    let mut registry =
        Registry::with_transport(Credentials::explicit(BTreeMap::new()), transport.clone());
    for ((name, adapter), &context_tokens) in MEMBERS.iter().zip(capacities) {
        registry
            .register(
                (*name).into(),
                ProviderSpec {
                    adapter: *adapter,
                    authentication: Authentication::Optional,
                    base_url: format!("http://localhost/{name}"),
                    headers: BTreeMap::new(),
                    max_event_bytes: 65536,
                    retry: crate::RetrySpec::DEFAULT,
                    timeouts: crate::TimeoutSpec::DEFAULT,
                    token_counter: Some(TokenCounterSpec {
                        path: "count".into(),
                        adapter: *adapter,
                        response_field: TokenCountField::InputTokens,
                        framing_tokens: 0,
                    }),
                    models: BTreeMap::from([(
                        "fixture".into(),
                        ModelSpec {
                            capacity: ModelContextCapacity {
                                context_tokens,
                                output_tokens: context_tokens / 4,
                            },
                            features: ModelFeatures {
                                tools: true,
                                images: true,
                            },
                        },
                    )]),
                },
            )
            .unwrap();
    }
    (registry, transport)
}
fn spec(select: AlloySelection) -> AlloySpec {
    AlloySpec {
        models: MEMBERS
            .iter()
            .map(|(name, _)| format!("{name}/fixture"))
            .collect(),
        select,
    }
}
fn turn(ordinal: u64) -> AlloyTurn<'static> {
    AlloyTurn {
        operation_id: OperationId::from_bytes([9; 16]),
        ordinal,
        role: "worker",
    }
}
fn required() -> ModelFeatures {
    ModelFeatures {
        tools: true,
        images: true,
    }
}

#[test]
fn all_policies_resolve_to_real_models_and_reject_unsupported_members() -> Result<()> {
    let (mut registry, transport) = registry(&[8192, 4096, 16384, 32768]);
    for (select, expected) in [
        (AlloySelection::First, "anthropic"),
        (AlloySelection::RoundRobin, "gemini"),
        (
            AlloySelection::Weighted {
                weights: vec![0, 0, 0, 1],
            },
            "chat",
        ),
        (
            AlloySelection::PerRole {
                roles: BTreeMap::from([("worker".into(), 1)]),
                fallback: 3,
            },
            "responses",
        ),
    ] {
        let alloy = registry.resolve_alloy(spec(select), required())?;
        let selected = alloy.select(turn(2));
        assert_eq!(selected.model().provider, expected);
        assert_eq!(
            selected.context_capacity(selected.model())?,
            alloy.capacity()
        );
        assert_eq!(
            alloy.capacity(),
            ModelContextCapacity {
                context_tokens: 4096,
                output_tokens: 1024
            }
        );
        #[cfg(not(target_arch = "wasm32"))]
        {
            let work = acyclic_fs::NativeMaterializationWork::new(
                acyclic_fs::performance::work_budget(100, 4096),
                &acyclic_fs::CancellationToken::new(),
            )
            .map_err(|(error, _)| error)?;
            assert_eq!(
                selected.context_capacity_with_native_work(selected.model(), &work)?,
                alloy.capacity()
            );
        }
    }
    for features in [
        ModelFeatures {
            tools: false,
            images: true,
        },
        ModelFeatures {
            tools: true,
            images: false,
        },
    ] {
        let mut member = registry.catalog()["gemini"].as_ref().clone();
        member.models.get_mut("fixture").unwrap().features = features;
        registry.register("gemini".into(), member)?;
        assert!(
            registry
                .resolve_alloy(spec(AlloySelection::First), required())
                .is_err()
        );
    }
    assert!(
        transport.sent.lock().unwrap().is_empty(),
        "resolution must not invoke a model"
    );
    Ok(())
}

#[test]
fn invalid_and_undeclared_policies_fail_before_dispatch() {
    let (registry, transport) = registry(&[8192; 4]);
    let mut invalid = vec![
        spec(AlloySelection::Weighted { weights: vec![1] }),
        spec(AlloySelection::Weighted {
            weights: vec![0; 4],
        }),
        spec(AlloySelection::PerRole {
            roles: BTreeMap::new(),
            fallback: 4,
        }),
        spec(AlloySelection::PerRole {
            roles: BTreeMap::from([("worker".into(), 4)]),
            fallback: 0,
        }),
    ];
    for models in [
        vec![],
        vec!["anthropic/fixture".into(); 2],
        vec!["unknown/fixture".into()],
        vec!["bad".into()],
    ] {
        invalid.push(AlloySpec {
            models,
            select: AlloySelection::First,
        });
    }
    for spec in invalid {
        assert!(registry.resolve_alloy(spec, required()).is_err());
    }
    let fallback = registry
        .resolve_alloy(
            spec(AlloySelection::PerRole {
                roles: BTreeMap::new(),
                fallback: 3,
            }),
            required(),
        )
        .unwrap();
    assert_eq!(fallback.select(turn(u64::MAX)).model().provider, "chat");
    assert!(transport.sent.lock().unwrap().is_empty());
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    #[test]
    fn selection_and_capacity_survive_resume_in_any_request_order(
        capacities in prop::collection::vec(1024u32..32768,4), ordinals in prop::collection::vec(any::<u64>(),1..24),
    ) {
        let (registry,_) = registry(&capacities);
        let policy=spec(AlloySelection::RoundRobin);
        let json=serde_json::to_vec(&policy).unwrap();
        let first=registry.resolve_alloy(policy,required()).unwrap();
        let resumed=registry.resolve_alloy(serde_json::from_slice(&json).unwrap(),required()).unwrap();
        let expected=*capacities.iter().min().unwrap();
        for ordinal in ordinals.into_iter().rev() {
            let a=first.select(turn(ordinal)); let b=resumed.select(turn(ordinal));
            prop_assert_eq!(a.model(),b.model());
            prop_assert_eq!(&a.model().provider,MEMBERS[(ordinal%4) as usize].0);
            let capacity=a.context_capacity(a.model()).unwrap();
            prop_assert_eq!(capacity.context_tokens,expected);
            prop_assert_eq!(capacity.output_tokens,expected/4);
        }
    }
    #[test]
    fn weighted_cycles_have_exact_configured_counts_and_replay_stable_members(
        weights in prop::collection::vec(0u32..5,1..8), operation in any::<[u8;16]>(), ordinal in any::<u64>(),
    ) {
        prop_assume!(weights.iter().any(|&w|w>0));
        let policy=AlloySpec {models:(0..weights.len()).map(|i|format!("local/m{i}")).collect(),select:AlloySelection::Weighted {weights:weights.clone()}};
        policy.validate().unwrap();
        let replay:AlloySpec=serde_json::from_slice(&serde_json::to_vec(&policy).unwrap()).unwrap();
        let make_turn=|ordinal|AlloyTurn {operation_id:OperationId::from_bytes(operation),ordinal,role:"worker"};
        prop_assert_eq!(policy.index(make_turn(ordinal)),replay.index(make_turn(ordinal)));
        let mut counts=vec![0u32;weights.len()];
        for i in 0..weights.iter().sum::<u32>() {counts[policy.index(make_turn(u64::from(i)))]+=1;}
        prop_assert_eq!(counts,weights);
    }
    #[test]
    fn arbitrary_manifest_bytes_are_lossless_or_rejected(bytes in prop::collection::vec(any::<u8>(),0..4096)) {
        crate::fuzzing::assert_alloy_invariants(&bytes);
    }
}

#[test]
fn checked_in_manifest_is_accepted_by_the_shared_fuzzer() {
    let seed = include_bytes!("../fuzz/corpus/provider_stream/alloy.json");
    let spec: AlloySpec = serde_json::from_slice(&seed[1..]).unwrap();
    spec.validate().unwrap();
    crate::fuzzing::assert_stream_invariants(seed);
}

#[test]
#[should_panic(expected = "resume changed model")]
fn negative_control_a_mutable_counter_changes_the_same_turn_on_resume() {
    let policy = spec(AlloySelection::RoundRobin);
    let mut counter = 0;
    let mut broken = || {
        let result = counter % policy.models.len();
        counter += 1;
        result
    };
    assert_eq!(broken(), broken(), "resume changed model");
}

#[tokio::test]
async fn rotating_alloy_replays_mixed_reasoning_and_tool_exchanges() -> Result<()> {
    let (registry, transport) = registry(&[8192, 4096, 16384, 32768]);
    let alloy = registry.resolve_alloy(spec(AlloySelection::RoundRobin), required())?;
    let mut history = vec![ModelMessage {
        role: ModelRole::User,
        content: ModelContent::Text("work".into()),
    }];
    let mut ids = std::collections::BTreeSet::new();
    for ordinal in 0..5 {
        let provider = alloy.select(turn(ordinal));
        assert_eq!(provider.model().provider, MEMBERS[(ordinal % 4) as usize].0);
        let request = ModelRequest {
            model: provider.model().clone(),
            messages: history.clone(),
            tools: vec![acyclic_harness::tool::ToolDefinition {
                name: "echo".into(),
                revision: "1".into(),
                description: "echo".into(),
                input_schema: json!({"type":"object"}),
                output_schema: json!({}),
                projection_schema: json!({}),
            }],
            max_output_tokens: Some(64),
        };
        let prepared = PreparedModelRequest::prepare(request, Limits::default(), None)?;
        let prepared = PreparedModelRequest::decode(prepared.bytes(), Limits::default(), None)?;
        crate::call_ids::CallIds::assert_round_trip(prepared.request());
        let body = serde_json::to_vec(&crate::projection::body_named(
            provider,
            prepared.request(),
            &crate::tool_names::Names::new(prepared.request()),
            &provider.reasoning_origin(None, None),
            &mut crate::files::FileText::default(),
        )?)
        .unwrap();
        for message in &history {
            if let ModelContent::Part(ModelContentPart::Reasoning { block }) = &message.content {
                let target = provider.reasoning_origin(None, None);
                if let ProjectedReasoning::Native { continuity, .. } =
                    crate::project_reasoning(block, &target)
                {
                    if matches!(
                        provider.config.adapter,
                        Adapter::Anthropic | Adapter::Responses
                    ) {
                        assert!(
                            body.windows(continuity.len())
                                .any(|window| window == continuity)
                        );
                    }
                }
            }
        }
        let dispatch = ModelDispatch {
            operation_id: turn(ordinal).operation_id,
            step: ordinal as u32,
            request_digest: prepared.request_digest(),
        };
        let events = provider
            .generate(prepared, dispatch)
            .try_collect::<Vec<_>>()
            .await?;
        {
            let sent = transport.sent.lock().unwrap();
            let captured = sent.last().expect("captured provider request");
            assert_eq!(captured.body.as_ref(), body.as_slice());
            assert_foreign_wire(&captured.body, provider.config.adapter, ordinal);
        }
        let replay: Vec<ModelEvent> =
            serde_json::from_slice(&serde_json::to_vec(&events).unwrap()).unwrap();
        assert_eq!(events, replay);
        let mut results = Vec::new();
        let mut calls = 0;
        let mut reasoning = 0;
        for event in replay {
            let part = match event {
                ModelEvent::ReasoningBlock { block } => {
                    reasoning += 1;
                    ModelContentPart::Reasoning { block }
                }
                ModelEvent::ToolCall(call) => {
                    calls += 1;
                    assert!(
                        ids.insert(call.call_id.clone()),
                        "provider ID reused across alloy turns"
                    );
                    results.push(ModelMessage {
                        role: ModelRole::Tool,
                        content: ModelContent::Part(ModelContentPart::ToolResult {
                            call_id: call.call_id.clone(),
                            name: call.name.clone(),
                            content: ToolResultContent::Json {
                                value: json!({"ok":true}),
                            },
                        }),
                    });
                    ModelContentPart::ToolCall(call)
                }
                _ => continue,
            };
            history.push(ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Part(part),
            });
        }
        assert_eq!(calls, 1);
        assert!(reasoning > 0);
        history.extend(results);
        history.push(ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text("continue".into()),
        });
    }
    assert_eq!(transport.sent.lock().unwrap().len(), 5);
    Ok(())
}

const WIRE_REASONING: [(&str, &str, &[&str]); 4] = [
    (
        "anthropic",
        "readable-anthropic-marker",
        &["opaque-anthropic-signature"],
    ),
    (
        "responses",
        "readable-responses-marker",
        &[
            "opaque-responses-encrypted",
            "opaque-responses-no-readable-content",
        ],
    ),
    (
        "gemini",
        "readable-gemini-marker",
        &["opaque-gemini-signature"],
    ),
    ("chat", "readable-chat-marker", &[]),
];

fn assert_foreign_wire(bytes: &[u8], adapter: Adapter, ordinal: u64) {
    let body: serde_json::Value = serde_json::from_slice(bytes).unwrap();
    let field = match adapter {
        Adapter::Responses => "input",
        Adapter::Gemini => "contents",
        _ => "messages",
    };
    let mut plain = Vec::new();
    for message in body[field].as_array().expect("projected messages") {
        let content = &message[if adapter == Adapter::Gemini {
            "parts"
        } else {
            "content"
        }];
        if let Some(text) = content.as_str() {
            plain.push(text);
        }
        for part in content.as_array().into_iter().flatten() {
            if part["thought"] != true {
                if let Some(text) = part["text"].as_str() {
                    plain.push(text);
                }
            }
        }
    }
    let plain = plain.join("\n");
    let wire = std::str::from_utf8(bytes).unwrap();
    let target = crate::stream::family(adapter);
    for (index, &(origin, readable, opaque)) in WIRE_REASONING.iter().enumerate() {
        if origin == target {
            continue;
        }
        if (index as u64) < ordinal {
            assert!(
                plain.contains(readable),
                "foreign readable reasoning missing from plain context: {origin}"
            );
            assert!(
                plain.contains(&format!("<prior-reasoning provider={origin:?}")),
                "foreign reasoning lacks its delimiter"
            );
        }
        for payload in opaque {
            assert!(
                !wire.contains(payload),
                "foreign opaque reasoning leaked: {origin}"
            );
        }
    }
}

#[test]
#[should_panic(expected = "foreign readable reasoning missing from plain context")]
fn negative_control_dropped_foreign_readable_reasoning_is_detected_on_the_wire() {
    assert_foreign_wire(
        br#"{"input":[{"role":"assistant","content":"missing"}]}"#,
        Adapter::Responses,
        1,
    );
}

#[test]
#[should_panic(expected = "foreign opaque reasoning leaked")]
fn negative_control_forwarded_foreign_opaque_reasoning_is_detected_on_the_wire() {
    let body = serde_json::to_vec(&json!({"input":[{"role":"assistant","content":"<prior-reasoning provider=\"anthropic\">\nreadable-anthropic-marker\n</prior-reasoning>"}], "forged_native":{"signature":"opaque-anthropic-signature"}})).unwrap();
    assert_foreign_wire(&body, Adapter::Responses, 1);
}
