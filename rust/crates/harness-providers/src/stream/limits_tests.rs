use super::*;
use serde_json::json;

#[test]
fn unsigned_gemini_fragments_coalesce_without_crossing_signature_boundaries() -> Result<()> {
    let mut state = State::new(4096);
    for _ in 0..512 {
        state.gemini_text(state.texts.len() as u64, "x", None)?;
    }
    assert_eq!(state.texts.len(), 1);
    let signed = r#"{"text":"y","thoughtSignature":"opaque"}"#;
    state.gemini_text(state.texts.len() as u64, "y", Some(signed))?;
    for _ in 0..512 {
        state.gemini_text(state.texts.len() as u64, "z", None)?;
    }
    assert_eq!(state.texts.len(), 3);
    state.terminal = true;
    let dispatch = ModelDispatch {
        operation_id: acyclic_harness_core::OperationId::from_bytes([1; 16]),
        step: 0,
        request_digest: [0; 32],
    };
    let parts: Vec<_> = finish(state, Adapter::Gemini, dispatch)?
        .into_iter()
        .filter_map(|event| match event {
            ModelEvent::TextPart { text, native } => {
                Some((text, native.map(|native| native.continuity)))
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        parts,
        vec![
            ("x".repeat(512), None),
            ("y".into(), Some(signed.as_bytes().to_vec())),
            ("z".repeat(512), None),
        ]
    );
    Ok(())
}

#[test]
fn small_events_cannot_exceed_retained_byte_capacity() -> Result<()> {
    let mut state = State::new(256);
    let mut rejected = false;
    for index in 0..10_000 {
        let event = json!({"choices":[{"delta":{"tool_calls":[{"index":index,"id":format!("c{index}"),"function":{"name":"echo","arguments":"{}"}}]}}]});
        assert!(event.to_string().len() < 256);
        if chat::parse(&mut state, &event).is_err() {
            rejected = true;
            break;
        }
    }
    assert!(rejected);
    assert!(state.calls.len() < 10_000);
    assert!(state.budget.used() <= 256);
    let before = state.calls.len();
    assert!(state.call(20_000).is_err());
    assert_eq!(
        state.calls.len(),
        before,
        "reject before allocating a call slot"
    );

    let mut state = State::new(1024 * 1024);
    let mut rejected = false;
    for index in 0..10_000 {
        let event = json!({"type":"content_block_start","index":index,"content_block":{"type":"thinking","thinking":"","signature":""}});
        assert!(event.to_string().len() < 256);
        if anthropic::parse(&mut state, &event, &event.to_string()).is_err() {
            rejected = true;
            break;
        }
    }
    assert!(rejected);
    assert!(state.thinking.len() < 10_000);
    assert!(state.thinking.len() > 1024);
    assert!(state.budget.used() <= 1024 * 1024);
    Ok(())
}

#[test]
fn recorded_parallel_calls_use_byte_capacity_without_a_hidden_item_ceiling() -> Result<()> {
    use futures::TryStreamExt as _;
    // Real llama.cpp b11889/Qwen3-0.6B capture, 2026-10-26; provenance and
    // original requests/headers are beside tool.body. Only call index/ID change.
    let recorded: Vec<_> =
        include_str!("../../../conformance/fixtures/inference/llamacpp/tool.body")
            .lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .collect();
    let deltas: Vec<Value> = recorded
        .iter()
        .filter_map(|data| serde_json::from_str::<Value>(data).ok())
        .filter(|value| value["choices"][0]["delta"]["tool_calls"].is_array())
        .collect();
    assert!(!deltas.is_empty());
    let tail = recorded
        .iter()
        .position(|data| {
            serde_json::from_str::<Value>(data)
                .is_ok_and(|value| value["choices"][0]["finish_reason"] == "tool_calls")
        })
        .expect("original tool completion");
    let dispatch = ModelDispatch {
        operation_id: acyclic_harness_core::OperationId::from_bytes([1; 16]),
        step: 0,
        request_digest: [0; 32],
    };
    for count in [1023, 1024, 1025] {
        let mut frames = Vec::new();
        for index in 0..count {
            for delta in &deltas {
                let mut delta = delta.clone();
                let call = &mut delta["choices"][0]["delta"]["tool_calls"][0];
                call["index"] = json!(index);
                if let Some(id) = call.get_mut("id") {
                    *id = json!(format!(
                        "{}-{index}",
                        id.as_str().expect("recorded call ID")
                    ));
                }
                frames.push(bytes::Bytes::from(format!("data: {delta}\n\n")));
            }
        }
        frames.extend(
            recorded[tail..]
                .iter()
                .map(|data| bytes::Bytes::from(format!("data: {data}\n\n"))),
        );
        for maximum in [2048, 4 * 1024 * 1024] {
            let body = Box::pin(futures::stream::iter(frames.iter().cloned().map(Ok)));
            let output = futures::executor::block_on(
                events(Adapter::Chat, body, maximum, dispatch).try_collect::<Vec<_>>(),
            );
            if maximum == 2048 {
                assert!(output.is_err(), "retained bytes still require capacity");
                continue;
            }
            let output = output?;
            assert_eq!(output.len(), count + 1);
            for (index, event) in output[..count].iter().enumerate() {
                let ModelEvent::ToolCall(call) = event else {
                    panic!("original call missing")
                };
                assert_eq!(call.call_id, dispatch.tool_call_id(index as u64));
                assert_eq!(call.name, "echo");
                assert_eq!(call.arguments, json!({"value":1}));
            }
            let Some(ModelEvent::Completed { metadata }) = output.last() else {
                panic!("original completion missing")
            };
            assert_eq!(metadata["usage"]["prompt_tokens"], 159);
            assert_eq!(metadata["usage"]["completion_tokens"], 19);
        }
    }
    Ok(())
}

#[test]
fn replacements_and_usage_updates_release_their_old_budget() -> Result<()> {
    let mut state = State::new(1024);
    let usage = json!({"input_tokens":10,"output_tokens":1});
    state.usage(&usage)?;
    let retained = state.budget.used();
    for _ in 0..10_000 {
        state.usage(&usage)?;
    }
    assert_eq!(state.budget.used(), retained);
    let item = json!({"type":"reasoning","encrypted_content":"opaque","summary":[]});
    state.retain_thinking(0, &item)?;
    let retained = state.budget.used();
    for _ in 0..10_000 {
        state.retain_thinking(0, &item)?;
    }
    assert_eq!(state.budget.used(), retained);
    Ok(())
}

#[test]
fn cumulative_fragments_and_json_payloads_are_admitted_before_growth() -> Result<()> {
    let mut state = State::new(256);
    let (call, budget) = state.call(0)?;
    budget.string(&mut call.id, "call")?;
    let fragment = Value::String("x".repeat(100));
    budget.append(&mut call.arguments, Some(&fragment))?;
    let before = (call.arguments.len(), call.arguments.capacity());
    assert!(budget.append(&mut call.arguments, Some(&fragment)).is_err());
    assert_eq!((call.arguments.len(), call.arguments.capacity()), before);
    assert!(budget.string(&mut call.native, &"x".repeat(256)).is_err());
    assert!(call.native.is_empty());
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
mod accounting {
    use super::*;
    use proptest::prelude::*;

    fn scanned_bytes(state: &State) -> (u64, usize) {
        let mut visits = 0;
        use acyclic_harness_core::model::json_value_logical_owned_bytes as owned;
        let calls: u64 = state
            .calls
            .values()
            .map(|c| {
                visits += 1;
                (std::mem::size_of::<(u64, Call)>()
                    + c.id.capacity()
                    + c.name.capacity()
                    + c.arguments.capacity()
                    + c.native.capacity()) as u64
            })
            .sum();
        let texts: u64 = state
            .texts
            .values()
            .map(|t| {
                visits += 1;
                (std::mem::size_of::<(u64, Text)>() + t.text.capacity() + t.native.capacity())
                    as u64
            })
            .sum();
        let metadata: u64 = state
            .metadata
            .iter()
            .map(|(k, v)| {
                visits += 1;
                (std::mem::size_of::<(String, Value)>() + k.capacity()) as u64 + owned(v).unwrap()
            })
            .sum();
        let thinking: u64 = state
            .thinking
            .values()
            .map(|v| {
                visits += 1;
                (std::mem::size_of::<(u64, reasoning::Thinking)>() + v.bytes()) as u64
            })
            .sum();
        (
            std::mem::size_of::<State>() as u64
                + calls
                + texts
                + metadata
                + thinking
                + state.origin.as_ref().map_or(0, |origin| {
                    (origin.provider.capacity() + origin.model.capacity()) as u64
                }),
            visits,
        )
    }

    #[test]
    fn recount_negative_control_exceeds_the_linear_work_bound() {
        for count in [128, 256] {
            let mut state = State::new(1024 * 1024);
            let mut rescanned = 0;
            for i in 0..count {
                state.call(i).unwrap();
                rescanned += scanned_bytes(&state).1;
            }
            assert_eq!(rescanned, (count * (count + 1) / 2) as usize);
            assert!(
                rescanned > 2 * count as usize,
                "the former complete inventory violates the per-frame linear bound"
            );
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        // Property: incremental inventory covers the independent complete heap
        // inventory; per-frame accounting does not traverse prior retained items.
        #[test]
        fn retained_byte_inventory_is_conservative_and_incremental(
            calls in 1u64..=256, text in ".{0,32}", reasoning in any::<bool>(),
        ) {
            let work = acyclic_fs::NativeMaterializationWork::new(
                acyclic_fs::performance::work_budget(100_000, 4*1024*1024),
                &acyclic_fs::CancellationToken::new(),
            ).unwrap();
            let mut state = State::native(1024*1024, &work);
            let mut queries = 0;
            for index in 0..calls {
                let event = json!({"choices":[{"delta":{"tool_calls":[{
                    "index":index,"id":format!("call-{index}"),
                    "function":{"name":"echo","arguments":text}
                }]}}]});
                if chat::parse(&mut state, &event).is_err() { break; }
                if reasoning {
                    if state.retain_thinking(index, &json!({"type":"reasoning","summary":[{"text":text}]})).is_err() { break; }
                }
                // A query reads exactly the scalar balance maintained at mutation.
                prop_assert_eq!(state.logical_owned_bytes().unwrap(),
                    std::mem::size_of::<State>() as u64 + state.budget.used() as u64);
                queries += 1;
                prop_assert_eq!(work.receipt().items_examined, queries);
            }
            prop_assert!(state.logical_owned_bytes().unwrap() >= scanned_bytes(&state).0);
            // Real production accounting does one examined item per query,
            // independently of the number of retained calls/reasoning blocks.
            prop_assert_eq!(work.receipt().items_examined, queries + 1);
        }
    }
}
