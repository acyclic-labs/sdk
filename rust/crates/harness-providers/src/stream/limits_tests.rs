use super::*;
use serde_json::json;

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    #[test]
    fn unused_visible_parts_return_the_complete_retained_balance(
        fragments in proptest::collection::vec(".{0,32}", 0..32),
        calls in proptest::bool::ANY, signed in proptest::bool::ANY,
    ) {
        let mut state=State::new(65_536);
        if calls {state.call(0).unwrap();}
        for text in fragments {state.visible_text(&text).unwrap();}
        if signed {
            let index=(state.calls.len()+state.thinking.len()) as u64;
            state.raw_part(index,&json!({"text":"signed","thoughtSignature":"opaque"}),None,true).unwrap();
        }
        let before=state.budget.used();
        state.discard_unsigned_final_text().unwrap();
        proptest::prop_assert_eq!(state.budget.used(),if calls||signed {before} else {(0,0)});
    }
}

#[test]
fn unsigned_gemini_fragments_coalesce_without_crossing_signature_boundaries() -> Result<()> {
    let mut state = State::new(4096);
    for _ in 0..512 {
        state.visible_text("x")?;
    }
    assert_eq!(state.thinking.len(), 1);
    state.raw_part(
        1,
        &json!({"text":"y","thoughtSignature":"opaque"}),
        None,
        true,
    )?;
    for _ in 0..512 {
        state.visible_text("z")?;
    }
    assert_eq!(state.thinking.len(), 3);
    state.terminal = true;
    let dispatch = ModelDispatch {
        operation_id: acyclic_harness_core::OperationId::from_bytes([1; 16]),
        step: 0,
        request_digest: [0; 32],
    };
    let events = finish(state, Adapter::Gemini, dispatch)?;
    let parts: Vec<_> = events
        .into_iter()
        .filter_map(|event| match event {
            ModelEvent::ReasoningBlock { block } => {
                Some(serde_json::from_slice::<Value>(&block.continuity).unwrap())
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        parts,
        vec![
            json!({"text":"x".repeat(512)}),
            json!({"text":"y","thoughtSignature":"opaque"}),
            json!({"text":"z".repeat(512)})
        ]
    );
    Ok(())
}

#[test]
fn small_events_cannot_accumulate_unbounded_calls_or_thinking() -> Result<()> {
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
    assert!(state.budget.used().0 <= 256);
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
    assert!(
        state.budget.used().0 < 1024 * 1024,
        "item bound fires independently of bytes"
    );
    assert!(state.budget.used().1 <= 1024);
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
    let signature = json!({"blocks":["x".repeat(256)]});
    assert!(budget.value(&mut call.metadata, &signature).is_err());
    assert!(call.metadata.is_null());
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
                    + c.arguments.capacity()) as u64
                    + owned(&c.metadata).unwrap()
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
                    std::mem::size_of::<State>() as u64 + state.budget.used().0 as u64);
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
