use super::instruction_tests::file;
use super::*;
use proptest::prelude::*;

fn exchange(file: &FileRef, range: Option<(u64, u64)>) -> [ModelMessage; 2] {
    let mut arguments =
        serde_json::json!({"path":file.path(),"version":serde_json::to_string(file).unwrap()});
    if let Some((start, end)) = range {
        arguments["range"] = serde_json::json!({"start":start,"end":end});
    }
    [
        ModelMessage {
            role: ModelRole::Assistant,
            content: ModelContent::Part(ModelContentPart::ToolCall(ToolCall::new(
                "reused".into(),
                "read".into(),
                arguments,
            ))),
        },
        ModelMessage {
            role: ModelRole::Tool,
            content: ModelContent::Part(ModelContentPart::ToolResult {
                call_id: "reused".into(),
                name: "read".into(),
                content: ToolResultContent::Json {
                    value: serde_json::json!({"path":file.path(),"version":serde_json::to_string(file).unwrap(),"text":"rules"}),
                },
            }),
        },
    ]
}
#[cfg(all(
    feature = "filesystem",
    feature = "test-support",
    not(target_arch = "wasm32")
))]
fn fixture(reads: usize) -> Vec<ModelMessage> {
    let source = file("AGENTS.md", "rules", "rules");
    let mut messages = vec![project_instruction(&source.clone().into(), None).unwrap()];
    for _ in 0..reads {
        messages.extend(exchange(&source, None));
    }
    messages
}
fn result_body(message: &ModelMessage) -> &str {
    let ModelContent::Part(ModelContentPart::ToolResult {
        content: ToolResultContent::Json { value },
        ..
    }) = &message.content
    else {
        panic!("result exchange lost")
    };
    value
        .get("text")
        .and_then(Value::as_str)
        .expect("read text")
}
#[test]
fn reused_ids_consume_pending_calls_in_both_orders_and_partial_reads() {
    for unrelated_first in [false, true] {
        for partial in [false, true] {
            let source = file("nested/AGENTS.md", "rules", "rules");
            let other = if partial {
                source.clone()
            } else {
                file("other.md", "rules", "rules")
            };
            let unrelated = exchange(&other, partial.then_some((0, 2)));
            let repeated = exchange(&source, None);
            let mut messages = vec![project_instruction(&source.clone().into(), None).unwrap()];
            let (first, second) = if unrelated_first {
                (unrelated, repeated)
            } else {
                (repeated, unrelated)
            };
            messages.extend(first);
            messages.extend(second);
            let calls = [messages[1].clone(), messages[3].clone()];
            deduplicate_reads(&mut messages, None).unwrap();
            assert_eq!([messages[1].clone(), messages[3].clone()], calls);
            let (kept, deduped) = if unrelated_first { (2, 4) } else { (4, 2) };
            assert_eq!(result_body(&messages[kept]), "rules");
            assert!(result_body(&messages[deduped]).starts_with("Read completed;"));
        }
    }
}

#[cfg(all(
    feature = "filesystem",
    feature = "test-support",
    not(target_arch = "wasm32")
))]
proptest! {
    #![proptest_config(ProptestConfig::with_cases(8))]
    #[test]
    fn read_projection_has_exact_linear_index_work_and_quadratic_negative(n in 2usize..16) {
        use acyclic_test_support::differential::observe;
        for native in [false,true] {
            let mut costs = Vec::new();
            for count in [n,2*n,3*n] {
                let ((output, receipt, remaining_custody), trace): (_,Vec<(&'static str,u64)>) = observe(||fixture(count), |mut messages| super::instruction_index::traced(|| {
                    let measured = crate::test_support::accounted::accounted(native,32*1024*1024,false,|io| {
                        deduplicate_reads(&mut messages,Some(io)).unwrap();
                    }).unwrap();
                    (messages,measured.1,measured.5)
                }));
                let steps:u64 = trace.iter().filter(|(name,_)|*name=="instruction-step").map(|(_,items)|items).sum();
                prop_assert_eq!(steps,4*count as u64+4,"history traversal must visit only the presence prefix and each message/part once");
                prop_assert!(receipt.items_examined >= steps);
                costs.push((receipt.items_examined,receipt.allocation_operations));
                prop_assert_eq!(remaining_custody,if native { acyclic_fs::NativeMaterializationWork::accounting_state_bytes() } else { 0 });
                // Independent baseline constructs the expected projection, never
                // calling the candidate decoder or sharing its retained state.
                let (baseline, baseline_trace): (_,Vec<(&'static str,u64)>) = observe(||fixture(count), |mut messages| {
                    use sha2::Digest;
                    for exchange in messages[1..].chunks_exact_mut(2) {
                        let ModelContent::Part(ModelContentPart::ToolCall(call))=&exchange[0].content else { unreachable!() };
                        let file:FileRef=serde_json::from_str(call.arguments["version"].as_str().unwrap()).unwrap();
                        let ModelContent::Part(ModelContentPart::ToolResult { content:ToolResultContent::Json{value},.. })=&mut exchange[1].content else { unreachable!() };
                        let Some(Value::String(body))=value.get_mut("text") else { unreachable!() };
                        let hash:[u8;32]=sha2::Sha256::digest(body.as_bytes()).into();
                        assert_eq!(hash,*file.descriptor().sha256());
                        acyclic_test_support::differential::record(("verify",body.len() as u64));
                        *body="Read completed; instructions already included from AGENTS.md for ..".into();
                    }
                    messages
                });
                // Compare the complete authentication observer trace. The independent
                // semantic baseline has no native custody implementation; check
                // that protocol separately, rather than fabricating its events.
                prop_assert_eq!(trace.iter().filter(|(name,_)|*name=="verify").copied().collect::<Vec<_>>(),baseline_trace);
                let custody:Vec<_>=trace.iter().filter(|(name,_)|name.starts_with("custody-")).copied().collect();
                prop_assert_eq!(custody.len(),if native { 2*count } else { 0 });
                for transfer in custody.chunks_exact(2) {
                    prop_assert_eq!(transfer[0].0,"custody-take");
                    prop_assert_eq!(transfer[1],("custody-absorb",result_body(&baseline[2]).len() as u64));
                }
                prop_assert_eq!(serde_json::to_vec(&output).unwrap(),serde_json::to_vec(&baseline).unwrap());
            }
            // Original receipts include the real shared-index work, decoding,
            // authentication and projection. Equal input deltas must have
            // equal work/allocation deltas; no private radix model is replayed.
            prop_assert_eq!(costs[2].0-costs[1].0,costs[1].0-costs[0].0);
            prop_assert_eq!(costs[2].1-costs[1].1,costs[1].1-costs[0].1);
        }
    }
}
