use super::instruction_tests::file;
use super::*;
use proptest::prelude::*;

fn exchange(file: &FileRef, range: Option<(u64, u64)>) -> [ModelMessage; 2] {
    let mut arguments = serde_json::json!({"file": file});
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
                    value: Value::String("rules".into()),
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
        content: ToolResultContent::Json {
            value: Value::String(body),
        },
        ..
    }) = &message.content
    else {
        panic!("result exchange lost")
    };
    body
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
        for count in [n,2*n] {
            for native in [false,true] {
                let ((output, receipt, remaining_custody), trace): (_,Vec<(&'static str,u64)>) = observe(||fixture(count), |mut messages| super::instruction_index::traced(|| {
                    let measured = crate::test_support::accounted::accounted(native,32*1024*1024,false,|io| {
                        deduplicate_reads(&mut messages,Some(io)).unwrap();
                    }).unwrap();
                    (messages,measured.1,measured.5)
                }));
                let steps:u64 = trace.iter().filter(|(name,_)|*name=="instruction-step").map(|(_,items)|items).sum();
                prop_assert_eq!(steps,144*count as u64-28);
                prop_assert!(receipt.items_examined >= steps);
                prop_assert_eq!(remaining_custody,if native { acyclic_fs::NativeMaterializationWork::accounting_state_bytes() } else { 0 });
                // Independent baseline constructs the expected projection, never
                // calling the candidate decoder or sharing its retained state.
                let (baseline, baseline_trace): (_,Vec<(&'static str,u64)>) = observe(||fixture(count), |mut messages| {
                    use sha2::Digest;
                    for exchange in messages[1..].chunks_exact_mut(2) {
                        let ModelContent::Part(ModelContentPart::ToolCall(call))=&exchange[0].content else { unreachable!() };
                        let input:crate::tool::files::ReadFileInput=serde_json::from_value(call.arguments.clone()).unwrap();
                        let ModelContent::Part(ModelContentPart::ToolResult { content:ToolResultContent::Json{value:Value::String(body)},.. })=&mut exchange[1].content else { unreachable!() };
                        let hash:[u8;32]=sha2::Sha256::digest(body.as_bytes()).into();
                        assert_eq!(hash,*input.file.descriptor().sha256());
                        acyclic_test_support::differential::record(("verify",body.len() as u64));
                        *body="Read completed; instructions already included from AGENTS.md for ..".into();
                    }
                    messages
                });
                // Compare the complete authentication observer trace. The independent
                // semantic baseline has no native custody implementation; check
                // that protocol separately, rather than fabricating its events.
                prop_assert_eq!(trace.iter().filter(|(name,_)|*name=="verify").copied().collect::<Vec<_>>(),baseline_trace);
                let custody:Vec<_>=trace.iter().filter(|(name,_)|name.starts_with("custody-")).map(|(name,_)|*name).collect();
                let expected_custody=if native {
                    ["custody-take","custody-absorb","custody-transfer","custody-absorb"].repeat(count)
                } else { Vec::new() };
                prop_assert_eq!(custody,expected_custody);
                prop_assert_eq!(serde_json::to_vec(&output).unwrap(),serde_json::to_vec(&baseline).unwrap());
                let (_, broken): (_,Vec<(&'static str,u64)>) = observe(||fixture(count), |mut messages| super::instruction_index::traced(|| {
                    // Deliberately restore a whole-history visit before each result.
                    for result in (2..messages.len()).step_by(2) {
                        for part in messages[..result].iter().flat_map(|m|m.content.parts()) {
                            std::hint::black_box(part);
                            super::instruction_index::step(None,1).unwrap();
                        }
                    }
                    deduplicate_reads(&mut messages,None).unwrap();
                }));
                prop_assert_eq!(broken.iter().filter(|(name,_)|*name=="instruction-step").map(|(_,items)|items).sum::<u64>(), steps + (count*(count+1)) as u64);
                prop_assert_ne!(broken.iter().filter(|(name,_)|*name=="instruction-step").map(|(_,items)|items).sum::<u64>(),144*count as u64-28);
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn radix_index_round_trips_updates_and_consumption(keys in prop::collection::vec(any::<[u8;32]>(),0..24)) {
        let mut work=None;
        let mut quota=crate::wire_codec::CodecQuota::new(&mut work,0).unwrap();
        let mut index=super::instruction_index::Index::new(keys.len(),&mut quota).unwrap();
        for (position,key) in keys.iter().enumerate() { index.set(0,*key,Some((position,0)),None).unwrap(); }
        for (position,key) in keys.iter().enumerate() {
            let expected=keys.iter().rposition(|candidate|candidate==key).unwrap();
            prop_assert_eq!(index.get(0,key,None).unwrap(),Some((expected,0)));
            prop_assert_eq!(index.get(1,key,None).unwrap(),None);
            let _=position;
        }
        for key in keys { index.set(0,key,None,None).unwrap(); prop_assert_eq!(index.get(0,&key,None).unwrap(),None); }
    }
}

#[cfg(all(
    feature = "filesystem",
    feature = "test-support",
    not(target_arch = "wasm32")
))]
#[test]
fn radix_storage_has_one_funded_allocation_with_redundant_storage_control() {
    for extra in [false, true] {
        let measured =
            crate::test_support::accounted::accounted(true, 8 * 1024 * 1024, false, |io| {
                let initial = io.receipt().allocation_operations;
                with_history_io(Some(io), |work| {
                    let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
                    let mut index = super::instruction_index::Index::new(256, &mut quota)?;
                    let redundant = extra.then(|| {
                        super::instruction_index::Index::<(usize, usize)>::new(256, &mut quota)
                            .unwrap()
                    });
                    for n in 0u8..=127 {
                        let mut key = [0u8; 32];
                        key[31] = n;
                        index.set(0, key, Some((n as usize, 0)), Some(io))?;
                        assert_eq!(index.get(0, &key, Some(io))?, Some((n as usize, 0)));
                    }
                    drop(redundant);
                    drop(index);
                    Ok::<_, Error>(())
                })
                .unwrap();
                io.receipt().allocation_operations - initial
            })
            .unwrap();
        assert_eq!(measured.0, 1 + u64::from(extra));
        assert_eq!(
            measured.5,
            acyclic_fs::NativeMaterializationWork::accounting_state_bytes()
        );
    }
}
