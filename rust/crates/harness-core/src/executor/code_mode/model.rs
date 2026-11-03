//! The production interpreter with real declarations, allocation and drop paths.
use super::{allocator::EngineAllocator, interpreter};
use acyclic_stream::OwnedBytes;
use futures::FutureExt;

fn native_work(deny_allocation: bool) -> acyclic_fs::NativeMaterializationWork {
    let mut budget = acyclic_fs::WorkBudget::UNBOUNDED;
    budget.peak_allocation_bytes = if deny_allocation {
        acyclic_fs::NativeMaterializationWork::accounting_state_bytes()
    } else {
        // Fuzz credit bounds hostile expansion; it changes no product default.
        64 * 1024 * 1024
    };
    acyclic_fs::NativeMaterializationWork::new(budget, &acyclic_fs::CancellationToken::new())
        .expect("fund the original accounting owner")
}

/// Programs and reply frames must retire their engine and original credit.
/// Quoted input and production-encoded replies have independent value oracles.
/// No host tool, provider or external process runs.
pub async fn check(input: &[u8]) {
    let definitions: Vec<_> = crate::tools::catalog()
        .map(|tool| tool.bind().expect("catalog declaration").definition)
        .collect();
    let tool_name = serde_json::to_string(&definitions[0].name).unwrap();
    let definitions: OwnedBytes = serde_json::to_vec(&definitions).unwrap().into();
    let text = String::from_utf8_lossy(input);
    let literal = serde_json::to_string(text.as_ref()).unwrap();
    check_bridge(&text).await;
    check_programs(input, &definitions, &literal, &text).await;
    check_progress(&definitions).await;
    check_replies(input, &definitions, &tool_name, &literal, &text).await;
    check_settlement(&definitions, &tool_name, &literal).await;
}

async fn check_bridge(text: &str) {
    let work = native_work(false);
    let original = work.live_allocation_bytes();
    let allocator = EngineAllocator::new(Some(&work));
    let runtime = rquickjs::AsyncRuntime::new_with_alloc(allocator.clone()).unwrap();
    let context = rquickjs::AsyncContext::full(&runtime).await.unwrap();
    context
        .with(|ctx| {
            let source = rquickjs::String::from_str(ctx, text).unwrap();
            let baseline = work.live_allocation_bytes();
            // Measure the actual engine view, including its native header. ASCII
            // may borrow the string; Unicode may require a temporary UTF-8 copy.
            let view = source.clone().to_cstring().unwrap();
            let view_bytes = work.live_allocation_bytes() - baseline;
            drop(view);
            assert_eq!(work.live_allocation_bytes(), baseline);

            let copy = allocator.copy_text(&source).unwrap();
            assert_eq!(copy.value, text);
            assert_eq!(
                work.live_allocation_bytes(),
                baseline + text.len() as u64,
                "bridge bytes must retain the original credit after the engine view retires"
            );
            drop(copy);
            assert_eq!(work.live_allocation_bytes(), baseline);

            // Reserve the remaining original capacity after startup. First fund
            // only the view, then neither allocation when the view needs bytes.
            for available in [
                (!text.is_empty()).then_some(view_bytes),
                (view_bytes != 0).then_some(0),
            ]
            .into_iter()
            .flatten()
            {
                let reserved = work
                    .claim(
                        work.budget().peak_allocation_bytes - baseline - available,
                        1,
                        acyclic_fs::WorkCounters::UNCHARGED,
                    )
                    .unwrap();
                let result = allocator.copy_text(&source);
                assert!(result.is_err(), "unfunded bridge bytes must refuse");
                // Consume the original cause as the execution boundary does;
                // the next refusal must produce its own cause on that carrier.
                assert!(matches!(
                    allocator.failure().lock().unwrap().take(),
                    Some(acyclic_fs::NativeTreeMaterializationError::Work(
                        acyclic_fs::WorkError::BudgetExceeded {
                            counter: "peak_allocation_bytes",
                            ..
                        }
                    ))
                ));
                drop(result);
                drop(reserved);
                assert_eq!(work.live_allocation_bytes(), baseline);
            }
        })
        .await;
    drop(context);
    drop(runtime);
    assert_eq!(work.live_allocation_bytes(), original);
}

async fn check_programs(input: &[u8], definitions: &OwnedBytes, literal: &str, text: &str) {
    for (source, maximum, deny_allocation) in [
        (
            std::str::from_utf8(input).ok(),
            literal.len().max(4096),
            false,
        ),
        (Some(literal), literal.len(), false),
        (Some(literal), literal.len() - 1, false),
        (Some(literal), literal.len(), true),
    ] {
        if let Some(source) = source {
            let work = native_work(deny_allocation);
            let baseline = work.live_allocation_bytes();
            let allocator = EngineAllocator::new(Some(&work));
            let failure = allocator.failure();
            let (send, receive) = tokio::sync::mpsc::channel(1);
            // Dropping the actual dispatcher is the producer-loss case. RPC
            // send/receive errors use the same channel path as a retired host.
            drop(receive);
            let result = interpreter::run(
                source.to_owned(),
                definitions.clone(),
                send,
                tokio_util::sync::CancellationToken::new(),
                maximum,
                allocator,
                16,
            )
            .await;
            assert_eq!(work.live_allocation_bytes(), baseline);
            if deny_allocation {
                assert!(result.is_err());
                assert!(failure.lock().unwrap().is_some());
            } else if source == literal && failure.lock().unwrap().is_none() {
                if maximum < literal.len() {
                    assert_eq!(
                        result,
                        Err("JavaScript result exceeds admitted output".into()),
                        "quoted output must refuse one byte beyond its admission"
                    );
                } else {
                    assert_eq!(result.unwrap(), serde_json::Value::String(text.to_string()));
                }
            }
        }
    }
}

async fn check_progress(definitions: &OwnedBytes) {
    // The original runtime idle event owns all possible promise producers. A
    // promise with none must refuse through the same native-funded execution
    // boundary, without a dispatcher reply or cancellation supplying a wake.
    for (source, producerless) in [
        ("await new Promise(() => {})", true),
        // Finite work still completes if the interrupt condition is broken,
        // so the negative control cannot strand its original blocking poll.
        (
            "for (let remaining = 1000000; remaining; remaining--) {}",
            false,
        ),
    ] {
        let work = native_work(false);
        let baseline = work.live_allocation_bytes();
        let allocator = EngineAllocator::new(Some(&work));
        let (send, receive) = tokio::sync::mpsc::channel(1);
        let execution = interpreter::run(
            source.into(),
            definitions.clone(),
            send,
            tokio_util::sync::CancellationToken::new(),
            4096,
            allocator,
            16,
        );
        let result = acyclic_native_runtime::blocking_future(async move {
            execution
                .now_or_never()
                .expect("no producer remains to wake this program")
        })
        .await
        .expect("the original blocking pool must execute");
        drop(receive);
        assert_eq!(work.live_allocation_bytes(), baseline);
        if producerless {
            assert_eq!(
                result,
                Err("JavaScript promise cannot settle: no pending jobs or tool calls".into()),
                "a producerless promise must retain its refusal"
            );
        } else {
            assert!(
                result.is_err_and(|error| error.contains("interrupt")),
                "finite CPU work must reach its protective interrupt"
            );
        }
    }
}

async fn check_replies(
    input: &[u8],
    definitions: &OwnedBytes,
    tool_name: &str,
    literal: &str,
    text: &str,
) {
    // Exercise the original reply producer and decoder together. The dispatcher
    // transports one frame; it does not stand in for tool admission or execution.
    let source = format!(
        "let result; try {{ result = {{ok:true,value:await tools[{tool_name}]({{}})}}; }} \
         catch (error) {{ result = {{ok:false,value:String(error.message)}}; }} result"
    );
    let success = serde_json::from_slice(input).unwrap_or_else(|_| {
        serde_json::json!([
            null, false, true, i64::MIN, u64::MAX, -0.0, 1.25, text,
            [], {}, {"nested": [text]}
        ])
    });
    let encoded = serde_json::to_string(&success).unwrap();
    let maximum = encoded.len().max(literal.len()).saturating_add(4096);
    let expected = canonical_reply(definitions, &encoded, maximum).await;
    let refusal = serde_json::Value::String(text.to_string());
    let expected_refusal = Ok(serde_json::json!({"ok": false, "value": &refusal}));
    for kind in 0..3 {
        let work = native_work(false);
        let baseline = work.live_allocation_bytes();
        let io = crate::filesystem::PreparationIo::from_native_work(&work).unwrap();
        let value = if kind == 0 { &success } else { &refusal };
        let reply = if kind < 2 {
            super::encode_reply(kind == 0, value, Some(&io))
        } else {
            // Arbitrary malformed frames retain their input credit through the
            // same OwnedBytes entrance as the encoder's valid frames.
            (|| {
                let mut custody =
                    Some(work.claim(input.len() as u64, 1, acyclic_fs::WorkCounters::UNCHARGED)?);
                let mut bytes = input.to_vec();
                acyclic_stream::OwnedBytes::from_vec_with_guard_in_place(
                    &mut bytes,
                    &mut custody,
                    &work.runtime_meter(),
                )
                .map_err(|cause| crate::Error::from(work.runtime_error(cause)))
            })()
        };
        let Ok(reply) = reply else {
            drop(io);
            assert_eq!(work.live_allocation_bytes(), baseline);
            continue;
        };
        let allocator = EngineAllocator::new(Some(&work));
        let failure = allocator.failure();
        let (send, mut receive) = tokio::sync::mpsc::channel(1);
        let run = interpreter::run(
            source.clone(),
            definitions.clone(),
            send,
            tokio_util::sync::CancellationToken::new(),
            maximum,
            allocator,
            16,
        );
        let dispatch = async move {
            // The interpreter owns the sender. Early engine retirement closes
            // recv; sending the only reply settles the only admitted RPC wait.
            if let Some(super::Request::Tool(call)) = receive.recv().await {
                let _ = call.reply.send(Some(reply));
            }
        };
        let (result, ()) = futures::join!(run, dispatch);
        drop(io);
        assert_eq!(work.live_allocation_bytes(), baseline);
        let failed = failure.lock().unwrap().is_some();
        if !failed && kind < 2 {
            assert_eq!(
                &result,
                if kind == 0 {
                    &expected
                } else {
                    &expected_refusal
                },
                "the original host reply must preserve its value and refusal"
            );
        } else if !failed && std::str::from_utf8(input).is_err() {
            assert_eq!(result.unwrap()["ok"], false, "invalid UTF-8 must refuse");
        }
    }
}

async fn canonical_reply(
    definitions: &OwnedBytes,
    encoded: &str,
    maximum: usize,
) -> Result<serde_json::Value, String> {
    // Use the original engine's JSON semantics for numbers and Unicode. Rust
    // neither predicts JS number rounding nor implements another reply decoder.
    // Compare the same envelope and allowance: JS number formatting can expand
    // valid JSON beyond the admitted output, which must retain its exact refusal.
    let source = format!(
        "({{ok:true,value:JSON.parse({})}})",
        serde_json::to_string(encoded).unwrap()
    );
    let work = native_work(false);
    let baseline = work.live_allocation_bytes();
    let allocator = EngineAllocator::new(Some(&work));
    let (send, receive) = tokio::sync::mpsc::channel(1);
    drop(receive);
    let result = interpreter::run(
        source,
        definitions.clone(),
        send,
        tokio_util::sync::CancellationToken::new(),
        maximum,
        allocator,
        16,
    )
    .await;
    assert_eq!(work.live_allocation_bytes(), baseline);
    assert!(
        result.is_ok()
            || matches!(&result, Err(error) if error == "JavaScript result exceeds admitted output"),
        "serializer-produced JSON must complete or retain its output refusal: {result:?}"
    );
    result
}

async fn check_settlement(definitions: &OwnedBytes, tool_name: &str, literal: &str) {
    #[derive(Clone, Copy)]
    enum Settlement {
        Retire,
        Console,
        Cancel,
    }
    // Unlike a failed send, retirement/cancellation after recv must wake the
    // original wait. Keep the real blocking-pool boundary in this control.
    for console in [false, true] {
        for settlement in [Settlement::Retire, Settlement::Console, Settlement::Cancel] {
            if !console && matches!(settlement, Settlement::Console) {
                continue; // Successful tool replies are checked above.
            }
            let work = native_work(false);
            let baseline = work.live_allocation_bytes();
            let allocator = EngineAllocator::new(Some(&work));
            let failure = allocator.failure();
            let source = if console {
                format!("await console.log({literal})")
            } else {
                format!("await tools[{tool_name}]({{}})")
            };
            let cancellation = tokio_util::sync::CancellationToken::new();
            let (send, mut receive) = tokio::sync::mpsc::channel(1);
            let run = acyclic_native_runtime::blocking_future(interpreter::run(
                source,
                definitions.clone(),
                send,
                cancellation.clone(),
                4096,
                allocator,
                16,
            ));
            let dispatch = async move {
                let Some(request) = receive.recv().await else {
                    return (false, None);
                };
                drop(receive);
                match settlement {
                    Settlement::Console => {
                        let super::Request::Log { reply, .. } = request else {
                            panic!("console must use its original log request");
                        };
                        let _ = reply.send(None);
                    }
                    Settlement::Cancel => {
                        cancellation.cancel();
                        // Keep the reply producer alive until execution ends:
                        // only the original cancellation event can wake it.
                        return (true, Some(request));
                    }
                    Settlement::Retire => drop(request),
                }
                (true, None)
            };
            let (result, (admitted, retained)) = futures::join!(run, dispatch);
            drop(retained);
            assert_eq!(work.live_allocation_bytes(), baseline);
            if failure.lock().unwrap().is_none() {
                assert!(admitted, "the engine must submit its original RPC");
                let result = result.expect("the original blocking pool must execute");
                match settlement {
                    Settlement::Retire => {
                        assert!(result.is_err(), "a retired reply producer must refuse");
                    }
                    Settlement::Console => {
                        assert_eq!(result.unwrap(), serde_json::Value::Null);
                    }
                    Settlement::Cancel => assert_eq!(
                        result,
                        Err("JavaScript interrupted".into()),
                        "parent cancellation must retain its interruption"
                    ),
                }
            }
        }
    }
}
