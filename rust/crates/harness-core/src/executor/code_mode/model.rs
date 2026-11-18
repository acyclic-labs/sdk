//! The production interpreter with real declarations, allocation and drop paths.
use super::{
    allocator::{EngineAllocator, EngineResult},
    interpreter,
};
use acyclic_stream::OwnedBytes;
use futures::FutureExt;

type ObservationError = Box<dyn std::error::Error + Send + Sync>;
type Observation<T> = Result<T, ObservationError>;

fn take_failure(
    failure: &std::sync::Mutex<Option<crate::Error>>,
) -> Observation<Option<crate::Error>> {
    Ok(failure
        .lock()
        .map_err(|_| "allocator failure carrier is poisoned")?
        .take())
}

fn native_work(deny_allocation: bool) -> Observation<acyclic_fs::NativeMaterializationWork> {
    let mut budget = acyclic_fs::WorkBudget::UNBOUNDED;
    budget.peak_allocation_bytes = if deny_allocation {
        acyclic_fs::NativeMaterializationWork::accounting_state_bytes()
    } else {
        // Fuzz credit bounds hostile expansion; it changes no product default.
        64 * 1024 * 1024
    };
    acyclic_fs::NativeMaterializationWork::new(budget, &acyclic_fs::CancellationToken::new())
        .map_err(Into::into)
}

/// Programs and reply frames must retire their engine and original credit.
/// Quoted input and production-encoded replies have independent value oracles.
/// No host tool, provider or external process runs.
pub async fn check(input: &[u8]) -> Observation<()> {
    let definitions: Vec<_> = crate::tools::catalog()
        .map(|tool| tool.bind().map(|tool| tool.definition))
        .collect::<crate::Result<_>>()?;
    let tool = definitions.first().ok_or("native tool catalog is empty")?;
    let tool_name = serde_json::to_string(&tool.name)?;
    let definitions: OwnedBytes = serde_json::to_vec(&definitions)?.into();
    let text = String::from_utf8_lossy(input);
    let literal = serde_json::to_string(text.as_ref())?;
    let value = check_arguments(input)?;
    check_bridge(&text).await?;
    check_programs(input, &definitions, &literal, &text).await?;
    check_progress(&definitions).await?;
    check_replies(input, value, &definitions, &tool_name, &literal, &text).await?;
    check_settlement(&definitions, &tool_name, &literal).await?;
    Ok(())
}

fn check_arguments(input: &[u8]) -> Observation<Option<serde_json::Value>> {
    use crate::executor::json::{JsonBounds, JsonValueSchema};
    use crate::wire::ErrorCode;
    // Refine the same owned-Value decoder through the caller's original native
    // adapter. Its scoped admission, rather than a test callback, owns storage.
    let expected = crate::executor::json::json_from_slice_bounded(
        std::marker::PhantomData::<serde_json::Value>,
        input,
        JsonBounds {
            maximum_bytes: Some(input.len() as u64),
            maximum_items: None,
            maximum_depth: crate::executor::json::RECORD_MAXIMUM_DEPTH,
        },
        Some(ErrorCode::Invalid),
        Some(&JsonValueSchema::ROOT),
        &mut |_| Ok(()),
    );
    let work = native_work(false)?;
    let baseline = work.live_allocation_bytes()?;
    let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
    let result = io.decode(
        std::marker::PhantomData::<serde_json::Value>,
        input,
        ErrorCode::Invalid,
    );
    match (&result, &expected) {
        (Ok((value, held)), Ok(expected)) => {
            assert_eq!(
                value, expected,
                "native argument admission must preserve the decoded value"
            );
            let owned = std::mem::size_of::<serde_json::Value>() as u64
                + crate::executor::json::json_value_retained_owned_bytes(value)?;
            assert!(
                held.bytes() >= owned,
                "decoded argument storage must retain its native admission"
            );
            assert_eq!(work.live_allocation_bytes()?, baseline + held.bytes());
        }
        (
            Err(crate::Error::NativeJson {
                code,
                source,
                custody,
            }),
            Err(crate::Error::NativeJson {
                code: expected_code,
                source: expected_source,
                ..
            }),
        ) => {
            assert_eq!(code, expected_code);
            assert_eq!(source.classify(), expected_source.classify());
            assert_eq!(
                (source.line(), source.column()),
                (expected_source.line(), expected_source.column())
            );
            let custody = custody
                .as_ref()
                .ok_or("native argument diagnostic lost its admission")?;
            assert!(
                custody.bytes() >= crate::executor::json::sdk_json_diagnostic_bytes(),
                "native argument diagnostic must retain its owned storage"
            );
            assert_eq!(work.live_allocation_bytes()?, baseline + custody.bytes());
        }
        (
            Err(crate::Error::NativeJsonAdmission { code, kind }),
            Err(crate::Error::NativeJsonAdmission {
                code: expected_code,
                kind: expected_kind,
            }),
        ) => assert_eq!((code, kind), (expected_code, expected_kind)),
        (
            Err(crate::Error::NativeBoundary { code, reason }),
            Err(crate::Error::NativeBoundary {
                code: expected_code,
                reason: expected_reason,
            }),
        ) => assert_eq!((code, reason), (expected_code, expected_reason)),
        _ => {
            return Err(format!(
                "native argument admission differs from the production decoder: {:?}; {expected:?}",
                result.as_ref().map(|(value, _)| value)
            )
            .into());
        }
    }
    drop(result);
    drop(io);
    assert_eq!(work.live_allocation_bytes()?, baseline);
    Ok(expected.ok())
}

async fn check_bridge(text: &str) -> Observation<()> {
    let work = native_work(false)?;
    let original = work.live_allocation_bytes()?;
    let allocator = EngineAllocator::new(Some(&work));
    let runtime = rquickjs::AsyncRuntime::new_with_alloc(allocator.clone())?;
    let context = rquickjs::AsyncContext::full(&runtime).await?;
    context
        .with(|ctx| -> Observation<()> {
            let source = rquickjs::String::from_str(ctx, text)?;
            let baseline = work.live_allocation_bytes()?;
            // Measure the actual engine view, including its native header. ASCII
            // may borrow the string; Unicode may require a temporary UTF-8 copy.
            let view = source.clone().to_cstring()?;
            let view_bytes = work.live_allocation_bytes()? - baseline;
            drop(view);
            assert_eq!(work.live_allocation_bytes()?, baseline);

            let copy = allocator.copy_text(&source)?;
            assert_eq!(copy.value, text);
            assert_eq!(
                work.live_allocation_bytes()?,
                baseline + text.len() as u64,
                "bridge bytes must retain the original credit after the engine view retires"
            );
            drop(copy);
            assert_eq!(work.live_allocation_bytes()?, baseline);

            // Reserve the remaining original capacity after startup. First fund
            // only the view, then neither allocation when the view needs bytes.
            for available in [
                (!text.is_empty()).then_some(view_bytes),
                (view_bytes != 0).then_some(0),
            ]
            .into_iter()
            .flatten()
            {
                let reserved = work.claim(
                    work.budget().peak_allocation_bytes - baseline - available,
                    1,
                    acyclic_fs::WorkCounters::UNCHARGED,
                )?;
                let result = allocator.copy_text(&source);
                assert!(result.is_err(), "unfunded bridge bytes must refuse");
                // Consume the original cause as the execution boundary does;
                // the next refusal must produce its own cause on that carrier.
                assert!(matches!(
                    take_failure(&allocator.failure())?,
                    Some(crate::Error::NativeWork {
                        cause: acyclic_fs::NativeTreeMaterializationError::Work(
                            acyclic_fs::WorkError::BudgetExceeded {
                                counter: "peak_allocation_bytes",
                                ..
                            }
                        ),
                        ..
                    })
                ));
                drop(result);
                drop(reserved);
                assert_eq!(work.live_allocation_bytes()?, baseline);
            }
            Ok(())
        })
        .await?;
    drop(context);
    drop(runtime);
    assert_eq!(
        work.live_allocation_bytes()?,
        original,
        "native UTF-8 refusal must retire the original engine allocation"
    );
    Ok(())
}

fn assert_retained_result(
    work: &acyclic_fs::NativeMaterializationWork,
    baseline: u64,
    result: &Result<EngineResult, String>,
) -> Observation<()> {
    let held = result
        .as_ref()
        .ok()
        .and_then(|result| result.custody.as_ref())
        .map_or(0, acyclic_fs::NativeAllocation::bytes);
    assert_eq!(
        work.live_allocation_bytes()?,
        baseline + held,
        "only the original result credit may outlive its engine"
    );
    Ok(())
}

async fn check_programs(
    input: &[u8],
    definitions: &OwnedBytes,
    literal: &str,
    text: &str,
) -> Observation<()> {
    let literal_module = format!("export default {literal}");
    for (source, maximum, deny_allocation) in [
        (
            std::str::from_utf8(input).ok(),
            literal.len().max(4096),
            false,
        ),
        (Some(literal_module.as_str()), literal.len(), false),
        (Some(literal_module.as_str()), literal.len() - 1, false),
        (Some(literal_module.as_str()), literal.len(), true),
        // Invalid syntax must not run an unfunded parser before admission.
        (Some("export default ("), literal.len(), true),
    ] {
        if let Some(source) = source {
            let work = native_work(deny_allocation)?;
            let baseline = work.live_allocation_bytes()?;
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
                false,
            )
            .await;
            let failed = take_failure(&failure)?.is_some();
            assert_retained_result(&work, baseline, &result)?;
            if deny_allocation {
                assert!(result.is_err());
                assert!(failed);
            } else if source == literal_module && !failed {
                if maximum < literal.len() {
                    assert_eq!(
                        result.as_ref().err().map(String::as_str),
                        Some("JavaScript result exceeds admitted output"),
                        "quoted output must refuse one byte beyond its admission"
                    );
                } else {
                    assert_eq!(
                        &result.as_ref().map_err(|error| error.as_str())?.value,
                        &serde_json::Value::String(text.to_string())
                    );
                }
            }
            drop(result);
            assert_eq!(work.live_allocation_bytes()?, baseline);
        }
    }
    Ok(())
}

async fn check_progress(definitions: &OwnedBytes) -> Observation<()> {
    // The original runtime idle event owns all possible promise producers. A
    // promise with none must refuse through the same native-funded execution
    // boundary, without a dispatcher reply or cancellation supplying a wake.
    for (source, producerless) in [
        ("export default await new Promise(() => {})", true),
        // Finite work still completes if the interrupt condition is broken,
        // so the negative control cannot strand its original blocking poll.
        (
            "for (let remaining = 1000000; remaining; remaining--) {}",
            false,
        ),
    ] {
        let work = native_work(false)?;
        let baseline = work.live_allocation_bytes()?;
        let allocator = EngineAllocator::new(Some(&work));
        let failure = allocator.failure();
        let (send, receive) = tokio::sync::mpsc::channel(1);
        let execution = interpreter::run(
            source.into(),
            definitions.clone(),
            send,
            tokio_util::sync::CancellationToken::new(),
            4096,
            allocator,
            16,
            false,
        );
        let result = acyclic_native_runtime::blocking_future(async move {
            execution
                .now_or_never()
                .ok_or("no producer remains to wake this program")
        })
        .await??;
        drop(receive);
        drop(take_failure(&failure)?);
        assert_retained_result(&work, baseline, &result)?;
        if producerless {
            assert_eq!(
                result.as_ref().err().map(String::as_str),
                Some("JavaScript promise cannot settle: no pending jobs or tool calls"),
                "a producerless promise must retain its refusal"
            );
        } else {
            assert!(
                result
                    .as_ref()
                    .is_err_and(|error| error.contains("interrupt")),
                "finite CPU work must reach its protective interrupt"
            );
        }
        drop(result);
        assert_eq!(work.live_allocation_bytes()?, baseline);
    }
    Ok(())
}

async fn check_replies(
    input: &[u8],
    value: Option<serde_json::Value>,
    definitions: &OwnedBytes,
    tool_name: &str,
    literal: &str,
    text: &str,
) -> Observation<()> {
    // Exercise the original reply producer and decoder together. The dispatcher
    // transports one frame; it does not stand in for tool admission or execution.
    let source = format!(
        "let result; try {{ result = {{ok:true,value:await tools[{tool_name}]({{}})}}; }} \
         catch (error) {{ result = {{ok:false,value:String(error.message),class:error.class}}; }}"
    );
    let success = value.unwrap_or_else(|| {
        serde_json::json!([
            null, false, true, i64::MIN, u64::MAX, -0.0, 1.25, text,
            [], {}, {"nested": [text]}
        ])
    });
    let encoded = serde_json::to_string(&success)?;
    let maximum = encoded.len().max(literal.len()).saturating_add(4096);
    let expected = canonical_reply(definitions, &encoded, maximum).await?;
    // Refusals use the same canonical cause producer as settled host calls.
    let cause =
        crate::executor::ToolFailureCause::of(&crate::Error::Invalid(text.to_owned()), &mut None)?;
    let (refusal, _) = crate::tool::value(&cause, None, None)?;
    let expected_refusal: Result<_, String> = Ok(serde_json::json!({
        "ok": false, "value": &cause.message, "class": cause.class,
    }));
    for kind in 0..3 {
        let work = native_work(false)?;
        let baseline = work.live_allocation_bytes()?;
        let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
        let value = if kind == 0 { &success } else { &refusal };
        let reply = if kind < 2 {
            super::encode_reply(kind == 0, value, None, Some(&io))
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
            assert_eq!(work.live_allocation_bytes()?, baseline);
            continue;
        };
        let allocator = EngineAllocator::new(Some(&work));
        let failure = allocator.failure();
        let (send, mut receive) = tokio::sync::mpsc::channel(1);
        let source = if kind == 2 {
            // Ask the original engine about syntax in the same execution. A
            // valid JSON scalar need not be a reply envelope; this relation
            // only requires malformed frames to preserve their refusal.
            format!(
                "let parseable = true; try {{ JSON.parse({literal}); }} \
                 catch {{ parseable = false; }} {source}; export default ({{parseable,outcome:result}})"
            )
        } else {
            format!("{source}; export default result")
        };
        let run = interpreter::run(
            source,
            definitions.clone(),
            send,
            tokio_util::sync::CancellationToken::new(),
            maximum,
            allocator,
            16,
            false,
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
        let failed = take_failure(&failure)?.is_some();
        assert_retained_result(&work, baseline, &result)?;
        if !failed && kind < 2 {
            assert_eq!(
                result.as_ref().map(|result| &result.value),
                if kind == 0 {
                    expected.as_ref().map(|result| &result.value)
                } else {
                    expected_refusal.as_ref()
                },
                "the original host reply must preserve its value and refusal"
            );
        } else if !failed {
            let report = match &result {
                Ok(report) => &report.value,
                Err(error) => {
                    assert_eq!(error, "JavaScript result exceeds admitted output");
                    continue;
                }
            };
            let parseable = report["parseable"]
                .as_bool()
                .ok_or("the original engine did not report raw reply syntax")?;
            if std::str::from_utf8(input).is_err() || !parseable {
                assert_eq!(
                    report["outcome"]["ok"], false,
                    "malformed host JSON must retain its parse refusal"
                );
            }
        }
        drop(result);
        assert_eq!(work.live_allocation_bytes()?, baseline);
    }
    Ok(())
}

async fn canonical_reply(
    definitions: &OwnedBytes,
    encoded: &str,
    maximum: usize,
) -> Observation<Result<EngineResult, String>> {
    // Use the original engine's JSON semantics for numbers and Unicode. Rust
    // neither predicts JS number rounding nor implements another reply decoder.
    // Compare the same envelope and allowance: JS number formatting can expand
    // valid JSON beyond the admitted output, which must retain its exact refusal.
    let source = format!(
        "export default ({{ok:true,value:JSON.parse({})}})",
        serde_json::to_string(encoded)?
    );
    let work = native_work(false)?;
    let baseline = work.live_allocation_bytes()?;
    let allocator = EngineAllocator::new(Some(&work));
    let failure = allocator.failure();
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
        false,
    )
    .await;
    drop(take_failure(&failure)?);
    assert_retained_result(&work, baseline, &result)?;
    assert!(
        result.is_ok()
            || matches!(&result, Err(error) if error == "JavaScript result exceeds admitted output"),
        "serializer-produced JSON must complete or retain its output refusal: {result:?}"
    );
    Ok(result)
}

async fn check_settlement(
    definitions: &OwnedBytes,
    tool_name: &str,
    literal: &str,
) -> Observation<()> {
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
            let work = native_work(false)?;
            let baseline = work.live_allocation_bytes()?;
            let allocator = EngineAllocator::new(Some(&work));
            let failure = allocator.failure();
            let source = if console {
                format!(
                    "export default await console.log({literal}, {{value:42}}, null, [], true, 1)"
                )
            } else {
                format!("export default await tools[{tool_name}]({{}})")
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
                false,
            ));
            let dispatch = async move {
                let Some(request) = receive.recv().await else {
                    return Ok((false, None));
                };
                drop(receive);
                match settlement {
                    Settlement::Console => {
                        let super::Request::Log { arguments, reply } = request else {
                            return Err("console must use its original log request".into());
                        };
                        check_logs(&arguments.value)?;
                        let _ = reply.send(None);
                    }
                    Settlement::Cancel => {
                        cancellation.cancel();
                        // Keep the reply producer alive until execution ends:
                        // only the original cancellation event can wake it.
                        return Ok((true, Some(request)));
                    }
                    Settlement::Retire => drop(request),
                }
                Ok::<_, ObservationError>((true, None))
            };
            let (result, dispatch) = futures::join!(run, dispatch);
            let (admitted, retained) = dispatch?;
            drop(retained);
            let failed = take_failure(&failure)?.is_some();
            if let Ok(result) = &result {
                assert_retained_result(&work, baseline, result)?;
            }
            if !failed {
                assert!(admitted, "the engine must submit its original RPC");
                let result = result.as_ref().map_err(|error| error.to_string())?;
                match settlement {
                    Settlement::Retire => {
                        assert!(result.is_err(), "a retired reply producer must refuse");
                    }
                    Settlement::Console => {
                        assert_eq!(
                            result.as_ref().map_err(|error| error.as_str())?.value,
                            serde_json::Value::Null
                        );
                    }
                    Settlement::Cancel => assert_eq!(
                        result.as_ref().err().map(String::as_str),
                        Some("JavaScript interrupted"),
                        "parent cancellation must retain its interruption"
                    ),
                }
            }
            drop(result);
            assert_eq!(work.live_allocation_bytes()?, baseline);
        }
    }
    Ok(())
}

/// Differential checks consume the frame the production engine actually emitted.
/// The same retained fuzz target covers escaped strings, bounds and native denials.
fn check_logs(frame: &str) -> Observation<()> {
    use super::ProgramLogs;
    use serde_json::{Value, json};
    let raw: &serde_json::value::RawValue = serde_json::from_str(frame)?;
    let arguments: Vec<Value> = serde_json::from_str(frame)?;
    let expected = arguments
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| value.to_string())
        })
        .collect::<Vec<_>>()
        .join(" ");
    for maximum in [0, 64, frame.len() + 64, frame.len() + 128] {
        let work = native_work(false)?;
        let baseline = work.live_allocation_bytes()?;
        {
            let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
            let mut logs = ProgramLogs::default();
            let before = io.receipt()?;
            logs.observe(raw, maximum, Some(&io))?;
            if logs.bytes > 0 {
                assert!(
                    io.receipt()?.bytes_copied - before.bytes_copied >= expected.len() as u64,
                    "retained log text must fund its actual copy"
                );
            }
            if maximum == 0 {
                assert!(logs.lines.is_empty());
                assert_eq!(logs.omitted, 1);
                assert_eq!(
                    io.receipt()?,
                    before,
                    "omitted logs need no decode, allocation or copy"
                );
                assert_eq!(work.live_allocation_bytes()?, baseline);
                continue;
            }
            let mut output = json!({"value":42});
            logs.attach(&mut output, maximum, Some(&io))?;
            let receipt = json!({"version":2,"output":output});
            let projected = crate::tools::code_mode::projected(&receipt)?;
            assert!(serde_json::to_vec(projected)?.len() <= maximum);
            if let Value::Object(output) = projected {
                assert_eq!(output["value"], 42);
                if output["logs"]
                    .as_array()
                    .is_some_and(|logs| !logs.is_empty())
                {
                    assert_eq!(output["logs"], json!([expected]));
                }
            } else {
                assert_eq!(projected, 42);
            }
        }
        assert_eq!(work.live_allocation_bytes()?, baseline);
    }
    // Exhaust the original owner after initialization. A fitting line must
    // refuse before its argument index or decoded text allocates. An omitted
    // line needs neither allocation and must still make progress.
    let work = native_work(false)?;
    let baseline = work.live_allocation_bytes()?;
    let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
    let held = work.claim(
        work.budget().peak_allocation_bytes - work.live_allocation_bytes()?,
        1,
        acyclic_fs::WorkCounters::UNCHARGED,
    )?;
    assert!(
        ProgramLogs::default()
            .observe(raw, frame.len() + 128, Some(&io))
            .is_err()
    );
    ProgramLogs::default().observe(raw, 0, Some(&io))?;
    drop(held);
    drop(io);
    assert_eq!(work.live_allocation_bytes()?, baseline);
    Ok(())
}

#[tokio::test]
async fn repeated_console_logs_release_temporary_decode_credit() -> Observation<()> {
    let mut budget = acyclic_fs::WorkBudget::UNBOUNDED;
    budget.peak_allocation_bytes = 32 * 1024;
    let work =
        acyclic_fs::NativeMaterializationWork::new(budget, &acyclic_fs::CancellationToken::new())?;
    let baseline = work.live_allocation_bytes()?;
    // Keep engine activity on its own finite owner so the collector's live
    // credit can be measured without racing the engine's drop paths.
    let engine_work = native_work(false)?;
    let engine_baseline = engine_work.live_allocation_bytes()?;
    let allocator = EngineAllocator::new(Some(&engine_work));
    let (send, receive) = tokio::sync::mpsc::channel(1);
    let run = acyclic_native_runtime::blocking_future(interpreter::run(
        "for (let i = 0; i < 256; i++) await console.log('a\\nb'); export default 42".into(),
        b"[]".as_slice().into(),
        send,
        tokio_util::sync::CancellationToken::new(),
        4096,
        allocator,
        16,
        false,
    ));
    let dispatch = async {
        let mut receive = receive;
        let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
        let mut logs = super::ProgramLogs::default();
        let mut retained_before = 0;
        while let Some(request) = receive.recv().await {
            let super::Request::Log { arguments, reply } = request else {
                return Err("the program only calls console.log".into());
            };
            let before = work.live_allocation_bytes()?;
            let raw = serde_json::from_str(&arguments.value)?;
            logs.observe(raw, 32 * 1024, Some(&io))?;
            let slots = logs.lines.capacity() * std::mem::size_of::<serde_json::Value>();
            let retained = logs.lines.iter().try_fold(slots, |retained, value| {
                let serde_json::Value::String(text) = value else {
                    return Err("console lines must be text");
                };
                Ok(retained + text.capacity())
            })?;
            assert_eq!(
                work.live_allocation_bytes()?,
                before + (retained - retained_before) as u64,
                "only retained log text and its index remain charged"
            );
            retained_before = retained;
            let _ = reply.send(None);
        }
        assert_eq!(logs.lines.len(), 256);
        assert!(logs.lines.iter().all(|line| line == "a\nb"));
        Ok::<_, ObservationError>(())
    };
    let (result, dispatch) = futures::join!(run, dispatch);
    dispatch?;
    let result = result??;
    assert_eq!(result.value, serde_json::json!(42));
    drop(result);
    assert_eq!(work.live_allocation_bytes()?, baseline);
    assert_eq!(engine_work.live_allocation_bytes()?, engine_baseline);
    Ok(())
}
