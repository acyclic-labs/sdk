use super::*;
use crate::OperationId;
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Memory;
impl CodeMemory for Memory {
    fn credit_bytes(&self) -> Option<NonZeroUsize> {
        None
    }
    fn acquire(&self) -> BoxProviderFuture<'_, Result<Box<dyn CodeCredit>>> {
        Box::pin(async { Ok(Box::new(()) as Box<dyn CodeCredit>) })
    }
}

struct Control {
    owner: tokio::sync::watch::Receiver<bool>,
    remaining: AtomicUsize,
}
impl CodeControl for Control {
    fn check(&self) -> Result<()> {
        self.remaining
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |remaining| {
                remaining.checked_sub(1)
            })
            .map(|_| ())
            .map_err(|_| rejected("original work credit exhausted"))
    }
    fn interrupted(&self) -> BoxProviderFuture<'_, Error> {
        let mut owner = self.owner.clone();
        Box::pin(async move {
            match owner.changed().await {
                Ok(()) => rejected("original execution cancelled"),
                Err(_) => rejected("original execution owner dropped"),
            }
        })
    }
}

fn original_control() -> (tokio::sync::watch::Sender<bool>, Arc<dyn CodeControl>) {
    let (owner, receiver) = tokio::sync::watch::channel(false);
    (
        owner,
        Arc::new(Control {
            owner: receiver,
            remaining: AtomicUsize::new(100_000),
        }),
    )
}

#[derive(Default)]
struct Echo(Mutex<Vec<ToolInvocation>>);
impl CodeHost for Echo {
    fn dispatch(
        &self,
        invocation: ToolInvocation,
    ) -> BoxProviderFuture<'_, Result<CodeHostResult>> {
        Box::pin(async move {
            let value = invocation.arguments.clone();
            lock(&self.0).push(invocation);
            Ok(ToolResult { value }.into())
        })
    }
}

fn outer() -> ToolInvocation {
    ToolInvocation {
        operation_id: OperationId::from_bytes([17; 16]),
        call_id: "original-code-call".into(),
        name: "code".into(),
        arguments: json!({}),
    }
}

fn echo_definition() -> ToolDefinition {
    ToolDefinition {
        name: "echo".into(),
        revision: "v1".into(),
        description: "Echo input".into(),
        input_schema: json!({"type":"object","properties":{"value":{"type":"string"}},"required":["value"],"additionalProperties":false}),
        output_schema: json!({"type":"object","additionalProperties":true}),
        projection_schema: json!({"type":"object"}),
    }
}

#[tokio::test]
async fn sandbox_uses_only_host_calls_and_replays_seeded_values_and_identity() -> Result<()> {
    let runtime = CodeRuntime::new(Arc::new(Memory))?;
    let (_owner, control) = original_control();
    let host = Arc::new(Echo::default());
    let source = r#"
        interface Message { value: string }
        const message: Message = {value: "admitted"};
        return {result: await echo(message), random: Math.random(), now: Date.now(),
            date: Date(), reflected: typeof Object.getPrototypeOf(Date).now,
            ambient: [typeof process, typeof require, typeof fetch, typeof std, typeof os],
            blocking: [typeof Atomics, typeof SharedArrayBuffer, typeof WeakRef]};
    "#;
    let tools = [echo_definition()];
    let result = runtime
        .execute(&outer(), source, &tools, host.clone(), control.clone())
        .await?;
    let replay = runtime
        .execute(&outer(), source, &tools, host.clone(), control.clone())
        .await?;
    assert_eq!(result, replay);
    assert_eq!(result["result"], json!({"value":"admitted"}));
    assert_eq!(result["reflected"], "undefined");
    assert_eq!(result["ambient"], json!(vec!["undefined"; 5]));
    assert_eq!(result["blocking"], json!(vec!["undefined"; 3]));
    let calls = lock(&host.0);
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0], calls[1]);
    assert_ne!(calls[0].operation_id, outer().operation_id);
    drop(calls);
    // Negative: an ambient filesystem/network authority cannot be manufactured.
    let refused = runtime
        .execute(
            &outer(),
            "return await fetch('https://invalid/');",
            &tools,
            host.clone(),
            control,
        )
        .await;
    assert!(refused.is_err());
    assert_eq!(lock(&host.0).len(), 2);
    let input = json!({"payload": "'); await echo({value:'injected'}); //", "nested": [1, true]});
    let (_owner, control) = original_control();
    assert_eq!(
        runtime
            .execute_with_input(
                &outer(),
                "return input;",
                &input,
                &[],
                host.clone(),
                control,
            )
            .await?,
        input
    );
    assert_eq!(lock(&host.0).len(), 2);
    Ok(())
}

#[cfg(feature = "filesystem")]
#[tokio::test]
async fn javascript_cannot_catch_original_heap_exhaustion_into_success() -> Result<()> {
    use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(
        WorkBudget {
            peak_allocation_bytes: 32 * 1024 * 1024,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let baseline = work.live_allocation_bytes();
    let runtime = CodeRuntime::new_with_native_work(Arc::new(Memory), &work)?;
    // Retain all but one MiB of the real remaining configured capacity after
    // loading the shared compiler. The tested execution allowance is unchanged.
    let blocker = work.claim(
        work.remaining()?
            .peak_allocation_bytes
            .saturating_sub(1024 * 1024),
        1,
        acyclic_fs::WorkCounters::UNCHARGED,
    )?;
    let (_owner, control) = original_control();
    let host = Arc::new(Echo::default());
    let result = runtime
        .execute(
            &outer(),
            "try { return new Array(1048576).fill(0).length; } catch (_) { return 'forged success'; }",
            &[],
            host.clone(),
            control.clone(),
        )
        .await;
    assert!(matches!(
        result,
        Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Work(
                acyclic_fs::WorkError::BudgetExceeded {
                    counter: "peak_allocation_bytes",
                    ..
                }
            ),
            ..
        })
    ));
    // The original denial has retired the transient realm; it cannot poison
    // independent subsequent code or acknowledge a host effect.
    assert_eq!(
        runtime
            .execute(&outer(), "return 7;", &[], host.clone(), control)
            .await?,
        json!(7)
    );
    assert!(lock(&host.0).is_empty());
    drop((runtime, blocker));
    assert_eq!(work.live_allocation_bytes(), baseline);
    Ok(())
}

#[tokio::test]
async fn caught_protocol_forgery_cannot_start_dependent_host_effects() -> Result<()> {
    let runtime = CodeRuntime::new(Arc::new(Memory))?;
    let (_owner, control) = original_control();
    let host = Arc::new(Echo::default());
    let tools = [echo_definition()];
    for source in [
        "try { __run(() => 'forged'); } catch (_) {} return await echo({value:'dependent'});",
        "const pending = echo({value:'original'}); try { __complete('0', '{\"value\":\"forged\"}'); } catch (_) {} return await pending;",
    ] {
        let result = runtime
            .execute(&outer(), source, &tools, host.clone(), control.clone())
            .await;
        assert!(matches!(
            result,
            Err(Error::ToolRejected {
                reason: "code forged an interpreter protocol entry",
                ..
            })
        ));
        assert!(lock(&host.0).is_empty());
    }
    // The permit is consumed before resolving a returned object. A hostile
    // thenable getter runs during the admitted completion, but cannot reuse its
    // permission or issue the dependent effect queued from that getter.
    let result = runtime
        .execute(
            &outer(),
            r#"
                const pending = echo({value: 'original'});
                Object.defineProperty(Object.prototype, 'then', {get() {
                    try { __run(() => 'forged'); } catch (_) {}
                    echo({value: 'dependent'});
                    return undefined;
                }});
                return await pending;
            "#,
            &tools,
            host.clone(),
            control.clone(),
        )
        .await;
    assert!(matches!(
        result,
        Err(Error::ToolRejected {
            reason: "code forged an interpreter protocol entry",
            ..
        })
    ));
    assert_eq!(lock(&host.0).len(), 1);
    assert_eq!(lock(&host.0)[0].arguments, json!({"value":"original"}));
    // The same shared runtime still delivers actual admitted host results.
    assert_eq!(
        runtime
            .execute(
                &outer(),
                "return await echo({value:'admitted'});",
                &tools,
                host.clone(),
                control,
            )
            .await?,
        json!({"value":"admitted"})
    );
    assert_eq!(lock(&host.0).len(), 2);
    Ok(())
}

#[tokio::test]
async fn producerless_promises_and_exhausted_original_work_refuse_without_waiting() -> Result<()> {
    let runtime = CodeRuntime::new(Arc::new(Memory))?;
    let (_owner, control) = original_control();
    let host = Arc::new(Echo::default());
    let failed = runtime
        .execute(
            &outer(),
            "return await new Promise(() => {});",
            &[],
            host.clone(),
            control,
        )
        .await;
    assert!(matches!(
        failed,
        Err(Error::ToolRejected {
            reason: "code promise has no live host-call producer",
            ..
        })
    ));
    let (_owner, receiver) = tokio::sync::watch::channel(false);
    let control = Arc::new(Control {
        owner: receiver,
        remaining: AtomicUsize::new(20),
    });
    let failed = runtime
        .execute(&outer(), "while (true) {}", &[], host, control)
        .await;
    assert!(matches!(
        failed,
        Err(Error::ToolRejected {
            reason: "original work credit exhausted",
            ..
        })
    ));
    Ok(())
}

struct HeldHost {
    started: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    released: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}
impl CodeHost for HeldHost {
    fn dispatch(&self, _: ToolInvocation) -> BoxProviderFuture<'_, Result<CodeHostResult>> {
        let started = lock(&self.started).take();
        let released = lock(&self.released).take();
        Box::pin(async move {
            started
                .ok_or_else(|| rejected("duplicate dispatch"))?
                .send(())
                .map_err(|_| rejected("execution owner dropped"))?;
            released
                .ok_or_else(|| rejected("duplicate dispatch"))?
                .await
                .map_err(|_| rejected("host producer dropped"))?;
            Ok(ToolResult {
                value: json!("released"),
            }
            .into())
        })
    }
}

#[tokio::test]
async fn host_wait_releases_shared_interpreter_and_owner_drop_terminates() -> Result<()> {
    let runtime = CodeRuntime::new(Arc::new(Memory))?;
    let (_owner, control) = original_control();
    let (started, ready) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let host = Arc::new(HeldHost {
        started: Mutex::new(Some(started)),
        released: Mutex::new(Some(released)),
    });
    let tools = [echo_definition()];
    let outer = outer();
    let waiting = runtime.execute(
        &outer,
        "return await echo({value: 'held'});",
        &tools,
        host.clone(),
        control.clone(),
    );
    let independent = async {
        ready
            .await
            .map_err(|_| rejected("host admission dropped"))?;
        let value = runtime
            .execute(
                &outer,
                "return 42;",
                &[],
                Arc::new(Echo::default()),
                control,
            )
            .await?;
        assert_eq!(value, json!(42));
        release
            .send(())
            .map_err(|_| rejected("host owner dropped"))?;
        Ok::<_, Error>(())
    };
    let (waiting, independent) = tokio::join!(waiting, independent);
    independent?;
    assert_eq!(waiting?, json!("released"));
    // Negative: dropping the original execution producer is an error, not a hang.
    let (owner, control) = original_control();
    drop(owner);
    let failed = runtime
        .execute(
            &outer,
            "return await new Promise(() => {});",
            &[],
            host,
            control,
        )
        .await;
    assert!(failed.is_err());
    Ok(())
}

#[test]
fn declarations_preserve_structural_and_recursive_schema_types() -> Result<()> {
    let mut tool = echo_definition();
    tool.input_schema = json!({"$ref":"#/$defs/Node","$defs":{"Node":{"type":"object","properties":{"next":{"anyOf":[{"$ref":"#/$defs/Node"},{"type":"null"}]}},"additionalProperties":false}}});
    let types = declarations(&[tool])?;
    assert!(types.contains("Tool0InputDef1"));
    assert!(types.contains("(Tool0InputDef1 | null)"));
    assert!(types.contains("declare const echo: typeof tools[\"echo\"];"));
    Ok(())
}

struct ReverseCompletion(tokio::sync::watch::Sender<bool>);
impl CodeHost for ReverseCompletion {
    fn dispatch(
        &self,
        invocation: ToolInvocation,
    ) -> BoxProviderFuture<'_, Result<CodeHostResult>> {
        Box::pin(async move {
            if invocation.arguments["value"] == "first" {
                let mut completed = self.0.subscribe();
                if !*completed.borrow() {
                    completed
                        .changed()
                        .await
                        .map_err(|_| rejected("second call producer dropped"))?;
                }
            } else {
                self.0.send_replace(true);
            }
            Ok(ToolResult {
                value: invocation.arguments,
            }
            .into())
        })
    }
}

#[tokio::test]
async fn concurrent_host_completion_order_cannot_change_replay_continuations() -> Result<()> {
    let runtime = CodeRuntime::new(Arc::new(Memory))?;
    let (_owner, control) = original_control();
    let (second, _) = tokio::sync::watch::channel(false);
    let host = Arc::new(ReverseCompletion(second));
    let result = runtime
        .execute(
            &outer(),
            "return await Promise.race([echo({value:'first'}), echo({value:'second'})]);",
            &[echo_definition()],
            host,
            control,
        )
        .await?;
    // The second effect completed first; original issue order owns JS results.
    assert_eq!(result, json!({"value":"first"}));
    Ok(())
}

struct Uncertain;
impl CodeHost for Uncertain {
    fn dispatch(&self, _: ToolInvocation) -> BoxProviderFuture<'_, Result<CodeHostResult>> {
        Box::pin(async { Err(Error::Storage("original unacknowledged request".into())) })
    }
}

#[tokio::test]
async fn javascript_cannot_catch_and_acknowledge_uncertain_host_custody() -> Result<()> {
    let runtime = CodeRuntime::new(Arc::new(Memory))?;
    let (_owner, control) = original_control();
    let result = runtime
        .execute(
            &outer(),
            "try { return await echo({value:'request'}); } catch (_) { return 'success'; }",
            &[echo_definition()],
            Arc::new(Uncertain),
            control,
        )
        .await;
    assert!(
        matches!(result, Err(Error::Storage(message)) if message == "original unacknowledged request")
    );
    Ok(())
}

proptest::proptest! {
    #[test]
    fn host_call_json_boundary_preserves_arbitrary_strings(text in ".*") {
        let executor = tokio::runtime::Builder::new_current_thread().build().unwrap();
        executor.block_on(async {
            let runtime = CodeRuntime::new(Arc::new(Memory)).unwrap();
            let (_owner, control) = original_control();
            let host = Arc::new(Echo::default());
            let literal = serde_json::to_string(&text).unwrap();
            let result = runtime.execute(&outer(),
                &format!("return await echo({{value:{literal}}});"),
                &[echo_definition()], host.clone(), control).await.unwrap();
            assert_eq!(result, json!({"value":text}));
            let calls = lock(&host.0);
            assert_eq!(calls.len(), 1);
            assert_eq!(calls[0].arguments, result);
        });
    }
}

#[tokio::test]
async fn credited_compiler_realm_is_private_and_recovers_after_parse_failure() -> Result<()> {
    let runtime = CodeRuntime::new(Arc::new(Memory))?;
    let (_owner, control) = original_control();
    let host = Arc::new(Echo::default());
    let tools = [echo_definition()];
    assert!(
        runtime
            .execute(
                &outer(),
                "const invalid: = await echo({value:'unreachable'});",
                &tools,
                host.clone(),
                control.clone()
            )
            .await
            .is_err()
    );
    assert!(lock(&host.0).is_empty());
    assert_eq!(
        runtime
            .execute(
                &outer(),
                r#"
        type Message = {value: string};
        const message: Message = {value: "after parse failure"};
        return {result: await echo(message),
            compiler: typeof __harness_typescript, prepare: typeof __prepare};
    "#,
                &tools,
                host.clone(),
                control
            )
            .await?,
        json!({
            "result":{"value":"after parse failure"},
            "compiler":"undefined", "prepare":"undefined"
        })
    );
    assert_eq!(lock(&host.0).len(), 1);
    Ok(())
}
