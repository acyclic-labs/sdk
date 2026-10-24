use super::*;
use futures::{FutureExt as _, task::AtomicWaker};
use std::sync::atomic::AtomicU64;

#[derive(Default)]
struct Gates {
    started: AtomicU64,
    released: AtomicU64,
    completed: AtomicU64,
    abandoned: AtomicUsize,
    invocations: AtomicUsize,
    wake: AtomicWaker,
}

impl Gates {
    fn release(&self, mask: u64) {
        self.released.fetch_or(mask, Ordering::SeqCst);
        self.wake.wake();
    }
}

struct GateTool {
    gates: Arc<Gates>,
    read: bool,
}

struct InFlight<'a> {
    gates: &'a Gates,
    mask: u64,
}
impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        if self.gates.completed.load(Ordering::SeqCst) & self.mask == 0 {
            self.gates.abandoned.fetch_add(1, Ordering::SeqCst);
        }
    }
}

impl crate::tool::ToolExecutor for GateTool {
    fn execution_class(&self) -> crate::tool::ToolExecutionClass {
        if self.read {
            crate::tool::ToolExecutionClass::Read
        } else {
            crate::tool::ToolExecutionClass::Exclusive
        }
    }

    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        _: Option<crate::runtime::ToolContext>,
    ) -> BoxFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            let mask = 1 << invocation.arguments["bit"].as_u64().unwrap();
            self.gates.started.fetch_or(mask, Ordering::SeqCst);
            self.gates.invocations.fetch_add(1, Ordering::SeqCst);
            let _in_flight = InFlight {
                gates: &self.gates,
                mask,
            };
            futures::future::poll_fn(|cx| {
                self.gates.wake.register(cx.waker());
                if self.gates.released.load(Ordering::SeqCst) & mask != 0 {
                    self.gates.completed.fetch_or(mask, Ordering::SeqCst);
                    std::task::Poll::Ready(Ok(ToolResult {
                        value: invocation.arguments.clone(),
                    }))
                } else {
                    std::task::Poll::Pending
                }
            })
            .await
        })
    }

    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<crate::runtime::ToolContext>,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async { Ok(None) })
    }
}

fn call(bit: u64, write: bool) -> ScriptedEvent {
    ScriptedEvent::Event(ModelEvent::ToolCall(crate::model::ToolCall::new(
        format!("call-{bit}"),
        if write {
            "example.write"
        } else {
            "example.echo"
        }
        .into(),
        json!({"bit":bit}),
    )))
}

fn fixture(
    gates: &Arc<Gates>,
    events: Vec<ScriptedEvent>,
) -> Result<(StockExecutor, Arc<ScriptedModelProvider>)> {
    let schema = json!({"type":"object"});
    let mut tools = echo_tools(
        Arc::new(GateTool {
            gates: gates.clone(),
            read: true,
        }),
        schema.clone(),
        schema.clone(),
        crate::tool::json_projection_schema(schema),
    )?;
    let mut write = tools.get("example.echo", None, None)?.unwrap().clone();
    write.definition.name = "example.write".into();
    write.executor = Arc::new(GateTool {
        gates: gates.clone(),
        read: false,
    });
    tools.register((write, None), &mut None, None)?;
    let provider = Arc::new(
        ScriptedModelProvider::new([
            ScriptedTurn::Events(events),
            ScriptedTurn::Text("done".into()),
        ])
        .with_token_counter(fixture_token_count),
    );
    let executor = uncompacted_executor(
        Model::new("test".into(), "model".into(), "1".into(), Value::Null)?,
        provider.clone(),
        ContextPipeline::default(),
        tools,
    )
    .with_tool_authority(
        RuntimeScope::new(
            Capabilities::new(["tool:call:example.echo", "tool:call:example.write"]),
            Limits::default(),
        )?,
        None,
    )?;
    Ok((executor, provider))
}

fn input() -> TurnInput {
    TurnInput {
        operation_id: OperationId::new(),
        input: ModelContent::Text("work".into()),
        selected_context: None,
        max_steps: 2,
    }
}

#[tokio::test]
async fn complete_reads_start_in_parallel_before_the_stream_finishes() -> Result<()> {
    let gates = Arc::new(Gates::default());
    let (executor, _) = fixture(
        &gates,
        vec![call(0, false), call(1, false), ScriptedEvent::Pending {}],
    )?;
    let journal = Journal::default();
    let mut execution = Box::pin(executor.execute(input(), &journal));
    assert!(execution.as_mut().now_or_never().is_none());
    assert_eq!(
        gates.started.load(Ordering::SeqCst),
        3,
        "both complete reads must start while the model stream is pending"
    );
    gates.release(3);
    assert!(execution.as_mut().now_or_never().is_none());
    assert_eq!(gates.completed.load(Ordering::SeqCst), 3);
    drop(execution);
    assert_eq!(gates.abandoned.load(Ordering::SeqCst), 0);
    assert_eq!(gates.invocations.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn writes_barrier_read_groups_and_results_keep_provider_order() -> Result<()> {
    let gates = Arc::new(Gates::default());
    let (executor, provider) = fixture(
        &gates,
        vec![
            call(0, false),
            call(1, false),
            call(2, true),
            call(3, false),
            call(4, false),
            ScriptedEvent::Event(ModelEvent::Completed {
                metadata: Value::Null,
            }),
        ],
    )?;
    let journal = Journal::default();
    let mut execution = Box::pin(executor.execute(input(), &journal));
    assert!(execution.as_mut().now_or_never().is_none());
    assert_eq!(gates.started.load(Ordering::SeqCst), 3);
    gates.release(2); // The second read finishes first.
    assert!(execution.as_mut().now_or_never().is_none());
    assert_eq!(gates.started.load(Ordering::SeqCst), 3);
    gates.release(1);
    assert!(execution.as_mut().now_or_never().is_none());
    assert_eq!(
        gates.started.load(Ordering::SeqCst),
        7,
        "write starts only after both reads settle"
    );
    gates.release(4);
    assert!(execution.as_mut().now_or_never().is_none());
    assert_eq!(
        gates.started.load(Ordering::SeqCst),
        31,
        "later reads start together after the write"
    );
    gates.release(24);
    assert_eq!(execution.await?.text, "done");
    let requests = provider.requests();
    let results: Vec<_> = requests[1]
        .messages
        .iter()
        .filter_map(|message| match &message.content {
            ModelContent::Part(ModelContentPart::ToolResult { call_id, .. }) => {
                Some(call_id.as_str())
            }
            _ => None,
        })
        .collect();
    assert_eq!(results, ["call-0", "call-1", "call-2", "call-3", "call-4"]);
    assert_eq!(gates.abandoned.load(Ordering::SeqCst), 0);
    assert_eq!(gates.invocations.load(Ordering::SeqCst), 5);
    Ok(())
}

#[tokio::test]
async fn transport_failure_drains_started_calls_before_returning() -> Result<()> {
    let gates = Arc::new(Gates::default());
    let (executor, _) = fixture(&gates, vec![call(0, false)])?;
    // EOF without Completed is a transport failure at the stock boundary.
    let journal = Journal::default();
    let mut execution = Box::pin(executor.execute(input(), &journal));
    assert!(execution.as_mut().now_or_never().is_none());
    assert_eq!(gates.started.load(Ordering::SeqCst), 1);
    assert_eq!(gates.abandoned.load(Ordering::SeqCst), 0);
    gates.release(1);
    assert!(execution.await.is_err());
    assert_eq!(gates.completed.load(Ordering::SeqCst), 1);
    assert_eq!(gates.abandoned.load(Ordering::SeqCst), 0);
    Ok(())
}

/// The fixture deliberately suspends the real tail-CAS boundary. No timing or
/// scheduler luck is needed to expose interleaving with streamed model records.
#[derive(Default)]
struct ControlledJournal {
    inner: Journal,
    cancellation_follows: AtomicUsize,
    cancelled: std::sync::atomic::AtomicBool,
    cancellation_error: bool,
    wake: AtomicWaker,
    suspend_cas: bool,
    cancel_after_tool_record: bool,
}

impl ExecutionJournal for ControlledJournal {
    fn claim_execution<'a>(
        &'a self,
        operation: OperationId,
        io: Option<&'a PreparationIo>,
    ) -> Result<ExecutionClaim<'a>> {
        self.inner.claim_execution(operation, io)
    }
    fn model_cancellation(&self) -> BoxFuture<'_, Result<()>> {
        self.cancellation_follows.fetch_add(1, Ordering::SeqCst);
        Box::pin(futures::future::poll_fn(|cx| {
            self.wake.register(cx.waker());
            if self.cancelled.load(Ordering::SeqCst) {
                std::task::Poll::Ready(if self.cancellation_error {
                    Err(Error::Invalid("cancellation follow failed".into()))
                } else {
                    Ok(())
                })
            } else {
                std::task::Poll::Pending
            }
        }))
    }
    fn replay<'a>(
        &'a self,
        operation: OperationId,
        after: u64,
        maximum: u32,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Vec<ExecutionRecord>>> {
        self.inner.replay(operation, after, maximum, io)
    }
    fn append<'a>(
        &'a self,
        operation: OperationId,
        key: &'a str,
        event: &'a ExecutionEvent,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            self.inner.append(operation, key, event, io)?.await?;
            if self.cancel_after_tool_record && key == "model:0:0" {
                self.cancelled.store(true, Ordering::SeqCst);
                self.wake.wake();
            }
            Ok(())
        })
    }
    fn append_if_tail<'a>(
        &'a self,
        operation: OperationId,
        tail: u64,
        key: &'a str,
        event: &'a ExecutionEvent,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, bool>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            if self.suspend_cas {
                tokio::task::yield_now().await;
            }
            self.inner
                .append_if_tail(operation, tail, key, event, io)?
                .await
        })
    }
    fn stage<'a>(
        &'a self,
        operation: OperationId,
        key: &'a str,
        bytes: &'a [u8],
        media: &'static str,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, FileRef>> {
        self.inner.stage(operation, key, bytes, media, io)
    }
    fn load<'a>(
        &'a self,
        file: &'a FileRef,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        self.inner.load(file, io)
    }
    fn verify_input_file<'a>(
        &'a self,
        file: &'a FileRef,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        self.inner.verify_input_file(file, io)
    }
    fn open_interaction<'a>(
        &'a self,
        id: InteractionId,
        interaction: &'a Interaction,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        self.inner.open_interaction(id, interaction, io)
    }
    fn interaction_outcome<'a>(
        &'a self,
        id: InteractionId,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Option<InteractionOutcome>>> {
        self.inner.interaction_outcome(id, io)
    }
}

#[tokio::test]
async fn one_cancellation_follow_owns_preparation_stream_and_drain() -> Result<()> {
    for entry in 0..3 {
        for cancellation_error in [false, true] {
            let gates = Arc::new(Gates::default());
            let (executor, _) = fixture(&gates, vec![call(0, false), ScriptedEvent::Pending {}])?;
            let journal = ControlledJournal {
                cancellation_error,
                ..Default::default()
            };
            let input = input();
            let mut execution = std::pin::pin!(async {
                match entry {
                    0 => {
                        executor.execute(input.clone(), &journal).await?;
                    }
                    1 => {
                        executor.execute_progress(input.clone(), &journal).await?;
                    }
                    _ => {
                        executor.model_step(&journal, &input, 0, &[]).await?;
                    }
                }
                Ok::<(), Error>(())
            });
            assert!(execution.as_mut().now_or_never().is_none());
            let started = u64::from(entry != 2);
            assert_eq!(gates.started.load(Ordering::SeqCst), started);
            assert_eq!(
                journal.cancellation_follows.load(Ordering::SeqCst),
                1,
                "entry {entry} must share the original cancellation follow"
            );
            journal.cancelled.store(true, Ordering::SeqCst);
            journal.wake.wake();
            let settled = execution.as_mut().now_or_never();
            if started != 0 {
                assert!(settled.is_none());
            }
            assert_eq!(gates.abandoned.load(Ordering::SeqCst), 0);
            gates.release(1);
            let error = match settled {
                Some(result) => result,
                None => execution.await,
            }
            .unwrap_err();
            if cancellation_error {
                assert!(
                    matches!(error, Error::Invalid(ref text) if text == "cancellation follow failed")
                );
            } else {
                assert!(matches!(
                    error,
                    Error::InteractionRejected(crate::InteractionRejection::Cancelled)
                ));
            }
            assert_eq!(journal.cancellation_follows.load(Ordering::SeqCst), 1);
            assert_eq!(gates.completed.load(Ordering::SeqCst), started);
            assert_eq!(gates.abandoned.load(Ordering::SeqCst), 0);
        }
    }
    Ok(())
}

#[tokio::test]
async fn cancellation_drains_started_reads_and_writes_before_returning() -> Result<()> {
    for write in [false, true] {
        let gates = Arc::new(Gates::default());
        let (executor, _) = fixture(&gates, vec![call(0, write), ScriptedEvent::Pending {}])?;
        let journal = ControlledJournal::default();
        let mut execution = Box::pin(executor.execute(input(), &journal));
        assert!(execution.as_mut().now_or_never().is_none());
        assert_eq!(gates.started.load(Ordering::SeqCst), 1);
        journal.cancelled.store(true, Ordering::SeqCst);
        journal.wake.wake();
        assert!(
            execution.as_mut().now_or_never().is_none(),
            "cancellation must retain the started effect until settlement"
        );
        assert_eq!(gates.abandoned.load(Ordering::SeqCst), 0);
        gates.release(1);
        assert!(matches!(
            execution.await,
            Err(Error::InteractionRejected(
                crate::InteractionRejection::Cancelled
            ))
        ));
        assert_eq!(gates.completed.load(Ordering::SeqCst), 1);
        assert_eq!(gates.abandoned.load(Ordering::SeqCst), 0);
    }
    Ok(())
}

#[tokio::test]
async fn journal_transactions_exclude_stream_and_tool_tail_races() -> Result<()> {
    let gates = Arc::new(Gates::default());
    gates.release(u64::MAX);
    let mut events: Vec<_> = (0..32).map(|bit| call(bit, false)).collect();
    events.push(ScriptedEvent::Event(ModelEvent::Completed {
        metadata: Value::Null,
    }));
    let (executor, _) = fixture(&gates, events)?;
    let journal = ControlledJournal {
        suspend_cas: true,
        ..Default::default()
    };
    assert_eq!(executor.execute(input(), &journal).await?.text, "done");
    assert_eq!(gates.completed.load(Ordering::SeqCst), (1u64 << 32) - 1);
    assert_eq!(gates.invocations.load(Ordering::SeqCst), 32);
    Ok(())
}

#[tokio::test]
async fn cancellation_never_starts_calls_queued_behind_a_barrier() -> Result<()> {
    for completed in [false, true] {
        let gates = Arc::new(Gates::default());
        let ending = if completed {
            ScriptedEvent::Event(ModelEvent::Completed {
                metadata: Value::Null,
            })
        } else {
            ScriptedEvent::Pending {}
        };
        let (executor, _) = fixture(
            &gates,
            vec![call(0, false), call(1, true), call(2, false), ending],
        )?;
        let journal = ControlledJournal::default();
        let mut execution = Box::pin(executor.execute(input(), &journal));
        assert!(execution.as_mut().now_or_never().is_none());
        assert_eq!(gates.started.load(Ordering::SeqCst), 1);
        journal.cancelled.store(true, Ordering::SeqCst);
        journal.wake.wake();
        assert!(execution.as_mut().now_or_never().is_none());
        gates.release(1);
        assert!(matches!(
            execution.await,
            Err(Error::InteractionRejected(
                crate::InteractionRejection::Cancelled
            ))
        ));
        assert_eq!(gates.started.load(Ordering::SeqCst), 1);
        assert_eq!(gates.completed.load(Ordering::SeqCst), 1);
        assert_eq!(gates.abandoned.load(Ordering::SeqCst), 0);
        assert_eq!(gates.invocations.load(Ordering::SeqCst), 1);
    }
    Ok(())
}

#[tokio::test]
async fn cancellation_at_admission_never_starts_the_new_call() -> Result<()> {
    let gates = Arc::new(Gates::default());
    let (executor, _) = fixture(&gates, vec![call(0, false), ScriptedEvent::Pending {}])?;
    let journal = ControlledJournal {
        cancel_after_tool_record: true,
        ..Default::default()
    };
    let mut execution = Box::pin(executor.execute(input(), &journal));
    let initial = execution.as_mut().now_or_never();
    assert!(
        journal.cancelled.load(Ordering::SeqCst),
        "the actual model append reached the cancellation boundary"
    );
    assert_eq!(
        gates.started.load(Ordering::SeqCst),
        0,
        "a cancellation observed at admission must precede the first tool poll"
    );
    let outcome = match initial {
        Some(result) => result,
        None => execution.await,
    };
    assert!(matches!(
        outcome,
        Err(Error::InteractionRejected(
            crate::InteractionRejection::Cancelled
        ))
    ));
    assert_eq!(gates.invocations.load(Ordering::SeqCst), 0);
    assert_eq!(gates.abandoned.load(Ordering::SeqCst), 0);
    Ok(())
}
