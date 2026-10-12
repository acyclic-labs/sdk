use super::*;
use crate::tool::{ToolDefinition, ToolExecutor, ToolInvocation, ToolProjection, ToolResult};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use futures::FutureExt as _;
use serde_json::{Value, json};

struct UnusedForkPreparer(crate::core::Authority, u64);

impl crate::fork::ForkPreparer for UnusedForkPreparer {
    fn parent_snapshot(&self) -> (&crate::core::Authority, u64) {
        (&self.0, self.1)
    }

    fn prepare<'a>(
        &'a self,
        _: crate::fork::ForkRequest,
    ) -> BoxFuture<'a, Result<crate::fork::ForkReport>> {
        async { Err(Error::Unsupported("unused test preparer".into())) }.boxed()
    }

    fn reconcile<'a>(
        &'a self,
        _: crate::fork::ForkRequest,
    ) -> BoxFuture<'a, Result<Option<crate::fork::ForkReport>>> {
        async { Ok(None) }.boxed()
    }
}

struct DelegatingLoop;

impl AgentLoop for DelegatingLoop {
    fn run(
        &self,
        context: crate::runtime::TaskContext,
        input: AgentInput,
    ) -> BoxFuture<'static, Result<crate::agent_loop::AgentOutput>> {
        Box::pin(async move {
            let definition = context.task::<String, String>("example.echo")?;
            let child = context.spawn(&definition, input.prompt).await?;
            let text = match child.result().await? {
                crate::Outcome::Succeeded(text) => text,
                crate::Outcome::Failed { message } => return Err(Error::Conflict(message)),
                crate::Outcome::Cancelled => {
                    return Err(Error::Conflict("child cancelled".into()));
                }
                crate::Outcome::Indeterminate { operation_id } => {
                    return Err(Error::Indeterminate(operation_id));
                }
            };
            Ok(crate::agent_loop::AgentOutput::text(text))
        })
    }
}

#[tokio::test]
async fn custom_agent_loop_uses_public_task_context_without_turn_journal() -> Result<()> {
    let bundle = HarnessBuilder::new()
        .agent_loop(Arc::new(DelegatingLoop))
        .grant("task:spawn:example.echo@1")
        .task(TaskDefinition::live(
            "example.echo",
            "1",
            |_context, value: String| async move { Ok(value) },
        )?)?
        .build()?;
    assert!(bundle.executor().is_none());
    let result = bundle.run_agent(AgentInput::text("hello")).await?;
    assert!(matches!(result.outcome, crate::Outcome::Succeeded(output) if output.text == "hello"));
    assert!(!result.task_id.is_empty());
    Ok(())
}

#[test]
fn fork_provider_requires_a_root_grant_before_runtime_construction() -> Result<()> {
    let preparer = || {
        Arc::new(UnusedForkPreparer(
            crate::core::Authority {
                kind: crate::core::AggregateKind::Conversation,
                id: "parent".into(),
            },
            1,
        ))
    };
    assert!(matches!(
        HarnessBuilder::new()
            .agent_loop(Arc::new(DelegatingLoop))
            .fork_preparer(preparer())
            .build(),
        Err(Error::Unauthorized(_))
    ));
    let bound = HarnessBuilder::new()
        .agent_loop(Arc::new(DelegatingLoop))
        .fork_preparer(preparer())
        .grant("fork:publish")
        .build()?;
    assert!(bound.capabilities().contains("fork:publish"));
    Ok(())
}

#[tokio::test]
async fn fork_binding_cannot_capture_a_different_parent_revision() -> Result<()> {
    let bundle = HarnessBuilder::new()
        .agent_loop(Arc::new(DelegatingLoop))
        .fork_preparer(Arc::new(UnusedForkPreparer(
            crate::core::Authority {
                kind: crate::core::AggregateKind::Conversation,
                id: "parent".into(),
            },
            1,
        )))
        .grant("fork:publish")
        .build()?;
    let request: crate::fork::ForkRequest =
        serde_json::from_str(include_str!("../../fixtures/v2/fork-request.json"))
            .map_err(|error: serde_json::Error| Error::Invalid(error.to_string()))?;
    assert!(matches!(
        bundle.runtime().prepare_fork(request).await,
        Err(Error::Conflict(_))
    ));
    let parent = crate::core::Authority {
        kind: crate::core::AggregateKind::Conversation,
        id: "parent".into(),
    };
    assert!(matches!(
        bundle.at_parent_snapshot(Arc::new(UnusedForkPreparer(parent.clone(), 1))),
        Err(Error::Conflict(_))
    ));
    assert!(matches!(
        bundle.at_parent_snapshot(Arc::new(UnusedForkPreparer(
            crate::core::Authority {
                kind: crate::core::AggregateKind::Conversation,
                id: "other".into(),
            },
            2
        ))),
        Err(Error::Unauthorized(_))
    ));
    let advanced = bundle.at_parent_snapshot(Arc::new(UnusedForkPreparer(parent, 2)))?;
    assert!(advanced.capabilities().contains("fork:publish"));
    Ok(())
}

#[derive(Default)]
struct RecordingTurnJournal {
    replays: std::sync::atomic::AtomicUsize,
    events: std::sync::Mutex<Vec<crate::executor::ExecutionEvent>>,
}

impl ExecutionJournal for RecordingTurnJournal {
    fn replay<'a>(
        &'a self,
        _: crate::OperationId,
        _: u64,
        _: u32,
        io: Option<&'a crate::conversation::ContentIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Vec<crate::executor::ExecutionRecord>>> {
        if io.is_some() {
            return Err(crate::Error::Unsupported("finite test journal IO is unavailable".into()));
        }
        self.replays
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        crate::conversation::BoundedContentFuture::new(io, || async { Ok(Vec::new()) })
    }

    fn append<'a>(
        &'a self,
        _: crate::OperationId,
        _: &'a str,
        event: &'a crate::executor::ExecutionEvent,
        io: Option<&'a crate::conversation::ContentIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        if io.is_some() { return Err(crate::Error::Unsupported("finite test journal append is unavailable".into())); }
        crate::conversation::BoundedContentFuture::new(io, || async move {
            self.events
                .lock()
                .map_err(|_| Error::Storage("journal lock poisoned".into()))?
                .push(event.clone());
            Err(Error::Unsupported(
                "stock journal dispatch forbidden".into(),
            ))
        })
    }

    fn stage<'a>(
        &'a self,
        operation: crate::OperationId,
        key: &'a str,
        bytes: &'a [u8],
        media_type: &'static str,
        io: Option<&'a crate::conversation::ContentIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, crate::conversation::FileRef>> {
        crate::conversation::BoundedContentFuture::new(io, || async { Err(Error::Unsupported("unused test journal".into())) })
    }

    fn load<'a>(
        &'a self,
        file: &'a crate::conversation::FileRef,
        io: Option<&'a crate::conversation::ContentIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        if io.is_some() {
            return Err(crate::Error::Unsupported("finite test journal IO is unavailable".into()));
        }
        crate::conversation::BoundedContentFuture::new(io, || async { Err(Error::Unsupported("unused test journal".into())) })
    }

    fn verify_input_file<'a>(
        &'a self,
        _: &'a crate::conversation::FileRef,
        io: Option<&'a crate::conversation::ContentIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(io, || async { Ok(()) })
    }

    fn verify_selected_context<'a>(
        &'a self,
        _: crate::OperationId,
        _: &'a crate::projection::SelectedModelContext,
        io: Option<&'a crate::conversation::ContentIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(io, || async { Ok(()) })
    }

    fn open_interaction<'a>(
        &'a self,
        id: crate::InteractionId,
        interaction: crate::interaction::Interaction,
    ) -> BoxFuture<'a, Result<()>> {
        async { Err(Error::Unsupported("unused test journal".into())) }.boxed()
    }

    fn interaction_outcome<'a>(
        &'a self,
        id: crate::InteractionId,
    ) -> BoxFuture<'a, Result<Option<crate::interaction::InteractionOutcome>>> {
        async { Err(Error::Unsupported("unused test journal".into())) }.boxed()
    }
}

struct RecordingExecutor {
    expected: Vec<TurnInput>,
    outputs: Vec<TurnOutput>,
    received: std::sync::Mutex<Vec<TurnInput>>,
}

impl Executor for RecordingExecutor {
    fn execute<'a>(
        &'a self,
        input: TurnInput,
        _: &'a dyn ExecutionJournal,
    ) -> BoxFuture<'a, Result<TurnOutput>> {
        async move {
            let mut received = self
                .received
                .lock()
                .map_err(|_| Error::Storage("executor lock poisoned".into()))?;
            let index = received.len();
            assert_eq!(input, self.expected[index], "complete executor input");
            received.push(input);
            Ok(self.outputs[index].clone())
        }
        .boxed()
    }
}

#[derive(Default)]
struct ForbiddenStockModel(std::sync::atomic::AtomicUsize);

impl ModelProvider for ForbiddenStockModel {
    fn generate<'a>(
        &'a self,
        _: crate::model::PreparedModelRequest,
        _: crate::model::ModelDispatch,
    ) -> acyclic_stream::BoxProviderStream<'a, Result<crate::model::ModelEvent>> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::pin(futures::stream::once(async {
            Err(Error::Unsupported("stock model dispatch forbidden".into()))
        }))
    }

    fn reconcile<'a>(
        &'a self,
        _dispatch: crate::model::ModelDispatch,
        _observed: &'a [crate::model::ModelEvent],
    ) -> BoxFuture<'a, Result<Option<Vec<crate::model::ModelEvent>>>> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        async {
            Err(Error::Unsupported(
                "stock model reconciliation forbidden".into(),
            ))
        }
        .boxed()
    }
}

#[derive(Default)]
struct ForbiddenStockTools(std::sync::atomic::AtomicUsize);

impl ToolExecutor for ForbiddenStockTools {
    fn execute<'a>(&'a self, _: ToolInvocation) -> BoxFuture<'a, Result<ToolResult>> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        async { Err(Error::Unsupported("stock tool dispatch forbidden".into())) }.boxed()
    }
    fn reconcile<'a>(&'a self, _: ToolInvocation) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        async {
            Err(Error::Unsupported(
                "stock tool reconciliation forbidden".into(),
            ))
        }
        .boxed()
    }
}
impl ToolProjection for ForbiddenStockTools {
    fn project(
        &self,
        _: &ToolInvocation,
        _: &ToolResult,
        work: crate::NativeWork<'_>,
    ) -> Result<(Value, crate::NativeCustody)> {
        self.supports_native_projection(work)?;
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Err(Error::Unsupported("stock tool projection forbidden".into()))
    }
}

#[tokio::test]
async fn custom_executor_owns_complete_turn_input_output_without_stock_events() -> Result<()> {
    use crate::conversation::{
        Attachment, FileDescriptor, FileRef, VolumeClass, VolumeOwner, VolumeRef,
    };
    use crate::model::{
        FileProjectionPolicy, ModelContent, ModelContentPart, ModelMessage, ModelRole,
    };
    use crate::projection::{ModelContextSelection, SelectedModelContext};
    use crate::resources::ProviderRef;
    use std::sync::atomic::Ordering;

    let file = FileRef::new(
        VolumeRef::new(
            ProviderRef::new("example".into(), "filesystem".into(), "1".into(), &mut None)?,
            "custom-turn".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(crate::AgentId::from_bytes(1_u128.to_be_bytes())),
            &mut None,
        )?,
        "artifact.txt".into(),
        "version-1".into(),
        FileDescriptor::from_bytes(b"custom artifact", "text/plain", &mut None)?,
        "artifact.txt".into(),
        &mut None,
    )?;
    let inputs = (1_u32..=2)
        .map(|index| {
            let content = ModelContent::Parts(vec![
                ModelContentPart::Text {
                    text: format!("custom input {index}"),
                },
                ModelContentPart::File {
                    file: file.clone(),
                    policy: FileProjectionPolicy::Reference,
                },
            ]);
            TurnInput::from_selected_context(
                crate::OperationId::from_bytes((10 + u128::from(index)).to_be_bytes()),
                SelectedModelContext {
                    selection: ModelContextSelection {
                        checkpoint: (index == 1).then(|| file.clone()),
                        conversation_revision: 20 + u64::from(index),
                        message_ids: vec![
                            uuid::Uuid::from_u128(30 + u128::from(index)),
                            uuid::Uuid::from_u128(40 + u128::from(index)),
                        ],
                    },
                    messages: vec![
                        ModelMessage {
                            role: ModelRole::System,
                            content: ModelContent::Text(format!("custom context {index}")),
                        },
                        ModelMessage {
                            role: ModelRole::User,
                            content,
                        },
                    ],
                },
                index,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let outputs = (1..=2)
            .map(|index| TurnOutput {
                text: format!("custom output {index}"),
                attachments: vec![Attachment { file: file.clone(), label: Some(format!("custom label {index}")) }],
                metadata: json!({"executor": "custom", "invocation": index, "nested": {"complete": true}}),
                steps: index,
            })
            .collect::<Vec<_>>();
    let executor = Arc::new(RecordingExecutor {
        expected: inputs.clone(),
        outputs: outputs.clone(),
        received: std::sync::Mutex::new(Vec::new()),
    });
    let journal = Arc::new(RecordingTurnJournal::default());
    let model = Arc::new(ForbiddenStockModel::default());
    let tools = Arc::new(ForbiddenStockTools::default());
    let bundle = HarnessBuilder::new()
        .model(
            Model::new("example".into(), "stock".into(), "1".into(), Value::Null)?,
            model.clone(),
        )
        .tool(Tool {
            definition: example_tool().definition,
            executor: tools.clone(),
            projection: tools.clone(),
        })?
        .grant("model:generate")
        .journal(journal.clone())
        .executor(executor.clone())
        .build()?;

    for (input, output) in inputs.iter().zip(&outputs) {
        assert_eq!(
            bundle.run(input.clone()).await?,
            *output,
            "complete executor output"
        );
    }
    assert_eq!(
        *executor
            .received
            .lock()
            .map_err(|_| Error::Storage("executor lock poisoned".into()))?,
        inputs
    );
    assert_eq!(journal.replays.load(Ordering::SeqCst), 0);
    assert!(
        journal
            .events
            .lock()
            .map_err(|_| Error::Storage("journal lock poisoned".into()))?
            .is_empty()
    );
    assert_eq!(model.0.load(Ordering::SeqCst), 0);
    assert_eq!(tools.0.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn builder_rejects_missing_or_conflicting_bindings() -> Result<()> {
    assert!(HarnessBuilder::new().build().is_err());
    let model = Model::new("example".into(), "model".into(), "1".into(), Value::Null)?;
    struct Provider;
    impl ModelProvider for Provider {
        fn generate<'a>(
            &'a self,
            _: crate::model::PreparedModelRequest,
            _dispatch: crate::model::ModelDispatch,
        ) -> acyclic_stream::BoxProviderStream<'a, Result<crate::model::ModelEvent>> {
            Box::pin(futures::stream::empty())
        }
    }
    let bundle = HarnessBuilder::new()
        .name("test")
        .model(model.clone(), Arc::new(Provider))
        .journal(Arc::new(RecordingTurnJournal::default()))
        .grant("model:generate")
        .build()?;
    assert_eq!(bundle.name, "test");
    assert!(bundle.capabilities.contains("model:generate"));
    assert!(
        bundle
            .runtime()
            .task::<TurnInput, TurnOutput>("acyclic.stock_turn@2")
            .is_ok()
    );
    assert!(matches!(bundle.run(TurnInput {
            operation_id: crate::OperationId::new(),
            input: crate::model::ModelContent::Text("typed failure".into()),
            selected_context: None,
            max_steps: 1,
        }).await, Err(Error::Unsupported(message)) if message == "selected model context capacity is unavailable"));
    assert!(matches!(HarnessBuilder::new()
            .model(model.clone(), Arc::new(Provider))
            .journal(Arc::new(RecordingTurnJournal::default()))
            .grant("model:generate")
            .task(TaskDefinition::live("acyclic.stock_turn", "2", |_context, _input: TurnInput| async {
                Err::<TurnOutput, _>(Error::Unauthorized("caller task must not be replaced".into()))
            })?)?
            .build(), Err(Error::Conflict(message)) if message.contains("already registered")));

    let stock_executor = bundle
        .executor()
        .ok_or_else(|| Error::Storage("stock executor missing".into()))?;
    let authority = crate::core::Authority {
        kind: crate::core::AggregateKind::Agent,
        id: "builder-original-registry".into(),
    };
    let issuer = crate::core::AuthorityIssuer::new("builder-owner", [81; 32], authority.clone());
    let mut schemas = crate::core::SchemaRegistry::new();
    schemas.register(
        "example.builder",
        1,
        json!({"type":"object"}),
        [82; 32],
        crate::core::ExtensionForkPolicy::Inherit,
        [],
        None,
    )?;
    let agent = Reducer::new(
        authority,
        issuer.verifier(&mut None).expect("verifier copy"),
        schemas.clone(),
    );
    let inspect = TaskDefinition::live("example.registry", "1", move |context, (): ()| {
        let expected = schemas.clone();
        async move {
            let original = context
                .scope()
                .extension_schema_registry()
                .ok_or_else(|| Error::Storage("builder lost original registry".into()))?;
            Ok(original == &expected && context.scope().grants().contains("model:generate"))
        }
    })?;
    let captured = HarnessBuilder::new()
        .executor(Arc::clone(&stock_executor))
        .journal(Arc::new(RecordingTurnJournal::default()))
        .task(inspect)?
        .extensions_from(&agent)?
        .grant("model:generate")
        .grant("task:spawn:example.registry@1")
        .build()?;
    let inspect = captured.runtime().task::<(), bool>("example.registry")?;
    assert!(matches!(
        captured
            .runtime()
            .spawn(&inspect, ())
            .await?
            .result()
            .await?,
        crate::Outcome::Succeeded(true)
    ));
    assert!(captured.capabilities().contains("model:generate"));
    assert!(
        HarnessBuilder::new()
            .extensions_from(&agent)?
            .extensions_from(&agent)
            .is_err()
    );
    let mut pinned = Bindings::local();
    pinned.scope = RuntimeScope::new(Capabilities::new(["model:generate"]), Limits::default())?
        .with_extensions_from(&agent)?;
    assert!(
        HarnessBuilder::new()
            .bindings(pinned)
            .extensions_from(&agent)
            .is_err()
    );
    HarnessBuilder::new()
        .model(model, Arc::new(Provider))
        .journal(Arc::new(RecordingTurnJournal::default()))
        .executor(Arc::clone(&stock_executor))
        .build()?;
    let custom = HarnessBuilder::new()
        .executor(Arc::clone(&stock_executor))
        .journal(Arc::new(RecordingTurnJournal::default()))
        .tool(example_tool())?
        .build()?;
    assert!(matches!(
        custom
            .runtime()
            .task::<TurnInput, TurnOutput>("acyclic.stock_turn@2"),
        Err(Error::NotFound(_))
    ));
    assert_eq!(custom.runtime().tool("example.echo")?.name, "example.echo");
    let scoped_bindings = || -> Result<Bindings> {
        let mut bindings = Bindings::local();
        let root = RuntimeScope::new(
            Capabilities::new(["tool:call:example.echo".to_owned()]),
            Limits::default(),
        )?;
        bindings.scope = root.narrow(root.grants().clone(), root.limits())?;
        bindings
            .tools
            .register((example_tool(), None), &mut None, None)?;
        Ok(bindings)
    };
    let composed = HarnessBuilder::new()
        .bindings(scoped_bindings()?)
        .executor(Arc::clone(&stock_executor))
        .journal(Arc::new(RecordingTurnJournal::default()))
        .build()?;
    assert!(composed.capabilities().contains("tool:call:example.echo"));
    assert_eq!(
        composed.runtime().tool("example.echo")?.name,
        "example.echo"
    );
    assert!(
        HarnessBuilder::new()
            .bindings(scoped_bindings()?)
            .grant("tool:call:acyclic.shell")
            .executor(Arc::clone(&stock_executor))
            .journal(Arc::new(RecordingTurnJournal::default()))
            .build()
            .is_err()
    );
    assert!(
        HarnessBuilder::new()
            .bindings(Bindings::local())
            .grant("tool:call:acyclic.shell")
            .executor(Arc::clone(&stock_executor))
            .journal(Arc::new(RecordingTurnJournal::default()))
            .build()
            .is_err()
    );
    assert!(
        HarnessBuilder::new()
            .grant("tool:call:acyclic.shell")
            .executor(Arc::clone(&stock_executor))
            .journal(Arc::new(RecordingTurnJournal::default()))
            .build()?
            .capabilities()
            .contains("tool:call:acyclic.shell")
    );
    Ok(())
}

struct Echo;

impl ToolExecutor for Echo {
    fn execute<'a>(&'a self, invocation: ToolInvocation) -> BoxFuture<'a, Result<ToolResult>> {
        async move {
            Ok(ToolResult {
                value: invocation.arguments,
            })
        }
        .boxed()
    }

    fn reconcile<'a>(&'a self, _: ToolInvocation) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        async { Ok(None) }.boxed()
    }
}

struct EchoProjection;

impl ToolProjection for EchoProjection {
    fn project(
        &self,
        _: &ToolInvocation,
        result: &ToolResult,
        work: crate::NativeWork<'_>,
    ) -> Result<(Value, crate::NativeCustody)> {
        self.supports_native_projection(work)?;
        Ok((json!({"kind":"json","value":{"echo": result.value}}), None))
    }
}

fn example_tool() -> Tool {
    Tool {
        definition: ToolDefinition {
            name: "example.echo".into(),
            revision: "1".into(),
            description: "Echo one string with a separately assembled projection".into(),
            input_schema: json!({"type":"string"}),
            output_schema: json!({"type":"string"}),
            projection_schema: crate::tool::json_projection_schema(json!({
                "type":"object", "properties":{"echo":{"type":"string"}},
                "required":["echo"], "additionalProperties":false
            })),
        },
        executor: Arc::new(Echo),
        projection: Arc::new(EchoProjection),
    }
}
