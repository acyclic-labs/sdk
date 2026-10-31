//! A hosted approval's success path, through the one shared core every host
//! serves `Approvals` and `Decide` with. A root created with no policy file
//! still runs under its host's preset policy: under the generic default, a
//! turn pauses on a tool call the human is asked about, the pending approval
//! lists with the exact action, a decision bound to another action is
//! refused, the exact decision resumes the turn to completion, and retrying
//! that decision after the agent resumed replays instead of deciding again.
//! Under a preset that explicitly accepts every action, the same call runs
//! with nothing pending.
use crate::conformance::{Backend, Result, observe::Observed, roots::volume, vectors};
use acyclic_actors::{addressing::ActorReference, local::SystemClock};
use acyclic_harness_core::{
    Capabilities, OperationId,
    agent::*,
    approvals::ApprovalPolicy,
    context::{CompactionPolicy, ContextPipeline},
    contract::capability::INTERACTION_ROUTE,
    conversation::{Limits, VolumeOperation},
    core::{AggregateKind, Authority, AuthorityRoot},
    executor::{Executor, StockExecutor},
    filesystem::{
        PRIVATE_VOLUME, TurnAuthority, TurnExecutors, TurnLimits,
        hosting::{Bound, Placement, RootRequest, RootSubmission, Turns, VerifiedCaller},
    },
    interaction::{ApprovalBinding, Interaction},
    model::{
        Model, ModelContent, ModelDispatch, ModelEvent, ModelProvider, PreparedModelRequest,
        ToolCall,
    },
    runtime::RuntimeScope,
    tool::{
        JsonToolProjection, Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolRegistry,
        ToolResult,
    },
};
use acyclic_stream::{BoxProviderFuture, BoxProviderStream};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

const TOOL: &str = "conformance.echo";
/// The default policy's one approver.
const HUMAN: &str = "human";

type Objects = crate::conformance::ObjectStore;

/// Calls the tool once, then answers.
struct Scripted;
impl ModelProvider for Scripted {
    fn generate<'a>(
        &'a self,
        _: &'a PreparedModelRequest,
        dispatch: ModelDispatch,
    ) -> BoxProviderStream<'a, acyclic_harness_core::Result<ModelEvent>> {
        let event = if dispatch.step == 0 {
            ModelEvent::ToolCall(ToolCall::new("call-0".into(), TOOL.into(), json!({})))
        } else {
            ModelEvent::Content {
                delta: "done".into(),
            }
        };
        Box::pin(futures::stream::iter([
            Ok(event),
            Ok(ModelEvent::Completed {
                metadata: Value::Null,
            }),
        ]))
    }
}

/// Counts its executions.
struct Echo(Arc<AtomicUsize>);
impl ToolExecutor for Echo {
    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        _: Option<acyclic_harness_core::runtime::ToolContext>,
    ) -> BoxProviderFuture<'a, acyclic_harness_core::Result<ToolResult>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            Ok(ToolResult {
                value: invocation.arguments,
            })
        })
    }

    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<acyclic_harness_core::runtime::ToolContext>,
    ) -> BoxProviderFuture<'a, acyclic_harness_core::Result<Option<ToolResult>>> {
        Box::pin(async { Ok(None) })
    }
}

/// Each turn: the scripted model, the counted tool, and the turn's resolved
/// approval gate. The executor adds no gate of its own.
struct Approving(Arc<AtomicUsize>);
impl TurnExecutors for Approving {
    fn executor<'a>(
        &'a self,
        _: &'a AgentState,
        _: OperationId,
        _: &'a ModelContent,
        authority: &'a TurnAuthority,
    ) -> BoxProviderFuture<'a, acyclic_harness_core::Result<Arc<dyn Executor>>> {
        Box::pin(async move {
            let mut tools = ToolRegistry::default();
            tools.register(
                (
                    Tool {
                        hook_names: &[],
                        definition: ToolDefinition {
                            name: TOOL.into(),
                            revision: "1".into(),
                            description: "Echo".into(),
                            input_schema: json!(true),
                            output_schema: json!(true),
                            projection_schema: json!(true),
                        },
                        executor: Arc::new(Echo(Arc::clone(&self.0))),
                        projection: Arc::new(JsonToolProjection),
                    },
                    None,
                ),
                &mut None,
                None,
            )?;
            let executor = StockExecutor::new(
                Model::new(
                    "conformance".into(),
                    "scripted".into(),
                    "1".into(),
                    Value::Null,
                )?,
                Arc::new(Scripted),
                ContextPipeline::default(),
                tools,
            )
            .with_compaction_policy(CompactionPolicy::Disabled)
            .with_tool_authority(
                RuntimeScope::new(
                    Capabilities::new([
                        "model:generate".to_owned(),
                        format!("tool:call:{TOOL}"),
                        INTERACTION_ROUTE.to_owned(),
                    ]),
                    Limits::default(),
                )?,
                Arc::clone(&authority.approval),
            )?;
            Ok(Arc::new(executor) as Arc<dyn Executor>)
        })
    }
}

pub(crate) async fn verify(backend: &dyn Backend) -> Result {
    human_decides(backend).await?;
    accepted(backend).await
}

/// A root with no policy file, under a host whose preset is `policy`, after
/// its first turn on a message asking it to call the tool.
async fn asked(
    backend: &dyn Backend,
    policy: ApprovalPolicy,
) -> Result<(
    Bound<Observed, Objects>,
    AgentPath,
    Arc<AtomicUsize>,
    Box<dyn crate::conformance::Storage>,
)> {
    let storage = backend.fresh().await?;
    let stream = Arc::new(Observed::new(storage.open().await?));
    let audience = Authority {
        kind: AggregateKind::Conversation,
        id: "workload".into(),
    };
    let issuer = AuthorityRoot::new("conformance", [7; 32], audience.clone());
    let agent = crate::conformance::fixtures::adopted().agent;
    let private = volume(PRIVATE_VOLUME, agent.clone())?;
    let grants = vec![
        INTERACTION_ROUTE.to_owned(),
        private
            .volume
            .capability(VolumeOperation::Read, &mut None)?,
        private
            .volume
            .capability(VolumeOperation::Write, &mut None)?,
    ];
    let scope = issuer.bootstrap_scope("root", &Capabilities::new(grants), None, &mut None)?;
    let objects = storage.objects().await?;
    let capabilities = objects.capabilities;
    let calls = Arc::new(AtomicUsize::new(0));
    let bound = Bound::admitted(
        issuer,
        VerifiedCaller { audience, scope },
        Placement {
            filesystem: acyclic_fs::Fs::new(
                acyclic_fs::StreamAuthorityStore::new(stream),
                objects.into_store(storage.as_ref())?,
                capabilities,
            ),
            provider: private.volume.provider().clone(),
            within: ActorReference::new(b"conformance", b"agents", b"host")?,
        },
        Turns {
            executors: Arc::new(Approving(Arc::clone(&calls))),
            limits: TurnLimits {
                maximum_bytes: 64 * 1024,
                max_steps: 4,
                history_turns: 8,
            },
            actors: vectors::limits()?,
            clock: Arc::new(SystemClock),
            approval_policy: policy,
        },
    )?;
    let RootSubmission::Created(root) = bound
        .create_root(
            &vectors::intent("root")?,
            RootRequest {
                profile: None,
                agent,
                fresh: vec![private],
                retained: Vec::new(),
                brief: None,
                policy: None,
            },
        )
        .await?
    else {
        return Err("the root's creation was uncertain".into());
    };
    let human = Sender::User {
        principal: HUMAN.into(),
    };
    let id = vectors::intent("ask")?;
    let body = bound
        .stage_text(
            &root,
            "message",
            &human,
            &id,
            ("call the tool", "call the tool"),
        )
        .await?;
    bound
        .agents()
        .send(
            &root,
            &human,
            &InboxRecord::Message(Message {
                from: human.clone(),
                id,
                body: Box::new(body),
                attachments: None,
            }),
        )
        .await?;
    bound
        .agents()
        .drain(&root)
        .await
        .map_err(|error| format!("initial approval drain: {error}"))?;
    Ok((bound, root, calls, storage))
}

/// The generic default asks the human, and the decision path completes.
async fn human_decides(backend: &dyn Backend) -> Result {
    let (bound, root, calls, _storage) = asked(backend, ApprovalPolicy::default()).await?;

    // The turn waits on the tool call, which has not run.
    let Some((
        pending,
        Interaction::Approval {
            operation_id,
            action_digest,
            ..
        },
    )) = bound.pending_approval(&root).await?
    else {
        return Err("the turn does not wait on an approval of its tool call".into());
    };
    if calls.load(Ordering::SeqCst) != 0 {
        return Err("the tool ran before its approval".into());
    }
    let exact = ApprovalBinding {
        operation_id,
        action_digest,
    };
    let mut other = exact.clone();
    other.action_digest[0] ^= 1;
    if bound
        .decide(&root, &pending, &other, HUMAN, true, None)
        .await
        .is_ok()
    {
        return Err("a decision bound to another action was accepted".into());
    }

    // The exact decision resumes the turn, which runs the tool and completes.
    bound
        .decide(&root, &pending, &exact, HUMAN, true, None)
        .await?;
    bound
        .agents()
        .drain(&root)
        .await
        .map_err(|error| format!("approved continuation drain: {error}"))?;
    let state = bound.agents().state(&root).await?;
    if state.phase != Phase::Idle || calls.load(Ordering::SeqCst) != 1 {
        return Err(format!(
            "the approved turn did not complete: {:?}, {} tool calls",
            state.phase,
            calls.load(Ordering::SeqCst)
        )
        .into());
    }
    if bound.pending_approval(&root).await?.is_some() {
        return Err("a decided approval is still pending".into());
    }

    // Retrying the identical decision replays it; it never decides again.
    bound
        .decide(&root, &pending, &exact, HUMAN, true, None)
        .await?;
    bound
        .agents()
        .drain(&root)
        .await
        .map_err(|error| format!("replayed decision drain: {error}"))?;
    if bound.agents().state(&root).await?.phase != Phase::Idle || calls.load(Ordering::SeqCst) != 1
    {
        return Err("a replayed decision ran the tool again".into());
    }
    Ok(())
}

/// A preset that explicitly accepts every action runs the call unasked.
async fn accepted(backend: &dyn Backend) -> Result {
    let (bound, root, calls, _storage) =
        asked(backend, ApprovalPolicy::accept_all("conformance")).await?;
    if bound.pending_approval(&root).await?.is_some() {
        return Err("an accept-all preset left the tool call pending".into());
    }
    let phase = bound.agents().state(&root).await?.phase;
    if phase != Phase::Idle || calls.load(Ordering::SeqCst) != 1 {
        return Err(format!(
            "the accepted turn did not complete: {phase:?}, {} tool calls",
            calls.load(Ordering::SeqCst)
        )
        .into());
    }
    Ok(())
}
