//! Original hosted inbox, immutable policy, and decision-file replay.
use super::*;
use crate::{
    agent::{CreationConfig, Delivered, Sender, SpawnContext, SpawnRequest},
    approvals::{AgentApprovals, ApprovalPolicy, Approver},
    conversation::Limits,
    filesystem::{StockExecutors, TurnContext},
    interaction::{ApprovalRoute, ApprovalSubject, Interaction, InteractionOutcome},
    model::{Model, ModelContent, ModelProvider, ModelResolver},
    test_support::{ScriptedModelProvider, ScriptedTurn},
    tool::ToolRegistry,
};

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

struct NoWorkerContext;
impl TurnContext for NoWorkerContext {
    fn prepare<'a>(
        &'a self,
        _: (crate::executor::StockExecutor, Limits),
        _: &'a AgentState,
        _: OperationId,
        _: &'a ModelContent,
        _: &'a TurnAuthority,
    ) -> BoxFuture<'a, crate::Result<crate::executor::StockExecutor>> {
        Box::pin(async { Err(Error::Invalid("review opened worker context".into())) })
    }
}

type Binding = crate::filesystem::hosting::Bound<
    acyclic_stream::MemoryStream,
    acyclic_fs::LogicalObjectStore<acyclic_objects::MemoryObjects>,
>;

fn original_models(provider: Arc<dyn ModelProvider>) -> Arc<ModelResolver> {
    Arc::new(ModelResolver::new("test/reviewer", move |selection| {
        let (name, model) = selection
            .split_once('/')
            .ok_or_else(|| Error::Invalid("expected provider/model selection".into()))?;
        Ok((
            Model::new(name.into(), model.into(), "1".into())?,
            provider.clone(),
        ))
    }))
}

async fn family(
    verdicts: impl IntoIterator<Item = &'static str>,
) -> TestResult<(
    Binding,
    AgentPath,
    Vec<AgentState>,
    Arc<ScriptedModelProvider>,
)> {
    let reviewer = Arc::new(ScriptedModelProvider::new(verdicts.into_iter().map(
        |verdict| {
            ScriptedTurn::Text(format!(
                r#"{{"verdict":"{verdict}","reason":"reviewed exact action"}}"#
            ))
        },
    )));
    let executors = StockExecutors::new(
        (
            Model::new("test".into(), "reviewer".into(), "1".into())?,
            reviewer.clone(),
        ),
        ToolRegistry::default(),
        (
            Limits {
                file_bytes: Some(65_536),
                ..Limits::default()
            },
            None::<crate::context::CompactionPolicy>,
        ),
        Arc::new(NoWorkerContext),
    );
    // The original root template may select a role model over the host fallback.
    let executors = executors.with_models(original_models(reviewer.clone()));
    let (bound, parent, children) = family_with(Arc::new(executors)).await?;
    Ok((bound, parent, children, reviewer))
}

async fn family_with(
    executors: Arc<dyn TurnExecutors>,
) -> TestResult<(Binding, AgentPath, Vec<AgentState>)> {
    let (bound, parent) = crate::filesystem::hosting::binding_with(
        executors,
        ApprovalPolicy {
            approvers: vec![Approver::Parent],
        },
    )
    .await?;
    let sender = Sender::User {
        principal: "human".into(),
    };
    let mut children = Vec::new();
    for name in ["left", "right"] {
        let record = InboxRecord::Spawn(SpawnRequest {
            by: sender.clone(),
            id: crate::agent::IntentId::try_from(name)?,
            brief: None,
            profile: CreationConfig {
                question_plugin: false,
                topology: None,
                model: None,
                continuation: None,
                context: SpawnContext::Fork,
                template: None,
                peers: Default::default(),
            },
            history: None,
        });
        let Delivered::Committed { inbox } = bound.agents().send(&parent, &sender, &record).await?
        else {
            return Err("child creation did not commit".into());
        };
        bound.agents().drain(&parent).await?;
        let (_, child) = crate::agent::requested_child(&parent, inbox)?;
        bound.agents().drain(&child).await?;
        children.push(bound.agents().state(&child).await?);
    }
    Ok((bound, parent, children))
}

#[tokio::test]
async fn original_parent_page_replays_decisions_and_merges_only_escalations() -> TestResult {
    let (bound, parent, children, reviewer) = family(["allow", "deny", "ask", "ask"]).await?;
    let sender = Sender::User {
        principal: "human".into(),
    };
    let turns = bound.turns();
    let parent_state = bound.agents().state(&parent).await?;
    let reference = crate::filesystem::bounded_io::content_workspace(
        &turns.host.provider,
        parent_state.volumes[0].volume()?,
        None,
    )?;
    let private = turns.host.open(&reference, None).await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let mut initial = private
        .begin_transaction(
            acyclic_fs::IdempotencyKey::new(),
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    initial
        .create_dir_all("/policy", acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .await?;
    initial
        .write_text("/policy/attention.md", "ORIGINAL_TRUSTED_POLICY")
        .await?;
    initial
        .commit::<acyclic_fs::WorkspaceError, _, _>(
            None,
            acyclic_fs::PublicationPermit::Unrestricted,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation,
            |_| {
                std::future::ready(Ok(acyclic_fs::OperationReceipt {
                    value: (),
                    work: acyclic_fs::WorkCounters::UNCHARGED,
                }))
            },
        )
        .await?;
    let mut original_requests = Vec::new();
    let mut expected_actions = Vec::new();
    let mut first = None;
    let mut end = 0;
    // Reusing the same request identity in distinct children must not share inference.
    for (round, id) in [
        crate::InteractionId::from_bytes([41; 16]),
        crate::InteractionId::from_bytes([42; 16]),
    ]
    .into_iter()
    .enumerate()
    {
        for (index, child) in children.iter().enumerate() {
            let (journal, bindings, files) = turns.approval_journal(child).await?;
            let (policy, _) = turns
                .approvals(child, bindings.journal.reader.as_ref())
                .await?;
            let owner = AgentApprovals::new(files, Arc::new(policy));
            let request = Interaction::Approval {
                prompt: format!(
                    "action-{round}-{index}{}",
                    if round == 1 {
                        "x".repeat(40_000)
                    } else {
                        String::new()
                    },
                ),
                operation_id: OperationId::from_bytes([u8::try_from(round * 2 + index + 1)?; 16]),
                action_digest: [u8::try_from(round * 2 + index + 1)?; 32],
                subject: ApprovalSubject::Tool {
                    route: ApprovalRoute::Policy,
                    tool: "write".into(),
                    call_id: "write".into(),
                    arguments: serde_json::json!({"path":format!("action-{round}-{index}")}),
                    target: None,
                },
            };
            expected_actions.push(ModelContent::Text(String::from_utf8(
                crate::contract::canonical_json_bytes(&request)?,
            )?));
            owner.open(id, &request).await?;
            assert_eq!(owner.outcome(id, &journal, None).await?, None);
            let request_id =
                crate::agent::IntentId::try_from(crate::approvals::agent_request_id(id))?;
            let Delivered::Committed { inbox } = bound
                .agents()
                .send(
                    &parent,
                    &Sender::Agent {
                        path: child.path.clone(),
                    },
                    &InboxRecord::ApprovalRequested(crate::agent::ApprovalRequested {
                        child: child.path.clone(),
                        id: request_id,
                    }),
                )
                .await?
            else {
                return Err("approval request did not commit".into());
            };
            first.get_or_insert(inbox);
            end = inbox + 1;
            original_requests.push((child.clone(), id));
        }
        if round == 0 {
            let id = crate::agent::IntentId::try_from("interleaved-human")?;
            let body = bound
                .stage_text(
                    &parent,
                    "message",
                    &sender,
                    &id,
                    ("human-input", "PRIVATE_WORKER_INPUT"),
                )
                .await?;
            bound
                .agents()
                .send(
                    &parent,
                    &sender,
                    &InboxRecord::Message(crate::agent::Message {
                        from: sender.clone(),
                        id,
                        body: Box::new(body),
                        re: None,
                        attachments: None,
                    }),
                )
                .await?;
        }
    }
    let selected = turns
        .select_approval_page(&parent_state, first.ok_or("no source records")?, end)
        .await?;
    assert!(selected.policy.is_some());
    let mut foreign = selected.clone();
    foreign.source = children[0].path.clone();
    assert!(
        turns
            .review_approval_page(&parent_state, &foreign)
            .await
            .is_err()
    );
    assert_eq!(
        reviewer.request_count(),
        0,
        "foreign page reached inference"
    );
    private
        .write_text("/policy/attention.md", "CHANGED_TRUSTED_POLICY")
        .await?;
    let reviewed = turns.review_approval_page(&parent_state, &selected).await?;
    // Reopen the original page after durable decisions but before wake publication.
    let replayed = turns.review_approval_page(&parent_state, &selected).await?;
    assert_eq!(reviewed.summary, replayed.summary);
    let wakes = |outs: &[Out]| {
        outs.iter()
            .filter_map(|out| match out {
                Out::Send {
                    to,
                    record: InboxRecord::Approval(approval),
                } => Some((to.clone(), approval.id.clone())),
                _ => None,
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(wakes(&reviewed.outs), wakes(&replayed.outs));
    let expected_wakes = original_requests
        .iter()
        .take(2)
        .map(|(child, id)| {
            Ok((
                child.path.clone(),
                crate::agent::IntentId::try_from(crate::approvals::agent_request_id(*id))?,
            ))
        })
        .collect::<std::result::Result<Vec<_>, crate::agent::Error>>()?;
    assert_eq!(wakes(&reviewed.outs), expected_wakes);
    let mut published = Vec::new();
    for outputs in [&reviewed.outs, &replayed.outs] {
        let mut current = Vec::new();
        for out in outputs {
            let Out::Send { to, record } = out else {
                return Err("review emitted a non-wake effect".into());
            };
            let Delivered::Committed { inbox } = bound
                .agents()
                .send(
                    to,
                    &Sender::Agent {
                        path: parent.clone(),
                    },
                    record,
                )
                .await?
            else {
                return Err("decision wake did not commit".into());
            };
            current.push((to.clone(), inbox));
        }
        if published.is_empty() {
            published = current;
        } else {
            assert_eq!(published, current, "wake repair appended a second response");
        }
    }
    for (index, (child, id)) in original_requests.iter().enumerate() {
        let (journal, bindings, files) = turns.approval_journal(child).await?;
        let (policy, _) = turns
            .approvals(child, bindings.journal.reader.as_ref())
            .await?;
        let owner = AgentApprovals::new(files, Arc::new(policy));
        assert_eq!(
            owner.outcome(*id, &journal, None).await?,
            match index {
                0 => Some(InteractionOutcome::Approved {}),
                1 => Some(InteractionOutcome::Declined {}),
                _ => None,
            }
        );
    }
    let summary = reviewed.summary.ok_or("escalations were not summarized")?;
    let authority = crate::filesystem::agent_authority(
        &turns.issuer,
        &turns.admission,
        &parent,
        &parent_state.volumes,
        std::slice::from_ref(&summary),
    )
    .await?;
    assert!(
        !authority
            .scope
            .capabilities()
            .contains(crate::contract::capability::HARNESS_HOST)
    );
    let reader = FilesystemContentVerifier::new(
        turns.host.clone(),
        &authority.verifier,
        &authority.scope,
        turns.limits.maximum_bytes,
        None,
    )?;
    let bytes = reader
        .read(&summary, summary.descriptor().byte_length(), None, None)?
        .await?;
    let text = std::str::from_utf8(&bytes)?;
    let path = text
        .trim_end()
        .strip_prefix("Child approvals require a decision. Read volume:///private/")
        .ok_or("summary did not name the readable private document")?;
    let volume = parent_state.volumes[0].volume()?;
    let maximum = turns.limits.maximum_bytes.ok_or("missing file credit")?;
    assert!(summary.descriptor().byte_length() < maximum);
    // The input remains small even when two individually admissible actions
    // exceed its credit together. Read the original details with agent custody,
    // without a host role or a grant to another agent's protected journal.
    let (selected, _, _) = reader
        .read_private_range(
            volume,
            "",
            path,
            None,
            acyclic_fs::ByteRange {
                offset: 0,
                length: 0,
            },
            crate::conversation::ContentAccess::default(),
        )?
        .await?;
    assert!(selected.byte_length > maximum);
    let full = reader.read_private_path(
        volume,
        "",
        path,
        Some(&selected.generation),
        Some(maximum),
        crate::conversation::ContentAccess::default(),
    );
    assert!(match full {
        Ok(read) => read.await.is_err(),
        Err(_) => true,
    });
    let mut details = Vec::new();
    while (details.len() as u64) < selected.byte_length {
        let offset = details.len() as u64;
        let (_, bytes, _) = reader
            .read_private_range(
                volume,
                "",
                path,
                Some(&selected.generation),
                acyclic_fs::ByteRange {
                    offset,
                    length: maximum.min(selected.byte_length - offset),
                },
                crate::conversation::ContentAccess::default(),
            )?
            .await?;
        assert!(
            !bytes.is_empty(),
            "bounded read stopped before the source end"
        );
        details.extend_from_slice(&bytes);
    }
    let text = std::str::from_utf8(&details)?;
    for original in &expected_actions[2..] {
        let ModelContent::Text(action) = original else {
            return Err("original action was not retained".into());
        };
        assert!(action.len() as u64 <= maximum);
        assert!(
            text.contains(action),
            "escalation lost original action bytes"
        );
    }
    for action in ["action-1-0", "action-1-1"] {
        assert!(text.contains(action));
    }
    for absent in [
        "action-0-0",
        "action-0-1",
        "ORIGINAL_TRUSTED_POLICY",
        "PRIVATE_WORKER_INPUT",
    ] {
        assert!(
            !text.contains(absent),
            "unrelated private data entered escalation"
        );
    }
    let requests = reviewer.requests();
    assert_eq!(
        requests.len(),
        4,
        "replay inferred again or child request identities collided"
    );
    for (request, original) in requests.into_iter().zip(expected_actions) {
        assert!(
            request
                .messages
                .iter()
                .any(|message| message.role == crate::model::ModelRole::User
                    && message.content == original),
            "review changed the original action bytes"
        );
        assert!(request.tools.is_empty());
        assert!(
            request
                .messages
                .iter()
                .any(|message| message.content
                    == ModelContent::Text("ORIGINAL_TRUSTED_POLICY".into()))
        );
        let encoded = serde_json::to_string(&request.messages)?;
        assert!(!encoded.contains("CHANGED_TRUSTED_POLICY"));
        assert!(!encoded.contains("PRIVATE_WORKER_INPUT"));
    }
    assert_eq!(
        bound.agents().state(&parent).await?.conversation_tail,
        parent_state.conversation_tail
    );
    Ok(())
}

#[tokio::test]
async fn reopening_after_parent_decisions_commits_original_wakes_once() -> TestResult {
    let (bound, parent, children, reviewer) = family(["allow", "deny"]).await?;
    let turns = bound.turns();
    let parent_before = bound.agents().state(&parent).await?;
    let id = crate::InteractionId::from_bytes([81; 16]);
    let request_id = crate::agent::IntentId::try_from(crate::approvals::agent_request_id(id))?;
    let mut delivered = Vec::new();
    let mut child_tails = Vec::new();
    for (index, child) in children.iter().enumerate() {
        let (_, bindings, files) = turns.approval_journal(child).await?;
        let (policy, _) = turns
            .approvals(child, bindings.journal.reader.as_ref())
            .await?;
        AgentApprovals::new(files, Arc::new(policy))
            .open(
                id,
                &Interaction::Approval {
                    prompt: format!("action-{index}"),
                    operation_id: OperationId::from_bytes([u8::try_from(index + 1)?; 16]),
                    action_digest: [u8::try_from(index + 1)?; 32],
                    subject: ApprovalSubject::Tool {
                        route: ApprovalRoute::Policy,
                        tool: "write".into(),
                        call_id: "write".into(),
                        arguments: serde_json::json!({"path":format!("action-{index}")}),
                        target: None,
                    },
                },
            )
            .await?;
        let Delivered::Committed { inbox } = bound
            .agents()
            .send(
                &parent,
                &Sender::Agent {
                    path: child.path.clone(),
                },
                &InboxRecord::ApprovalRequested(crate::agent::ApprovalRequested {
                    child: child.path.clone(),
                    id: request_id.clone(),
                }),
            )
            .await?
        else {
            return Err("request did not commit".into());
        };
        delivered.push(inbox);
        child_tails.push(turns.stream.tail(child.path.inbox()?).await?);
    }
    let end = delivered[1] + 1;
    let selected = turns
        .select_approval_page(&parent_before, delivered[0], end)
        .await?;
    let prepared = turns
        .review_approval_page(&parent_before, &selected)
        .await?;
    assert!(prepared.summary.is_none());
    assert_eq!(prepared.outs.len(), 2);
    assert_eq!(reviewer.request_count(), 2);
    // Durable approval files survived, but the actor checkpoint/wakes did not commit.
    drop(prepared);
    assert!(
        !bound
            .agents()
            .input_progress(&parent, delivered[0])
            .await?
            .settled
    );
    for (child, original) in children.iter().zip(&child_tails) {
        assert_eq!(turns.stream.tail(child.path.inbox()?).await?, *original);
    }
    let reopen = || -> TestResult<_> {
        let handler = crate::agent::AgentHandler::new(
            crate::agent::actor_store(&turns.stream)?,
            crate::agent::ExecutorDriver::new(turns.clone()),
            acyclic_actors::domain::ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
        )?
        .with_close();
        Ok(bound.agents().rehosted(
            Arc::new(handler),
            Arc::new(acyclic_actors::local::SystemClock),
            None,
        )?)
    };
    let reopened = reopen()?;
    reopened.drain(&parent).await?;
    let completed = reopened.state(&parent).await?;
    assert!(completed.approval_control.is_none());
    assert_eq!(completed.conversation_tail, parent_before.conversation_tail);
    for inbox in delivered {
        assert!(reopened.input_progress(&parent, inbox).await?.settled);
    }
    for (child, original) in children.iter().zip(&child_tails) {
        assert_eq!(turns.stream.tail(child.path.inbox()?).await?, original + 1);
        let mut records = turns
            .stream
            .read(ReadRequest {
                key: None,
                path: child.path.inbox()?,
                from: *original,
                limit: 1,
                at_least: Default::default(),
            })
            .await?;
        let record = records
            .try_next()
            .await?
            .ok_or("original child wake is missing")?;
        assert_eq!(
            InboxRecord::decode(&record.value)?,
            InboxRecord::Approval(Approval {
                id: request_id.clone()
            })
        );
    }
    // A second runtime opens only committed state; it cannot repeat either effect.
    let again = reopen()?;
    again.drain(&parent).await?;
    assert_eq!(
        reviewer.request_count(),
        2,
        "reopen dispatched another inference"
    );
    for (child, original) in children.iter().zip(&child_tails) {
        assert_eq!(turns.stream.tail(child.path.inbox()?).await?, original + 1);
    }
    Ok(())
}

#[tokio::test]
async fn child_approval_pauses_and_resumes_its_original_operation_after_parent_review() -> TestResult
{
    use crate::executor::{ExecutionJournal, ExecutionProgress, Executor, TurnInput, TurnOutput};
    use std::sync::Mutex;
    type Calls = Arc<Mutex<Vec<(AgentPath, OperationId, Option<InteractionOutcome>)>>>;
    struct Worker {
        child: AgentPath,
        calls: Calls,
    }
    impl Executor for Worker {
        fn execute<'a>(
            &'a self,
            input: TurnInput,
            journal: &'a dyn ExecutionJournal,
        ) -> BoxFuture<'a, crate::Result<TurnOutput>> {
            Box::pin(async move { self.execute_progress(input, journal).await?.into_output() })
        }
        fn execute_progress<'a>(
            &'a self,
            input: TurnInput,
            journal: &'a dyn ExecutionJournal,
        ) -> BoxFuture<'a, crate::Result<ExecutionProgress<TurnOutput>>> {
            Box::pin(async move {
                let id = crate::InteractionId::from_bytes([91; 16]);
                journal
                    .open_interaction(
                        id,
                        &Interaction::Approval {
                            prompt: format!("execute for {}", self.child),
                            operation_id: input.operation_id,
                            action_digest: *blake3::hash(&input.operation_id.into_bytes())
                                .as_bytes(),
                            subject: ApprovalSubject::Opaque {},
                        },
                        None,
                    )?
                    .await?;
                let outcome = journal.interaction_outcome(id, None)?.await?;
                self.calls.lock().unwrap().push((
                    self.child.clone(),
                    input.operation_id,
                    outcome.clone(),
                ));
                Ok(match outcome {
                    None => ExecutionProgress::Pending(input.operation_id),
                    Some(outcome) => ExecutionProgress::Ready(TurnOutput::new(
                        "action settled".into(),
                        serde_json::to_value(outcome)
                            .map_err(|error| Error::Invalid(error.to_string()))?,
                        1,
                    )),
                })
            })
        }
    }
    struct Executors {
        review: StockExecutors,
        calls: Calls,
    }
    impl TurnExecutors for Executors {
        fn review<'a>(
            &'a self,
            agent: &'a AgentState,
            execution: (
                &'a dyn ExecutionJournal,
                Option<&'a crate::executor::PreparationIo>,
            ),
            request: (&'a AgentPath, &'a crate::approvals::ApprovalRequest<'a>),
            principal: &'a str,
            policy: &'a str,
        ) -> BoxFuture<'a, crate::Result<(RuleVerdict, Option<String>)>> {
            self.review
                .review(agent, execution, request, principal, policy)
        }
        fn executor<'a>(
            &'a self,
            agent: &'a AgentState,
            _: OperationId,
            _: &'a ModelContent,
            _: &'a TurnAuthority,
        ) -> BoxFuture<'a, crate::Result<Arc<dyn Executor>>> {
            Box::pin(async move {
                if agent.parent.is_none() {
                    return Err(Error::Invalid(
                        "resolved asks opened parent worker context".into(),
                    ));
                }
                Ok(Arc::new(Worker {
                    child: agent.path.clone(),
                    calls: self.calls.clone(),
                }) as Arc<dyn Executor>)
            })
        }
    }
    let reviewer = Arc::new(ScriptedModelProvider::new(["allow", "deny"].map(
        |verdict| ScriptedTurn::Text(format!(r#"{{"verdict":"{verdict}","reason":null}}"#)),
    )));
    let calls: Calls = Default::default();
    let review = StockExecutors::new(
        (
            Model::new("test".into(), "reviewer".into(), "1".into())?,
            reviewer.clone(),
        ),
        ToolRegistry::default(),
        (Limits::default(), None::<crate::context::CompactionPolicy>),
        Arc::new(NoWorkerContext),
    )
    .with_models(original_models(reviewer.clone()));
    let (bound, parent, children) = family_with(Arc::new(Executors {
        review,
        calls: calls.clone(),
    }))
    .await?;
    let parent_before = bound.agents().state(&parent).await?;
    let sender = Sender::User {
        principal: "human".into(),
    };
    let mut original_turns = Vec::new();
    for child in &children {
        let id = crate::agent::IntentId::try_from("work")?;
        let body = bound
            .stage_text(
                &child.path,
                "message",
                &sender,
                &id,
                ("work", "execute action"),
            )
            .await?;
        bound
            .agents()
            .send(
                &child.path,
                &sender,
                &InboxRecord::Message(crate::agent::Message {
                    from: sender.clone(),
                    id,
                    body: Box::new(body),
                    re: None,
                    attachments: None,
                }),
            )
            .await?;
        bound.agents().drain(&child.path).await?;
        let paused = bound.agents().state(&child.path).await?;
        assert!(matches!(
            paused.phase,
            crate::agent::Phase::WaitingApproval {
                binding: Some(_),
                ..
            }
        ));
        original_turns.push(
            paused
                .turn
                .ok_or("approval did not retain its worker invocation")?
                .inbox,
        );
        assert_eq!(paused.conversation_tail, child.conversation_tail);
    }
    assert_eq!(
        reviewer.request_count(),
        0,
        "child dispatched a parent review in its own context"
    );
    assert!(
        calls
            .lock()
            .unwrap()
            .iter()
            .all(|(_, _, outcome)| outcome.is_none())
    );
    // A new runtime processes the original delegated requests and paused children.
    let turns = bound.turns();
    let handler = crate::agent::AgentHandler::new(
        crate::agent::actor_store(&turns.stream)?,
        crate::agent::ExecutorDriver::new(turns.clone()),
        acyclic_actors::domain::ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
    )?
    .with_close();
    let reopened = bound.agents().rehosted(
        Arc::new(handler),
        Arc::new(acyclic_actors::local::SystemClock),
        None,
    )?;
    reopened.drain(&parent).await?;
    let parent_after = reopened.state(&parent).await?;
    assert_eq!(
        parent_after.conversation_tail,
        parent_before.conversation_tail
    );
    assert!(parent_after.turn.is_none());
    assert_eq!(reviewer.request_count(), 2);
    for (index, child) in children.iter().enumerate() {
        reopened.drain(&child.path).await?;
        let resumed = reopened.state(&child.path).await?;
        assert!(matches!(resumed.phase, crate::agent::Phase::Idle));
        assert!(resumed.turn.is_none());
        assert_eq!(resumed.conversation_tail, child.conversation_tail + 1);
        let entry =
            crate::agent::answer(&*turns.stream, &child.path, child.conversation_tail).await?;
        assert_eq!(entry.inbox, original_turns[index]);
        let calls = calls.lock().unwrap();
        let child_calls = calls
            .iter()
            .filter(|(path, _, _)| path == &child.path)
            .collect::<Vec<_>>();
        assert_eq!(
            child_calls.len(),
            2,
            "approval replay executed an extra worker step"
        );
        assert_eq!(
            child_calls[0].1, child_calls[1].1,
            "approval wake substituted another operation"
        );
        assert_eq!(
            child_calls[1].2,
            Some(if index == 0 {
                InteractionOutcome::Approved {}
            } else {
                InteractionOutcome::Declined {}
            })
        );
    }
    assert!(
        reviewer
            .requests()
            .iter()
            .all(|request| request.tools.is_empty())
    );
    Ok(())
}
