//! Original hosted inbox, immutable policy, and decision-file replay.
use super::*;
use crate::{
    agent::{CreationConfig, Delivered, Sender, SpawnContext, SpawnRequest},
    approvals::{AgentApprovals, ApprovalPolicy, Approver},
    conversation::Limits,
    filesystem::{StockExecutors, TurnContext},
    interaction::{ApprovalRoute, ApprovalSubject, Interaction, InteractionOutcome},
    model::{Model, ModelContent},
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

#[tokio::test]
async fn original_parent_page_replays_decisions_and_merges_only_escalations() -> TestResult {
    let reviewer = Arc::new(ScriptedModelProvider::new(
        ["allow", "deny", "ask", "ask"].into_iter().map(|verdict| {
            ScriptedTurn::Text(format!(
                r#"{{"verdict":"{verdict}","reason":"reviewed exact action"}}"#
            ))
        }),
    ));
    let executors = StockExecutors::new(
        (
            Model::new("test".into(), "reviewer".into(), "1".into())?,
            reviewer.clone(),
        ),
        ToolRegistry::default(),
        (Limits::default(), None::<crate::context::CompactionPolicy>),
        Arc::new(NoWorkerContext),
    );
    let (bound, parent) = crate::filesystem::hosting::binding_with(
        Arc::new(executors),
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
                prompt: format!("action-{round}-{index}"),
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
    let (_, bindings, _) = turns.parent_review_binding(&parent_state, None).await?;
    let bytes = bindings
        .journal
        .reader
        .read(&summary, summary.descriptor().byte_length(), None, None)?
        .await?;
    let text = std::str::from_utf8(&bytes)?;
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
