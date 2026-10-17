//! Crash cuts across the durable approval boundaries: request admission,
//! decision admission and the decision observation that gates execution.
use super::*;
use crate::{
    AgentId, Capabilities, TaskId,
    conversation::FileDescriptor,
    core::{Action, AggregateKind, Authority, AuthorityIssuer, Command},
    durable_host::tests::fixture::Fixture,
    interaction::{ApprovalDecisionFile, ApprovalVerdict, InteractionResolution},
    task_volume::{Component, FileKey, FileValue, Query, TaskFile, TaskFiles},
};
use acyclic_fs::{MemoryAuthorityBackend, MemoryObjectBackend};
use acyclic_stream::MemoryStream;

#[path = "approval_install_tests.rs"]
mod install;

type Journal =
    FilesystemExecutionJournal<MemoryStream, MemoryAuthorityBackend, MemoryObjectBackend>;
const TASK: u8 = 80;

fn conversation() -> AuthorityIssuer {
    AuthorityIssuer::new(
        "approval-files",
        [8; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "approval-files".into(),
        },
    )
}

async fn bind(fixture: &Fixture, conversation: &AuthorityIssuer) -> Result<()> {
    let mut aggregate = StreamAggregate::open(
        &fixture.stream,
        (
            conversation.audience().clone(),
            conversation.verifier(&mut None)?,
            SchemaRegistry::new(),
        ),
        None,
        crate::store::default_projection_read_limits(),
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?;
    aggregate
        .execute(
            Command {
                operation_id: OperationId::from_bytes([30; 16]),
                idempotency_key: IdempotencyKey::new("bind")?,
                expected_revision: 0,
                scope: conversation.issue(
                    "binder",
                    &Capabilities::new(["conversation:bind"]),
                    None,
                    None,
                    &mut None,
                )?,
                causal_parent: None,
                action: Action::BindConversation {
                    agent: AgentId::from_bytes([1; 16]),
                },
            },
            None,
            &mut None,
        )
        .await?;
    Ok(())
}

async fn journal(
    fixture: &Fixture,
    conversation: &AuthorityIssuer,
    files: Arc<TaskFiles<MemoryStream>>,
) -> Result<Journal> {
    let owner_scope = conversation.issue(
        "interaction-owner",
        &Capabilities::new([
            crate::contract::capability::INTERACTION_OPEN.to_owned(),
            fixture
                .storage
                .capability(VolumeOperation::Read, &mut None)?,
            fixture
                .storage
                .capability(VolumeOperation::Write, &mut None)?,
        ]),
        Some(AgentId::from_bytes([1; 16])),
        None,
        &mut None,
    )?;
    fixture
        .execution(
            fixture.owner(TASK).await?,
            OperationId::from_bytes([TASK; 16]),
        )?
        .with_task_files(Some(files))
        .with_interaction_owner(
            &conversation.verifier(&mut None)?,
            &SchemaRegistry::new(),
            &owner_scope,
            &fixture.storage,
            None,
        )
}

fn task() -> Component {
    Component::new(TaskId::from_bytes([TASK; 16]).to_string()).unwrap()
}

fn request_id(id: InteractionId) -> Component {
    Component::new(id.to_string()).unwrap()
}

/// The task owner's scope, as the journal itself holds it.
async fn owner_scope(fixture: &Fixture) -> Result<Scope> {
    Ok(fixture
        .execution(
            fixture.owner(TASK).await?,
            OperationId::from_bytes([TASK; 16]),
        )?
        .scope
        .clone())
}

async fn approvals(files: &TaskFiles<MemoryStream>) -> Result<Vec<FileValue>> {
    Ok(files.snapshot(&Query::Approvals(task()), 16).await?.files)
}

fn decision(id: InteractionId, file_principal: &str, body: &ApprovalDecisionFile) -> FileValue {
    FileValue::new(
        FileKey::Task {
            task: task(),
            file: TaskFile::ApprovalDecision {
                request: request_id(id),
                principal: Component::new(ApprovalDecisionFile::principal_file(file_principal))
                    .unwrap(),
            },
        },
        serde_json::to_vec(body).unwrap(),
    )
}

fn responder(conversation: &AuthorityIssuer, name: &str, ids: &[InteractionId]) -> Result<Scope> {
    conversation.issue(
        name,
        &Capabilities::new(
            std::iter::once(crate::contract::capability::INTERACTION_RESOLVE.to_owned()).chain(
                ids.iter()
                    .map(|id| crate::contract::capability::interaction_respond(*id)),
            ),
        ),
        None,
        None,
        &mut None,
    )
}

#[tokio::test]
async fn approval_files_are_owner_copies_published_before_execution_sees_a_decision() -> Result<()>
{
    let fixture = Fixture::new(
        Arc::new(MemoryStream::default()),
        acyclic_fs::Fs::memory(),
        serde_json::json!({"type":"integer"}),
        None,
        &[],
    )
    .await?;
    fixture.running(TASK).await?;
    let conversation = conversation();
    bind(&fixture, &conversation).await?;
    // The unpublished journal is the same owner after a crash that lost every
    // task-file publication following a durable commit: its files land on a
    // scratch stream that the followed volume never sees.
    let open_files = |stream: Arc<MemoryStream>| {
        let fixture = &fixture;
        async move {
            Ok::<_, Error>(Arc::new(
                TaskFiles::open(
                    stream,
                    &fixture.storage,
                    &fixture.issuer.verifier(&mut None)?,
                    &owner_scope(fixture).await?,
                    65_536,
                )
                .await?,
            ))
        }
    };
    let lost = open_files(Arc::new(MemoryStream::default())).await?;
    let unpublished = journal(&fixture, &conversation, lost).await?;
    let files = open_files(fixture.stream.clone()).await?;
    let trail = journal(&fixture, &conversation, files.clone()).await?;
    let (id, denied) = (
        InteractionId::from_bytes([82; 16]),
        InteractionId::from_bytes([83; 16]),
    );
    let human = responder(&conversation, "human", &[id, denied])?;
    let reviewer = responder(&conversation, "reviewer", &[id])?;
    let bystander = responder(&conversation, "bystander", &[])?;
    let request = Interaction::approval(
        "Run the exact tool".into(),
        OperationId::from_bytes([81; 16]),
        [5; 32],
    )?;

    // Cut 1: the request is admitted but its file is lost; reopening republishes it.
    unpublished.open_interaction(id, &request, None)?.await?;
    assert!(approvals(&files).await?.is_empty());
    trail.open_interaction(id, &request, None)?.await?;
    let opened = approvals(&files).await?;
    assert_eq!(opened.len(), 1);
    assert_eq!(
        opened[0].key,
        FileKey::Task {
            task: task(),
            file: TaskFile::ApprovalRequest(request_id(id)),
        }
    );
    assert_eq!(
        serde_json::from_slice::<Interaction>(&opened[0].bytes).unwrap(),
        request
    );
    let sha =
        *FileDescriptor::from_bytes(&opened[0].bytes, "application/json", &mut None)?.sha256();
    assert!(trail.interaction_outcome(id, None)?.await?.is_none());

    // Every unbound decision is refused before settlement and leaves no file.
    let approve = |principal: &str, sha: &[u8; 32], version| {
        ApprovalDecisionFile::new(principal, sha, version, true, None).unwrap()
    };
    let mut wrong_task = decision(id, "human", &approve("human", &sha, 1));
    wrong_task.key = FileKey::Task {
        task: Component::new("other-task").unwrap(),
        file: TaskFile::ApprovalDecision {
            request: request_id(id),
            principal: Component::new(ApprovalDecisionFile::principal_file("human")).unwrap(),
        },
    };
    for (file, scope) in [
        (decision(id, "reviewer", &approve("human", &sha, 1)), &human),
        (wrong_task, &human),
    ] {
        assert!(matches!(
            trail.write_approval_decision(&file, scope).await,
            Err(Error::Unauthorized(_))
        ));
    }
    for body in [
        approve("intruder", &sha, 1),
        approve("human", &[9; 32], 1),
        approve("human", &sha, 2),
    ] {
        assert!(matches!(
            trail
                .write_approval_decision(&decision(id, "human", &body), &human)
                .await,
            Err(Error::Conflict(_))
        ));
    }
    assert!(
        trail
            .write_approval_decision(
                &decision(id, "bystander", &approve("bystander", &sha, 1)),
                &bystander
            )
            .await
            .is_err()
    );
    assert_eq!(approvals(&files).await?.len(), 1);

    // Cut 2: the decision commits but its file is lost. The owner already holds
    // the verdict (negative control), yet execution only observes it through the
    // journal, which publishes the decision file before returning it.
    unpublished
        .write_approval_decision(&decision(id, "human", &approve("human", &sha, 1)), &human)
        .await?;
    assert!(matches!(
        unpublished.interaction_outcome(id, None)?.await?,
        Some(InteractionOutcome::Approved {})
    ));
    assert_eq!(approvals(&files).await?.len(), 1);
    assert!(matches!(
        trail.interaction_outcome(id, None)?.await?,
        Some(InteractionOutcome::Approved {})
    ));
    let decided = approvals(&files).await?;
    assert_eq!(decided.len(), 2);
    let file = decided
        .iter()
        .find(|file| {
            matches!(
                file.key,
                FileKey::Task {
                    file: TaskFile::ApprovalDecision { .. },
                    ..
                }
            )
        })
        .unwrap();
    assert_eq!(
        file.key,
        decision(id, "human", &approve("human", &sha, 1)).key
    );
    let body = ApprovalDecisionFile::decode(&file.bytes, 4096)?;
    assert_eq!(
        (body.principal.as_str(), body.verdict, body.binds(&sha, 1)),
        ("human", ApprovalVerdict::Approve, true)
    );

    // A retried decision is idempotent; a competing verdict cannot settle again.
    trail
        .write_approval_decision(&decision(id, "human", &approve("human", &sha, 1)), &human)
        .await?;
    let deny = ApprovalDecisionFile::new("reviewer", &sha, 1, false, Some("no".into()))?;
    assert!(matches!(
        trail
            .write_approval_decision(&decision(id, "reviewer", &deny), &reviewer)
            .await,
        Err(Error::Conflict(_))
    ));
    assert_eq!(approvals(&files).await?, decided);

    // Uncut path: a written denial is settled and published in one call.
    trail.open_interaction(denied, &request, None)?.await?;
    let denial = ApprovalDecisionFile::new("human", &sha, 1, false, Some("too broad".into()))?;
    trail
        .write_approval_decision(&decision(denied, "human", &denial), &human)
        .await?;
    let all = approvals(&files).await?;
    assert_eq!(all.len(), 4);
    let published = all
        .iter()
        .find(|file| file.key == decision(denied, "human", &denial).key)
        .unwrap();
    let body = ApprovalDecisionFile::decode(&published.bytes, 4096)?;
    assert_eq!(
        (body.verdict, body.reason.as_deref()),
        (ApprovalVerdict::Deny, Some("too broad"))
    );
    assert!(matches!(
        trail.interaction_outcome(denied, None)?.await?,
        Some(InteractionOutcome::Declined {})
    ));
    Ok(())
}

/// Records a pending `Indeterminate` resolution directly in the owner's
/// conversation, as a racing settlement would, advancing the open version.
async fn indeterminate(
    fixture: &Fixture,
    conversation: &AuthorityIssuer,
    id: InteractionId,
    responder: &Scope,
) -> Result<()> {
    let mut aggregate = StreamAggregate::open(
        &fixture.stream,
        (
            conversation.audience().clone(),
            conversation.verifier(&mut None)?,
            SchemaRegistry::new(),
        ),
        None,
        crate::store::default_projection_read_limits(),
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?;
    let operation_id = OperationId::from_bytes([84; 16]);
    let revision = aggregate.reducer().revision();
    aggregate
        .execute(
            Command {
                operation_id,
                idempotency_key: IdempotencyKey::new("racing-indeterminate")?,
                expected_revision: revision,
                scope: responder.clone(),
                causal_parent: None,
                action: Action::ResolveInteraction {
                    resolution: InteractionResolution {
                        id: uuid::Uuid::from_bytes(id.into_bytes()),
                        expected_version: 1,
                        outcome: InteractionOutcome::Indeterminate { operation_id },
                        detail: None,
                    },
                },
            },
            None,
            &mut None,
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn a_submitted_decision_settles_only_its_own_version() -> Result<()> {
    let fixture = Fixture::new(
        Arc::new(MemoryStream::default()),
        acyclic_fs::Fs::memory(),
        serde_json::json!({"type":"integer"}),
        None,
        &[],
    )
    .await?;
    fixture.running(TASK).await?;
    let conversation = conversation();
    bind(&fixture, &conversation).await?;
    let files = Arc::new(
        TaskFiles::open(
            fixture.stream.clone(),
            &fixture.storage,
            &fixture.issuer.verifier(&mut None)?,
            &owner_scope(&fixture).await?,
            65_536,
        )
        .await?,
    );
    let trail = journal(&fixture, &conversation, files.clone()).await?;
    let id = InteractionId::from_bytes([85; 16]);
    let human = responder(&conversation, "human", &[id])?;
    let request = Interaction::approval(
        "Run the exact tool".into(),
        OperationId::from_bytes([81; 16]),
        [5; 32],
    )?;
    trail.open_interaction(id, &request, None)?.await?;
    let opened = approvals(&files).await?;
    let sha =
        *FileDescriptor::from_bytes(&opened[0].bytes, "application/json", &mut None)?.sha256();
    let version = |version| {
        decision(
            id,
            "human",
            &ApprovalDecisionFile::new("human", &sha, version, true, None).unwrap(),
        )
    };
    // The decision for version 1 was validated by its writer; a racing
    // settlement then moves the interaction to version 2. Re-binding the
    // verdict to the current version would admit a decision nobody wrote.
    indeterminate(&fixture, &conversation, id, &human).await?;
    assert!(matches!(
        trail.write_approval_decision(&version(1), &human).await,
        Err(Error::Conflict(_))
    ));
    assert!(
        trail
            .interaction_outcome(id, None)?
            .await?
            .is_none_or(|outcome| { matches!(outcome, InteractionOutcome::Indeterminate { .. }) })
    );
    assert_eq!(approvals(&files).await?.len(), 1);
    // The writer's decision for the current version is the one admitted.
    trail.write_approval_decision(&version(2), &human).await?;
    assert!(matches!(
        trail.interaction_outcome(id, None)?.await?,
        Some(InteractionOutcome::Approved {})
    ));
    let published = approvals(&files).await?;
    let body = ApprovalDecisionFile::decode(
        &published
            .iter()
            .find(|file| file.key == version(2).key)
            .unwrap()
            .bytes,
        4096,
    )?;
    assert!(body.binds(&sha, 2));
    Ok(())
}
