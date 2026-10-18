use super::*;
use crate::{
    TaskId,
    conversation::{ContentGrant, Limits},
    goal::GoalContinuationPolicy,
};
use acyclic_stream::{StreamPath, StreamProvider};
use futures::TryStreamExt as _;
use prost::Message as _;
use serde::Deserialize;

#[derive(Deserialize)]
struct Case {
    name: String,
    enabled: Option<bool>,
    cap: Option<u32>,
    writes: Vec<Option<String>>,
    tasks: usize,
    calls: usize,
    final_goal: Option<String>,
    panel: Vec<String>,
    initial_goal: Option<String>,
    path: String,
    #[serde(default)]
    unreachable: bool,
    private_mount: Option<bool>,
}

#[cfg(feature = "tui")]
mod rendering;

#[tokio::test]
async fn local_goal_scenarios_follow_ordinary_frames_and_files() -> Result<()> {
    futures::future::try_join_all(cases()?.into_iter().map(run)).await?;
    Ok(())
}

fn cases() -> Result<Vec<Case>> {
    let cases: Vec<Case> =
        serde_json::from_str(include_str!("goals.json")).map_err(super::super::storage)?;
    #[cfg(feature = "native-execution")]
    let cases = {
        let mounted: Vec<Case> = serde_json::from_str(include_str!("goals/mounted.json"))
            .map_err(super::super::storage)?;
        cases.into_iter().chain(mounted).collect()
    };
    Ok(cases)
}

async fn run(case: Case) -> Result<()> {
    assert!(
        !case.panel.is_empty(),
        "scenario must declare rendering expectations"
    );
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let project = volume(directory.path()).await?;
    let script = script(&case);
    let provider = scripted(script);
    let config = || goal_config(&case, directory.path(), project.clone(), provider.clone());
    let harness = LocalHarness::open(config()?).await?;
    if let Some(initial) = &case.initial_goal {
        write_goal(&harness, initial).await?;
    }
    let stream = crate::test_support::local_task_stream(&harness);
    let operation = OperationId::new();
    let Admission::Accepted(crate::runtime::RuntimeTask::Durable { task_id, .. }) =
        harness.submit(operation, "Start the objective").await?
    else {
        panic!("{}", case.name)
    };
    let mut followed = stream
        .follow(
            StreamPath::new("harness/v3/coordinator/events").map_err(super::super::storage)?,
            0,
        )
        .await
        .map_err(super::super::storage)?;
    let execution = async { check_output(&case, harness.resume(task_id).await) };
    let frames = async {
        let mut admitted = Vec::new();
        let mut completed = Vec::new();
        let mut revision = 0;
        while completed.len() < case.tasks {
            let record = followed
                .try_next()
                .await
                .map_err(super::super::storage)?
                .expect("live task frames ended");
            let frame = crate::wire::SchedulerEventEnvelope::decode(record.value.as_ref())
                .map_err(super::super::storage)?;
            assert_eq!(frame.revision, revision + 1, "{}", case.name);
            revision = frame.revision;
            let event: crate::scheduler::SchedulerEvent =
                serde_json::from_slice(&frame.canonical_event_json)
                    .map_err(super::super::storage)?;
            match event {
                crate::scheduler::SchedulerEvent::Declared { spec } => {
                    admitted.push(spec.operation_id)
                }
                crate::scheduler::SchedulerEvent::Completed { operation_id, .. } => {
                    completed.push(operation_id)
                }
                _ => {}
            }
        }
        assert_eq!(admitted.len(), case.tasks, "{}", case.name);
        assert_eq!(admitted, completed, "{}", case.name);
        assert_eq!(admitted[0], operation);
        Ok::<_, Error>(admitted)
    };
    let (_, admitted) = tokio::time::timeout(
        std::time::Duration::from_secs(8),
        futures::future::try_join(execution, frames),
    )
    .await
    .expect("goal scenario stalled")?;
    #[cfg(feature = "native-execution")]
    if case.tasks > 1 {
        let requests =
            serde_json::to_string(&provider.requests()).map_err(super::super::storage)?;
        assert!(requests.contains("Continue the active goal in .volumes/scratch/GOAL.md."));
    }
    assert_eq!(provider.requests().len(), case.calls, "{}", case.name);
    assert_eq!(provider.remaining(), 0, "{}", case.name);
    let grant = ContentGrant::verify_directory_read(
        &harness.verifier,
        &harness.signed,
        harness.scratch(),
        "",
        &mut None,
    )?;
    let read = harness
        .files
        .read_private_path(
            harness.scratch(),
            &grant,
            "GOAL.md",
            None,
            Limits::default().file_bytes,
            None,
        )
        .await;
    match &case.final_goal {
        Some(expected) => assert_eq!(read?.1.as_ref(), expected.as_bytes(), "{}", case.name),
        None => assert!(matches!(read, Err(Error::NotFound(_)))),
    }
    if let Some(Some(expected)) = case.writes.last().filter(|_| case.path == "GOAL.md") {
        let project = crate::filesystem::workspace_ref(
            harness.project.provider().clone(),
            &harness.project.storage_name(&mut None)?,
        )?;
        let bytes = harness
            .files
            .read(
                &project,
                None,
                "/GOAL.md",
                Limits::default().file_bytes,
                None,
            )
            .await?;
        assert_eq!(
            bytes.as_ref(),
            expected.as_bytes(),
            "stock writer's actual destination"
        );
    }
    if case.unreachable || declared_blocked(&case) {
        check_output(&case, harness.resume(task_id).await)?;
        assert_eq!(provider.requests().len(), case.calls);
    }
    // Reopen/replay never reissues model work or resets the finite allowance.
    drop(followed);
    drop(stream);
    drop(harness);
    // Disabling future automatic admissions does not discard an already
    // published ordinary continuation or change the result of its replay.
    let reopen_config = config()?.goal_continuations(None);
    let reopened = LocalHarness::open(reopen_config).await?;
    assert_eq!(
        reopened.resume(task_id).await?.text,
        format!("settle-{}", case.writes.len() - 1),
        "{}",
        case.name
    );
    assert_eq!(provider.requests().len(), case.calls, "{}", case.name);
    assert_eq!(admitted.len(), case.tasks);
    Ok(())
}

fn goal_config(
    case: &Case,
    directory: &Path,
    project: VolumeRef,
    provider: Arc<dyn ModelProvider>,
) -> Result<LocalHarnessConfig> {
    let policy = (case.enabled != Some(false)).then_some(GoalContinuationPolicy {
        max_continuations: case.cap.unwrap_or(32),
    });
    let config = volume_config(directory, project, provider)?.goal_continuations(policy);
    // Without bash the local host binds the stock project writer without a
    // scratch mount. Keep that real unreachable route covered after #503.
    Ok(if case.private_mount == Some(false) {
        config
            .without_default_tools()
            .tool(crate::tool::files::write()?)
    } else {
        config
    })
}

/// Whether the loop settles on a blocked GOAL.md that the agent or the cap wrote.
fn declared_blocked(case: &Case) -> bool {
    !case.unreachable
        && case.enabled != Some(false)
        && case.final_goal.as_deref().is_some_and(|goal| {
            goal.starts_with(
                "---
status: blocked
",
            )
        })
}

fn check_output(case: &Case, output: Result<crate::executor::TurnOutput>) -> Result<()> {
    if case.unreachable || declared_blocked(case) {
        let reason = if case.unreachable {
            crate::goal::GoalBlockedReason::TaskViewUnreachable
        } else {
            crate::goal::GoalBlockedReason::Declared
        };
        assert!(
            matches!(&output, Err(Error::GoalBlocked(actual)) if actual == &reason),
            "{}: {output:?}",
            case.name
        );
        let error = Error::GoalBlocked(reason);
        assert_eq!(error.code(), crate::wire::ErrorCode::Unsupported);
        assert!(!error.is_native_failure());
        // `-p` exits with the stable blocked code, never as completed.
        assert_eq!(
            crate::client::HeadlessOutcome::from_error(&error),
            crate::client::HeadlessOutcome::Blocked
        );
    } else {
        assert_eq!(
            output?.text,
            format!("settle-{}", case.writes.len() - 1),
            "{}",
            case.name
        );
    }
    Ok(())
}

fn script(case: &Case) -> Vec<ScriptedTurn> {
    let mut script = Vec::new();
    for (index, write) in case.writes.iter().enumerate() {
        if let Some(text) = write {
            script.push(ScriptedTurn::Events(vec![
                ScriptedEvent::Event(ModelEvent::ToolCall(crate::model::ToolCall::new(
                    format!("goal-{index}"),
                    "write".into(),
                    json!({"path":case.path, "text":text,
                        "media_type":"text/markdown", "display_name":"GOAL.md"}),
                ))),
                ScriptedEvent::Event(ModelEvent::Completed {
                    metadata: Value::Null,
                }),
            ]));
        }
        script.push(ScriptedTurn::Text(format!("settle-{index}")));
    }
    script
}

#[tokio::test]
async fn local_goal_settle_loses_to_a_new_user_admission() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let project = volume(directory.path()).await?;
    let provider = scripted([
        ScriptedTurn::Text("first".into()),
        ScriptedTurn::Text("user".into()),
    ]);
    let harness =
        LocalHarness::open(volume_config(directory.path(), project, provider.clone())?).await?;
    let first = OperationId::new();
    let Admission::Accepted(crate::runtime::RuntimeTask::Durable { task_id, .. }) =
        harness.submit(first, "first").await?
    else {
        panic!()
    };
    harness.resume_turn(task_id).await?;
    let publisher = crate::filesystem::FilesystemContentPublisher::new(
        harness.files.clone(),
        harness.scratch(),
        &harness.verifier,
        &harness.signed,
        Limits::default().file_bytes,
        None,
    )?;
    use crate::conversation::ContentPublisher;
    publisher
        .stage_once(
            OperationId::new(),
            "GOAL.md",
            b"Continue",
            "text/markdown",
            "GOAL.md",
            None,
        )?
        .await?;
    let user = OperationId::new();
    let Admission::Accepted(crate::runtime::RuntimeTask::Durable { task_id: next, .. }) =
        harness.submit(user, "user").await?
    else {
        panic!()
    };
    assert_eq!(harness.continue_goal(task_id, first).await?, None);
    assert!(matches!(
        harness.submit(OperationId::new(), "duplicate").await?,
        Admission::Rejected { .. }
    ));
    assert_eq!(next, TaskId::from_bytes(user.into_bytes()));
    assert_eq!(provider.requests().len(), 1);
    Ok(())
}

pub(super) async fn write_goal(harness: &LocalHarness, contents: &str) -> Result<()> {
    use crate::conversation::ContentPublisher;
    crate::filesystem::FilesystemContentPublisher::new(
        harness.files.clone(),
        harness.scratch(),
        &harness.verifier,
        &harness.signed,
        Limits::default().file_bytes,
        None,
    )?
    .stage_once(
        OperationId::new(),
        "GOAL.md",
        contents.as_bytes(),
        "text/markdown",
        "GOAL.md",
        None,
    )?
    .await?;
    Ok(())
}

#[tokio::test]
async fn local_goal_recovers_staged_input_before_reading_an_edited_goal() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let project = volume(directory.path()).await?;
    let provider = scripted([
        ScriptedTurn::Text("root".into()),
        ScriptedTurn::Text("recovered".into()),
    ]);
    let config = || -> Result<LocalHarnessConfig> {
        Ok(
            volume_config(directory.path(), project.clone(), provider.clone())?.goal_continuations(
                Some(GoalContinuationPolicy {
                    max_continuations: 1,
                }),
            ),
        )
    };
    let harness = LocalHarness::open(config()?).await?;
    let root = OperationId::new();
    let Admission::Accepted(crate::runtime::RuntimeTask::Durable { task_id, .. }) =
        harness.submit(root, "start").await?
    else {
        panic!()
    };
    harness.resume_turn(task_id).await?;
    write_goal(&harness, "original objective").await?;
    let continuation = crate::goal::continuation_operation(task_id, root);
    // Interrupt the production path after immutable inputs, before HEAD publication.
    let original = harness
        .stage_turn(
            continuation,
            "Continue the active goal in GOAL.md.\n\noriginal objective",
        )
        .await?;
    write_goal(&harness, "changed objective").await?;
    // Negative control: rebuilding from the edited file reproduces the old failure.
    assert!(matches!(
        harness
            .stage_turn(
                continuation,
                "Continue the active goal in GOAL.md.\n\nchanged objective",
            )
            .await,
        Err(Error::Conflict(reason)) if reason.contains("upload identity belongs to a different request")
    ));
    drop(harness);
    let harness = LocalHarness::open(config()?).await?;
    assert_eq!(harness.resume(task_id).await?.text, "recovered");
    assert_eq!(
        harness.recover_turn_inputs(continuation).await?,
        Some(original)
    );
    assert_eq!(provider.requests().len(), 2);
    assert_eq!(provider.remaining(), 0);
    let request = serde_json::to_string(&provider.requests()[1]).map_err(super::super::storage)?;
    assert!(request.contains("original objective"));
    assert!(!request.contains("changed objective"));
    Ok(())
}

#[tokio::test]
async fn local_goal_resuming_an_old_continuation_drives_its_pending_successor() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let project = volume(directory.path()).await?;
    let provider = scripted([
        ScriptedTurn::Text("root".into()),
        ScriptedTurn::Text("first".into()),
        ScriptedTurn::Text("second".into()),
    ]);
    let config = || -> Result<LocalHarnessConfig> {
        Ok(
            volume_config(directory.path(), project.clone(), provider.clone())?.goal_continuations(
                Some(GoalContinuationPolicy {
                    max_continuations: 2,
                }),
            ),
        )
    };
    let harness = LocalHarness::open(config()?).await?;
    let root = OperationId::new();
    let Admission::Accepted(crate::runtime::RuntimeTask::Durable { task_id, .. }) =
        harness.submit(root, "start").await?
    else {
        panic!()
    };
    harness.resume_turn(task_id).await?;
    write_goal(&harness, "Keep going").await?;
    // Retained inputs must still recover even if the task view becomes unreachable.
    let first_operation = crate::goal::continuation_operation(task_id, root);
    harness
        .stage_turn(first_operation, "first retained continuation")
        .await?;
    let first = harness.continue_goal(task_id, root).await?.unwrap();
    assert_eq!(harness.resume_turn(first).await?.text, "first");
    let second_operation = crate::goal::continuation_operation(task_id, first_operation);
    harness
        .stage_turn(second_operation, "second retained continuation")
        .await?;
    let second = harness.continue_goal(first, root).await?.unwrap();
    // Negative control: the former wrapper used the resumed task as its root.
    assert_eq!(harness.resume_turn(first).await?.text, "first");
    assert_eq!(
        harness
            .continue_goal(first, OperationId::from_bytes(first.into_bytes()))
            .await?,
        None
    );
    assert!(harness.runtime.task_host().outcome(second).await?.is_none());
    assert_eq!(provider.requests().len(), 2);
    drop(harness);
    let harness = LocalHarness::open(config()?).await?;
    assert_eq!(harness.resume(first).await?.text, "second");
    assert!(matches!(
        harness.runtime.task_host().outcome(second).await?,
        Some(crate::Outcome::Succeeded(_))
    ));
    assert_eq!(harness.resume(first).await?.text, "second");
    assert_eq!(provider.requests().len(), 3);
    assert_eq!(provider.remaining(), 0);
    Ok(())
}
