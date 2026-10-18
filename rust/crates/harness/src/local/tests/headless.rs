//! Actual local admission through the shared headless entry points.
use super::*;
use crate::client::{HeadlessOutcome, connect_local, local_prompt, run_headless, run_jsonl};
use std::io::{self, Write};

async fn local() -> Result<(
    tempfile::TempDir,
    Arc<LocalHarness>,
    Arc<ScriptedModelProvider>,
)> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let provider = text();
    let harness = Arc::new(
        LocalHarness::open(
            crate::test_support::local_harness_config(
                directory.path(),
                (model()?, provider.clone()),
            )?
            .without_default_tools(),
        )
        .await?,
    );
    Ok((directory, harness, provider))
}
async fn close(harness: Arc<LocalHarness>) -> Result<()> {
    Arc::try_unwrap(harness)
        .map_err(|_| Error::Conflict("headless client retained its host".into()))?
        .close()
        .await
}
fn final_text(output: &[u8]) -> String {
    let changes = std::str::from_utf8(output)
        .unwrap()
        .lines()
        .skip(1)
        .map(|line| crate::client::jsonl::parse_change(line).unwrap());
    changes
        .flat_map(|change| change.file.into_iter().chain(change.files))
        .filter(|file| {
            matches!(
                file.key,
                crate::task_volume::FileKey::Session {
                    file: crate::task_volume::TaskFile::ExecutionResult(_),
                    ..
                }
            )
        })
        .map(|file| {
            serde_json::from_slice::<Value>(&file.bytes).unwrap()["ok"]["text"]
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect::<Vec<_>>()
        .join("")
}

#[tokio::test]
async fn prompt_runs_through_files_and_reopening_does_not_resend() -> Result<()> {
    let (_directory, harness, provider) = local().await?;
    let operation = OperationId::new();
    let (mut client, target) = connect_local(harness.clone(), operation).await?;
    assert_eq!(provider.requests().len(), 0);
    let command = local_prompt(&harness, operation, "hello").await?;
    let mut output = Vec::new();
    assert_eq!(
        run_headless(&mut client, command.clone(), target, &mut output).await?,
        HeadlessOutcome::Completed
    );
    assert_eq!(final_text(&output), "done");
    assert_eq!(provider.requests().len(), 1);
    drop(client);
    let (mut client, target) = connect_local(harness.clone(), operation).await?;
    let mut restored = Vec::new();
    assert_eq!(
        run_headless(&mut client, command, target, &mut restored).await?,
        HeadlessOutcome::Completed
    );
    assert_eq!(provider.requests().len(), 1);
    drop(client);
    close(harness).await
}

#[tokio::test]
async fn jsonl_eof_drains_the_admitted_file_request() -> Result<()> {
    let (_directory, harness, provider) = local().await?;
    let operation = OperationId::new();
    let command = local_prompt(&harness, operation, "hello").await?;
    let line = crate::client::jsonl::request(&command)?;
    let (mut client, target) = connect_local(harness.clone(), operation).await?;
    let mut output = Vec::new();
    let input = futures::stream::iter([Ok(line)]);
    assert_eq!(
        run_jsonl(&mut client, target, input, &mut output).await?,
        HeadlessOutcome::Completed
    );
    assert_eq!(final_text(&output), "done");
    assert_eq!(provider.requests().len(), 1);
    drop(client);
    close(harness).await
}

struct RejectOutcome {
    rejected: bool,
}
impl Write for RejectOutcome {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if !self.rejected
            && bytes
                .windows(b"ExecutionResult".len())
                .any(|part| part == b"ExecutionResult")
        {
            self.rejected = true;
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
#[tokio::test]
async fn stdout_failure_keeps_the_exact_pending_outcome_and_owner() -> Result<()> {
    let (_directory, harness, provider) = local().await?;
    let operation = OperationId::new();
    let command = local_prompt(&harness, operation, "hello").await?;
    let (mut client, target) = connect_local(harness.clone(), operation).await?;
    let mut broken = RejectOutcome { rejected: false };
    assert!(
        run_headless(&mut client, command.clone(), target, &mut broken)
            .await
            .is_err()
    );
    assert!(broken.rejected);
    assert_eq!(provider.requests().len(), 1);
    let mut output = Vec::new();
    assert_eq!(
        run_headless(&mut client, command, target, &mut output).await?,
        HeadlessOutcome::Completed
    );
    // The initial snapshot was already published; retry emits the pending change.
    let change = crate::client::jsonl::parse_change(
        std::str::from_utf8(output.split(|byte| *byte == b'\n').next().unwrap()).unwrap(),
    )?;
    assert!(matches!(
        change.file.unwrap().key,
        crate::task_volume::FileKey::Session {
            file: crate::task_volume::TaskFile::ExecutionResult(_),
            ..
        }
    ));
    assert_eq!(provider.requests().len(), 1);
    drop(client);
    close(harness).await
}

#[tokio::test]
async fn opening_and_empty_jsonl_do_not_start_a_model() -> Result<()> {
    let (_directory, harness, provider) = local().await?;
    let (mut client, target) = connect_local(harness.clone(), OperationId::new()).await?;
    assert_eq!(provider.requests().len(), 0);
    assert!(matches!(
        run_jsonl(
            &mut client,
            target,
            futures::stream::empty(),
            &mut Vec::new()
        )
        .await,
        Err(Error::NotFound(_))
    ));
    assert_eq!(provider.requests().len(), 0);
    drop(client);
    close(harness).await
}

#[tokio::test]
async fn task_files_share_the_snapshot_follow_and_reopen_cut() -> Result<()> {
    use crate::task_volume::{Component, FileKey, FileValue, Query, TaskFile, ToolFile};
    let (_directory, harness, provider) = local().await?;
    let operation = OperationId::new();
    let task = Component::new(crate::runtime::task_id_for_admission(operation).to_string())?;
    let volume = &harness.task_files().inner;
    let file = |task: Component, call: &str| -> Result<FileValue> {
        Ok(FileValue::new(
            FileKey::Task {
                task,
                file: TaskFile::Tool {
                    turn: 0,
                    call: Component::new(call)?,
                    file: ToolFile::Result,
                },
            },
            call.as_bytes().to_vec(),
        ))
    };
    let before = file(task.clone(), "before")?;
    let after = file(task.clone(), "after")?;
    let foreign = file(Component::new("unrelated")?, "foreign")?;
    let cut = volume.snapshot(&Query::Root, 4096).await?;
    let cut = volume.append(cut.generation, &before).await?;
    let cut = volume.append(cut, &foreign).await?;
    let (mut client, target) = connect_local(harness.clone(), operation).await?;
    volume.append(cut, &after).await?;
    let mut output = Vec::new();
    assert_eq!(
        run_headless(
            &mut client,
            local_prompt(&harness, operation, "hello").await?,
            target,
            &mut output
        )
        .await?,
        HeadlessOutcome::Completed
    );
    let mut lines = std::str::from_utf8(&output).unwrap().lines();
    let initial: Value = serde_json::from_str(lines.next().unwrap()).unwrap();
    let head_keys: Vec<FileKey> = initial[1]["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|file| serde_json::from_value(file["key"].clone()).unwrap())
        .collect();
    assert!(head_keys.contains(&before.key));
    assert!(!head_keys.contains(&foreign.key));
    let changes: Vec<crate::task_volume::Change> = lines
        .map(|line| crate::client::jsonl::parse_change(line).unwrap())
        .collect();
    let keys: Vec<_> = changes
        .iter()
        .flat_map(|change| change.file.iter().chain(&change.files))
        .map(|file| &file.key)
        .collect();
    assert!(keys.contains(&&after.key));
    assert!(!keys.contains(&&foreign.key));
    drop(client);
    let (mut client, target) = connect_local(harness.clone(), operation).await?;
    let mut restored = Vec::new();
    run_headless(
        &mut client,
        local_prompt(&harness, operation, "hello").await?,
        target,
        &mut restored,
    )
    .await?;
    let initial: Value = serde_json::from_str(
        std::str::from_utf8(&restored)
            .unwrap()
            .lines()
            .next()
            .unwrap(),
    )
    .unwrap();
    let head_keys: Vec<FileKey> = initial[1]["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|file| serde_json::from_value(file["key"].clone()).unwrap())
        .collect();
    assert!(head_keys.contains(&before.key));
    assert!(head_keys.contains(&after.key));
    assert!(!head_keys.contains(&foreign.key));
    assert_eq!(provider.requests().len(), 1);
    drop(client);
    close(harness).await
}

#[tokio::test]
async fn restored_jsonl_checks_ready_input_without_waiting_for_a_scheduled_frame() -> Result<()> {
    let (_directory, harness, provider) = local().await?;
    let operation = OperationId::new();
    let (mut client, target) = connect_local(harness.clone(), operation).await?;
    run_headless(
        &mut client,
        local_prompt(&harness, operation, "hello").await?,
        target,
        &mut Vec::new(),
    )
    .await?;
    drop(client);
    for ready in [false, true] {
        let (mut client, target) = connect_local(harness.clone(), operation).await?;
        let input = Box::pin(futures::stream::once(async move {
            if !ready {
                tokio::task::yield_now().await;
            }
            Ok("not JSON".to_owned())
        }));
        let outcome = run_jsonl(&mut client, target, input, &mut Vec::new()).await;
        if ready {
            assert!(matches!(outcome, Err(Error::Invalid(_))));
        } else {
            assert_eq!(outcome?, HeadlessOutcome::Completed);
        }
        assert_eq!(provider.requests().len(), 1);
        drop(client);
    }
    close(harness).await
}

#[tokio::test]
async fn disconnect_preserves_an_unfinished_admission_and_releases_its_mounts() -> Result<()> {
    let (directory, harness, provider) = local().await?;
    let operation = OperationId::new();
    let task = match harness.submit(operation, "hello").await? {
        Admission::Accepted(crate::runtime::RuntimeTask::Durable { task_id, .. }) => task_id,
        _ => panic!("expected durable admission"),
    };
    assert!(provider.requests().is_empty());
    // Retain the actual mount owners: disconnect, rather than a fortunate last
    // Arc drop, must free their destinations before the next process opens.
    let retained = harness
        .workspace
        .as_ref()
        .map(|workspace| workspace.mounts.clone());
    crate::client::disconnect_local(harness).await?;
    let reopened = LocalHarness::open(
        crate::test_support::local_harness_config(directory.path(), (model()?, provider.clone()))?
            .without_default_tools(),
    )
    .await?;
    match reopened.submit(operation, "hello").await? {
        Admission::Accepted(crate::runtime::RuntimeTask::Durable { task_id, .. }) => {
            assert_eq!(task_id, task)
        }
        _ => panic!("expected the same durable admission"),
    }
    assert_eq!(reopened.resume(task).await?.text, "done");
    assert_eq!(provider.requests().len(), 1);
    reopened.close().await?;
    drop(retained);
    Ok(())
}

#[tokio::test]
async fn workspace_launch_identity_reuses_only_unfinished_requests() -> Result<()> {
    let (_directory, harness, provider) = local().await?;
    let prompt = "resume private request";
    let operation = crate::client::local_identity(&harness, Some(prompt), None).await?;
    assert_eq!(
        operation,
        crate::client::local_identity(&harness, Some(prompt), None).await?
    );
    assert_ne!(
        operation,
        crate::client::local_identity(&harness, Some("another"), None).await?
    );
    let interactive = crate::client::local_identity(&harness, None, None).await?;
    assert_eq!(
        interactive,
        crate::client::local_identity(&harness, None, None).await?
    );
    let explicit = OperationId::new();
    assert_eq!(
        explicit,
        crate::client::local_identity(&harness, Some(prompt), Some(explicit)).await?
    );
    assert!(
        provider.requests().is_empty(),
        "identity lookup cannot admit work"
    );
    let (mut client, target) = connect_local(harness.clone(), operation).await?;
    let request = local_prompt(&harness, operation, prompt).await?;
    assert_eq!(
        run_headless(&mut client, request, target, &mut Vec::new()).await?,
        HeadlessOutcome::Completed
    );
    drop(client);
    assert_ne!(
        operation,
        crate::client::local_identity(&harness, Some(prompt), None).await?
    );
    assert_eq!(provider.requests().len(), 1);
    close(harness).await
}

#[tokio::test]
async fn prompt_just_under_request_bound_has_independent_observation_budget() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    // This boundary exercises request bytes, not the small-context fixture's
    // deliberately conservative one-byte-per-token upper bound.
    let provider = Arc::new(
        ScriptedModelProvider::new([ScriptedTurn::Text("done".into())]).with_context_capacity(
            ModelContextCapacity {
                context_tokens: 131_072,
                output_tokens: 4096,
            },
        ),
    );
    let harness = Arc::new(
        LocalHarness::open(
            crate::test_support::local_harness_config(
                directory.path(),
                (model()?, provider.clone()),
            )?
            .without_default_tools(),
        )
        .await?,
    );
    let operation = OperationId::new();
    let one = crate::client::prepare_local_prompt(operation, "x")?;
    let mut claim = one.clone();
    if let crate::task_volume::FileKey::Session { file, .. } = &mut claim.key {
        *file = crate::task_volume::TaskFile::ExecutionClaim(crate::task_volume::Component::new(
            operation.to_string(),
        )?);
    }
    let framing = crate::task_volume::record_bytes(&claim)? - 1;
    let text = "x".repeat(acyclic_stream::MAX_RECORD_BYTES - framing - 1);
    let request = local_prompt(&harness, operation, &text).await?;
    claim.bytes = request.bytes.clone();
    assert_eq!(
        crate::task_volume::record_bytes(&claim)?,
        acyclic_stream::MAX_RECORD_BYTES - 1
    );
    assert!(crate::client::prepare_local_prompt(operation, &(text.clone() + "xx")).is_err());
    let (mut client, target) = connect_local(harness.clone(), operation).await?;
    assert_eq!(
        run_headless(&mut client, request, target, &mut Vec::new()).await?,
        HeadlessOutcome::Completed
    );
    assert_eq!(provider.requests().len(), 1);
    drop(client);
    close(harness).await
}

/// Whether a recorded model request carried the default GOAL.md guidance.
fn goal_guided(request: &crate::model::ModelRequest) -> bool {
    request.messages.iter().any(|message| {
        matches!(&message.content, crate::model::ModelContent::Text(text)
            if text.starts_with("Keep the current goal in GOAL.md"))
    })
}

#[tokio::test]
async fn goal_guidance_and_the_blocked_exit_follow_goal_md() -> Result<()> {
    for (goal, prompt, guided, outcome) in [
        // One-shot prompts get no goal framing.
        (
            None,
            "Reply with exactly PONG.",
            false,
            HeadlessOutcome::Completed,
        ),
        (
            None,
            "Work in goal mode on the release.",
            true,
            HeadlessOutcome::Completed,
        ),
        // Negative control: a goal that settled done completes.
        (
            Some("---\nstatus: done\n---\nShip"),
            "hello",
            true,
            HeadlessOutcome::Completed,
        ),
        (
            Some("---\nstatus: blocked\nreason: waiting\n---\nShip"),
            "hello",
            true,
            HeadlessOutcome::Blocked,
        ),
    ] {
        let (_directory, harness, provider) = local().await?;
        if let Some(goal) = goal {
            super::goals::write_goal(&harness, goal).await?;
        }
        let operation = OperationId::new();
        let (mut client, target) = connect_local(harness.clone(), operation).await?;
        let command = local_prompt(&harness, operation, prompt).await?;
        let mut output = Vec::new();
        let settled = run_headless(&mut client, command, target, &mut output).await?;
        assert_eq!(settled, outcome, "{prompt} with {goal:?}");
        assert_eq!(
            settled.exit_code(),
            if goal.is_some_and(|goal| goal.contains("blocked")) {
                3
            } else {
                0
            }
        );
        let requests = provider.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(goal_guided(&requests[0]), guided, "{prompt} with {goal:?}");
        drop(client);
        close(harness).await?;
    }
    Ok(())
}
