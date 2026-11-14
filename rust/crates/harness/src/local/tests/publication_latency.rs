//! Timings include inbox admission, execution, result publication and follow delivery.
use super::*;
use crate::model::{
    ModelContent, ModelContentPart, ModelDispatch, ModelEvent, ModelRequest, ModelStream,
    PreparedModelRequest, ToolCall, ToolResultContent,
};
use crate::test_support::ScriptedEvent;
use acyclic_fs::{CancellationToken, WorkBudget};
use futures::TryStreamExt as _;
use std::sync::Mutex;

struct WritingModel {
    counter: Arc<ScriptedModelProvider>,
    calls: Mutex<Vec<ToolCall>>,
    requests: Mutex<Vec<ModelRequest>>,
    next_child: Mutex<
        Option<(
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        )>,
    >,
}

impl WritingModel {
    fn last_request(&self) -> Option<ModelRequest> {
        self.requests.lock().unwrap().last().cloned()
    }
}

impl ModelProvider for WritingModel {
    fn context_capacity(&self, model: &Model) -> Result<ModelContextCapacity> {
        self.counter.context_capacity(model)
    }

    fn count_tokens<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
        ceiling: u64,
    ) -> ModelStream<'a> {
        self.counter.count_tokens(request, ceiling)
    }

    fn generate<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
        dispatch: ModelDispatch,
    ) -> ModelStream<'a> {
        self.requests
            .lock()
            .unwrap()
            .push(request.request().clone());
        if let Some((entered, release)) = self.next_child.lock().unwrap().take() {
            let _ = entered.send(());
            return ModelStream::new(Box::pin(
                futures::stream::once(async move {
                    release
                        .await
                        .map_err(|_| Error::Conflict("child model gate closed".into()))?;
                    Ok(ScriptedTurn::Text("ready".into()).stream())
                })
                .try_flatten(),
            ));
        }
        let calls = self.calls.lock().unwrap().clone();
        let turn = if dispatch.step == 0 && !calls.is_empty() {
            let mut events = calls
                .into_iter()
                .map(|call| ScriptedEvent::Event(ModelEvent::ToolCall(call)))
                .collect::<Vec<_>>();
            events.push(ScriptedEvent::Event(ModelEvent::Completed {
                metadata: json!({}),
            }));
            ScriptedTurn::Events(events)
        } else {
            ScriptedTurn::Text("ready".into())
        };
        ModelStream::new(turn.stream())
    }
}

#[tokio::test]
async fn concurrent_model_writes_settle_project_and_home_payloads() -> Result<()> {
    let backing = tempfile::tempdir().map_err(crate::local::storage)?;
    acyclic_native_runtime::initialize_native_io_services().map_err(crate::local::storage)?;
    acyclic_fs::initialize_native_mount_services().map_err(crate::local::storage)?;
    let checkout = backing.path().join("checkout");
    std::fs::create_dir(&checkout).map_err(crate::local::storage)?;
    for file in 0..128 {
        std::fs::write(checkout.join(format!("source-{file:03}.txt")), b"source\n")
            .map_err(crate::local::storage)?;
    }
    let provider = Arc::new(WritingModel {
        counter: scripted([]),
        calls: Mutex::new(Vec::new()),
        requests: Mutex::new(Vec::new()),
        next_child: Mutex::new(None),
    });
    let harness = Arc::new(
        open_scripted(
            crate::test_support::local_harness_config(&checkout, (model()?, provider.clone()))?
                .state_directory(backing.path().join("state")),
        )
        .await?,
    );
    let (agents, root) = harness.agents().await?;
    let (agents, root) = (Arc::clone(agents), root.clone());
    let connection = harness.connect().await?;
    let cancellation = CancellationToken::new();
    let mut writers: Vec<ObservedConversation> = Vec::new();
    let mut children: Vec<ObservedConversation> = Vec::new();
    let mut releases = Vec::new();
    let mut projects = Vec::new();
    let mut homes = Vec::new();
    for index in 0..8 {
        provider.calls.lock().unwrap().clear();
        let name = format!("writer-{index}");
        let acyclic_harness_client::AgentReply::Spawned { agent: child, .. } = connection
            .execute(&acyclic_harness_client::AgentCommand::Spawn {
                parent: root.clone(),
                id: crate::agent::IntentId::try_from(name.as_str())
                    .map_err(crate::local::storage)?,
                brief: ModelContent::Text("prepare this workspace".into()),
                role: "worker".into(),
            })
            .await?
        else {
            return Err(Error::Conflict("spawn did not return its child".into()));
        };
        let conversation = Conversation::open(connection.select(child.clone()).await?);
        let mut observed = ObservedConversation::observe(conversation, None).await?;
        // Consume the child's original brief before changing the model script.
        loop {
            match observed.observer.next().await? {
                Event::Turn { entry, .. } => {
                    assert!(matches!(entry.input, crate::agent::TurnInput::Brief { .. }));
                    assert_eq!(entry.outcome, crate::agent::TurnOutcome::Completed);
                    break;
                }
                Event::Status(status)
                    if matches!(status.phase, crate::agent::Phase::Done { .. }) =>
                {
                    return Err(Error::Conflict(format!("child finished: {status:?}")));
                }
                _ => {}
            }
        }
        // Every measured writer is a parent with a live child model request,
        // rather than a leaf that never crosses a parent effect boundary.
        let (entered, active) = tokio::sync::oneshot::channel();
        let (release, held) = tokio::sync::oneshot::channel();
        *provider.next_child.lock().unwrap() = Some((entered, held));
        let acyclic_harness_client::AgentReply::Spawned {
            agent: descendant, ..
        } = connection
            .execute(&acyclic_harness_client::AgentCommand::Spawn {
                parent: child.clone(),
                id: crate::agent::IntentId::try_from("reader").map_err(crate::local::storage)?,
                brief: ModelContent::Text("retain this independent view".into()),
                role: "worker".into(),
            })
            .await?
        else {
            return Err(Error::Conflict(
                "spawn did not return its descendant".into(),
            ));
        };
        active.await.map_err(crate::local::storage)?;
        children.push(
            ObservedConversation::observe(
                Conversation::open(connection.select(descendant).await?),
                None,
            )
            .await?,
        );
        releases.push(release);
        *provider.calls.lock().unwrap() = vec![
            ToolCall::new(
                "prepare-project".into(),
                "write".into(),
                json!({"path": "reports/prepared.txt", "content": "baseline"}),
            ),
            ToolCall::new(
                "prepare-home".into(),
                "write".into(),
                json!({"path": "volume:///home/reports/prepared.txt", "content": "baseline"}),
            ),
        ];
        let warm = observed
            .send(&format!("prepare-{index}"), "prepare this workspace")
            .await?;
        assert_eq!(warm.outcome, crate::agent::TurnOutcome::Completed);
        assert_eq!(
            returned_path(&provider, "prepare-project")?,
            "reports/prepared.txt"
        );
        assert_eq!(
            returned_path(&provider, "prepare-home")?,
            "volume:///home/reports/prepared.txt"
        );
        let state = agents.state(&child).await.map_err(crate::local::storage)?;
        let volume = |name: &str| -> Result<VolumeRef> {
            Ok(state
                .volumes
                .iter()
                .find(|binding| binding.name == name)
                .ok_or_else(|| Error::NotFound(format!("child {name} volume")))?
                .volume()?
                .clone())
        };
        let project = volume("project")?;
        let home = volume("home")?;
        assert_ne!(project, home);
        assert!(!projects.contains(&project), "private child projects");
        assert!(!homes.contains(&home), "private child homes");
        projects.push(project);
        homes.push(home);
        writers.push(observed);
    }
    for count in [1, 8] {
        for prefix in ["volume:///home/", ""] {
            let call = format!("write-{count}-{}", prefix.is_empty());
            let path = format!("{prefix}reports/settled-{count}.txt");
            let content = format!("writers {count}\n");
            *provider.calls.lock().unwrap() = vec![ToolCall::new(
                call.clone(),
                "write".into(),
                json!({"path": path, "content": content}),
            )];
            let request_start = provider.requests.lock().unwrap().len();
            let barrier = tokio::sync::Barrier::new(count);
            let results = futures::future::join_all(writers[..count].iter_mut().enumerate().map(
                |(index, writer)| {
                    let (barrier, call) = (&barrier, &call);
                    async move {
                        barrier.wait().await;
                        let started = std::time::Instant::now();
                        let conversation = Arc::clone(&writer.conversation);
                        let inbox = conversation
                            .send(&format!("{call}-{index}"), "publish the file")
                            .await?;
                        let (settled, completed) = tokio::join!(
                            async {
                                let settled = writer.settled(inbox).await?;
                                assert_eq!(settled.outcome, crate::agent::TurnOutcome::Completed);
                                Ok::<_, Error>(started.elapsed())
                            },
                            tool_latency(&conversation, inbox, call, started),
                        );
                        Ok::<_, Error>((settled?, completed?))
                    }
                },
            ))
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
            let mut elapsed = results.iter().map(|result| result.0).collect::<Vec<_>>();
            elapsed.sort_unstable();
            let mut completed = results
                .iter()
                .map(|result| result.1.completed)
                .collect::<Vec<_>>();
            completed.sort_unstable();
            let mut admission = results
                .iter()
                .map(|result| result.1.admission)
                .collect::<Vec<_>>();
            admission.sort_unstable();
            let mut execution = results
                .iter()
                .map(|result| result.1.execution)
                .collect::<Vec<_>>();
            execution.sort_unstable();
            eprintln!(
                "TOOL_ADMISSION_LATENCY family=parent_child volume={} writers={count} p50_us={} p99_us={} execution_p50_us={} execution_p99_us={}",
                if prefix.is_empty() { "project" } else { "home" },
                percentile(&admission, 50).as_micros(),
                percentile(&admission, 99).as_micros(),
                percentile(&execution, 50).as_micros(),
                percentile(&execution, 99).as_micros(),
            );
            eprintln!(
                "TOOL_TURN_LATENCY volume={} writers={count} p50_us={} max_us={} tool_completed_observed_p50_us={} tool_completed_observed_max_us={}",
                if prefix.is_empty() { "project" } else { "home" },
                elapsed[elapsed.len() / 2].as_micros(),
                elapsed.last().expect("writers").as_micros(),
                completed[completed.len() / 2].as_micros(),
                completed.last().expect("writers").as_micros()
            );
            // Inspect model delivery and committed payloads after all timers stop.
            let mut delivered = 0;
            for request in provider.requests.lock().unwrap().iter().skip(request_start) {
                if let Some(returned) = result_path(request, &call)? {
                    assert_eq!(returned, path);
                    delivered += 1;
                }
            }
            assert_eq!(delivered, count, "every child model receives its result");
            for index in 0..count {
                let volume = if prefix.is_empty() {
                    &projects[index]
                } else {
                    &homes[index]
                };
                let workspace = harness
                    .workspace
                    .as_ref()
                    .expect("source-backed checkout")
                    .filesystem
                    .open_workspace(
                        volume.storage_name(&mut None)?,
                        WorkBudget::UNBOUNDED,
                        &cancellation,
                    )
                    .await
                    .map_err(|failure| crate::local::storage(failure.error))?
                    .value;
                let bytes = workspace
                    .head(WorkBudget::UNBOUNDED, &cancellation)
                    .await
                    .map_err(|failure| crate::local::storage(failure.error))?
                    .value
                    .read(
                        &format!("/reports/settled-{count}.txt"),
                        64,
                        WorkBudget::UNBOUNDED,
                        &cancellation,
                    )
                    .await
                    .map_err(|failure| crate::local::storage(failure.error))?
                    .value;
                assert_eq!(bytes.as_ref(), content.as_bytes());
            }
        }
    }
    for release in releases {
        release
            .send(())
            .map_err(|_| Error::Conflict("child request ended before release".into()))?;
    }
    for child in &mut children {
        loop {
            match child.observer.next().await? {
                Event::Turn { entry, .. } => {
                    assert!(matches!(entry.input, crate::agent::TurnInput::Brief { .. }));
                    assert_eq!(entry.outcome, crate::agent::TurnOutcome::Completed);
                    break;
                }
                Event::Status(status)
                    if matches!(status.phase, crate::agent::Phase::Done { .. }) =>
                {
                    return Err(Error::Conflict(format!("child finished: {status:?}")));
                }
                _ => {}
            }
        }
    }
    drop(children);
    drop(writers);
    drop(connection);
    drop(agents);
    Arc::try_unwrap(harness)
        .map_err(|_| Error::Conflict("measurement still owns its harness".into()))?
        .disconnect()
        .await?;
    Ok(())
}

struct ToolLatency {
    completed: std::time::Duration,
    admission: std::time::Duration,
    execution: std::time::Duration,
}

fn percentile(sorted: &[std::time::Duration], percent: usize) -> std::time::Duration {
    sorted[(sorted.len() * percent).div_ceil(100) - 1]
}

async fn tool_latency(
    conversation: &Conversation,
    inbox: u64,
    call: &str,
    started: std::time::Instant,
) -> Result<ToolLatency> {
    let mut after = 0;
    let mut model_output = None;
    let mut accepted = None;
    loop {
        let query = conversation.execution_page(inbox, after, true).await?;
        let settled = query.settled;
        let page = query
            .page
            .filter(|_| query.input == inbox)
            .ok_or_else(|| Error::Conflict("original tool journal is absent".into()))?;
        for record in page.items {
            let at = record.committed_at_micros;
            let timestamp = || {
                at.ok_or_else(|| Error::Conflict("original publication timestamp is absent".into()))
            };
            match record.event {
                crate::executor::ExecutionEvent::ModelOutput { step, .. } => {
                    model_output = Some((step, timestamp()?));
                }
                crate::executor::ExecutionEvent::ToolStarted {
                    step, ref call_id, ..
                } if call_id == call => {
                    let (model_step, ready) = model_output.ok_or_else(|| {
                        Error::Conflict("tool start has no original model output".into())
                    })?;
                    assert_eq!(model_step, step);
                    let at = timestamp()?;
                    accepted = Some((
                        at,
                        std::time::Duration::from_micros(at.checked_sub(ready).ok_or_else(
                            || Error::Conflict("tool start predates its model output".into()),
                        )?),
                    ));
                }
                crate::executor::ExecutionEvent::ToolCompleted { ref call_id, .. }
                    if call_id == call =>
                {
                    let (at, admission) = accepted.ok_or_else(|| {
                        Error::Conflict("completion has no original tool start".into())
                    })?;
                    return Ok(ToolLatency {
                        completed: started.elapsed(),
                        admission,
                        execution: std::time::Duration::from_micros(
                            timestamp()?.checked_sub(at).ok_or_else(|| {
                                Error::Conflict("completion predates its tool start".into())
                            })?,
                        ),
                    });
                }
                crate::executor::ExecutionEvent::ToolFailed { ref call_id, .. }
                    if call_id == call =>
                {
                    return Err(Error::Conflict("measured publication failed".into()));
                }
                _ => {}
            }
        }
        if settled || page.end <= after {
            return Err(Error::Conflict(
                "tool journal ended before completion".into(),
            ));
        }
        after = page.end;
    }
}

fn returned_path(provider: &WritingModel, call: &str) -> Result<String> {
    let request = provider
        .last_request()
        .ok_or_else(|| Error::Conflict("missing result delivery".into()))?;
    result_path(&request, call)?
        .ok_or_else(|| Error::Conflict("settled turn omitted its tool result".into()))
}

fn result_path(request: &ModelRequest, call: &str) -> Result<Option<String>> {
    for part in request
        .messages
        .iter()
        .flat_map(|message| message.content.parts())
    {
        if let ModelContentPart::ToolResult {
            call_id, content, ..
        } = part
            && call_id == call
        {
            return match content {
                ToolResultContent::Json { value } => {
                    let path = value["path"].as_str();
                    let version = value["version"].as_str();
                    match (path, version) {
                        (Some(path), Some(version)) if !version.is_empty() => Ok(Some(path.into())),
                        _ => Err(Error::Invalid(
                            "write omitted its public path or version".into(),
                        )),
                    }
                }
                _ => Err(Error::Invalid(format!(
                    "write did not return a file: {content:?}"
                ))),
            };
        }
    }
    Ok(None)
}
