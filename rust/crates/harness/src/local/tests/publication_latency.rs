//! Timings include inbox admission, execution, result publication and follow delivery.
use super::*;
use crate::model::{
    ModelContent, ModelContentPart, ModelDispatch, ModelEvent, ModelRequest, ModelStream,
    PreparedModelRequest, ToolCall, ToolResultContent,
};
use crate::test_support::ScriptedEvent;
use acyclic_fs::{CancellationToken, WorkBudget};
use std::sync::Mutex;

struct WritingModel {
    counter: Arc<ScriptedModelProvider>,
    calls: Mutex<Vec<ToolCall>>,
    requests: Mutex<Vec<ModelRequest>>,
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
    let mut projects = Vec::new();
    let mut homes = Vec::new();
    for index in 0..8 {
        provider.calls.lock().unwrap().clear();
        let name = format!("writer-{index}");
        let crate::client::AgentReply::Spawned { agent: child, .. } = connection
            .execute(&crate::client::AgentCommand::Spawn {
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
        *provider.calls.lock().unwrap() = vec![
            ToolCall::new(
                "prepare-project".into(),
                "write".into(),
                json!({"path": "prepared.txt", "content": "baseline"}),
            ),
            ToolCall::new(
                "prepare-home".into(),
                "write".into(),
                json!({"path": "volume:///home/prepared.txt", "content": "baseline"}),
            ),
        ];
        let warm = observed
            .send(&format!("prepare-{index}"), "prepare this workspace")
            .await?;
        assert_eq!(warm.outcome, crate::agent::TurnOutcome::Completed);
        assert_eq!(returned_path(&provider, "prepare-project")?, "prepared.txt");
        assert_eq!(
            returned_path(&provider, "prepare-home")?,
            "volume:///home/prepared.txt"
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
            let path = format!("{prefix}settled-{count}.txt");
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
                            tool_completed_at(&conversation, inbox, call, started),
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
            let mut completed = results.iter().map(|result| result.1).collect::<Vec<_>>();
            completed.sort_unstable();
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
                        &format!("/settled-{count}.txt"),
                        64,
                        WorkBudget::UNBOUNDED,
                        &cancellation,
                    )
                    .await
                    .map_err(|failure| crate::local::storage(failure.error))?
                    .value;
                assert_eq!(bytes.as_slice(), content.as_bytes());
            }
        }
    }
    drop(writers);
    drop(connection);
    drop(agents);
    Arc::try_unwrap(harness)
        .map_err(|_| Error::Conflict("measurement still owns its harness".into()))?
        .disconnect()
        .await?;
    Ok(())
}

async fn tool_completed_at(
    conversation: &Conversation,
    inbox: u64,
    call: &str,
    started: std::time::Instant,
) -> Result<std::time::Duration> {
    let mut after = 0;
    loop {
        let query = conversation.execution_page(inbox, after, true).await?;
        let settled = query.settled;
        let page = query
            .page
            .filter(|_| query.input == inbox)
            .ok_or_else(|| Error::Conflict("original tool journal is absent".into()))?;
        for record in page.items {
            if matches!(record.event, crate::executor::ExecutionEvent::ToolCompleted { ref call_id, .. } if call_id == call)
            {
                return Ok(started.elapsed());
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
