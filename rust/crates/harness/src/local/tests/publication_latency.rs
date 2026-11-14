//! Timings include inbox admission, execution, result publication and follow delivery.
use super::*;
use crate::model::{ModelContentPart, ModelEvent, ToolCall, ToolResultContent};
use crate::test_support::ScriptedEvent;
use acyclic_fs::{CancellationToken, WorkBudget};

struct Writer {
    harness: Arc<LocalHarness>,
    observed: ObservedConversation,
    provider: Arc<ScriptedModelProvider>,
}

#[tokio::test]
async fn concurrent_model_writes_settle_project_and_home_payloads() -> Result<()> {
    let backing = tempfile::tempdir().map_err(crate::local::storage)?;
    acyclic_native_runtime::initialize_native_io_services().map_err(crate::local::storage)?;
    acyclic_fs::initialize_native_mount_services().map_err(crate::local::storage)?;
    let cancellation = CancellationToken::new();
    let mut writers: Vec<Writer> = Vec::new();
    let mut projects = Vec::new();
    let mut homes = Vec::new();
    for index in 0..8 {
        let checkout = backing.path().join(format!("checkout-{index}"));
        std::fs::create_dir(&checkout).map_err(crate::local::storage)?;
        for file in 0..128 {
            std::fs::write(checkout.join(format!("source-{file:03}.txt")), b"source\n")
                .map_err(crate::local::storage)?;
        }
        let provider = scripted([
            ScriptedTurn::Events(vec![
                ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
                    "prepare-project".into(),
                    "write".into(),
                    json!({"path": "prepared.txt", "content": "baseline"}),
                ))),
                ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
                    "prepare-home".into(),
                    "write".into(),
                    json!({"path": "volume:///home/prepared.txt", "content": "baseline"}),
                ))),
                ScriptedEvent::Event(ModelEvent::Completed {
                    metadata: json!({}),
                }),
            ]),
            ScriptedTurn::Text("prepared".into()),
        ]);
        let harness = Arc::new(
            open_scripted(
                crate::test_support::local_harness_config(&checkout, (model()?, provider.clone()))?
                    .state_directory(backing.path().join(format!("state-{index}"))),
            )
            .await?,
        );
        let mut observed = ObservedConversation::open(&harness).await?;
        // Initial capture and executor preparation are outside write timings.
        let warm = observed.send("prepare", "prepare this workspace").await?;
        assert_eq!(warm.outcome, crate::agent::TurnOutcome::Completed);
        assert_eq!(returned_path(&provider, "prepare-project")?, "prepared.txt");
        assert_eq!(
            returned_path(&provider, "prepare-home")?,
            "volume:///home/prepared.txt"
        );
        let selected = harness.project.clone();
        let home = harness.scratch.clone();
        assert_ne!(selected, home);
        assert!(
            !projects.contains(&selected),
            "writers must have separate projects"
        );
        projects.push(selected);
        assert!(!homes.contains(&home), "writers must have private homes");
        homes.push(home);
        for earlier in &writers {
            assert_ne!(
                observed.conversation.agent(),
                earlier.observed.conversation.agent()
            );
        }
        writers.push(Writer {
            harness,
            observed,
            provider,
        });
    }
    for count in [1, 8] {
        for prefix in ["volume:///home/", ""] {
            let barrier = tokio::sync::Barrier::new(count);
            let results = futures::future::join_all(writers[..count].iter_mut().enumerate().map(
                |(index, writer)| {
                    let barrier = &barrier;
                    async move {
                        let call = format!("write-{count}-{index}-{}", prefix.is_empty());
                        let path = format!("{prefix}settled-{count}-{index}.txt");
                        let content = format!("writer {index}\n");
                        writer.provider.extend([
                            ScriptedTurn::Events(vec![
                                ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
                                    call.clone(),
                                    "write".into(),
                                    json!({"path": path, "content": content}),
                                ))),
                                ScriptedEvent::Event(ModelEvent::Completed {
                                    metadata: json!({}),
                                }),
                            ]),
                            ScriptedTurn::Text("published".into()),
                        ]);
                        barrier.wait().await;
                        let started = std::time::Instant::now();
                        let settled = writer.observed.send(&call, "publish the file").await?;
                        let elapsed = started.elapsed();
                        assert_eq!(settled.outcome, crate::agent::TurnOutcome::Completed);
                        Ok::<_, Error>((elapsed, call, content))
                    }
                },
            ))
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
            let mut elapsed = results.iter().map(|result| result.0).collect::<Vec<_>>();
            elapsed.sort_unstable();
            eprintln!(
                "TOOL_TURN_LATENCY volume={} writers={count} p50_us={} max_us={}",
                if prefix.is_empty() { "project" } else { "home" },
                elapsed[elapsed.len() / 2].as_micros(),
                elapsed.last().expect("writers").as_micros()
            );
            // Inspect committed payloads only after every writer has settled.
            for (index, (writer, (_, call, content))) in
                writers[..count].iter().zip(results).enumerate()
            {
                assert_eq!(
                    returned_path(&writer.provider, &call)?,
                    format!("{prefix}settled-{count}-{index}.txt")
                );
                let volume = if prefix.is_empty() {
                    &projects[index]
                } else {
                    &homes[index]
                };
                let workspace = writer
                    .harness
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
                let head = workspace
                    .head(WorkBudget::UNBOUNDED, &cancellation)
                    .await
                    .map_err(|failure| crate::local::storage(failure.error))?
                    .value;
                let bytes = head
                    .read(
                        &format!("/settled-{count}-{index}.txt"),
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
    for Writer {
        harness, observed, ..
    } in writers
    {
        drop(observed);
        Arc::try_unwrap(harness)
            .map_err(|_| Error::Conflict("measurement still owns its harness".into()))?
            .disconnect()
            .await?;
    }
    Ok(())
}

fn returned_path(provider: &ScriptedModelProvider, call: &str) -> Result<String> {
    let request = provider
        .last_request()
        .ok_or_else(|| Error::Conflict("missing result delivery".into()))?;
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
                        (Some(path), Some(version)) if !version.is_empty() => Ok(path.into()),
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
    Err(Error::Conflict(
        "settled turn omitted its tool result".into(),
    ))
}
