//! The local agent host: a prompt is one staged file and one inbox commit, and
//! the root agent's actor runs its turn on the stock executor.

use super::super::LocalAgents;
use super::{ScriptedModelProvider, text, volume, volume_config};
use crate::{
    Error, LocalHarness, Result,
    agent::{AgentPath, AgentRecord, ConversationEntry, Reply, StatusRecord, TurnInput},
    local::agent_conversation::Conversation,
};
use acyclic_stream::{ReadRequest, StreamError, StreamPath, StreamProvider};
use futures::TryStreamExt;
use std::sync::Arc;

/// The root conversation `harness` serves to its own in-process session.
async fn conversation(harness: &Arc<LocalHarness>) -> Result<Arc<Conversation>> {
    Conversation::open(harness.connect().await?)
}

fn agent(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}

async fn records(stream: &impl StreamProvider, path: StreamPath) -> Result<Vec<Vec<u8>>> {
    match stream
        .read(ReadRequest {
            key: None,
            path,
            from: 0,
            limit: 16,
            at_least: Default::default(),
        })
        .await
    {
        Ok(records) => Ok(records
            .map_ok(|record| record.value.to_vec())
            .try_collect()
            .await
            .map_err(agent)?),
        Err(StreamError::NotFound) => Ok(Vec::new()),
        Err(error) => Err(agent(error)),
    }
}

/// The root's conversation once it has settled every input sent before
/// this call: idle past that inbox tail. Its adoption's own `Idle` status
/// precedes those inputs, so it never counts.
async fn settled(agents: &LocalAgents, root: &AgentPath) -> Result<Vec<Vec<u8>>> {
    let inbox = crate::agent::inbox_tail(&**agents.provider(), root)
        .await
        .map_err(agent)?;
    let mut statuses = acyclic_stream::records_only(
        agents
            .provider()
            .follow(acyclic_stream::FollowRequest::new(
                root.status().map_err(agent)?,
                0,
            ))
            .await
            .map_err(agent)?,
    );
    tokio::time::timeout(std::time::Duration::from_secs(8), async {
        while let Some(record) = statuses.try_next().await.map_err(agent)? {
            let status = StatusRecord::decode(&record.value).map_err(agent)?;
            match status.phase {
                crate::agent::Phase::Idle if status.cursor >= inbox => {
                    return records(&**agents.provider(), root.conversation().map_err(agent)?)
                        .await;
                }
                crate::agent::Phase::Done { outcome } => {
                    return Err(Error::Conflict(format!(
                        "agent ended before settlement: {outcome:?}"
                    )));
                }
                _ => {}
            }
        }
        Err(Error::Storage(
            "agent status follow ended before settlement".into(),
        ))
    })
    .await
    .map_err(|_| Error::Conflict("agent did not publish settlement".into()))?
}

#[tokio::test]
async fn a_prompt_runs_one_root_agent_turn_and_replays_exactly() -> Result<()> {
    let directory = tempfile::tempdir().map_err(agent)?;
    let project = volume(directory.path()).await?;
    let provider = text();
    let harness = Arc::new(
        LocalHarness::open(
            volume_config(directory.path(), project, provider.clone())?
                .compaction(crate::context::CompactionPolicy::Disabled),
        )
        .await?,
    );
    let (agents, root) = harness.agents().await?;
    let (agents, root) = (Arc::clone(agents), root.clone());
    let client = conversation(&harness).await?;
    let inbox = client.send("hello", "hello").await?;
    let conversation = root.conversation().map_err(agent)?;
    let entries = settled(&agents, &root).await?;
    assert_eq!(entries.len(), 1);
    let entry = ConversationEntry::decode(&entries[0]).map_err(agent)?;
    assert_eq!(entry.inbox, inbox);
    assert!(matches!(entry.input, TurnInput::Message { .. }));
    assert_eq!(provider.request_count(), 1);
    // A client reads the settled output from the root's private volume.
    let output = harness
        .client_reader()?
        .read(&entry.output, 1 << 20, None, None)?
        .await?;
    assert!(!output.is_empty());
    // The same intent replays its commit at the same sequence; no second turn runs.
    assert_eq!(client.send("hello", "hello").await?, inbox);
    assert_eq!(records(&**agents.provider(), conversation).await?.len(), 1);
    Ok(())
}

/// The model's real channel owns progress; dropping it must close the same
/// Session that carries the local commands and follows, retaining queued input.
#[tokio::test]
async fn lost_model_producer_closes_local_session_without_settling_input() -> Result<()> {
    use crate::model::{Model, ModelDispatch, ModelEvent, ModelProvider, PreparedModelRequest};
    use acyclic_stream::{BoxProviderFuture, BoxProviderStream};
    use futures::StreamExt as _;
    use std::sync::Mutex;
    use tokio::sync::oneshot;

    struct ClosingModel {
        counts: Arc<ScriptedModelProvider>,
        gate: Mutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>,
    }
    impl ModelProvider for ClosingModel {
        fn context_capacity(&self, model: &Model) -> Result<crate::context::ModelContextCapacity> {
            self.counts.context_capacity(model)
        }
        fn count_tokens<'a>(
            &'a self,
            request: &'a PreparedModelRequest,
        ) -> BoxProviderFuture<'a, Result<crate::context::ModelTokenCount>> {
            self.counts.count_tokens(request)
        }
        fn generate<'a>(
            &'a self,
            _: &'a PreparedModelRequest,
            _: ModelDispatch,
        ) -> BoxProviderStream<'a, Result<ModelEvent>> {
            let gate = self.gate.lock().unwrap().take();
            futures::stream::once(async move {
                let (entered, result) =
                    gate.ok_or_else(|| Error::Invalid("second dispatch".into()))?;
                let _ = entered.send(());
                result
                    .await
                    .map_err(|_| Error::Storage("model producer closed".into()))?;
                Err(Error::Invalid("model gate unexpectedly released".into()))
            })
            .boxed()
        }
    }

    tokio::time::timeout(std::time::Duration::from_secs(8), async {
        let directory = tempfile::tempdir().map_err(agent)?;
        let project = volume(directory.path()).await?;
        let (entered, entering) = oneshot::channel();
        let (producer, result) = oneshot::channel();
        let harness = Arc::new(LocalHarness::open(
            volume_config(directory.path(), project, Arc::new(ClosingModel {
                counts: text(),
                gate: Mutex::new(Some((entered, result))),
            }))?.compaction(crate::context::CompactionPolicy::Disabled),
        ).await?);
        let client = conversation(&harness).await?;
        let mut events = client.observe(0).await?;
        let inbox = client.send("lost-producer", "hello").await?;
        entering.await.map_err(agent)?;
        let waiting = async {
            loop {
                match events.next().await? {
                    super::super::Event::Turn(_) => return Err::<(), _>(Error::Conflict("unsettled model produced a turn".into())),
                    super::super::Event::Status(_) | super::super::Event::Output => {}
                }
            }
        };
        tokio::pin!(waiting);
        assert!(futures::poll!(&mut waiting).is_pending(), "live producer must retain the wait");
        drop(producer);
        assert!(matches!(waiting.await, Err(Error::Storage(_))));
        let hosted = harness.agents.get().unwrap();
        assert!(matches!(hosted.runtime.completion().await, Err(Error::Storage(reason)) if reason.contains("model producer closed")));
        assert_eq!(hosted.agents().reply(hosted.root(), &crate::agent::IntentId::try_from("lost-producer").map_err(agent)?, inbox).await.map_err(agent)?, crate::agent::Reply::Pending, "failed input stays recoverable");
        assert!(records(&**hosted.agents().provider(), hosted.root().conversation().map_err(agent)?).await?.is_empty());
        assert!(matches!(harness.connect().await, Err(Error::Storage(_))), "late clients observe the already closed runtime");
        Ok(())
    }).await.map_err(|_| Error::Conflict("closed local runtime retained a Session wait".into()))?
}

/// The names directly under `path` on `volume`.
async fn names(
    harness: &LocalHarness,
    volume: &crate::conversation::VolumeRef,
    path: &str,
) -> Result<Vec<String>> {
    let grant = crate::conversation::ContentGrant::verify_directory_read(
        &harness.verifier,
        &harness.signed,
        volume,
        "",
        &mut None,
    )?;
    let (_, page) = harness
        .files
        .list_private_directory(
            volume,
            &grant,
            path,
            crate::filesystem::DirectoryOptions {
                generation: None,
                after: None,
                maximum_entries: 16,
                ceiling: None,
            },
        )
        .await?;
    Ok(page
        .entries
        .iter()
        .map(|entry| String::from_utf8_lossy(entry.name.as_bytes()).into_owned())
        .collect())
}

#[tokio::test]
async fn a_shell_call_stages_its_output_on_the_private_volume() -> Result<()> {
    use tracing_subscriber::{Layer as _, layer::SubscriberExt as _};
    let waits = acyclic_native_runtime::obs::WaitReport::default();
    let _subscriber = tracing::subscriber::set_default(
        tracing_subscriber::registry().with(waits.clone()).with(
            tracing_subscriber::fmt::layer()
                .with_filter(tracing_subscriber::filter::LevelFilter::WARN),
        ),
    );
    // Retain the exact admission the served tree derives its credentials from.
    // Minting another scope with the same grants does not identify that parent.
    struct Admission(std::sync::Mutex<Option<crate::core::Scope>>);
    impl crate::local::Verifier for Admission {
        fn caller(
            &self,
            custody: &crate::local::Custody<'_>,
            owner: &crate::core::Scope,
        ) -> Result<crate::hosting::VerifiedCaller> {
            let caller =
                crate::local::Verifier::caller(&crate::local::KeyFileVerifier, custody, owner)?;
            *self.0.lock().map_err(agent)? = Some(caller.scope.clone());
            Ok(caller)
        }
        fn session(
            &self,
            custody: &crate::local::Custody<'_>,
        ) -> Result<Arc<acyclic_stream::session::Authority>> {
            crate::local::Verifier::session(&crate::local::KeyFileVerifier, custody)
        }
    }
    let admission = Arc::new(Admission(std::sync::Mutex::new(None)));
    let directory = tempfile::tempdir().map_err(agent)?;
    let provider = super::scripted([
        super::checked_commands::call(0, crate::tools::shell::PLATFORM.name, "echo staged"),
        crate::test_support::ScriptedTurn::Text("done".into()),
    ]);
    let mut config = super::checkout_config(directory.path(), (super::model()?, provider))?;
    config.served = Some(admission.clone());
    let harness = Arc::new(LocalHarness::open(config).await?);
    let (agents, root) = harness.agents().await?;
    let (agents, root) = (Arc::clone(agents), root.clone());
    let completion = harness.agents.get().unwrap().runtime.completion();
    conversation(&harness)
        .await?
        .send("shell", "run one command")
        .await?;
    let entries = tokio::select! {
        result = settled(&agents, &root) => result,
        failure = completion => Err(Error::Conflict(format!("shell turn failed before settlement: {failure:?}"))),
    };
    let entries = entries.inspect_err(|_| {
        let _ = waits.report(&mut std::io::stderr());
    })?;
    let entry = ConversationEntry::decode(&entries[0]).map_err(agent)?;
    let state = agents.state(&root).await.map_err(agent)?;
    // Each turn's calls share one directory, named by the turn.
    let turn = crate::agent::turn_id(&state, &entry.input).map_err(agent)?;
    let private = &state
        .volumes
        .iter()
        .find(|binding| binding.name == crate::filesystem::PRIVATE_VOLUME)
        .ok_or_else(|| Error::NotFound("private volume".into()))?
        .volume;
    let output = harness
        .client_reader()?
        .read(&entry.output, 1 << 20, None, None)?
        .await?;
    let root = names(&harness, private, "").await?;
    assert!(
        root.iter().any(|name| name == "tools"),
        "no tools directory in {root:?}; turn output {}",
        String::from_utf8_lossy(&output)
    );
    assert_eq!(names(&harness, private, "tools").await?, [turn.to_string()]);
    let turn_id = turn.to_string();
    let turn = format!("tools/{turn}");
    let calls = names(&harness, private, &turn).await?;
    let [call] = calls.as_slice() else {
        return Err(Error::Conflict(format!(
            "one shell call staged, got {calls:?}"
        )));
    };
    assert_eq!(
        names(&harness, private, &format!("{turn}/{call}")).await?,
        ["request.json", "result.json", "stdout"]
    );
    assert_eq!(
        names(&harness, private, &format!("{turn}/{call}/stdout")).await?,
        ["0000000000000000"]
    );
    let scope = admission
        .0
        .lock()
        .map_err(agent)?
        .clone()
        .ok_or_else(|| Error::NotFound("served admission".into()))?;
    follow_tool_output(&harness, &state.path, &turn_id, &scope).await
}

async fn observation(
    harness: &LocalHarness,
    authority: &crate::filesystem::AgentAuthority<'_>,
    path: &AgentPath,
) -> Result<()> {
    harness
        .files
        .agent_observation(
            authority,
            path,
            &authority.private.volume,
            Arc::new(acyclic_stream::SystemUnixMillisClock),
            None,
        )
        .await
        .map(drop)
}

/// The agent's own scope yields a live observation of its tool output, and
/// none from a scope that cannot list and read all of `tools/`; revoking the
/// scope ends an open observation.
async fn follow_tool_output(
    harness: &Arc<LocalHarness>,
    root: &AgentPath,
    turn: &str,
    admitted: &crate::core::Scope,
) -> Result<()> {
    use acyclic_fs::access::{Action, Capability, Generations, Grant, ResourcePattern};
    let (agents, _) = harness.agents().await?;
    let state = agents.state(root).await.map_err(agent)?;
    let issuer = harness
        .root_authority
        .as_ref()
        .ok_or_else(|| Error::NotFound("issuer".into()))?;
    let (path, volumes) = (&state.path, &state.volumes);
    let runtime = &(*issuer).clone().into();
    let authority = move || {
        crate::filesystem::agent_authority(
            runtime,
            &crate::filesystem::TurnAdmission::LocalRoot,
            path,
            volumes,
            &[],
        )
    };
    // A structured scope of `actions` on `paths`, for the same agent.
    let structured = async |actions: &[Action], paths: &[&str]| -> Result<_> {
        let mut scoped = authority().await?;
        let capability = Capability {
            layers: vec![
                paths
                    .iter()
                    .map(|path| Grant {
                        actions: actions.iter().copied().collect(),
                        resource: ResourcePattern {
                            lineage_root: None,
                            volume: None,
                            generations: Generations::All,
                            path: (*path).into(),
                        },
                    })
                    .collect(),
            ],
            expires_at: None,
        };
        scoped.scope = scoped
            .issuer
            .native()
            .ok_or_else(|| Error::Invalid("native fixture custody required".into()))?
            .bootstrap_scope(
                "observer",
                &crate::Capabilities::new([capability.to_scope_label().map_err(agent)?]),
                scoped.scope.agent(),
                &mut None,
            )?;
        Ok(scoped)
    };
    // Negative control: the same agent, attenuated to no grants, gets none.
    let mut denied = authority().await?;
    denied.scope = denied.scope.attenuate_workload(
        "denied",
        &crate::Capabilities::new(Vec::<String>::new()),
        &issuer.public_workload_trust()?,
        &crate::core::Authority {
            kind: crate::core::AggregateKind::Conversation,
            id: state.path.id().as_str().to_owned(),
        },
        acyclic_stream::UnixMillisClock::now_unix_millis(&acyclic_stream::SystemUnixMillisClock)
            / 1000,
    )?;
    assert!(
        observation(harness, &denied, path).await.is_err(),
        "a scope without read on tools/ yields no credential"
    );
    // A structured scope is held to its compiled view: writing everywhere
    // reads nothing, and reading only `tools/` is enough.
    let writer = structured(&[Action::Create, Action::Write], &["**"]).await?;
    assert!(
        observation(harness, &writer, path).await.is_err(),
        "a write-only structured scope yields no credential"
    );
    let reader = structured(&[Action::List, Action::Read], &["tools", "tools/**"]).await?;
    observation(harness, &reader, path).await?;
    // From after the settled turn: its output stays the newest call's.
    let mut observer = conversation(harness).await?.observe(1).await?;
    let wait = std::time::Duration::from_secs(10);
    tokio::time::timeout(wait, async {
        loop {
            if matches!(observer.next().await?, super::super::Event::Output) {
                observer.absorb().await?;
                if observer.output().contains("staged") {
                    return Ok::<_, Error>(());
                }
            }
        }
    })
    .await
    .map_err(|_| Error::Conflict(format!("no output of turn {turn} arrived")))??;
    // Revoking the agent scope the connection's credential derives from
    // ends its observation.
    // Negative control: an unrelated root with identical grants cannot close it.
    let unrelated = authority().await?;
    unrelated
        .issuer
        .native()
        .ok_or_else(|| Error::Invalid("native fixture custody required".into()))?
        .revoke_workload(&unrelated.scope)?;
    issuer.verifier(&mut None)?.verify(admitted, &mut None)?;
    issuer.revoke_workload(admitted)?;
    tokio::time::timeout(wait, async {
        loop {
            if observer.next().await.is_err() {
                return;
            }
        }
    })
    .await
    .map_err(|_| Error::Conflict("a revoked observation stayed open".into()))
}

/// A local HTTP peer that answers each request with its own ordinal, so a
/// hook that ran again would add different context.
#[cfg(unix)]
struct CountingServer {
    port: u16,
    calls: Arc<std::sync::atomic::AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

#[cfg(unix)]
impl Drop for CountingServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[cfg(unix)]
async fn counting_server() -> Result<CountingServer> {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(agent)?;
    let port = listener.local_addr().map_err(agent)?.port();
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let served = Arc::clone(&count);
    let task = tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let ordinal = served.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            let mut request = [0; 8192];
            let mut used: usize = 0;
            loop {
                let start = used.saturating_sub(3);
                match stream.read(&mut request[used..]).await {
                    Ok(0) | Err(_) => return,
                    Ok(read) => used += read,
                }
                if request[start..used]
                    .windows(4)
                    .any(|window| window == b"\r\n\r\n")
                {
                    break;
                }
            }
            let body = format!("HOOK-HTTP-{ordinal}");
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            if stream.write_all(response.as_bytes()).await.is_err() {
                return;
            }
        }
    });
    Ok(CountingServer {
        port,
        calls: count,
        task,
    })
}

/// A turn whose `UserPromptSubmit` hook called an HTTP peer is interrupted
/// after the hook's outcome was recorded; the reopened agent replays the
/// turn from `hooks/{turn}/{op}.json`, with no new hook run or HTTP call,
/// and the model sees the identical recorded context.
#[cfg(unix)]
#[tokio::test]
async fn a_reopened_turn_replays_its_recorded_hook_outcome() -> Result<()> {
    let directory = tempfile::tempdir().map_err(agent)?;
    let peer = counting_server().await?;
    let (port, calls) = (peer.port, &peer.calls);
    let home = directory.path().join("home");
    std::fs::create_dir_all(home.join(".claude")).map_err(agent)?;
    let hook = format!("cat >/dev/null; curl -sS http://127.0.0.1:{port}/");
    std::fs::write(
        home.join(".claude/settings.json"),
        serde_json::to_vec(
            &serde_json::json!({"hooks": {"UserPromptSubmit": [{"hooks": [
                {"type": "command", "shell": "sh", "command": hook}
            ]}]}}),
        )
        .map_err(agent)?,
    )
    .map_err(agent)?;
    let workspace = directory.path().join("work");
    std::fs::create_dir_all(&workspace).map_err(agent)?;
    let open = |provider: Arc<ScriptedModelProvider>| {
        let (workspace, home) = (workspace.clone(), home.clone());
        let state = directory.path().join("state");
        async move {
            let config = super::checkout_config(&workspace, (super::model()?, provider))?
                .state_directory(state)
                .settings(
                    crate::native_settings::SettingsConfig::new(&workspace, &home)
                        .codex_home(home.join(".codex")),
                );
            Ok::<_, Error>(Arc::new(LocalHarness::open(config).await?))
        }
    };
    let (dispatched, mut requests) = tokio::sync::mpsc::channel(1);
    let pending = Arc::new(
        ScriptedModelProvider::new([crate::test_support::ScriptedTurn::Events(vec![
            crate::test_support::ScriptedEvent::Pending {},
        ])])
        .with_context_capacity(super::capacity())
        .with_dispatch_observer(move |_| {
            let _ = dispatched.try_send(());
        }),
    );
    let harness = open(pending.clone()).await?;
    harness.agents().await?;
    let completion = harness.agents.get().unwrap().runtime.completion();
    conversation(&harness)
        .await?
        .send("hooked", "hello")
        .await?;
    tokio::time::timeout(std::time::Duration::from_secs(8), async {
        // A turn that fails for good commits its agent's failure, so the
        // agent settling before dispatch is that failure.
        tokio::select! {
            request = requests.recv() => request.ok_or_else(|| Error::Storage("model dispatch producer closed".into())),
            failure = completion => Err(Error::Conflict(format!("hooked turn failed before model dispatch: {failure:?}"))),
        }
    })
    .await
    .map_err(|_| Error::Conflict("the hooked turn never reached the model".into()))??;
    let recorded = serde_json::to_string(&pending.request(0)).map_err(agent)?;
    assert!(recorded.contains("HOOK-HTTP-1"), "{recorded}");
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    // Crash: the turn is still waiting on the model when the harness drops.
    drop(harness);
    let resumed = Arc::new(
        ScriptedModelProvider::new([crate::test_support::ScriptedTurn::Text("done".into())])
            .with_context_capacity(super::capacity())
            .with_reconciliations([crate::test_support::ScriptedReconciliation {
                orphan: true,
                observed: Vec::new(),
                continuation: Some(vec![
                    serde_json::from_value(serde_json::json!({"kind": "content", "delta": "done"}))
                        .map_err(agent)?,
                    serde_json::from_value(
                        serde_json::json!({"kind": "completed", "metadata": {"finish": "stop"}}),
                    )
                    .map_err(agent)?,
                ]),
            }]),
    );
    let harness = open(resumed.clone()).await?;
    let (agents, root) = harness.agents().await?;
    let (agents, root) = (Arc::clone(agents), root.clone());
    let entries = settled(&agents, &root).await?;
    assert_eq!(entries.len(), 1);
    // The replay ran no hook and made no HTTP call.
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    // A re-dispatched request carries the identical recorded context.
    if let Some(request) = resumed.request(0) {
        let replayed = serde_json::to_string(&request).map_err(agent)?;
        assert!(
            replayed.contains("HOOK-HTTP-1") && !replayed.contains("HOOK-HTTP-2"),
            "{replayed}"
        );
    }
    // The outcome is the one file recorded under the turn, beside its marker.
    let entry = ConversationEntry::decode(&entries[0]).map_err(agent)?;
    let state = agents.state(&root).await.map_err(agent)?;
    let turn = crate::agent::turn_id(&state, &entry.input).map_err(agent)?;
    let private = &state
        .volumes
        .iter()
        .find(|binding| binding.name == crate::filesystem::PRIVATE_VOLUME)
        .ok_or_else(|| Error::NotFound("private volume".into()))?
        .volume;
    assert_eq!(names(&harness, private, "hooks").await?, [turn.to_string()]);
    let recorded = names(&harness, private, &format!("hooks/{turn}")).await?;
    let [outcome, marker] = recorded.as_slice() else {
        return Err(Error::Conflict(format!(
            "one hook outcome, got {recorded:?}"
        )));
    };
    assert_eq!(
        outcome.strip_suffix(".json"),
        marker.strip_suffix(".started")
    );
    Ok(())
}

/// The files an attachment manifest lists, read through `harness`'s reader.
async fn listed(
    harness: &LocalHarness,
    manifest: &crate::conversation::FileRef,
) -> Result<Vec<crate::conversation::FileRef>> {
    let reader = harness.client_reader()?;
    let bytes = reader
        .read(manifest, manifest.descriptor().byte_length(), None, None)?
        .await?;
    Ok(
        crate::conversation::decode_attachment_manifest(manifest, bytes.as_ref(), None, None)?
            .into_iter()
            .map(|item| item.file)
            .collect(),
    )
}

/// A harness over a fresh checkout holding `files`, written after it opens.
async fn checkout_with(
    files: &[(String, Vec<u8>)],
) -> Result<(
    tempfile::TempDir,
    Arc<LocalHarness>,
    Arc<ScriptedModelProvider>,
)> {
    checkout_scripted(files, text()).await
}

async fn checkout_scripted(
    files: &[(String, Vec<u8>)],
    provider: Arc<ScriptedModelProvider>,
) -> Result<(
    tempfile::TempDir,
    Arc<LocalHarness>,
    Arc<ScriptedModelProvider>,
)> {
    let directory = tempfile::tempdir().map_err(agent)?;
    std::fs::create_dir(directory.path().join(".git")).map_err(agent)?;
    let harness = Arc::new(
        LocalHarness::open(super::checkout_config(
            directory.path(),
            (super::model()?, provider.clone()),
        )?)
        .await?,
    );
    let checkout = &harness.workspace.as_ref().expect("checkout").path;
    for (path, bytes) in files {
        let path = checkout.join(path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(agent)?;
        }
        std::fs::write(path, bytes).map_err(agent)?;
    }
    Ok((directory, harness, provider))
}

#[tokio::test]
async fn a_prompt_attaches_the_checkout_files_it_mentions_once_each() -> Result<()> {
    use crate::model::{FileProjectionPolicy, ModelContent, ModelContentPart};
    const PIXEL: &[u8] = &[
        0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0, 0, 0, 0x0d, 0x49, 0x48, 0x44, 0x52, 0,
        0, 0, 1, 0, 0, 0, 1, 8, 4, 0, 0, 0, 0xb5, 0x1c, 0x0c, 0x02, 0, 0, 0, 0x0b, 0x49, 0x44,
        0x41, 0x54, 0x78, 0xda, 0x63, 0xfc, 0xff, 0x1f, 0, 0x03, 0x03, 0x02, 0, 0xef, 0x9a, 0x5e,
        0x4f, 0, 0, 0, 0, 0x49, 0x45, 0x4e, 0x44, 0xae, 0x42, 0x60, 0x82,
    ];
    let (_directory, harness, provider) = checkout_with(&[
        ("mention.txt".into(), b"MENTIONED\n".to_vec()),
        ("pixel.png".into(), PIXEL.to_vec()),
    ])
    .await?;
    let checkout = harness.workspace.as_ref().expect("checkout").path.clone();
    let (agents, root) = harness.agents().await?;
    let (agents, root) = (Arc::clone(agents), root.clone());
    let client = conversation(&harness).await?;
    let prompt = "inspect @mention.txt and @pixel.png then @mention.txt again";
    // A capture for one message replays its pinned manifest exactly.
    let first = harness.prompt_attachments(b"direct", Some(prompt)).await?;
    assert_eq!(
        harness.prompt_attachments(b"direct", Some(prompt)).await?,
        first
    );
    let inbox = client.send("mention", prompt).await?;
    let entries = settled(&agents, &root).await?;
    // Later publications on the private volume leave a replay unchanged.
    assert_eq!(
        harness.prompt_attachments(b"direct", Some(prompt)).await?,
        first
    );
    let entry = ConversationEntry::decode(&entries[0]).map_err(agent)?;
    let TurnInput::Message {
        attachments: Some(manifest),
        ..
    } = &entry.input
    else {
        return Err(Error::Conflict(
            "the prompt started no attached message turn".into(),
        ));
    };
    // Each mentioned file attaches once, in first-mention order, pinned.
    let reader = harness.client_reader()?;
    let mut read = Vec::new();
    for file in listed(&harness, manifest).await? {
        read.push((
            file.path()
                .rsplit('/')
                .next()
                .unwrap_or_default()
                .to_owned(),
            file.descriptor().media_type().to_owned(),
            reader.read(&file, 1 << 20, None, None)?.await?.to_vec(),
        ));
    }
    assert_eq!(
        read,
        [
            (
                super::super::context::copy_name("mention.txt"),
                "text/plain".into(),
                b"MENTIONED\n".to_vec()
            ),
            (
                super::super::context::copy_name("pixel.png"),
                "image/png".into(),
                PIXEL.to_vec()
            ),
        ]
    );
    // The model sees the prompt, then the text file as text and the image natively.
    let request = provider.last_request().expect("one model request");
    let Some(ModelContent::Parts(parts)) = request.messages.last().map(|message| &message.content)
    else {
        return Err(Error::Conflict("the turn input has no attachments".into()));
    };
    let policies = parts
        .iter()
        .map(|part| match part {
            ModelContentPart::File { policy, .. } => {
                Some(matches!(policy, FileProjectionPolicy::Native(_)))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(policies, [Some(false), Some(false), Some(true)]);
    // A resend after its files changed or vanished replays the pinned
    // manifest, so it still matches the delivered message exactly.
    std::fs::write(checkout.join("mention.txt"), "CHANGED\n").map_err(agent)?;
    std::fs::remove_file(checkout.join("pixel.png")).map_err(agent)?;
    assert_eq!(
        harness.prompt_attachments(b"direct", Some(prompt)).await?,
        first
    );
    assert_eq!(client.send("mention", prompt).await?, inbox);
    // A new message sees the files as they are now.
    let fresh = harness
        .prompt_attachments(b"fresh", Some("inspect @mention.txt"))
        .await?
        .expect("a manifest");
    let fresh = listed(&harness, &fresh).await?;
    assert_eq!(
        reader.read(&fresh[0], 1 << 20, None, None)?.await?.to_vec(),
        b"CHANGED\n"
    );
    Ok(())
}

#[tokio::test]
async fn a_replayed_command_answers_from_its_first_delivery_after_the_command_is_gone() -> Result<()>
{
    let (_directory, harness, _provider) = checkout_with(&[
        (
            ".claude/commands/marker.md".into(),
            b"MARKER $ARGUMENTS with @note.txt
"
            .to_vec(),
        ),
        (
            "note.txt".into(),
            b"NOTE
"
            .to_vec(),
        ),
    ])
    .await?;
    let checkout = harness.workspace.as_ref().expect("checkout").path.clone();
    let (agents, root) = harness.agents().await?;
    let (agents, root) = (Arc::clone(agents), root.clone());
    let client = conversation(&harness).await?;
    let inbox = client.send("marker", "/marker now").await?;
    let entries = settled(&agents, &root).await?;
    let entry = ConversationEntry::decode(&entries[0]).map_err(agent)?;
    // The command, the skill catalog and the mentioned file are all gone.
    std::fs::remove_dir_all(checkout.join(".claude")).map_err(agent)?;
    std::fs::remove_file(checkout.join("note.txt")).map_err(agent)?;
    // Negative control: a new delivery reads the project as it is now.
    assert!(client.send("fresh", "/marker now").await.is_err());
    // The replay is the first delivery, answered from its committed record.
    assert_eq!(client.send("marker", "/marker now").await?, inbox);
    let Reply::Answered {
        entry: replayed, ..
    } = client.reply("marker", inbox).await?
    else {
        return Err(Error::Conflict("the replay lost its saved answer".into()));
    };
    assert_eq!(replayed, entry);
    Ok(())
}

#[tokio::test]
async fn a_ten_mebibyte_prompt_is_accepted_and_reads_back_exactly() -> Result<()> {
    use acyclic_harness_client::AgentCommand;
    let (_directory, harness, _provider) = checkout_with(&[]).await?;
    let (agents, root) = harness.agents().await?;
    let (agents, root) = (Arc::clone(agents), root.clone());
    let client = conversation(&harness).await?;
    // Far past one command frame: the client stages it in parts and the
    // command names them by digest.
    let prompt = "0123456789abcdef\n".repeat(10 * 1024 * 1024 / 17 + 1);
    let inbox = client.send("large", &prompt).await?;
    let entries = settled(&agents, &root).await?;
    let entry = ConversationEntry::decode(&entries[0]).map_err(agent)?;
    let (read, _) = client.turn(&entry).await?;
    assert!(read.as_deref() == Some(prompt.as_str()));
    // A resend is the same delivery.
    assert_eq!(client.send("large", &prompt).await?, inbox);
    // Negative controls: a head is refused when its parts were never staged,
    // and when the staged parts do not match its digest.
    let connection = harness.connect().await?;
    let digest = blake3::hash(b"{}").to_hex().to_string();
    let head = |digest: &str| AgentCommand::Staged {
        agent: root.clone(),
        command: "prompt".into(),
        digest: digest.to_owned(),
        parts: 1,
        bytes: 2,
    };
    assert!(connection.execute(&head(&digest)).await.is_err());
    connection
        .execute(&AgentCommand::Stage {
            agent: root.clone(),
            digest: digest.clone(),
            part: 0,
            text: "[]".into(),
        })
        .await?;
    assert!(connection.execute(&head(&digest)).await.is_err());
    Ok(())
}

#[tokio::test]
async fn parent_and_child_writes_retain_their_own_project_volume() -> Result<()> {
    use crate::model::{ModelContent, ModelContentPart, ModelEvent, ToolCall, ToolResultContent};
    use crate::test_support::{ScriptedEvent, ScriptedTurn};
    use acyclic_harness_client::{AgentCommand, AgentReply, ReadTarget};
    let _subscriber = tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::WARN)
            .with_test_writer()
            .finish(),
    );

    let write = |text: &str| {
        ScriptedTurn::Events(vec![
            ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
                "write-seed".into(),
                "write".into(),
                serde_json::json!({
                    "path": "seed.txt", "content": text,
                }),
            ))),
            ScriptedEvent::Event(ModelEvent::Completed {
                metadata: serde_json::json!({}),
            }),
        ])
    };
    let provider = super::scripted([
        write("child"),
        ScriptedTurn::Text("child done".into()),
        write("parent"),
        ScriptedTurn::Text("parent done".into()),
    ]);
    let publication = |index| -> Result<crate::conversation::FileRef> {
        let request = provider
            .request(index)
            .ok_or_else(|| Error::NotFound("write successor request".into()))?;
        let value = request
            .messages
            .iter()
            .flat_map(|message| message.content.parts())
            .find_map(|part| match part {
                ModelContentPart::ToolResult {
                    name,
                    content: ToolResultContent::Json { value },
                    ..
                } if name == "write" => Some(value.clone()),
                _ => None,
            })
            .ok_or_else(|| Error::NotFound("successful write result".into()))?;
        Ok(
            serde_json::from_value::<crate::tool::files::FileResult>(value)
                .map_err(agent)?
                .file,
        )
    };
    let directory = tempfile::tempdir().map_err(agent)?;
    let project = volume(directory.path()).await?;
    let harness = Arc::new(
        LocalHarness::open(
            volume_config(directory.path(), project.clone(), provider.clone())?
                .compaction(crate::context::CompactionPolicy::Disabled),
        )
        .await?,
    );
    let (agents, root) = harness.agents().await?;
    let (agents, root) = (Arc::clone(agents), root.clone());
    let connection = harness.connect().await?;
    let AgentReply::Spawned { agent: child, .. } = connection
        .execute(&AgentCommand::Spawn {
            parent: root.clone(),
            id: crate::agent::IntentId::try_from("isolated-write").map_err(agent)?,
            brief: ModelContent::Text("write child seed".into()),
        })
        .await?
    else {
        return Err(Error::Invalid("spawn did not return its child".into()));
    };
    let runtime = &harness.agents.get().expect("hosted agent runtime").runtime;
    let settlement = tokio::select! {
        result = settled(&agents, &child) => result.map(|_| ()),
        result = runtime.completion() => result,
    };
    if let Err(error) = settlement {
        return Err(Error::Conflict(format!(
            "child write did not settle: {error}; model_requests={}; child_state={:?}",
            provider.request_count(),
            agents.state(&child).await,
        )));
    }
    assert_eq!(provider.request_count(), 2);
    let child_file = publication(1)?;
    let child_state = agents.state(&child).await.map_err(agent)?;
    let child_project = &child_state
        .volumes
        .iter()
        .find(|binding| binding.name == "project")
        .ok_or_else(|| Error::NotFound("child project volume".into()))?
        .volume;
    assert_ne!(child_project, &project);
    assert_eq!(child_file.volume(), child_project);

    conversation(&harness)
        .await?
        .send("parent-write", "write parent seed")
        .await?;
    tokio::select! {
        result = settled(&agents, &root) => result.map(|_| ())?,
        result = runtime.completion() => result?,
    }
    assert_eq!(provider.request_count(), 4);
    let parent_file = publication(3)?;
    assert_eq!(parent_file.volume(), &project);
    for (owner, file, expected) in [
        (&child, &child_file, b"child".as_slice()),
        (&root, &parent_file, b"parent".as_slice()),
    ] {
        assert_eq!(
            Conversation::read(&connection, owner, ReadTarget::File { file: file.clone() }).await?,
            expected,
        );
    }
    // The parent's later write cannot retarget the child's retained result.
    assert!(
        Conversation::read(&connection, &child, ReadTarget::File { file: parent_file })
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn a_spawn_brief_past_one_frame_is_the_childs_first_input_exactly() -> Result<()> {
    use acyclic_harness_client::{AgentCommand, AgentReply, ReadTarget};
    // The child's brief turn, then the parent's own.
    let turns = ["child", "parent"].map(|text| super::ScriptedTurn::Text(text.into()));
    let (_directory, harness, provider) = checkout_scripted(&[], super::scripted(turns)).await?;
    let (agents, root) = harness.agents().await?;
    let (agents, root) = (Arc::clone(agents), root.clone());
    let connection = harness.connect().await?;
    // Two frames and more: the client stages the command, and the host stages
    // the brief once on the parent, where the child's adoption names it.
    let brief = "brief \u{e9}\u{1f600}\n".repeat(2 * 1024 * 1024 / 12);
    let spawn = AgentCommand::Spawn {
        parent: root.clone(),
        id: crate::agent::IntentId::try_from("large").map_err(agent)?,
        brief: crate::model::ModelContent::Text(brief.clone()),
    };
    let AgentReply::Spawned { agent: child, .. } = connection.execute(&spawn).await? else {
        return Err(Error::Invalid("spawn was not delivered".into()));
    };
    let entries = settled(&agents, &child).await?;
    let request = provider
        .request(0)
        .ok_or_else(|| Error::NotFound("child request".into()))?;
    let context = serde_json::to_string(&request).map_err(agent)?;
    assert!(context.contains(&format!("working for parent {root}")));
    assert!(context.contains("final message is your report"));
    assert!(context.contains(&format!("Use send with agent {root}")));
    let child_state = agents.state(&child).await.map_err(agent)?;
    let child_project = &child_state
        .volumes
        .iter()
        .find(|binding| binding.name == "project")
        .ok_or_else(|| Error::NotFound("child project".into()))?
        .volume;
    let root_workspace = harness
        .workspace
        .as_ref()
        .ok_or_else(|| Error::NotFound("root workspace".into()))?;
    let fork = root_workspace.spawned_project_view(child_project).await?;
    let scratch = child_state
        .volumes
        .iter()
        .find(|binding| binding.name == "scratch")
        .ok_or_else(|| Error::NotFound("child scratch".into()))?
        .volume
        .clone();
    let authority = crate::filesystem::NativeMountAuthority::admitted(
        harness.verifier.clone(),
        harness.signed.copy(&mut None)?,
    );
    let (fork, _) = root_workspace
        .bound_task_view(
            fork,
            child_project,
            scratch,
            &super::super::workspace::owner::AgentOwner(child.clone()),
            &authority,
        )
        .await?;
    // The request records the child's admitted mount, not the parent's cwd.
    let cwd = serde_json::to_string(&fork.path.to_string_lossy()).map_err(agent)?;
    assert!(context.contains(cwd.trim_matches('"')));
    assert_ne!(fork.path, root_workspace.path);
    assert!(context.contains("git_status"));
    let entry = ConversationEntry::decode(&entries[0]).map_err(agent)?;
    let TurnInput::Brief { brief: file } = entry.input else {
        return Err(Error::Invalid(
            "the child's first input is not its brief".into(),
        ));
    };
    let read = Conversation::read(&connection, &child, ReadTarget::File { file: *file }).await?;
    assert!(read == brief.as_bytes());
    // Negative control: the child reads no other file of its parent's.
    let client = conversation(&harness).await?;
    client.send("other", "a parent's own prompt").await?;
    let entries = settled(&agents, &root).await?;
    let entry = ConversationEntry::decode(&entries[0]).map_err(agent)?;
    let TurnInput::Message { body, .. } = entry.input else {
        return Err(Error::Invalid(
            "the parent's input is not its prompt".into(),
        ));
    };
    assert!(
        Conversation::read(
            &connection,
            &child,
            ReadTarget::File {
                file: (*body).clone()
            }
        )
        .await
        .is_err()
    );
    // The parent reads it, so the refusal is the grant's.
    Conversation::read(&connection, &root, ReadTarget::File { file: *body }).await?;
    // A retried spawn names the same child.
    assert!(matches!(
        connection.execute(&spawn).await?,
        AgentReply::Spawned { agent, .. } if agent == child
    ));
    Ok(())
}

#[tokio::test]
async fn a_prompt_carries_a_thousand_mentions_as_one_reference() -> Result<()> {
    use crate::model::{ModelContent, ModelContentPart};
    const MENTIONS: usize = 1_000;
    let files = (0..MENTIONS)
        .map(|index| {
            (
                format!("m/f{index}.txt"),
                format!("F{index}\n").into_bytes(),
            )
        })
        .collect::<Vec<_>>();
    let (_directory, harness, provider) = checkout_with(&files).await?;
    let (agents, root) = harness.agents().await?;
    let (agents, root) = (Arc::clone(agents), root.clone());
    let client = conversation(&harness).await?;
    let prompt = files
        .iter()
        .map(|(path, _)| format!("@{path}"))
        .collect::<Vec<_>>()
        .join(" ");
    client.send("many", &prompt).await?;
    let entries = settled(&agents, &root).await?;
    let entry = ConversationEntry::decode(&entries[0]).map_err(agent)?;
    let TurnInput::Message {
        attachments: Some(manifest),
        ..
    } = &entry.input
    else {
        return Err(Error::Conflict(
            "the prompt started no attached message turn".into(),
        ));
    };
    let listed = listed(&harness, manifest).await?;
    // Negative control: the same files listed inline in the record, as the
    // message carried them before, exceed the stream's record bound.
    let inline = serde_json::to_vec(&listed).map_err(agent)?;
    assert!(
        inline.len() > crate::agent::MAX_RECORD_BYTES,
        "{} inline bytes fit a record",
        inline.len()
    );
    // The records stay one reference: as small as a one-file prompt's.
    let inbox = records(&**agents.provider(), root.inbox().map_err(agent)?).await?;
    for record in inbox.iter().chain(&entries) {
        assert!(record.len() < 4096, "{} record bytes", record.len());
    }
    assert_eq!(listed.len(), MENTIONS);
    // The model sees the prompt and every file, in mention order.
    let request = provider.last_request().expect("one model request");
    let Some(ModelContent::Parts(parts)) = request.messages.last().map(|message| &message.content)
    else {
        return Err(Error::Conflict("the turn input has no attachments".into()));
    };
    assert_eq!(parts.len(), MENTIONS + 1);
    for (index, part) in parts[1..].iter().enumerate() {
        let ModelContentPart::File { file, .. } = part else {
            return Err(Error::Conflict("an attachment is not a file".into()));
        };
        assert_eq!(
            file.path().rsplit('/').next(),
            Some(super::super::context::copy_name(&format!("m/f{index}.txt")).as_str())
        );
    }
    Ok(())
}

#[tokio::test]
async fn a_volume_project_mention_is_copied_beside_its_manifest() -> Result<()> {
    use crate::model::ModelContent;
    const MENTIONS: usize = 32;
    let directory = tempfile::tempdir().map_err(agent)?;
    let project = volume(directory.path()).await?;
    let provider = text();
    let harness = Arc::new(
        LocalHarness::open(
            volume_config(directory.path(), project.clone(), provider.clone())?
                .compaction(crate::context::CompactionPolicy::Disabled),
        )
        .await?,
    );
    let workspace = crate::filesystem::workspace_ref(
        project.provider().clone(),
        &project.storage_name(&mut None)?,
    )?;
    let mutations = std::iter::once(crate::filesystem::WorkspaceMutation::CreateDirectory {
        path: "/d".into(),
    })
    .chain(
        (0..MENTIONS).map(|index| crate::filesystem::WorkspaceMutation::PutFile {
            path: format!("/d/n{index}.txt"),
            bytes: acyclic_stream::OwnedBytes::from(format!("{index}\n").into_bytes()),
        }),
    )
    .collect::<Vec<_>>();
    harness
        .files
        .apply(
            &workspace,
            None,
            &mutations,
            &crate::contract::IdempotencyKey::new("project-files")?,
        )
        .await?;
    let (agents, root) = harness.agents().await?;
    let (agents, root) = (Arc::clone(agents), root.clone());
    let client = conversation(&harness).await?;
    let prompt = (0..MENTIONS)
        .map(|index| format!("@d/n{index}.txt"))
        .collect::<Vec<_>>()
        .join(" ");
    client.send("project", &prompt).await?;
    let entries = settled(&agents, &root).await?;
    let entry = ConversationEntry::decode(&entries[0]).map_err(agent)?;
    let TurnInput::Message {
        attachments: Some(manifest),
        ..
    } = &entry.input
    else {
        return Err(Error::Conflict(
            "the prompt started no attached message turn".into(),
        ));
    };
    // Every member lives beside its manifest on the private volume, inside
    // the directory a forked child is granted with the manifest; none stays
    // on the project volume a child does not inherit.
    let (base, _) = manifest
        .path()
        .rsplit_once('/')
        .expect("manifest directory");
    let listed = listed(&harness, manifest).await?;
    assert_eq!(listed.len(), MENTIONS);
    for file in &listed {
        assert_eq!(file.volume(), manifest.volume());
        assert_ne!(file.volume(), &project);
        assert!(
            file.path().starts_with(&format!("{base}/")),
            "{}",
            file.path()
        );
    }
    // The turn reached the model with every file.
    let request = provider.last_request().expect("one model request");
    let Some(ModelContent::Parts(parts)) = request.messages.last().map(|message| &message.content)
    else {
        return Err(Error::Conflict("the turn input has no attachments".into()));
    };
    assert_eq!(parts.len(), MENTIONS + 1);
    Ok(())
}

/// A copy's path has one length however long the mentioned path is, so a
/// path at the path bound is mentioned like any other. The control: naming
/// a copy by its path below the same directory exceeds the bound.
#[tokio::test]
async fn a_mention_at_the_path_bound_is_copied_under_a_fixed_length_name() -> Result<()> {
    use crate::conversation::MAX_PATH_BYTES;
    let directory = tempfile::tempdir().map_err(agent)?;
    let project = volume(directory.path()).await?;
    let provider = text();
    let harness = Arc::new(
        LocalHarness::open(
            volume_config(directory.path(), project.clone(), provider.clone())?
                .compaction(crate::context::CompactionPolicy::Disabled),
        )
        .await?,
    );
    let workspace = crate::filesystem::workspace_ref(
        project.provider().clone(),
        &project.storage_name(&mut None)?,
    )?;
    // Nested 200-byte components, then a file name, to one byte under the bound.
    let mut parents = Vec::new();
    let mut long = String::new();
    while MAX_PATH_BYTES - 1 - long.len() > 200 {
        long.push_str(&format!("{}{}/", parents.len() % 10, "d".repeat(199)));
        parents.push(format!("/{}", long.trim_end_matches('/')));
    }
    long.push_str(&"f".repeat(MAX_PATH_BYTES - 1 - long.len()));
    assert_eq!(long.len() + 1, MAX_PATH_BYTES);
    let mutations = parents
        .into_iter()
        .map(|path| crate::filesystem::WorkspaceMutation::CreateDirectory { path })
        .chain(
            [("short.txt", "short\n"), (long.as_str(), "long\n")].map(|(path, text)| {
                crate::filesystem::WorkspaceMutation::PutFile {
                    path: format!("/{path}"),
                    bytes: acyclic_stream::OwnedBytes::from(text.as_bytes().to_vec()),
                }
            }),
        )
        .collect::<Vec<_>>();
    harness
        .files
        .apply(
            &workspace,
            None,
            &mutations,
            &crate::contract::IdempotencyKey::new("project-files")?,
        )
        .await?;
    let (agents, root) = harness.agents().await?;
    let (agents, root) = (Arc::clone(agents), root.clone());
    let client = conversation(&harness).await?;
    client.send("long", &format!("@short.txt @{long}")).await?;
    let entries = settled(&agents, &root).await?;
    let entry = ConversationEntry::decode(&entries[0]).map_err(agent)?;
    let TurnInput::Message {
        attachments: Some(manifest),
        ..
    } = &entry.input
    else {
        return Err(Error::Conflict(
            "the prompt started no attached message turn".into(),
        ));
    };
    let listed = listed(&harness, manifest).await?;
    assert_eq!(listed.len(), 2);
    assert_eq!(listed[0].path().len(), listed[1].path().len());
    let reader = harness.client_reader()?;
    for (file, text) in listed.iter().zip(["short\n", "long\n"]) {
        let bytes = reader
            .read(file, file.descriptor().byte_length(), None, None)?
            .await?;
        assert_eq!(bytes.as_ref(), text.as_bytes());
    }
    let (directory, _) = listed[1].path().rsplit_once('/').expect("copy directory");
    assert!(format!("{directory}/{long}").len() > MAX_PATH_BYTES);
    Ok(())
}

#[tokio::test]
async fn a_small_file_bound_never_fails_a_turn_on_its_lists() -> Result<()> {
    use crate::model::{ModelContent, ModelRole};
    const MENTIONS: usize = 32;
    // Each mentioned file and all of them together fit this bound; the
    // attachment manifest, the turn's context manifest and its journaled
    // input do not.
    const FILE_BYTES: u64 = 1024;
    let directory = tempfile::tempdir().map_err(agent)?;
    let project = volume(directory.path()).await?;
    let provider = super::scripted([
        crate::test_support::ScriptedTurn::Text("done".into()),
        crate::test_support::ScriptedTurn::Text("done".into()),
    ]);
    let harness = Arc::new(
        LocalHarness::open(
            volume_config(directory.path(), project.clone(), provider.clone())?
                .compaction(crate::context::CompactionPolicy::Disabled)
                .limits(crate::conversation::Limits {
                    file_bytes: FILE_BYTES,
                    ..Default::default()
                }),
        )
        .await?,
    );
    let workspace = crate::filesystem::workspace_ref(
        project.provider().clone(),
        &project.storage_name(&mut None)?,
    )?;
    let mutations = std::iter::once(crate::filesystem::WorkspaceMutation::CreateDirectory {
        path: "/d".into(),
    })
    .chain(
        (0..MENTIONS).map(|index| crate::filesystem::WorkspaceMutation::PutFile {
            path: format!("/d/n{index}.txt"),
            bytes: acyclic_stream::OwnedBytes::from(format!("{index}\n").into_bytes()),
        }),
    )
    .collect::<Vec<_>>();
    harness
        .files
        .apply(
            &workspace,
            None,
            &mutations,
            &crate::contract::IdempotencyKey::new("project-files")?,
        )
        .await?;
    let (agents, root) = harness.agents().await?;
    let (agents, root) = (Arc::clone(agents), root.clone());
    let client = conversation(&harness).await?;
    let prompt = (0..MENTIONS)
        .map(|index| format!("@d/n{index}.txt"))
        .collect::<Vec<_>>()
        .join(" ");
    client.send("small", &prompt).await?;
    let entries = settled(&agents, &root).await?;
    let entry = ConversationEntry::decode(&entries[0]).map_err(agent)?;
    let TurnInput::Message {
        attachments: Some(manifest),
        ..
    } = &entry.input
    else {
        return Err(Error::Conflict(
            "the prompt started no attached message turn".into(),
        ));
    };
    // Control: the list alone exceeds the configured file bound.
    assert!(manifest.descriptor().byte_length() > FILE_BYTES);
    let attached = |content: &ModelContent| match content {
        ModelContent::Parts(parts) => parts.len(),
        _ => 1,
    };
    let request = provider.last_request().expect("one model request");
    assert_eq!(
        request
            .messages
            .last()
            .map(|message| attached(&message.content)),
        Some(MENTIONS + 1)
    );
    // The next turn replays that message, attachments expanded, under the
    // same bound.
    client.send("next", "next").await?;
    let conversation = root.conversation().map_err(agent)?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(8);
    while records(&**agents.provider(), conversation.clone())
        .await?
        .len()
        < 2
    {
        assert!(
            std::time::Instant::now() < deadline,
            "the second turn did not settle"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let request = provider.last_request().expect("a second model request");
    let replayed = request
        .messages
        .iter()
        .filter(|message| message.role == ModelRole::User)
        .map(|message| attached(&message.content))
        .collect::<Vec<_>>();
    assert_eq!(replayed, vec![MENTIONS + 1, 1]);
    Ok(())
}
