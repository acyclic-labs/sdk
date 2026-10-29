//! The local agent host: a prompt is one staged file and one inbox commit, and
//! the root agent's actor runs its turn on the stock executor.

use super::super::LocalAgents;
use super::{ScriptedModelProvider, text, volume, volume_config};
use crate::{
    Error, LocalHarness, Result,
    agent::{AgentPath, AgentRecord, ConversationEntry, StatusRecord, TurnInput},
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

/// The root's conversation once its first turn has settled.
async fn settled(agents: &LocalAgents, root: &AgentPath) -> Result<Vec<Vec<u8>>> {
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
                crate::agent::Phase::Idle if status.cursor > 0 => {
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
    let mut failures = harness
        .agent_failures()
        .ok_or_else(|| Error::NotFound("agent failures".into()))?;
    conversation(&harness)
        .await?
        .send("shell", "run one command")
        .await?;
    let entries = tokio::select! {
        result = settled(&agents, &root) => result,
        failure = failures.recv() => Err(Error::Conflict(format!("shell turn failed before settlement: {failure:?}"))),
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
    let authority = move || {
        crate::filesystem::agent_authority(
            issuer,
            &crate::filesystem::TurnAdmission::LocalRoot,
            path,
            volumes,
            &[],
        )
    };
    // A structured scope of `actions` on `paths`, for the same agent.
    let structured = |actions: &[Action], paths: &[&str]| -> Result<_> {
        let mut scoped = authority()?;
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
        scoped.scope = scoped.issuer.bootstrap_scope(
            "observer",
            &crate::Capabilities::new([capability.to_scope_label().map_err(agent)?]),
            scoped.scope.agent(),
            &mut None,
        )?;
        Ok(scoped)
    };
    // Negative control: the same agent, attenuated to no grants, gets none.
    let mut denied = authority()?;
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
    let writer = structured(&[Action::Create, Action::Write], &["**"])?;
    assert!(
        observation(harness, &writer, path).await.is_err(),
        "a write-only structured scope yields no credential"
    );
    let reader = structured(&[Action::List, Action::Read], &["tools", "tools/**"])?;
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
    let unrelated = authority()?;
    unrelated.issuer.revoke_workload(&unrelated.scope)?;
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
            let mut used = 0;
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
    let mut failures = harness
        .agent_failures()
        .ok_or_else(|| Error::NotFound("agent failures".into()))?;
    conversation(&harness)
        .await?
        .send("hooked", "hello")
        .await?;
    tokio::time::timeout(std::time::Duration::from_secs(8), async {
        tokio::select! {
            request = requests.recv() => request.ok_or_else(|| Error::Storage("model dispatch producer closed".into())),
            failure = failures.recv() => Err(Error::Conflict(format!("hooked turn failed before model dispatch: {failure:?}"))),
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
    let directory = tempfile::tempdir().map_err(agent)?;
    std::fs::create_dir(directory.path().join(".git")).map_err(agent)?;
    let provider = text();
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
    let first = harness.prompt_attachments(b"direct", prompt).await?;
    assert_eq!(harness.prompt_attachments(b"direct", prompt).await?, first);
    let inbox = client.send("mention", prompt).await?;
    let entries = settled(&agents, &root).await?;
    // Later publications on the private volume leave a replay unchanged.
    assert_eq!(harness.prompt_attachments(b"direct", prompt).await?, first);
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
                "mention.txt".into(),
                "text/plain".into(),
                b"MENTIONED\n".to_vec()
            ),
            ("pixel.png".into(), "image/png".into(), PIXEL.to_vec()),
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
    assert_eq!(harness.prompt_attachments(b"direct", prompt).await?, first);
    assert_eq!(client.send("mention", prompt).await?, inbox);
    // A new message sees the files as they are now.
    let fresh = harness
        .prompt_attachments(b"fresh", "inspect @mention.txt")
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
            Some(format!("f{index}.txt").as_str())
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
