//! The local agent host: a prompt is one staged file and one inbox commit, and
//! the root agent's actor runs its turn on the stock executor.

use super::super::LocalAgents;
use super::{text, volume, volume_config};
use crate::{
    Error, LocalHarness, Result,
    agent::{
        AgentPath, AgentRecord, ConversationEntry, Delivered, IntentId, StatusRecord, TurnInput,
    },
};
use acyclic_stream::{ReadRequest, StreamError, StreamPath, StreamProvider};
use futures::TryStreamExt;
use std::sync::Arc;

fn agent(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}

async fn records(stream: &impl StreamProvider, path: StreamPath) -> Result<Vec<Vec<u8>>> {
    match stream
        .read(ReadRequest {
            path,
            from: 0,
            limit: 16,
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
    let conversation = root.conversation().map_err(agent)?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(8);
    loop {
        let entries = records(&**agents.provider(), conversation.clone()).await?;
        if !entries.is_empty() {
            return Ok(entries);
        }
        if std::time::Instant::now() > deadline {
            let statuses = records(&**agents.provider(), root.status().map_err(agent)?).await?;
            let statuses = statuses
                .iter()
                .map(|status| StatusRecord::decode(status).map(|status| status.phase))
                .collect::<Vec<_>>();
            return Err(Error::Conflict(format!(
                "no settled turn; statuses {statuses:?}"
            )));
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
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
    let id = IntentId::try_from("hello").map_err(agent)?;
    let delivered = harness.prompt(&id, "hello").await.map_err(agent)?;
    let Delivered::Committed { inbox } = delivered else {
        return Err(Error::Conflict(format!(
            "prompt was not delivered: {delivered:?}"
        )));
    };
    let conversation = root.conversation().map_err(agent)?;
    let entries = settled(&agents, &root).await?;
    assert_eq!(entries.len(), 1);
    let entry = ConversationEntry::decode(&entries[0]).map_err(agent)?;
    assert!(matches!(entry.input, TurnInput::Message { inbox: settled, .. } if settled == inbox));
    assert_eq!(provider.requests().len(), 1);
    // A client reads the settled output from the root's private volume.
    let output = harness
        .client_reader()?
        .read(&entry.output, 1 << 20, None, None)?
        .await?;
    assert!(!output.is_empty());
    // The same intent replays its commit at the same sequence; no second turn runs.
    assert_eq!(
        harness.prompt(&id, "hello").await.map_err(agent)?,
        delivered
    );
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
    let directory = tempfile::tempdir().map_err(agent)?;
    let provider = super::scripted([
        super::checked_commands::call(0, crate::tools::shell::PLATFORM.name, "echo staged"),
        crate::test_support::ScriptedTurn::Text("done".into()),
    ]);
    let harness = Arc::new(
        LocalHarness::open(super::checkout_config(
            directory.path(),
            (super::model()?, provider),
        )?)
        .await?,
    );
    let (agents, root) = harness.agents().await?;
    let (agents, root) = (Arc::clone(agents), root.clone());
    let id = IntentId::try_from("shell").map_err(agent)?;
    harness
        .prompt(&id, "run one command")
        .await
        .map_err(agent)?;
    let entries = settled(&agents, &root).await?;
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
    follow_tool_output(&harness, &state.path, &turn_id).await
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
    let mut observer = harness.observe(root, 1).await?;
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
    // Revoking the scope the observation derives from ends it.
    authority()?.issuer.revoke_workload(&observer.scope)?;
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
