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
    settled(&agents, &root).await?;
    let state = agents.state(&root).await.map_err(agent)?;
    let private = &state
        .volumes
        .iter()
        .find(|binding| binding.name == crate::filesystem::PRIVATE_VOLUME)
        .ok_or_else(|| Error::NotFound("private volume".into()))?
        .volume;
    let calls = names(&harness, private, "tools").await?;
    let [call] = calls.as_slice() else {
        return Err(Error::Conflict(format!(
            "one shell call staged, got {calls:?}"
        )));
    };
    assert_eq!(
        names(&harness, private, &format!("tools/{call}")).await?,
        ["request.json", "result.json", "stdout"]
    );
    assert_eq!(
        names(&harness, private, &format!("tools/{call}/stdout")).await?,
        ["0000000000000000"]
    );
    Ok(())
}
