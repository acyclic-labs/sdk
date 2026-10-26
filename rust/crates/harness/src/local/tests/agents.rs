//! The local agent host: a prompt is one staged file and one inbox commit, and
//! the root agent's actor runs its turn on the stock executor.

use super::{text, volume, volume_config};
use crate::{
    Error, LocalHarness, Result,
    agent::{AgentRecord, ConversationEntry, Delivered, IntentId, StatusRecord, TurnInput},
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
        return Err(Error::Conflict(format!("prompt was not delivered: {delivered:?}")));
    };
    let conversation = root.conversation().map_err(agent)?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(8);
    let entries = loop {
        let entries = records(&**agents.provider(), conversation.clone()).await?;
        if !entries.is_empty() {
            break entries;
        }
        if std::time::Instant::now() > deadline {
            let statuses = records(&**agents.provider(), root.status().map_err(agent)?).await?;
            let statuses = statuses
                .iter()
                .map(|status| StatusRecord::decode(status).map(|status| status.phase))
                .collect::<Vec<_>>();
            return Err(Error::Conflict(format!("no settled turn; statuses {statuses:?}")));
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    };
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
    assert_eq!(harness.prompt(&id, "hello").await.map_err(agent)?, delivered);
    assert_eq!(records(&**agents.provider(), conversation).await?.len(), 1);
    Ok(())
}
