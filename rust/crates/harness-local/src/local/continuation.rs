//! CLI-independent entry to the ordinary historical agent fork.
use super::Connection;
use crate::client::{AgentCommand, AgentReply};
use crate::{
    Error, Result,
    agent::{AgentPath, ContinuationRequest, IntentId},
    model::ModelContent,
};

/// Fork the decider at `AGENT_PATH@EVENT_OFFSET` (inclusive, zero-based
/// conversation event). The resulting connection is driven like any normal
/// run. Reuse `operation` with identical inputs after an uncertain outcome.
///
/// # Errors
/// Returns an error for malformed cuts, unavailable history, non-decider
/// sources, or failure to commit the ordinary agent fork.
pub async fn fork_from(
    connection: &Connection,
    source: &str,
    operation: &str,
    request: ContinuationRequest,
) -> Result<Connection> {
    let (run, event) = source
        .rsplit_once('@')
        .ok_or_else(|| Error::Invalid("fork-from requires AGENT_PATH@EVENT_OFFSET".into()))?;
    let parent =
        AgentPath::try_from(run.to_owned()).map_err(|error| Error::Invalid(error.to_string()))?;
    let at = event
        .parse()
        .map_err(|_| Error::Invalid("fork event offset must be a u64".into()))?;
    let id = IntentId::try_from(operation).map_err(|error| Error::Invalid(error.to_string()))?;
    let reply = connection
        .execute(&AgentCommand::Fork {
            parent,
            id,
            at,
            brief: ModelContent::Text("Continue from the selected boundary.".into()),
            role: "decider".into(),
            continuation: Some(request),
        })
        .await?;
    let AgentReply::Spawned { agent, .. } = reply else {
        return Err(Error::Invalid("fork returned another reply".into()));
    };
    connection.select(agent).await
}
