//! [`TurnEffects`] as internal files on the agent's private volume: one file
//! `effects/<turn>/<call>.json` per agent tool call, in the internal interaction
//! area, so no file tool can forge an effect past the call's approval.

use super::agent_internal::AgentInternalFiles;
use crate::{
    Error, OperationId, Result,
    agent::{AgentPath, TurnEffect, TurnEffects},
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use acyclic_stream::{BoxProviderFuture, StreamProvider};
use std::sync::Arc;

/// The effects of one agent turn.
pub(in crate::filesystem) struct FilesystemTurnEffects<P, A: 'static, O: 'static> {
    files: Arc<AgentInternalFiles<A, O>>,
    stream: Arc<P>,
    agent: AgentPath,
    directory: String,
    maximum_bytes: u64,
}

impl<P, A, O> FilesystemTurnEffects<P, A, O> {
    pub(in crate::filesystem) fn new(
        files: Arc<AgentInternalFiles<A, O>>,
        stream: Arc<P>,
        agent: AgentPath,
        turn: OperationId,
        maximum_bytes: u64,
    ) -> Self {
        Self {
            files,
            stream,
            agent,
            directory: format!("effects/{turn}"),
            maximum_bytes,
        }
    }
}

fn decode(bytes: &[u8]) -> Result<TurnEffect> {
    serde_json::from_slice(bytes)
        .map_err(|_| Error::Storage("recorded agent effect is malformed".into()))
}

impl<P, A, O> TurnEffects for FilesystemTurnEffects<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn record<'a>(
        &'a self,
        call: &'a str,
        effect: &'a TurnEffect,
    ) -> BoxProviderFuture<'a, Result<TurnEffect>> {
        Box::pin(async move {
            if call.is_empty()
                || !call
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            {
                return Err(Error::Invalid("agent effect call key is not a file name".into()));
            }
            // Calls run one at a time, so the number of effects already
            // recorded is this call's position in the turn; a retried call
            // finds its own file by its key.
            let names = self.files.list(&self.directory).await?;
            let suffix = format!("-{call}.json");
            if let Some(name) = names.iter().find(|name| name.ends_with(&suffix)) {
                let existing = self
                    .files
                    .read(&format!("{}/{name}", self.directory), self.maximum_bytes)
                    .await?
                    .ok_or_else(|| Error::Storage("listed agent effect is missing".into()))?;
                return decode(&existing);
            }
            let path = format!("{}/{:010}{suffix}", self.directory, names.len());
            // A spawn names a new child: a name already taken would make the
            // step's fork conflict on every retry.
            if let TurnEffect::Spawn { intent, .. } = effect {
                let child = TurnEffect::child(&self.agent, intent)
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                match crate::agent::inbox_tail(&*self.stream, &child).await {
                    Err(crate::agent::Error::NoSuchAgent) => {}
                    Ok(_) => return Err(Error::Conflict("an agent with this name exists".into())),
                    Err(error) => return Err(Error::Storage(error.to_string())),
                }
            }
            let bytes = crate::contract::canonical_json_bytes(effect)?;
            match self.files.create(&path, &bytes, self.maximum_bytes).await? {
                Some(existing) => decode(&existing),
                None => Ok(effect.clone()),
            }
        })
    }

    fn recorded(&self) -> BoxProviderFuture<'_, Result<Vec<TurnEffect>>> {
        Box::pin(async move {
            let mut recorded = Vec::new();
            for name in self.files.list(&self.directory).await? {
                let bytes = self
                    .files
                    .read(&format!("{}/{name}", self.directory), self.maximum_bytes)
                    .await?
                    .ok_or_else(|| Error::Storage("listed agent effect is missing".into()))?;
                recorded.push(decode(&bytes)?);
            }
            Ok(recorded)
        })
    }
}
