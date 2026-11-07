//! Session files use the conversation's existing authenticated connection.
use super::*;
use crate::{
    OperationId,
    client_files::{ClientFileChange, ClientFiles},
    resources::GenerationRef,
};

impl Conversation {
    /// The default client session of this selected conversation. Callers that
    /// need independent UI sessions supply their own stable session identity.
    #[must_use]
    pub fn client_session(&self) -> OperationId {
        OperationId::from_bytes(self.agent().id_bytes())
    }

    /// Snapshot explicit session paths, optionally waiting for the original
    /// filesystem publication event after a previously observed generation.
    pub async fn client_files(
        &self,
        session: OperationId,
        paths: Vec<String>,
        after: Option<GenerationRef>,
    ) -> Result<ClientFiles> {
        match self
            .connection
            .execute(&AgentCommand::ClientFiles {
                agent: self.agent().clone(),
                session,
                paths,
                after,
            })
            .await?
        {
            AgentReply::ClientFiles { snapshot } => Ok(snapshot),
            _ => Err(unexpected()),
        }
    }

    /// Publish an atomic conditional file batch. An uncertain outcome retains
    /// the caller's operation identity and exact original source descriptors.
    pub async fn publish_client_files(
        &self,
        session: OperationId,
        operation: OperationId,
        changes: Vec<ClientFileChange>,
    ) -> Result<GenerationRef> {
        match self
            .connection
            .execute(&AgentCommand::PublishClientFiles {
                agent: self.agent().clone(),
                session,
                operation,
                changes,
            })
            .await?
        {
            AgentReply::ClientFilesPublished { generation } => Ok(generation),
            _ => Err(unexpected()),
        }
    }
}
