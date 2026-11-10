//! Directory pages through the selected conversation's original connection.
use super::{Conversation, unexpected};
use crate::{Result, conversation::PrivateDirectoryPage, resources::GenerationRef};
use acyclic_harness_client::{AgentCommand, AgentPageRequest, AgentReply, ReadTarget};

impl Conversation {
    /// Read one bounded file range at the original directory generation.
    /// Continue from the returned byte count until reaching the whole length.
    ///
    /// # Errors
    /// Original credential, policy, source or shared reply validation failure.
    pub async fn read_private_range(
        &self,
        path: &str,
        generation: &GenerationRef,
        offset: u64,
    ) -> Result<(Vec<u8>, u64)> {
        Self::read_at(
            &self.connection,
            self.agent(),
            ReadTarget::Private {
                path: path.to_owned(),
                generation: generation.clone(),
            },
            offset,
        )
        .await
    }

    /// Read visible private names. Continue with the returned immutable
    /// generation and the last child name as the exclusive cursor.
    ///
    /// # Errors
    /// Original credential, policy, source or shared reply validation failure.
    pub async fn directory_page(
        &self,
        path: &str,
        generation: Option<&GenerationRef>,
        page: AgentPageRequest,
    ) -> Result<PrivateDirectoryPage> {
        match self
            .connection
            .execute(&AgentCommand::Directory {
                agent: self.agent().clone(),
                path: path.to_owned(),
                generation: generation.cloned(),
                page,
            })
            .await?
        {
            AgentReply::Directory { page } => Ok(page),
            _ => Err(unexpected()),
        }
    }
}
