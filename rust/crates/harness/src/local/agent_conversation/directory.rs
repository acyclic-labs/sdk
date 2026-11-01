//! Directory pages through the selected conversation's original connection.
use super::{Conversation, unexpected};
use crate::{
    Result,
    conversation::{DirectoryChanges, PrivateDirectoryPage},
    resources::GenerationRef,
};
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

    /// Read one authorized private directory page. The initial page returns its
    /// matching original publication fence. Continue with that exact generation
    /// and the last returned child name as the page cursor. Neither a name nor
    /// a fence is a grant.
    ///
    /// # Errors
    /// Original credential, policy, source or shared reply validation failure.
    pub async fn directory_page(
        &self,
        path: &str,
        generation: Option<&GenerationRef>,
        page: AgentPageRequest,
    ) -> Result<(PrivateDirectoryPage, Option<DirectoryChanges>)> {
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
            AgentReply::Directory { page, changes } => Ok((page, changes)),
            _ => Err(unexpected()),
        }
    }
}
