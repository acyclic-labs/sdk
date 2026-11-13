//! Directory pages through the selected conversation's original connection.
use super::{Conversation, unexpected};
use crate::{
    Result,
    conversation::{DirectoryTarget, PrivateDirectoryPage},
    resources::GenerationRef,
};
use acyclic_harness_client::{AgentCommand, AgentPageRequest, AgentReply, ReadTarget};

impl Conversation {
    /// Read a bounded completion page at an exact original status record.
    pub async fn child_wakes(
        &self,
        status: u64,
        page: AgentPageRequest,
    ) -> Result<(Vec<crate::agent::ChildWake>, Option<String>)> {
        match self
            .connection
            .execute(&AgentCommand::ChildWakes {
                agent: self.agent().clone(),
                status,
                page,
            })
            .await?
        {
            AgentReply::ChildWakes { items, next } => Ok((items, next)),
            _ => Err(unexpected()),
        }
    }

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
            ReadTarget::Path {
                target: DirectoryTarget::Private {},
                path: path.to_owned(),
                generation: generation.clone(),
            },
            offset,
        )
        .await
    }

    /// Read visible private names. Continue with the returned immutable
    /// generation and the opaque `next` cursor, including after empty pages.
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
                target: DirectoryTarget::Private {},
                path: path.to_owned(),
                generation: generation.cloned(),
                after_generation: None,
                page,
            })
            .await?
        {
            AgentReply::Directory { page } => Ok(page),
            _ => Err(unexpected()),
        }
    }
}
