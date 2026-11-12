//! Directory pages through the selected conversation's original connection.
use super::{Conversation, unexpected};
use crate::{Result, conversation::PrivateDirectoryPage, resources::GenerationRef};
use acyclic_harness_client::{AgentCommand, AgentPageRequest, AgentReply, ReadTarget};

/// The query and the original connection which serves its pinned selection.
pub struct ProjectDirectory {
    query: acyclic_harness_client::DirectoryQuery,
    _connection: super::Connection,
}

impl ProjectDirectory {
    /// Consume original source progress; an empty page is not completion.
    pub async fn next(
        &mut self,
    ) -> std::result::Result<
        Option<Vec<crate::conversation::PrivateDirectoryEntry>>,
        acyclic_harness_client::AgentExchangeError,
    > {
        self.query.next().await
    }
}

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
                target: acyclic_harness_client::DirectoryTarget::Private,
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

    /// Names in one project directory at its original source selection.
    pub async fn project_directory(
        &self,
        path: String,
        page: AgentPageRequest,
    ) -> std::result::Result<ProjectDirectory, acyclic_harness_client::AgentExchangeError> {
        let query = self
            .connection
            .controls
            .directory_query(self.agent().clone(), path, page)
            .await?;
        Ok(ProjectDirectory {
            query,
            _connection: self.connection.clone(),
        })
    }
}
