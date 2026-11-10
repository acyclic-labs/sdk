//! Session controls and projections over the ordinary authenticated connection.
//! Titles are authored files; list rows and prompt previews are disposable.
use super::{Connection, agent_conversation::Conversation, storage};
use crate::{
    Error, OperationId, Result,
    agent::{AgentPath, IntentId, StatusRecord, TurnInput},
    client::{AgentCommand, AgentPageRequest, AgentReply, ReadTarget},
    client_files::ClientFileChange,
};

/// One visible root-session row. Nothing in this projection is stored separately.
#[derive(Clone, Debug, serde::Serialize)]
pub struct Session {
    /// Canonical root identity.
    pub agent: AgentPath,
    /// Authored ui/title, otherwise the first prompt's bounded preview.
    pub title: Option<String>,
    /// Last published phase and conversation cut at the listing's snapshot.
    pub status: Option<StatusRecord>,
}

/// One bounded page of roots and its original exclusive continuation.
#[derive(Debug, serde::Serialize)]
pub struct Page {
    /// Only the rows the caller requested.
    pub items: Vec<Session>,
    /// Pass unchanged when the viewer asks for another page.
    pub next: Option<String>,
}

fn unexpected() -> Error {
    Error::Invalid("session command returned another reply".into())
}

/// Create a root through the host's one admitted volume/actor publication.
pub async fn create(connection: &Connection, operation: &str) -> Result<Connection> {
    let id = IntentId::try_from(operation).map_err(storage)?;
    let AgentReply::Root { agent } = connection.execute(&AgentCommand::NewSession { id }).await?
    else {
        return Err(unexpected());
    };
    connection.select(agent).await
}

/// Read the latest committed directory entry without walking any sessions.
pub async fn latest(connection: &Connection) -> Result<Option<Connection>> {
    let AgentReply::LatestSession { agent } =
        connection.execute(&AgentCommand::LatestSession {}).await?
    else {
        return Err(unexpected());
    };
    match agent {
        Some(agent) => connection.select(agent).await.map(Some),
        None => Ok(None),
    }
}

/// Select a canonical path or a root's complete stable id. Naming does not grant access.
pub async fn select(connection: &Connection, name: &str) -> Result<Connection> {
    let agent = if name.contains('/') {
        AgentPath::try_from(name.to_owned()).map_err(storage)?
    } else {
        AgentPath::root(&crate::agent::AgentId::try_from(name.to_owned()).map_err(storage)?)
    };
    connection.select(agent).await
}

/// Page roots, then read only those visible rows' title files. A missing authored
/// title reads one bounded chunk of the first prompt; no transcript scan occurs.
pub async fn page(connection: &Connection, page: AgentPageRequest) -> Result<Page> {
    let AgentReply::Children { items, next } =
        connection.execute(&AgentCommand::Sessions { page }).await?
    else {
        return Err(unexpected());
    };
    let items = futures::future::try_join_all(items.into_iter().map(|item| async move {
        let AgentReply::ClientFiles { snapshot } = connection
            .execute(&AgentCommand::AgentFiles {
                agent: item.agent.clone(),
                paths: vec!["ui/title".into()],
                after: None,
            })
            .await?
        else {
            return Err(unexpected());
        };
        let mut title = snapshot
            .files
            .get("ui/title")
            .and_then(Option::as_ref)
            .map(|file| file.text.clone());
        if title.is_none()
            && item
                .status
                .as_ref()
                .is_some_and(|status| status.conversation > 0)
        {
            let selected = connection.select(item.agent.clone()).await?;
            let conversation = Conversation::open(selected);
            if let Some(entry) = conversation.entries(0, 1).await?.first()
                && let TurnInput::Message { body, .. } = &entry.input
            {
                let (bytes, _) = Conversation::read_at(
                    connection,
                    &item.agent,
                    ReadTarget::File {
                        file: *body.clone(),
                    },
                    0,
                )
                .await?;
                title = Some(String::from_utf8_lossy(&bytes).into_owned());
            }
        }
        Ok(Session {
            agent: item.agent,
            title,
            status: item.status,
        })
    }))
    .await?;
    Ok(Page { items, next })
}

/// Rename by one conditional file publication; a competing rename is a conflict.
pub async fn rename(connection: &Connection, operation: OperationId, title: String) -> Result<()> {
    let agent = connection.root().clone();
    let AgentReply::ClientFiles { snapshot } = connection
        .execute(&AgentCommand::AgentFiles {
            agent: agent.clone(),
            paths: vec!["ui/title".into()],
            after: None,
        })
        .await?
    else {
        return Err(unexpected());
    };
    let existing = snapshot.files.get("ui/title").and_then(Option::as_ref);
    let change = ClientFileChange {
        path: "ui/title".into(),
        expected: existing.map(|file| file.file.clone()),
        text: title,
    };
    let AgentReply::ClientFilesPublished { .. } = connection
        .execute(&AgentCommand::PublishAgentFiles {
            agent,
            operation,
            changes: vec![change],
        })
        .await?
    else {
        return Err(unexpected());
    };
    Ok(())
}

/// Archive uses the agent's existing closing input and owned-resource release.
pub async fn archive(connection: &Connection, operation: &str) -> Result<()> {
    let id = IntentId::try_from(operation).map_err(storage)?;
    let AgentReply::Delivered { .. } = connection
        .execute(&AgentCommand::Archive {
            agent: connection.root().clone(),
            id,
        })
        .await?
    else {
        return Err(unexpected());
    };
    Ok(())
}
