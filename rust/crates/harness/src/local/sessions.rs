//! Session controls and projections over the ordinary authenticated connection.
//! Titles are authored files; list rows and prompt previews are disposable.
use super::{Connection, agent_conversation::Conversation, storage};
use crate::{
    Error, OperationId, Result,
    agent::{AgentPath, IntentId, StatusRecord, TurnInput},
    client::{AgentCommand, AgentPageRequest, AgentReply},
    client_files::ClientFileChange,
};
use acyclic_harness_client::ReadTarget;

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

/// Fork a session at one settled turn. The source keeps its later history and
/// project files; the returned session remains paused for inspection.
pub async fn fork(
    connection: &Connection,
    source: AgentPath,
    at: u64,
    operation: &str,
) -> Result<Connection> {
    let id = IntentId::try_from(operation).map_err(storage)?;
    let AgentReply::Root { agent } = connection
        .execute(&AgentCommand::ForkSession {
            agent: source,
            at,
            id,
        })
        .await?
    else {
        return Err(unexpected());
    };
    connection.select(agent).await
}

/// Persist an explicit user attach, without changing read-only selection.
pub async fn selected(connection: &Connection, operation: &str) -> Result<()> {
    let id = IntentId::try_from(operation).map_err(storage)?;
    let AgentReply::Root { .. } = connection
        .execute(&AgentCommand::SelectSession {
            agent: connection.root().clone(),
            id,
        })
        .await?
    else {
        return Err(unexpected());
    };
    Ok(())
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

/// Select a canonical path, complete stable id or exact indexed alias.
/// Naming does not grant access.
pub async fn select(connection: &Connection, name: &str) -> Result<Connection> {
    let agent = if let Ok(id) = crate::agent::AgentId::try_from(name.to_owned()) {
        AgentPath::root(&id)
    } else if let Ok(path) = AgentPath::try_from(name.to_owned()) {
        path
    } else {
        let AgentReply::Root { agent } = connection
            .execute(&AgentCommand::ResolveSession { name: name.into() })
            .await?
        else {
            return Err(unexpected());
        };
        agent
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
        let AgentReply::FilePreviews { snapshot } = connection
            .execute(&AgentCommand::AgentFilePreviews {
                agent: item.agent.clone(),
                paths: vec!["ui/title".into()],
                after: None,
                bytes: crate::client::MAX_READ_BYTES,
            })
            .await?
        else {
            return Err(unexpected());
        };
        let mut title = snapshot
            .files
            .get("ui/title")
            .and_then(Option::as_ref)
            .cloned();
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
                    ReadTarget::Conversation {
                        sequence: 0,
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

/// Bind an additional alias through the scope's shared immutable name index.
pub async fn name(connection: &Connection, name: String, operation: &str) -> Result<()> {
    let id = IntentId::try_from(operation).map_err(storage)?;
    let AgentReply::Root { .. } = connection
        .execute(&AgentCommand::NameSession {
            agent: connection.root().clone(),
            name,
            id,
        })
        .await?
    else {
        return Err(unexpected());
    };
    Ok(())
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

/// Pause each parent before paging its children, so a paused subtree cannot
/// grow behind the traversal. Resume uses the same existing actor control.
/// Only the current page and the path back through the tree are retained.
pub fn pause_subtree<'a>(
    connection: &'a Connection,
    agent: AgentPath,
    paused: bool,
) -> acyclic_stream::BoxProviderFuture<'a, Result<()>> {
    Box::pin(async move {
        let AgentReply::Paused { .. } = connection
            .execute(&AgentCommand::Pause {
                agent: agent.clone(),
                paused,
            })
            .await?
        else {
            return Err(unexpected());
        };
        let mut cursor = None;
        loop {
            let AgentReply::Children { items, next } = connection
                .execute(&AgentCommand::Children {
                    agent: agent.clone(),
                    page: AgentPageRequest::new(32, 32 * 1024, cursor).map_err(storage)?,
                })
                .await?
            else {
                return Err(unexpected());
            };
            for child in items {
                pause_subtree(connection, child.agent, paused).await?;
            }
            let Some(next) = next else {
                return Ok(());
            };
            cursor = Some(next);
        }
    })
}

/// Explicit all-session control visits directory pages without retaining a roster.
pub async fn pause_all(connection: &Connection, paused: bool) -> Result<()> {
    let mut cursor = None;
    loop {
        let AgentReply::Children { items, next } = connection
            .execute(&AgentCommand::Sessions {
                page: AgentPageRequest::new(32, 32 * 1024, cursor).map_err(storage)?,
            })
            .await?
        else {
            return Err(unexpected());
        };
        for root in items {
            pause_subtree(connection, root.agent, paused).await?;
        }
        let Some(next) = next else {
            return Ok(());
        };
        cursor = Some(next);
    }
}

/// Integrate the selected root through the ordinary filesystem merge. Retain
/// this operation on uncertainty or while resolving its checkout conflict files.
pub async fn merge(connection: &Connection, operation: &str) -> Result<Option<String>> {
    let id = IntentId::try_from(operation).map_err(storage)?;
    match connection
        .execute(&AgentCommand::MergeSession {
            agent: connection.root().clone(),
            id,
        })
        .await?
    {
        AgentReply::Merged { commit } => Ok(commit),
        _ => Err(unexpected()),
    }
}
