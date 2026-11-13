//! Original human prompt text over lazy directory and credited input queries.
use super::{Connection, storage, unexpected};
use crate::{
    Result,
    agent::AgentPath,
    client::{AgentCommand, AgentPageRequest, AgentReply, InputPreviewStep, MAX_READ_BYTES},
    local::agent_conversation::{Conversation, ConversationInput},
};

struct Directory {
    parent: Option<AgentPath>,
    cursor: Option<String>,
    ended: bool,
}

/// A demand-driven history projection over the user's original session inputs.
/// The selected agent comes first; other owned roots and descendants follow
/// their directory order. Each inbox is newest first. Inherited messages are
/// excluded by the shared input source, not copied or deduplicated by text.
pub struct PromptHistory {
    connection: Connection,
    selected: AgentPath,
    pending: Option<AgentPath>,
    text: String,
    original: Option<(u64, u64)>,
    directories: Vec<Directory>,
    input: Option<ConversationInput>,
}

impl PromptHistory {
    /// Retain the original authenticated connection. Opening performs no IO.
    pub fn new(connection: Connection) -> Self {
        Self {
            selected: connection.root().clone(),
            pending: Some(connection.root().clone()),
            text: String::new(),
            original: None,
            connection,
            directories: vec![Directory {
                parent: None,
                cursor: None,
                ended: false,
            }],
            input: None,
        }
    }

    /// Read the next original human prompt, fetching only the source pages
    /// and UTF-8 ranges needed to reach it. A source failure is never EOF.
    /// Dropping the query drops its connection and any in-flight source.
    pub async fn next(&mut self) -> Result<Option<String>> {
        loop {
            if self.input.is_none() {
                if self.pending.is_none() {
                    self.pending = self.next_agent().await?;
                }
                let Some(agent) = self.pending.as_ref() else {
                    return Ok(None);
                };
                let selected = if agent == &self.selected {
                    self.connection.clone()
                } else {
                    self.connection.select(agent.clone()).await?
                };
                self.input = Some(
                    Conversation::open(selected)
                        .prompt_history_query(
                            AgentPageRequest::new(1, MAX_READ_BYTES as u32, None)
                                .map_err(storage)?,
                        )
                        .await
                        .map_err(storage)?,
                );
                self.pending = None;
            }
            match self
                .input
                .as_mut()
                .expect("selected input source")
                .next()
                .await
                .map_err(storage)?
            {
                Some(InputPreviewStep::Progress) => {}
                Some(InputPreviewStep::MessageRange {
                    inbox,
                    offset,
                    preview,
                }) => {
                    if offset != self.text.len() as u64
                        || self.original.is_some_and(|(selected, length)| {
                            selected != inbox || length != preview.bytes
                        })
                    {
                        return Err(crate::Error::Invalid(
                            "prompt history range lost its original input".into(),
                        ));
                    }
                    self.original = Some((inbox, preview.bytes));
                    self.text.push_str(&preview.text);
                    if self.text.len() as u64 == preview.bytes {
                        self.original = None;
                        return Ok(Some(std::mem::take(&mut self.text)));
                    }
                }
                Some(InputPreviewStep::Selected(None)) if self.original.is_none() => {
                    self.input = None
                }
                _ => {
                    return Err(crate::Error::Invalid(
                        "prompt history ended without its original completion".into(),
                    ));
                }
            }
        }
    }

    async fn next_agent(&mut self) -> Result<Option<AgentPath>> {
        while let Some(directory) = self.directories.last_mut() {
            if directory.ended {
                self.directories.pop();
                continue;
            }
            let page = AgentPageRequest::new(1, MAX_READ_BYTES as u32, directory.cursor.clone())
                .map_err(storage)?;
            let command = match &directory.parent {
                Some(agent) => AgentCommand::Children {
                    agent: agent.clone(),
                    page,
                },
                None => AgentCommand::Sessions {
                    requester: None,
                    scope: acyclic_harness_client::SessionsScope::Owned,
                    page,
                },
            };
            let AgentReply::Children { mut items, next } =
                self.connection.execute(&command).await?
            else {
                return Err(unexpected());
            };
            if items.len() > 1 {
                return Err(crate::Error::Invalid(
                    "prompt history directory exceeded its requested page".into(),
                ));
            }
            directory.ended = next.is_none();
            directory.cursor = next;
            if let Some(item) = items.pop() {
                if directory
                    .parent
                    .as_ref()
                    .map_or(item.agent.tree_root() != item.agent, |parent| {
                        !item.agent.is_child_of(parent)
                    })
                {
                    return Err(crate::Error::Invalid(
                        "prompt history directory returned an unrelated agent".into(),
                    ));
                }
                self.directories.push(Directory {
                    parent: Some(item.agent.clone()),
                    cursor: None,
                    ended: false,
                });
                if item.agent != self.selected {
                    return Ok(Some(item.agent));
                }
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests;
