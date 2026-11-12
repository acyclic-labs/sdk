//! Original owned project-directory query; no replayable native cursor escapes.
use crate::{AgentCommand, AgentExchangeError, CommandError};
use acyclic_harness_core::conversation::PrivateDirectoryEntry;
use acyclic_stream::{StreamError, session::ActorSocket};

/// One directory selection under the original connection and credential.
/// Dropping it cancels only this consumer and releases the source cursor.
pub struct DirectoryQuery {
    socket: Option<ActorSocket>,
    command: AgentCommand,
    ended: bool,
    failed: bool,
}

impl DirectoryQuery {
    pub(crate) fn new(socket: ActorSocket, command: AgentCommand) -> Self {
        Self {
            socket: Some(socket),
            command,
            ended: false,
            failed: false,
        }
    }

    /// One credited page. An empty page reports real source progress; only
    /// authenticated terminal framing followed by original EOF completes it.
    pub async fn next(&mut self) -> Result<Option<Vec<PrivateDirectoryEntry>>, AgentExchangeError> {
        let result = self.receive().await;
        if result.is_err() {
            self.socket = None;
            self.failed = true;
        }
        result
    }

    async fn receive(&mut self) -> Result<Option<Vec<PrivateDirectoryEntry>>, AgentExchangeError> {
        if self.failed {
            return Err(CommandError::Invalid.into());
        }
        let AgentCommand::Directory { page, .. } = &self.command else {
            return Err(CommandError::Invalid.into());
        };
        let Some(socket) = &mut self.socket else {
            return Ok(None);
        };
        loop {
            let Some(frame) = socket.next().await.map_err(AgentExchangeError::Session)? else {
                if !self.ended {
                    return Err(CommandError::Invalid.into());
                }
                self.socket = None;
                return Ok(None);
            };
            if frame.len() > page.bytes() as usize + 1 {
                return Err(CommandError::Capacity.into());
            }
            let Some((&kind, bytes)) = frame.split_first() else {
                return Err(CommandError::Invalid.into());
            };
            match kind {
                4 if !self.ended && bytes.is_empty() => return Ok(Some(Vec::new())),
                9 if !self.ended && bytes.is_empty() => self.ended = true,
                10 if !self.ended => {
                    let entries: Vec<PrivateDirectoryEntry> =
                        serde_json::from_slice(bytes).map_err(|_| CommandError::Invalid)?;
                    if entries.is_empty() || entries.len() > page.items() as usize {
                        return Err(CommandError::Capacity.into());
                    }
                    for entry in &entries {
                        acyclic_harness_core::conversation::validate_content_path(
                            &entry.name,
                            &mut None,
                        )
                        .map_err(|_| CommandError::Invalid)?;
                        if entry.name.contains('/') || entry.name == ".system" {
                            return Err(CommandError::Invalid.into());
                        }
                    }
                    return Ok(Some(entries));
                }
                5 => match crate::decode_agent_reply(bytes, &self.command)? {
                    Err(error) => return Err(error.into()),
                    Ok(_) => return Err(CommandError::Invalid.into()),
                },
                _ => return Err(AgentExchangeError::Session(StreamError::InvalidResponse)),
            }
        }
    }
}
