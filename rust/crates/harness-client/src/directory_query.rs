//! Original owned project-directory query; no replayable native cursor escapes.
use crate::{AgentCommand, AgentExchangeError, CommandError};
use acyclic_harness_core::conversation::PrivateDirectoryEntry;
use acyclic_stream::{StreamError, session::ActorSocket};

/// Names admitted by one source page; the continuation stays on its original socket.
pub struct DirectoryQueryPage {
    /// Visible names, bounded by the original row and byte credit.
    pub entries: Vec<PrivateDirectoryEntry>,
    /// The original source has another page, including filtered source progress.
    pub has_more: bool,
}

/// One directory selection under the original connection and credential.
/// Dropping it cancels only this consumer and releases the source cursor.
pub struct DirectoryQuery {
    socket: Option<ActorSocket>,
    command: AgentCommand,
    failed: bool,
    terminal_page: Option<Vec<PrivateDirectoryEntry>>,
}

impl DirectoryQuery {
    pub(crate) fn new(socket: ActorSocket, command: AgentCommand) -> Self {
        Self {
            socket: Some(socket),
            command,
            failed: false,
            terminal_page: None,
        }
    }

    /// One credited page. An empty page reports real source progress; only
    /// an authenticated final page followed by original EOF completes it.
    pub async fn next(&mut self) -> Result<Option<DirectoryQueryPage>, AgentExchangeError> {
        let result = self.receive().await;
        if result.is_err() {
            self.socket = None;
            self.terminal_page = None;
            self.failed = true;
        }
        result
    }

    async fn receive(&mut self) -> Result<Option<DirectoryQueryPage>, AgentExchangeError> {
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
                let entries = self.terminal_page.take().ok_or(CommandError::Invalid)?;
                self.socket = None;
                return Ok(Some(entries)
                    .filter(|entries| !entries.is_empty())
                    .map(|entries| DirectoryQueryPage {
                        entries,
                        has_more: false,
                    }));
            };
            if frame.len() > page.bytes() as usize + 1 {
                return Err(CommandError::Capacity.into());
            }
            let Some((&kind, bytes)) = frame.split_first() else {
                return Err(CommandError::Invalid.into());
            };
            match kind {
                4 if self.terminal_page.is_none() && bytes.is_empty() => {
                    return Ok(Some(DirectoryQueryPage {
                        entries: Vec::new(),
                        has_more: true,
                    }));
                }
                10 if self.terminal_page.is_none() => {
                    let Some((&more, bytes)) = bytes.split_first() else {
                        return Err(CommandError::Invalid.into());
                    };
                    if more > 1 {
                        return Err(CommandError::Invalid.into());
                    }
                    let entries: Vec<PrivateDirectoryEntry> =
                        serde_json::from_slice(bytes).map_err(|_| CommandError::Invalid)?;
                    if more != 0 && entries.is_empty() || entries.len() > page.items() as usize {
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
                    if more == 0 {
                        // Do not publish the final page before its original
                        // socket EOF. A source failure still poisons this read.
                        self.terminal_page = Some(entries);
                    } else {
                        return Ok(Some(DirectoryQueryPage {
                            entries,
                            has_more: true,
                        }));
                    }
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
