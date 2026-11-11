//! Last-input prefixes on the same credited actor socket as other projections.
use crate::{AgentCommand, AgentExchangeError, AgentPageRequest, CommandError};
pub use acyclic_harness_core::agent::{InputPreview, InputPreviewStep};
use acyclic_stream::session::ActorSocket;

/// One original query. A failure closes its source and can never become EOF.
pub struct InputQuery {
    socket: Option<ActorSocket>,
    command: AgentCommand,
    maximum: usize,
    selected: bool,
    failed: bool,
}

impl InputQuery {
    pub(crate) fn new(socket: ActorSocket, command: AgentCommand, page: &AgentPageRequest) -> Self {
        Self {
            socket: Some(socket),
            command,
            maximum: page.bytes() as usize,
            selected: false,
            failed: false,
        }
    }

    /// Receive one bounded work step or the selected prefix. Only normal EOF
    /// after a selection proves that the query completed.
    pub async fn next(&mut self) -> Result<Option<InputPreviewStep>, AgentExchangeError> {
        let result = self.receive().await;
        if result.is_err() {
            self.socket = None;
            self.failed = true;
        }
        result
    }

    async fn receive(&mut self) -> Result<Option<InputPreviewStep>, AgentExchangeError> {
        if self.failed {
            return Err(CommandError::Invalid.into());
        }
        let Some(socket) = &mut self.socket else {
            return Ok(None);
        };
        let Some(frame) = socket.next().await.map_err(AgentExchangeError::Session)? else {
            if !self.selected {
                return Err(CommandError::Invalid.into());
            }
            self.socket = None;
            return Ok(None);
        };
        if frame.len() > self.maximum + 1 {
            return Err(CommandError::Capacity.into());
        }
        let Some((&kind, body)) = frame.split_first() else {
            return Err(CommandError::Invalid.into());
        };
        if kind == 5 {
            return match crate::decode_agent_reply(body, &self.command)? {
                Err(failure) => Err(failure.into()),
                Ok(_) => Err(CommandError::Invalid.into()),
            };
        }
        if self.selected {
            return Err(CommandError::Invalid.into());
        }
        let event = match kind {
            4 if body.is_empty() => InputPreviewStep::Progress,
            6 if body.len() >= 8 => {
                let bytes =
                    u64::from_le_bytes(body[..8].try_into().map_err(|_| CommandError::Invalid)?);
                let text = std::str::from_utf8(&body[8..]).map_err(|_| CommandError::Invalid)?;
                if text.len() as u64 > bytes {
                    return Err(CommandError::Invalid.into());
                }
                self.selected = true;
                InputPreviewStep::Selected(Some(InputPreview {
                    text: text.to_owned(),
                    bytes,
                }))
            }
            7 if body.is_empty() => {
                self.selected = true;
                InputPreviewStep::Selected(None)
            }
            _ => return Err(CommandError::Invalid.into()),
        };
        Ok(Some(event))
    }
}
