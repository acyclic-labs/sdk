//! Product projections over one original canonical invocation and socket.
use crate::{CommandError, DiffProjection};
pub use acyclic_harness_core::agent::InputSelection;
use bytes::Bytes;

/// The view selected before admitting the original command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryProjection {
    /// Workspace changes, backed by the original Diff invocation.
    Diff(DiffProjection),
    /// Last accepted input in the selected History context.
    Input(InputSelection),
}

impl QueryProjection {
    /// Frame one canonical invocation, without a second command codec.
    pub fn open(self, invocation: &[u8]) -> Result<Bytes, CommandError> {
        if invocation.is_empty() || invocation.len() > acyclic_stream::MAX_COMMAND_BYTES - 2 {
            return Err(CommandError::Capacity);
        }
        let kind = match self {
            Self::Diff(DiffProjection::Patch) => 0,
            Self::Diff(DiffProjection::Summary) => 1,
            Self::Input(InputSelection::Latest) => 2,
            Self::Input(InputSelection::Pending) => 3,
        };
        let mut bytes = Vec::with_capacity(invocation.len() + 2);
        bytes.extend_from_slice(&[1, kind]);
        bytes.extend_from_slice(invocation);
        Ok(bytes.into())
    }

    /// Select a projection before the host decodes the original invocation.
    pub fn decode_open(bytes: &[u8]) -> Result<(Self, &[u8]), CommandError> {
        if bytes.len() < 3 || bytes.len() > acyclic_stream::MAX_COMMAND_BYTES {
            return Err(CommandError::Capacity);
        }
        let projection = match &bytes[..2] {
            [1, 0] => Self::Diff(DiffProjection::Patch),
            [1, 1] => Self::Diff(DiffProjection::Summary),
            [1, 2] => Self::Input(InputSelection::Latest),
            [1, 3] => Self::Input(InputSelection::Pending),
            _ => return Err(CommandError::Invalid),
        };
        Ok((projection, &bytes[2..]))
    }
}
