//! Event-owned conditions share the actor runtime's stream subscriptions.
use super::{Error, ReplyWatch};
use acyclic_stream::StreamPath;

/// One condition and its original, lossless event position. The host selects
/// and authorizes the source; a wake conveys no file or control authority.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Watch {
    /// The first record at or after the selected cursor resumes the turn.
    Record {
        /// Host-admitted source stream.
        path: StreamPath,
        /// First publication not covered by the original observation.
        from: u64,
    },
    /// A recipient status may settle this original send. Other status changes
    /// advance its cursor without satisfying the condition.
    Reply(ReplyWatch),
}

impl Watch {
    pub(crate) fn is_subscription(id: &str) -> bool {
        id.starts_with("reply-v1:") || id.starts_with("record-v1:")
    }

    pub(crate) fn subscription(&self) -> Result<String, Error> {
        let (prefix, body) = match self {
            Self::Reply(reply) => ("reply-v1:", serde_json::to_string(reply)),
            Self::Record { path, from } => ("record-v1:", serde_json::to_string(&(path, from))),
        };
        body.map(|body| format!("{prefix}{body}"))
            .map_err(|_| Error::Record("watch subscription does not encode"))
    }

    pub(crate) fn decode_subscription(id: &str) -> Result<Self, Error> {
        let invalid = || Error::Record("watch subscription is malformed");
        if let Some(body) = id.strip_prefix("reply-v1:") {
            return serde_json::from_str(body)
                .map(Self::Reply)
                .map_err(|_| invalid());
        }
        let body = id.strip_prefix("record-v1:").ok_or_else(invalid)?;
        let (path, from) = serde_json::from_str(body).map_err(|_| invalid())?;
        Ok(Self::Record { path, from })
    }

    pub(crate) fn path(&self) -> Result<StreamPath, Error> {
        match self {
            Self::Record { path, .. } => Ok(path.clone()),
            Self::Reply(reply) => reply.to.status(),
        }
    }

    pub(crate) fn from(&self) -> u64 {
        match self {
            Self::Record { from, .. } => *from,
            Self::Reply(reply) => reply.from,
        }
    }
}
