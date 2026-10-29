//! A selected process's diagnostic request and acknowledged snapshot.
use super::{WaitReport, WaitSnapshot};
use std::{
    io,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::io::{AsyncBufReadExt as _, AsyncRead, AsyncWrite, AsyncWriteExt as _, BufReader};

#[cfg(unix)]
mod unix;

/// An owner's one report request, bound to its selected launch and drain deadline.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReportRequest {
    /// Opaque, unique 128-bit request identity encoded as lowercase hexadecimal.
    pub request_id: String,
    /// The original selected launch's identity in the same representation.
    pub generation: String,
    /// The original owner's diagnostic deadline; this creates no new timer.
    pub deadline_unix_ms: u64,
}

/// Terminal diagnostic disposition, independent of the operation's own result.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReportOutcome {
    /// The snapshot contains live waits.
    Report,
    /// The snapshot was taken and contains no live waits.
    Empty,
    /// The diagnostic sink failed; the operation's result remains unchanged.
    SinkFailed,
    /// The selected owner exited before producing a snapshot.
    OwnerExited,
    /// The selected launch does not admit this request.
    Unsupported,
}

/// An acknowledgement can only satisfy the exact original request and generation.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReportAck {
    /// Original request identity.
    pub request_id: String,
    /// Original selected launch identity.
    pub generation: String,
    /// Snapshot or typed terminal disposition.
    pub outcome: ReportOutcome,
    /// Complete live snapshot, empty for terminal refusals.
    pub waits: Vec<WaitSnapshot>,
}

trait ReportIo: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> ReportIo for T {}

/// One inherited asynchronous control channel. It owns no process or timer.
pub struct ReportPort {
    generation: String,
    channel: BufReader<Box<dyn ReportIo>>,
    request: RequestState,
}

enum RequestState {
    Fresh,
    Received(u128, u128),
    Finished,
}

fn identity(value: &str) -> bool {
    value.len() == 32
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

impl ReportPort {
    /// Binds a transport admitted by the original native process owner.
    pub fn new(
        generation: String,
        channel: impl AsyncRead + AsyncWrite + Unpin + Send + 'static,
    ) -> io::Result<Self> {
        if !identity(&generation) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid report generation",
            ));
        }
        Ok(Self {
            generation,
            channel: BufReader::new(Box::new(channel)),
            request: RequestState::Fresh,
        })
    }

    /// Opens the inherited selected-launch endpoint, if the owner supplied one.
    pub fn from_environment() -> io::Result<Option<Self>> {
        #[cfg(unix)]
        {
            unix::open()
        }
        #[cfg(not(unix))]
        {
            Ok(None)
        }
    }

    /// Announces capability on this exact channel before accepting a request.
    pub async fn ready(&mut self) -> io::Result<()> {
        let line = format!(
            "{{\"ready\":true,\"generation\":\"{}\"}}\n",
            self.generation
        );
        self.channel.get_mut().write_all(line.as_bytes()).await
    }

    /// Waits for the original channel's request or EOF, with no readiness probes.
    pub async fn request(&mut self) -> io::Result<Option<ReportRequest>> {
        if !matches!(self.request, RequestState::Fresh) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "report request already consumed",
            ));
        }
        self.request = RequestState::Finished;
        let mut line = String::new();
        if self.channel.read_line(&mut line).await? == 0 {
            return Ok(None);
        }
        let request: ReportRequest = serde_json::from_str(&line).map_err(io::Error::other)?;
        if !identity(&request.request_id) || !identity(&request.generation) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid report request identity",
            ));
        }
        self.request = RequestState::Received(
            u128::from_str_radix(&request.request_id, 16).map_err(io::Error::other)?,
            u128::from_str_radix(&request.generation, 16).map_err(io::Error::other)?,
        );
        Ok(Some(request))
    }

    /// Snapshots the same wait registry before cancellation, without holding
    /// its lock across encoding or transport work.
    pub fn snapshot(&self, request: ReportRequest, report: &WaitReport) -> ReportAck {
        let current = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let admitted = request.generation == self.generation
            && u128::from(request.deadline_unix_ms) >= current;
        let waits = if admitted {
            report.snapshot()
        } else {
            Vec::new()
        };
        let outcome = if !admitted {
            ReportOutcome::Unsupported
        } else if waits.is_empty() {
            ReportOutcome::Empty
        } else {
            ReportOutcome::Report
        };
        ReportAck {
            request_id: request.request_id,
            generation: request.generation,
            outcome,
            waits,
        }
    }

    /// Writes the exact request's acknowledgement. A stalled sink yields to
    /// the executor; the original shutdown event can still cancel this write.
    pub async fn acknowledge(&mut self, ack: &ReportAck) -> io::Result<()> {
        let RequestState::Received(request, generation) = self.request else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "no unanswered report request",
            ));
        };
        if u128::from_str_radix(&ack.request_id, 16).ok() != Some(request)
            || u128::from_str_radix(&ack.generation, 16).ok() != Some(generation)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "acknowledgement is for a different report request",
            ));
        }
        self.request = RequestState::Finished;
        let mut bytes = serde_json::to_vec(ack).map_err(io::Error::other)?;
        bytes.push(b'\n');
        self.channel.get_mut().write_all(&bytes).await
    }
}

#[cfg(test)]
mod tests;
