//! A selected process's diagnostic request and acknowledged snapshot.
use super::{WaitReport, WaitSnapshot};
use std::{
    io,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::io::{AsyncBufReadExt as _, AsyncRead, AsyncWrite, AsyncWriteExt as _, BufReader};

#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;

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
    WriteFailed,
    /// The selected owner exited before producing a snapshot.
    Exited,
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

/// The original inherited report endpoint, admitted before runtime construction.
///
/// Admission validates the selected launch and owns its original OS endpoint,
/// without registering it with a reactor or submitting IO. Dropping this owner
/// closes that endpoint; it does not certify that a CLI failure was delivered.
pub struct ReportAdmission {
    generation: String,
    #[cfg(unix)]
    channel: std::os::unix::net::UnixStream,
    #[cfg(windows)]
    channel: std::os::windows::io::OwnedHandle,
}

impl ReportAdmission {
    /// Takes the selected launch's inherited endpoint exactly once.
    pub fn from_environment() -> io::Result<Option<Self>> {
        #[cfg(unix)]
        return unix::open();
        #[cfg(windows)]
        return windows::open();
        #[cfg(not(any(unix, windows)))]
        Ok(None)
    }

    /// Transfers the same endpoint into the original enabled Tokio IO reactor.
    /// Failure closes it and retains the original admission/registration error.
    pub fn into_port(self) -> io::Result<ReportPort> {
        tokio::runtime::Handle::try_current().map_err(io::Error::other)?;
        #[cfg(unix)]
        return ReportPort::new(
            self.generation,
            tokio::net::UnixStream::from_std(self.channel)?,
        );
        #[cfg(windows)]
        {
            use std::os::windows::io::IntoRawHandle as _;
            // SAFETY: admission owns this overlapped pipe exclusively. The
            // original reactor takes that same handle, including on error.
            #[expect(unsafe_code, reason = "transfers the admitted pipe to its one reactor")]
            let channel = unsafe {
                tokio::net::windows::named_pipe::NamedPipeClient::from_raw_handle(
                    self.channel.into_raw_handle(),
                )
            }?;
            ReportPort::new(self.generation, channel)
        }
        #[cfg(not(any(unix, windows)))]
        Err(io::ErrorKind::Unsupported.into())
    }
}

/// One inherited asynchronous control channel. It owns no process or timer.
pub struct ReportPort {
    generation: String,
    channel: BufReader<Box<dyn ReportIo>>,
    request: RequestState,
}

enum RequestState {
    Fresh,
    Ready,
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
        ReportAdmission::from_environment()?
            .map(ReportAdmission::into_port)
            .transpose()
    }

    /// Announces capability on this exact channel before accepting a request.
    pub async fn ready(&mut self) -> io::Result<()> {
        self.announce()?;
        let line = format!(
            "{{\"ready\":true,\"generation\":\"{}\"}}\n",
            self.generation
        );
        self.channel.get_mut().write_all(line.as_bytes()).await
    }

    fn announce(&mut self) -> io::Result<()> {
        if !matches!(self.request, RequestState::Fresh) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "report startup disposition already submitted",
            ));
        }
        // A partial write cannot be followed by another startup disposition.
        self.request = RequestState::Ready;
        Ok(())
    }

    /// Rejects setup on this same launch's announcement, before ready/request.
    ///
    /// The caller retains its original error through delivery and any native
    /// write failure. This is rejection before execution, not a diagnostic ACK
    /// or physical process retirement. No request, timer or retry is created.
    /// If runtime construction or endpoint admission failed, this method was
    /// never admitted: the caller must retain incomplete delivery separately.
    pub async fn reject_startup(
        &mut self,
        phase: &'static str,
        cause: &(dyn std::error::Error + 'static),
    ) -> io::Result<()> {
        self.announce()?;
        self.request = RequestState::Finished;
        #[derive(serde::Serialize)]
        struct Rejection<'a> {
            generation: &'a str,
            rejected: Failure<'a>,
        }
        #[derive(serde::Serialize)]
        struct Failure<'a> {
            phase: &'static str,
            #[serde(serialize_with = "display_error")]
            cause: &'a (dyn std::error::Error + 'static),
            raw_os_error: Option<i32>,
        }
        fn display_error<S: serde::Serializer>(
            cause: &&(dyn std::error::Error + 'static),
            serializer: S,
        ) -> Result<S::Ok, S::Error> {
            serializer.collect_str(cause)
        }
        let rejection = Rejection {
            generation: &self.generation,
            rejected: Failure {
                phase,
                cause,
                raw_os_error: cause
                    .downcast_ref::<io::Error>()
                    .and_then(io::Error::raw_os_error),
            },
        };
        let mut bytes = serde_json::to_vec(&rejection).map_err(io::Error::other)?;
        bytes.push(b'\n');
        self.channel.get_mut().write_all(&bytes).await
    }

    /// Waits for the original channel's request or EOF, with no readiness probes.
    pub async fn request(&mut self) -> io::Result<Option<ReportRequest>> {
        if !matches!(self.request, RequestState::Fresh | RequestState::Ready) {
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
