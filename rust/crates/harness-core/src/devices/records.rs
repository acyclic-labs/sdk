//! The v1 records of attached devices: presence, queued runs and run events.
use crate::{
    Error, OperationId, Result,
    agent::{AgentPath, TargetRecord},
    core::Scope,
    tools::ShellInput,
};
use acyclic_actors::addressing::ActorReference;
use acyclic_stream::StreamPath;
use serde::{Deserialize, Serialize};

/// The format version of every record here.
pub const RECORD_VERSION: u32 = 1;

/// A JSON byte consumes at most four bytes, including its separator. Half the
/// record remains available for report, socket and committed-event metadata.
/// This partitions output; it never limits the command's total output.
pub(super) const OUTPUT_CHUNK_BYTES: usize = acyclic_stream::MAX_RECORD_BYTES / 8;

/// Where the devices under one root keep their streams.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TargetPaths {
    root: StreamPath,
    owner: ActorReference,
}

impl TargetPaths {
    /// Devices under `root`.
    #[must_use]
    pub const fn new(root: StreamPath, owner: ActorReference) -> Self {
        Self { root, owner }
    }

    /// Original tree owner. Addresses derived here retain its account.
    #[must_use]
    pub const fn owner(&self) -> &ActorReference {
        &self.owner
    }

    /// Device identity within this original tree and account.
    pub fn actor(&self, target: &str) -> Result<ActorReference> {
        let mut hash = blake3::Hasher::new_derive_key("acyclic.harness.devices.actor.v1");
        hash.update(&self.owner.encode());
        hash.update(self.root.as_str().as_bytes());
        hash.update(&[0]);
        hash.update(target.as_bytes());
        ActorReference::new(
            self.owner.account().as_bytes(),
            b"targets",
            hash.finalize().to_hex().as_bytes(),
        )
        .map_err(|error| Error::Invalid(error.to_string()))
    }

    /// The root every device stream lives under.
    #[must_use]
    pub const fn root(&self) -> &StreamPath {
        &self.root
    }

    fn path(&self, suffix: std::fmt::Arguments<'_>) -> Result<StreamPath> {
        StreamPath::new(format!("{}/{suffix}", self.root)).map_err(Error::from)
    }

    /// Every device's presence, keyed by its name: one [`TargetStatus`] per change.
    ///
    /// # Errors
    /// When the root cannot be extended.
    pub fn presence(&self) -> Result<StreamPath> {
        self.path(format_args!("presence"))
    }

    /// Device `target`'s actor inbox, which carries its socket's events.
    ///
    /// # Errors
    /// When the name cannot be a path segment.
    pub fn inbox(&self, target: &str) -> Result<StreamPath> {
        self.path(format_args!("{target}/inbox"))
    }

    /// Device `target`'s queue of [`Run`] records.
    ///
    /// # Errors
    /// When the name cannot be a path segment.
    pub fn queue(&self, target: &str) -> Result<StreamPath> {
        self.path(format_args!("{target}/queue"))
    }

    /// One immutable end marker for the original physical connection.
    pub fn session_end(&self, target: &str, generation: u64) -> Result<StreamPath> {
        self.path(format_args!("{target}/sessions/{generation}/ended"))
    }

    /// The [`RunEvent`]s of run `op` on device `target`.
    ///
    /// # Errors
    /// When the name cannot be a path segment.
    pub fn run(&self, target: &str, op: OperationId) -> Result<StreamPath> {
        let segment: String = op
            .into_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        self.path(format_args!("{target}/runs/{segment}"))
    }
}

/// A device's presence: its roster record, online with its current session
/// or offline. `generation` counts the sessions it has opened.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetStatus {
    /// Format version, [`RECORD_VERSION`].
    pub v: u32,
    /// The device as rosters list it, with its state.
    pub target: TargetRecord,
    /// The session the device opened its socket with, while it is online.
    pub session: Option<String>,
    /// Original host socket; observation never authorizes a replacement socket.
    pub socket: Option<String>,
    /// The number of sessions the device has opened.
    pub generation: u64,
}

/// What a queued run asks the device to do; its grant binds exactly this.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunCommand {
    /// The tool call's operation: the run's identity.
    pub op: OperationId,
    /// The model's call identity.
    pub call_id: String,
    /// Original agent whose grant admitted this call.
    pub agent: AgentPath,
    /// Original project selection, including a child's retained fork identity.
    pub volume: crate::conversation::VolumeRef,
    /// Digest of the original account/tree/device Actor reference.
    pub actor: [u8; 32],
    /// The device's name.
    pub target: String,
    /// The device session it was dispatched to; any other session refuses it.
    pub session: String,
    /// The original presence generation, retained with the session.
    pub generation: u64,
    /// Original task deadline; absence adds no deadline.
    pub deadline_epoch_ms: Option<u64>,
    /// The call's `shell` arguments exactly as the model gave them, so the
    /// device's policy judges the same call the agent's did.
    pub arguments: serde_json::Value,
}

impl RunCommand {
    /// The command as a `shell` input on the device, which runs it here.
    ///
    /// # Errors
    /// When the arguments are not a valid `shell` input.
    pub fn input(&self) -> Result<ShellInput> {
        let mut input: ShellInput = serde_json::from_value(self.arguments.clone())
            .map_err(|error| Error::Invalid(format!("shell input decode failed: {error}")))?;
        input.validate()?;
        input.target = None;
        Ok(input)
    }

    /// The identity its grant must carry: a digest of every field.
    ///
    /// # Errors
    /// When the command does not encode.
    pub fn grant_id(&self) -> Result<String> {
        let bytes = crate::contract::canonical_json_bytes(self)?;
        let mut hasher = blake3::Hasher::new_derive_key("acyclic.harness.devices.run.v1");
        hasher.update(&bytes);
        Ok(format!("run:{}", hasher.finalize().to_hex()))
    }
}

/// One queued run (v1).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Run {
    /// Format version, [`RECORD_VERSION`].
    pub v: u32,
    /// What to run.
    pub command: RunCommand,
    /// A sealed grant for `tool:call:shell` whose identity is
    /// [`RunCommand::grant_id`]: it proves admission and delegates nothing.
    pub grant: Scope,
}

/// One committed queue record handed over in queue order. Its sequence is
/// the original Stream sequence, not a second device delivery counter.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct HandoverRecord<T = Run> {
    pub(super) v: u32,
    pub(super) sequence: u64,
    pub(super) run: T,
}

/// Admit the complete wire handover before publishing its Run. Its eventual
/// queue sequence can use every decimal digit of u64. Serialize only the fixed
/// envelope here; the already encoded Run is embedded unchanged by serde.
pub(super) fn handover_frame_len(run_bytes: usize) -> Result<usize> {
    struct Length(usize);
    impl std::io::Write for Length {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .ok_or_else(|| std::io::Error::other("device handover encoding length overflow"))?;
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut length = Length(0);
    serde_json::to_writer(
        &mut length,
        &HandoverRecord {
            v: RECORD_VERSION,
            sequence: u64::MAX,
            run: (),
        },
    )
    .map_err(|error| Error::Invalid(error.to_string()))?;
    // Unit serializes to `null`; replace those four bytes by the actual Run.
    let text_bytes = (length.0 - b"null".len())
        .checked_add(run_bytes)
        .ok_or_else(|| Error::Invalid("device handover encoding length overflow".into()))?;
    acyclic_actors::sockets::SocketFrame::text_frame_len(text_bytes)
        .ok_or_else(|| Error::Invalid("device handover encoding length overflow".into()))
}

/// What happened to a run, as the device's actor records it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum RunEvent {
    /// The actor handed it to the device's session `session`; the device
    /// runs it only once this is recorded.
    Started {
        /// The session it runs in.
        session: String,
        /// The original host socket retained before physical handover.
        socket: String,
        /// Original Actor connection generation.
        generation: u64,
    },
    /// An immutable output prefix, visible before command completion.
    Output {
        /// Original connection owning the physical command.
        session: String,
        /// Original connection generation.
        generation: u64,
        /// True for stderr; false for stdout.
        stderr: bool,
        /// Exact channel bytes; text decoding belongs to the reader.
        data: Vec<u8>,
    },
    /// It ran to its end; `result` is the `shell` result.
    Exit {
        /// `{success, exit_code, stdout, stderr, stop}`.
        result: serde_json::Value,
    },
    /// The device disconnected while it ran: it may have run partly or fully.
    Lost {
        /// Why the device went away.
        reason: String,
    },
    /// It did not run: the device declined it, or its session had ended
    /// before the actor could hand it over.
    Refused {
        /// Why.
        reason: String,
    },
}

/// One [`RunEvent`] on `runs/<op>` (v1).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RunRecord {
    pub(super) v: u32,
    pub(super) event: RunEvent,
}

/// What a device tells its actor over its socket (v1). The actor sends the
/// device each run it is to execute as the run's queue record itself.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DeviceMessage {
    /// One credited output chunk from the original command stream.
    Output {
        /// Original command identity.
        op: OperationId,
        /// True for stderr; false for stdout.
        stderr: bool,
        /// Exact channel bytes, at most one protective chunk.
        data: Vec<u8>,
    },
    /// Execution may have started before its original authority was lost.
    Lost {
        /// The original run, which must never be dispatched again.
        op: OperationId,
        /// Original cancellation or authority loss.
        reason: String,
    },
    /// `op` ran to its end.
    Exit {
        /// The run.
        op: OperationId,
        /// The `shell` result.
        result: serde_json::Value,
    },
    /// It declined `op` without running it.
    Refused {
        /// The run.
        op: OperationId,
        /// Why.
        reason: String,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct MessageRecord {
    pub(super) v: u32,
    pub(super) message: DeviceMessage,
}

pub(super) fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|error| Error::Invalid(error.to_string()))
}

/// Decodes a v1 record; any other version is refused whole.
pub(super) fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    #[derive(Deserialize)]
    struct Version {
        v: u32,
    }
    let version: Version =
        serde_json::from_slice(bytes).map_err(|error| Error::Invalid(error.to_string()))?;
    if version.v != RECORD_VERSION {
        return Err(Error::Invalid(format!(
            "device record version {} is not {RECORD_VERSION}",
            version.v
        )));
    }
    serde_json::from_slice(bytes).map_err(|error| Error::Invalid(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_actors::{guest::SocketPayload, sockets::SocketFrame};

    #[test]
    fn device_records_admit_complete_frames_and_binary_output() -> Result<()> {
        let maximum = acyclic_stream::MAX_RECORD_BYTES;
        // Generated codec inputs exercise the length-prefix boundaries and the
        // largest eventual queue position, independently of command admission.
        for size in [0, 127, 128, 16_383, 16_384, maximum - 16, maximum] {
            let run = "x".repeat(size);
            let raw = encode(&run)?;
            let text = String::from_utf8(encode(&HandoverRecord {
                v: RECORD_VERSION,
                sequence: u64::MAX,
                run: &run,
            })?)
            .map_err(|error| Error::Invalid(error.to_string()))?;
            let frame = SocketFrame::Message(SocketPayload::Text { text }).encode();
            assert_eq!(handover_frame_len(raw.len())?, frame.len());
            if raw.len() <= maximum && size == maximum - 16 {
                // The old inner-record check accepted this undeliverable Run.
                assert!(frame.len() > maximum);
            }
        }
        assert!(handover_frame_len(usize::MAX).is_err());

        let op = OperationId::from_bytes([255; 16]);
        for stderr in [false, true] {
            let text = String::from_utf8(encode(&MessageRecord {
                v: RECORD_VERSION,
                message: DeviceMessage::Output {
                    op,
                    stderr,
                    data: vec![255; OUTPUT_CHUNK_BYTES],
                },
            })?)
            .map_err(|error| Error::Invalid(error.to_string()))?;
            assert!(
                SocketFrame::Message(SocketPayload::Text { text })
                    .encode()
                    .len()
                    <= maximum
            );
            let event = RunRecord {
                v: RECORD_VERSION,
                event: RunEvent::Output {
                    // Even maximal JSON escaping of the admitted session fits.
                    session: "\\".repeat(super::super::actor::MAX_SESSION_BYTES),
                    generation: u64::MAX,
                    stderr,
                    data: vec![255; OUTPUT_CHUNK_BYTES],
                },
            };
            assert!(encode(&event)?.len() <= maximum);
            let old = encode(&MessageRecord {
                v: RECORD_VERSION,
                message: DeviceMessage::Output {
                    op,
                    stderr,
                    data: vec![255; 16 * 1024],
                },
            })?;
            assert!(old.len() > maximum);
        }
        Ok(())
    }
}
