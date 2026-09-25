//! Closed, exactly negotiated control-plane wire protocol.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::fs;
use std::io::{self, Read as _, Seek as _};
use std::path::Path;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

pub(crate) const CONTROL_PROTOCOL_MAJOR: u32 = 4;
const CONTROL_SCHEMA: &[u8] = br#"{"envelope":{"operation":"uuid-v4","protocol":{"capabilities":{"atMostOnceOperations":true,"boundedFrames":true,"exactNegotiation":true},"major":4,"schemaDigest":"blake3-32"},"request":"ControlRequest-v1","requestId":"ascii-id"},"operations":{"retransmitWithinSeconds":300},"response":{"error":"string?","ok":"bool","requestId":"ascii-id","result":"json?","version":2}}"#;
const MAXIMUM_ID_BYTES: usize = 128;

/// A client may retransmit an operation only within this window after its
/// first transmission that may have been delivered. The Acyclic client never
/// retransmits after such a transmission at all: it retries only a transmission
/// that provably never reached the service.
const RETRANSMIT_WINDOW: Duration = Duration::from_secs(300);

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct ProtocolOffer {
    pub(crate) major: u32,
    pub(crate) schema_digest: [u8; 32],
    pub(crate) capabilities: CapabilitySet,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct CapabilitySet {
    pub(crate) exact_negotiation: bool,
    pub(crate) at_most_once_operations: bool,
    pub(crate) bounded_frames: bool,
}

impl ProtocolOffer {
    pub(crate) fn current() -> Self {
        Self {
            major: CONTROL_PROTOCOL_MAJOR,
            schema_digest: *blake3::hash(CONTROL_SCHEMA).as_bytes(),
            capabilities: CapabilitySet {
                exact_negotiation: true,
                at_most_once_operations: true,
                bounded_frames: true,
            },
        }
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        if *self == Self::current() {
            Ok(())
        } else {
            Err(
                "incompatible Acyclic control protocol; drain the old service before retrying"
                    .to_owned(),
            )
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub(crate) struct RequestId(String);

impl RequestId {
    pub(crate) fn fresh() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }

    fn validate(&self) -> Result<(), String> {
        validate_id("request ID", &self.0)
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn from_wire(value: String) -> Result<Self, String> {
        let request_id = Self(value);
        request_id.validate()?;
        Ok(request_id)
    }
}

/// Identifies one logical operation across every transmission of its
/// envelope: 122 random bits, unique without any coordination.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub(crate) struct OperationId(uuid::Uuid);

impl OperationId {
    fn fresh() -> Self {
        Self(uuid::Uuid::new_v4())
    }

    fn validate(self) -> Result<(), String> {
        if self.0.get_version() == Some(uuid::Version::Random)
            && self.0.get_variant() == uuid::Variant::RFC4122
        {
            Ok(())
        } else {
            Err("Acyclic operation identity must be a random UUID".to_owned())
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct ControlEnvelope<T> {
    pub(crate) protocol: ProtocolOffer,
    pub(crate) request_id: RequestId,
    pub(crate) operation: OperationId,
    pub(crate) request: T,
}

impl<T> ControlEnvelope<T> {
    pub(crate) fn new(request: T) -> Self {
        Self {
            protocol: ProtocolOffer::current(),
            request_id: RequestId::fresh(),
            operation: OperationId::fresh(),
            request,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        self.protocol.validate()?;
        self.request_id.validate()?;
        self.operation.validate()
    }
}

/// The ledger remembers every outcome for at least the retransmit window,
/// measured on the monotonic clock from its completion, which follows every
/// transmission that could have delivered it. Admission therefore depends on
/// neither wall-clock time nor other clients' traffic: an unknown operation
/// never ran.
const OUTCOME_RETENTION: Duration = RETRANSMIT_WINDOW;
/// Memory bound on remembered operations, over 200 per second sustained for
/// the whole window. Reaching it refuses new work rather than forgetting.
const MAXIMUM_OPERATIONS: usize = 65_536;
/// Responses beyond this budget are released oldest first; their operations
/// stay remembered, so a retry is refused rather than executed.
const RETAINED_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
const JOURNAL_REWRITE_SLACK: u64 = 1024 * 1024;
/// The journal alternates between two slots. Compaction rewrites the inactive
/// slot and publishes it by writing its generation frame last, so the active
/// slot only ever changes by appending past its end.
const JOURNAL_SLOTS: [&str; 2] = ["control-ledger-v6.a", "control-ledger-v6.b"];
/// Little-endian record length, then the digest of the record.
const FRAME_HEADER_BYTES: usize = 8 + 32;
const GENERATION_FRAME_BYTES: usize = FRAME_HEADER_BYTES + 1 + 8;
/// The largest record: a retained outcome carrying the largest response.
const MAXIMUM_RECORD_BYTES: usize = 1 + 16 + 32 + 1 + crate::MAXIMUM_CONTROL_MESSAGE_BYTES;

/// One journal record, which is also one transition of the ledger state.
/// The live ledger commits a transition by appending its record and then
/// applying it, and opening applies the same records in order, so replay is
/// the live history itself: every history the ledger accepted reopens to the
/// state it had.
///
/// The journal is never flushed: at-most-once only has to survive a service
/// crash, because every client that could retry an operation runs on the same
/// machine and dies with it, and a crashed process loses no completed write.
/// Each frame carries a digest, so a torn tail left by a crash, a failed write
/// or power loss reads as the end of the journal.
#[derive(Clone, Copy)]
enum Record<'a> {
    Generation(u64),
    /// An unknown operation starts.
    Begin {
        operation: OperationId,
        request_digest: [u8; 32],
    },
    /// A started operation completes with its encoded response.
    Complete {
        operation: OperationId,
        response: &'a [u8],
    },
    /// The oldest response still held is released.
    Release(OperationId),
    /// The oldest remembered outcome is forgotten.
    Expire(OperationId),
    /// A snapshot's outcome, retained in the order the live ledger held it.
    Retained {
        operation: OperationId,
        request_digest: [u8; 32],
        outcome: RetainedOutcome<'a>,
    },
}

#[derive(Clone, Copy)]
enum RetainedOutcome<'a> {
    Completed(&'a [u8]),
    Settled,
    Interrupted,
}

impl<'a> Record<'a> {
    fn encode_into(&self, frames: &mut Vec<u8>) {
        let mut record = Vec::new();
        match self {
            Self::Generation(generation) => {
                record.push(0);
                record.extend_from_slice(&generation.to_le_bytes());
            }
            Self::Begin {
                operation,
                request_digest,
            } => {
                record.push(1);
                record.extend_from_slice(operation.0.as_bytes());
                record.extend_from_slice(request_digest);
            }
            Self::Complete {
                operation,
                response,
            } => {
                record.push(2);
                record.extend_from_slice(operation.0.as_bytes());
                record.extend_from_slice(response);
            }
            Self::Release(operation) => {
                record.push(3);
                record.extend_from_slice(operation.0.as_bytes());
            }
            Self::Expire(operation) => {
                record.push(4);
                record.extend_from_slice(operation.0.as_bytes());
            }
            Self::Retained {
                operation,
                request_digest,
                outcome,
            } => {
                record.push(5);
                record.extend_from_slice(operation.0.as_bytes());
                record.extend_from_slice(request_digest);
                match outcome {
                    RetainedOutcome::Completed(response) => {
                        record.push(0);
                        record.extend_from_slice(response);
                    }
                    RetainedOutcome::Settled => record.push(1),
                    RetainedOutcome::Interrupted => record.push(2),
                }
            }
        }
        frames.extend_from_slice(&(record.len() as u64).to_le_bytes());
        frames.extend_from_slice(&record_digest(&record));
        frames.extend_from_slice(&record);
    }

    fn decode(record: &'a [u8]) -> Option<Self> {
        let (kind, body) = record.split_first()?;
        let (operation, rest) = body
            .split_first_chunk::<16>()
            .map(|(operation, rest)| (OperationId(uuid::Uuid::from_bytes(*operation)), rest))
            .unzip();
        match (kind, operation, rest) {
            (0, _, _) => Some(Self::Generation(u64::from_le_bytes(body.try_into().ok()?))),
            (1, Some(operation), Some(request_digest)) => Some(Self::Begin {
                operation,
                request_digest: request_digest.try_into().ok()?,
            }),
            (2, Some(operation), Some(response)) => Some(Self::Complete {
                operation,
                response,
            }),
            (3, Some(operation), Some([])) => Some(Self::Release(operation)),
            (4, Some(operation), Some([])) => Some(Self::Expire(operation)),
            (5, Some(operation), Some(rest)) => {
                let (request_digest, rest) = rest.split_first_chunk::<32>()?;
                let outcome = match rest.split_first()? {
                    (0, response) => RetainedOutcome::Completed(response),
                    (1, []) => RetainedOutcome::Settled,
                    (2, []) => RetainedOutcome::Interrupted,
                    _ => return None,
                };
                Some(Self::Retained {
                    operation,
                    request_digest: *request_digest,
                    outcome,
                })
            }
            _ => None,
        }
    }
}

fn record_digest(record: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic-control-ledger-v6\0");
    hasher.update(record);
    *hasher.finalize().as_bytes()
}

/// Reads a slot's frames in order, up to the first frame that is not intact.
/// Frames are read one at a time, so no slot is too large to open.
struct FrameReader<'a> {
    reader: io::BufReader<&'a fs::File>,
    record: Vec<u8>,
}

impl<'a> FrameReader<'a> {
    fn new(mut file: &'a fs::File) -> Result<Self, String> {
        file.rewind().map_err(|error| error.to_string())?;
        Ok(Self {
            reader: io::BufReader::new(file),
            record: Vec::new(),
        })
    }

    fn next(&mut self) -> Result<Option<Record<'_>>, String> {
        let mut header = [0; FRAME_HEADER_BYTES];
        if !self.read_exact(&mut header)? {
            return Ok(None);
        }
        let (length, digest) = header.split_at(8);
        let Some(length) = length
            .try_into()
            .ok()
            .map(u64::from_le_bytes)
            .and_then(|length| usize::try_from(length).ok())
            .filter(|length| *length <= MAXIMUM_RECORD_BYTES)
        else {
            return Ok(None);
        };
        let mut record = std::mem::take(&mut self.record);
        record.resize(length, 0);
        let complete = self.read_exact(&mut record)?;
        self.record = record;
        if !complete || digest != record_digest(&self.record) {
            return Ok(None);
        }
        Ok(Record::decode(&self.record))
    }

    /// Fills `buffer`, or reports a torn end of the slot.
    fn read_exact(&mut self, buffer: &mut [u8]) -> Result<bool, String> {
        match self.reader.read_exact(buffer) {
            Ok(()) => Ok(true),
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => Ok(false),
            Err(error) => Err(error.to_string()),
        }
    }
}

enum Outcome {
    Completed {
        request_digest: [u8; 32],
        response: Vec<u8>,
    },
    /// Completed, but its response was released to bound memory.
    Settled { request_digest: [u8; 32] },
    /// Began before a service crash, so its effects are unknown.
    Interrupted { request_digest: [u8; 32] },
}

impl Outcome {
    const fn request_digest(&self) -> &[u8; 32] {
        match self {
            Self::Completed { request_digest, .. }
            | Self::Settled { request_digest }
            | Self::Interrupted { request_digest } => request_digest,
        }
    }
}

/// Every operation that began and could still be retransmitted is started or
/// retained as an outcome, so an unknown operation never ran.
#[derive(Default)]
struct LedgerState {
    started: BTreeMap<OperationId, [u8; 32]>,
    outcomes: BTreeMap<OperationId, Outcome>,
    /// Outcomes in the order they were retained, with the instant they were.
    retained: VecDeque<(Instant, OperationId)>,
    /// Outcomes still holding a response, oldest first.
    responses: VecDeque<OperationId>,
    response_bytes: usize,
}

impl LedgerState {
    /// Why `record` is not a transition of this state, if it is not.
    fn refusal(&self, record: &Record<'_>) -> Option<&'static str> {
        let known = |operation| {
            self.started.contains_key(operation) || self.outcomes.contains_key(operation)
        };
        let admissible = match record {
            Record::Generation(_) => false,
            Record::Begin { operation, .. } | Record::Retained { operation, .. } => {
                !known(operation)
            }
            Record::Complete {
                operation,
                response,
            } => {
                self.started.contains_key(operation)
                    && response.len() < crate::MAXIMUM_CONTROL_MESSAGE_BYTES
            }
            Record::Release(operation) => self.responses.front() == Some(operation),
            Record::Expire(operation) => {
                self.retained.front().map(|(_, oldest)| oldest) == Some(operation)
                    && (!matches!(
                        self.outcomes.get(operation),
                        Some(Outcome::Completed { .. })
                    ) || self.responses.front() == Some(operation))
            }
        };
        (!admissible).then_some("Acyclic control journal record is not a transition of its ledger")
    }

    /// Applies one transition, refusing a record that is not one without
    /// changing anything.
    fn apply(&mut self, record: Record<'_>, now: Instant) -> Result<(), String> {
        if let Some(refusal) = self.refusal(&record) {
            return Err(refusal.to_owned());
        }
        match record {
            Record::Generation(_) => {}
            Record::Begin {
                operation,
                request_digest,
            } => {
                self.started.insert(operation, request_digest);
            }
            Record::Complete {
                operation,
                response,
            } => {
                if let Some(request_digest) = self.started.remove(&operation) {
                    self.retain(
                        operation,
                        Outcome::Completed {
                            request_digest,
                            response: response.to_vec(),
                        },
                        now,
                    );
                }
            }
            Record::Release(operation) => {
                self.responses.pop_front();
                if let Some(outcome) = self.outcomes.get_mut(&operation)
                    && let Outcome::Completed {
                        request_digest,
                        response,
                    } = outcome
                {
                    self.response_bytes -= response.len();
                    let request_digest = *request_digest;
                    *outcome = Outcome::Settled { request_digest };
                }
            }
            Record::Expire(operation) => {
                self.retained.pop_front();
                if let Some(Outcome::Completed { response, .. }) = self.outcomes.remove(&operation)
                {
                    self.responses.pop_front();
                    self.response_bytes -= response.len();
                }
            }
            Record::Retained {
                operation,
                request_digest,
                outcome,
            } => {
                let outcome = match outcome {
                    RetainedOutcome::Completed(response) => Outcome::Completed {
                        request_digest,
                        response: response.to_vec(),
                    },
                    RetainedOutcome::Settled => Outcome::Settled { request_digest },
                    RetainedOutcome::Interrupted => Outcome::Interrupted { request_digest },
                };
                self.retain(operation, outcome, now);
            }
        }
        Ok(())
    }

    fn retain(&mut self, operation: OperationId, outcome: Outcome, now: Instant) {
        if let Outcome::Completed { response, .. } = &outcome {
            self.response_bytes += response.len();
            self.responses.push_back(operation);
        }
        self.outcomes.insert(operation, outcome);
        self.retained.push_back((now, operation));
    }

    /// Operations that were in flight when the service stopped become
    /// indeterminate outcomes.
    fn interrupt_started(&mut self, now: Instant) {
        for (operation, request_digest) in std::mem::take(&mut self.started) {
            self.retain(operation, Outcome::Interrupted { request_digest }, now);
        }
    }

    /// The transition the retention policy takes next, if any: forget an
    /// outcome past its window, then release responses beyond the budget.
    fn due(&self, now: Instant) -> Option<Record<'static>> {
        if let Some(&(retained, operation)) = self.retained.front()
            && now.saturating_duration_since(retained) >= OUTCOME_RETENTION
        {
            return Some(Record::Expire(operation));
        }
        (self.response_bytes > RETAINED_RESPONSE_BYTES)
            .then(|| self.responses.front().copied().map(Record::Release))
            .flatten()
    }

    /// The records that rebuild this state, in the order it holds them.
    fn snapshot(&self, generation: u64) -> Vec<u8> {
        let mut frames = Vec::new();
        Record::Generation(generation).encode_into(&mut frames);
        for (_, operation) in &self.retained {
            let Some(outcome) = self.outcomes.get(operation) else {
                continue;
            };
            Record::Retained {
                operation: *operation,
                request_digest: *outcome.request_digest(),
                outcome: match outcome {
                    Outcome::Completed { response, .. } => RetainedOutcome::Completed(response),
                    Outcome::Settled { .. } => RetainedOutcome::Settled,
                    Outcome::Interrupted { .. } => RetainedOutcome::Interrupted,
                },
            }
            .encode_into(&mut frames);
        }
        for (&operation, &request_digest) in &self.started {
            Record::Begin {
                operation,
                request_digest,
            }
            .encode_into(&mut frames);
        }
        frames
    }
}

struct Journal {
    /// The active slot, then the slot the next compaction overwrites.
    slots: [fs::File; 2],
    generation: u64,
    end: u64,
    rewrite_at: u64,
}

impl Journal {
    fn append(&mut self, record: &Record<'_>) -> Result<(), String> {
        let mut frame = Vec::new();
        record.encode_into(&mut frame);
        // Writing at the tracked end rather than the file length lets the next
        // append overwrite whatever a failed append left behind.
        let [active, _] = &self.slots;
        write_all_at(active, self.end, &frame).map_err(|error| error.to_string())?;
        self.end += frame.len() as u64;
        Ok(())
    }

    /// Publishes `state` as the next generation in the inactive slot.
    fn rewrite(&mut self, state: &LedgerState) -> Result<(), String> {
        let generation = self
            .generation
            .checked_add(1)
            .ok_or("Acyclic control journal generation is exhausted")?;
        let frames = state.snapshot(generation);
        let (header, records) = frames
            .split_at_checked(GENERATION_FRAME_BYTES)
            .ok_or("Acyclic control journal snapshot omitted its generation")?;
        let [_, inactive] = &self.slots;
        inactive.set_len(0).map_err(|error| error.to_string())?;
        write_all_at(inactive, GENERATION_FRAME_BYTES as u64, records)
            .map_err(|error| error.to_string())?;
        write_all_at(inactive, 0, header).map_err(|error| error.to_string())?;
        self.slots.swap(0, 1);
        self.generation = generation;
        self.end = frames.len() as u64;
        self.rewrite_at = self.end * 2 + JOURNAL_REWRITE_SLACK;
        Ok(())
    }
}

/// The live ledger: its state, and the journal that records every transition.
struct Ledger {
    state: LedgerState,
    journal: Journal,
}

impl Ledger {
    /// Records a transition, then applies it. A transition that could not be
    /// recorded is not applied, so the journal never lags the state.
    fn commit(&mut self, record: Record<'_>, now: Instant) -> Result<(), String> {
        if let Some(refusal) = self.state.refusal(&record) {
            return Err(refusal.to_owned());
        }
        self.journal.append(&record)?;
        self.state.apply(record, now)
    }

    /// Takes every retention transition that is due. One that cannot be
    /// recorded is left for later: remembering longer is always safe.
    fn maintain(&mut self, now: Instant) {
        while let Some(record) = self.state.due(now) {
            if self.commit(record, now).is_err() {
                break;
            }
        }
    }
}

#[cfg(unix)]
fn write_all_at(file: &fs::File, offset: u64, bytes: &[u8]) -> io::Result<()> {
    std::os::unix::fs::FileExt::write_all_at(file, bytes, offset)
}

#[cfg(windows)]
fn write_all_at(file: &fs::File, mut offset: u64, mut bytes: &[u8]) -> io::Result<()> {
    while !bytes.is_empty() {
        let written = std::os::windows::fs::FileExt::seek_write(file, bytes, offset)?;
        if written == 0 {
            return Err(io::ErrorKind::WriteZero.into());
        }
        bytes = bytes.get(written..).unwrap_or_default();
        offset += written as u64;
    }
    Ok(())
}

/// Reads a slot's generation, or `None` when it holds no published snapshot.
fn slot_generation(file: &fs::File) -> Result<Option<u64>, String> {
    Ok(match FrameReader::new(file)?.next()? {
        Some(Record::Generation(generation)) => Some(generation),
        _ => None,
    })
}

#[derive(Debug)]
pub(crate) enum LedgerDecision {
    Execute,
    /// The encoded response the operation's first execution sent.
    Completed(Vec<u8>),
}

/// Service-side at-most-once ledger: a retried operation replays its retained
/// response, and one that may already have run is refused, never re-executed.
pub(crate) struct ControlLedger {
    inner: Mutex<Ledger>,
}

impl ControlLedger {
    pub(crate) fn open(data: &Path) -> Result<Self, String> {
        Self::open_at(data, Instant::now())
    }

    fn open_at(data: &Path, now: Instant) -> Result<Self, String> {
        let [first, second] = JOURNAL_SLOTS.map(|name| {
            fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(data.join(name))
                .map_err(|error| error.to_string())
        });
        let mut slots = [first?, second?];
        let [first, second] = slots.each_ref().map(slot_generation);
        let (first, second) = (first?, second?);
        if second > first {
            slots.swap(0, 1);
        }
        let generation = first.max(second);
        // Retention restarts when the service does: outcomes are only ever
        // remembered longer than their window, never shorter.
        let mut state = LedgerState::default();
        if generation.is_some() {
            let [active, _] = &slots;
            let mut frames = FrameReader::new(active)?;
            frames.next()?;
            while let Some(record) = frames.next()? {
                state.apply(record, now)?;
            }
        }
        state.interrupt_started(now);
        let mut journal = Journal {
            slots,
            generation: generation.unwrap_or(0),
            end: 0,
            rewrite_at: 0,
        };
        journal.rewrite(&state)?;
        Ok(Self {
            inner: Mutex::new(Ledger { state, journal }),
        })
    }

    pub(crate) fn begin<T: Serialize>(
        &self,
        envelope: &ControlEnvelope<T>,
    ) -> Result<LedgerDecision, String> {
        self.begin_at(envelope, Instant::now())
    }

    fn begin_at<T: Serialize>(
        &self,
        envelope: &ControlEnvelope<T>,
        now: Instant,
    ) -> Result<LedgerDecision, String> {
        envelope.validate()?;
        let request = serde_json::to_vec(&envelope.request).map_err(|error| error.to_string())?;
        let mut digest = blake3::Hasher::new();
        digest.update(b"acyclic-control-operation-v2\0");
        digest.update(envelope.request_id.as_str().as_bytes());
        digest.update(&request);
        let request_digest = *digest.finalize().as_bytes();
        let operation = envelope.operation;
        let mut ledger = self.lock()?;
        ledger.maintain(now);
        let state = &ledger.state;
        let known = match (
            state.started.get(&operation),
            state.outcomes.get(&operation),
        ) {
            (Some(known), _) => Some((known, None)),
            (None, Some(outcome)) => Some((outcome.request_digest(), Some(outcome))),
            (None, None) => None,
        };
        if let Some((known, outcome)) = known {
            if *known != request_digest {
                return Err(
                    "Acyclic operation identity was reused for a different request".to_owned(),
                );
            }
            return match outcome {
                Some(Outcome::Completed { response, .. }) => {
                    Ok(LedgerDecision::Completed(response.clone()))
                }
                Some(Outcome::Settled { .. }) => Err(
                    "Acyclic operation completed but its response is no longer retained; it will not be replayed"
                        .to_owned(),
                ),
                Some(Outcome::Interrupted { .. }) | None => Err(
                    "Acyclic operation outcome is indeterminate; it will not be replayed"
                        .to_owned(),
                ),
            };
        }
        if state.started.len() + state.outcomes.len() >= MAXIMUM_OPERATIONS {
            return Err(
                "Acyclic control replay ledger is full; retry after recent operations age out"
                    .to_owned(),
            );
        }
        ledger.commit(
            Record::Begin {
                operation,
                request_digest,
            },
            now,
        )?;
        Ok(LedgerDecision::Execute)
    }

    /// Records the encoded response sent for a started operation. A response
    /// must fit one control message.
    pub(crate) fn complete<T>(
        &self,
        envelope: &ControlEnvelope<T>,
        response: &[u8],
    ) -> Result<(), String> {
        self.complete_at(envelope, response, Instant::now())
    }

    fn complete_at<T>(
        &self,
        envelope: &ControlEnvelope<T>,
        response: &[u8],
        now: Instant,
    ) -> Result<(), String> {
        if response.len() >= crate::MAXIMUM_CONTROL_MESSAGE_BYTES {
            return Err("Acyclic control response exceeds the 4 MiB bound".to_owned());
        }
        let mut ledger = self.lock()?;
        ledger.commit(
            Record::Complete {
                operation: envelope.operation,
                response,
            },
            now,
        )?;
        ledger.maintain(now);
        let Ledger { state, journal } = &mut *ledger;
        if journal.end >= journal.rewrite_at {
            // A failed compaction leaves the active slot complete and
            // authoritative; the next completion retries it.
            let _ = journal.rewrite(state);
        }
        Ok(())
    }

    fn lock(&self) -> Result<MutexGuard<'_, Ledger>, String> {
        self.inner
            .lock()
            .map_err(|_| "Acyclic control ledger lock is poisoned".to_owned())
    }
}

fn validate_id(kind: &str, value: &str) -> Result<(), String> {
    if value.is_empty()
        || value.len() > MAXIMUM_ID_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(format!(
            "Acyclic {kind} must be 1-{MAXIMUM_ID_BYTES} ASCII identifier bytes"
        ));
    }
    Ok(())
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions deliberately fail fast and index fixed test arrays"
)]
mod tests {
    use super::*;

    fn begin(
        ledger: &ControlLedger,
        envelope: &ControlEnvelope<&str>,
        now: Instant,
    ) -> Result<bool, String> {
        ledger
            .begin_at(envelope, now)
            .map(|decision| match decision {
                LedgerDecision::Execute => true,
                LedgerDecision::Completed(response) => {
                    assert_eq!(response, response_for(envelope));
                    false
                }
            })
    }

    fn response_for(envelope: &ControlEnvelope<&str>) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({"ok": envelope.request})).expect("response")
    }

    fn run(ledger: &ControlLedger, envelope: &ControlEnvelope<&str>, now: Instant) {
        assert!(begin(ledger, envelope, now).expect("begin operation"));
        ledger
            .complete_at(envelope, &response_for(envelope), now)
            .expect("complete operation");
    }

    fn active_slot(data: &Path) -> std::path::PathBuf {
        JOURNAL_SLOTS
            .map(|name| data.join(name))
            .into_iter()
            .max_by_key(|path| {
                slot_generation(&fs::File::open(path).expect("journal slot")).expect("generation")
            })
            .expect("active slot")
    }

    #[test]
    fn an_operation_begun_again_after_its_outcome_expired_reopens() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let now = Instant::now();
        let ledger = ControlLedger::open(temporary.path()).expect("ledger");
        let reused = ControlEnvelope::new("reused");
        run(&ledger, &reused, now);
        // The live path forgets the outcome after its window and accepts the
        // identity as new work; the journal now begins it twice.
        run(&ledger, &reused, now + OUTCOME_RETENTION);
        drop(ledger);
        let reopened = ControlLedger::open(temporary.path()).expect("reopen after reuse");
        assert_eq!(begin(&reopened, &reused, Instant::now()), Ok(false));
    }

    #[test]
    fn an_interrupted_operation_begun_again_after_expiry_reopens() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let ledger = ControlLedger::open(temporary.path()).expect("ledger");
        let reused = ControlEnvelope::new("reused");
        assert_eq!(begin(&ledger, &reused, Instant::now()), Ok(true));
        drop(ledger);
        let reopened = ControlLedger::open(temporary.path()).expect("reopen after crash");
        assert_eq!(
            begin(&reopened, &reused, Instant::now() + OUTCOME_RETENTION),
            Ok(true)
        );
        drop(reopened);
        let reopened = ControlLedger::open(temporary.path()).expect("reopen after reuse");
        assert!(
            begin(&reopened, &reused, Instant::now())
                .expect_err("the second attempt is itself interrupted")
                .contains("indeterminate")
        );
    }

    #[test]
    fn an_oversized_journal_still_opens() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let now = Instant::now();
        let ledger = ControlLedger::open(temporary.path()).expect("ledger");
        let kept = ControlEnvelope::new("kept");
        run(&ledger, &kept, now);
        drop(ledger);
        // Whatever failed writes or skipped compactions leave behind, the
        // journal's size alone must never prevent the service from starting.
        let slot = active_slot(temporary.path());
        fs::OpenOptions::new()
            .write(true)
            .open(&slot)
            .expect("journal slot")
            .set_len(129 * 1024 * 1024)
            .expect("extend journal");
        let reopened = ControlLedger::open(temporary.path()).expect("reopen oversized journal");
        assert_eq!(begin(&reopened, &kept, Instant::now()), Ok(false));
    }

    /// The ledger state without its retention instants, which restart when
    /// the service does.
    /// One retained outcome: its operation, request, kind and response.
    type RetainedEntry = (OperationId, [u8; 32], &'static str, Option<Vec<u8>>);

    #[derive(Debug, Eq, PartialEq)]
    struct Canonical {
        retained: Vec<RetainedEntry>,
        started: Vec<(OperationId, [u8; 32])>,
        responses: Vec<OperationId>,
        response_bytes: usize,
    }

    fn canonical(state: &LedgerState) -> Canonical {
        Canonical {
            retained: state
                .retained
                .iter()
                .map(|(_, operation)| {
                    let outcome = &state.outcomes[operation];
                    let (response, kind) = match outcome {
                        Outcome::Completed { response, .. } => {
                            (Some(response.clone()), "completed")
                        }
                        Outcome::Settled { .. } => (None, "settled"),
                        Outcome::Interrupted { .. } => (None, "interrupted"),
                    };
                    (*operation, *outcome.request_digest(), kind, response)
                })
                .collect(),
            started: state
                .started
                .iter()
                .map(|(key, value)| (*key, *value))
                .collect(),
            responses: state.responses.iter().copied().collect(),
            response_bytes: state.response_bytes,
        }
    }

    /// Random live histories, with time running past the retention window,
    /// identities retried and begun again after their outcomes expire,
    /// responses released past the budget, compactions, and crashes, always
    /// reopen to exactly the state the live ledger held, with in-flight work
    /// interrupted.
    #[test]
    fn every_live_history_reopens_to_the_state_it_left() {
        let mut seed = 0x9e37_79b9_7f4a_7c15_u64;
        let mut random = move |bound: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed % bound
        };
        let (mut reused, mut released, mut compacted) = (0, 0, 0);
        for _ in 0..12 {
            let temporary = tempfile::tempdir().expect("temporary directory");
            let mut now = Instant::now();
            let mut ledger = ControlLedger::open_at(temporary.path(), now).expect("ledger");
            let pool = (0..6)
                .map(|_| ControlEnvelope::new("pooled"))
                .collect::<Vec<_>>();
            let mut executed = std::collections::BTreeSet::new();
            let mut in_flight = Vec::new();
            for _ in 0..120 {
                match random(20) {
                    0..=8 => {
                        let envelope = if random(4) == 0 {
                            ControlEnvelope::new("fresh")
                        } else {
                            pool[usize::try_from(random(6)).expect("index")].clone()
                        };
                        if let Ok(LedgerDecision::Execute) = ledger.begin_at(&envelope, now) {
                            reused += usize::from(!executed.insert(envelope.operation));
                            in_flight.push(envelope);
                        }
                    }
                    9..=15 if !in_flight.is_empty() => {
                        let envelope = in_flight.swap_remove(
                            usize::try_from(random(in_flight.len() as u64)).expect("index"),
                        );
                        let size = if random(2) == 0 { 2 * 1024 * 1024 } else { 16 };
                        let generation = ledger.lock().expect("live").journal.generation;
                        ledger
                            .complete_at(&envelope, &vec![b'r'; size], now)
                            .expect("complete in-flight work");
                        let live = ledger.lock().expect("live");
                        compacted += usize::from(live.journal.generation != generation);
                        released += live
                            .state
                            .outcomes
                            .values()
                            .filter(|outcome| matches!(outcome, Outcome::Settled { .. }))
                            .count();
                    }
                    16 => now += OUTCOME_RETENTION / 2 + Duration::from_secs(random(200)),
                    17 => now += Duration::from_secs(random(5)),
                    _ => {
                        let mut expected = {
                            let live = ledger.lock().expect("live state");
                            let mut expected = canonical(&live.state);
                            for (operation, request_digest) in std::mem::take(&mut expected.started)
                            {
                                expected.retained.push((
                                    operation,
                                    request_digest,
                                    "interrupted",
                                    None,
                                ));
                            }
                            expected
                        };
                        expected.started.clear();
                        drop(ledger);
                        ledger = ControlLedger::open_at(temporary.path(), now).expect("reopen");
                        assert_eq!(canonical(&ledger.lock().expect("reopened").state), expected);
                        in_flight.clear();
                    }
                }
            }
        }
        assert!(
            reused > 0 && released > 0 && compacted > 0,
            "histories must reuse expired identities ({reused}), release responses ({released}) and compact ({compacted})"
        );
    }

    #[test]
    fn exact_offer_and_random_operations_are_required() {
        let envelope = ControlEnvelope::new("request");
        envelope.validate().expect("current envelope");
        assert_eq!(
            hex::encode(ProtocolOffer::current().schema_digest),
            "d53c8cc7a3519cd3d3c61dd4ba712f71eac7dd572d8bdc5789abb23d35a428e2"
        );

        let mut incompatible = envelope.clone();
        incompatible.protocol.major += 1;
        assert!(incompatible.validate().is_err());

        let mut predictable = envelope;
        predictable.operation = OperationId(uuid::Uuid::now_v7());
        assert!(predictable.validate().is_err());
    }

    #[test]
    fn completed_operations_replay_across_restarts_and_identities_bind_one_request() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let now = Instant::now();
        let ledger = ControlLedger::open(temporary.path()).expect("ledger");
        let completed = ControlEnvelope::new("completed");
        run(&ledger, &completed, now);
        assert_eq!(begin(&ledger, &completed, now), Ok(false));
        drop(ledger);

        let reopened = ControlLedger::open(temporary.path()).expect("reopen ledger");
        assert_eq!(begin(&reopened, &completed, Instant::now()), Ok(false));
        let mut reused = completed.clone();
        reused.request_id = RequestId::fresh();
        assert!(
            begin(&reopened, &reused, Instant::now())
                .expect_err("an operation cannot identify a later transmission")
                .contains("different request")
        );
        run(
            &reopened,
            &ControlEnvelope::new("after restart"),
            Instant::now(),
        );
    }

    #[test]
    fn a_crash_between_begin_and_complete_is_indeterminate_and_never_replayed() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let ledger = ControlLedger::open(temporary.path()).expect("ledger");
        let started = ControlEnvelope::new("started");
        assert_eq!(begin(&ledger, &started, Instant::now()), Ok(true));
        assert!(
            begin(&ledger, &started, Instant::now())
                .expect_err("in-flight work must not run twice")
                .contains("indeterminate")
        );
        drop(ledger);
        for _ in 0..2 {
            let reopened = ControlLedger::open(temporary.path()).expect("reopen ledger");
            assert!(
                begin(&reopened, &started, Instant::now())
                    .expect_err("interrupted work must not replay")
                    .contains("indeterminate")
            );
            assert!(
                reopened.complete(&started, b"{}").is_err(),
                "an interrupted operation cannot complete after a restart"
            );
        }
    }

    #[test]
    fn a_torn_journal_tail_is_ignored_and_overwritten() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let now = Instant::now();
        let ledger = ControlLedger::open(temporary.path()).expect("ledger");
        let kept = ControlEnvelope::new("kept");
        run(&ledger, &kept, now);
        let torn = ControlEnvelope::new("torn");
        run(&ledger, &torn, now);
        drop(ledger);

        let slot = active_slot(temporary.path());
        let journal = fs::read(&slot).expect("journal");
        let mut flipped = journal.clone();
        *flipped.last_mut().expect("journal byte") ^= 1;
        let mut garbage = journal.clone();
        garbage.extend_from_slice(&[0xff; 7]);
        for damaged in [journal[..journal.len() - 1].to_vec(), flipped, garbage] {
            let tail_intact = damaged.starts_with(&journal);
            fs::write(&slot, damaged).expect("damage journal tail");
            let reopened = ControlLedger::open(temporary.path()).expect("reopen torn journal");
            let now = Instant::now();
            assert_eq!(begin(&reopened, &kept, now), Ok(false));
            if tail_intact {
                assert_eq!(begin(&reopened, &torn, now), Ok(false));
            } else {
                assert!(
                    begin(&reopened, &torn, now)
                        .expect_err("a torn completion leaves its operation indeterminate")
                        .contains("indeterminate")
                );
            }
            run(&reopened, &ControlEnvelope::new("after tear"), now);
            drop(reopened);
            fs::write(&slot, &journal).expect("restore journal");
            for name in JOURNAL_SLOTS {
                let path = temporary.path().join(name);
                if path != slot {
                    fs::write(path, b"").expect("clear inactive slot");
                }
            }
        }

        for name in JOURNAL_SLOTS {
            fs::write(temporary.path().join(name), b"torn").expect("tear every slot");
        }
        let fresh = ControlLedger::open(temporary.path()).expect("slots without a snapshot");
        run(&fresh, &ControlEnvelope::new("fresh"), Instant::now());
    }

    #[test]
    fn admission_is_independent_of_traffic_and_retention_is_bounded_by_time() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let now = Instant::now();
        let ledger = ControlLedger::open(temporary.path()).expect("ledger");
        let stalled = ControlEnvelope::new("stalled");
        run(&ledger, &stalled, now);
        let interrupted = ControlEnvelope::new("interrupted");
        assert_eq!(begin(&ledger, &interrupted, now), Ok(true));
        for _ in 0..20_000 {
            let envelope = ControlEnvelope::new("x");
            assert_eq!(begin(&ledger, &envelope, now), Ok(true));
            ledger
                .complete_at(&envelope, b"null", now)
                .expect("complete other traffic");
        }
        let late = now + OUTCOME_RETENTION - Duration::from_millis(1);
        assert_eq!(
            begin(&ledger, &stalled, late),
            Ok(false),
            "other clients' traffic must not evict an outcome inside its window"
        );
        drop(ledger);

        let reopened = ControlLedger::open(temporary.path()).expect("reopen ledger");
        let now = Instant::now();
        assert_eq!(begin(&reopened, &stalled, now), Ok(false));
        assert!(
            begin(&reopened, &interrupted, now)
                .expect_err("interrupted work stays unresolved")
                .contains("indeterminate")
        );
        let expired = now + OUTCOME_RETENTION;
        run(&reopened, &ControlEnvelope::new("after window"), expired);
        {
            let ledger = reopened.lock().expect("ledger state");
            assert_eq!(
                ledger.state.outcomes.len(),
                1,
                "outcomes past their window are forgotten"
            );
        }
        drop(reopened);
        let journal_bytes = JOURNAL_SLOTS
            .map(|name| {
                fs::metadata(temporary.path().join(name))
                    .expect("journal slot")
                    .len()
            })
            .into_iter()
            .max()
            .expect("slot sizes");
        assert!(
            journal_bytes < 16 * JOURNAL_REWRITE_SLACK,
            "journal grew to {journal_bytes} bytes"
        );
    }

    #[test]
    fn capacity_and_response_bounds_refuse_rather_than_forget() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let now = Instant::now();
        let ledger = ControlLedger::open(temporary.path()).expect("ledger");
        let released = ControlEnvelope::new("released");
        run(&ledger, &released, now);
        let large = vec![b'x'; 3 * 1024 * 1024];
        for _ in 0..6 {
            let envelope = ControlEnvelope::new("large");
            assert_eq!(begin(&ledger, &envelope, now), Ok(true));
            ledger
                .complete_at(&envelope, &large, now)
                .expect("complete large response");
        }
        assert!(
            begin(&ledger, &released, now)
                .expect_err("a released response must not re-execute")
                .contains("no longer retained")
        );

        {
            let mut guard = ledger.lock().expect("ledger state");
            let state = &mut guard.state;
            while state.started.len() + state.outcomes.len() < MAXIMUM_OPERATIONS {
                state.started.insert(OperationId::fresh(), [0; 32]);
            }
        }
        assert!(
            begin(&ledger, &ControlEnvelope::new("full"), now)
                .expect_err("a full ledger refuses new work")
                .contains("full")
        );
        assert_eq!(
            begin(
                &ledger,
                &ControlEnvelope::new("after window"),
                now + OUTCOME_RETENTION
            ),
            Ok(true),
            "outcomes past their window free their capacity"
        );
    }
}
