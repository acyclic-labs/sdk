//! Closed, exactly negotiated control-plane wire protocol.

use serde::{Deserialize, Serialize};
use serde_json::Value;
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
const MAXIMUM_JOURNAL_BYTES: u64 = 128 * 1024 * 1024;
const JOURNAL_REWRITE_SLACK: u64 = 1024 * 1024;
/// The journal alternates between two slots. Compaction rewrites the inactive
/// slot and publishes it by writing its generation frame last, so the active
/// slot only ever changes by appending past its end.
const JOURNAL_SLOTS: [&str; 2] = ["control-ledger-v5.a", "control-ledger-v5.b"];
/// State of earlier releases, which no current binary reads.
const OBSOLETE_STATE: [&str; 7] = [
    "control-ledger-v3.json",
    "control-ledger-v3.next",
    "control-client-v2.json",
    "control-client-v2.next",
    "control-client-v2.lock",
    "control-ledger-v4.a",
    "control-ledger-v4.b",
];
/// Little-endian record length, then the digest of the record.
const FRAME_HEADER_BYTES: usize = 8 + 32;
const GENERATION_FRAME_BYTES: usize = FRAME_HEADER_BYTES + 1 + 8;

/// One journal record. The journal is never flushed: at-most-once only has to
/// survive a service crash, because every client that could retry an operation
/// runs on the same machine and dies with it, and a crashed process loses no
/// completed write. Each frame carries a digest, so a torn tail left by a
/// crash, a failed write or power loss reads as the end of the journal.
#[derive(Clone, Copy)]
enum Record<'a> {
    Generation(u64),
    Begin {
        operation: OperationId,
        request_digest: [u8; 32],
    },
    Complete {
        operation: OperationId,
        response: &'a [u8],
    },
    /// Completed, with a response the ledger no longer retains.
    Settle(OperationId),
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
            Self::Settle(operation) => {
                record.push(3);
                record.extend_from_slice(operation.0.as_bytes());
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
            (3, Some(operation), Some([])) => Some(Self::Settle(operation)),
            _ => None,
        }
    }
}

fn record_digest(record: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic-control-ledger-v5\0");
    hasher.update(record);
    *hasher.finalize().as_bytes()
}

/// Yields the journal's records up to the first frame that is not intact.
fn decode_frames(mut bytes: &[u8]) -> impl Iterator<Item = Record<'_>> {
    std::iter::from_fn(move || {
        let (length, rest) = bytes.split_first_chunk::<8>()?;
        let (digest, rest) = rest.split_first_chunk::<32>()?;
        let (record, rest) =
            rest.split_at_checked(usize::try_from(u64::from_le_bytes(*length)).ok()?)?;
        if *digest != record_digest(record) {
            return None;
        }
        bytes = rest;
        Record::decode(record)
    })
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
    fn replay(&mut self, record: Record<'_>, now: Instant) -> Result<(), String> {
        match record {
            Record::Begin {
                operation,
                request_digest,
            } if !self.started.contains_key(&operation)
                && !self.outcomes.contains_key(&operation) =>
            {
                self.started.insert(operation, request_digest);
            }
            Record::Complete {
                operation,
                response,
            } => {
                let request_digest = self.finish(operation)?;
                self.retain(
                    operation,
                    Outcome::Completed {
                        request_digest,
                        response: response.to_vec(),
                    },
                    now,
                );
            }
            Record::Settle(operation) => {
                let request_digest = self.finish(operation)?;
                self.retain(operation, Outcome::Settled { request_digest }, now);
            }
            Record::Generation(_) | Record::Begin { .. } => {
                return Err("Acyclic control journal is inconsistent".to_owned());
            }
        }
        Ok(())
    }

    fn finish(&mut self, operation: OperationId) -> Result<[u8; 32], String> {
        self.started
            .remove(&operation)
            .ok_or_else(|| "Acyclic control journal completes an operation it never began".into())
    }

    fn retain(&mut self, operation: OperationId, outcome: Outcome, now: Instant) {
        if let Outcome::Completed { response, .. } = &outcome {
            self.response_bytes += response.len();
            self.responses.push_back(operation);
        }
        self.outcomes.insert(operation, outcome);
        self.retained.push_back((now, operation));
        while self.response_bytes > RETAINED_RESPONSE_BYTES {
            let Some(oldest) = self.responses.pop_front() else {
                break;
            };
            if let Some(outcome) = self.outcomes.get_mut(&oldest)
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
    }

    /// Forgets outcomes that no retransmission can reach any more.
    fn expire(&mut self, now: Instant) {
        while let Some(&(retained, operation)) = self.retained.front()
            && now.saturating_duration_since(retained) >= OUTCOME_RETENTION
        {
            self.retained.pop_front();
            if let Some(Outcome::Completed { response, .. }) = self.outcomes.remove(&operation) {
                self.response_bytes -= response.len();
            }
        }
        // Both queues are in completion order, so every response whose outcome
        // has gone sits at the front.
        while let Some(operation) = self.responses.front()
            && !matches!(
                self.outcomes.get(operation),
                Some(Outcome::Completed { .. })
            )
        {
            self.responses.pop_front();
        }
    }

    fn snapshot(&self, generation: u64) -> Vec<u8> {
        let mut frames = Vec::new();
        Record::Generation(generation).encode_into(&mut frames);
        for (&operation, &request_digest) in &self.started {
            Record::Begin {
                operation,
                request_digest,
            }
            .encode_into(&mut frames);
        }
        for (_, operation) in &self.retained {
            let Some(outcome) = self.outcomes.get(operation) else {
                continue;
            };
            Record::Begin {
                operation: *operation,
                request_digest: *outcome.request_digest(),
            }
            .encode_into(&mut frames);
            match outcome {
                Outcome::Completed { response, .. } => Record::Complete {
                    operation: *operation,
                    response,
                }
                .encode_into(&mut frames),
                Outcome::Settled { .. } => Record::Settle(*operation).encode_into(&mut frames),
                Outcome::Interrupted { .. } => {}
            }
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
    let mut header = Vec::with_capacity(GENERATION_FRAME_BYTES);
    file.take(GENERATION_FRAME_BYTES as u64)
        .read_to_end(&mut header)
        .map_err(|error| error.to_string())?;
    Ok(match decode_frames(&header).next() {
        Some(Record::Generation(generation)) => Some(generation),
        _ => None,
    })
}

fn read_slot(mut file: &fs::File) -> Result<Vec<u8>, String> {
    if file.metadata().map_err(|error| error.to_string())?.len() > MAXIMUM_JOURNAL_BYTES {
        return Err("Acyclic control journal exceeds its byte bound".to_owned());
    }
    file.rewind().map_err(|error| error.to_string())?;
    let mut bytes = Vec::new();
    file.take(MAXIMUM_JOURNAL_BYTES)
        .read_to_end(&mut bytes)
        .map_err(|error| error.to_string())?;
    Ok(bytes)
}

#[derive(Debug)]
pub(crate) enum LedgerDecision {
    Execute,
    Completed(Value),
}

/// Service-side at-most-once ledger: a retried operation replays its retained
/// response, and one that may already have run is refused, never re-executed.
pub(crate) struct ControlLedger {
    inner: Mutex<(LedgerState, Journal)>,
}

impl ControlLedger {
    pub(crate) fn open(data: &Path) -> Result<Self, String> {
        for name in OBSOLETE_STATE {
            // Nothing reads these files, so one an old client still holds open
            // is simply removed by a later start.
            let _ = fs::remove_file(data.join(name));
        }
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
        let now = Instant::now();
        let mut state = LedgerState::default();
        if generation.is_some() {
            let [active, _] = &slots;
            let bytes = read_slot(active)?;
            for record in decode_frames(&bytes).skip(1) {
                state.replay(record, now)?;
            }
        }
        for (operation, request_digest) in std::mem::take(&mut state.started) {
            state.retain(operation, Outcome::Interrupted { request_digest }, now);
        }
        let mut journal = Journal {
            slots,
            generation: generation.unwrap_or(0),
            end: 0,
            rewrite_at: 0,
        };
        journal.rewrite(&state)?;
        Ok(Self {
            inner: Mutex::new((state, journal)),
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
        let mut guard = self.lock()?;
        let (state, journal) = &mut *guard;
        state.expire(now);
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
                Some(Outcome::Completed { response, .. }) => serde_json::from_slice(response)
                    .map(LedgerDecision::Completed)
                    .map_err(|error| error.to_string()),
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
        journal.append(&Record::Begin {
            operation,
            request_digest,
        })?;
        state.started.insert(operation, request_digest);
        Ok(LedgerDecision::Execute)
    }

    pub(crate) fn complete<T>(
        &self,
        envelope: &ControlEnvelope<T>,
        response: &Value,
    ) -> Result<(), String> {
        self.complete_at(envelope, response, Instant::now())
    }

    fn complete_at<T>(
        &self,
        envelope: &ControlEnvelope<T>,
        response: &Value,
        now: Instant,
    ) -> Result<(), String> {
        let response = serde_json::to_vec(response).map_err(|error| error.to_string())?;
        let operation = envelope.operation;
        let mut guard = self.lock()?;
        let (state, journal) = &mut *guard;
        let request_digest = *state
            .started
            .get(&operation)
            .ok_or("Acyclic control operation was not started")?;
        journal.append(&Record::Complete {
            operation,
            response: &response,
        })?;
        state.started.remove(&operation);
        state.retain(
            operation,
            Outcome::Completed {
                request_digest,
                response,
            },
            now,
        );
        state.expire(now);
        if journal.end >= journal.rewrite_at {
            // A failed compaction leaves the active slot complete and
            // authoritative; the next completion retries it.
            let _ = journal.rewrite(state);
        }
        Ok(())
    }

    fn lock(&self) -> Result<MutexGuard<'_, (LedgerState, Journal)>, String> {
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
                    assert_eq!(response, serde_json::json!({"ok": envelope.request}));
                    false
                }
            })
    }

    fn run(ledger: &ControlLedger, envelope: &ControlEnvelope<&str>, now: Instant) {
        assert!(begin(ledger, envelope, now).expect("begin operation"));
        ledger
            .complete_at(envelope, &serde_json::json!({"ok": envelope.request}), now)
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
    fn opening_removes_state_of_earlier_releases() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        for name in OBSOLETE_STATE {
            fs::write(temporary.path().join(name), b"obsolete").expect("obsolete state");
        }
        let unrelated = temporary.path().join("adapter-state.a");
        fs::write(&unrelated, b"kept").expect("unrelated state");
        drop(ControlLedger::open(temporary.path()).expect("ledger"));
        assert!(
            OBSOLETE_STATE
                .iter()
                .all(|name| !temporary.path().join(name).exists())
        );
        assert_eq!(fs::read(unrelated).expect("unrelated state"), b"kept");
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
                reopened.complete(&started, &serde_json::json!({})).is_err(),
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
                .complete_at(&envelope, &Value::Null, now)
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
            let guard = reopened.lock().expect("ledger state");
            assert_eq!(
                guard.0.outcomes.len(),
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
        let large = Value::String("x".repeat(RETAINED_RESPONSE_BYTES / 4));
        for _ in 0..5 {
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
            let (state, _) = &mut *guard;
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
