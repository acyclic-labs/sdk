//! Closed, exactly negotiated control-plane wire protocol.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs;
use std::io::{self, Read as _, Seek as _};
use std::path::Path;
use std::sync::{Mutex, MutexGuard};

pub(crate) const CONTROL_PROTOCOL_MAJOR: u32 = 3;
const CONTROL_SCHEMA: &[u8] = br#"{"envelope":{"operation":"uuid-v7","protocol":{"capabilities":{"atMostOnceOperations":true,"boundedFrames":true,"exactNegotiation":true},"major":3,"schemaDigest":"blake3-32"},"request":"ControlRequest-v1","requestId":"ascii-id"},"response":{"error":"string?","ok":"bool","requestId":"ascii-id","result":"json?","version":2}}"#;
const MAXIMUM_ID_BYTES: usize = 128;

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
/// envelope. A `UUIDv7` is unique without any coordination (74 random bits within
/// each millisecond) and ordered by issue time, which is all the ledger needs
/// to bound its replay window.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub(crate) struct OperationId(uuid::Uuid);

impl OperationId {
    fn fresh() -> Self {
        Self(uuid::Uuid::now_v7())
    }

    fn validate(self) -> Result<(), String> {
        if self.0.get_version() == Some(uuid::Version::SortRand)
            && self.0.get_variant() == uuid::Variant::RFC4122
        {
            Ok(())
        } else {
            Err("Acyclic operation identity must be a UUIDv7".to_owned())
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

/// Newest terminal outcomes kept for replay; older ones fall below the horizon.
const RETAINED_OUTCOMES: usize = 1_024;
const RETAINED_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
const MAXIMUM_STARTED_OPERATIONS: usize = 16_384;
const MAXIMUM_JOURNAL_BYTES: u64 = 64 * 1024 * 1024;
const JOURNAL_REWRITE_SLACK: u64 = 1024 * 1024;
/// The journal alternates between two slots. Compaction rewrites the inactive
/// slot and publishes it by writing its generation frame last, so the active
/// slot only ever changes by appending past its end.
const JOURNAL_SLOTS: [&str; 2] = ["control-ledger-v4.a", "control-ledger-v4.b"];
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
    Horizon(OperationId),
    Begin {
        operation: OperationId,
        request_digest: [u8; 32],
    },
    Complete {
        operation: OperationId,
        response: &'a [u8],
    },
}

impl<'a> Record<'a> {
    fn encode_into(&self, frames: &mut Vec<u8>) {
        let mut record = Vec::new();
        match self {
            Self::Generation(generation) => {
                record.push(0);
                record.extend_from_slice(&generation.to_le_bytes());
            }
            Self::Horizon(operation) => {
                record.push(1);
                record.extend_from_slice(operation.0.as_bytes());
            }
            Self::Begin {
                operation,
                request_digest,
            } => {
                record.push(2);
                record.extend_from_slice(operation.0.as_bytes());
                record.extend_from_slice(request_digest);
            }
            Self::Complete {
                operation,
                response,
            } => {
                record.push(3);
                record.extend_from_slice(operation.0.as_bytes());
                record.extend_from_slice(response);
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
            (1, Some(operation), Some([])) => Some(Self::Horizon(operation)),
            (2, Some(operation), Some(request_digest)) => Some(Self::Begin {
                operation,
                request_digest: request_digest.try_into().ok()?,
            }),
            (3, Some(operation), Some(response)) => Some(Self::Complete {
                operation,
                response,
            }),
            _ => None,
        }
    }
}

fn record_digest(record: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic-control-ledger-v4\0");
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
    /// Began before a service crash, so its effects are unknown.
    Interrupted { request_digest: [u8; 32] },
}

impl Outcome {
    const fn request_digest(&self) -> &[u8; 32] {
        match self {
            Self::Completed { request_digest, .. } | Self::Interrupted { request_digest } => {
                request_digest
            }
        }
    }
}

/// Every operation that ever began is started, retained as an outcome, or at
/// or below the horizon. An unknown operation above the horizon therefore
/// never ran, and an unknown one at or below it may have.
#[derive(Default)]
struct LedgerState {
    horizon: Option<OperationId>,
    started: BTreeMap<OperationId, [u8; 32]>,
    outcomes: BTreeMap<OperationId, Outcome>,
    response_bytes: usize,
}

impl LedgerState {
    fn replay(&mut self, record: Record<'_>) -> Result<(), String> {
        match record {
            Record::Horizon(operation) => self.horizon = self.horizon.max(Some(operation)),
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
                let request_digest = self
                    .started
                    .remove(&operation)
                    .ok_or("Acyclic control journal completes an operation it never began")?;
                self.retain(
                    operation,
                    Outcome::Completed {
                        request_digest,
                        response: response.to_vec(),
                    },
                );
            }
            Record::Generation(_) | Record::Begin { .. } => {
                return Err("Acyclic control journal is inconsistent".to_owned());
            }
        }
        Ok(())
    }

    /// Records a terminal outcome, evicting the oldest ones below the horizon
    /// once the retained window is full.
    fn retain(&mut self, operation: OperationId, outcome: Outcome) {
        if let Outcome::Completed { response, .. } = &outcome {
            self.response_bytes += response.len();
        }
        self.outcomes.insert(operation, outcome);
        while self.outcomes.len() > RETAINED_OUTCOMES
            || self.response_bytes > RETAINED_RESPONSE_BYTES
        {
            let Some((evicted, outcome)) = self.outcomes.pop_first() else {
                break;
            };
            if let Outcome::Completed { response, .. } = outcome {
                self.response_bytes -= response.len();
            }
            self.horizon = self.horizon.max(Some(evicted));
        }
    }

    fn snapshot(&self, generation: u64) -> Vec<u8> {
        let mut frames = Vec::new();
        Record::Generation(generation).encode_into(&mut frames);
        if let Some(horizon) = self.horizon {
            Record::Horizon(horizon).encode_into(&mut frames);
        }
        let started = self
            .started
            .iter()
            .map(|(operation, request_digest)| (operation, request_digest, None));
        let outcomes = self.outcomes.iter().map(|(operation, outcome)| {
            let response = match outcome {
                Outcome::Completed { response, .. } => Some(response.as_slice()),
                Outcome::Interrupted { .. } => None,
            };
            (operation, outcome.request_digest(), response)
        });
        for (&operation, &request_digest, response) in started.chain(outcomes) {
            Record::Begin {
                operation,
                request_digest,
            }
            .encode_into(&mut frames);
            if let Some(response) = response {
                Record::Complete {
                    operation,
                    response,
                }
                .encode_into(&mut frames);
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
        let mut state = LedgerState::default();
        if generation.is_some() {
            let [active, _] = &slots;
            let bytes = read_slot(active)?;
            for record in decode_frames(&bytes).skip(1) {
                state.replay(record)?;
            }
        }
        for (operation, request_digest) in std::mem::take(&mut state.started) {
            state.retain(operation, Outcome::Interrupted { request_digest });
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
                Some(Outcome::Interrupted { .. }) | None => Err(
                    "Acyclic operation outcome is indeterminate; it will not be replayed"
                        .to_owned(),
                ),
            };
        }
        if state.horizon >= Some(operation) {
            return Err(
                "Acyclic operation outcome is indeterminate or outside the retained replay window; it will not be replayed"
                    .to_owned(),
            );
        }
        if state.started.len() >= MAXIMUM_STARTED_OPERATIONS {
            return Err(
                "Acyclic control replay ledger has too many concurrent operations".to_owned(),
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
        );
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

    fn begin(ledger: &ControlLedger, envelope: &ControlEnvelope<&str>) -> Result<bool, String> {
        ledger.begin(envelope).map(|decision| match decision {
            LedgerDecision::Execute => true,
            LedgerDecision::Completed(response) => {
                assert_eq!(response, serde_json::json!({"ok": envelope.request}));
                false
            }
        })
    }

    fn run(ledger: &ControlLedger, envelope: &ControlEnvelope<&str>) {
        assert!(begin(ledger, envelope).expect("begin operation"));
        ledger
            .complete(envelope, &serde_json::json!({"ok": envelope.request}))
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
    fn exact_offer_and_time_ordered_operations_are_required() {
        let envelope = ControlEnvelope::new("request");
        envelope.validate().expect("current envelope");
        assert_eq!(
            hex::encode(ProtocolOffer::current().schema_digest),
            "cbd3b7cb3c07dcd17a7f1091b43b945b8d057a541e5933f1175ad05ee1ccd4f6"
        );

        let mut incompatible = envelope.clone();
        incompatible.protocol.major += 1;
        assert!(incompatible.validate().is_err());

        let mut unordered = envelope;
        unordered.operation = OperationId(uuid::Uuid::new_v4());
        assert!(unordered.validate().is_err());
    }

    #[test]
    fn operations_are_unique_and_ordered_without_client_state() {
        let operations = (0..100_000)
            .map(|_| OperationId::fresh())
            .collect::<Vec<_>>();
        assert!(operations.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn completed_operations_replay_across_restarts_and_identities_bind_one_request() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let ledger = ControlLedger::open(temporary.path()).expect("ledger");
        let completed = ControlEnvelope::new("completed");
        run(&ledger, &completed);
        assert_eq!(begin(&ledger, &completed), Ok(false));
        drop(ledger);

        // A restarted service replays the outcome and still admits operations
        // issued by clients that started after it.
        let reopened = ControlLedger::open(temporary.path()).expect("reopen ledger");
        assert_eq!(begin(&reopened, &completed), Ok(false));
        let mut reused = completed.clone();
        reused.request_id = RequestId::fresh();
        assert!(
            begin(&reopened, &reused)
                .expect_err("an operation cannot identify a later transmission")
                .contains("different request")
        );
        run(&reopened, &ControlEnvelope::new("after restart"));
    }

    #[test]
    fn a_crash_between_begin_and_complete_is_indeterminate_and_never_replayed() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let ledger = ControlLedger::open(temporary.path()).expect("ledger");
        let started = ControlEnvelope::new("started");
        assert_eq!(begin(&ledger, &started), Ok(true));
        assert!(
            begin(&ledger, &started)
                .expect_err("in-flight work must not run twice")
                .contains("indeterminate")
        );
        drop(ledger);
        for _ in 0..2 {
            let reopened = ControlLedger::open(temporary.path()).expect("reopen ledger");
            assert!(
                begin(&reopened, &started)
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
        let ledger = ControlLedger::open(temporary.path()).expect("ledger");
        let kept = ControlEnvelope::new("kept");
        run(&ledger, &kept);
        let torn = ControlEnvelope::new("torn");
        run(&ledger, &torn);
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
            assert_eq!(begin(&reopened, &kept), Ok(false));
            if tail_intact {
                assert_eq!(begin(&reopened, &torn), Ok(false));
            } else {
                assert!(
                    begin(&reopened, &torn)
                        .expect_err("a torn completion leaves its operation indeterminate")
                        .contains("indeterminate")
                );
            }
            run(&reopened, &ControlEnvelope::new("after tear"));
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
        run(&fresh, &ControlEnvelope::new("fresh"));
    }

    #[test]
    fn replay_window_and_journal_stay_bounded() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let ledger = ControlLedger::open(temporary.path()).expect("ledger");
        let evicted = ControlEnvelope::new("evicted");
        run(&ledger, &evicted);
        let interrupted = ControlEnvelope::new("interrupted");
        assert_eq!(begin(&ledger, &interrupted), Ok(true));
        for _ in 0..3 * RETAINED_OUTCOMES {
            run(&ledger, &ControlEnvelope::new("filler"));
        }
        let retained = ControlEnvelope::new("retained");
        run(&ledger, &retained);
        for _ in 0..20_000 {
            let envelope = ControlEnvelope::new("x");
            assert_eq!(begin(&ledger, &envelope), Ok(true));
            ledger
                .complete(&envelope, &Value::Null)
                .expect("complete filler");
        }
        drop(ledger);
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
            journal_bytes < 3 * JOURNAL_REWRITE_SLACK,
            "journal grew to {journal_bytes} bytes"
        );

        let reopened = ControlLedger::open(temporary.path()).expect("reopen ledger");
        assert!(
            begin(&reopened, &evicted)
                .expect_err("an evicted operation must not run again")
                .contains("outside the retained replay window")
        );
        assert!(
            begin(&reopened, &interrupted)
                .expect_err("an in-flight operation stays unresolved while work continues")
                .contains("indeterminate")
        );
        run(&reopened, &ControlEnvelope::new("fresh"));
    }
}
