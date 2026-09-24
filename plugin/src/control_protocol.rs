//! Closed, exactly negotiated control-plane wire protocol.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs;
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(test)]
use std::sync::{Arc, OnceLock};

use acyclic_native_runtime::{RenameMode, durable_rename};
use fs2::FileExt as _;

pub(crate) const CONTROL_PROTOCOL_MAJOR: u32 = 2;
const CONTROL_SCHEMA: &[u8] = br#"{"envelope":{"operation":{"clientId":"ascii-id","sequence":"nonzero-u64"},"protocol":{"capabilities":{"atMostOnceOperations":true,"boundedFrames":true,"exactNegotiation":true},"major":2,"schemaDigest":"blake3-32"},"request":"ControlRequest-v1","requestId":"ascii-id"},"response":{"error":"string?","ok":"bool","requestId":"ascii-id","result":"json?","version":2}}"#;
const MAXIMUM_ID_BYTES: usize = 128;
const CONTROL_LEDGER_VERSION: u32 = 3;
const MAXIMUM_LEDGER_ENTRIES: usize = 16_384;
const MAXIMUM_LEDGER_BYTES: u64 = 16 * 1024 * 1024;
const RETAINED_COMPLETED_RESPONSES: usize = 1_024;

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

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct OperationId {
    client_id: String,
    sequence: u64,
}

impl OperationId {
    #[cfg(test)]
    pub(crate) fn fresh() -> Self {
        static CLIENT_ID: OnceLock<String> = OnceLock::new();
        static SEQUENCE: AtomicU64 = AtomicU64::new(1);
        Self {
            client_id: CLIENT_ID
                .get_or_init(|| uuid::Uuid::new_v4().to_string())
                .clone(),
            sequence: SEQUENCE.fetch_add(1, Ordering::Relaxed),
        }
    }

    pub(crate) fn allocate(data: &Path) -> Result<Self, String> {
        fs::create_dir_all(data).map_err(|error| error.to_string())?;
        let lock_path = data.join("control-client-v2.lock");
        let lock = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path)
            .map_err(|error| error.to_string())?;
        lock.lock_exclusive().map_err(|error| error.to_string())?;
        let state_path = data.join("control-client-v2.json");
        let mut state = match fs::read(&state_path) {
            Ok(bytes) => serde_json::from_slice::<ClientSequenceState>(&bytes)
                .map_err(|error| error.to_string())?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => ClientSequenceState {
                version: 1,
                client_id: uuid::Uuid::new_v4().to_string(),
                next_sequence: 1,
            },
            Err(error) => return Err(error.to_string()),
        };
        state.validate()?;
        let sequence = state.next_sequence;
        state.next_sequence = sequence
            .checked_add(1)
            .ok_or_else(|| "Acyclic control operation sequence is exhausted".to_owned())?;
        persist_json(&state_path, &state)?;
        Ok(Self {
            client_id: state.client_id,
            sequence,
        })
    }

    fn ephemeral() -> Self {
        Self {
            client_id: uuid::Uuid::new_v4().to_string(),
            sequence: 1,
        }
    }

    fn validate(&self) -> Result<(), String> {
        validate_id("client ID", &self.client_id)?;
        if self.sequence == 0 {
            return Err("Acyclic operation sequence must be nonzero".to_owned());
        }
        Ok(())
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
    #[cfg(test)]
    pub(crate) fn new(request: T) -> Self {
        Self {
            protocol: ProtocolOffer::current(),
            request_id: RequestId::fresh(),
            operation: OperationId::fresh(),
            request,
        }
    }

    pub(crate) fn new_for_install(data: &Path, request: T) -> Result<Self, String> {
        Ok(Self {
            protocol: ProtocolOffer::current(),
            request_id: RequestId::fresh(),
            operation: OperationId::allocate(data)?,
            request,
        })
    }

    pub(crate) fn ephemeral(request: T) -> Self {
        Self {
            protocol: ProtocolOffer::current(),
            request_id: RequestId::fresh(),
            operation: OperationId::ephemeral(),
            request,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        self.protocol.validate()?;
        self.request_id.validate()?;
        self.operation.validate()
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ClientSequenceState {
    version: u32,
    client_id: String,
    next_sequence: u64,
}

impl ClientSequenceState {
    fn validate(&self) -> Result<(), String> {
        if self.version != 1 || self.next_sequence == 0 {
            return Err("invalid Acyclic control client sequence state".to_owned());
        }
        validate_id("client ID", &self.client_id)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct LedgerState {
    version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    client_id: Option<String>,
    completed_through: u64,
    entries: BTreeMap<u64, LedgerEntry>,
}

impl Default for LedgerState {
    fn default() -> Self {
        Self {
            version: CONTROL_LEDGER_VERSION,
            client_id: None,
            completed_through: 0,
            entries: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct LedgerEntry {
    request_digest: [u8; 32],
    phase: LedgerPhase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    response: Option<Value>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum LedgerPhase {
    Started,
    Completed,
}

#[derive(Debug)]
pub(crate) enum LedgerDecision {
    Execute,
    Completed(Value),
}

pub(crate) struct ControlLedger {
    path: PathBuf,
    state: Mutex<LedgerState>,
}

impl ControlLedger {
    pub(crate) fn open(data: &Path) -> Result<Self, String> {
        let path = data.join("control-ledger-v3.json");
        let mut state = match fs::File::open(&path) {
            Ok(file) => read_ledger(file)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => LedgerState::default(),
            Err(error) => return Err(error.to_string()),
        };
        validate_ledger(&state)?;
        let recovered = terminalize_interrupted_operations(&mut state);
        compact_ledger(&mut state);
        if recovered {
            persist_ledger(&path, &state)?;
        }
        Ok(Self {
            path,
            state: Mutex::new(state),
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
        let client_id = &envelope.operation.client_id;
        let sequence = envelope.operation.sequence;
        let mut state = self
            .state
            .lock()
            .map_err(|_| "Acyclic control ledger lock is poisoned".to_owned())?;
        if let Some(bound_client) = &state.client_id
            && bound_client != client_id
        {
            return Err(
                "Acyclic control ledger belongs to a different installation client".to_owned(),
            );
        }
        if let Some(entry) = state.entries.get(&sequence) {
            if entry.request_digest != request_digest {
                return Err(
                    "Acyclic operation identity was reused for a different request".to_owned(),
                );
            }
            match entry.phase {
                LedgerPhase::Completed => {
                    return entry
                        .response
                        .clone()
                        .map(LedgerDecision::Completed)
                        .ok_or_else(|| {
                            "completed Acyclic operation omitted its response".to_owned()
                        });
                }
                LedgerPhase::Started => {
                    return Err(
                        "Acyclic operation outcome is indeterminate; it will not be replayed"
                            .to_owned(),
                    );
                }
            }
        }
        if sequence <= state.completed_through {
            return Err(
                        "Acyclic operation outcome is indeterminate or outside the retained replay window; it will not be replayed"
                            .to_owned(),
                    );
        }
        if state.entries.len() >= MAXIMUM_LEDGER_ENTRIES {
            return Err(
                "Acyclic control replay ledger has too many unresolved or concurrent operations"
                    .to_owned(),
            );
        }
        let mut next = state.clone();
        next.client_id.get_or_insert_with(|| client_id.clone());
        next.entries.insert(
            sequence,
            LedgerEntry {
                request_digest,
                phase: LedgerPhase::Started,
                response: None,
            },
        );
        self.persist(&next)?;
        *state = next;
        Ok(LedgerDecision::Execute)
    }

    pub(crate) fn complete<T>(
        &self,
        envelope: &ControlEnvelope<T>,
        response: Value,
    ) -> Result<(), String> {
        let client_id = &envelope.operation.client_id;
        let sequence = envelope.operation.sequence;
        let mut state = self
            .state
            .lock()
            .map_err(|_| "Acyclic control ledger lock is poisoned".to_owned())?;
        let mut next = state.clone();
        if next.client_id.as_deref() != Some(client_id.as_str()) {
            return Err("Acyclic control operation used the wrong installation client".to_owned());
        }
        let entry = next
            .entries
            .get_mut(&sequence)
            .ok_or_else(|| "Acyclic control operation was not started".to_owned())?;
        if entry.phase == LedgerPhase::Completed {
            if entry.response.as_ref() == Some(&response) {
                return Ok(());
            }
            return Err("completed Acyclic operation response changed".to_owned());
        }
        entry.phase = LedgerPhase::Completed;
        entry.response = Some(response);
        compact_ledger(&mut next);
        self.persist(&next)?;
        *state = next;
        Ok(())
    }

    fn persist(&self, state: &LedgerState) -> Result<(), String> {
        persist_ledger(&self.path, state)
    }
}

fn persist_ledger(path: &Path, state: &LedgerState) -> Result<(), String> {
    validate_ledger(state)?;
    let encoded = serde_json::to_vec(state).map_err(|error| error.to_string())?;
    if encoded.len() as u64 > MAXIMUM_LEDGER_BYTES {
        return Err("Acyclic control replay ledger exceeds its byte bound".to_owned());
    }
    let next = path.with_extension("next");
    let mut file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&next)
        .map_err(|error| error.to_string())?;
    file.write_all(&encoded)
        .map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())?;
    drop(file);
    durable_rename(&next, path, RenameMode::Replace).map_err(|error| error.to_string())
}

fn terminalize_interrupted_operations(state: &mut LedgerState) -> bool {
    let interrupted = state
        .entries
        .iter()
        .filter_map(|(&sequence, entry)| {
            (entry.phase != LedgerPhase::Completed).then_some(sequence)
        })
        .collect::<Vec<_>>();
    for sequence in &interrupted {
        state.entries.remove(sequence);
        state.completed_through = state.completed_through.max(*sequence);
    }
    !interrupted.is_empty()
}

fn compact_ledger(state: &mut LedgerState) {
    while state
        .entries
        .values()
        .filter(|entry| entry.phase == LedgerPhase::Completed)
        .count()
        > RETAINED_COMPLETED_RESPONSES
    {
        let Some(sequence) = state.entries.iter().find_map(|(&sequence, entry)| {
            (entry.phase == LedgerPhase::Completed).then_some(sequence)
        }) else {
            break;
        };
        state.entries.remove(&sequence);
        state.completed_through = state.completed_through.max(sequence);
    }
}

fn persist_json(path: &Path, value: &impl Serialize) -> Result<(), String> {
    let encoded = serde_json::to_vec(value).map_err(|error| error.to_string())?;
    let next = path.with_extension("next");
    let mut file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&next)
        .map_err(|error| error.to_string())?;
    file.write_all(&encoded)
        .map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())?;
    drop(file);
    durable_rename(&next, path, RenameMode::Replace).map_err(|error| error.to_string())
}

fn read_ledger(file: fs::File) -> Result<LedgerState, String> {
    if file.metadata().map_err(|error| error.to_string())?.len() > MAXIMUM_LEDGER_BYTES {
        return Err("Acyclic control replay ledger exceeds its byte bound".to_owned());
    }
    let mut bytes = Vec::new();
    file.take(MAXIMUM_LEDGER_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| error.to_string())?;
    if bytes.len() as u64 > MAXIMUM_LEDGER_BYTES {
        return Err("Acyclic control replay ledger exceeds its byte bound".to_owned());
    }
    serde_json::from_slice(&bytes).map_err(|error| error.to_string())
}

fn validate_ledger(state: &LedgerState) -> Result<(), String> {
    if state.version != CONTROL_LEDGER_VERSION {
        return Err(format!(
            "unsupported Acyclic control ledger version {}; expected {CONTROL_LEDGER_VERSION}",
            state.version
        ));
    }
    if state.entries.len() > MAXIMUM_LEDGER_ENTRIES {
        return Err("Acyclic control replay ledger exceeds its entry bound".to_owned());
    }
    if let Some(client_id) = &state.client_id {
        validate_id("client ID", client_id)?;
    } else if !state.entries.is_empty() || state.completed_through != 0 {
        return Err("Acyclic control ledger state has no installation client".to_owned());
    }
    if state.entries.keys().any(|sequence| *sequence == 0) {
        return Err("Acyclic control ledger contains a zero sequence".to_owned());
    }
    Ok(())
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
    reason = "test assertions deliberately fail fast and fixed-size windows are proven by windows(2)"
)]
mod tests {
    use super::*;

    #[test]
    fn exact_offer_and_bounded_ids_are_required() {
        let envelope = ControlEnvelope::new("request");
        envelope.validate().expect("current envelope");
        assert_eq!(
            hex::encode(ProtocolOffer::current().schema_digest),
            "1ba707606bca897425a8a2429fa53d0426eda7ddf9abb9680d531a91dad475f1"
        );

        let mut incompatible = envelope.clone();
        incompatible.protocol.major += 1;
        assert!(incompatible.validate().is_err());

        let mut malformed = envelope;
        malformed.operation.client_id = "contains spaces".to_owned();
        assert!(malformed.validate().is_err());
    }

    #[test]
    fn durable_ledger_returns_completed_outcomes_and_never_replays_started_work() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let ledger = ControlLedger::open(temporary.path()).expect("ledger");
        let completed = ControlEnvelope::new("completed");
        assert!(matches!(
            ledger.begin(&completed).expect("prepare completed"),
            LedgerDecision::Execute
        ));
        ledger
            .complete(&completed, serde_json::json!({"ok":true}))
            .expect("complete operation");
        assert!(matches!(
            ledger.begin(&completed).expect("replay completed"),
            LedgerDecision::Completed(response) if response == serde_json::json!({"ok":true})
        ));
        let mut reused_sequence = completed.clone();
        reused_sequence.request_id = RequestId::fresh();
        assert!(
            ledger
                .begin(&reused_sequence)
                .expect_err("a sequence cannot identify a later transmission")
                .contains("different request")
        );

        let started = ControlEnvelope::new("started");
        assert!(matches!(
            ledger.begin(&started).expect("prepare started"),
            LedgerDecision::Execute
        ));
        drop(ledger);

        let reopened = ControlLedger::open(temporary.path()).expect("reopen ledger");
        assert!(
            reopened
                .begin(&started)
                .expect_err("started work must not replay")
                .contains("indeterminate")
        );
    }

    #[test]
    fn installation_sequences_survive_process_reopen() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let first = OperationId::allocate(temporary.path()).expect("first operation");
        let second = OperationId::allocate(temporary.path()).expect("second operation");
        assert_eq!(first.client_id, second.client_id);
        assert_eq!(second.sequence, first.sequence + 1);
    }

    #[test]
    fn concurrent_installation_allocators_never_reuse_an_operation() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let path = Arc::new(temporary.path().to_path_buf());
        let mut threads = Vec::new();
        for _ in 0..32 {
            let path = Arc::clone(&path);
            threads.push(std::thread::spawn(move || {
                OperationId::allocate(&path).expect("operation allocation")
            }));
        }
        let mut operations = threads
            .into_iter()
            .map(|thread| thread.join().expect("allocator thread"))
            .collect::<Vec<_>>();
        operations.sort_by_key(|operation| operation.sequence);
        assert!(
            operations
                .windows(2)
                .all(|pair| pair[0].client_id == pair[1].client_id
                    && pair[1].sequence == pair[0].sequence + 1)
        );
    }

    #[test]
    fn completed_history_compacts_to_a_bounded_replay_window() {
        let mut ledger = LedgerState {
            client_id: Some("installation".to_owned()),
            ..LedgerState::default()
        };
        for sequence in 1..=20_000 {
            ledger.entries.insert(
                sequence,
                LedgerEntry {
                    request_digest: [0; 32],
                    phase: LedgerPhase::Completed,
                    response: Some(serde_json::json!({"sequence":sequence})),
                },
            );
            compact_ledger(&mut ledger);
        }
        assert_eq!(ledger.entries.len(), RETAINED_COMPLETED_RESPONSES);
        assert_eq!(
            ledger.completed_through,
            20_000 - u64::try_from(RETAINED_COMPLETED_RESPONSES).expect("retention fits u64")
        );

        let mut gapped = LedgerState {
            client_id: Some("installation".to_owned()),
            ..LedgerState::default()
        };
        gapped.entries.insert(
            1,
            LedgerEntry {
                request_digest: [1; 32],
                phase: LedgerPhase::Started,
                response: None,
            },
        );
        for sequence in 2..=20_001 {
            gapped.entries.insert(
                sequence,
                LedgerEntry {
                    request_digest: [0; 32],
                    phase: LedgerPhase::Completed,
                    response: Some(Value::Null),
                },
            );
            compact_ledger(&mut gapped);
        }
        assert_eq!(
            gapped.entries.len(),
            RETAINED_COMPLETED_RESPONSES + 1,
            "one unresolved operation must not prevent completed traffic from compacting"
        );
        assert!(gapped.entries.contains_key(&1));
    }

    #[test]
    fn ledger_rejects_a_second_installation_client() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let ledger = ControlLedger::open(temporary.path()).expect("ledger");
        let first = ControlEnvelope::new("first");
        assert!(matches!(
            ledger.begin(&first).expect("first installation client"),
            LedgerDecision::Execute
        ));
        let mut second = ControlEnvelope::new("second");
        second.operation.client_id = uuid::Uuid::new_v4().to_string();
        assert!(
            ledger
                .begin(&second)
                .expect_err("second client must be rejected")
                .contains("different installation client")
        );
    }

    #[test]
    fn restart_terminalizes_interrupted_operations_without_exhausting_the_ledger() {
        let mut state = LedgerState {
            client_id: Some("installation".to_owned()),
            ..LedgerState::default()
        };
        for sequence in 1..=MAXIMUM_LEDGER_ENTRIES as u64 {
            state.entries.insert(
                sequence,
                LedgerEntry {
                    request_digest: [0; 32],
                    phase: LedgerPhase::Started,
                    response: None,
                },
            );
        }
        assert!(terminalize_interrupted_operations(&mut state));
        assert!(state.entries.is_empty());
        assert_eq!(state.completed_through, MAXIMUM_LEDGER_ENTRIES as u64);
    }
}
