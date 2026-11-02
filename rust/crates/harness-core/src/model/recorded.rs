//! Replay from a recording: one adapter at the [`ModelProvider`] seam.
//!
//! A [`Recording`] holds a source run's completed model calls, keyed by the
//! [`replay_key`] of the request they answered, each with its exact canonical event bytes and the
//! validated results of the tool calls it made. While a request matches an
//! unused recorded call, [`RecordedModel`] serves that call's events verbatim,
//! native reasoning included, and the executor takes each of its tool results
//! instead of running the tool ([`ModelProvider::recorded_tool_result`]). The
//! first request with no recorded match goes to the live provider, and the
//! sink records that divergence point once. Determinism is never assumed: a
//! request matches only when the model would see the same content it saw
//! before, so a fresh run or a fork of the source replays it.
//!
//! A turn goes live at its first divergent step and stays live; its agent's
//! later turns carry the live answer, so they match only a request whose
//! content the source also sent. Other agents keep replaying while
//! their requests still match. A recorded call serves exactly one request;
//! identical requests in the source are answered in source order. Adapters
//! share an immutable source through `Arc<Recording>`; each fresh adapter
//! consumes independently, without rebuilding or copying source calls.
//!
//! Replayed tool calls do not run, so their effects are not reproduced: a
//! fork's volumes stay as its cut selected them. Executors whose call is a
//! harness effect (spawn, send, archive, wait) always run
//! ([`crate::tool::ToolExecutor::replays_recorded`]).
use super::*;
use acyclic_collections::{FixedIndex, IndexWork};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

/// How a recorded tool call settled.
#[derive(Clone, Debug, PartialEq)]
pub enum RecordedToolOutcome {
    /// Its validated result.
    Completed(crate::tool::ToolResult),
    /// Its failure, settled again with the same reason and model-visible cause.
    Failed {
        /// Stage that failed.
        reason: crate::executor::ToolFailureKind,
        /// Sanitized cause the model received.
        cause: crate::executor::ToolFailureCause,
    },
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl RecordedToolOutcome {
    fn copy_with_custody(
        &self,
        io: &crate::filesystem::PreparationIo,
    ) -> Result<(Self, crate::NativeCustody)> {
        use crate::executor::json::{JsonBounds, RECORD_MAXIMUM_DEPTH, json_value_copy_bounded};
        let before = io.owned_bytes()?;
        let outcome = match self {
            Self::Completed(result) => {
                let value = json_value_copy_bounded(
                    &result.value,
                    JsonBounds {
                        maximum_bytes: u64::MAX,
                        maximum_items: io.remaining()?.items_examined,
                        maximum_depth: RECORD_MAXIMUM_DEPTH,
                    },
                    Some(crate::wire::ErrorCode::Storage),
                    &mut |charge| io.codec_charge(charge, false),
                )?;
                Self::Completed(crate::tool::ToolResult { value })
            }
            Self::Failed { reason, cause } => {
                let message = crate::executor::stock_string_copy(&cause.message, Some(io))?;
                Self::Failed {
                    reason: *reason,
                    cause: crate::executor::ToolFailureCause {
                        class: cause.class,
                        message,
                    },
                }
            }
        };
        // Transfer only the output's storage. Temporary lookup funding stays
        // with this preparation until its owned key has been destroyed.
        let custody = io.take_native_custody(io.owned_since(before)?)?;
        Ok((outcome, Some(custody)))
    }
}

/// One recorded model call.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RecordedCall {
    /// Original admitted dispatch, absent for sources without that observation.
    pub source: Option<ModelDispatch>,
    /// Exact canonical [`ModelEvent`] JSON, in source order, ending with completion.
    pub events: Vec<Vec<u8>>,
    /// How the tool calls this answer made settled, inner calls of a
    /// composition included, by call id.
    pub tools: Vec<(String, RecordedToolOutcome)>,
}

/// A source run's completed calls, keyed by [`replay_key`]. Build once, then
/// share an `Arc<Recording>` across independently consuming adapters. Call
/// payloads remain immutable; a trial never clones or rebuilds this source.
#[derive(Debug, Default, PartialEq)]
pub struct Recording {
    calls: HashMap<[u8; 32], Vec<RecordedCall>>,
    count: usize,
}

impl Recording {
    /// Offline projection grouped by request key, with calls in source
    /// occurrence order. Key order is unspecified; traversal is proportional
    /// to the recording's capacity.
    pub fn calls(&self) -> impl Iterator<Item = (&[u8; 32], &[RecordedCall])> {
        self.calls
            .iter()
            .map(|(key, calls)| (key, calls.as_slice()))
    }

    /// Adds `call` as the next answer to the request with replay `key`.
    pub fn insert(&mut self, key: [u8; 32], call: RecordedCall) {
        self.calls.entry(key).or_default().push(call);
        self.count += 1;
    }

    /// Source calls, independent of any trial's consumption.
    #[must_use]
    pub fn len(&self) -> usize {
        self.count
    }

    /// Whether the source contains no calls.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
}

/// One position per consumed source key, never a retained call payload.
/// Fixed-width indexing avoids history-dependent hash-table resizing. A new
/// key allocates at most two nodes; repeated keys update one existing integer.
#[derive(Default)]
struct Cursor {
    next: FixedIndex<usize, crate::NativeCustody, 32>,
    used: usize,
}

impl Cursor {
    fn take<'s>(
        &mut self,
        source: &'s Recording,
        key: [u8; 32],
        work: &mut impl IndexWork<Error = Error, Allocation = crate::NativeCustody>,
    ) -> Result<Option<(usize, &'s RecordedCall)>> {
        let Some(calls) = source.calls.get(&key) else {
            work.check()?;
            return Ok(None);
        };
        let next = self.next.get_or_try_insert_with(key, work, |_| Ok(0))?;
        let Some(call) = calls.get(*next) else {
            return Ok(None);
        };
        let index = *next;
        *next += 1;
        self.used += 1;
        Ok(Some((index, call)))
    }
}

/// The original invocation funds cursor nodes before allocation. Portable
/// calls use the existing unbounded-work choice, not a second private ledger.
struct CursorWork<'a>(crate::NativeWork<'a>);

impl IndexWork for CursorWork<'_> {
    type Error = Error;
    type Allocation = crate::NativeCustody;

    fn check(&mut self) -> Result<()> {
        self.charge_items(0)
    }

    fn charge_items(&mut self, items: u64) -> Result<()> {
        let _ = (self.0, items);
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(original) = self.0 {
            original.admit(acyclic_fs::WorkCounters {
                items_examined: items,
                ..Default::default()
            })?;
        }
        Ok(())
    }

    fn claim(&mut self, bytes: u64) -> Result<Self::Allocation> {
        let _ = (self.0, bytes);
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(original) = self.0 {
            return original
                .claim(bytes, 1, acyclic_fs::WorkCounters::UNCHARGED)
                .map(Some)
                .map_err(Into::into);
        }
        Ok(None)
    }
}

/// The replay key of exact canonical request bytes: their BLAKE3 digest once
/// fresh harness identity is erased, so a request matches whenever the model
/// would see the same content. Only harness-owned values are touched: system
/// messages (the skill catalog among them), notices and file references.
/// There every UUID (agent, volume, operation and input names embed them), a
/// file reference's storage version and a discovery generation's key are
/// erased; file content stays pinned by its descriptor digest. User text,
/// model output and tool JSON are kept exactly. One pass over the request,
/// on the replay path only.
pub fn replay_key(request: &[u8]) -> Result<[u8; 32]> {
    use serde_json::Value;
    let invalid = |error: serde_json::Error| Error::Invalid(format!("replay request: {error}"));
    let mut value: Value = crate::contract::json_from_slice(request).map_err(invalid)?;
    if let Some(messages) = value.get_mut("messages").and_then(Value::as_array_mut) {
        for message in messages {
            if message.get("role").and_then(Value::as_str) == Some("system") {
                erase_owned(message);
            } else {
                erase_references(message);
            }
        }
    }
    Ok(*blake3::hash(&serde_json::to_vec(&value).map_err(invalid)?).as_bytes())
}

/// Erases identity inside the harness-owned values of a conversation message.
fn erase_references(value: &mut serde_json::Value) {
    use serde_json::Value;
    match value {
        Value::Object(map) => {
            let reference = ["volume", "path", "version", "descriptor"]
                .iter()
                .all(|key| map.contains_key(*key));
            if reference || map.get("kind").and_then(Value::as_str) == Some("notices") {
                erase_owned(value);
            } else {
                map.values_mut().for_each(erase_references);
            }
        }
        Value::Array(items) => items.iter_mut().for_each(erase_references),
        _ => {}
    }
}

/// Erases identity throughout a harness-owned value, JSON-valued text included.
fn erase_owned(value: &mut serde_json::Value) {
    erase_owned_at(value, None);
}

// Outer structure was admitted with its request. Embedded text starts a single
// saved-record depth allowance, shared by any further JSON encoded in strings.
fn erase_owned_at(value: &mut serde_json::Value, embedded_depth: Option<u32>) {
    use serde_json::Value;
    let depth = embedded_depth.unwrap_or(crate::executor::json::RECORD_MAXIMUM_DEPTH);
    let child_depth = embedded_depth.map(|depth| depth.saturating_sub(1));
    match value {
        Value::Object(map) => {
            if map.contains_key("descriptor") {
                map.remove("version");
            }
            if map.get("kind").and_then(Value::as_str) == Some("generation") {
                map.remove("key");
            }
            map.values_mut()
                .for_each(|value| erase_owned_at(value, child_depth));
        }
        Value::Array(items) => items
            .iter_mut()
            .for_each(|value| erase_owned_at(value, child_depth)),
        Value::String(text) => match embedded_value(text, depth) {
            Some(mut nested) => {
                erase_owned_at(&mut nested, Some(depth));
                *text = nested.to_string();
            }
            _ => erase_uuids(text),
        },
        _ => {}
    }
}

/// Text is not an admitted JSON tree. Interpret embedded structure only within
/// the saved-record codec's existing depth contract; deeper or malformed text
/// remains text and still receives UUID erasure. Admission precedes recursive
/// decode, traversal, serialization and drop, including on a failed parse.
fn embedded_value(text: &str, maximum_depth: u32) -> Option<serde_json::Value> {
    use crate::executor::json::{JsonBounds, JsonValueSchema, json_from_slice_bounded};
    if maximum_depth == 0 || !matches!(text.trim_start().as_bytes().first(), Some(b'{' | b'[')) {
        return None;
    }
    json_from_slice_bounded(
        std::marker::PhantomData::<serde_json::Value>,
        text.as_bytes(),
        JsonBounds {
            maximum_bytes: text.len() as u64,
            maximum_items: text.len() as u64,
            maximum_depth,
        },
        None,
        Some(JsonValueSchema::Root),
        &mut |_| Ok(()),
    )
    .ok()
}

/// Replaces each hyphenated UUID in `text` with `*`.
fn erase_uuids(text: &mut String) {
    const LENGTH: usize = 36;
    let bytes = text.as_bytes();
    let uuid = |at: usize| {
        bytes.get(at..at + LENGTH).is_some_and(|window| {
            window.iter().enumerate().all(|(index, byte)| match index {
                8 | 13 | 18 | 23 => *byte == b'-',
                _ => byte.is_ascii_hexdigit(),
            })
        })
    };
    let (mut erased, mut resume) = (String::with_capacity(text.len()), 0);
    for (at, character) in text.char_indices() {
        if at < resume {
            continue;
        }
        if uuid(at) {
            erased.push('*');
            resume = at + LENGTH;
        } else {
            erased.push(character);
        }
    }
    // Each erasure shortens the text, so equal length means none happened.
    if erased.len() != text.len() {
        *text = erased;
    }
}

/// The first request a recording could not answer.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Divergence {
    /// The live call's identity: its turn, step and exact request digest.
    pub dispatch: ModelDispatch,
    /// The request's [`replay_key`], which no recorded call answers.
    pub key: [u8; 32],
}

/// Receives the divergence point once, when the first request goes live.
pub trait DivergenceSink: acyclic_stream::ProviderPlatform {
    /// Records `divergence`; failures are the sink's to report.
    fn diverged(&self, divergence: Divergence);
}

#[derive(Default)]
struct Replay {
    cursor: Cursor,
    // Original source coordinates: copy the outcome only if it is requested.
    tools: HashMap<(OperationId, String), ([u8; 32], usize, usize)>,
    /// Turns that went live; each stays live.
    live: std::collections::HashSet<OperationId>,
    diverged: bool,
}

/// Serves a [`Recording`] while requests match it, then the live provider.
pub struct RecordedModel {
    live: Arc<dyn ModelProvider>,
    recording: Arc<Recording>,
    replay: Arc<Mutex<Replay>>,
    sink: Arc<dyn DivergenceSink>,
}

impl RecordedModel {
    /// Bind one independent trial to an immutable source and its live provider.
    /// Admission shares the source without scanning or copying calls. Reusing
    /// this adapter shares consumption; construct another for another trial.
    #[must_use]
    pub fn new(
        live: Arc<dyn ModelProvider>,
        recording: Arc<Recording>,
        sink: Arc<dyn DivergenceSink>,
    ) -> Self {
        Self {
            live,
            recording,
            replay: Arc::new(Mutex::new(Replay::default())),
            sink,
        }
    }

    /// Whether any request has gone live.
    #[must_use]
    pub fn diverged(&self) -> bool {
        self.lock().diverged
    }

    /// Recorded calls no request has used.
    #[must_use]
    pub fn unused(&self) -> usize {
        self.recording.len() - self.lock().cursor.used
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Replay> {
        self.replay.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The recorded answer to this request, or `None` once it goes live.
    fn take(
        &self,
        request: &PreparedModelRequest,
        dispatch: ModelDispatch,
        work: crate::NativeWork<'_>,
    ) -> Result<Option<&RecordedCall>> {
        let key = replay_key(request.bytes())?;
        let mut replay = self.lock();
        if replay.live.contains(&dispatch.operation_id) {
            return Ok(None);
        }
        let Some((index, call)) =
            replay
                .cursor
                .take(&self.recording, key, &mut CursorWork(work))?
        else {
            replay.live.insert(dispatch.operation_id);
            let first = !std::mem::replace(&mut replay.diverged, true);
            drop(replay);
            if first {
                self.sink.diverged(Divergence { dispatch, key });
            }
            return Ok(None);
        };
        for (ordinal, (call_id, _)) in call.tools.iter().enumerate() {
            replay.tools.insert(
                (dispatch.operation_id, call_id.clone()),
                (key, index, ordinal),
            );
        }
        Ok(Some(call))
    }
}

fn decode(bytes: &[u8]) -> Result<ModelEvent> {
    crate::contract::json_from_slice(bytes)
        .map_err(|error| Error::Storage(format!("recorded model event: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use serde_json::{Value, json};

    struct Quiet;
    impl DivergenceSink for Quiet {
        fn diverged(&self, _: Divergence) {}
    }

    proptest! {
        #[test]
        fn recorded_call_consumption_preserves_order_and_unused_count(
            operations in prop::collection::vec((any::<u8>(), any::<bool>()), 0..256),
        ) {
            let mut recording = Recording::default();
            let mut cursor = Cursor::default();
            let mut source = Vec::new();
            let mut inserted = 0;
            for (sequence, (key, insert)) in operations.into_iter().enumerate() {
                let key = [key; 32];
                if insert {
                    // Generated opaque operands, not external provider fixtures.
                    let call = RecordedCall {
                        source: None,
                        events: vec![sequence.to_le_bytes().to_vec()],
                        tools: Vec::new(),
                    };
                    source.push((key, call.clone()));
                    recording.insert(key, call);
                    inserted += 1;
                } else {
                    let expected = source.iter().position(|(candidate, _)| *candidate == key)
                        .map(|at| source.remove(at).1);
                    let actual = cursor.take(&recording, key, &mut CursorWork(None))
                        .map_err(|error| TestCaseError::fail(error.to_string()))?
                        .map(|(_, call)| call.clone());
                    prop_assert_eq!(actual, expected);
                }
                prop_assert_eq!(recording.len(), inserted);
                prop_assert_eq!(recording.is_empty(), inserted == 0);
                prop_assert_eq!(recording.len() - cursor.used, source.len());
            }
            for (key, expected) in source {
                prop_assert_eq!(
                    cursor.take(&recording, key, &mut CursorWork(None))
                        .map_err(|error| TestCaseError::fail(error.to_string()))?
                        .map(|(_, call)| call.clone()),
                    Some(expected)
                );
            }
            prop_assert_eq!(cursor.used, recording.len());
            prop_assert!(cursor.take(&recording, [0; 32], &mut CursorWork(None))
                .map_err(|error| TestCaseError::fail(error.to_string()))?.is_none());
            prop_assert_eq!(cursor.used, recording.len());
        }
    }

    #[test]
    fn trial_admission_shares_source_without_copying_its_population() {
        let live: Arc<dyn ModelProvider> =
            Arc::new(crate::test_support::ScriptedModelProvider::new([]));
        let sink: Arc<dyn DivergenceSink> = Arc::new(Quiet);
        for size in [1u64, 2048] {
            let mut source = Recording::default();
            for key in 0..size {
                let mut bytes = [0; 32];
                bytes[..8].copy_from_slice(&key.to_be_bytes());
                // Empty opaque operands; no provider event fixture is invented.
                source.insert(bytes, RecordedCall::default());
            }
            let source = Arc::new(source);
            let mut trial = None;
            let admission = acyclic_test_support::measure(|| {
                trial = Some(RecordedModel::new(
                    Arc::clone(&live),
                    Arc::clone(&source),
                    Arc::clone(&sink),
                ));
            });
            assert_eq!(admission.count_total, 1, "one independent replay state");
            let trial = trial.unwrap();
            assert!(Arc::ptr_eq(&trial.recording, &source));
            let copying = acyclic_test_support::measure(|| {
                std::hint::black_box(source.calls.clone());
            });
            assert!(
                copying.count_total > admission.count_total,
                "copying the source is the allocation negative control"
            );
        }
    }

    #[test]
    fn cursor_work_is_bounded_by_key_width_not_consumed_history() -> Result<()> {
        #[derive(Default)]
        struct Meter {
            items: u64,
            allocations: u64,
        }
        impl IndexWork for Meter {
            type Error = Error;
            type Allocation = crate::NativeCustody;
            fn check(&mut self) -> Result<()> {
                Ok(())
            }
            fn charge_items(&mut self, items: u64) -> Result<()> {
                self.items += items;
                Ok(())
            }
            fn claim(&mut self, _: u64) -> Result<Self::Allocation> {
                self.allocations += 1;
                Ok(None)
            }
        }
        let key = |index: u64| {
            let mut key = [0; 32];
            key[..8].copy_from_slice(&index.to_be_bytes());
            key
        };
        let bound = FixedIndex::<usize, crate::NativeCustody, 32>::insertion_bound(1).unwrap();
        for size in [2u64, 2048] {
            let mut source = Recording::default();
            for index in 0..size {
                source.insert(key(index), RecordedCall::default());
                source.insert(key(index), RecordedCall::default());
            }
            let mut cursor = Cursor::default();
            let mut work = Meter::default();
            for index in 0..size - 1 {
                assert_eq!(cursor.take(&source, key(index), &mut work)?.unwrap().0, 0);
            }
            let before = work.items;
            let allocations = acyclic_test_support::measure(|| {
                assert_eq!(
                    cursor
                        .take(&source, key(size - 1), &mut work)
                        .unwrap()
                        .unwrap()
                        .0,
                    0
                );
            });
            assert!(work.items - before <= bound.examined_items);
            assert!(allocations.count_total <= 2);
            let before = work.allocations;
            let repeated = acyclic_test_support::measure(|| {
                assert_eq!(
                    cursor
                        .take(&source, key(size - 1), &mut work)
                        .unwrap()
                        .unwrap()
                        .0,
                    1
                );
            });
            assert_eq!(work.allocations, before);
            assert_eq!(repeated.count_total, 0);
            assert!(cursor.take(&source, key(size - 1), &mut work)?.is_none());
            assert_eq!(cursor.used, size as usize + 1);
            let mut independent = Cursor::default();
            assert_eq!(
                independent
                    .take(&source, key(size - 1), &mut work)?
                    .unwrap()
                    .0,
                0,
                "a fresh trial starts at the original first answer"
            );
            assert!(
                cursor.take(&source, key(size - 1), &mut work)?.is_none(),
                "reusing the consumed cursor is the independence negative control"
            );
            if size == 2048 {
                let mut scanned = 0;
                let total: usize = source
                    .calls
                    .values()
                    .map(|calls| {
                        scanned += 1;
                        calls.len()
                    })
                    .sum();
                assert_eq!(total, size as usize * 2);
                assert!(
                    scanned > bound.examined_items,
                    "recounting history is the examined-item negative control"
                );
            }
        }
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn cursor_refusal_and_cancellation_preserve_the_original_answer() -> Result<()> {
        let mut source = Recording::default();
        source.insert([0; 32], RecordedCall::default());
        let cancellation = acyclic_fs::CancellationToken::new();
        let mut budget = acyclic_fs::performance::BOUNDARY_BUDGET;
        budget.items_examined = 0;
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(error, _)| error)?;
        let retained = work.live_allocation_bytes();
        let mut cursor = Cursor::default();
        assert!(matches!(
            cursor.take(&source, [0; 32], &mut CursorWork(Some(&work))),
            Err(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::Work(
                    acyclic_fs::WorkError::BudgetExceeded { .. }
                ),
                ..
            })
        ));
        assert_eq!(cursor.used, 0);
        assert_eq!(work.live_allocation_bytes(), retained);
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::BOUNDARY_BUDGET,
            &cancellation,
        )
        .map_err(|(error, _)| error)?;
        cancellation.cancel();
        assert!(matches!(
            cursor.take(&source, [0; 32], &mut CursorWork(Some(&work))),
            Err(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
                ..
            })
        ));
        assert!(matches!(
            cursor.take(&source, [1; 32], &mut CursorWork(Some(&work))),
            Err(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
                ..
            })
        ));
        assert_eq!(cursor.used, 0);
        assert_eq!(
            cursor
                .take(&source, [0; 32], &mut CursorWork(None))?
                .unwrap()
                .0,
            0
        );
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::BOUNDARY_BUDGET,
            &cancellation,
        )
        .map_err(|(error, _)| error)?;
        let retained = work.live_allocation_bytes();
        let mut funded = Cursor::default();
        assert_eq!(
            funded
                .take(&source, [0; 32], &mut CursorWork(Some(&work)))?
                .unwrap()
                .0,
            0
        );
        assert!(work.live_allocation_bytes() > retained);
        drop(funded);
        assert_eq!(
            work.live_allocation_bytes(),
            retained,
            "the original ledger retains and refunds cursor storage"
        );
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn tool_copy_uses_original_work_and_refusal_keeps_the_settlement() -> Result<()> {
        let live: Arc<dyn ModelProvider> =
            Arc::new(crate::test_support::ScriptedModelProvider::new([]));
        let sink: Arc<dyn DivergenceSink> = Arc::new(Quiet);
        let operation = OperationId::new();
        let call_id = "0";
        // Generated opaque copy operands, not provider or tool-output fixtures.
        for size in [1usize, 2048] {
            let text = "0".repeat(size);
            for original in [
                RecordedToolOutcome::Completed(crate::tool::ToolResult {
                    value: Value::Array(vec![Value::String(text.clone())]),
                }),
                RecordedToolOutcome::Failed {
                    reason: crate::executor::ToolFailureKind::ExecutorRejected,
                    cause: crate::executor::ToolFailureCause {
                        class: crate::executor::ToolErrorClass::Failed,
                        message: text.clone(),
                    },
                },
            ] {
                let mut source = Recording::default();
                source.insert(
                    [0; 32],
                    RecordedCall {
                        source: None,
                        events: Vec::new(),
                        tools: vec![(call_id.into(), original.clone())],
                    },
                );
                let source = Arc::new(source);
                let trial =
                    RecordedModel::new(Arc::clone(&live), Arc::clone(&source), Arc::clone(&sink));
                trial
                    .lock()
                    .tools
                    .insert((operation, call_id.into()), ([0; 32], 0, 0));
                let cancellation = acyclic_fs::CancellationToken::new();
                let mut budget = acyclic_fs::performance::BOUNDARY_BUDGET;
                // Admit the lookup key, then refuse the payload copy itself.
                budget.bytes_copied = call_id.len() as u64;
                let refused = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
                    .map_err(|(error, _)| error)?;
                let baseline = refused.live_allocation_bytes();
                let denial = trial.recorded_tool_result(operation, call_id, Some(&refused));
                let cause = match &denial {
                    Err(Error::NativeWork { cause, .. }) => Some(cause),
                    Err(Error::NativeFilesystem { source, .. }) => match source.as_ref() {
                        acyclic_fs::FsError::NativeKernel(cause) => Some(cause),
                        _ => None,
                    },
                    _ => None,
                };
                assert!(
                    matches!(
                        cause,
                        Some(acyclic_fs::NativeTreeMaterializationError::Work(
                            acyclic_fs::WorkError::BudgetExceeded {
                                counter: "bytes_copied",
                                ..
                            }
                        ))
                    ),
                    "original copy denial was {denial:?}"
                );
                assert_eq!(refused.live_allocation_bytes(), baseline);
                assert_eq!(trial.lock().tools.len(), 1);

                let work = acyclic_fs::NativeMaterializationWork::new(
                    acyclic_fs::performance::BOUNDARY_BUDGET,
                    &cancellation,
                )
                .map_err(|(error, _)| error)?;
                cancellation.cancel();
                assert!(matches!(
                    trial.recorded_tool_result(operation, call_id, Some(&work)),
                    Err(Error::NativeWork {
                        cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
                        ..
                    })
                ));
                assert_eq!(trial.lock().tools.len(), 1);

                let cancellation = acyclic_fs::CancellationToken::new();
                let work = acyclic_fs::NativeMaterializationWork::new(
                    acyclic_fs::performance::BOUNDARY_BUDGET,
                    &cancellation,
                )
                .map_err(|(error, _)| error)?;
                let baseline = work.live_allocation_bytes();
                let before = work.receipt();
                let copied = trial
                    .recorded_tool_result(operation, call_id, Some(&work))?
                    .unwrap();
                assert_eq!(copied.0, original);
                assert!(copied.1.as_ref().unwrap().work().shares_ledger(&work));
                assert_eq!(
                    work.receipt().bytes_copied - before.bytes_copied,
                    (call_id.len() + size) as u64,
                    "one lookup copy and one payload copy, with no document copy"
                );
                assert!(work.live_allocation_bytes() > baseline);
                assert!(
                    trial
                        .recorded_tool_result(operation, call_id, None)?
                        .is_none()
                );
                assert_eq!(source.calls[&[0; 32]][0].tools[0].1, original);
                drop(copied);
                assert_eq!(work.live_allocation_bytes(), baseline);

                // A plain clone allocates but charges none of the original
                // work: this detects the previous unaccounted-copy path.
                let before = work.receipt();
                let unaccounted = acyclic_test_support::measure(|| {
                    std::hint::black_box(original.clone());
                });
                assert!(unaccounted.count_total > 0);
                assert_eq!(work.receipt(), before);
            }
        }
        Ok(())
    }

    #[test]
    fn replay_preserves_admitted_nesting_and_owned_identity_rules() -> Result<()> {
        for depth in [126, 127, 128] {
            let nest = |mut value, depth| {
                for _ in 0..depth {
                    value = Value::Array(vec![value]);
                }
                value
            };
            let request = |generation: &str, argument: &str| {
                serde_json::to_vec(&json!({"messages": [
                    {"role": "system", "content": {
                        "kind": "text",
                        "text": nest(json!({"kind":"generation", "key":generation}), depth - 1).to_string()
                    }},
                    {"role": "assistant", "content": {
                        "kind": "tool_call", "arguments": nest(json!(argument), depth)
                    }}
                ]}))
                .expect("generated request JSON")
            };
            let original = request("original-generation", "original-argument");
            assert!(
                serde_json::from_slice::<Value>(&original).is_err(),
                "default decoder negative control must reject admitted nesting"
            );
            let key = replay_key(&original)?;
            assert_eq!(
                key,
                replay_key(&request("new-generation", "original-argument"))?,
                "nested harness-owned generation identity is erased"
            );
            assert_ne!(
                key,
                replay_key(&request("original-generation", "changed-argument"))?,
                "deep tool JSON remains content-exact"
            );

            let event = ModelEvent::Completed {
                metadata: nest(json!(true), depth),
            };
            let mut bytes = serde_json::to_vec(&event).expect("generated canonical model event");
            if depth == 128 {
                assert!(serde_json::from_slice::<ModelEvent>(&bytes).is_err());
            }
            assert_eq!(decode(&bytes)?, event);
            bytes.push(b']');
            assert!(
                matches!(decode(&bytes), Err(Error::Storage(_))),
                "shared decoding must still reject trailing data"
            );
            let mut malformed = original;
            malformed.push(b']');
            assert!(matches!(replay_key(&malformed), Err(Error::Invalid(_))));
        }
        Ok(())
    }

    #[test]
    fn embedded_text_is_admitted_before_recursive_interpretation() -> Result<()> {
        use crate::executor::json::RECORD_MAXIMUM_DEPTH;
        let uuid = "01234567-89ab-cdef-0123-456789abcdef";
        let mut rejected_allocations = None;
        for depth in [8192, 32768] {
            let text = format!("{}\"{uuid}\"{}", "[".repeat(depth), "]".repeat(depth));
            let allocations = acyclic_test_support::measure(|| {
                assert!(embedded_value(&text, RECORD_MAXIMUM_DEPTH).is_none());
            });
            let counts = (allocations.count_total, allocations.bytes_total);
            if let Some(previous) = rejected_allocations {
                assert_eq!(
                    counts, previous,
                    "no deep value graph is allocated or dropped"
                );
            }
            rejected_allocations = Some(counts);
            let expected = Value::String(text.replace(uuid, "*"));
            let mut original = Value::String(text.clone());
            erase_owned(&mut original);
            assert_eq!(original, expected, "deeper structure remains opaque text");
            let request =
                |text: &str| json!({"messages":[{"role":"system","content":text}]}).to_string();
            assert_eq!(
                replay_key(request(&text).as_bytes())?,
                replay_key(request(expected.as_str().expect("text")).as_bytes())?,
                "the whole request remains usable and UUID normalization is preserved"
            );
        }
        // Structured interpretation within the original contract remains
        // distinct from the opaque-text path, without executing an unsafe
        // unbounded recursive decode as the negative control.
        let mut structured = json!("[{\"kind\":\"generation\",\"key\":\"epoch\"}]");
        erase_owned(&mut structured);
        assert_eq!(structured, json!("[{\"kind\":\"generation\"}]"));
        let nest = |text| format!("{}{}{}", "[".repeat(90), text, "]".repeat(90));
        let inner = nest(r#"{"kind":"generation","key":"epoch"}"#.to_owned());
        let outer = nest(serde_json::to_string(&inner).expect("encoded nested text"));
        let mut encoded = Value::String(outer.clone());
        erase_owned(&mut encoded);
        assert_eq!(
            encoded,
            Value::String(outer),
            "encoded strings share one depth allowance rather than restarting it"
        );
        for text in ["[0] trailing", r#"[{"version":1"#] {
            assert!(
                embedded_value(text, RECORD_MAXIMUM_DEPTH).is_none(),
                "malformed JSON remains text"
            );
        }
        Ok(())
    }
}

impl ModelProvider for RecordedModel {
    fn bind_content(
        &self,
        reader: Arc<dyn crate::conversation::ContentResidencyVerifier>,
        maximum_bytes: u64,
    ) -> Option<Arc<dyn ModelProvider>> {
        let live = self.live.bind_content(reader, maximum_bytes)?;
        Some(Arc::new(Self {
            live,
            recording: Arc::clone(&self.recording),
            replay: Arc::clone(&self.replay),
            sink: Arc::clone(&self.sink),
        }))
    }
    fn model_profile(&self, model: &Model) -> Result<Option<ModelProfileBinding>> {
        self.live.model_profile(model)
    }
    fn native_tool_profile(&self, model: &Model) -> Result<Option<NativeToolProfile>> {
        self.live.native_tool_profile(model)
    }
    fn context_capacity(&self, model: &Model) -> Result<crate::context::ModelContextCapacity> {
        self.live.context_capacity(model)
    }
    fn count_tokens<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
    ) -> BoxFuture<'a, Result<crate::context::ModelTokenCount>> {
        self.live.count_tokens(request)
    }
    fn generate<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
        dispatch: ModelDispatch,
    ) -> ModelStream<'a> {
        match self.take(request, dispatch, None) {
            Ok(Some(call)) => ModelStream::new(Box::pin(futures::stream::iter(
                call.events.iter().map(|bytes| decode(bytes)),
            )))
            .with_delivery(ModelDelivery::Recorded {
                source: call.source,
            }),
            Ok(None) => self.live.generate(request, dispatch),
            Err(error) => ModelStream::new(Box::pin(futures::stream::iter([Err(error)]))),
        }
    }
    fn reconcile<'a>(
        &'a self,
        dispatch: ModelDispatch,
        observed: &'a [ModelEvent],
    ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        self.live.reconcile(dispatch, observed)
    }
    fn recorded_tool_result(
        &self,
        operation: OperationId,
        call_id: &str,
        work: crate::NativeWork<'_>,
    ) -> Result<Option<(RecordedToolOutcome, crate::NativeCustody)>> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let io = work
            .map(crate::filesystem::PreparationIo::from_native_work)
            .transpose()?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let lookup = (
            operation,
            crate::executor::stock_string_copy(call_id, io.as_ref())?,
        );
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let lookup = {
            let _ = work;
            (operation, call_id.to_owned())
        };
        let mut replay = self.lock();
        let Some(&(key, index, ordinal)) = replay.tools.get(&lookup) else {
            return Ok(None);
        };
        let original = &self.recording.calls[&key][index].tools[ordinal].1;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let copied = match io.as_ref() {
            Some(io) => original.copy_with_custody(io)?,
            None => (original.clone(), None),
        };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let copied = (original.clone(), None);
        replay.tools.remove(&lookup);
        Ok(Some(copied))
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_model(&self, work: &NativeMaterializationWork) -> Result<bool> {
        self.live.supports_native_model(work)
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn context_capacity_with_native_work(
        &self,
        model: &Model,
        work: &NativeMaterializationWork,
    ) -> Result<crate::context::ModelContextCapacity> {
        self.live.context_capacity_with_native_work(model, work)
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn count_tokens_with_native_work<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (crate::context::ModelTokenCount, NativeAllocation)>>
    {
        self.live.count_tokens_with_native_work(request, work)
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn generate_with_native_work<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
        dispatch: ModelDispatch,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, NativeModelStream<'a>>> {
        let Some(call) = self.take(request, dispatch, Some(work))? else {
            return self.live.generate_with_native_work(request, dispatch, work);
        };
        // Each recorded event is admitted on the original ledger exactly as
        // a provider's decoded bytes are.
        crate::NativeFutureBundle::new(work, move || async move {
            NativeModelStream::new(work, move || {
                futures::stream::iter(
                    call.events
                        .iter()
                        .map(move |bytes| ModelEvent::decode_with_native_work(bytes, work)),
                )
            })
            .map(|stream| {
                stream.with_delivery(ModelDelivery::Recorded {
                    source: call.source,
                })
            })
        })
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn reconcile_with_native_work<'a>(
        &'a self,
        dispatch: ModelDispatch,
        observed: &'a [ModelEvent],
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<Vec<ModelEvent>>, NativeAllocation)>> {
        self.live
            .reconcile_with_native_work(dispatch, observed, work)
    }
}
