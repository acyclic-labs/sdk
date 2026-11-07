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
//! before. Fresh storage locators can replay verified content; model-visible
//! discovery provenance remains exact until its producer declares typed identity.
//!
//! A turn goes live at its first divergent step and stays live; its agent's
//! later turns carry the live answer, so they match only a request whose
//! content the source also sent. Other agents keep replaying while
//! their requests still match. A recorded call serves exactly one accepted
//! dispatch; retrying that dispatch retains its original answer. Identical
//! requests in the source are answered in source order. Adapters
//! share an immutable source through `Arc<Recording>`; each fresh adapter
//! consumes independently, without rebuilding or copying source calls.
//!
//! Replayed tool calls do not run, so their effects are not reproduced: a
//! fork's volumes stay as its cut selected them. Executors whose call is a
//! harness effect (spawn, send, archive, wait) always run
//! ([`crate::tool::ToolExecutor::replays_recorded`]).
use super::*;
use crate::NativeIndexWork;
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
    fn select<'s, T>(
        &mut self,
        source: &'s Recording,
        key: [u8; 32],
        work: &mut impl IndexWork<Error = Error, Allocation = crate::NativeCustody>,
        admit: impl FnOnce(Option<(usize, &'s RecordedCall)>) -> Result<T>,
    ) -> Result<T> {
        let Some(calls) = source.calls.get(&key) else {
            work.check()?;
            return admit(None);
        };
        let next = self.next.get_or_try_insert_with(key, work, |_| Ok(0))?;
        let Some(call) = calls.get(*next) else {
            return admit(None);
        };
        let result = admit(Some((*next, call)))?;
        *next += 1;
        self.used += 1;
        Ok(result)
    }

    #[cfg(test)]
    fn take<'s>(
        &mut self,
        source: &'s Recording,
        key: [u8; 32],
        work: &mut impl IndexWork<Error = Error, Allocation = crate::NativeCustody>,
    ) -> Result<Option<(usize, &'s RecordedCall)>> {
        self.select(source, key, work, Ok)
    }
}

mod identity;

/// Replay identity of an encoded provider-neutral request. Authored text,
/// schemas, tool JSON, notices and native artifacts remain exact. Only typed
/// files whose policy projects verified content omit their storage locator;
/// metadata-only references and instruction framing retain their identity.
///
/// This decodes saved input once. Execution uses the admitted request's cached
/// [`PreparedModelRequest::replay_key`] instead. No embedded text is parsed and
/// no UUID or object shape declares a replaceable identity slot.
pub fn replay_key(request: &[u8]) -> Result<[u8; 32]> {
    let request: ModelRequest = crate::contract::json_from_slice(request)
        .map_err(|error| Error::Invalid(format!("replay request: {error}")))?;
    replay_key_of(&request)
}

pub(super) fn replay_key_of(request: &ModelRequest) -> Result<[u8; 32]> {
    identity::key(request)
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
    dispatches: FixedIndex<Option<ReplaySelection>, crate::NativeCustody, 52>,
    // Original source coordinates: copy the outcome only if it is requested.
    tools: HashMap<(OperationId, String), ([u8; 32], usize, usize)>,
    /// Turns that went live; each stays live.
    live: FixedIndex<bool, crate::NativeCustody, 16>,
    diverged: bool,
}

#[derive(Clone, Copy)]
enum ReplaySelection {
    Pending,
    Recorded { key: [u8; 32], index: usize },
    Live,
}

fn dispatch_key(dispatch: ModelDispatch) -> [u8; 52] {
    let mut key = [0; 52];
    key[..16].copy_from_slice(&dispatch.operation_id.into_bytes());
    key[16..20].copy_from_slice(&dispatch.step.to_be_bytes());
    key[20..].copy_from_slice(&dispatch.request_digest);
    key
}

/// Owns only a not-yet-accepted live admission. Dropping a refused builder
/// releases this original registration; accepted dispatches remain retained.
struct ReplayAdmission<'a> {
    owner: &'a RecordedModel,
    dispatch: ModelDispatch,
    key: Option<[u8; 32]>,
    accepted: bool,
}

impl ReplayAdmission<'_> {
    fn accept(mut self) {
        let mut replay = self.owner.lock();
        let selection = replay
            .dispatches
            .get_retained_mut(
                dispatch_key(self.dispatch),
                |selection| matches!(selection, Some(ReplaySelection::Pending)),
                |_| {},
            )
            .expect("original replay admission owns its registration");
        *selection = Some(ReplaySelection::Live);
        *replay
            .live
            .get_retained_mut(self.dispatch.operation_id.into_bytes(), |_| true, |_| {})
            .expect("original live admission retains its funded turn slot") = true;
        let first = !std::mem::replace(&mut replay.diverged, true);
        self.accepted = true;
        drop(replay);
        if first {
            self.owner.sink.diverged(Divergence {
                dispatch: self.dispatch,
                key: self
                    .key
                    .expect("first live admission retains its replay key"),
            });
        }
    }
}

impl Drop for ReplayAdmission<'_> {
    fn drop(&mut self) {
        if !self.accepted {
            if let Some(selection) = self.owner.lock().dispatches.get_retained_mut(
                dispatch_key(self.dispatch),
                |selection| matches!(selection, Some(ReplaySelection::Pending)),
                |_| {},
            ) {
                *selection = None;
            }
        }
    }
}

enum ReplayPreparation<'a, T> {
    Recorded(T),
    Live(ReplayAdmission<'a>),
}

/// Serves a [`Recording`] while requests match it, then the live provider.
/// Native live-turn storage is admitted before calling that provider. Accepting
/// its returned generation object only publishes into the retained turn slot;
/// it never grows or resizes a membership table.
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

    fn with_live(&self, live: Arc<dyn ModelProvider>) -> Self {
        Self {
            live,
            recording: Arc::clone(&self.recording),
            replay: Arc::clone(&self.replay),
            sink: Arc::clone(&self.sink),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Replay> {
        self.replay.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Accepts source consumption only after the complete returned object has
    /// been admitted. The builder runs under the original selection lock and
    /// must not invoke another provider or poll a future.
    fn prepare<'a, T>(
        &'a self,
        request: &PreparedModelRequest,
        dispatch: ModelDispatch,
        work: crate::NativeWork<'_>,
        build: impl FnOnce(&'a RecordedCall) -> Result<T>,
    ) -> Result<ReplayPreparation<'a, T>> {
        let mut replay = self.lock();
        let Replay {
            cursor,
            dispatches,
            tools,
            live,
            ..
        } = &mut *replay;
        let selection = dispatches.get_or_try_insert_with(
            dispatch_key(dispatch),
            &mut NativeIndexWork(work),
            |_| Ok(None),
        )?;
        match selection {
            Some(ReplaySelection::Recorded { key, index }) => {
                return build(&self.recording.calls[&*key][*index])
                    .map(ReplayPreparation::Recorded);
            }
            Some(ReplaySelection::Live) => {
                return Err(Error::Conflict(
                    "accepted live dispatch requires reconciliation".into(),
                ));
            }
            Some(ReplaySelection::Pending) => {
                return Err(Error::Conflict(
                    "replay dispatch admission is pending".into(),
                ));
            }
            None => {}
        }
        // Retained dispatches and live turns cannot select another recording.
        // Only a new replay candidate needs the request's semantic identity.
        let key = if live
            .get(
                dispatch.operation_id.into_bytes(),
                &mut NativeIndexWork(work),
            )?
            .copied()
            .unwrap_or(false)
        {
            None
        } else {
            Some(request.replay_key()?)
        };
        let output = if let Some(key) = key {
            cursor.select(
                &self.recording,
                key,
                &mut NativeIndexWork(work),
                |candidate| {
                    let Some((index, call)) = candidate else {
                        return Ok(None);
                    };
                    let output = build(call)?;
                    for (ordinal, (call_id, _)) in call.tools.iter().enumerate() {
                        tools.insert(
                            (dispatch.operation_id, call_id.clone()),
                            (key, index, ordinal),
                        );
                    }
                    *selection = Some(ReplaySelection::Recorded { key, index });
                    Ok(Some(output))
                },
            )?
        } else {
            None
        };
        match output {
            Some(output) => Ok(ReplayPreparation::Recorded(output)),
            None => {
                // Fund the permanent live-turn slot before invoking the live
                // provider. Acceptance only publishes into retained storage;
                // a refused builder leaves the original turn non-live.
                live.get_or_try_insert_with(
                    dispatch.operation_id.into_bytes(),
                    &mut NativeIndexWork(work),
                    |_| Ok(false),
                )?;
                *selection = Some(ReplaySelection::Pending);
                Ok(ReplayPreparation::Live(ReplayAdmission {
                    owner: self,
                    dispatch,
                    key,
                    accepted: false,
                }))
            }
        }
    }

    fn selected_call(
        &self,
        dispatch: ModelDispatch,
        work: crate::NativeWork<'_>,
    ) -> Result<Option<&RecordedCall>> {
        let replay = self.lock();
        match replay
            .dispatches
            .get(dispatch_key(dispatch), &mut NativeIndexWork(work))?
            .copied()
            .flatten()
        {
            Some(ReplaySelection::Recorded { key, index }) => {
                Ok(Some(&self.recording.calls[&key][index]))
            }
            Some(ReplaySelection::Pending) => Err(Error::Conflict(
                "replay dispatch admission is pending".into(),
            )),
            Some(ReplaySelection::Live) | None => Ok(None),
        }
    }
}

fn replay_suffix<C>(
    call: &RecordedCall,
    observed: &[ModelEvent],
    mut decode: impl FnMut(&[u8]) -> Result<(ModelEvent, C)>,
    mut retain: impl FnMut(ModelEvent, C) -> Result<()>,
) -> Result<()> {
    let conflict = || Error::Conflict("recorded dispatch observed a different event prefix".into());
    if observed.len() > call.events.len() {
        return Err(conflict());
    }
    for (index, bytes) in call.events.iter().enumerate() {
        let (event, custody) = decode(bytes)?;
        if let Some(expected) = observed.get(index) {
            let matches = event == *expected;
            // Destroy discarded storage before refunding its original owner.
            drop(event);
            drop(custody);
            if !matches {
                return Err(conflict());
            }
        } else {
            retain(event, custody)?;
        }
    }
    Ok(())
}

fn decode(bytes: &[u8]) -> Result<ModelEvent> {
    crate::contract::json_from_slice(bytes)
        .map_err(|error| Error::Storage(format!("recorded model event: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

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
                    let actual = cursor.take(&recording, key, &mut NativeIndexWork(None))
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
                    cursor.take(&recording, key, &mut NativeIndexWork(None))
                        .map_err(|error| TestCaseError::fail(error.to_string()))?
                        .map(|(_, call)| call.clone()),
                    Some(expected)
                );
            }
            prop_assert_eq!(cursor.used, recording.len());
            prop_assert!(cursor.take(&recording, [0; 32], &mut NativeIndexWork(None))
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
            cursor.take(&source, [0; 32], &mut NativeIndexWork(Some(&work))),
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
            cursor.take(&source, [0; 32], &mut NativeIndexWork(Some(&work))),
            Err(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
                ..
            })
        ));
        assert!(matches!(
            cursor.take(&source, [1; 32], &mut NativeIndexWork(Some(&work))),
            Err(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
                ..
            })
        ));
        assert_eq!(cursor.used, 0);
        assert_eq!(
            cursor
                .take(&source, [0; 32], &mut NativeIndexWork(None))?
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
                .take(&source, [0; 32], &mut NativeIndexWork(Some(&work)))?
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
    #[tokio::test]
    async fn live_turn_storage_is_funded_before_allocation_free_acceptance() -> Result<()> {
        let request = PreparedModelRequest::prepare(
            ModelRequest {
                summary: false,
                compose_tools: false,
                model: Model::new("recorded-control".into(), "admission".into(), "1".into())?,
                messages: vec![ModelMessage {
                    role: ModelRole::User,
                    content: ModelContent::Text("source".into()),
                }],
                tools: Vec::new(),
                sampling: Default::default(),
                max_output_tokens: None,
            },
            crate::conversation::Limits::default(),
            None,
        )?;
        let key = request.replay_key()?;
        let event = ModelEvent::Completed {
            metadata: Value::Null,
        };
        let bound = FixedIndex::<bool, crate::NativeCustody, 16>::insertion_bound(1).unwrap();
        let dispatch_bound =
            FixedIndex::<Option<ReplaySelection>, crate::NativeCustody, 52>::insertion_bound(1)
                .unwrap();
        for population in [1u128, 1024] {
            let mut source = Recording::default();
            source.insert(
                key,
                RecordedCall {
                    events: vec![crate::contract::canonical_json_bytes(&event)?],
                    ..Default::default()
                },
            );
            // Both layers are the shipped recording adapter. The outer live
            // path must deliver the inner source's original canonical event.
            let live = Arc::new(RecordedModel::new(
                Arc::new(crate::test_support::ScriptedModelProvider::new([])),
                Arc::new(source),
                Arc::new(Quiet),
            ));
            let trial = RecordedModel::new(live.clone(), Arc::default(), Arc::new(Quiet));
            let cancellation = acyclic_fs::CancellationToken::new();
            let mut budget = acyclic_fs::performance::BOUNDARY_BUDGET;
            budget.allocation_operations = 1;
            let refused = NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(error, _)| error)?;
            let dispatch = |operation: u128| ModelDispatch {
                operation_id: OperationId::from_bytes(operation.to_be_bytes()),
                step: 0,
                request_digest: request.request_digest(),
            };
            assert!(
                matches!(
                    trial.generate_with_native_work(&request, dispatch(0), &refused),
                    Err(Error::NativeWork { .. })
                ),
                "live turn storage escaped original allocation admission"
            );
            assert!(!trial.diverged());
            assert_eq!(live.unused(), 1, "refusal reached the live producer");
            let funded = NativeMaterializationWork::new(
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &cancellation,
            )
            .map_err(|(error, _)| error)?;
            let retained = funded.live_allocation_bytes();
            for operation in 0..population {
                let before = funded.receipt();
                let ReplayPreparation::Live(admission) =
                    trial.prepare(&request, dispatch(operation), Some(&funded), |_| Ok(()))?
                else {
                    return Err(Error::Invalid("empty recording selected an answer".into()));
                };
                let prepared = funded.receipt();
                assert!(
                    prepared.items_examined - before.items_examined
                        <= dispatch_bound.examined_items + 2 * bound.examined_items
                );
                assert!(prepared.allocation_operations - before.allocation_operations <= 4);
                assert_eq!(
                    acyclic_test_support::measure(|| admission.accept()).count_total,
                    0,
                    "live acceptance allocated after admission"
                );
                assert_eq!(funded.receipt(), prepared, "acceptance spent new work");
            }
            assert!(funded.live_allocation_bytes() > retained);
            assert_eq!(
                live.unused(),
                1,
                "admission control consumed the live source"
            );
            let mut stream = trial
                .generate_with_native_work(&request, dispatch(population), &funded)?
                .await?;
            let (actual, custody) = futures::StreamExt::next(&mut stream).await.unwrap()?;
            assert_eq!(actual, event);
            drop(custody);
            assert!(futures::StreamExt::next(&mut stream).await.is_none());
            drop(stream);
            assert_eq!(live.unused(), 0);
            assert!(trial.diverged());
            drop(trial);
            // The independently retained downstream adapter owns its nodes.
            drop(live);
            assert_eq!(funded.live_allocation_bytes(), retained);
        }
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn refused_generation_funding_preserves_replay_and_tool_coordinates() -> Result<()> {
        use crate::executor::json::CodecOperation;
        use acyclic_test_support::differential::observe;

        // Generated canonical SDK inputs, not a captured provider response.
        let request = PreparedModelRequest::prepare(
            ModelRequest {
                summary: false,
                compose_tools: false,
                model: Model::new("recorded-control".into(), "admission".into(), "1".into())?,
                messages: vec![ModelMessage {
                    role: ModelRole::User,
                    content: ModelContent::Text("source".into()),
                }],
                tools: Vec::new(),
                sampling: Default::default(),
                max_output_tokens: None,
            },
            crate::conversation::Limits::default(),
            None,
        )?;
        let dispatch = ModelDispatch {
            operation_id: OperationId::new(),
            step: 0,
            request_digest: request.request_digest(),
        };
        let key = replay_key(request.bytes())?;
        let events = [0, 1].map(|occurrence| ModelEvent::Completed {
            metadata: serde_json::json!({"occurrence": occurrence}),
        });
        let mut source = Recording::default();
        for (ordinal, event) in events.iter().enumerate() {
            source.insert(
                key,
                RecordedCall {
                    source: Some(ModelDispatch {
                        step: ordinal as u32,
                        ..dispatch
                    }),
                    events: vec![crate::contract::canonical_json_bytes(event)?],
                    tools: vec![(
                        format!("original-{ordinal}"),
                        RecordedToolOutcome::Completed(crate::tool::ToolResult {
                            value: Value::Null,
                        }),
                    )],
                },
            );
        }
        let source = Arc::new(source);
        let trial = || {
            RecordedModel::new(
                Arc::new(crate::test_support::ScriptedModelProvider::new([])),
                Arc::clone(&source),
                Arc::new(Quiet),
            )
        };
        // Measure every returned-object allocation through the actual public entry.
        // No event is decoded by this probe: per-event funding remains demand driven.
        let cancellation = acyclic_fs::CancellationToken::new();
        let probe =
            NativeMaterializationWork::new(acyclic_fs::performance::BOUNDARY_BUDGET, &cancellation)
                .map_err(|(error, _)| error)?;
        let probe_trial = trial();
        let stream = probe_trial
            .generate_with_native_work(&request, dispatch, &probe)?
            .await?;
        let complete_allocations = probe.receipt().allocation_operations;
        drop(stream);
        assert!(complete_allocations > 1);
        for allocations in 1..complete_allocations {
            let mut budget = acyclic_fs::performance::BOUNDARY_BUDGET;
            budget.allocation_operations = allocations;
            let refused = NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(error, _)| error)?;
            let trial = trial();
            assert!(
                matches!(
                    trial.generate_with_native_work(&request, dispatch, &refused),
                    Err(Error::NativeWork { .. })
                ),
                "returned-object admission must refuse synchronously at allocation {allocations}",
            );
            assert_eq!(
                trial.unused(),
                2,
                "refused funding consumed a source occurrence"
            );
            assert!(
                trial.lock().tools.is_empty(),
                "refused funding published tool coordinates"
            );
            assert!(
                !trial.diverged(),
                "refused replay admission entered the live provider"
            );
            let funded = NativeMaterializationWork::new(
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &cancellation,
            )
            .map_err(|(error, _)| error)?;
            let mut stream = trial
                .generate_with_native_work(&request, dispatch, &funded)?
                .await?;
            assert_eq!(
                stream.delivery(),
                ModelDelivery::Recorded {
                    source: Some(dispatch)
                }
            );
            let (actual, custody) = futures::StreamExt::next(&mut stream).await.unwrap()?;
            assert_eq!(
                actual, events[0],
                "retry skipped the original source occurrence"
            );
            drop(custody);
            assert!(futures::StreamExt::next(&mut stream).await.is_none());
            assert_eq!(trial.unused(), 1);
            assert_eq!(trial.lock().tools.len(), 1);
        }
        // Cancellation after acceptance retains the selected original terminal.
        let trial = trial();
        let accepted_cancellation = acyclic_fs::CancellationToken::new();
        let accepted = NativeMaterializationWork::new(
            acyclic_fs::performance::BOUNDARY_BUDGET,
            &accepted_cancellation,
        )
        .map_err(|(error, _)| error)?;
        let mut interrupted = trial
            .generate_with_native_work(&request, dispatch, &accepted)?
            .await?;
        accepted_cancellation.cancel();
        assert!(matches!(
            futures::StreamExt::next(&mut interrupted).await,
            Some(Err(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
                ..
            }))
        ));
        assert!(futures::StreamExt::next(&mut interrupted).await.is_none());
        drop(interrupted);
        let funded =
            NativeMaterializationWork::new(acyclic_fs::performance::BOUNDARY_BUDGET, &cancellation)
                .map_err(|(error, _)| error)?;
        let mut resumed = trial
            .generate_with_native_work(&request, dispatch, &funded)?
            .await?;
        assert_eq!(
            resumed.delivery(),
            ModelDelivery::Recorded {
                source: Some(dispatch)
            }
        );
        let (actual, custody) = futures::StreamExt::next(&mut resumed).await.unwrap()?;
        assert_eq!(
            actual, events[0],
            "accepted failure consumed a different recorded terminal"
        );
        drop(custody);
        drop(resumed);
        assert_eq!(trial.unused(), 1);
        assert_eq!(trial.lock().tools.len(), 1);
        let next = ModelDispatch {
            step: 1,
            ..dispatch
        };
        let mut stream = trial
            .generate_with_native_work(&request, next, &funded)?
            .await?;
        assert_eq!(
            stream.delivery(),
            ModelDelivery::Recorded { source: Some(next) }
        );
        let (actual, custody) = futures::StreamExt::next(&mut stream).await.unwrap()?;
        assert_eq!(actual, events[1]);
        drop(custody);
        assert_eq!(trial.unused(), 0);
        assert_eq!(trial.lock().tools.len(), 2);
        assert!(!trial.diverged());

        // Reconciliation and the portable entrance retain this accepted
        // source coordinate even after the recording is otherwise exhausted.
        assert_eq!(
            trial.reconcile(dispatch, &[]).await?,
            Some(vec![events[0].clone()])
        );
        assert_eq!(trial.reconcile(dispatch, &events[..1]).await?, Some(vec![]));
        let (reconciled, custody) = trial
            .reconcile_with_native_work(dispatch, &[], &funded)?
            .await?;
        assert_eq!(reconciled, Some(vec![events[0].clone()]));
        drop(custody);
        let retained = funded.live_allocation_bytes();
        let (reconciled, custody) = trial
            .reconcile_with_native_work(dispatch, &events[..1], &funded)?
            .await?;
        assert_eq!(reconciled, Some(vec![]));
        assert_eq!(
            funded.live_allocation_bytes(),
            retained,
            "observed replay prefix retained discarded storage funding"
        );
        drop(reconciled);
        drop(custody);
        assert!(matches!(
            trial.reconcile(dispatch, &events[1..]).await,
            Err(Error::Conflict(_))
        ));
        let fresh = PreparedModelRequest::prepare(
            request.request().clone(),
            crate::conversation::Limits::default(),
            None,
        )?;
        let (mut portable, trace) =
            observe::<_, _, CodecOperation>(|| (), |()| trial.generate(&fresh, dispatch));
        assert!(
            trace.is_empty(),
            "retained dispatch recomputed replay identity"
        );
        assert_eq!(
            futures::StreamExt::next(&mut portable).await.unwrap()?,
            events[0]
        );
        assert_eq!(trial.unused(), 0);
        let (identity, cold) = observe::<_, _, CodecOperation>(|| (), |()| fresh.replay_key());
        assert_eq!(identity?, key);
        assert_eq!(
            cold,
            [CodecOperation::Serialize],
            "cold identity negative control"
        );
        let fresh = PreparedModelRequest::prepare(
            request.request().clone(),
            crate::conversation::Limits::default(),
            None,
        )?;
        let (resumed, trace) = observe::<_, _, CodecOperation>(
            || (),
            |()| trial.generate_with_native_work(&fresh, dispatch, &funded),
        );
        assert!(
            trace.is_empty(),
            "native retained dispatch recomputed replay identity"
        );
        let mut resumed = resumed?.await?;
        let (actual, custody) = futures::StreamExt::next(&mut resumed).await.unwrap()?;
        assert_eq!(actual, events[0]);
        drop(custody);
        drop(resumed);

        // The real trait's unsupported-native refusal must not pin live mode.
        // The scripted producer is only the subsequent fallthrough observer.
        #[derive(Default)]
        struct Divergences(Mutex<Vec<Divergence>>);
        impl DivergenceSink for Divergences {
            fn diverged(&self, divergence: Divergence) {
                self.0.lock().unwrap().push(divergence);
            }
        }
        let live = Arc::new(crate::test_support::ScriptedModelProvider::new([]));
        let divergences = Arc::new(Divergences::default());
        let live_trial = RecordedModel::new(
            live.clone(),
            Arc::new(Recording::default()),
            divergences.clone(),
        );
        assert!(matches!(
            live_trial.generate_with_native_work(&request, dispatch, &funded),
            Err(Error::NativeBoundary { .. })
        ));
        assert!(!live_trial.diverged());
        let ReplayPreparation::Live(pending) =
            live_trial.prepare(&request, dispatch, None, |_| Ok(()))?
        else {
            return Err(Error::Invalid(
                "empty recording unexpectedly selected an answer".into(),
            ));
        };
        assert!(matches!(
            live_trial.prepare(&request, dispatch, None, |_| Ok(())),
            Err(Error::Conflict(_))
        ));
        drop(pending);
        assert!(!live_trial.diverged());
        let mut portable = live_trial.generate(&request, dispatch);
        assert!(
            futures::StreamExt::next(&mut portable)
                .await
                .unwrap()
                .is_err()
        );
        assert!(live_trial.diverged());
        let mut retry = live_trial.generate(&request, dispatch);
        assert!(matches!(
            futures::StreamExt::next(&mut retry).await,
            Some(Err(Error::Conflict(_)))
        ));
        assert_eq!(
            live.dispatches().len(),
            1,
            "accepted live failure was dispatched again"
        );
        for count in [1, 1024] {
            let mut input = request.request().clone();
            input.messages = vec![input.messages[0].clone(); count];
            let fresh =
                PreparedModelRequest::prepare(input, crate::conversation::Limits::default(), None)?;
            let next = ModelDispatch {
                step: count as u32,
                request_digest: fresh.request_digest(),
                ..dispatch
            };
            let (refused, trace) = observe::<_, _, CodecOperation>(
                || (),
                |()| live_trial.generate_with_native_work(&fresh, next, &funded),
            );
            assert!(matches!(refused, Err(Error::NativeBoundary { .. })));
            assert!(trace.is_empty(), "live turn recomputed replay identity");
            let (mut stream, trace) =
                observe::<_, _, CodecOperation>(|| (), |()| live_trial.generate(&fresh, next));
            assert!(
                trace.is_empty(),
                "portable live turn recomputed replay identity"
            );
            assert!(
                futures::StreamExt::next(&mut stream)
                    .await
                    .unwrap()
                    .is_err()
            );
            assert!(live_trial.diverged());
        }
        assert_eq!(
            *divergences.0.lock().unwrap(),
            [Divergence { dispatch, key }]
        );
        assert_eq!(live.dispatches().len(), 3);
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

                let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
                let retained = work.live_allocation_bytes();
                let foreign = acyclic_fs::NativeMaterializationWork::new(
                    acyclic_fs::performance::BOUNDARY_BUDGET,
                    &cancellation,
                )
                .map_err(|(error, _)| error)?;
                let foreign_custody = foreign.claim(0, 0, Default::default())?;
                assert!(matches!(
                    io.hold_native_custody(foreign_custody),
                    Err(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Invalid,
                        ..
                    })
                ));
                let resource =
                    foreign.native_volume_work(acyclic_fs::performance::BOUNDARY_BUDGET)?;
                assert!(foreign.shares_ledger(&resource));
                assert!(!foreign.shares_allocation_context(&resource));
                let foreign_io = crate::filesystem::PreparationIo::from_native_work(&foreign)?;
                let resource_custody = resource.claim(0, 0, Default::default())?;
                assert!(matches!(
                    foreign_io.hold_native_custody(resource_custody),
                    Err(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Invalid,
                        ..
                    })
                ));
                assert_eq!(work.live_allocation_bytes(), retained);
                for _ in 0..2 {
                    trial
                        .lock()
                        .tools
                        .insert((operation, call_id.into()), ([0; 32], 0, 0));
                    let (outcome, custody) = trial
                        .recorded_tool_result(operation, call_id, Some(&work))?
                        .unwrap();
                    let receipt = (outcome, io.hold_native_custody(custody.unwrap())?);
                    assert_eq!(receipt.0, original);
                    assert!(work.live_allocation_bytes() > retained);
                    drop(receipt);
                    assert_eq!(
                        work.live_allocation_bytes(),
                        retained,
                        "result custody refunds while the preparation remains alive"
                    );
                }

                // Absorbing the same custody into the preparation reproduces
                // the old retention: dropping the payload cannot refund it.
                trial
                    .lock()
                    .tools
                    .insert((operation, call_id.into()), ([0; 32], 0, 0));
                let (outcome, custody) = trial
                    .recorded_tool_result(operation, call_id, Some(&work))?
                    .unwrap();
                io.absorb_native_custody(custody.unwrap())?;
                drop(outcome);
                assert!(work.live_allocation_bytes() > retained);
                drop(io);
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
}

impl ModelProvider for RecordedModel {
    fn bind_content(
        &self,
        reader: Arc<dyn crate::conversation::ContentResidencyVerifier>,
        maximum_bytes: u64,
    ) -> Option<Arc<dyn ModelProvider>> {
        let live = self.live.bind_content(reader, maximum_bytes)?;
        Some(Arc::new(self.with_live(live)))
    }
    fn model_profile(&self, model: &Model) -> Result<Option<ModelProfileBinding>> {
        self.live.model_profile(model)
    }
    #[cfg(feature = "filesystem")]
    fn admit_profile(
        &self,
        model: &Model,
        profile: &crate::profile::AgentProfile<'_>,
    ) -> Result<(Model, Arc<dyn ModelProvider>)> {
        let (model, live) = self.live.admit_profile(model, profile)?;
        Ok((model, Arc::new(self.with_live(live))))
    }
    fn native_tool_profile(&self, model: &Model) -> Result<Option<NativeToolProfile>> {
        self.live.native_tool_profile(model)
    }
    #[cfg(feature = "filesystem")]
    fn native_tools(
        &self,
        model: &Model,
    ) -> Result<Option<Arc<crate::tools::native::NativeTools>>> {
        self.live.native_tools(model)
    }
    fn context_capacity(&self, model: &Model) -> Result<crate::context::ModelContextCapacity> {
        self.live.context_capacity(model)
    }
    fn count_tokens<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
        ceiling: u64,
    ) -> BoxFuture<'a, Result<crate::context::ModelTokenCount>> {
        self.live.count_tokens(request, ceiling)
    }
    fn generate<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
        dispatch: ModelDispatch,
    ) -> ModelStream<'a> {
        match self.prepare(request, dispatch, None, |call| {
            Ok(ModelStream::new(Box::pin(futures::stream::iter(
                call.events.iter().map(|bytes| decode(bytes)),
            )))
            .with_delivery(ModelDelivery::Recorded {
                source: call.source,
            }))
        }) {
            Ok(ReplayPreparation::Recorded(stream)) => stream,
            Ok(ReplayPreparation::Live(admission)) => {
                let stream = self.live.generate(request, dispatch);
                admission.accept();
                stream
            }
            Err(error) => ModelStream::new(Box::pin(futures::stream::iter([Err(error)]))),
        }
    }
    fn reconcile<'a>(
        &'a self,
        dispatch: ModelDispatch,
        observed: &'a [ModelEvent],
    ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        match self.selected_call(dispatch, None) {
            Ok(Some(call)) => Box::pin(async move {
                let mut events = Vec::new();
                replay_suffix(
                    call,
                    observed,
                    |bytes| decode(bytes).map(|event| (event, ())),
                    |event, ()| {
                        events.push(event);
                        Ok(())
                    },
                )?;
                Ok(Some(events))
            }),
            Ok(None) => self.live.reconcile(dispatch, observed),
            Err(error) => Box::pin(async move { Err(error) }),
        }
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
        ceiling: u64,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (crate::context::ModelTokenCount, NativeAllocation)>>
    {
        self.live
            .count_tokens_with_native_work(request, ceiling, work)
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn generate_with_native_work<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
        dispatch: ModelDispatch,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, NativeModelStream<'a>>> {
        // Each recorded event is admitted on the original ledger exactly as
        // a provider's decoded bytes are. The complete returned stream and
        // future are funded before original source consumption is accepted.
        match self.prepare(request, dispatch, Some(work), |call| {
            let stream = NativeModelStream::new(work, move || {
                futures::stream::iter(
                    call.events
                        .iter()
                        .map(move |bytes| ModelEvent::decode_with_native_work(bytes, work)),
                )
            })?
            .with_delivery(ModelDelivery::Recorded {
                source: call.source,
            });
            crate::NativeFutureBundle::new(work, || std::future::ready(Ok(stream)))
        })? {
            ReplayPreparation::Recorded(future) => Ok(future),
            ReplayPreparation::Live(admission) => {
                let future = self
                    .live
                    .generate_with_native_work(request, dispatch, work)?;
                admission.accept();
                Ok(future)
            }
        }
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn reconcile_with_native_work<'a>(
        &'a self,
        dispatch: ModelDispatch,
        observed: &'a [ModelEvent],
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<Vec<ModelEvent>>, NativeAllocation)>> {
        let Some(call) = self.selected_call(dispatch, Some(work))? else {
            return self
                .live
                .reconcile_with_native_work(dispatch, observed, work);
        };
        crate::NativeFutureBundle::new(work, || async move {
            let io = crate::filesystem::PreparationIo::from_native_work(work)?;
            let mut events = Vec::new();
            crate::executor::stock_vec_reserve(
                &mut events,
                call.events.len().saturating_sub(observed.len()),
                Some(&io),
            )?;
            replay_suffix(
                call,
                observed,
                |bytes| ModelEvent::decode_with_native_work(bytes, work),
                |event, custody| {
                    io.absorb_native_custody(custody)?;
                    events.push(event);
                    Ok(())
                },
            )?;
            let retained = io.owned_bytes()?;
            Ok((Some(events), io.into_native_custody(retained)?))
        })
    }
}
