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
//! their requests still match. A recorded call serves exactly one request;
//! identical requests in the source are answered in source order.
//!
//! Replayed tool calls do not run, so their effects are not reproduced: a
//! fork's volumes stay as its cut selected them. Executors whose call is a
//! harness effect (spawn, send, archive, wait) always run
//! ([`crate::tool::ToolExecutor::replays_recorded`]).
use super::*;
use acyclic_stream::{BoxProviderFuture as BoxFuture, BoxProviderStream as BoxStream};
use std::collections::{HashMap, VecDeque};
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

/// One recorded model call.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RecordedCall {
    /// Exact canonical [`ModelEvent`] JSON, in source order, ending with completion.
    pub events: Vec<Vec<u8>>,
    /// How the tool calls this answer made settled, inner calls of a
    /// composition included, by call id.
    pub tools: Vec<(String, RecordedToolOutcome)>,
}

/// A source run's completed calls, keyed by [`replay_key`]. Lookup and
/// consumption are O(1) per request.
#[derive(Debug, Default, PartialEq)]
pub struct Recording {
    calls: HashMap<[u8; 32], VecDeque<RecordedCall>>,
    remaining: usize,
}

impl Recording {
    /// Adds `call` as the next answer to the request with replay `key`.
    pub fn insert(&mut self, key: [u8; 32], call: RecordedCall) {
        self.calls.entry(key).or_default().push_back(call);
        self.remaining += 1;
    }

    /// Unused recorded calls.
    #[must_use]
    pub fn len(&self) -> usize {
        self.remaining
    }

    /// Whether every recorded call has been used.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.remaining == 0
    }

    fn take(&mut self, key: &[u8; 32]) -> Option<RecordedCall> {
        let queue = self.calls.get_mut(key)?;
        let call = queue.pop_front()?;
        self.remaining -= 1;
        if queue.is_empty() {
            self.calls.remove(key);
        }
        Some(call)
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
    recording: Recording,
    tools: HashMap<(OperationId, String), RecordedToolOutcome>,
    /// Turns that went live; each stays live.
    live: std::collections::HashSet<OperationId>,
    diverged: bool,
}

/// Serves a [`Recording`] while requests match it, then the live provider.
pub struct RecordedModel {
    live: Arc<dyn ModelProvider>,
    replay: Arc<Mutex<Replay>>,
    sink: Arc<dyn DivergenceSink>,
}

impl RecordedModel {
    /// Replays `recording` in front of `live`, reporting divergence to `sink`.
    #[must_use]
    pub fn new(
        live: Arc<dyn ModelProvider>,
        recording: Recording,
        sink: Arc<dyn DivergenceSink>,
    ) -> Self {
        Self {
            live,
            replay: Arc::new(Mutex::new(Replay {
                recording,
                ..Replay::default()
            })),
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
        self.lock().recording.len()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Replay> {
        self.replay.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The recorded answer to this request, or `None` once it goes live.
    fn take(
        &self,
        request: &PreparedModelRequest,
        dispatch: ModelDispatch,
    ) -> Result<Option<Vec<Vec<u8>>>> {
        let key = request.replay_key()?;
        let mut replay = self.lock();
        if replay.live.contains(&dispatch.operation_id) {
            return Ok(None);
        }
        let Some(call) = replay.recording.take(&key) else {
            replay.live.insert(dispatch.operation_id);
            let first = !std::mem::replace(&mut replay.diverged, true);
            drop(replay);
            if first {
                self.sink.diverged(Divergence { dispatch, key });
            }
            return Ok(None);
        };
        for (call_id, result) in call.tools {
            replay
                .tools
                .insert((dispatch.operation_id, call_id), result);
        }
        Ok(Some(call.events))
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

    proptest! {
        #[test]
        fn recorded_call_consumption_preserves_order_and_unused_count(
            operations in prop::collection::vec((any::<u8>(), any::<bool>()), 0..256),
        ) {
            let mut recording = Recording::default();
            let mut source = Vec::new();
            for (sequence, (key, insert)) in operations.into_iter().enumerate() {
                let key = [key; 32];
                if insert {
                    // Generated opaque operands, not external provider fixtures.
                    let call = RecordedCall {
                        events: vec![sequence.to_le_bytes().to_vec()],
                        tools: Vec::new(),
                    };
                    source.push((key, call.clone()));
                    recording.insert(key, call);
                } else {
                    let expected = source.iter().position(|(candidate, _)| *candidate == key)
                        .map(|at| source.remove(at).1);
                    prop_assert_eq!(recording.take(&key), expected);
                }
                prop_assert_eq!(recording.len(), source.len());
                prop_assert_eq!(recording.is_empty(), source.is_empty());
            }
            for (key, expected) in source {
                prop_assert_eq!(recording.take(&key), Some(expected));
            }
            prop_assert_eq!(recording.len(), 0);
            prop_assert!(recording.calls.is_empty());
            prop_assert!(recording.take(&[0; 32]).is_none());
            prop_assert_eq!(recording.len(), 0);
        }
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
    ) -> BoxStream<'a, Result<ModelEvent>> {
        match self.take(request, dispatch) {
            Ok(Some(events)) => Box::pin(futures::stream::iter(
                events.into_iter().map(|bytes| decode(&bytes)),
            )),
            Ok(None) => self.live.generate(request, dispatch),
            Err(error) => Box::pin(futures::stream::iter([Err(error)])),
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
    ) -> Option<RecordedToolOutcome> {
        self.lock().tools.remove(&(operation, call_id.to_owned()))
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
        let Some(events) = self.take(request, dispatch)? else {
            return self.live.generate_with_native_work(request, dispatch, work);
        };
        // Each recorded event is admitted on the original ledger exactly as
        // a provider's decoded bytes are.
        crate::NativeFutureBundle::new(work, move || async move {
            NativeModelStream::new(work, move || {
                futures::stream::iter(
                    events
                        .into_iter()
                        .map(move |bytes| ModelEvent::decode_with_native_work(&bytes, work)),
                )
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
