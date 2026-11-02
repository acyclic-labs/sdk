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
//! An agent goes live at its first divergent step and stays live, since every
//! later request carries the live answer. Other agents keep replaying while
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

/// One recorded model call.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RecordedCall {
    /// Exact canonical [`ModelEvent`] JSON, in source order, ending with completion.
    pub events: Vec<Vec<u8>>,
    /// Validated results of the tool calls this answer made, by call id.
    pub tools: Vec<(String, crate::tool::ToolResult)>,
}

/// A source run's completed calls, keyed by [`replay_key`]. Lookup and
/// consumption are O(1) per request.
#[derive(Debug, Default, PartialEq)]
pub struct Recording {
    calls: HashMap<[u8; 32], VecDeque<RecordedCall>>,
}

impl Recording {
    /// Adds `call` as the next answer to the request with replay `key`.
    pub fn insert(&mut self, key: [u8; 32], call: RecordedCall) {
        self.calls.entry(key).or_default().push_back(call);
    }

    /// Unused recorded calls.
    #[must_use]
    pub fn len(&self) -> usize {
        self.calls.values().map(VecDeque::len).sum()
    }

    /// Whether every recorded call has been used.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.calls.is_empty()
    }

    fn take(&mut self, key: &[u8; 32]) -> Option<RecordedCall> {
        let queue = self.calls.get_mut(key)?;
        let call = queue.pop_front();
        if queue.is_empty() {
            self.calls.remove(key);
        }
        call
    }
}

/// The replay key of exact canonical request bytes: their BLAKE3 digest once
/// fresh identity is erased, so a request matches whenever the model would
/// see the same content. Erased are every UUID (agent, volume, operation and
/// input names embed them), a file reference's storage version and a
/// discovery generation's key, also inside JSON-valued text such as the
/// skill catalog. File content stays pinned by its descriptor digest. One
/// pass over the request, on the replay path only.
pub fn replay_key(request: &[u8]) -> Result<[u8; 32]> {
    let invalid = |error: serde_json::Error| Error::Invalid(format!("replay request: {error}"));
    let mut value: serde_json::Value = serde_json::from_slice(request).map_err(invalid)?;
    erase(&mut value);
    Ok(*blake3::hash(&serde_json::to_vec(&value).map_err(invalid)?).as_bytes())
}

fn erase(value: &mut serde_json::Value) {
    use serde_json::Value;
    match value {
        Value::Object(map) => {
            if map.contains_key("descriptor") {
                map.remove("version");
            }
            if map.get("kind").and_then(Value::as_str) == Some("generation") {
                map.remove("key");
            }
            map.values_mut().for_each(erase);
        }
        Value::Array(items) => items.iter_mut().for_each(erase),
        Value::String(text) => match serde_json::from_str::<Value>(text) {
            Ok(mut nested) if nested.is_object() || nested.is_array() => {
                erase(&mut nested);
                *text = nested.to_string();
            }
            _ => erase_uuids(text),
        },
        _ => {}
    }
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
    recording: Recording,
    tools: HashMap<(OperationId, String), crate::tool::ToolResult>,
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
        let key = replay_key(request.bytes())?;
        let mut replay = self.lock();
        let Some(call) = replay.recording.take(&key) else {
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
    serde_json::from_slice(bytes)
        .map_err(|error| Error::Storage(format!("recorded model event: {error}")))
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
    ) -> Option<crate::tool::ToolResult> {
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
        self.live.reconcile_with_native_work(dispatch, observed, work)
    }
}
