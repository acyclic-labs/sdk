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
        let key = replay_key(request.bytes())?;
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
    use serde_json::{Value, json};

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
