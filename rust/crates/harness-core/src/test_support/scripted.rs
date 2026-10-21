use crate::{
    Error, Result,
    context::{ModelContextCapacity, ModelTokenCount},
    model::{Model, ModelDispatch, ModelEvent, ModelProvider, ModelRequest, PreparedModelRequest},
};
use acyclic_stream::{BoxProviderFuture, BoxProviderStream};
use futures::{StreamExt as _, TryStreamExt as _};
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{SeqAccess, Visitor, value::SeqAccessDeserializer},
};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

/// One provider response. A string emits text followed by completion; an event
/// list is replayed exactly, including errors, incomplete streams and metadata.
#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
pub enum ScriptedTurn {
    /// Complete text-only response.
    Text(String),
    /// Exact ordered streaming response.
    Events(Vec<ScriptedEvent>),
}

impl<'de> Deserialize<'de> for ScriptedTurn {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct TurnVisitor;
        impl<'de> Visitor<'de> for TurnVisitor {
            type Value = ScriptedTurn;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a response string or an array of tagged scripted events")
            }
            fn visit_str<E: serde::de::Error>(
                self,
                value: &str,
            ) -> std::result::Result<Self::Value, E> {
                Ok(ScriptedTurn::Text(value.into()))
            }
            fn visit_string<E: serde::de::Error>(
                self,
                value: String,
            ) -> std::result::Result<Self::Value, E> {
                Ok(ScriptedTurn::Text(value))
            }
            fn visit_seq<A: SeqAccess<'de>>(
                self,
                seq: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                Vec::deserialize(SeqAccessDeserializer::new(seq)).map(ScriptedTurn::Events)
            }
        }
        deserializer.deserialize_any(TurnVisitor)
    }
}

/// One event or failure in an exact streaming response.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ScriptedEvent {
    /// Provider-neutral stream event.
    Event(ModelEvent),
    /// Failure at this position in the stream.
    Error {
        /// Diagnostic returned as a provider storage error.
        error: String,
    },
    /// Remains pending until the consumer drops the stream.
    Pending {},
}

/// One reconciliation of a previously admitted model request.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScriptedReconciliation {
    /// Explicitly permits recovery on a fresh provider that never generated this dispatch.
    #[serde(default)]
    pub orphan: bool,
    /// Exact durable prefix that the executor must supply.
    pub observed: Vec<ModelEvent>,
    /// Remaining events, or `None` when the provider cannot recover the request.
    pub continuation: Option<Vec<ModelEvent>>,
}

/// Observes each dispatch; thread-safe wherever providers must be.
trait DispatchObserver: Fn(ModelDispatch) + acyclic_stream::ProviderPlatform {}

impl<T: Fn(ModelDispatch) + acyclic_stream::ProviderPlatform> DispatchObserver for T {}

#[derive(Default)]
struct State {
    turns: VecDeque<ScriptedTurn>,
    requests: Vec<PreparedModelRequest>,
    dispatches: Vec<ModelDispatch>,
    recoveries: VecDeque<ScriptedReconciliation>,
    reconciliations: Vec<ModelDispatch>,
}

type TokenCounter = dyn Fn(&PreparedModelRequest) -> Result<ModelTokenCount> + Send + Sync;
type ContentReader = (Arc<dyn crate::conversation::ContentResidencyVerifier>, u64);

/// Replays responses once and retains the exact requests delivered by the executor.
/// Exhaustion fails instead of silently repeating the final response.
pub struct ScriptedModelProvider {
    state: Arc<Mutex<State>>,
    capacity: Option<ModelContextCapacity>,
    token_counter: Option<Arc<TokenCounter>>,
    dispatch_observer: Option<Arc<dyn DispatchObserver>>,
    reads_content: bool,
    content: Option<ContentReader>,
}

impl ScriptedModelProvider {
    /// Constructs an independent script; sharing the provider shares its cursor.
    #[must_use]
    pub fn new(turns: impl IntoIterator<Item = ScriptedTurn>) -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                turns: turns.into_iter().collect(),
                ..State::default()
            })),
            capacity: Some(ModelContextCapacity {
                context_tokens: 131_072,
                output_tokens: Some(4_096),
            }),
            token_counter: Some(Arc::new(serialized_tokens)),
            dispatch_observer: None,
            reads_content: false,
            content: None,
        }
    }

    /// Reads every file part's bytes through the reader the harness binds for
    /// the executing task before answering, as a real provider projects them.
    /// An unbound or refused read fails the request; the script cursor is shared
    /// with every bound copy.
    #[must_use]
    pub const fn reading_content(mut self) -> Self {
        self.reads_content = true;
        self
    }

    /// Replaces the model's advertised context and output capacities.
    #[must_use]
    pub fn with_context_capacity(mut self, capacity: ModelContextCapacity) -> Self {
        self.capacity = Some(capacity);
        self
    }

    /// Replaces deterministic request token accounting.
    #[must_use]
    pub fn with_token_counter(
        mut self,
        counter: impl Fn(&PreparedModelRequest) -> Result<ModelTokenCount> + Send + Sync + 'static,
    ) -> Self {
        self.token_counter = Some(Arc::new(counter));
        self
    }

    /// Leaves capacity and token accounting unavailable, as in providers without compaction support.
    #[must_use]
    pub fn without_accounting(mut self) -> Self {
        self.capacity = None;
        self.token_counter = None;
        self
    }

    /// Signals an accepted scripted dispatch after recording it and releasing the state lock.
    /// Runs before returning its stream; invalid requests and exhausted scripts do not signal.
    #[must_use]
    pub fn with_dispatch_observer(
        mut self,
        observer: impl Fn(ModelDispatch) + acyclic_stream::ProviderPlatform + 'static,
    ) -> Self {
        self.dispatch_observer = Some(Arc::new(observer));
        self
    }

    /// Configures ordered recovery replies before sharing this provider.
    pub fn with_reconciliations(
        self,
        replies: impl IntoIterator<Item = ScriptedReconciliation>,
    ) -> Self {
        self.lock().recoveries = replies.into_iter().collect();
        self
    }

    /// Snapshots complete requests, including system messages, tools and context.
    pub fn requests(&self) -> Vec<ModelRequest> {
        self.lock()
            .requests
            .iter()
            .map(|r| r.request().clone())
            .collect()
    }

    /// Snapshots dispatch identities in the same order as the recorded requests.
    pub fn dispatches(&self) -> Vec<ModelDispatch> {
        self.lock().dispatches.clone()
    }

    /// Number of responses not yet requested.
    pub fn remaining(&self) -> usize {
        self.lock().turns.len()
    }

    /// Appends responses after fixture initialization has selected its immutable files.
    pub fn extend(&self, turns: impl IntoIterator<Item = ScriptedTurn>) {
        self.lock().turns.extend(turns);
    }

    /// Snapshots interrupted dispatches submitted for reconciliation.
    pub fn reconciliations(&self) -> Vec<ModelDispatch> {
        self.lock().reconciliations.clone()
    }

    /// Snapshots the scripted recovery replies that have not been consumed.
    pub fn recoveries(&self) -> Vec<ScriptedReconciliation> {
        self.lock().recoveries.iter().cloned().collect()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn next(&self, request: PreparedModelRequest, dispatch: ModelDispatch) -> Result<ScriptedTurn> {
        if dispatch.request_digest != request.request_digest()
            || dispatch.request_digest != *blake3::hash(request.bytes()).as_bytes()
            || request.bytes() != crate::contract::canonical_json_bytes(request.request())?
        {
            return Err(Error::Invalid(
                "scripted model request digest mismatch".into(),
            ));
        }
        let mut state = self.lock();
        state.requests.push(request);
        state.dispatches.push(dispatch);
        state
            .turns
            .pop_front()
            .ok_or_else(|| Error::Invalid("scripted model exhausted".into()))
    }
}

/// Reads every file part a provider projects as bytes through `reader`, as the
/// stock providers do; the first unbound or refused read fails. Reference parts
/// travel as identities and are not read.
pub async fn read_request_files(
    reader: Option<(&dyn crate::conversation::ContentResidencyVerifier, u64)>,
    request: &PreparedModelRequest,
) -> Result<()> {
    for message in &request.request().messages {
        for (file, policy) in message.content.file_parts() {
            if !matches!(
                policy,
                crate::model::FileProjectionPolicy::BoundedFull
                    | crate::model::FileProjectionPolicy::ProjectInstructions(_)
            ) {
                continue;
            }
            let (reader, maximum) = reader.ok_or_else(|| {
                Error::Unauthorized("scripted provider has no content reader".into())
            })?;
            reader.read(file, maximum, None)?.await?;
        }
    }
    Ok(())
}

impl ScriptedModelProvider {
    fn answer(
        &self,
        request: PreparedModelRequest,
        dispatch: ModelDispatch,
    ) -> BoxProviderStream<'_, Result<ModelEvent>> {
        let turn = self.next(request, dispatch);
        if turn.is_ok()
            && let Some(observer) = &self.dispatch_observer
        {
            observer(dispatch);
        }
        match turn {
            Ok(turn) => turn.stream(),
            Err(error) => Box::pin(futures::stream::iter([Err(error)])),
        }
    }
}

impl ModelProvider for ScriptedModelProvider {
    fn bind_content(
        &self,
        reader: Arc<dyn crate::conversation::ContentResidencyVerifier>,
        maximum_bytes: u64,
    ) -> Option<Arc<dyn ModelProvider>> {
        self.reads_content.then(|| {
            Arc::new(Self {
                state: Arc::clone(&self.state),
                capacity: self.capacity,
                token_counter: self.token_counter.clone(),
                dispatch_observer: self.dispatch_observer.clone(),
                reads_content: true,
                content: Some((reader, maximum_bytes)),
            }) as Arc<dyn ModelProvider>
        })
    }

    fn context_capacity(&self, _: &Model) -> Result<ModelContextCapacity> {
        self.capacity.ok_or_else(|| {
            Error::Unsupported("selected model context capacity is unavailable".into())
        })
    }

    fn count_tokens<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<ModelTokenCount>> {
        Box::pin(async move {
            let counter = self.token_counter.as_ref().ok_or_else(|| {
                Error::Unsupported("selected model token counting is unavailable".into())
            })?;
            counter(request)
        })
    }

    fn generate<'a>(
        &'a self,
        request: PreparedModelRequest,
        dispatch: ModelDispatch,
    ) -> BoxProviderStream<'a, Result<ModelEvent>> {
        if !self.reads_content {
            return self.answer(request, dispatch);
        }
        Box::pin(
            futures::stream::once(async move {
                let reader = self
                    .content
                    .as_ref()
                    .map(|(reader, maximum)| (reader.as_ref(), *maximum));
                read_request_files(reader, &request).await?;
                Ok::<_, Error>(self.answer(request, dispatch))
            })
            .try_flatten(),
        )
    }

    fn reconcile<'a>(
        &'a self,
        dispatch: ModelDispatch,
        observed: &'a [ModelEvent],
    ) -> BoxProviderFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        Box::pin(async move {
            let mut state = self.lock();
            state.reconciliations.push(dispatch);
            if state.dispatches.iter().any(|original| {
                original.operation_id == dispatch.operation_id
                    && original.step == dispatch.step
                    && original.request_digest != dispatch.request_digest
            }) {
                return Err(Error::Conflict(
                    "scripted reconciliation dispatch mismatch".into(),
                ));
            }
            if !state.dispatches.contains(&dispatch)
                && !state.recoveries.front().is_some_and(|reply| reply.orphan)
            {
                return Err(Error::Conflict(
                    "scripted reconciliation dispatch was not generated".into(),
                ));
            }
            let reply = state
                .recoveries
                .pop_front()
                .ok_or_else(|| Error::Invalid("scripted reconciliation exhausted".into()))?;
            if reply.observed != observed {
                state.recoveries.push_front(reply);
                return Err(Error::Conflict(
                    "scripted reconciliation prefix mismatch".into(),
                ));
            }
            Ok(reply.continuation)
        })
    }
}

fn serialized_tokens(request: &PreparedModelRequest) -> Result<ModelTokenCount> {
    Ok(request.serialized_token_upper_bound(None)?.0)
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;

impl ScriptedTurn {
    #[doc(hidden)]
    pub fn stream(self) -> BoxProviderStream<'static, Result<ModelEvent>> {
        match self {
            Self::Text(delta) => Box::pin(futures::stream::iter([
                Ok(ModelEvent::Content { delta }),
                Ok(ModelEvent::Completed {
                    metadata: serde_json::Value::Null,
                }),
            ])),
            Self::Events(events) => {
                Box::pin(futures::stream::iter(events).then(|event| async move {
                    match event {
                        ScriptedEvent::Event(event) => Ok(event),
                        ScriptedEvent::Error { error } => Err(Error::Storage(error)),
                        ScriptedEvent::Pending {} => std::future::pending().await,
                    }
                }))
            }
        }
    }
}
