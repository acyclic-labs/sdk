use super::*;
use acyclic_stream::{BoxProviderFuture as BoxFuture, BoxProviderStream as BoxStream};

/// Ordered streaming model event.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum ModelEvent {
    /// Abandon the incomplete output of this dispatch and begin its next transport attempt.
    /// Consumers discard all earlier events in this step; request and tool identities stay fixed.
    Restarted,
    /// Original provider admission or transport recovery observation. This does
    /// not change model output, call identity or completion, and contains no
    /// remote error text.
    Wait(ModelWait),
    /// Verified token admission, terminating a token-count stream. Generation
    /// streams terminate with `Completed` instead.
    Counted {
        /// Count bound to the exact canonical request.
        count: crate::context::ModelTokenCount,
    },
    /// User-visible fragment.
    Content {
        /// Incremental content.
        delta: String,
    },
    /// Authoritative text of the final completed assistant message in a step.
    /// Deltas remain independently available for live transcript rendering.
    MessageCompleted {
        /// Exact message text, including an empty message.
        text: String,
    },
    /// One completed visible text part in transcript order, after the
    /// Content deltas that streamed it: interim text in a tool step included.
    TextPart {
        /// The part's exact text.
        text: String,
        /// The provider's exact original part, when it has one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "wasm", tsify(optional))]
        native: Option<NativePart>,
    },
    /// Provider-exposed reasoning fragment.
    Reasoning {
        /// Incremental reasoning.
        delta: String,
    },
    /// Completed canonical reasoning, retained in transcript order.
    ReasoningBlock {
        /// Exact canonical continuity and readable content.
        block: ReasoningBlock,
    },
    /// Complete registered tool invocation.
    ToolCall(ToolCall),
    /// Completed model step, which need not end the actor's turn.
    Completed {
        /// Usage and finish metadata. Providers normalize output-token
        /// exhaustion to `finish_reason: "length"`; the turn must continue.
        #[cfg_attr(feature = "wasm", tsify(type = "WasmModelJsonValue"))]
        #[serde(deserialize_with = "crate::deserialize_json_value")]
        #[heap_layout(as = serde_json::Value)]
        metadata: Value,
    },
}

/// Compact recovery facts carried by the original ordered model stream.
/// An unfinished start remains visible when cancellation or timer failure wins.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum ModelWait {
    /// The shared endpoint admission began waiting for this cause.
    AdmissionStarted {
        /// The gate's original reason for withholding admission.
        cause: ModelAdmissionCause,
    },
    /// The gate changed why the same original caller remains queued.
    AdmissionChanged {
        /// The gate's current reason for withholding admission.
        cause: ModelAdmissionCause,
        /// Elapsed gate wait at this transition, on the transport clock.
        #[cfg_attr(feature = "wasm", tsify(type = "{ secs: bigint; nanos: number }"))]
        waited_for: std::time::Duration,
    },
    /// The original caller received admission. Cancellation leaves its start
    /// visible without inventing a successful admission.
    AdmissionFinished {
        /// Total gate wait on the original transport's monotonic clock.
        #[cfg_attr(feature = "wasm", tsify(type = "{ secs: bigint; nanos: number }"))]
        waited_for: std::time::Duration,
        /// Admission time in Unix milliseconds.
        #[cfg_attr(feature = "wasm", tsify(type = "bigint"))]
        admitted_at: u64,
    },
    /// Redelivery is delayed by the original recovery timer.
    Started {
        /// Typed transport cause; absent for an unclassified unavailable error.
        cause: Option<crate::ModelTransportFailure>,
        /// Exact selected delay, without narrowing the timer's capacity.
        #[cfg_attr(feature = "wasm", tsify(type = "{ secs: bigint; nanos: number }"))]
        delay: std::time::Duration,
        /// Whether the provider supplied the delay rather than local backoff.
        retry_after: bool,
    },
    /// The original timer completed successfully, before the next send.
    Finished {
        /// Measured on the transport's monotonic clock around the timer wait.
        #[cfg_attr(feature = "wasm", tsify(type = "{ secs: bigint; nanos: number }"))]
        elapsed: std::time::Duration,
    },
}

/// Why the one shared endpoint admission is withholding a request.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum ModelAdmissionCause {
    /// The provider's cooldown, on the original transport's monotonic clock.
    Cooldown {
        /// The instant that can reopen the endpoint.
        #[cfg_attr(feature = "wasm", tsify(type = "{ secs: bigint; nanos: number }"))]
        until: std::time::Duration,
    },
    /// Waiting in arrival order for the endpoint's in-flight credit.
    Credit,
}

/// Executor identity supplied separately from canonical model bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ModelDispatch {
    /// Admitted operation identity.
    #[cfg_attr(feature = "wasm", tsify(type = "string"))]
    pub operation_id: OperationId,
    /// Zero-based executor step.
    pub step: u32,
    /// Canonical request digest.
    #[cfg_attr(feature = "wasm", tsify(type = "readonly number[]"))]
    pub request_digest: [u8; 32],
}

/// Provider-declared delivery path, separate from historical event metadata.
/// This identifies execution, not billing or successful provider completion.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ModelDelivery {
    /// The producer did not supply an observation. Never infer one from usage,
    /// elapsed time, request equality or the absence of divergence.
    #[default]
    Unknown,
    /// Generation selected the live provider path.
    Live,
    /// Events came from a recording. Token and price metadata describe its
    /// source answer and cannot certify spending by this delivery.
    Recorded {
        /// Original admitted dispatch, when retained by the source producer.
        source: Option<ModelDispatch>,
    },
}

/// One generation's events and immutable provider-declared delivery path.
/// Adds no allocation to the original stream box and retains no event payload.
pub struct ModelStream<'a> {
    stream: BoxStream<'a, Result<ModelEvent>>,
    delivery: ModelDelivery,
}

impl<'a> ModelStream<'a> {
    /// Carries a local or host-provided count through the same ordered event
    /// contract as remote counters, without inventing wait observations.
    pub fn from_count(
        count: impl std::future::Future<Output = Result<crate::context::ModelTokenCount>>
        + acyclic_stream::ProviderTask
        + 'a,
    ) -> Self {
        Self::new(Box::pin(futures::stream::once(async move {
            count.await.map(|count| ModelEvent::Counted { count })
        })))
    }

    /// Carries a stream whose producer has not declared its delivery path.
    pub fn new(stream: BoxStream<'a, Result<ModelEvent>>) -> Self {
        Self {
            stream,
            delivery: ModelDelivery::Unknown,
        }
    }

    /// Attaches the original producer's declaration before returning the stream.
    #[must_use]
    pub fn with_delivery(mut self, delivery: ModelDelivery) -> Self {
        self.delivery = delivery;
        self
    }

    /// Original observation; independent of whether the stream has been polled.
    #[must_use]
    pub fn delivery(&self) -> ModelDelivery {
        self.delivery
    }
}

impl futures::Stream for ModelStream<'_> {
    type Item = Result<ModelEvent>;

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.get_mut().stream.as_mut().poll_next(cx)
    }
}

/// Concrete stream box admitted before invoking its builder.
/// The first error or EOF terminates the stream; later polls return EOF.
/// Cancellation wakes a pending stream independently of its source.
#[cfg(not(target_arch = "wasm32"))]
pub struct NativeModelStream<'a> {
    stream: BoxStream<'a, Result<(ModelEvent, NativeAllocation)>>,
    cancelled: Option<acyclic_fs::Cancelled<'static>>,
    custody: NativeAllocation,
    terminated: bool,
    delivery: ModelDelivery,
}
#[cfg(not(target_arch = "wasm32"))]
impl<'a> NativeModelStream<'a> {
    /// Funds the concrete frame and original cancellation subscription before
    /// invoking the stream builder.
    pub fn new<S, B>(work: &NativeMaterializationWork, build: B) -> Result<Self>
    where
        S: futures::Stream<Item = Result<(ModelEvent, NativeAllocation)>> + Send + 'a,
        B: FnOnce() -> S,
    {
        work.remaining()?;
        let bytes = std::mem::size_of::<S>() as u64;
        let custody = work.claim(
            bytes,
            u64::from(bytes != 0),
            acyclic_fs::WorkCounters::UNCHARGED,
        )?;
        let cancelled = work.cancellation_wait()?;
        Ok(Self {
            stream: Box::pin(build()),
            cancelled: Some(cancelled),
            custody,
            terminated: false,
            delivery: ModelDelivery::Unknown,
        })
    }
    /// Original ledger funding this stream.
    #[must_use]
    pub fn work(&self) -> &NativeMaterializationWork {
        self.custody.work()
    }
    /// Attaches the original producer's declaration without changing event custody.
    #[must_use]
    pub fn with_delivery(mut self, delivery: ModelDelivery) -> Self {
        self.delivery = delivery;
        self
    }
    /// Original observation; unknown producers remain unknown.
    #[must_use]
    pub fn delivery(&self) -> ModelDelivery {
        self.delivery
    }
}
#[cfg(not(target_arch = "wasm32"))]
impl futures::Stream for NativeModelStream<'_> {
    type Item = Result<(ModelEvent, NativeAllocation)>;
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.terminated {
            return std::task::Poll::Ready(None);
        }
        if let Err(cause) = this.custody.work().remaining() {
            this.terminated = true;
            drop(this.cancelled.take());
            return std::task::Poll::Ready(Some(Err(cause.into())));
        }
        if let Some(cancelled) = &mut this.cancelled {
            if std::future::Future::poll(std::pin::Pin::new(cancelled), cx).is_ready() {
                this.terminated = true;
                drop(this.cancelled.take());
                return std::task::Poll::Ready(Some(Err(
                    acyclic_fs::NativeTreeMaterializationError::Cancelled.into(),
                )));
            }
        }
        let next = this.stream.as_mut().poll_next(cx);
        this.terminated = matches!(&next, std::task::Poll::Ready(None | Some(Err(_))));
        if this.terminated {
            drop(this.cancelled.take());
        }
        next
    }
}
#[cfg(not(target_arch = "wasm32"))]
impl futures::stream::FusedStream for NativeModelStream<'_> {
    fn is_terminated(&self) -> bool {
        self.terminated
    }
}
#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "provider_tests.rs"]
mod tests;
impl ModelEvent {
    pub(crate) fn retained_owned_bytes(&self) -> Result<u64> {
        let payload = match self {
            Self::Restarted | Self::Wait(_) => 0,
            Self::Counted { count } => {
                (count.message_tokens.capacity() * std::mem::size_of::<u32>()) as u64
            }
            Self::Content { delta } | Self::Reasoning { delta } => delta.capacity() as u64,
            Self::MessageCompleted { text } => text.capacity() as u64,
            Self::TextPart { text, native } => add(
                text.capacity() as u64,
                super::tool_call::native_bytes(native.as_ref())?,
            )?,
            Self::ReasoningBlock { block } => block.payload_bytes()?,
            Self::ToolCall(call) => call.retained_payload_bytes()?,
            Self::Completed { metadata } => {
                crate::executor::json::json_value_retained_owned_bytes(metadata)?
            }
        };
        add(std::mem::size_of::<Self>() as u64, payload)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl ModelEvent {
    /// Resident capacity transferred with a native event, including unused
    /// object-node slots retained until the event's JSON values are destroyed.
    pub fn logical_owned_bytes(&self) -> Result<u64> {
        self.retained_owned_bytes()
    }
    /// Decodes a paid SDK event from borrowed provider bytes.
    pub fn decode_with_native_work(
        bytes: &[u8],
        work: &NativeMaterializationWork,
    ) -> Result<(Self, NativeAllocation)> {
        let io = PreparationIo::from_native_work(work)?;
        let event = crate::filesystem::retain_decoded(io.decode(
            std::marker::PhantomData::<Self>,
            bytes,
            Storage,
        )?)?;
        let retained = event.logical_owned_bytes()?;
        Ok((event, io.into_native_custody(retained)?))
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn unsupported() -> Error {
    Error::NativeBoundary {
        code: Unsupported,
        reason: "model provider has no original native-ledger producer",
    }
}

/// Replaceable streaming model provider.
pub trait ModelProvider: acyclic_stream::ProviderPlatform {
    /// Binds transport affinity to one conversation, independently of prompt
    /// content, model steps and turns. Providers without affinity need no copy.
    fn bind_conversation(
        &self,
        _: crate::OperationId,
    ) -> Option<std::sync::Arc<dyn ModelProvider>> {
        None
    }
    /// Derives an immutable epoch selection from this original provider. The
    /// caller retains the returned model and provider together; the original
    /// selection remains available to other sessions. Admission performs no I/O.
    fn admit_profile(
        &self,
        _: &Model,
        _: &crate::profile::AgentProfile<'_>,
    ) -> Result<(Model, std::sync::Arc<dyn ModelProvider>)> {
        Err(Error::Unsupported(
            "selected provider cannot admit a profile; recovery: select a provider with epoch profile admission".into(),
        ))
    }
    /// Binds the composition's authenticated reader and optional file projection credit.
    /// `None` leaves projection size unbounded; native work admission still applies.
    /// Providers that consume canonical file references directly need no rebinding.
    fn bind_content(
        &self,
        _: std::sync::Arc<dyn crate::conversation::ContentResidencyVerifier>,
        _: Option<u64>,
    ) -> Option<std::sync::Arc<dyn ModelProvider>> {
        None
    }
    /// Sealed profile admitted with the exact immutable model selection.
    fn model_profile(&self, _: &Model) -> Result<Option<ModelProfileBinding>> {
        Ok(None)
    }
    /// The tool family the exact model selection was trained on. Stock turns
    /// offer the tools it maps under that family's declarations.
    fn native_tool_profile(&self, _: &Model) -> Result<Option<NativeToolProfile>> {
        Ok(None)
    }
    /// Native declarations admitted with this immutable selection. Providers
    /// with replaced declarations return their original binding owner here;
    /// family defaults use the shared native constructor. The stock executor
    /// reserves and retains the returned owner once, before preparing a turn.
    fn native_tools(
        &self,
        model: &Model,
    ) -> Result<Option<std::sync::Arc<crate::tools::native::NativeTools>>> {
        self.native_tool_profile(model)?
            .map(crate::tools::native::NativeTools::new)
            .transpose()
            .map(|tools| tools.map(std::sync::Arc::new))
    }
    /// Capacities of the exact immutable model selection.
    fn context_capacity(&self, _: &Model) -> Result<crate::context::ModelContextCapacity> {
        Err(Error::Unsupported(
            "selected model context capacity is unavailable".into(),
        ))
    }
    /// Admission for one request and its per-message compaction planning
    /// weights. `input_tokens` is a locally proven upper bound when one is
    /// within `ceiling`, so no count request precedes generation; otherwise it
    /// is the provider's verified count when a count endpoint exists. Without
    /// one, a usage-anchored bound still reports pressure above the ceiling.
    /// The stream emits original wait observations as they occur, followed by
    /// one [`ModelEvent::Counted`]. Dropping it cancels the original request.
    fn count_tokens<'a>(&'a self, _: &'a PreparedModelRequest, _ceiling: u64) -> ModelStream<'a> {
        ModelStream::from_count(async { Err(Error::ModelTokenCountUnavailable) })
    }
    /// Borrows one admitted request and emits ordered events. The stream cannot
    /// outlive that admission; after dropping it the caller can continue the
    /// original request without copying its history or encoded prefix.
    fn generate<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
        dispatch: ModelDispatch,
    ) -> ModelStream<'a>;
    /// Reconciles an interrupted run without starting another request.
    fn reconcile<'a>(
        &'a self,
        _: ModelDispatch,
        _: &'a [ModelEvent],
    ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        Box::pin(async { Ok(None) })
    }
    /// A recorded settlement standing in for tool call `call_id` of
    /// `operation`, taken once. Only a replaying provider
    /// ([`super::RecordedModel`]) has one, for calls made by an answer it
    /// served from its recording.
    /// The original native work admits the owned copy before consuming the
    /// settlement and returns its custody. Refusal leaves it available.
    fn recorded_tool_result(
        &self,
        _operation: OperationId,
        _call_id: &str,
        _work: acyclic_fs::NativeWork<'_>,
    ) -> Result<Option<(super::RecordedToolOutcome, acyclic_fs::NativeCustody)>> {
        Ok(None)
    }
    /// Pure native capability query.
    #[cfg(not(target_arch = "wasm32"))]
    fn supports_native_model(&self, work: &NativeMaterializationWork) -> Result<bool> {
        work.remaining()?;
        Ok(false)
    }
    /// Native capacities without ordinary fallback.
    #[cfg(not(target_arch = "wasm32"))]
    fn context_capacity_with_native_work(
        &self,
        _: &Model,
        work: &NativeMaterializationWork,
    ) -> Result<crate::context::ModelContextCapacity> {
        work.remaining()?;
        Err(unsupported())
    }
    /// Original-ledger count stream: admission waits precede the paid count.
    #[cfg(not(target_arch = "wasm32"))]
    fn count_tokens_with_native_work<'a>(
        &'a self,
        _: &'a PreparedModelRequest,
        _ceiling: u64,
        work: &'a NativeMaterializationWork,
    ) -> Result<acyclic_fs::NativeFutureBundle<'a, NativeModelStream<'a>, crate::Error>> {
        work.remaining()?;
        Err(unsupported())
    }
    /// Starts an original-ledger stream of paid SDK events.
    #[cfg(not(target_arch = "wasm32"))]
    fn generate_with_native_work<'a>(
        &'a self,
        _: &'a PreparedModelRequest,
        _: ModelDispatch,
        work: &'a NativeMaterializationWork,
    ) -> Result<acyclic_fs::NativeFutureBundle<'a, NativeModelStream<'a>, crate::Error>> {
        work.remaining()?;
        Err(unsupported())
    }
    /// Reconciles a borrowed durable prefix on the original ledger.
    #[cfg(not(target_arch = "wasm32"))]
    fn reconcile_with_native_work<'a>(
        &'a self,
        _: ModelDispatch,
        _: &'a [ModelEvent],
        work: &'a NativeMaterializationWork,
    ) -> Result<
        acyclic_fs::NativeFutureBundle<
            'a,
            (Option<Vec<ModelEvent>>, NativeAllocation),
            crate::Error,
        >,
    > {
        work.remaining()?;
        Err(unsupported())
    }
}
