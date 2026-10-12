use super::*;
use acyclic_stream::{BoxProviderFuture as BoxFuture, BoxProviderStream as BoxStream};

/// Ordered streaming model event.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum ModelEvent {
    /// User-visible fragment.
    Content {
        /// Incremental content.
        delta: String,
    },
    /// Provider-exposed reasoning fragment.
    Reasoning {
        /// Incremental reasoning.
        delta: String,
    },
    /// Complete registered tool invocation.
    ToolCall {
        /// Provider-owned call identity.
        #[serde(alias = "callId")]
        call_id: String,
        /// Registered tool name.
        name: String,
        /// Schema-defined arguments.
        #[cfg_attr(feature = "wasm", tsify(type = "WasmModelJsonValue"))]
        arguments: Value,
    },
    /// Completed model step.
    Completed {
        /// Usage and finish metadata.
        #[cfg_attr(feature = "wasm", tsify(type = "WasmModelJsonValue"))]
        metadata: Value,
    },
}
/// Executor identity supplied separately from canonical model bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
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

/// Concrete stream box admitted before invoking its builder.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub struct NativeModelStream<'a> { stream: BoxStream<'a, Result<(ModelEvent, NativeAllocation)>>, custody: NativeAllocation }
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl<'a> NativeModelStream<'a> {
    /// Funds the concrete stream frame before construction.
    pub fn new<S, B>(work: &NativeMaterializationWork, build: B) -> Result<Self>
    where S: futures::Stream<Item = Result<(ModelEvent, NativeAllocation)>> + Send + 'a, B: FnOnce() -> S {
        work.remaining()?;
        let bytes = std::mem::size_of::<S>() as u64;
        let custody = work.claim(bytes, u64::from(bytes != 0), acyclic_fs::WorkCounters::UNCHARGED)?;
        Ok(Self { stream: Box::pin(build()), custody })
    }
    /// Original ledger funding this stream.
    #[must_use]
    pub fn work(&self) -> &NativeMaterializationWork { self.custody.work() }
}
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl futures::Stream for NativeModelStream<'_> {
    type Item = Result<(ModelEvent, NativeAllocation)>;
    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Err(cause) = this.custody.work().remaining() { return std::task::Poll::Ready(Some(Err(cause.into()))); }
        this.stream.as_mut().poll_next(cx)
    }
}
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl ModelEvent {
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        let payload = match self {
            Self::Content { delta } | Self::Reasoning { delta } => delta.capacity() as u64,
            Self::ToolCall { call_id, name, arguments } => add(add(call_id.capacity() as u64, name.capacity() as u64)?, crate::contract::json_value_logical_owned_bytes(arguments)?)?,
            Self::Completed { metadata } => crate::contract::json_value_logical_owned_bytes(metadata)?,
        };
        add(std::mem::size_of::<Self>() as u64, payload)
    }
    /// Decodes a paid SDK event from borrowed provider bytes.
    pub fn decode_with_native_work(bytes: &[u8], work: &NativeMaterializationWork) -> Result<(Self, NativeAllocation)> {
        let io = PreparationIo::from_native_work(work)?;
        let event = crate::filesystem::retain_decoded(io.decode(std::marker::PhantomData::<Self>, bytes, Storage)?);
        let retained = event.logical_owned_bytes()?;
        Ok((event, io.into_native_custody(retained)?))
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn unsupported() -> Error { Error::NativeBoundary { code: Unsupported, reason: "model provider has no original native-ledger producer" } }

/// Replaceable streaming model provider.
pub trait ModelProvider: acyclic_stream::ProviderPlatform {
    /// Capacities of the exact immutable model selection.
    fn context_capacity(&self, _: &Model) -> Result<crate::context::ModelContextCapacity> {
        Err(Error::Unsupported("selected model context capacity is unavailable".into()))
    }
    /// Deterministic additive token upper bounds.
    fn count_tokens(&self, _: &PreparedModelRequest) -> Result<crate::context::ModelTokenCount> {
        Err(Error::Unsupported("selected model token counting is unavailable".into()))
    }
    /// Starts one request and emits ordered events.
    fn generate<'a>(&'a self, request: PreparedModelRequest, dispatch: ModelDispatch) -> BoxStream<'a, Result<ModelEvent>>;
    /// Reconciles an interrupted run without starting another request.
    fn reconcile<'a>(&'a self, _: ModelDispatch, _: &'a [ModelEvent]) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> { Box::pin(async { Ok(None) }) }
    /// Pure native capability query.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_model(&self, work: &NativeMaterializationWork) -> Result<bool> { work.remaining()?; Ok(false) }
    /// Native capacities without ordinary fallback.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn context_capacity_with_native_work(&self, _: &Model, work: &NativeMaterializationWork) -> Result<crate::context::ModelContextCapacity> {
        work.remaining()?; Err(unsupported())
    }
    /// Paid token DTO for the admitted native request.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn count_tokens_with_native_work(&self, _: &PreparedModelRequest, work: &NativeMaterializationWork) -> Result<(crate::context::ModelTokenCount, NativeAllocation)> {
        work.remaining()?; Err(unsupported())
    }
    /// Starts an original-ledger stream of paid SDK events.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn generate_with_native_work<'a>(&'a self, _: &'a PreparedModelRequest, _: ModelDispatch, work: &'a NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, NativeModelStream<'a>>> { work.remaining()?; Err(unsupported()) }
    /// Reconciles a borrowed durable prefix on the original ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn reconcile_with_native_work<'a>(&'a self, _: ModelDispatch, _: &'a [ModelEvent], work: &'a NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, (Option<Vec<ModelEvent>>, NativeAllocation)>> { work.remaining()?; Err(unsupported()) }
}
