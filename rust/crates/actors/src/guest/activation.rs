//! Host dispatch through one initialization and checkpoint-admission path.
use super::{
    Activation, ActorDefinition, Checkpoint, GuestContext, GuestError, GuestEvent,
    GuestEventResult, GuestState, GuestStreamingRequest, GuestStreamingResponse, negotiate_abi,
};
use std::sync::Arc;
use tokio::sync::RwLock;

/// A namespace class activation. Events interleave; checkpoints wait for all handlers.
pub struct GuestActivation<State, Bindings> {
    state: Activation<GuestState<State>>,
    definition: ActorDefinition<State, Bindings>,
    context: Arc<GuestContext<Bindings>>,
    deliveries: RwLock<()>,
}
impl<State: Send + 'static, Bindings: Send + Sync + 'static> GuestActivation<State, Bindings> {
    /// Defer restore/constructor work until the first delivery, exactly once.
    pub fn new(
        definition: ActorDefinition<State, Bindings>,
        checkpoint: Option<Checkpoint>,
        context: GuestContext<Bindings>,
    ) -> Result<Self, GuestError> {
        negotiate_abi(definition.abi_version)?;
        let address = context
            .actor
            .reference()
            .map_err(|e| GuestError::Handler(e.to_string()))?;
        if let Some(caller) = &context.admitted_identity {
            let audience = caller
                .reference()
                .map_err(|e| GuestError::Handler(e.to_string()))?;
            if address.as_ref() != Some(&audience) {
                return Err(GuestError::Handler(
                    "admitted actor audience differs from activation".into(),
                ));
            }
        }
        let context = Arc::new(context);
        let initialize_context = context.clone();
        let restore = definition.restore;
        let state = Activation::new(async move {
            Ok(GuestState::new(
                restore(checkpoint.as_ref(), &initialize_context).await?,
            ))
        });
        Ok(Self {
            state,
            definition,
            context,
            deliveries: RwLock::new(()),
        })
    }
    /// Every stream, invocation, alarm and socket event shares the constructor gate.
    pub async fn event(
        &self,
        event: GuestEvent,
        context: &GuestContext<Bindings>,
    ) -> Result<Option<GuestEventResult>, GuestError> {
        self.admit_delivery(context)?;
        let _delivery = self.deliveries.read().await;
        self.state
            .deliver(async |state| {
                let result = (self.definition.event)(&state, event, context).await?;
                if let Some(value) = &result {
                    value.validate()?;
                }
                Ok(result)
            })
            .await
    }
    /// HTTP delivery shares initialization with the event union.
    pub async fn fetch(
        &self,
        request: GuestStreamingRequest,
        context: &GuestContext<Bindings>,
    ) -> Result<GuestStreamingResponse, GuestError> {
        self.admit_delivery(context)?;
        let _delivery = self.deliveries.read().await;
        self.state
            .deliver(async |state| (self.definition.fetch)(&state, request, context).await)
            .await
    }
    // Delivery metadata is invocation-scoped; never reuse the constructor's caller or retry key.
    fn admit_delivery(&self, context: &GuestContext<Bindings>) -> Result<(), GuestError> {
        let address = context
            .actor
            .reference()
            .map_err(|e| GuestError::Handler(e.to_string()))?;
        let expected = self
            .context
            .actor
            .reference()
            .map_err(|e| GuestError::Handler(e.to_string()))?;
        if address != expected {
            return Err(GuestError::Handler(
                "delivery actor differs from activation".into(),
            ));
        }
        if let Some(caller) = &context.admitted_identity {
            if Some(
                caller
                    .reference()
                    .map_err(|e| GuestError::Handler(e.to_string()))?,
            ) != expected
            {
                return Err(GuestError::Handler(
                    "delivery audience differs from activation".into(),
                ));
            }
        }
        Ok(())
    }
    /// Gate newly delivered events while application initialization runs.
    pub async fn block_concurrency_while<R>(
        &self,
        callback: impl AsyncFnOnce(Arc<GuestState<State>>) -> Result<R, GuestError>,
    ) -> Result<R, GuestError> {
        self.state.block_concurrency_while(callback).await
    }
    /// Capture only after active handlers finish; new deliveries cannot enter meanwhile.
    pub async fn checkpoint(&self) -> Result<Checkpoint, GuestError> {
        let _quiesced = self.deliveries.write().await;
        self.state
            .block_concurrency_while(async |state| {
                (self.definition.checkpoint)(&state, &self.context).await
            })
            .await
    }
}
