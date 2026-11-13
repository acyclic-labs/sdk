use crate::{HttpRequest, ResolvedModel, counting, dispatch, projection};
use acyclic_harness_core::{
    Error, Result,
    model::{ModelEvent, PreparedModelRequest},
};
use bytes::Bytes;
use futures::StreamExt as _;
use std::sync::Arc;

/// Request-local immutable HTTP inputs. Canonical admission, file verification
/// and provider projection happen once, bound to their original selection.
pub(crate) struct PreparedInput {
    provider: Arc<ResolvedModel>,
    pub request: PreparedModelRequest,
    pub generation: Bytes,
    cache: Option<acyclic_harness_core::executor::ModelCacheObservation>,
    count: Option<Bytes>,
}

impl PreparedInput {
    pub async fn new(provider: Arc<ResolvedModel>, request: PreparedModelRequest) -> Result<Self> {
        // The encoded bodies and their credentials are one immutable selection.
        // Protected transports still resolve current authority on every send.
        let provider = Arc::new(provider.current_credentials()?);
        provider.validate(&request.request().model)?;
        let counter = match counting::specification(&provider.config, None) {
            Ok(spec) => Some(spec),
            Err(Error::ModelTokenCountUnavailable) => None,
            Err(error) => return Err(error),
        };
        let names = crate::tool_names::Names::new(request.request());
        let auth = dispatch::authenticate(
            &provider,
            #[cfg(not(target_arch = "wasm32"))]
            None,
        )
        .await?;
        let origin = provider.reasoning_origin(auth.0.as_deref(), auth.1.as_deref());
        let mut files = crate::files::resolve(&provider.files, &request, None).await?;
        let body =
            projection::body_named(&provider, request.request(), &names, &origin, &mut files)?;
        let encoded = projection::cache::encode(&body, &provider, &origin)?;
        let count = counter
            .map(|spec| counting::wire::body(body, &provider.model.name, spec).map(Bytes::from))
            .transpose()?;
        drop(auth);
        drop(names);
        Ok(Self {
            provider,
            request,
            generation: encoded.bytes.into(),
            cache: encoded.cache,
            count,
        })
    }

    /// Token admission and generation use the same observable event contract.
    pub fn count(
        prepared: impl AsRef<Self> + acyclic_stream::ProviderTask,
    ) -> impl futures::Stream<Item = Result<ModelEvent>> + acyclic_stream::ProviderTask {
        async_stream::try_stream! {
            let input = prepared.as_ref();
            let provider = &input.provider;
            let spec = counting::specification(&provider.config, None)?;
            let auth = dispatch::authenticate(provider,
                #[cfg(not(target_arch = "wasm32"))] None).await?;
            let exchange = HttpRequest {
                method: http::Method::POST,
                url: counting::wire::endpoint(&provider.config, &provider.model.name, spec)?,
                headers: dispatch::headers(provider, auth.0.as_deref(), auth.1.as_deref(), false),
                body: input.count.clone().ok_or_else(|| crate::invalid("prepared input has no token counter body"))?,
            };
            let events = counting::send_count(provider, exchange,
                projection::plan(provider, &input.request, 0)?, None, None);
            let mut events = core::pin::pin!(events);
            while let Some(event) = events.next().await {
                match event? {
                    crate::admission::Event::Wait(wait) => yield ModelEvent::Wait(wait),
                    crate::admission::Event::Trace(record) => yield ModelEvent::Trace { record },
                    crate::admission::Event::Ready((count, _custody)) => yield ModelEvent::Counted { count },
                }
            }
        }
    }

    /// Moves the caller's existing Arc or Rc into the stream without another
    /// erased frame or a separately selectable provider.
    pub fn generate(
        prepared: impl AsRef<Self> + acyclic_stream::ProviderTask,
        dispatch: acyclic_harness_core::model::ModelDispatch,
    ) -> impl futures::Stream<Item = Result<acyclic_harness_core::model::ModelEvent>>
    + acyclic_stream::ProviderTask {
        async_stream::try_stream! {
            let input = prepared.as_ref();
            let provider = &input.provider;
            if input.request.request_digest() != dispatch.request_digest {
                Err(crate::invalid("model dispatch digest differs from prepared request"))?;
            }
            let auth = dispatch::authenticate(
                provider,
                #[cfg(not(target_arch = "wasm32"))]
                None,
            ).await?;
            let names = crate::tool_names::Names::new(input.request.request());
            let events = dispatch::generate_encoded(provider, &input.request,
                dispatch, input.generation.clone(), input.cache, auth, names);
            futures::pin_mut!(events);
            while let Some(event) = events.next().await { yield event?; }
        }
    }
}
