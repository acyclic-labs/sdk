use crate::{HttpRequest, ResolvedModel, counting, dispatch, projection};
use acyclic_harness_core::{Result, context::ModelTokenCount, model::PreparedModelRequest};
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
    count: Bytes,
}

impl PreparedInput {
    pub async fn new(provider: Arc<ResolvedModel>, request: PreparedModelRequest) -> Result<Self> {
        let current = provider.current_credentials()?;
        current.validate(&request.request().model)?;
        let spec = counting::specification(&current.config, None)?;
        let names = crate::tool_names::Names::new(request.request());
        let auth = dispatch::authenticate(
            &current,
            #[cfg(not(target_arch = "wasm32"))]
            None,
        )
        .await?;
        let origin = current.reasoning_origin(auth.0.as_deref(), auth.1.as_deref());
        let mut files = crate::files::resolve(&current.files, &request, None).await?;
        let body =
            projection::body_named(&current, request.request(), &names, &origin, &mut files)?;
        let encoded = projection::cache::encode(&body, &current, &origin)?;
        let count = counting::wire::body(body, &current.model.name, spec.adapter)?.into();
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

    pub async fn count(&self) -> Result<ModelTokenCount> {
        let current = self.provider.current_credentials()?;
        let provider = &current;
        let spec = counting::specification(&provider.config, None)?;
        let auth = dispatch::authenticate(
            provider,
            #[cfg(not(target_arch = "wasm32"))]
            None,
        )
        .await?;
        let exchange = HttpRequest {
            method: http::Method::POST,
            url: counting::wire::endpoint(&provider.config, &provider.model.name, spec)?,
            headers: dispatch::headers(provider, auth.0.as_deref(), auth.1.as_deref(), false),
            body: self.count.clone(),
        };
        counting::send_count(
            provider,
            exchange,
            projection::plan(provider, &self.request, 0)?,
            None,
            None,
        )
        .await
        .map(|(count, _)| count)
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
            let current = input.provider.current_credentials()?;
            let provider = &current;
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
