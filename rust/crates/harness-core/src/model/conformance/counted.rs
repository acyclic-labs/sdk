//! Forward the original provider's futures, streams and native custody directly.
use super::super::*;
use acyclic_stream::{
    BoxProviderFuture, BoxProviderStream,
    testing::{Counted, RequestClass},
};
use std::{ops::Deref, sync::Arc};

/// Instrument a concrete model provider before erasing its type.
/// The Arc also lets bind_content preserve this ledger on its returned provider.
pub fn counted<P: ModelProvider + 'static>(provider: P) -> Counted<Arc<P>> {
    Counted::new(Arc::new(provider))
}

impl<P> ModelProvider for Counted<P>
where
    P: Deref + acyclic_stream::ProviderPlatform,
    P::Target: ModelProvider,
{
    fn bind_content(
        &self,
        reader: Arc<dyn crate::conversation::ContentResidencyVerifier>,
        max_file_bytes: u64,
    ) -> Option<Arc<dyn ModelProvider>> {
        self.inner()
            .bind_content(reader, max_file_bytes)
            .map(|bound| {
                Arc::new(Counted::with_ledger(bound, self.ledger())) as Arc<dyn ModelProvider>
            })
    }
    fn context_capacity(&self, model: &Model) -> Result<crate::context::ModelContextCapacity> {
        self.record(RequestClass::Metadata);
        self.inner().context_capacity(model)
    }
    fn count_tokens<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
    ) -> BoxProviderFuture<'a, Result<crate::context::ModelTokenCount>> {
        self.record(RequestClass::Token);
        self.inner().count_tokens(request)
    }
    fn generate<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
        dispatch: ModelDispatch,
    ) -> BoxProviderStream<'a, Result<ModelEvent>> {
        self.record(RequestClass::Data);
        self.inner().generate(request, dispatch)
    }
    fn reconcile<'a>(
        &'a self,
        dispatch: ModelDispatch,
        prefix: &'a [ModelEvent],
    ) -> BoxProviderFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        self.record(RequestClass::Metadata);
        self.inner().reconcile(dispatch, prefix)
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_model(&self, work: &NativeMaterializationWork) -> Result<bool> {
        self.inner().supports_native_model(work)
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn context_capacity_with_native_work(
        &self,
        model: &Model,
        work: &NativeMaterializationWork,
    ) -> Result<crate::context::ModelContextCapacity> {
        self.record(RequestClass::Metadata);
        self.inner().context_capacity_with_native_work(model, work)
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn count_tokens_with_native_work<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (crate::context::ModelTokenCount, NativeAllocation)>>
    {
        self.record(RequestClass::Token);
        self.inner().count_tokens_with_native_work(request, work)
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn generate_with_native_work<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
        dispatch: ModelDispatch,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, NativeModelStream<'a>>> {
        self.record(RequestClass::Data);
        self.inner()
            .generate_with_native_work(request, dispatch, work)
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn reconcile_with_native_work<'a>(
        &'a self,
        dispatch: ModelDispatch,
        prefix: &'a [ModelEvent],
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<Vec<ModelEvent>>, NativeAllocation)>> {
        self.record(RequestClass::Metadata);
        self.inner()
            .reconcile_with_native_work(dispatch, prefix, work)
    }
}
