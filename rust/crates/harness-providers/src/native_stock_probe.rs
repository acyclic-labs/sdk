// Test-only callback boundaries measure provider work without including the
// executor's publication/history work between yielded events. All callbacks
// retain the stock request, cancellation and original allocation ledger.
use super::*;
use acyclic_fs::NativeAllocation;
use acyclic_harness_core::{
    NativeFutureBundle,
    context::{ModelContextCapacity, ModelTokenCount},
    model::NativeModelStream,
};
use std::sync::atomic::AtomicU64;

#[derive(Default, Debug)]
pub(super) struct Stages {
    pub(super) capacity: AtomicU64,
    pub(super) count: AtomicU64,
    pub(super) request: AtomicU64,
    pub(super) stream: AtomicU64,
}

pub(super) struct Probe {
    pub(super) provider: crate::ResolvedModel,
    pub(super) stages: Arc<Stages>,
}

impl ModelProvider for Probe {
    fn generate<'a>(
        &'a self,
        _: PreparedModelRequest,
        _: ModelDispatch,
    ) -> acyclic_stream::BoxProviderStream<'a, Result<ModelEvent>> {
        panic!("stock execution must use the native callback")
    }
    fn supports_native_model(&self, work: &NativeMaterializationWork) -> Result<bool> {
        self.provider.supports_native_model(work)
    }
    fn context_capacity_with_native_work(
        &self,
        model: &Model,
        work: &NativeMaterializationWork,
    ) -> Result<ModelContextCapacity> {
        let before = work.receipt().items_examined;
        let result = self.provider.context_capacity_with_native_work(model, work);
        self.stages
            .capacity
            .fetch_add(work.receipt().items_examined - before, Ordering::Relaxed);
        result
    }
    fn count_tokens_with_native_work<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
        work: &'a NativeMaterializationWork,
    ) -> Result<NativeFutureBundle<'a, (ModelTokenCount, NativeAllocation)>> {
        NativeFutureBundle::new(work, || async move {
            let before = work.receipt().items_examined;
            let result = match self.provider.count_tokens_with_native_work(request, work) {
                Ok(future) => future.await,
                Err(error) => Err(error),
            };
            self.stages
                .count
                .fetch_add(work.receipt().items_examined - before, Ordering::Relaxed);
            result
        })
    }
    fn generate_with_native_work<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
        dispatch: ModelDispatch,
        work: &'a NativeMaterializationWork,
    ) -> Result<NativeFutureBundle<'a, NativeModelStream<'a>>> {
        NativeFutureBundle::new(work, || async move {
            let before = work.receipt().items_examined;
            let result = match self
                .provider
                .generate_with_native_work(request, dispatch, work)
            {
                Ok(future) => future.await,
                Err(error) => Err(error),
            };
            self.stages
                .request
                .fetch_add(work.receipt().items_examined - before, Ordering::Relaxed);
            let mut inner = result?;
            NativeModelStream::new(work, || {
                async_stream::try_stream! {
                    loop {
                        let before = work.receipt().items_examined;
                        let next = inner.next().await;
                        self.stages.stream.fetch_add(work.receipt().items_examined - before, Ordering::Relaxed);
                        let Some(event) = next else { break; };
                        yield event?;
                    }
                }
            })
        })
    }
}
