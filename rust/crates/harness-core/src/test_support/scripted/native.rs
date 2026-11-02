//! Native custody adapter over the same scripted provider and response cursor.
use super::*;
use crate::{NativeFutureBundle, model::NativeModelStream};
use acyclic_fs::{NativeAllocation, NativeMaterializationWork, WorkCounters};

struct NativeScripted(Arc<ScriptedModelProvider>);

impl ScriptedModelProvider {
    /// Uses the same script and request observations through the native model
    /// contract. Fixture payloads are already owned; this adapter retains their
    /// SDK inventory, without qualifying any external provider's physical IO.
    #[must_use]
    pub fn native(self: &Arc<Self>) -> Arc<dyn ModelProvider> {
        Arc::new(NativeScripted(Arc::clone(self)))
    }
}

impl ModelProvider for NativeScripted {
    fn context_capacity(&self, model: &Model) -> Result<ModelContextCapacity> {
        self.0.context_capacity(model)
    }

    fn count_tokens<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
    ) -> BoxProviderFuture<'a, Result<ModelTokenCount>> {
        self.0.count_tokens(request)
    }

    fn generate<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
        dispatch: ModelDispatch,
    ) -> BoxProviderStream<'a, Result<ModelEvent>> {
        self.0.generate(request, dispatch)
    }

    fn reconcile<'a>(
        &'a self,
        dispatch: ModelDispatch,
        observed: &'a [ModelEvent],
    ) -> BoxProviderFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        self.0.reconcile(dispatch, observed)
    }

    fn supports_native_model(&self, work: &NativeMaterializationWork) -> Result<bool> {
        work.remaining()?;
        Ok(true)
    }

    fn context_capacity_with_native_work(
        &self,
        model: &Model,
        work: &NativeMaterializationWork,
    ) -> Result<ModelContextCapacity> {
        work.remaining()?;
        self.0.context_capacity(model)
    }

    fn count_tokens_with_native_work<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
        work: &'a NativeMaterializationWork,
    ) -> Result<NativeFutureBundle<'a, (ModelTokenCount, NativeAllocation)>> {
        NativeFutureBundle::new(work, || async move {
            let count = self.0.count_tokens(request).await?;
            let custody = work.claim(
                (count.message_tokens.capacity() * std::mem::size_of::<u32>()) as u64,
                1,
                WorkCounters::UNCHARGED,
            )?;
            Ok((count, custody))
        })
    }

    fn generate_with_native_work<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
        dispatch: ModelDispatch,
        work: &'a NativeMaterializationWork,
    ) -> Result<NativeFutureBundle<'a, NativeModelStream<'a>>> {
        NativeFutureBundle::new(work, || async move {
            NativeModelStream::new(work, || {
                let work = work.clone();
                self.0.generate(request, dispatch).map(move |event| {
                    let event = event?;
                    let custody =
                        work.claim(event.logical_owned_bytes()?, 1, WorkCounters::UNCHARGED)?;
                    Ok((event, custody))
                })
            })
        })
    }

    fn reconcile_with_native_work<'a>(
        &'a self,
        dispatch: ModelDispatch,
        observed: &'a [ModelEvent],
        work: &'a NativeMaterializationWork,
    ) -> Result<NativeFutureBundle<'a, (Option<Vec<ModelEvent>>, NativeAllocation)>> {
        NativeFutureBundle::new(work, || async move {
            let events = self.0.reconcile(dispatch, observed).await?;
            let bytes = events.as_ref().map_or(Ok(0), |events| {
                events.iter().try_fold(
                    (events.capacity() * std::mem::size_of::<ModelEvent>()) as u64,
                    |bytes, event| Ok::<_, Error>(bytes + event.logical_owned_bytes()?),
                )
            })?;
            let custody = work.claim(bytes, 1, WorkCounters::UNCHARGED)?;
            Ok((events, custody))
        })
    }
}
