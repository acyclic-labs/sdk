//! Forward every provider operation explicitly so new contract methods fail to
//! compile here until their request class and forwarding semantics are chosen.
use super::{Counted, RequestClass};
use crate::*;
#[cfg(not(target_arch = "wasm32"))]
use std::{future::Future, sync::Arc};

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl<P: StreamProvider> ExplicitStreamProvider for Counted<P> {
    fn context(&self) -> crate::ProviderContext<'_> {
        self.inner().context()
    }
    fn credential_provider(&self) -> Option<&acyclic_auth::credential::CredentialProvider> {
        self.inner().credential_provider()
    }
    async fn durability_barrier(&self) -> Result<(), StreamError> {
        self.record(RequestClass::Data);
        self.inner().durability_barrier().await
    }

    async fn inspect_idempotency(
        &self,
        idempotency_key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        self.record(RequestClass::Metadata);
        self.inner().inspect_idempotency(idempotency_key).await
    }
    async fn heads(&self, request: crate::HeadsRequest) -> Result<crate::Heads, StreamError> {
        self.record(if request.paths.is_empty() && request.children.is_some() {
            RequestClass::List
        } else {
            RequestClass::Head
        });
        self.inner().heads(request).await
    }

    fn write(
        &self,
        request: crate::WriteRequest,
    ) -> crate::BoxProviderFuture<'_, std::result::Result<crate::WriteOutcome, crate::StreamError>>
    {
        self.record(RequestClass::Data);
        self.inner().write(request)
    }

    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        self.record(RequestClass::Data);
        self.inner().read(request).await
    }
    async fn follow(&self, request: FollowRequest) -> Result<FollowStream, StreamError> {
        self.record(RequestClass::Data);
        self.inner().follow(request).await
    }

    async fn read_commit(&self, commit_id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        self.record(RequestClass::Data);
        self.inner().read_commit(commit_id).await
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl<P: crate::NativeStreamProvider> crate::ExplicitNativeStreamProvider for Counted<P> {
    #[cfg(not(target_arch = "wasm32"))]
    fn native_durability_barrier(
        &self,
        _meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, ()> {
        self.record(RequestClass::Data);
        self.inner().native_durability_barrier(_meter)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn prepare_native_durability_barrier(
        &self,
        _meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> std::io::Result<NativeProviderFuture<'_, ()>> {
        self.record(RequestClass::Data);
        self.inner().prepare_native_durability_barrier(_meter)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn native_inspect_idempotency(
        &self,
        key: IdempotencyKey,
        _meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, Option<IdempotencyObservation>> {
        self.record(RequestClass::Metadata);
        self.inner().native_inspect_idempotency(key, _meter)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn native_tail(
        &self,
        path: StreamPath,
        _meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, u64> {
        self.record(RequestClass::Head);
        self.inner().native_tail(path, _meter)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn native_read(
        &self,
        request: ReadRequest,
        _meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, NativeRecordStream> {
        self.record(RequestClass::Data);
        self.inner().native_read(request, _meter)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn native_write(
        &self,
        request: crate::WriteRequest,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, crate::WriteOutcome> {
        self.record(RequestClass::Data);
        self.inner().native_write(request, meter)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn native_commit_guarded<E, F, Fut>(
        &self,
        request: CommitRequest,
        deadline_unix_millis: Option<u64>,
        _meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
        before: F,
    ) -> NativeProviderFuture<'_, CommitOutcome, E>
    where
        Self: Sized,
        E: From<StreamError> + Send + 'static,
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
    {
        self.record(RequestClass::Data);
        self.inner()
            .native_commit_guarded(request, deadline_unix_millis, _meter, before)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn native_prepare_commit(
        &self,
        request: CommitRequest,
        maximum_record_bytes: usize,
        _meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, PreparedNativeCommit> {
        self.record(RequestClass::Data);
        self.inner()
            .native_prepare_commit(request, maximum_record_bytes, _meter)
    }
}
