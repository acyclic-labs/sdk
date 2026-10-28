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
    async fn durability_barrier(&self) -> Result<(), StreamError> {
        self.record(RequestClass::Data);
        self.inner().durability_barrier().await
    }
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
    async fn inspect_idempotency(
        &self,
        idempotency_key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        self.record(RequestClass::Metadata);
        self.inner().inspect_idempotency(idempotency_key).await
    }
    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        self.record(RequestClass::Head);
        self.inner().tail(path).await
    }
    async fn heads(&self, request: crate::HeadsRequest) -> Result<crate::Heads, StreamError> {
        self.record(RequestClass::Head);
        self.inner().heads(request).await
    }

    async fn watermarks(&self, path: StreamPath) -> Result<StreamWatermarks, StreamError> {
        self.record(RequestClass::Head);
        self.inner().watermarks(path).await
    }
    async fn durable_receipt(
        &self,
        path: StreamPath,
        through: u64,
    ) -> Result<Option<DurableReceipt>, StreamError> {
        self.record(RequestClass::Head);
        self.inner().durable_receipt(path, through).await
    }
    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
        self.record(RequestClass::Data);
        self.inner().append(request).await
    }
    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
        self.record(RequestClass::Data);
        self.inner().fork(request).await
    }
    async fn wait_applied(
        &self,
        at_least: &crate::RevisionCut,
    ) -> Result<crate::Revision, StreamError> {
        self.record(RequestClass::Head);
        self.inner().wait_applied(at_least).await
    }
    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        self.record(RequestClass::Data);
        self.inner().read(request).await
    }
    async fn follow(&self, request: FollowRequest) -> Result<FollowStream, StreamError> {
        self.record(RequestClass::Data);
        self.inner().follow(request).await
    }
    async fn children_page(
        &self,
        request: ChildrenPageRequest,
    ) -> Result<ChildrenPage, StreamError> {
        self.record(RequestClass::List);
        self.inner().children_page(request).await
    }
    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        self.record(RequestClass::Data);
        self.inner().commit(request).await
    }
    async fn read_commit(&self, commit_id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        self.record(RequestClass::Data);
        self.inner().read_commit(commit_id).await
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
    fn native_append(
        &self,
        request: AppendRequest,
        _meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, AppendOutcome> {
        self.record(RequestClass::Data);
        self.inner().native_append(request, _meter)
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
    fn native_commit(
        &self,
        request: CommitRequest,
        deadline: Option<u64>,
        _meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, CommitOutcome> {
        self.record(RequestClass::Data);
        self.inner().native_commit(request, deadline, _meter)
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
