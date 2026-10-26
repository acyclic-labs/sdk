//! The storage an agent handler runs over, injected by its host: one stream
//! provider (agent actors, inboxes, conversations and the volumes' authority)
//! and one object store (the volumes' immutable objects). The handler and
//! the guest artifact own no storage. The browser host injects `IndexedDB`,
//! tests (and any host without a browser) inject memory, and further hosts add
//! their store here as one more [`Streams`] and [`Objects`] variant.

use super::{
    Authorities, BrowserHandler, Error, Executors, HISTORY_TURNS, TURN_BACKSTOP_MS,
    WasmAgentRegistry, WasmBrowserAgentOptions, WasmReducer, agent_error, from_js, js_error,
    stream_error,
};
use crate::{
    agent::{AgentHandler, ExecutorDriver, actor_store},
    filesystem::{FilesystemHost, FilesystemTurnHost, TurnAdmission, TurnLimits},
};
use acyclic_actors::domain::ActorLimits;
use acyclic_fs::{
    AsyncObjectStore, AuthorityWork, EmbeddedCapabilities, Fs, LogicalObjectStore, ObjectId,
    ObjectRead, ObjectReadRequest, ObjectStoreError, PublicationHold, PublicationScope,
    StreamAuthorityStore,
    async_storage::{DecodedCacheAdmission, DecodedCacheKey, DecodedCacheValue},
    browser::IndexedDbObjectStore,
    storage::ObjectWrite,
};
use acyclic_objects::v1::{MemoryObjects, MemoryOptions};
use acyclic_stream::{
    AppendOutcome, AppendRequest, BrowserStream, BrowserStreamLimits, ChildrenPage,
    ChildrenPageRequest, CommitId, CommitOutcome, CommitRequest, CommittedEnvelope, DurableReceipt,
    FollowFilter, FollowStream, ForkReceipt, ForkRequest, IdempotencyKey, IdempotencyObservation,
    MemoryLimits, MemoryStream, OperationClaims, OwnedBytes, ReadRequest, RecordStream,
    StreamBounds, StreamError, StreamPath, StreamProvider, StreamWatermarks,
};
use std::sync::Arc;
use wasm_bindgen::prelude::*;

/// The stream provider of one host's agents.
pub(super) enum Streams {
    /// The browser profile's `IndexedDB` stream.
    IndexedDb(BrowserStream),
    /// Process memory, for hosts without durable browser storage.
    Memory(MemoryStream),
}

macro_rules! stream {
    ($streams:expr, $stream:ident => $body:expr) => {
        match $streams {
            Streams::IndexedDb($stream) => $body,
            Streams::Memory($stream) => $body,
        }
    };
}

#[async_trait::async_trait(?Send)]
impl StreamProvider for Streams {
    fn operation_claims(&self) -> &OperationClaims {
        stream!(self, inner => inner.operation_claims())
    }

    fn has_exclusive_process_owner(&self) -> bool {
        stream!(self, inner => inner.has_exclusive_process_owner())
    }

    fn supports_deferred_durability(&self) -> bool {
        stream!(self, inner => inner.supports_deferred_durability())
    }

    async fn durability_barrier(&self) -> Result<(), StreamError> {
        stream!(self, inner => inner.durability_barrier().await)
    }

    async fn inspect_idempotency(
        &self,
        idempotency_key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        stream!(self, inner => inner.inspect_idempotency(idempotency_key).await)
    }

    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        stream!(self, inner => inner.tail(path).await)
    }

    async fn bounds(&self, path: StreamPath) -> Result<StreamBounds, StreamError> {
        stream!(self, inner => inner.bounds(path).await)
    }

    async fn watermarks(&self, path: StreamPath) -> Result<StreamWatermarks, StreamError> {
        stream!(self, inner => inner.watermarks(path).await)
    }

    async fn durable_receipt(
        &self,
        path: StreamPath,
        through: u64,
    ) -> Result<Option<DurableReceipt>, StreamError> {
        stream!(self, inner => inner.durable_receipt(path, through).await)
    }

    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
        stream!(self, inner => inner.append(request).await)
    }

    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
        stream!(self, inner => inner.fork(request).await)
    }

    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        stream!(self, inner => inner.read(request).await)
    }

    async fn follow(
        &self,
        path: StreamPath,
        from: u64,
        filter: FollowFilter,
    ) -> Result<RecordStream, StreamError> {
        stream!(self, inner => inner.follow(path, from, filter).await)
    }

    async fn follow_checked(
        &self,
        path: StreamPath,
        from: u64,
        preceding: CommitId,
        filter: FollowFilter,
    ) -> Result<RecordStream, StreamError> {
        stream!(self, inner => inner.follow_checked(path, from, preceding, filter).await)
    }

    async fn follow_updates(
        &self,
        path: StreamPath,
        from: u64,
        preceding: Option<CommitId>,
        filter: FollowFilter,
    ) -> Result<FollowStream, StreamError> {
        stream!(self, inner => inner.follow_updates(path, from, preceding, filter).await)
    }

    async fn children_page(
        &self,
        request: ChildrenPageRequest,
    ) -> Result<ChildrenPage, StreamError> {
        stream!(self, inner => inner.children_page(request).await)
    }

    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        stream!(self, inner => inner.commit(request).await)
    }

    async fn commit_before(
        &self,
        request: CommitRequest,
        deadline_unix_millis: u64,
    ) -> Result<CommitOutcome, StreamError> {
        stream!(self, inner => inner.commit_before(request, deadline_unix_millis).await)
    }

    async fn read_commit(&self, commit_id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        stream!(self, inner => inner.read_commit(commit_id).await)
    }
}

/// The object store of one host's volumes.
pub(super) enum Objects {
    /// The browser profile's `IndexedDB` objects.
    IndexedDb(IndexedDbObjectStore),
    /// An objects provider (process memory today), for hosts without
    /// durable browser storage.
    Memory(LogicalObjectStore<MemoryObjects>),
}

macro_rules! each {
    ($objects:expr, $store:ident => $body:expr) => {
        match $objects {
            Objects::IndexedDb($store) => $body,
            Objects::Memory($store) => $body,
        }
    };
}

impl AsyncObjectStore for Objects {
    fn supports_decoded_cache(&self) -> bool {
        each!(self, store => store.supports_decoded_cache())
    }

    fn decoded_cache_get(
        &self,
        key: DecodedCacheKey,
    ) -> Result<Option<DecodedCacheValue>, ObjectStoreError> {
        each!(self, store => store.decoded_cache_get(key))
    }

    fn decoded_cache_admit(
        &self,
        key: DecodedCacheKey,
        value: DecodedCacheValue,
    ) -> Result<DecodedCacheAdmission, ObjectStoreError> {
        each!(self, store => store.decoded_cache_admit(key, value))
    }

    async fn put(
        &self,
        object_id: ObjectId,
        bytes: OwnedBytes,
        context: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        each!(self, store => store.put(object_id, bytes, context).await)
    }

    async fn put_hashed(
        &self,
        object: acyclic_fs::HashedObject,
        context: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        each!(self, store => store.put_hashed(object, context).await)
    }

    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        context: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        each!(self, store => store.put_many(writes, context).await)
    }

    async fn put_many_hashed(
        &self,
        objects: &[acyclic_fs::HashedObject],
        context: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        each!(self, store => store.put_many_hashed(objects, context).await)
    }

    async fn flush_before_publish(
        &self,
        scope: PublicationScope<'_>,
        context: &mut AuthorityWork<'_>,
    ) -> Result<PublicationHold, ObjectStoreError> {
        each!(self, store => store.flush_before_publish(scope, context).await)
    }

    async fn historical_generation_evidence(
        &self,
        authority_id: acyclic_fs::AuthorityId,
        record: &acyclic_fs::DurableCommit,
        maximum_bytes: u64,
        maximum_objects: u64,
        context: &mut AuthorityWork<'_>,
    ) -> Result<Option<acyclic_fs::kernel::HistoricalGenerationEvidence>, ObjectStoreError> {
        each!(self, store => store
            .historical_generation_evidence(
                authority_id,
                record,
                maximum_bytes,
                maximum_objects,
                context,
            )
            .await)
    }

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        context: &mut AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        each!(self, store => store.read(object_id, maximum_bytes, context).await)
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        context: &mut AuthorityWork<'_>,
    ) -> Result<(Vec<ObjectRead>, acyclic_fs::AuthorityAllocation), ObjectStoreError> {
        each!(self, store => store.read_many(requests, context).await)
    }

    async fn contains(
        &self,
        object_id: ObjectId,
        context: &mut AuthorityWork<'_>,
    ) -> Result<bool, ObjectStoreError> {
        each!(self, store => store.contains(object_id, context).await)
    }

    fn collection(&self) -> Option<&Arc<acyclic_fs::Collection>> {
        each!(self, store => store.collection())
    }

    fn collection_sweeps(&self) -> u64 {
        each!(self, store => store.collection_sweeps())
    }
}

/// One host's agent storage: what every agent handler, turn and control
/// reads and writes.
#[derive(Clone)]
pub(super) struct Providers {
    pub(super) stream: Arc<Streams>,
    pub(super) filesystem: Arc<FilesystemHost<Authorities, Objects>>,
}

impl Providers {
    fn new(
        stream: Arc<Streams>,
        authority: Authorities,
        objects: Objects,
        durable: bool,
        options: &WasmBrowserAgentOptions,
    ) -> Result<Self, JsValue> {
        options.limits.validate(&mut None).map_err(js_error)?;
        let filesystem = FilesystemHost::new(
            Fs::new(authority, objects, EmbeddedCapabilities { durable }),
            &options.filesystem_provider,
            None,
        )
        .map_err(js_error)?;
        Ok(Self { stream, filesystem })
    }

    fn memory_limits(options: &WasmBrowserAgentOptions) -> MemoryLimits {
        let limits = &options.stream_limits;
        MemoryLimits {
            paths: limits.paths as usize,
            path_bytes: limits.path_bytes as usize,
            records: limits.records as usize,
            payload_bytes: limits.payload_bytes as usize,
            commits: limits.commits as usize,
            idempotency_results: limits.idempotency_results as usize,
        }
    }

    /// The browser profile's `IndexedDB` databases named by `options`.
    pub(super) async fn indexed_db(options: &WasmBrowserAgentOptions) -> Result<Self, JsValue> {
        let storage = |error: StreamError| stream_error(error);
        let authority = acyclic_fs::browser::open_authority(
            &options.filesystem_database,
            BrowserStreamLimits::default(),
        )
        .await
        .map_err(storage)?;
        let objects =
            IndexedDbObjectStore::open(&options.filesystem_database, options.maximum_object_bytes)
                .await
                .map_err(|error| js_error(Error::Storage(error.to_string())))?;
        let stream = BrowserStream::open(
            &options.stream_database,
            BrowserStreamLimits {
                commands: options.stream_limits.commands,
                journal_bytes: options.stream_limits.journal_bytes,
                memory: Self::memory_limits(options),
            },
        )
        .await
        .map_err(storage)?;
        Self::new(
            Arc::new(Streams::IndexedDb(stream)),
            authority,
            Objects::IndexedDb(objects),
            true,
            options,
        )
    }

    /// One in-memory stream (agents and volume authority) and object store:
    /// nothing outlives the providers, and no browser storage is touched.
    /// The database names in `options` are unused.
    pub(super) fn memory(options: &WasmBrowserAgentOptions) -> Result<Self, JsValue> {
        let stream = Arc::new(Streams::Memory(MemoryStream::new(Self::memory_limits(
            options,
        ))));
        let storage = |error: String| js_error(Error::Storage(error));
        let (objects, bucket) = MemoryObjects::with_bucket("agents", MemoryOptions::default())
            .map_err(|error| storage(error.to_string()))?;
        let objects = LogicalObjectStore::new(Arc::new(objects), bucket)
            .with_object_limit(options.maximum_object_bytes)
            .map_err(|error| storage(error.to_string()))?;
        let authority: Arc<dyn StreamProvider> = stream.clone();
        Self::new(
            stream,
            StreamAuthorityStore::new(authority),
            Objects::Memory(objects),
            false,
            options,
        )
    }

    /// The one agent handler package over these providers: the handler the
    /// in-process host runs and the guest artifact exports.
    pub(super) fn handler(
        &self,
        options: &WasmBrowserAgentOptions,
        owner: &WasmReducer,
        registry: &WasmAgentRegistry,
    ) -> Result<BrowserHandler, JsValue> {
        let (model, provider, compaction) = registry
            .model
            .clone()
            .ok_or_else(|| js_error(Error::Invalid("no model is configured".into())))?;
        let turn_limits = TurnLimits {
            maximum_bytes: options.limits.file_bytes,
            max_steps: u32::try_from(options.limits.model_steps)
                .map_err(|_| js_error(Error::Invalid("model step limit exceeds u32".into())))?,
            history_turns: HISTORY_TURNS,
        };
        let turns = Arc::new(FilesystemTurnHost::new(
            Arc::clone(&self.stream),
            Arc::clone(&self.filesystem),
            owner.issuer.clone(),
            TurnAdmission::LocalRoot,
            Arc::new(Executors {
                model,
                provider,
                compaction,
                tools: registry.tools.clone(),
                grants: registry.grants.clone(),
                limits: options.limits,
            }),
            turn_limits,
        ));
        AgentHandler::new(
            actor_store(&self.stream).map_err(agent_error)?,
            ExecutorDriver::new(turns),
            ActorLimits::new(TURN_BACKSTOP_MS, 1 << 20, 1 << 16)
                .map_err(|error| js_error(Error::Invalid(error.to_string())))?,
        )
        .map_err(|error| js_error(Error::Invalid(error.to_string())))
    }
}

/// Agent storage a host injects into [`super::guest::AgentGuest`] and
/// [`super::WasmAgents::open_hosting`]; one handle shared by a guest and its
/// host is one store.
#[wasm_bindgen]
pub struct WasmAgentProviders {
    pub(super) providers: Providers,
}

#[wasm_bindgen]
impl WasmAgentProviders {
    /// The browser profile's `IndexedDB` databases (the browser host's default).
    #[wasm_bindgen(js_name = indexedDb)]
    pub async fn indexed_db(
        #[wasm_bindgen(unchecked_param_type = "WasmBrowserAgentOptions")] options: JsValue,
    ) -> Result<WasmAgentProviders, JsValue> {
        let options: WasmBrowserAgentOptions = from_js(options)?;
        Ok(Self {
            providers: Providers::indexed_db(&options).await?,
        })
    }

    /// In-memory storage, for hosts and tests without browser storage.
    pub fn memory(
        #[wasm_bindgen(unchecked_param_type = "WasmBrowserAgentOptions")] options: JsValue,
    ) -> Result<WasmAgentProviders, JsValue> {
        let options: WasmBrowserAgentOptions = from_js(options)?;
        Ok(Self {
            providers: Providers::memory(&options)?,
        })
    }
}
