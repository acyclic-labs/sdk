//! The storage an agent handler runs over, injected by its host: one stream
//! provider (agent actors, inboxes, conversations and the volumes' authority)
//! and one object store (the volumes' immutable objects). The handler and
//! the guest artifact own no storage. The browser host injects `IndexedDB`,
//! tests (and any host without a browser) inject memory, and further hosts add
//! their StreamProvider and immutable object store through the same boundary.

use super::{
    Authorities, BrowserHandler, Error, HISTORY_TURNS, TURN_BACKSTOP_MS, WasmAgentAuthority,
    WasmAgentRegistry, WasmBrowserAgentOptions, agent_error, from_js, js_error, stream_error,
};
use crate::{
    agent::{AgentHandler, ExecutorDriver, actor_store},
    filesystem::{
        FilesystemHost, FilesystemSpawnVolumes, FilesystemTurnHost, Grants, StockExecutors, Tasks,
        TurnAdmission, TurnContext, TurnLimits,
    },
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
use acyclic_objects::{Blobs, MemoryObjects};
use acyclic_stream::{
    BrowserStream, BrowserStreamLimits, MemoryLimits, MemoryStream, OwnedBytes, StreamError,
    StreamProvider,
};
use std::sync::Arc;
use wasm_bindgen::prelude::*;

/// The object store injected by one host.
pub(super) enum Objects {
    /// The browser profile's IndexedDB objects.
    IndexedDb(IndexedDbObjectStore),
    /// Any Rust objects provider.
    Provider(LogicalObjectStore<dyn Blobs>),
    /// Any JavaScript immutable-object provider.
    Injected(acyclic_fs::InjectedObjectStore),
}

macro_rules! each {
    ($objects:expr, $store:ident => $body:expr) => {
        match $objects {
            Objects::IndexedDb($store) => $body,
            Objects::Provider($store) => $body,
            Objects::Injected($store) => $body,
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
    pub(super) stream: Arc<dyn StreamProvider>,
    pub(super) filesystem: Arc<FilesystemHost<Authorities, Objects>>,
    pub(super) placement: Fs<Authorities, Objects>,
}

impl Providers {
    fn new(
        stream: Arc<dyn StreamProvider>,
        authority: Authorities,
        objects: Objects,
        durable: bool,
        options: &WasmBrowserAgentOptions,
    ) -> Result<Self, JsValue> {
        let placement = Fs::new(authority, objects, EmbeddedCapabilities { durable });
        let filesystem = FilesystemHost::new(placement.clone(), &options.filesystem_provider, None)
            .map_err(js_error)?;
        Ok(Self {
            stream,
            filesystem,
            placement,
        })
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
        let stream: Arc<dyn StreamProvider> = Arc::new(stream);
        Self::new(
            stream.clone(),
            StreamAuthorityStore::new(stream),
            Objects::IndexedDb(objects),
            true,
            options,
        )
    }

    /// One in-memory stream (agents and volume authority) and object store:
    /// nothing outlives the providers, and no browser storage is touched.
    /// The database names in `options` are unused.
    pub(super) fn memory(options: &WasmBrowserAgentOptions) -> Result<Self, JsValue> {
        Self::over(
            Arc::new(MemoryStream::new(Self::memory_limits(options))),
            Arc::new(MemoryObjects::memory()),
            false,
            options,
        )
    }

    /// A host's own stream and objects providers: the stream carries the
    /// agents and their volumes' authority, `objects` the volumes' blobs,
    /// and `durable` says whether both outlive the host.
    pub(super) fn over(
        stream: Arc<dyn StreamProvider>,
        objects: Arc<dyn Blobs>,
        durable: bool,
        options: &WasmBrowserAgentOptions,
    ) -> Result<Self, JsValue> {
        let objects = LogicalObjectStore::new(objects)
            .with_object_limit(options.maximum_object_bytes)
            .map_err(|error| js_error(Error::Storage(error.to_string())))?;
        Self::new(
            Arc::clone(&stream),
            StreamAuthorityStore::new(stream),
            Objects::Provider(objects),
            durable,
            options,
        )
    }

    /// The one agent handler package over these providers: the handler the
    /// in-process host runs and the guest artifact exports.
    /// Its turns and spawns act under `admission`: the admission its host
    /// was admitted with, never chosen here.
    pub(super) fn handler(
        &self,
        options: &WasmBrowserAgentOptions,
        owner: &WasmAgentAuthority,
        registry: &WasmAgentRegistry,
        admission: &TurnAdmission,
    ) -> Result<BrowserHandler, JsValue> {
        // The options a handler runs under are its own, not its providers'.
        let limits = options.preset.execution_limits(options.limits);
        limits.validate(&mut None).map_err(js_error)?;
        let turn_limits = TurnLimits {
            maximum_bytes: limits.file_bytes,
            max_steps: u32::try_from(limits.model_steps)
                .map_err(|_| js_error(Error::Invalid("model step limit exceeds u32".into())))?,
            history_turns: HISTORY_TURNS,
        };
        let turns = Arc::new(
            FilesystemTurnHost::new(
                Arc::clone(&self.stream),
                Arc::clone(&self.filesystem),
                owner.issuer.clone(),
                admission.clone(),
                Arc::new(Self::executors(options, registry)?),
                turn_limits,
                options.preset.approval_policy.clone(),
            )
            .with_prompt(options.preset.prompt.as_deref()),
        );
        AgentHandler::new(
            actor_store(&self.stream).map_err(agent_error)?,
            ExecutorDriver::new(turns),
            ActorLimits::new(
                options.handler_timeout_ms.unwrap_or(TURN_BACKSTOP_MS),
                1 << 20,
                1 << 16,
            )
            .map_err(|error| js_error(Error::Invalid(error.to_string())))?,
        )
        .map(|handler| handler.with_spawn_volumes(self.spawn_volumes()))
        .map_err(|error| js_error(Error::Invalid(error.to_string())))
    }

    pub(super) fn executors(
        options: &WasmBrowserAgentOptions,
        registry: &WasmAgentRegistry,
    ) -> Result<StockExecutors, JsValue> {
        let limits = options.preset.execution_limits(options.limits);
        limits.validate(&mut None).map_err(js_error)?;
        let (model, provider, compaction) = registry
            .model
            .clone()
            .ok_or_else(|| js_error(Error::Invalid("no model is configured".into())))?;
        let mut tools = options
            .preset
            .tools(
                |name| {
                    registry
                        .workspace
                        .as_ref()
                        .is_some_and(|workspace| workspace.tools.contains(&name))
                },
                Vec::new(),
            )
            .map_err(js_error)?;
        tools.extend(registry.tools.clone()).map_err(js_error)?;
        let grants = options.preset.tool_grants(&tools).map_err(js_error)?;
        let context: Arc<dyn TurnContext> = match &registry.workspace {
            Some(workspace) => Arc::new(Tasks(
                crate::runtime::Bindings {
                    workspace_tools: Some(Arc::clone(&workspace.executor)),
                    scope: crate::runtime::RuntimeScope::new(
                        crate::Capabilities::new(
                            grants
                                .into_iter()
                                .chain([crate::contract::capability::MODEL_GENERATE.to_owned()]),
                        ),
                        limits,
                    )
                    .map_err(js_error)?,
                    ..Default::default()
                }
                .build()
                .map_err(js_error)?,
            )),
            None => Arc::new(Grants(grants)),
        };
        Ok(options
            .preset
            .executors((model, provider), tools, (limits, compaction), context))
    }

    pub(super) fn spawn_volumes(&self) -> Arc<FilesystemSpawnVolumes<dyn StreamProvider, Objects>> {
        Arc::new(FilesystemSpawnVolumes::new(self.filesystem.clone()))
    }
}

/// Agent storage a host injects into [`super::guest::AgentGuest`] and
/// [`super::WasmAgents::open_hosting`]; one handle shared by a guest and its
/// host is one store.
#[wasm_bindgen]
pub struct WasmAgentProviders {
    pub(super) providers: Providers,
}

impl WasmAgentProviders {
    /// A host's own storage, for a host that builds its artifact over this
    /// crate: any stream provider (the agents and their volumes' authority)
    /// and any objects provider, which holds the volumes' blobs;
    /// `durable` says whether both outlive the host. The handle serves
    /// [`AgentGuest::open`](super::guest::AgentGuest::open) and
    /// [`WasmAgents::open_hosting`](super::WasmAgents::open_hosting) like
    /// the built-in constructors.
    ///
    /// # Errors
    /// When `options` or the object limit are invalid.
    pub fn over(
        stream: Arc<dyn StreamProvider>,
        objects: Arc<dyn Blobs>,
        durable: bool,
        options: JsValue,
    ) -> Result<Self, JsValue> {
        let options: WasmBrowserAgentOptions = from_js(options)?;
        Ok(Self {
            providers: Providers::over(stream, objects, durable, &options)?,
        })
    }
}

#[wasm_bindgen]
impl WasmAgentProviders {
    /// Open storage injection: any Stream handle plus immutable length/read/put
    /// promises. `durable` requires both providers to retain accepted writes.
    pub fn injected(
        stream: &acyclic_stream::WasmStream,
        objects: JsValue,
        durable: bool,
        #[wasm_bindgen(unchecked_param_type = "WasmBrowserAgentOptions")] options: JsValue,
    ) -> Result<WasmAgentProviders, JsValue> {
        let options: WasmBrowserAgentOptions = from_js(options)?;
        let stream = stream.provider();
        let authority = StreamAuthorityStore::new(stream.clone());
        let objects = acyclic_fs::InjectedObjectStore::new(objects, options.maximum_object_bytes)
            .map_err(|error| js_error(Error::Storage(error.to_string())))?;
        Ok(Self {
            providers: Providers::new(
                stream,
                authority,
                Objects::Injected(objects),
                durable,
                &options,
            )?,
        })
    }

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
