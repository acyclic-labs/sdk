//! Browser persistence and WebAssembly bindings for the canonical Rust engine.

#[cfg(any(test, target_arch = "wasm32"))]
mod authority_codec;

#[cfg(target_arch = "wasm32")]
mod indexed_db;

#[cfg(target_arch = "wasm32")]
mod opfs;

#[cfg(target_arch = "wasm32")]
pub use indexed_db::{IndexedDbAuthorityStore, IndexedDbObjectStore, IndexedDbOpenError};

#[cfg(target_arch = "wasm32")]
pub use opfs::{OpfsAcceleratedObjectStore, OpfsOpenError};

#[cfg(target_arch = "wasm32")]
mod bindings {
    use super::{IndexedDbAuthorityStore, IndexedDbObjectStore, OpfsAcceleratedObjectStore};
    use acyclic_fs::kernel::{
        AttributeClass, AttributeName, DecodeLimits, ExtentKind, ExtentSeekTarget, FileKind,
        FileMetadata, FilePayload, FileRecord, LogicalName, NameEncoding, NamespacePath,
        RebaseDecision, TransferCursor, TreeEntry, decode_file_metadata, encode_file_metadata,
    };
    use acyclic_fs::model::{
        AccessMode, CaseSensitivity, CheckoutMode, ConcurrencyMode, ConsistencyMode,
        FilesystemProfile, GenerationSelector, Lifecycle, MutationMode, UnicodePolicy,
        VolumeConfig, VolumeConfigError, VolumeLimits,
    };
    use acyclic_fs::path::PortablePath;
    use acyclic_fs::{
        ApplyOptions, AuthoredMutation, ByteRange, CachedObjectStore, CancellationToken, ChangeSet,
        Checkout, CheckoutCommitOutcome, Digest, EmbeddedCapabilities, FileCloneRequest, FileId,
        ForkOptions, Fs, Generation, GenerationExportManifest, IdempotencyKey, JoinHistory,
        JoinOutcome, JoinPlan, LiveMutationOutcome, MemoryAuthorityStore, MemoryObjectStore,
        MergeConflict, MergePreparation, NamedAttributeWriteMode, ObjectCacheOptions, ObjectId,
        ObjectKind, ObjectReadRequest, ObjectResidency, OperationId, PromotionAdmission,
        PromotionDestination, PromotionRejection, PromotionSpeculatorOptions, ResidencyAdmission,
        ResidencyHint, ResidencyReason, ResidencyRejection, ResidencySpeculatorOptions,
        SpeculationController, SpeculationOptions, StorageLocationId, StorageTier, Transaction,
        TransactionCommit, TransactionConflict, TransactionConflictRegion,
        TransactionDependencyUse, TransactionRebase, TransactionSparseSeek, Volume, VolumeId,
        WorkBudget, Workspace, WorkspaceDelete, WorkspaceDirectoryPage, WorkspaceExtentKind,
        WorkspaceExtentPlan, WorkspaceMetadata, WorkspaceRebase, WorkspaceStat,
        decode_generation_export_manifest, encode_generation_export_manifest,
    };
    use serde::{Deserialize, Serialize};
    use wasm_bindgen::prelude::*;

    type IndexedDbObjects = CachedObjectStore<IndexedDbObjectStore>;
    type OpfsObjects = CachedObjectStore<OpfsAcceleratedObjectStore>;
    type MemoryObjects = CachedObjectStore<MemoryObjectStore>;

    #[derive(Clone)]
    enum BrowserEngine {
        IndexedDb(Fs<IndexedDbAuthorityStore, IndexedDbObjects>),
        IndexedDbOpfs(Fs<IndexedDbAuthorityStore, OpfsObjects>),
        Memory(Fs<MemoryAuthorityStore, MemoryObjects>),
    }

    enum BrowserVolumeEngine {
        IndexedDb(Volume<IndexedDbAuthorityStore, IndexedDbObjects>),
        IndexedDbOpfs(Volume<IndexedDbAuthorityStore, OpfsObjects>),
        Memory(Volume<MemoryAuthorityStore, MemoryObjects>),
    }

    enum BrowserCheckoutEngine {
        IndexedDb(Checkout<IndexedDbAuthorityStore, IndexedDbObjects>),
        IndexedDbOpfs(Checkout<IndexedDbAuthorityStore, OpfsObjects>),
        Memory(Checkout<MemoryAuthorityStore, MemoryObjects>),
    }

    enum BrowserWorkspaceEngine {
        IndexedDb(Workspace<IndexedDbAuthorityStore, IndexedDbObjects>),
        IndexedDbOpfs(Workspace<IndexedDbAuthorityStore, OpfsObjects>),
        Memory(Workspace<MemoryAuthorityStore, MemoryObjects>),
    }

    enum BrowserGenerationEngine {
        IndexedDb(Generation<IndexedDbAuthorityStore, IndexedDbObjects>),
        IndexedDbOpfs(Generation<IndexedDbAuthorityStore, OpfsObjects>),
        Memory(Generation<MemoryAuthorityStore, MemoryObjects>),
    }

    enum BrowserChangeSetEngine {
        IndexedDb(ChangeSet<IndexedDbAuthorityStore, IndexedDbObjects>),
        IndexedDbOpfs(ChangeSet<IndexedDbAuthorityStore, OpfsObjects>),
        Memory(ChangeSet<MemoryAuthorityStore, MemoryObjects>),
    }

    enum BrowserJoinPlanEngine {
        IndexedDb(JoinPlan<IndexedDbAuthorityStore, IndexedDbObjects>),
        IndexedDbOpfs(JoinPlan<IndexedDbAuthorityStore, OpfsObjects>),
        Memory(JoinPlan<MemoryAuthorityStore, MemoryObjects>),
    }

    enum BrowserTransactionEngine {
        IndexedDb(Transaction<IndexedDbAuthorityStore, IndexedDbObjects>),
        IndexedDbOpfs(Transaction<IndexedDbAuthorityStore, OpfsObjects>),
        Memory(Transaction<MemoryAuthorityStore, MemoryObjects>),
    }

    macro_rules! with_checkout_mut {
        ($engine:expr, $checkout:ident, $body:expr) => {
            match $engine {
                BrowserCheckoutEngine::IndexedDb($checkout) => $body,
                BrowserCheckoutEngine::IndexedDbOpfs($checkout) => $body,
                BrowserCheckoutEngine::Memory($checkout) => $body,
            }
        };
    }

    /// Browser-safe handle backed by the canonical Rust engine.
    #[wasm_bindgen]
    pub struct BrowserFs {
        engine: Option<BrowserEngine>,
        capabilities: Capabilities,
    }

    /// Browser owner of one volume generation's residency and promotion engines.
    #[wasm_bindgen]
    pub struct BrowserSpeculation {
        engine: BrowserEngine,
        controller: std::cell::RefCell<SpeculationController>,
        cancellation: CancellationToken,
    }

    /// One independently configured browser volume.
    #[wasm_bindgen]
    pub struct BrowserVolume {
        engine: BrowserVolumeEngine,
        limits: VolumeLimits,
        acquisition_work: acyclic_fs::WorkCounters,
    }

    /// One immutable-generation checkout with optional private COW mutations.
    #[wasm_bindgen]
    pub struct BrowserCheckout {
        engine: BrowserCheckoutEngine,
        limits: VolumeLimits,
        acquisition_work: acyclic_fs::WorkCounters,
    }

    /// One named customer workspace.
    #[wasm_bindgen]
    pub struct BrowserWorkspace {
        engine: BrowserWorkspaceEngine,
    }

    /// One exact immutable browser workspace generation.
    #[wasm_bindgen]
    pub struct BrowserGeneration {
        engine: BrowserGenerationEngine,
    }

    /// One immutable semantic delta between exact generations.
    #[wasm_bindgen]
    pub struct BrowserChangeSet {
        engine: BrowserChangeSetEngine,
    }

    /// One immutable, side-effect-free workspace join plan.
    #[wasm_bindgen]
    pub struct BrowserJoinPlan {
        engine: BrowserJoinPlanEngine,
    }

    /// One sparse atomic browser workspace transaction.
    #[wasm_bindgen]
    pub struct BrowserTransaction {
        engine: BrowserTransactionEngine,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct BrowserWorkspaceCommit {
        status: &'static str,
        generation_id: Option<serde_bytes::ByteBuf>,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct BrowserTransactionRebase {
        status: &'static str,
        generation_id: Option<serde_bytes::ByteBuf>,
        conflicts: Vec<BrowserTransactionConflict>,
        truncated: bool,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct BrowserTransactionConflict {
        region: &'static str,
        file_id: Option<serde_bytes::ByteBuf>,
        directory_id: Option<serde_bytes::ByteBuf>,
        offset: Option<u64>,
        length: Option<u64>,
        sparse_target: Option<&'static str>,
        name: Option<BrowserWorkspaceName>,
        maximum_entries: Option<u32>,
        usage: &'static str,
        expected: Option<serde_bytes::ByteBuf>,
        actual: Option<serde_bytes::ByteBuf>,
    }

    #[derive(Deserialize, Serialize)]
    #[serde(rename_all = "camelCase")]
    struct BrowserWorkspaceName {
        encoding: String,
        #[serde(with = "serde_bytes")]
        bytes: Vec<u8>,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct BrowserWorkspaceMetadata {
        posix_mode: Option<u32>,
        posix_uid: Option<u32>,
        posix_gid: Option<u32>,
        posix_flags: Option<u64>,
        windows_attributes: Option<u32>,
        created_ns: Option<i64>,
        modified_ns: Option<i64>,
        accessed_ns: Option<i64>,
        changed_ns: Option<i64>,
        has_named_attributes: bool,
        has_acl: bool,
        has_security_descriptor: bool,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct BrowserWorkspaceStat {
        #[serde(with = "serde_bytes")]
        file_id: Vec<u8>,
        kind: &'static str,
        link_count: u64,
        logical_bytes: Option<u64>,
        metadata: BrowserWorkspaceMetadata,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct BrowserWorkspaceDirectoryEntry {
        name: BrowserWorkspaceName,
        #[serde(with = "serde_bytes")]
        file_id: Vec<u8>,
        kind: &'static str,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct BrowserWorkspaceDirectoryPage {
        entries: Vec<BrowserWorkspaceDirectoryEntry>,
        has_more: bool,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct BrowserWorkspaceExtentSpan {
        offset: u64,
        length: u64,
        source_end: u64,
        kind: &'static str,
    }

    #[derive(Serialize)]
    struct BrowserWorkspaceExtentPlan {
        spans: Vec<BrowserWorkspaceExtentSpan>,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct BrowserJoinOptions {
        history: String,
        maximum_generations: u32,
        maximum_changes: u32,
        maximum_conflicts: u32,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct BrowserJoinResult {
        status: &'static str,
        generation_id: Option<serde_bytes::ByteBuf>,
        conflicts: Vec<MergeConflictResult>,
        truncated: bool,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct BrowserWorkspaceRebaseResult {
        status: &'static str,
        generation_id: Option<serde_bytes::ByteBuf>,
        conflicts: Vec<MergeConflictResult>,
        truncated: bool,
    }

    #[wasm_bindgen]
    impl BrowserWorkspace {
        /// Canonical workspace name.
        #[wasm_bindgen(getter)]
        pub fn name(&self) -> String {
            match &self.engine {
                BrowserWorkspaceEngine::IndexedDb(value) => value.name().as_str().to_owned(),
                BrowserWorkspaceEngine::IndexedDbOpfs(value) => value.name().as_str().to_owned(),
                BrowserWorkspaceEngine::Memory(value) => value.name().as_str().to_owned(),
            }
        }

        /// Stable opaque workspace identity.
        #[wasm_bindgen(getter)]
        pub fn id(&self) -> Vec<u8> {
            match &self.engine {
                BrowserWorkspaceEngine::IndexedDb(value) => value.id().into_bytes().to_vec(),
                BrowserWorkspaceEngine::IndexedDbOpfs(value) => value.id().into_bytes().to_vec(),
                BrowserWorkspaceEngine::Memory(value) => value.id().into_bytes().to_vec(),
            }
        }

        /// Current immutable generation identity.
        #[wasm_bindgen]
        pub async fn head(&self) -> Result<Vec<u8>, JsValue> {
            let id = match &self.engine {
                BrowserWorkspaceEngine::IndexedDb(value) => {
                    value.head().await.map_err(js_error)?.id()
                }
                BrowserWorkspaceEngine::IndexedDbOpfs(value) => {
                    value.head().await.map_err(js_error)?.id()
                }
                BrowserWorkspaceEngine::Memory(value) => value.head().await.map_err(js_error)?.id(),
            };
            Ok(id.digest().into_bytes().to_vec())
        }

        /// Synchronizes prior operations and returns the exact immutable head.
        #[wasm_bindgen]
        pub async fn sync(&self) -> Result<BrowserGeneration, JsValue> {
            let engine = match &self.engine {
                BrowserWorkspaceEngine::IndexedDb(value) => BrowserGenerationEngine::IndexedDb(
                    value.sync().await.map_err(js_error)?.into_generation(),
                ),
                BrowserWorkspaceEngine::IndexedDbOpfs(value) => {
                    BrowserGenerationEngine::IndexedDbOpfs(
                        value.sync().await.map_err(js_error)?.into_generation(),
                    )
                }
                BrowserWorkspaceEngine::Memory(value) => BrowserGenerationEngine::Memory(
                    value.sync().await.map_err(js_error)?.into_generation(),
                ),
            };
            Ok(BrowserGeneration { engine })
        }

        /// Retains the current generation under one human-readable label.
        #[wasm_bindgen]
        pub async fn checkpoint(&self, label: String) -> Result<BrowserGeneration, JsValue> {
            let engine = match &self.engine {
                BrowserWorkspaceEngine::IndexedDb(value) => BrowserGenerationEngine::IndexedDb(
                    value
                        .checkpoint(label)
                        .await
                        .map_err(js_error)?
                        .generation()
                        .clone(),
                ),
                BrowserWorkspaceEngine::IndexedDbOpfs(value) => {
                    BrowserGenerationEngine::IndexedDbOpfs(
                        value
                            .checkpoint(label)
                            .await
                            .map_err(js_error)?
                            .generation()
                            .clone(),
                    )
                }
                BrowserWorkspaceEngine::Memory(value) => BrowserGenerationEngine::Memory(
                    value
                        .checkpoint(label)
                        .await
                        .map_err(js_error)?
                        .generation()
                        .clone(),
                ),
            };
            Ok(BrowserGeneration { engine })
        }

        /// Retains the current generation under one opaque stable identity.
        #[wasm_bindgen]
        pub async fn pin(&self, identity: String) -> Result<BrowserGeneration, JsValue> {
            let engine = match &self.engine {
                BrowserWorkspaceEngine::IndexedDb(value) => BrowserGenerationEngine::IndexedDb(
                    value
                        .pin(identity)
                        .await
                        .map_err(js_error)?
                        .generation()
                        .clone(),
                ),
                BrowserWorkspaceEngine::IndexedDbOpfs(value) => {
                    BrowserGenerationEngine::IndexedDbOpfs(
                        value
                            .pin(identity)
                            .await
                            .map_err(js_error)?
                            .generation()
                            .clone(),
                    )
                }
                BrowserWorkspaceEngine::Memory(value) => BrowserGenerationEngine::Memory(
                    value
                        .pin(identity)
                        .await
                        .map_err(js_error)?
                        .generation()
                        .clone(),
                ),
            };
            Ok(BrowserGeneration { engine })
        }

        /// Terminally removes this mutable workspace head.
        #[wasm_bindgen]
        pub async fn delete(&self, idempotency_key: Option<Vec<u8>>) -> Result<String, JsValue> {
            let key = idempotency_key.map_or_else(
                || Ok(IdempotencyKey::new()),
                |value| fixed_16_owned(value).map(IdempotencyKey::from_bytes),
            )?;
            let outcome = match &self.engine {
                BrowserWorkspaceEngine::IndexedDb(value) => value.delete(key).await,
                BrowserWorkspaceEngine::IndexedDbOpfs(value) => value.delete(key).await,
                BrowserWorkspaceEngine::Memory(value) => value.delete(key).await,
            }
            .map_err(js_error)?;
            Ok(match outcome {
                WorkspaceDelete::Deleted => "deleted",
                WorkspaceDelete::AlreadyDeleted => "already-deleted",
                WorkspaceDelete::Conflict => "conflict",
                WorkspaceDelete::IdempotencyConflict => "idempotency-conflict",
            }
            .to_owned())
        }

        /// Reads one complete regular file under a byte bound.
        #[wasm_bindgen]
        pub async fn read(&self, path: String, maximum_bytes: u64) -> Result<Vec<u8>, JsValue> {
            let bytes = match &self.engine {
                BrowserWorkspaceEngine::IndexedDb(value) => value.read(&path, maximum_bytes).await,
                BrowserWorkspaceEngine::IndexedDbOpfs(value) => {
                    value.read(&path, maximum_bytes).await
                }
                BrowserWorkspaceEngine::Memory(value) => value.read(&path, maximum_bytes).await,
            }
            .map_err(js_error)?;
            Ok(bytes.to_vec())
        }

        #[wasm_bindgen(js_name = readRange)]
        pub async fn read_range(
            &self,
            path: String,
            offset: u64,
            length: u64,
        ) -> Result<Vec<u8>, JsValue> {
            let bytes = match &self.engine {
                BrowserWorkspaceEngine::IndexedDb(value) => {
                    value.read_range(&path, offset, length).await
                }
                BrowserWorkspaceEngine::IndexedDbOpfs(value) => {
                    value.read_range(&path, offset, length).await
                }
                BrowserWorkspaceEngine::Memory(value) => {
                    value.read_range(&path, offset, length).await
                }
            }
            .map_err(js_error)?;
            Ok(bytes.to_vec())
        }

        #[wasm_bindgen]
        pub async fn stat(&self, path: String) -> Result<JsValue, JsValue> {
            let value = match &self.engine {
                BrowserWorkspaceEngine::IndexedDb(value) => value.stat(&path).await,
                BrowserWorkspaceEngine::IndexedDbOpfs(value) => value.stat(&path).await,
                BrowserWorkspaceEngine::Memory(value) => value.stat(&path).await,
            }
            .map_err(js_error)?;
            serde_wasm_bindgen::to_value(&browser_workspace_stat(value)).map_err(js_error)
        }

        #[wasm_bindgen(js_name = listDirectory)]
        pub async fn list_directory(
            &self,
            path: String,
            after: Option<JsValue>,
            maximum_entries: u32,
        ) -> Result<JsValue, JsValue> {
            let after = after.map(browser_workspace_name).transpose()?;
            let value = match &self.engine {
                BrowserWorkspaceEngine::IndexedDb(value) => {
                    value
                        .list_directory(&path, after.as_ref(), maximum_entries)
                        .await
                }
                BrowserWorkspaceEngine::IndexedDbOpfs(value) => {
                    value
                        .list_directory(&path, after.as_ref(), maximum_entries)
                        .await
                }
                BrowserWorkspaceEngine::Memory(value) => {
                    value
                        .list_directory(&path, after.as_ref(), maximum_entries)
                        .await
                }
            }
            .map_err(js_error)?;
            serde_wasm_bindgen::to_value(&browser_workspace_directory_page(value)).map_err(js_error)
        }

        #[wasm_bindgen(js_name = readSymbolicLink)]
        pub async fn read_symbolic_link(&self, path: String) -> Result<Vec<u8>, JsValue> {
            let value = match &self.engine {
                BrowserWorkspaceEngine::IndexedDb(value) => value.read_symbolic_link(&path).await,
                BrowserWorkspaceEngine::IndexedDbOpfs(value) => {
                    value.read_symbolic_link(&path).await
                }
                BrowserWorkspaceEngine::Memory(value) => value.read_symbolic_link(&path).await,
            }
            .map_err(js_error)?;
            Ok(value.to_vec())
        }

        #[wasm_bindgen(js_name = planExtents)]
        pub async fn plan_extents(
            &self,
            path: String,
            offset: u64,
            length: u64,
            maximum_spans: u32,
        ) -> Result<JsValue, JsValue> {
            let value = match &self.engine {
                BrowserWorkspaceEngine::IndexedDb(value) => {
                    value
                        .plan_extents(&path, offset, length, maximum_spans)
                        .await
                }
                BrowserWorkspaceEngine::IndexedDbOpfs(value) => {
                    value
                        .plan_extents(&path, offset, length, maximum_spans)
                        .await
                }
                BrowserWorkspaceEngine::Memory(value) => {
                    value
                        .plan_extents(&path, offset, length, maximum_spans)
                        .await
                }
            }
            .map_err(js_error)?;
            serde_wasm_bindgen::to_value(&browser_workspace_extent_plan(value)).map_err(js_error)
        }

        /// Atomically creates or replaces one complete file.
        #[wasm_bindgen]
        pub async fn write(&self, path: String, bytes: Vec<u8>) -> Result<JsValue, JsValue> {
            let outcome = match &self.engine {
                BrowserWorkspaceEngine::IndexedDb(value) => value
                    .write(&path, bytes::Bytes::from(bytes))
                    .await
                    .map(browser_workspace_commit),
                BrowserWorkspaceEngine::IndexedDbOpfs(value) => value
                    .write(&path, bytes::Bytes::from(bytes))
                    .await
                    .map(browser_workspace_commit),
                BrowserWorkspaceEngine::Memory(value) => value
                    .write(&path, bytes::Bytes::from(bytes))
                    .await
                    .map(browser_workspace_commit),
            }
            .map_err(js_error)?;
            serde_wasm_bindgen::to_value(&outcome).map_err(js_error)
        }

        /// Removes one existing path atomically.
        #[wasm_bindgen]
        pub async fn remove(&self, path: String) -> Result<JsValue, JsValue> {
            let outcome = match &self.engine {
                BrowserWorkspaceEngine::IndexedDb(value) => {
                    value.remove(&path).await.map(browser_workspace_commit)
                }
                BrowserWorkspaceEngine::IndexedDbOpfs(value) => {
                    value.remove(&path).await.map(browser_workspace_commit)
                }
                BrowserWorkspaceEngine::Memory(value) => {
                    value.remove(&path).await.map(browser_workspace_commit)
                }
            }
            .map_err(js_error)?;
            serde_wasm_bindgen::to_value(&outcome).map_err(js_error)
        }

        /// Forks the current generation into an independent named workspace.
        #[wasm_bindgen]
        pub async fn fork(
            &self,
            destination: String,
            idempotency_key: Option<Vec<u8>>,
        ) -> Result<BrowserWorkspace, JsValue> {
            let idempotency_key = idempotency_key.map_or_else(
                || Ok(IdempotencyKey::new()),
                |value| fixed_16_owned(value).map(IdempotencyKey::from_bytes),
            )?;
            let engine = match &self.engine {
                BrowserWorkspaceEngine::IndexedDb(value) => {
                    let generation = value.head().await.map_err(js_error)?;
                    BrowserWorkspaceEngine::IndexedDb(
                        value
                            .fork(
                                destination,
                                ForkOptions::from_generation(generation, idempotency_key),
                            )
                            .await
                            .map_err(js_error)?,
                    )
                }
                BrowserWorkspaceEngine::IndexedDbOpfs(value) => {
                    let generation = value.head().await.map_err(js_error)?;
                    BrowserWorkspaceEngine::IndexedDbOpfs(
                        value
                            .fork(
                                destination,
                                ForkOptions::from_generation(generation, idempotency_key),
                            )
                            .await
                            .map_err(js_error)?,
                    )
                }
                BrowserWorkspaceEngine::Memory(value) => {
                    let generation = value.head().await.map_err(js_error)?;
                    BrowserWorkspaceEngine::Memory(
                        value
                            .fork(
                                destination,
                                ForkOptions::from_generation(generation, idempotency_key),
                            )
                            .await
                            .map_err(js_error)?,
                    )
                }
            };
            Ok(BrowserWorkspace { engine })
        }

        /// Creates an independent workspace at one caller-selected exact generation.
        #[wasm_bindgen(js_name = forkAt)]
        pub async fn fork_at(
            &self,
            destination: String,
            generation: &BrowserGeneration,
            idempotency_key: Option<Vec<u8>>,
        ) -> Result<BrowserWorkspace, JsValue> {
            let idempotency_key = idempotency_key.map_or_else(
                || Ok(IdempotencyKey::new()),
                |value| fixed_16_owned(value).map(IdempotencyKey::from_bytes),
            )?;
            let engine = match (&self.engine, &generation.engine) {
                (
                    BrowserWorkspaceEngine::IndexedDb(value),
                    BrowserGenerationEngine::IndexedDb(generation),
                ) => BrowserWorkspaceEngine::IndexedDb(
                    value
                        .fork(
                            destination,
                            ForkOptions::from_generation(generation.clone(), idempotency_key),
                        )
                        .await
                        .map_err(js_error)?,
                ),
                (
                    BrowserWorkspaceEngine::IndexedDbOpfs(value),
                    BrowserGenerationEngine::IndexedDbOpfs(generation),
                ) => BrowserWorkspaceEngine::IndexedDbOpfs(
                    value
                        .fork(
                            destination,
                            ForkOptions::from_generation(generation.clone(), idempotency_key),
                        )
                        .await
                        .map_err(js_error)?,
                ),
                (
                    BrowserWorkspaceEngine::Memory(value),
                    BrowserGenerationEngine::Memory(generation),
                ) => BrowserWorkspaceEngine::Memory(
                    value
                        .fork(
                            destination,
                            ForkOptions::from_generation(generation.clone(), idempotency_key),
                        )
                        .await
                        .map_err(js_error)?,
                ),
                _ => {
                    return Err(JsValue::from_str(
                        "generation belongs to another filesystem",
                    ));
                }
            };
            Ok(BrowserWorkspace { engine })
        }

        /// Begins one sparse atomic transaction at the current workspace head.
        #[wasm_bindgen(js_name = beginTransaction)]
        pub async fn begin_transaction(
            &self,
            idempotency_key: Option<Vec<u8>>,
        ) -> Result<BrowserTransaction, JsValue> {
            let idempotency_key = idempotency_key.map_or_else(
                || Ok(IdempotencyKey::new()),
                |value| fixed_16_owned(value).map(IdempotencyKey::from_bytes),
            )?;
            let engine = match &self.engine {
                BrowserWorkspaceEngine::IndexedDb(value) => BrowserTransactionEngine::IndexedDb(
                    value
                        .begin_transaction(idempotency_key)
                        .await
                        .map_err(js_error)?,
                ),
                BrowserWorkspaceEngine::IndexedDbOpfs(value) => {
                    BrowserTransactionEngine::IndexedDbOpfs(
                        value
                            .begin_transaction(idempotency_key)
                            .await
                            .map_err(js_error)?,
                    )
                }
                BrowserWorkspaceEngine::Memory(value) => BrowserTransactionEngine::Memory(
                    value
                        .begin_transaction(idempotency_key)
                        .await
                        .map_err(js_error)?,
                ),
            };
            Ok(BrowserTransaction { engine })
        }

        /// Advances this fork onto its source workspace's current generation.
        #[wasm_bindgen(js_name = liveRebase)]
        pub async fn live_rebase(
            &self,
            idempotency_key: Option<Vec<u8>>,
            maximum_generations: u32,
            maximum_changes: u32,
            maximum_conflicts: u32,
        ) -> Result<JsValue, JsValue> {
            let idempotency_key = idempotency_key.map_or_else(
                || Ok(IdempotencyKey::new()),
                |value| fixed_16_owned(value).map(IdempotencyKey::from_bytes),
            )?;
            let outcome = match &self.engine {
                BrowserWorkspaceEngine::IndexedDb(value) => value
                    .live_rebase(
                        idempotency_key,
                        maximum_generations,
                        maximum_changes,
                        maximum_conflicts,
                    )
                    .await
                    .map(browser_workspace_rebase_result),
                BrowserWorkspaceEngine::IndexedDbOpfs(value) => value
                    .live_rebase(
                        idempotency_key,
                        maximum_generations,
                        maximum_changes,
                        maximum_conflicts,
                    )
                    .await
                    .map(browser_workspace_rebase_result),
                BrowserWorkspaceEngine::Memory(value) => value
                    .live_rebase(
                        idempotency_key,
                        maximum_generations,
                        maximum_changes,
                        maximum_conflicts,
                    )
                    .await
                    .map(browser_workspace_rebase_result),
            }
            .map_err(js_error)?;
            serde_wasm_bindgen::to_value(&outcome).map_err(js_error)
        }

        /// Computes one immutable bounded semantic delta between exact generations.
        #[wasm_bindgen]
        pub async fn diff(
            &self,
            from: &BrowserGeneration,
            to: &BrowserGeneration,
            maximum_changes: u32,
        ) -> Result<BrowserChangeSet, JsValue> {
            let engine = match (&self.engine, &from.engine, &to.engine) {
                (
                    BrowserWorkspaceEngine::IndexedDb(workspace),
                    BrowserGenerationEngine::IndexedDb(from),
                    BrowserGenerationEngine::IndexedDb(to),
                ) => BrowserChangeSetEngine::IndexedDb(
                    workspace
                        .diff(from, to, maximum_changes)
                        .await
                        .map_err(js_error)?,
                ),
                (
                    BrowserWorkspaceEngine::IndexedDbOpfs(workspace),
                    BrowserGenerationEngine::IndexedDbOpfs(from),
                    BrowserGenerationEngine::IndexedDbOpfs(to),
                ) => BrowserChangeSetEngine::IndexedDbOpfs(
                    workspace
                        .diff(from, to, maximum_changes)
                        .await
                        .map_err(js_error)?,
                ),
                (
                    BrowserWorkspaceEngine::Memory(workspace),
                    BrowserGenerationEngine::Memory(from),
                    BrowserGenerationEngine::Memory(to),
                ) => BrowserChangeSetEngine::Memory(
                    workspace
                        .diff(from, to, maximum_changes)
                        .await
                        .map_err(js_error)?,
                ),
                _ => {
                    return Err(JsValue::from_str(
                        "generation belongs to another filesystem",
                    ));
                }
            };
            Ok(BrowserChangeSet { engine })
        }

        /// Builds one immutable side-effect-free plan for joining this workspace into a target.
        #[wasm_bindgen(js_name = joinInto)]
        pub async fn join_into(
            &self,
            target: &BrowserWorkspace,
            options: JsValue,
        ) -> Result<BrowserJoinPlan, JsValue> {
            let options: BrowserJoinOptions =
                serde_wasm_bindgen::from_value(options).map_err(js_error)?;
            let history = browser_join_history(&options.history)?;
            let engine = match (&self.engine, &target.engine) {
                (
                    BrowserWorkspaceEngine::IndexedDb(source),
                    BrowserWorkspaceEngine::IndexedDb(target),
                ) => BrowserJoinPlanEngine::IndexedDb(
                    source
                        .join_into(target)
                        .history(history)
                        .bounds(
                            options.maximum_generations,
                            options.maximum_changes,
                            options.maximum_conflicts,
                        )
                        .plan()
                        .await
                        .map_err(js_error)?,
                ),
                (
                    BrowserWorkspaceEngine::IndexedDbOpfs(source),
                    BrowserWorkspaceEngine::IndexedDbOpfs(target),
                ) => BrowserJoinPlanEngine::IndexedDbOpfs(
                    source
                        .join_into(target)
                        .history(history)
                        .bounds(
                            options.maximum_generations,
                            options.maximum_changes,
                            options.maximum_conflicts,
                        )
                        .plan()
                        .await
                        .map_err(js_error)?,
                ),
                (
                    BrowserWorkspaceEngine::Memory(source),
                    BrowserWorkspaceEngine::Memory(target),
                ) => BrowserJoinPlanEngine::Memory(
                    source
                        .join_into(target)
                        .history(history)
                        .bounds(
                            options.maximum_generations,
                            options.maximum_changes,
                            options.maximum_conflicts,
                        )
                        .plan()
                        .await
                        .map_err(js_error)?,
                ),
                _ => return Err(JsValue::from_str("workspace belongs to another filesystem")),
            };
            Ok(BrowserJoinPlan { engine })
        }
    }

    #[wasm_bindgen]
    impl BrowserChangeSet {
        /// Exact immutable base endpoint.
        #[wasm_bindgen(getter)]
        pub fn from(&self) -> BrowserGeneration {
            let engine = match &self.engine {
                BrowserChangeSetEngine::IndexedDb(value) => {
                    BrowserGenerationEngine::IndexedDb(value.from().clone())
                }
                BrowserChangeSetEngine::IndexedDbOpfs(value) => {
                    BrowserGenerationEngine::IndexedDbOpfs(value.from().clone())
                }
                BrowserChangeSetEngine::Memory(value) => {
                    BrowserGenerationEngine::Memory(value.from().clone())
                }
            };
            BrowserGeneration { engine }
        }

        /// Exact immutable resulting endpoint.
        #[wasm_bindgen(getter)]
        pub fn to(&self) -> BrowserGeneration {
            let engine = match &self.engine {
                BrowserChangeSetEngine::IndexedDb(value) => {
                    BrowserGenerationEngine::IndexedDb(value.to().clone())
                }
                BrowserChangeSetEngine::IndexedDbOpfs(value) => {
                    BrowserGenerationEngine::IndexedDbOpfs(value.to().clone())
                }
                BrowserChangeSetEngine::Memory(value) => {
                    BrowserGenerationEngine::Memory(value.to().clone())
                }
            };
            BrowserGeneration { engine }
        }

        /// Stable path-independent records and namespace binding changes.
        pub fn changes(&self) -> Result<JsValue, JsValue> {
            let (changes, work) = match &self.engine {
                BrowserChangeSetEngine::IndexedDb(value) => (value.changes().clone(), value.work()),
                BrowserChangeSetEngine::IndexedDbOpfs(value) => {
                    (value.changes().clone(), value.work())
                }
                BrowserChangeSetEngine::Memory(value) => (value.changes().clone(), value.work()),
            };
            encode_generation_diff(changes, work)
        }

        /// Composes contiguous immutable deltas by diffing their outer endpoints.
        #[wasm_bindgen]
        pub async fn compose(
            &self,
            next: &BrowserChangeSet,
            maximum_changes: u32,
        ) -> Result<BrowserChangeSet, JsValue> {
            let engine = match (&self.engine, &next.engine) {
                (
                    BrowserChangeSetEngine::IndexedDb(value),
                    BrowserChangeSetEngine::IndexedDb(next),
                ) => BrowserChangeSetEngine::IndexedDb(
                    value
                        .compose(next, maximum_changes)
                        .await
                        .map_err(js_error)?,
                ),
                (
                    BrowserChangeSetEngine::IndexedDbOpfs(value),
                    BrowserChangeSetEngine::IndexedDbOpfs(next),
                ) => BrowserChangeSetEngine::IndexedDbOpfs(
                    value
                        .compose(next, maximum_changes)
                        .await
                        .map_err(js_error)?,
                ),
                (BrowserChangeSetEngine::Memory(value), BrowserChangeSetEngine::Memory(next)) => {
                    BrowserChangeSetEngine::Memory(
                        value
                            .compose(next, maximum_changes)
                            .await
                            .map_err(js_error)?,
                    )
                }
                _ => {
                    return Err(JsValue::from_str(
                        "change set belongs to another filesystem",
                    ));
                }
            };
            Ok(BrowserChangeSet { engine })
        }
    }

    #[wasm_bindgen]
    impl BrowserJoinPlan {
        /// Target generation observed while planning.
        #[wasm_bindgen(getter, js_name = targetHead)]
        pub fn target_head(&self) -> Vec<u8> {
            match &self.engine {
                BrowserJoinPlanEngine::IndexedDb(value) => value.target_head(),
                BrowserJoinPlanEngine::IndexedDbOpfs(value) => value.target_head(),
                BrowserJoinPlanEngine::Memory(value) => value.target_head(),
            }
            .digest()
            .into_bytes()
            .to_vec()
        }

        /// Exact discovered common ancestor.
        #[wasm_bindgen(getter, js_name = commonAncestor)]
        pub fn common_ancestor(&self) -> Vec<u8> {
            match &self.engine {
                BrowserJoinPlanEngine::IndexedDb(value) => value.common_ancestor(),
                BrowserJoinPlanEngine::IndexedDbOpfs(value) => value.common_ancestor(),
                BrowserJoinPlanEngine::Memory(value) => value.common_ancestor(),
            }
            .digest()
            .into_bytes()
            .to_vec()
        }

        /// Applies this immutable plan through one exact target-head CAS.
        #[wasm_bindgen]
        pub async fn apply(
            &self,
            if_target: Vec<u8>,
            idempotency_key: Option<Vec<u8>>,
        ) -> Result<JsValue, JsValue> {
            let if_target = acyclic_fs::GenerationId::new(Digest::from_bytes(fixed_32(
                &if_target,
                "target generation identity",
            )?));
            let idempotency_key = idempotency_key.map_or_else(
                || Ok(IdempotencyKey::new()),
                |value| fixed_16_owned(value).map(IdempotencyKey::from_bytes),
            )?;
            let options = ApplyOptions {
                if_target,
                idempotency_key,
            };
            let result = match &self.engine {
                BrowserJoinPlanEngine::IndexedDb(value) => {
                    browser_join_result(value.apply(options).await.map_err(js_error)?)
                }
                BrowserJoinPlanEngine::IndexedDbOpfs(value) => {
                    browser_join_result(value.apply(options).await.map_err(js_error)?)
                }
                BrowserJoinPlanEngine::Memory(value) => {
                    browser_join_result(value.apply(options).await.map_err(js_error)?)
                }
            };
            serde_wasm_bindgen::to_value(&result).map_err(js_error)
        }
    }

    #[wasm_bindgen]
    impl BrowserGeneration {
        /// Content-addressed generation identity.
        #[wasm_bindgen(getter)]
        pub fn id(&self) -> Vec<u8> {
            match &self.engine {
                BrowserGenerationEngine::IndexedDb(value) => {
                    value.id().digest().into_bytes().to_vec()
                }
                BrowserGenerationEngine::IndexedDbOpfs(value) => {
                    value.id().digest().into_bytes().to_vec()
                }
                BrowserGenerationEngine::Memory(value) => value.id().digest().into_bytes().to_vec(),
            }
        }

        /// Owning opaque workspace identity.
        #[wasm_bindgen(getter, js_name = workspaceId)]
        pub fn workspace_id(&self) -> Vec<u8> {
            match &self.engine {
                BrowserGenerationEngine::IndexedDb(value) => {
                    value.workspace_id().into_bytes().to_vec()
                }
                BrowserGenerationEngine::IndexedDbOpfs(value) => {
                    value.workspace_id().into_bytes().to_vec()
                }
                BrowserGenerationEngine::Memory(value) => {
                    value.workspace_id().into_bytes().to_vec()
                }
            }
        }

        /// Reads one complete file from this exact immutable state.
        #[wasm_bindgen]
        pub async fn read(&self, path: String, maximum_bytes: u64) -> Result<Vec<u8>, JsValue> {
            let bytes = match &self.engine {
                BrowserGenerationEngine::IndexedDb(value) => value.read(&path, maximum_bytes).await,
                BrowserGenerationEngine::IndexedDbOpfs(value) => {
                    value.read(&path, maximum_bytes).await
                }
                BrowserGenerationEngine::Memory(value) => value.read(&path, maximum_bytes).await,
            }
            .map_err(js_error)?;
            Ok(bytes.to_vec())
        }

        #[wasm_bindgen(js_name = readRange)]
        pub async fn read_range(
            &self,
            path: String,
            offset: u64,
            length: u64,
        ) -> Result<Vec<u8>, JsValue> {
            let bytes = match &self.engine {
                BrowserGenerationEngine::IndexedDb(value) => {
                    value.read_range(&path, offset, length).await
                }
                BrowserGenerationEngine::IndexedDbOpfs(value) => {
                    value.read_range(&path, offset, length).await
                }
                BrowserGenerationEngine::Memory(value) => {
                    value.read_range(&path, offset, length).await
                }
            }
            .map_err(js_error)?;
            Ok(bytes.to_vec())
        }

        #[wasm_bindgen]
        pub async fn stat(&self, path: String) -> Result<JsValue, JsValue> {
            let value = match &self.engine {
                BrowserGenerationEngine::IndexedDb(value) => value.stat(&path).await,
                BrowserGenerationEngine::IndexedDbOpfs(value) => value.stat(&path).await,
                BrowserGenerationEngine::Memory(value) => value.stat(&path).await,
            }
            .map_err(js_error)?;
            serde_wasm_bindgen::to_value(&browser_workspace_stat(value)).map_err(js_error)
        }

        #[wasm_bindgen(js_name = listDirectory)]
        pub async fn list_directory(
            &self,
            path: String,
            after: Option<JsValue>,
            maximum_entries: u32,
        ) -> Result<JsValue, JsValue> {
            let after = after.map(browser_workspace_name).transpose()?;
            let value = match &self.engine {
                BrowserGenerationEngine::IndexedDb(value) => {
                    value
                        .list_directory(&path, after.as_ref(), maximum_entries)
                        .await
                }
                BrowserGenerationEngine::IndexedDbOpfs(value) => {
                    value
                        .list_directory(&path, after.as_ref(), maximum_entries)
                        .await
                }
                BrowserGenerationEngine::Memory(value) => {
                    value
                        .list_directory(&path, after.as_ref(), maximum_entries)
                        .await
                }
            }
            .map_err(js_error)?;
            serde_wasm_bindgen::to_value(&browser_workspace_directory_page(value)).map_err(js_error)
        }

        #[wasm_bindgen(js_name = readSymbolicLink)]
        pub async fn read_symbolic_link(&self, path: String) -> Result<Vec<u8>, JsValue> {
            let value = match &self.engine {
                BrowserGenerationEngine::IndexedDb(value) => value.read_symbolic_link(&path).await,
                BrowserGenerationEngine::IndexedDbOpfs(value) => {
                    value.read_symbolic_link(&path).await
                }
                BrowserGenerationEngine::Memory(value) => value.read_symbolic_link(&path).await,
            }
            .map_err(js_error)?;
            Ok(value.to_vec())
        }

        #[wasm_bindgen(js_name = planExtents)]
        pub async fn plan_extents(
            &self,
            path: String,
            offset: u64,
            length: u64,
            maximum_spans: u32,
        ) -> Result<JsValue, JsValue> {
            let value = match &self.engine {
                BrowserGenerationEngine::IndexedDb(value) => {
                    value
                        .plan_extents(&path, offset, length, maximum_spans)
                        .await
                }
                BrowserGenerationEngine::IndexedDbOpfs(value) => {
                    value
                        .plan_extents(&path, offset, length, maximum_spans)
                        .await
                }
                BrowserGenerationEngine::Memory(value) => {
                    value
                        .plan_extents(&path, offset, length, maximum_spans)
                        .await
                }
            }
            .map_err(js_error)?;
            serde_wasm_bindgen::to_value(&browser_workspace_extent_plan(value)).map_err(js_error)
        }

        /// Retains this exact generation under one opaque identity.
        #[wasm_bindgen]
        pub async fn pin(&self, identity: String) -> Result<BrowserGeneration, JsValue> {
            let engine = match &self.engine {
                BrowserGenerationEngine::IndexedDb(value) => BrowserGenerationEngine::IndexedDb(
                    value
                        .pin(identity)
                        .await
                        .map_err(js_error)?
                        .generation()
                        .clone(),
                ),
                BrowserGenerationEngine::IndexedDbOpfs(value) => {
                    BrowserGenerationEngine::IndexedDbOpfs(
                        value
                            .pin(identity)
                            .await
                            .map_err(js_error)?
                            .generation()
                            .clone(),
                    )
                }
                BrowserGenerationEngine::Memory(value) => BrowserGenerationEngine::Memory(
                    value
                        .pin(identity)
                        .await
                        .map_err(js_error)?
                        .generation()
                        .clone(),
                ),
            };
            Ok(BrowserGeneration { engine })
        }
    }

    #[wasm_bindgen]
    impl BrowserTransaction {
        /// Creates every absent directory on one canonical path.
        #[wasm_bindgen(js_name = createDirAll)]
        pub async fn create_dir_all(&mut self, path: String) -> Result<(), JsValue> {
            match &mut self.engine {
                BrowserTransactionEngine::IndexedDb(value) => value.create_dir_all(&path).await,
                BrowserTransactionEngine::IndexedDbOpfs(value) => value.create_dir_all(&path).await,
                BrowserTransactionEngine::Memory(value) => value.create_dir_all(&path).await,
            }
            .map_err(js_error)
        }

        /// Creates exactly one empty directory.
        #[wasm_bindgen(js_name = createDirectory)]
        pub async fn create_directory(&mut self, path: String) -> Result<(), JsValue> {
            match &mut self.engine {
                BrowserTransactionEngine::IndexedDb(value) => value.create_directory(&path).await,
                BrowserTransactionEngine::IndexedDbOpfs(value) => {
                    value.create_directory(&path).await
                }
                BrowserTransactionEngine::Memory(value) => value.create_directory(&path).await,
            }
            .map_err(js_error)
        }

        /// Creates one symbolic link with an opaque target.
        #[wasm_bindgen(js_name = createSymbolicLink)]
        pub async fn create_symbolic_link(
            &mut self,
            path: String,
            target: Vec<u8>,
        ) -> Result<(), JsValue> {
            let target = bytes::Bytes::from(target);
            match &mut self.engine {
                BrowserTransactionEngine::IndexedDb(value) => {
                    value.create_symbolic_link(&path, target).await
                }
                BrowserTransactionEngine::IndexedDbOpfs(value) => {
                    value.create_symbolic_link(&path, target).await
                }
                BrowserTransactionEngine::Memory(value) => {
                    value.create_symbolic_link(&path, target).await
                }
            }
            .map_err(js_error)
        }

        /// Creates or replaces one complete file inside this transaction.
        #[wasm_bindgen]
        pub async fn write(&mut self, path: String, bytes: Vec<u8>) -> Result<(), JsValue> {
            let bytes = bytes::Bytes::from(bytes);
            match &mut self.engine {
                BrowserTransactionEngine::IndexedDb(value) => value.write(&path, bytes).await,
                BrowserTransactionEngine::IndexedDbOpfs(value) => value.write(&path, bytes).await,
                BrowserTransactionEngine::Memory(value) => value.write(&path, bytes).await,
            }
            .map_err(js_error)
        }

        /// Removes one existing namespace binding inside this transaction.
        #[wasm_bindgen]
        pub async fn remove(&mut self, path: String) -> Result<(), JsValue> {
            match &mut self.engine {
                BrowserTransactionEngine::IndexedDb(value) => value.remove(&path).await,
                BrowserTransactionEngine::IndexedDbOpfs(value) => value.remove(&path).await,
                BrowserTransactionEngine::Memory(value) => value.remove(&path).await,
            }
            .map_err(js_error)
        }

        /// Clones one complete regular file without copying its body.
        #[wasm_bindgen]
        pub async fn copy(&mut self, source: String, destination: String) -> Result<(), JsValue> {
            match &mut self.engine {
                BrowserTransactionEngine::IndexedDb(value) => {
                    value.copy(&source, &destination).await
                }
                BrowserTransactionEngine::IndexedDbOpfs(value) => {
                    value.copy(&source, &destination).await
                }
                BrowserTransactionEngine::Memory(value) => value.copy(&source, &destination).await,
            }
            .map_err(js_error)
        }

        /// Atomically renames one namespace binding inside this transaction.
        #[wasm_bindgen]
        pub async fn rename(&mut self, source: String, destination: String) -> Result<(), JsValue> {
            match &mut self.engine {
                BrowserTransactionEngine::IndexedDb(value) => {
                    value.rename(&source, &destination).await
                }
                BrowserTransactionEngine::IndexedDbOpfs(value) => {
                    value.rename(&source, &destination).await
                }
                BrowserTransactionEngine::Memory(value) => {
                    value.rename(&source, &destination).await
                }
            }
            .map_err(js_error)
        }

        /// Creates one same-workspace hard link.
        #[wasm_bindgen(js_name = hardLink)]
        pub async fn hard_link(
            &mut self,
            source: String,
            destination: String,
        ) -> Result<(), JsValue> {
            match &mut self.engine {
                BrowserTransactionEngine::IndexedDb(value) => {
                    value.hard_link(&source, &destination).await
                }
                BrowserTransactionEngine::IndexedDbOpfs(value) => {
                    value.hard_link(&source, &destination).await
                }
                BrowserTransactionEngine::Memory(value) => {
                    value.hard_link(&source, &destination).await
                }
            }
            .map_err(js_error)
        }

        /// Replaces one sparse regular-file range.
        #[wasm_bindgen(js_name = writeRange)]
        pub async fn write_range(
            &mut self,
            path: String,
            offset: u64,
            bytes: Vec<u8>,
        ) -> Result<(), JsValue> {
            let bytes = bytes::Bytes::from(bytes);
            match &mut self.engine {
                BrowserTransactionEngine::IndexedDb(value) => {
                    value.write_range(&path, offset, bytes).await
                }
                BrowserTransactionEngine::IndexedDbOpfs(value) => {
                    value.write_range(&path, offset, bytes).await
                }
                BrowserTransactionEngine::Memory(value) => {
                    value.write_range(&path, offset, bytes).await
                }
            }
            .map_err(js_error)
        }

        /// Changes one regular file's logical length.
        pub async fn resize(&mut self, path: String, logical_bytes: u64) -> Result<(), JsValue> {
            match &mut self.engine {
                BrowserTransactionEngine::IndexedDb(value) => {
                    value.resize(&path, logical_bytes).await
                }
                BrowserTransactionEngine::IndexedDbOpfs(value) => {
                    value.resize(&path, logical_bytes).await
                }
                BrowserTransactionEngine::Memory(value) => value.resize(&path, logical_bytes).await,
            }
            .map_err(js_error)
        }

        /// Punches a hole or installs allocated zeros over one exact range.
        #[wasm_bindgen(js_name = zeroRange)]
        pub async fn zero_range(
            &mut self,
            path: String,
            offset: u64,
            length: u64,
            allocated: bool,
            extend: bool,
        ) -> Result<(), JsValue> {
            let range = ByteRange { offset, length };
            match &mut self.engine {
                BrowserTransactionEngine::IndexedDb(value) => {
                    value.zero_range(&path, range, allocated, extend).await
                }
                BrowserTransactionEngine::IndexedDbOpfs(value) => {
                    value.zero_range(&path, range, allocated, extend).await
                }
                BrowserTransactionEngine::Memory(value) => {
                    value.zero_range(&path, range, allocated, extend).await
                }
            }
            .map_err(js_error)
        }

        /// Preallocates one sparse range without replacing content.
        pub async fn preallocate(
            &mut self,
            path: String,
            offset: u64,
            length: u64,
            keep_size: bool,
        ) -> Result<(), JsValue> {
            let range = ByteRange { offset, length };
            match &mut self.engine {
                BrowserTransactionEngine::IndexedDb(value) => {
                    value.preallocate(&path, range, keep_size).await
                }
                BrowserTransactionEngine::IndexedDbOpfs(value) => {
                    value.preallocate(&path, range, keep_size).await
                }
                BrowserTransactionEngine::Memory(value) => {
                    value.preallocate(&path, range, keep_size).await
                }
            }
            .map_err(js_error)
        }

        /// Clones one immutable range without reading file bytes.
        #[wasm_bindgen(js_name = cloneRange)]
        pub async fn clone_range(
            &mut self,
            source: String,
            source_offset: u64,
            destination: String,
            destination_offset: u64,
            length: u64,
        ) -> Result<(), JsValue> {
            match &mut self.engine {
                BrowserTransactionEngine::IndexedDb(value) => {
                    value
                        .clone_range(
                            &source,
                            source_offset,
                            &destination,
                            destination_offset,
                            length,
                        )
                        .await
                }
                BrowserTransactionEngine::IndexedDbOpfs(value) => {
                    value
                        .clone_range(
                            &source,
                            source_offset,
                            &destination,
                            destination_offset,
                            length,
                        )
                        .await
                }
                BrowserTransactionEngine::Memory(value) => {
                    value
                        .clone_range(
                            &source,
                            source_offset,
                            &destination,
                            destination_offset,
                            length,
                        )
                        .await
                }
            }
            .map_err(js_error)
        }

        /// Publishes the complete candidate through one idempotent head CAS.
        #[wasm_bindgen]
        pub async fn commit(&mut self) -> Result<JsValue, JsValue> {
            let outcome = match &mut self.engine {
                BrowserTransactionEngine::IndexedDb(value) => {
                    value.commit().await.map(browser_workspace_commit)
                }
                BrowserTransactionEngine::IndexedDbOpfs(value) => {
                    value.commit().await.map(browser_workspace_commit)
                }
                BrowserTransactionEngine::Memory(value) => {
                    value.commit().await.map(browser_workspace_commit)
                }
            }
            .map_err(js_error)?;
            serde_wasm_bindgen::to_value(&outcome).map_err(js_error)
        }

        /// Safely advances this retained candidate and sparsely replays its work.
        #[wasm_bindgen]
        pub async fn rebase(&mut self, maximum_conflicts: u32) -> Result<JsValue, JsValue> {
            let outcome = match &mut self.engine {
                BrowserTransactionEngine::IndexedDb(value) => value
                    .rebase(maximum_conflicts)
                    .await
                    .map(browser_transaction_rebase),
                BrowserTransactionEngine::IndexedDbOpfs(value) => value
                    .rebase(maximum_conflicts)
                    .await
                    .map(browser_transaction_rebase),
                BrowserTransactionEngine::Memory(value) => value
                    .rebase(maximum_conflicts)
                    .await
                    .map(browser_transaction_rebase),
            }
            .map_err(js_error)?;
            serde_wasm_bindgen::to_value(&outcome).map_err(js_error)
        }
    }

    fn browser_workspace_commit<A, O>(outcome: TransactionCommit<A, O>) -> BrowserWorkspaceCommit {
        match outcome {
            TransactionCommit::Committed(value) => BrowserWorkspaceCommit {
                status: "committed",
                generation_id: Some(value.id().digest().into_bytes().to_vec().into()),
            },
            TransactionCommit::AlreadyCommitted(value) => BrowserWorkspaceCommit {
                status: "already-committed",
                generation_id: Some(value.id().digest().into_bytes().to_vec().into()),
            },
            TransactionCommit::Conflict { actual } => BrowserWorkspaceCommit {
                status: "conflict",
                generation_id: Some(actual.id().digest().into_bytes().to_vec().into()),
            },
            TransactionCommit::Fenced => BrowserWorkspaceCommit {
                status: "fenced",
                generation_id: None,
            },
            TransactionCommit::IdempotencyConflict => BrowserWorkspaceCommit {
                status: "idempotency-conflict",
                generation_id: None,
            },
        }
    }

    fn browser_transaction_rebase<A, O>(
        outcome: TransactionRebase<A, O>,
    ) -> BrowserTransactionRebase {
        match outcome {
            TransactionRebase::Rebased(generation) => BrowserTransactionRebase {
                status: "rebased",
                generation_id: Some(generation.id().digest().into_bytes().to_vec().into()),
                conflicts: Vec::new(),
                truncated: false,
            },
            TransactionRebase::Conflicted {
                conflicts,
                truncated,
            } => BrowserTransactionRebase {
                status: "conflicted",
                generation_id: None,
                conflicts: conflicts
                    .into_iter()
                    .map(browser_transaction_conflict)
                    .collect(),
                truncated,
            },
        }
    }

    fn browser_transaction_conflict(value: TransactionConflict) -> BrowserTransactionConflict {
        let mut result = BrowserTransactionConflict {
            region: "",
            file_id: None,
            directory_id: None,
            offset: None,
            length: None,
            sparse_target: None,
            name: None,
            maximum_entries: None,
            usage: match value.usage {
                TransactionDependencyUse::Observation => "observation",
                TransactionDependencyUse::Mutation => "mutation",
                TransactionDependencyUse::ObservationAndMutation => "observation-and-mutation",
            },
            expected: value
                .expected
                .map(|digest| digest.into_bytes().to_vec().into()),
            actual: value
                .actual
                .map(|digest| digest.into_bytes().to_vec().into()),
        };
        match value.region {
            TransactionConflictRegion::FileRecord(file_id) => {
                result.region = "file-record";
                result.file_id = Some(file_id.into_bytes().to_vec().into());
            }
            TransactionConflictRegion::Metadata(file_id) => {
                result.region = "metadata";
                result.file_id = Some(file_id.into_bytes().to_vec().into());
            }
            TransactionConflictRegion::FileLength(file_id) => {
                result.region = "file-length";
                result.file_id = Some(file_id.into_bytes().to_vec().into());
            }
            TransactionConflictRegion::ContentRange {
                file_id,
                offset,
                length,
            } => {
                result.region = "content-range";
                result.file_id = Some(file_id.into_bytes().to_vec().into());
                result.offset = Some(offset);
                result.length = Some(length);
            }
            TransactionConflictRegion::SparseSeek {
                file_id,
                offset,
                target,
            } => {
                result.region = "sparse-seek";
                result.file_id = Some(file_id.into_bytes().to_vec().into());
                result.offset = Some(offset);
                result.sparse_target = Some(match target {
                    TransactionSparseSeek::Data => "data",
                    TransactionSparseSeek::Hole => "hole",
                });
            }
            TransactionConflictRegion::DirectoryName { directory_id, name } => {
                result.region = "directory-name";
                result.directory_id = Some(directory_id.into_bytes().to_vec().into());
                result.name = Some(browser_workspace_name_value(name));
            }
            TransactionConflictRegion::DirectoryRange {
                directory_id,
                after,
                maximum_entries,
            } => {
                result.region = "directory-range";
                result.directory_id = Some(directory_id.into_bytes().to_vec().into());
                result.name = after.map(browser_workspace_name_value);
                result.maximum_entries = Some(maximum_entries);
            }
        }
        result
    }

    #[allow(clippy::needless_pass_by_value)]
    fn browser_workspace_name_value(name: LogicalName) -> BrowserWorkspaceName {
        BrowserWorkspaceName {
            encoding: match name.encoding() {
                NameEncoding::Utf8 => "utf8",
                NameEncoding::PosixBytes => "posix-bytes",
                NameEncoding::WindowsUtf16Le => "windows-utf16le",
            }
            .to_owned(),
            bytes: name.as_bytes().to_vec(),
        }
    }

    fn browser_workspace_name(value: JsValue) -> Result<LogicalName, JsValue> {
        let value: BrowserWorkspaceName =
            serde_wasm_bindgen::from_value(value).map_err(js_error)?;
        let encoding = match value.encoding.as_str() {
            "utf8" => NameEncoding::Utf8,
            "posix-bytes" => NameEncoding::PosixBytes,
            "windows-utf16le" => NameEncoding::WindowsUtf16Le,
            _ => return Err(JsValue::from_str("unknown name encoding")),
        };
        LogicalName::new(encoding, value.bytes, u32::MAX).map_err(js_error)
    }

    fn browser_workspace_metadata(value: WorkspaceMetadata) -> BrowserWorkspaceMetadata {
        BrowserWorkspaceMetadata {
            posix_mode: value.posix_mode,
            posix_uid: value.posix_uid,
            posix_gid: value.posix_gid,
            posix_flags: value.posix_flags,
            windows_attributes: value.windows_attributes,
            created_ns: value.created_ns,
            modified_ns: value.modified_ns,
            accessed_ns: value.accessed_ns,
            changed_ns: value.changed_ns,
            has_named_attributes: value.has_named_attributes,
            has_acl: value.has_acl,
            has_security_descriptor: value.has_security_descriptor,
        }
    }

    #[allow(clippy::needless_pass_by_value)]
    fn browser_workspace_stat(value: WorkspaceStat) -> BrowserWorkspaceStat {
        BrowserWorkspaceStat {
            file_id: value.file_id.into_bytes().to_vec(),
            kind: file_kind(value.kind),
            link_count: value.link_count,
            logical_bytes: value.logical_bytes,
            metadata: browser_workspace_metadata(value.metadata),
        }
    }

    fn browser_workspace_directory_page(
        value: WorkspaceDirectoryPage,
    ) -> BrowserWorkspaceDirectoryPage {
        BrowserWorkspaceDirectoryPage {
            entries: value
                .entries
                .into_iter()
                .map(|entry| BrowserWorkspaceDirectoryEntry {
                    name: BrowserWorkspaceName {
                        encoding: match entry.name.encoding() {
                            NameEncoding::Utf8 => "utf8",
                            NameEncoding::PosixBytes => "posix-bytes",
                            NameEncoding::WindowsUtf16Le => "windows-utf16le",
                        }
                        .to_owned(),
                        bytes: entry.name.as_bytes().to_vec(),
                    },
                    file_id: entry.file_id.into_bytes().to_vec(),
                    kind: file_kind(entry.kind),
                })
                .collect(),
            has_more: value.has_more,
        }
    }

    fn browser_workspace_extent_plan(value: WorkspaceExtentPlan) -> BrowserWorkspaceExtentPlan {
        BrowserWorkspaceExtentPlan {
            spans: value
                .spans
                .into_iter()
                .map(|span| BrowserWorkspaceExtentSpan {
                    offset: span.offset,
                    length: span.length,
                    source_end: span.source_end,
                    kind: match span.kind {
                        WorkspaceExtentKind::Hole => "hole",
                        WorkspaceExtentKind::AllocatedZero => "allocated-zero",
                        WorkspaceExtentKind::Content => "content",
                    },
                })
                .collect(),
        }
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    #[allow(clippy::struct_excessive_bools)]
    struct Capabilities {
        version: &'static str,
        platform: &'static str,
        architecture: &'static str,
        authority: &'static str,
        immutable_objects: &'static str,
        native_mount: &'static str,
        writable_native_mount: bool,
        native_watch: bool,
        native_watch_backend: String,
        native_watch_persistent_restart: bool,
        native_watch_root_identity_fencing: bool,
        provider_process_io_observable: bool,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct BrowserObjectCacheStats {
        hits: String,
        decoded_hits: String,
        misses: String,
        coalesced_reads: String,
        evictions: String,
        resident_entries: String,
        resident_bytes: String,
        resident_canonical_objects: String,
        resident_canonical_bytes: String,
        resident_decoded_pages: String,
        resident_decoded_bytes: String,
        in_flight: String,
    }

    #[derive(Deserialize, Serialize)]
    #[serde(rename_all = "camelCase")]
    struct BrowserSpeculationOptions {
        residency: BrowserResidencySpeculationOptions,
        promotion: BrowserPromotionSpeculationOptions,
    }

    #[derive(Deserialize, Serialize)]
    #[serde(rename_all = "camelCase")]
    struct BrowserResidencySpeculationOptions {
        maximum_active_operations: u32,
        maximum_active_bytes: u64,
        outcome_window: u32,
        traffic_window: u32,
        speculative_cost_basis_points: u16,
        minimum_usefulness_samples: u32,
        minimum_usefulness_basis_points: u16,
    }

    #[derive(Deserialize, Serialize)]
    #[serde(rename_all = "camelCase")]
    struct BrowserPromotionSpeculationOptions {
        maximum_active_operations: u32,
        maximum_active_bytes: u64,
        maximum_active_cost_units: u64,
        maximum_residency_facts: u32,
        maximum_destinations: u32,
        maximum_accepted_tiers: u32,
        outcome_window: u32,
        minimum_usefulness_samples: u32,
        minimum_usefulness_basis_points: u16,
    }

    #[derive(Deserialize, Serialize)]
    #[serde(rename_all = "camelCase")]
    struct BrowserResidencyObservation {
        operation_id: Vec<u8>,
        volume_id: Vec<u8>,
        generation_id: Vec<u8>,
        foreground_bytes: u64,
        object_id: Vec<u8>,
        maximum_bytes: u64,
        reason: String,
    }

    #[derive(Deserialize, Serialize)]
    #[serde(rename_all = "camelCase")]
    struct BrowserPromotionRequest {
        operation_id: Vec<u8>,
        accepted_tiers: Vec<String>,
        residency: Vec<BrowserObjectResidency>,
        destinations: Vec<BrowserPromotionDestination>,
    }

    #[derive(Deserialize, Serialize)]
    #[serde(rename_all = "camelCase")]
    struct BrowserObjectResidency {
        object_id: Vec<u8>,
        location_id: Vec<u8>,
        tier: String,
        source_priority: u16,
    }

    #[derive(Deserialize, Serialize)]
    #[serde(rename_all = "camelCase")]
    struct BrowserPromotionDestination {
        location_id: Vec<u8>,
        tier: String,
        writable: bool,
        maximum_object_bytes: u64,
        priority: u16,
        cost_units_per_byte: u64,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct BrowserAdmissionResult {
        status: &'static str,
        rejection: Option<&'static str>,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct BrowserResidencyExecution {
        object_bytes: String,
        work: acyclic_fs::WorkCounters,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct BrowserPromotionAdmission {
        status: &'static str,
        rejection: Option<&'static str>,
        operation_id: Option<Vec<u8>>,
        object_id: Option<Vec<u8>>,
        source_location_id: Option<Vec<u8>>,
        destination_location_id: Option<Vec<u8>>,
        estimated_cost_units: Option<String>,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct BrowserSpeculationPreemption {
        residency_operation_ids: Vec<Vec<u8>>,
        promotion_operation_ids: Vec<Vec<u8>>,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct BrowserResidencyMetrics {
        candidates: String,
        admitted: String,
        active: String,
        active_bytes: String,
        useful: String,
        wasted: String,
        rejected_fence: String,
        rejected_duplicate: String,
        rejected_capacity: String,
        rejected_cost: String,
        rejected_usefulness: String,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct BrowserPromotionMetrics {
        candidates: String,
        satisfied: String,
        planned: String,
        active: String,
        active_bytes: String,
        active_cost_units: String,
        useful: String,
        wasted: String,
        rejected: String,
    }

    #[derive(Serialize)]
    struct BrowserSpeculationMetrics {
        residency: BrowserResidencyMetrics,
        promotion: BrowserPromotionMetrics,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct BrowserOptions {
        database_name: String,
        maximum_object_bytes: u64,
        object_acceleration: Acceleration,
        object_cache: BrowserObjectCacheOptions,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "kebab-case")]
    enum Acceleration {
        Indexeddb,
        OpfsRequired,
        OpfsIfAvailable,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct MemoryOptions {
        maximum_object_bytes: u64,
        object_cache: BrowserObjectCacheOptions,
    }

    #[derive(Deserialize)]
    struct BrowserObjectCacheOptions {
        #[serde(rename = "maximumEntries")]
        entries: u32,
        #[serde(rename = "maximumBytes")]
        bytes: u64,
        #[serde(rename = "maximumInFlight")]
        in_flight: u32,
        #[serde(rename = "maximumWaitersPerObject")]
        waiters_per_object: u32,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct VolumeOptions {
        profile: Profile,
        concurrency: Concurrency,
        lifecycle: VolumeLifecycle,
        case_sensitivity: NameCase,
        unicode: Unicode,
        symbolic_links: bool,
        hard_links: bool,
        sparse_files: bool,
        limits: BrowserVolumeLimits,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "kebab-case")]
    enum Profile {
        Portable,
        Posix,
        Windows,
        Browser,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "kebab-case")]
    enum Concurrency {
        ExclusiveWriter,
        Optimistic,
        SerializedAuthority,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "kebab-case")]
    enum VolumeLifecycle {
        Ephemeral,
        Durable,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "kebab-case")]
    enum NameCase {
        Sensitive,
        ProfileFolded,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "kebab-case")]
    enum Unicode {
        Preserve,
        RequireNfc,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    #[allow(clippy::struct_field_names)]
    struct BrowserVolumeLimits {
        maximum_path_bytes: u32,
        maximum_component_bytes: u32,
        maximum_path_depth: u16,
        maximum_object_bytes: u64,
        maximum_mutations_per_batch: u32,
        maximum_paths_per_batch: u32,
        maximum_checkout_dependencies: u32,
        maximum_directory_page_entries: u32,
        maximum_page_height: u16,
        maximum_read_bytes: u64,
        maximum_files_per_generation: u64,
        maximum_objects_per_generation: u64,
        maximum_generation_bytes: u64,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct CheckoutOptions {
        access: CheckoutAccess,
        consistency: Consistency,
        mutation_mode: CheckoutMutationMode,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "kebab-case")]
    enum CheckoutAccess {
        ReadOnly,
        ReadWrite,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "kebab-case")]
    enum Consistency {
        Pinned,
        TrackingSafe,
        Live,
        Manual,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "kebab-case")]
    enum CheckoutMutationMode {
        None,
        PrivateCow,
        DirectLive,
    }

    #[derive(Deserialize)]
    #[serde(
        tag = "kind",
        rename_all = "kebab-case",
        rename_all_fields = "camelCase"
    )]
    enum TransactionOperation {
        CreateFile {
            path: String,
            bytes: serde_bytes::ByteBuf,
        },
        CreateDirectory {
            path: String,
        },
        CreateSymbolicLink {
            path: String,
            target: serde_bytes::ByteBuf,
        },
        CreateSpecial {
            path: String,
            file_kind: String,
        },
        CreateDevice {
            path: String,
            file_kind: String,
            major: u32,
            minor: u32,
        },
        CreateReparsePoint {
            path: String,
            payload: serde_bytes::ByteBuf,
        },
        Remove {
            path: String,
            expected_file_id: Option<serde_bytes::ByteBuf>,
        },
        Rename {
            source: String,
            destination: String,
            replace: bool,
        },
        HardLink {
            source: String,
            destination: String,
        },
        Write {
            path: String,
            offset: u64,
            bytes: serde_bytes::ByteBuf,
        },
        SetMetadata {
            path: String,
            canonical_bytes: serde_bytes::ByteBuf,
        },
        Resize {
            path: String,
            logical_bytes: u64,
        },
        ZeroRange {
            path: String,
            offset: u64,
            length: u64,
            allocated: bool,
            extend: bool,
        },
        Preallocate {
            path: String,
            offset: u64,
            length: u64,
            keep_size: bool,
        },
        CloneRange {
            source: String,
            source_offset: u64,
            destination: String,
            destination_offset: u64,
            length: u64,
        },
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct LookupResult {
        exists: bool,
        file_id: Option<Vec<u8>>,
        file_kind: Option<&'static str>,
        resolved_components: u16,
        work: acyclic_fs::WorkCounters,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct BatchLookupEntryResult {
        exists: bool,
        file_id: Option<Vec<u8>>,
        file_kind: Option<&'static str>,
        resolved_components: u16,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct BatchLookupResult {
        entries: Vec<BatchLookupEntryResult>,
        retained_allocation_bytes: String,
        work: acyclic_fs::WorkCounters,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct StatResult {
        exists: bool,
        record: Option<FileRecordResult>,
        metadata_canonical_bytes: Option<Vec<u8>>,
        work: acyclic_fs::WorkCounters,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct FileReadResult {
        bytes: Vec<u8>,
        work: acyclic_fs::WorkCounters,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct ExtentSeekResult {
        offset: Option<String>,
        work: acyclic_fs::WorkCounters,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct ExtentSpanResult {
        kind: &'static str,
        offset: String,
        length: String,
        source_end: String,
        object_id: Option<Vec<u8>>,
        object_offset: Option<String>,
    }

    #[derive(Serialize)]
    #[serde(
        tag = "kind",
        rename_all = "kebab-case",
        rename_all_fields = "camelCase"
    )]
    enum ExtentPlanResult {
        Inline {
            work: acyclic_fs::WorkCounters,
        },
        Sparse {
            spans: Vec<ExtentSpanResult>,
            retained_allocation_bytes: String,
            work: acyclic_fs::WorkCounters,
        },
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct DirectoryEntryResult {
        name: Vec<u8>,
        file_id: Vec<u8>,
        file_kind: &'static str,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct DirectoryPageResult {
        entries: Vec<DirectoryEntryResult>,
        has_more: bool,
        work: acyclic_fs::WorkCounters,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct DirectoryRecordEntryResult {
        name: Vec<u8>,
        record: FileRecordResult,
        metadata_canonical_bytes: Vec<u8>,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct DirectoryRecordPageResult {
        entries: Vec<DirectoryRecordEntryResult>,
        has_more: bool,
        work: acyclic_fs::WorkCounters,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct FileRecordReadResult {
        record: FileRecordResult,
        work: acyclic_fs::WorkCounters,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct MutationResult {
        file_id: Option<Vec<u8>>,
        work: acyclic_fs::WorkCounters,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct TransactionResult {
        created_file_ids: Vec<Option<Vec<u8>>>,
        work: acyclic_fs::WorkCounters,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct CheckpointResult {
        generation_id: Vec<u8>,
        work: acyclic_fs::WorkCounters,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct CommitResult {
        status: &'static str,
        generation_id: Option<Vec<u8>>,
        epoch: Option<String>,
        sequence: Option<String>,
        committed_fingerprint: Option<Vec<u8>>,
        work: acyclic_fs::WorkCounters,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct RebaseResult {
        status: &'static str,
        generation_id: Option<Vec<u8>>,
        conflict_count: u32,
        truncated: bool,
        work: acyclic_fs::WorkCounters,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct LiveMutationResult {
        status: &'static str,
        generation_id: Option<Vec<u8>>,
        epoch: Option<String>,
        sequence: Option<String>,
        conflict_count: u32,
        truncated: bool,
        committed_fingerprint: Option<Vec<u8>>,
        work: acyclic_fs::WorkCounters,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct ExportManifestResult {
        manifest_bytes: Vec<u8>,
        objects: Vec<Vec<u8>>,
        work: acyclic_fs::WorkCounters,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct FileRecordResult {
        file_id: Vec<u8>,
        file_kind: &'static str,
        link_count: String,
        metadata_object: Vec<u8>,
        payload_kind: &'static str,
        logical_bytes: Option<String>,
        payload_object: Option<Vec<u8>>,
        inline_bytes: Option<Vec<u8>>,
        device_major: Option<u32>,
        device_minor: Option<u32>,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct MetadataResult {
        canonical_bytes: Vec<u8>,
        work: acyclic_fs::WorkCounters,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct NamedAttributeResult {
        exists: bool,
        bytes: Option<Vec<u8>>,
        work: acyclic_fs::WorkCounters,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct NamedAttributePageResult {
        entries: Vec<NamedAttributeNameResult>,
        has_more: bool,
        work: acyclic_fs::WorkCounters,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct NamedAttributeNameResult {
        attribute_class: &'static str,
        name: Vec<u8>,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct FileRecordChangeResult {
        file_id: Vec<u8>,
        before: Option<FileRecordResult>,
        after: Option<FileRecordResult>,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct NameComponentResult {
        encoding: &'static str,
        bytes: Vec<u8>,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct TreeEntryResult {
        name: NameComponentResult,
        file_id: Vec<u8>,
        file_kind: &'static str,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct BindingChangeResult {
        directory_id: Vec<u8>,
        name: NameComponentResult,
        before: Option<TreeEntryResult>,
        after: Option<TreeEntryResult>,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct GenerationDiffResult {
        files: Vec<FileRecordChangeResult>,
        bindings: Vec<BindingChangeResult>,
        truncated: bool,
        work: acyclic_fs::WorkCounters,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct MergeConflictResult {
        kind: &'static str,
        file_id: Option<Vec<u8>>,
        directory_id: Option<Vec<u8>>,
        name: Option<NameComponentResult>,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct MergePreparationResult {
        status: &'static str,
        generation_id: Option<Vec<u8>>,
        conflicts: Vec<MergeConflictResult>,
        truncated: bool,
        work: acyclic_fs::WorkCounters,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct ImportManifest {
        manifest_bytes: Vec<u8>,
        objects: Vec<Vec<u8>>,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct GenerationTransferBatchResult {
        first_object: String,
        next_object: Option<String>,
        objects: Vec<Vec<u8>>,
        work: acyclic_fs::WorkCounters,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct GenerationTransferCursorResult {
        next_object: String,
        work: acyclic_fs::WorkCounters,
    }

    #[wasm_bindgen]
    impl BrowserFs {
        /// Exact backend facts selected during open.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error when capability serialization fails.
        #[wasm_bindgen(getter)]
        pub fn capabilities(&self) -> Result<JsValue, JsValue> {
            serde_wasm_bindgen::to_value(&self.capabilities).map_err(js_error)
        }

        /// Exact process-local immutable-object accelerator telemetry.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error after close, poisoned cache state, or
        /// serialization failure.
        #[wasm_bindgen(js_name = objectCacheStats)]
        pub fn object_cache_stats(&self) -> Result<JsValue, JsValue> {
            let stats = match self.engine.as_ref().ok_or_else(closed_error)? {
                BrowserEngine::IndexedDb(fs) => fs.object_cache_stats(),
                BrowserEngine::IndexedDbOpfs(fs) => fs.object_cache_stats(),
                BrowserEngine::Memory(fs) => fs.object_cache_stats(),
            }
            .map_err(js_error)?;
            serde_wasm_bindgen::to_value(&BrowserObjectCacheStats {
                hits: stats.hits.to_string(),
                decoded_hits: stats.decoded_hits.to_string(),
                misses: stats.misses.to_string(),
                coalesced_reads: stats.coalesced_reads.to_string(),
                evictions: stats.evictions.to_string(),
                resident_entries: stats.resident_entries.to_string(),
                resident_bytes: stats.resident_bytes.to_string(),
                resident_canonical_objects: stats.resident_canonical_objects.to_string(),
                resident_canonical_bytes: stats.resident_canonical_bytes.to_string(),
                resident_decoded_pages: stats.resident_decoded_pages.to_string(),
                resident_decoded_bytes: stats.resident_decoded_bytes.to_string(),
                in_flight: stats.in_flight.to_string(),
            })
            .map_err(js_error)
        }

        /// Discards resident acceleration without changing persistent state.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error after close or poisoned cache state.
        #[wasm_bindgen(js_name = clearObjectCache)]
        pub fn clear_object_cache(&self) -> Result<(), JsValue> {
            match self.engine.as_ref().ok_or_else(closed_error)? {
                BrowserEngine::IndexedDb(fs) => fs.clear_object_cache(),
                BrowserEngine::IndexedDbOpfs(fs) => fs.clear_object_cache(),
                BrowserEngine::Memory(fs) => fs.clear_object_cache(),
            }
            .map_err(js_error)
        }

        /// Creates both generation-fenced speculation engines over this
        /// browser filesystem's authenticated object backend and shared cache.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error after close, for malformed identities or
        /// options, or when either engine rejects its hard policy.
        #[wasm_bindgen(js_name = createSpeculation)]
        pub fn create_speculation(
            &self,
            volume_id: Vec<u8>,
            generation_id: Vec<u8>,
            options: JsValue,
        ) -> Result<BrowserSpeculation, JsValue> {
            let options: BrowserSpeculationOptions =
                serde_wasm_bindgen::from_value(options).map_err(js_error)?;
            let controller = SpeculationController::new(
                browser_speculation_options(&options),
                VolumeId::from_bytes(fixed_16_owned(volume_id)?),
                acyclic_fs::GenerationId::new(Digest::from_bytes(fixed_32_owned(
                    generation_id,
                    "generation identity",
                )?)),
            )
            .map_err(js_error)?;
            Ok(BrowserSpeculation {
                engine: self.engine.as_ref().ok_or_else(closed_error)?.clone(),
                controller: std::cell::RefCell::new(controller),
                cancellation: CancellationToken::new(),
            })
        }

        /// Releases browser handles. Durable state remains in `IndexedDB`.
        pub fn close(&mut self) {
            if let Some(engine) = self.engine.take() {
                match engine {
                    BrowserEngine::IndexedDb(fs) => drop(fs),
                    BrowserEngine::IndexedDbOpfs(fs) => drop(fs),
                    BrowserEngine::Memory(fs) => drop(fs),
                }
            }
        }

        /// Creates or idempotently reopens one named workspace.
        ///
        /// # Errors
        ///
        /// Returns closed-engine, invalid-name, authority, or storage failures.
        #[wasm_bindgen(js_name = createWorkspace)]
        pub async fn create_workspace(&self, name: String) -> Result<BrowserWorkspace, JsValue> {
            let engine = match self.engine.as_ref().ok_or_else(closed_error)? {
                BrowserEngine::IndexedDb(fs) => BrowserWorkspaceEngine::IndexedDb(
                    fs.create_workspace(name).await.map_err(js_error)?,
                ),
                BrowserEngine::IndexedDbOpfs(fs) => BrowserWorkspaceEngine::IndexedDbOpfs(
                    fs.create_workspace(name).await.map_err(js_error)?,
                ),
                BrowserEngine::Memory(fs) => BrowserWorkspaceEngine::Memory(
                    fs.create_workspace(name).await.map_err(js_error)?,
                ),
            };
            Ok(BrowserWorkspace { engine })
        }

        /// Opens one existing named workspace.
        ///
        /// # Errors
        ///
        /// Returns closed-engine, invalid-name, absence, authority, or storage failures.
        #[wasm_bindgen(js_name = openWorkspace)]
        pub async fn open_workspace(&self, name: String) -> Result<BrowserWorkspace, JsValue> {
            let engine = match self.engine.as_ref().ok_or_else(closed_error)? {
                BrowserEngine::IndexedDb(fs) => BrowserWorkspaceEngine::IndexedDb(
                    fs.open_workspace(name).await.map_err(js_error)?,
                ),
                BrowserEngine::IndexedDbOpfs(fs) => BrowserWorkspaceEngine::IndexedDbOpfs(
                    fs.open_workspace(name).await.map_err(js_error)?,
                ),
                BrowserEngine::Memory(fs) => {
                    BrowserWorkspaceEngine::Memory(fs.open_workspace(name).await.map_err(js_error)?)
                }
            };
            Ok(BrowserWorkspace { engine })
        }

        /// Creates one independently configured volume.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for unsupported durability, storage,
        /// authentication, cancellation, or bounded-work failure.
        #[wasm_bindgen(js_name = createVolume)]
        pub async fn create_volume(&self, options: JsValue) -> Result<BrowserVolume, JsValue> {
            self.create_volume_internal(None, options).await
        }

        /// Idempotently creates one caller-selected volume identity.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed identity, incompatible
        /// existing configuration, unsupported semantics, or storage failure.
        #[wasm_bindgen(js_name = createVolumeWithId)]
        pub async fn create_volume_with_id(
            &self,
            volume_id: Vec<u8>,
            options: JsValue,
        ) -> Result<BrowserVolume, JsValue> {
            self.create_volume_internal(Some(VolumeId::from_bytes(fixed_16(&volume_id)?)), options)
                .await
        }

        async fn create_volume_internal(
            &self,
            volume_id: Option<VolumeId>,
            options: JsValue,
        ) -> Result<BrowserVolume, JsValue> {
            let options: VolumeOptions =
                serde_wasm_bindgen::from_value(options).map_err(js_error)?;
            let config = browser_volume_config(options).map_err(js_error)?;
            let cancellation = CancellationToken::default();
            let (engine, acquisition_work) = match self
                .engine
                .as_ref()
                .ok_or_else(|| JsValue::from_str("browser filesystem is closed"))?
            {
                BrowserEngine::IndexedDb(fs) => {
                    let receipt = match volume_id {
                        Some(volume_id) => {
                            fs.create_volume_with_id(
                                volume_id,
                                config,
                                boundary_budget(),
                                &cancellation,
                            )
                            .await
                        }
                        None => {
                            fs.create_volume(config, boundary_budget(), &cancellation)
                                .await
                        }
                    }
                    .map_err(js_error)?;
                    (BrowserVolumeEngine::IndexedDb(receipt.value), receipt.work)
                }
                BrowserEngine::IndexedDbOpfs(fs) => {
                    let receipt = match volume_id {
                        Some(volume_id) => {
                            fs.create_volume_with_id(
                                volume_id,
                                config,
                                boundary_budget(),
                                &cancellation,
                            )
                            .await
                        }
                        None => {
                            fs.create_volume(config, boundary_budget(), &cancellation)
                                .await
                        }
                    }
                    .map_err(js_error)?;
                    (
                        BrowserVolumeEngine::IndexedDbOpfs(receipt.value),
                        receipt.work,
                    )
                }
                BrowserEngine::Memory(fs) => {
                    let receipt = match volume_id {
                        Some(volume_id) => {
                            fs.create_volume_with_id(
                                volume_id,
                                config,
                                boundary_budget(),
                                &cancellation,
                            )
                            .await
                        }
                        None => {
                            fs.create_volume(config, boundary_budget(), &cancellation)
                                .await
                        }
                    }
                    .map_err(js_error)?;
                    (BrowserVolumeEngine::Memory(receipt.value), receipt.work)
                }
            };
            Ok(BrowserVolume {
                engine,
                limits: config.limits,
                acquisition_work,
            })
        }

        /// Opens one previously created or restored persistent volume.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed identity, authenticated
        /// absence, storage corruption, cancellation, or bounded work.
        #[wasm_bindgen(js_name = openVolume)]
        pub async fn open_volume(&self, volume_id: Vec<u8>) -> Result<BrowserVolume, JsValue> {
            let volume_id = VolumeId::from_bytes(fixed_16(&volume_id)?);
            let cancellation = CancellationToken::default();
            let (engine, acquisition_work) = match self.engine.as_ref().ok_or_else(closed_error)? {
                BrowserEngine::IndexedDb(fs) => {
                    let receipt = fs
                        .open_volume(volume_id, boundary_budget(), &cancellation)
                        .await
                        .map_err(js_error)?;
                    (BrowserVolumeEngine::IndexedDb(receipt.value), receipt.work)
                }
                BrowserEngine::IndexedDbOpfs(fs) => {
                    let receipt = fs
                        .open_volume(volume_id, boundary_budget(), &cancellation)
                        .await
                        .map_err(js_error)?;
                    (
                        BrowserVolumeEngine::IndexedDbOpfs(receipt.value),
                        receipt.work,
                    )
                }
                BrowserEngine::Memory(fs) => {
                    let receipt = fs
                        .open_volume(volume_id, boundary_budget(), &cancellation)
                        .await
                        .map_err(js_error)?;
                    (BrowserVolumeEngine::Memory(receipt.value), receipt.work)
                }
            };
            let config = match &engine {
                BrowserVolumeEngine::IndexedDb(volume) => volume.config(),
                BrowserVolumeEngine::IndexedDbOpfs(volume) => volume.config(),
                BrowserVolumeEngine::Memory(volume) => volume.config(),
            };
            Ok(BrowserVolume {
                engine,
                limits: config.limits,
                acquisition_work,
            })
        }

        /// Exports one exact authenticated immutable object for resumable transfer.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed identity, absence,
        /// corruption, cancellation, storage, or bounded work.
        #[wasm_bindgen(js_name = exportObject)]
        pub async fn export_object(
            &self,
            object_id: Vec<u8>,
            maximum_bytes: u64,
        ) -> Result<JsValue, JsValue> {
            let object_id = decode_object_id(&object_id)?;
            let cancellation = CancellationToken::default();
            let receipt = match self.engine.as_ref().ok_or_else(closed_error)? {
                BrowserEngine::IndexedDb(fs) => fs
                    .export_object(object_id, maximum_bytes, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
                BrowserEngine::IndexedDbOpfs(fs) => fs
                    .export_object(object_id, maximum_bytes, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
                BrowserEngine::Memory(fs) => fs
                    .export_object(object_id, maximum_bytes, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
            };
            serde_wasm_bindgen::to_value(&FileReadResult {
                bytes: receipt.value.bytes.to_vec(),
                work: receipt.work,
            })
            .map_err(js_error)
        }

        /// Idempotently imports one immutable object under its authenticated identity.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed identity, digest mismatch,
        /// cancellation, storage, or bounded work.
        #[wasm_bindgen(js_name = importObject)]
        pub async fn import_object(
            &self,
            object_id: Vec<u8>,
            bytes: Vec<u8>,
        ) -> Result<JsValue, JsValue> {
            let object_id = decode_object_id(&object_id)?;
            let bytes = bytes::Bytes::from(bytes);
            let cancellation = CancellationToken::default();
            let receipt = match self.engine.as_ref().ok_or_else(closed_error)? {
                BrowserEngine::IndexedDb(fs) => fs
                    .import_object(object_id, bytes, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
                BrowserEngine::IndexedDbOpfs(fs) => fs
                    .import_object(object_id, bytes, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
                BrowserEngine::Memory(fs) => fs
                    .import_object(object_id, bytes, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
            };
            mutation_value(receipt.work)
        }

        /// Exports one bounded manifest-ordered immutable-object page.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed manifests, invalid cursors,
        /// cancellation, storage, allocation, or bounded-work failures.
        #[wasm_bindgen(js_name = exportGenerationBatch)]
        pub async fn export_generation_batch(
            &self,
            manifest: JsValue,
            cursor: u64,
            maximum_objects: u32,
            maximum_object_bytes: u64,
        ) -> Result<JsValue, JsValue> {
            let manifest: ImportManifest =
                serde_wasm_bindgen::from_value(manifest).map_err(js_error)?;
            let manifest = decode_export_manifest(&manifest)?;
            let cancellation = CancellationToken::default();
            let receipt = match self.engine.as_ref().ok_or_else(closed_error)? {
                BrowserEngine::IndexedDb(fs) => fs
                    .export_generation_batch(
                        &manifest,
                        TransferCursor::new(cursor),
                        maximum_objects,
                        maximum_object_bytes,
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?,
                BrowserEngine::IndexedDbOpfs(fs) => fs
                    .export_generation_batch(
                        &manifest,
                        TransferCursor::new(cursor),
                        maximum_objects,
                        maximum_object_bytes,
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?,
                BrowserEngine::Memory(fs) => fs
                    .export_generation_batch(
                        &manifest,
                        TransferCursor::new(cursor),
                        maximum_objects,
                        maximum_object_bytes,
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?,
            };
            serde_wasm_bindgen::to_value(&GenerationTransferBatchResult {
                first_object: receipt.value.first_object.next_object().to_string(),
                next_object: receipt
                    .value
                    .next
                    .map(|next| next.next_object().to_string()),
                objects: receipt
                    .value
                    .objects
                    .into_iter()
                    .map(|object| object.bytes.to_vec())
                    .collect(),
                work: receipt.work,
            })
            .map_err(js_error)
        }

        /// Idempotently imports one manifest-aligned immutable-object page.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed manifests, cursor/body
        /// bounds, cancellation, storage, or bounded-work failures.
        #[wasm_bindgen(js_name = importGenerationBatch)]
        pub async fn import_generation_batch(
            &self,
            manifest: JsValue,
            cursor: u64,
            objects: JsValue,
            maximum_objects: u32,
        ) -> Result<JsValue, JsValue> {
            let manifest: ImportManifest =
                serde_wasm_bindgen::from_value(manifest).map_err(js_error)?;
            let manifest = decode_export_manifest(&manifest)?;
            let objects = serde_wasm_bindgen::from_value::<Vec<Vec<u8>>>(objects)
                .map_err(js_error)?
                .into_iter()
                .map(bytes::Bytes::from)
                .collect::<Vec<_>>();
            let cancellation = CancellationToken::default();
            let receipt = match self.engine.as_ref().ok_or_else(closed_error)? {
                BrowserEngine::IndexedDb(fs) => fs
                    .import_generation_batch(
                        &manifest,
                        TransferCursor::new(cursor),
                        &objects,
                        maximum_objects,
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?,
                BrowserEngine::IndexedDbOpfs(fs) => fs
                    .import_generation_batch(
                        &manifest,
                        TransferCursor::new(cursor),
                        &objects,
                        maximum_objects,
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?,
                BrowserEngine::Memory(fs) => fs
                    .import_generation_batch(
                        &manifest,
                        TransferCursor::new(cursor),
                        &objects,
                        maximum_objects,
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?,
            };
            serde_wasm_bindgen::to_value(&GenerationTransferCursorResult {
                next_object: receipt.value.next_object().to_string(),
                work: receipt.work,
            })
            .map_err(js_error)
        }

        /// Restores authority only after authenticating a complete imported closure.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed manifest, incomplete or
        /// corrupt closure, conflicting authority, cancellation, or bounded work.
        #[wasm_bindgen(js_name = restoreVolume)]
        pub async fn restore_volume(&self, manifest: JsValue) -> Result<BrowserVolume, JsValue> {
            let manifest: ImportManifest =
                serde_wasm_bindgen::from_value(manifest).map_err(js_error)?;
            let manifest = decode_export_manifest(&manifest)?;
            let config = manifest.config;
            let cancellation = CancellationToken::default();
            let (engine, acquisition_work) = match self.engine.as_ref().ok_or_else(closed_error)? {
                BrowserEngine::IndexedDb(fs) => {
                    let receipt = fs
                        .restore_volume(&manifest, boundary_budget(), &cancellation)
                        .await
                        .map_err(js_error)?;
                    (BrowserVolumeEngine::IndexedDb(receipt.value), receipt.work)
                }
                BrowserEngine::IndexedDbOpfs(fs) => {
                    let receipt = fs
                        .restore_volume(&manifest, boundary_budget(), &cancellation)
                        .await
                        .map_err(js_error)?;
                    (
                        BrowserVolumeEngine::IndexedDbOpfs(receipt.value),
                        receipt.work,
                    )
                }
                BrowserEngine::Memory(fs) => {
                    let receipt = fs
                        .restore_volume(&manifest, boundary_budget(), &cancellation)
                        .await
                        .map_err(js_error)?;
                    (BrowserVolumeEngine::Memory(receipt.value), receipt.work)
                }
            };
            Ok(BrowserVolume {
                engine,
                limits: config.limits,
                acquisition_work,
            })
        }
    }

    #[wasm_bindgen]
    impl BrowserSpeculation {
        /// Records foreground demand and admits one authenticated successor.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed input or a failed bounded transition.
        pub fn observe(&self, observation: JsValue) -> Result<JsValue, JsValue> {
            let observation: BrowserResidencyObservation =
                serde_wasm_bindgen::from_value(observation).map_err(js_error)?;
            let admission = self
                .controller
                .try_borrow_mut()
                .map_err(|_| speculation_busy())?
                .observe_hint(
                    OperationId::from_bytes(fixed_16(&observation.operation_id)?),
                    VolumeId::from_bytes(fixed_16(&observation.volume_id)?),
                    acyclic_fs::GenerationId::new(Digest::from_bytes(fixed_32(
                        &observation.generation_id,
                        "generation identity",
                    )?)),
                    observation.foreground_bytes,
                    ResidencyHint {
                        request: ObjectReadRequest {
                            object_id: decode_object_id(&observation.object_id)?,
                            maximum_bytes: observation.maximum_bytes,
                        },
                        reason: browser_residency_reason(&observation.reason)?,
                    },
                )
                .map_err(js_error)?;
            let result = match admission {
                ResidencyAdmission::Admitted(_) => BrowserAdmissionResult {
                    status: "admitted",
                    rejection: None,
                },
                ResidencyAdmission::Rejected(rejection) => BrowserAdmissionResult {
                    status: "rejected",
                    rejection: Some(browser_residency_rejection(rejection)),
                },
            };
            serde_wasm_bindgen::to_value(&result).map_err(js_error)
        }

        /// Executes one admitted residency prediction through the browser's
        /// authenticated object backend and shared cache.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for an inactive operation, storage or
        /// authentication failure, bounded-work exhaustion, or cancellation.
        #[wasm_bindgen(js_name = executeResidency)]
        pub async fn execute_residency(&self, operation_id: Vec<u8>) -> Result<JsValue, JsValue> {
            let operation_id = OperationId::from_bytes(fixed_16(&operation_id)?);
            let controller = self
                .controller
                .try_borrow()
                .map_err(|_| speculation_busy())?
                .clone();
            let permit = controller
                .active_residency_permit(operation_id)
                .ok_or_else(|| JsValue::from_str("residency operation is not active"))?;
            let receipt = match &self.engine {
                BrowserEngine::IndexedDb(fs) => {
                    fs.execute_residency(
                        controller.residency(),
                        permit,
                        boundary_budget(),
                        &self.cancellation,
                    )
                    .await
                }
                BrowserEngine::IndexedDbOpfs(fs) => {
                    fs.execute_residency(
                        controller.residency(),
                        permit,
                        boundary_budget(),
                        &self.cancellation,
                    )
                    .await
                }
                BrowserEngine::Memory(fs) => {
                    fs.execute_residency(
                        controller.residency(),
                        permit,
                        boundary_budget(),
                        &self.cancellation,
                    )
                    .await
                }
            }
            .map_err(js_error)?;
            serde_wasm_bindgen::to_value(&BrowserResidencyExecution {
                object_bytes: receipt.value.to_string(),
                work: receipt.work,
            })
            .map_err(js_error)
        }

        /// Records terminal usefulness for one residency operation.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed or inactive operation identities.
        #[wasm_bindgen(js_name = finishResidency)]
        pub fn finish_residency(&self, operation_id: Vec<u8>, useful: bool) -> Result<(), JsValue> {
            self.controller
                .try_borrow_mut()
                .map_err(|_| speculation_busy())?
                .finish_residency(
                    OperationId::from_bytes(fixed_16_owned(operation_id)?),
                    useful,
                )
                .map_err(js_error)
        }

        /// Plans one bounded promotion from exact caller-observed location facts.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed facts, unsupported tiers,
        /// inactive residency, or a failed bounded transition.
        #[wasm_bindgen(js_name = planPromotion)]
        pub fn plan_promotion(&self, request: JsValue) -> Result<JsValue, JsValue> {
            let request: BrowserPromotionRequest =
                serde_wasm_bindgen::from_value(request).map_err(js_error)?;
            let operation_id = OperationId::from_bytes(fixed_16(&request.operation_id)?);
            let mut controller = self
                .controller
                .try_borrow_mut()
                .map_err(|_| speculation_busy())?;
            let permit = controller
                .active_residency_permit(operation_id)
                .ok_or_else(|| JsValue::from_str("residency operation is not active"))?;
            let accepted_tiers = request
                .accepted_tiers
                .iter()
                .map(|tier| browser_storage_tier(tier))
                .collect::<Result<Vec<_>, _>>()?;
            let residency = request
                .residency
                .iter()
                .map(browser_object_residency)
                .collect::<Result<Vec<_>, _>>()?;
            let destinations = request
                .destinations
                .iter()
                .map(browser_promotion_destination)
                .collect::<Result<Vec<_>, _>>()?;
            let admission = controller
                .plan_promotion(permit, accepted_tiers, &residency, &destinations)
                .map_err(js_error)?;
            serde_wasm_bindgen::to_value(&browser_promotion_admission(admission)).map_err(js_error)
        }

        /// Records terminal usefulness for one promotion operation.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed or inactive operation identities.
        #[wasm_bindgen(js_name = finishPromotion)]
        pub fn finish_promotion(&self, operation_id: Vec<u8>, useful: bool) -> Result<(), JsValue> {
            self.controller
                .try_borrow_mut()
                .map_err(|_| speculation_busy())?
                .finish_promotion(
                    OperationId::from_bytes(fixed_16_owned(operation_id)?),
                    useful,
                )
                .map_err(js_error)
        }

        /// Atomically preempts both engines before recording foreground bytes.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error if exact bounded accounting fails.
        #[wasm_bindgen(js_name = preemptForForeground)]
        pub fn preempt_for_foreground(&self, bytes: u64) -> Result<JsValue, JsValue> {
            let value = self
                .controller
                .try_borrow_mut()
                .map_err(|_| speculation_busy())?
                .preempt_for_foreground(bytes)
                .map_err(js_error)?;
            serde_wasm_bindgen::to_value(&browser_speculation_preemption(value)).map_err(js_error)
        }

        /// Atomically fences both engines onto a new immutable generation.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for a malformed identity or failed transition.
        #[wasm_bindgen(js_name = replaceGeneration)]
        pub fn replace_generation(&self, generation_id: Vec<u8>) -> Result<JsValue, JsValue> {
            let value = self
                .controller
                .try_borrow_mut()
                .map_err(|_| speculation_busy())?
                .replace_generation(acyclic_fs::GenerationId::new(Digest::from_bytes(
                    fixed_32_owned(generation_id, "generation identity")?,
                )))
                .map_err(js_error)?;
            serde_wasm_bindgen::to_value(&browser_speculation_preemption(value)).map_err(js_error)
        }

        /// Returns exact payload-free metrics for both engines.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error if metrics cannot be serialized.
        pub fn metrics(&self) -> Result<JsValue, JsValue> {
            let metrics = self
                .controller
                .try_borrow()
                .map_err(|_| speculation_busy())?
                .metrics();
            serde_wasm_bindgen::to_value(&BrowserSpeculationMetrics {
                residency: browser_residency_metrics(metrics.residency),
                promotion: browser_promotion_metrics(metrics.promotion),
            })
            .map_err(js_error)
        }

        /// Cooperatively cancels future residency execution from this owner.
        pub fn cancel(&self) {
            self.cancellation.cancel();
        }
    }

    #[wasm_bindgen]
    impl BrowserVolume {
        /// Returns the canonical 16-byte volume identity.
        #[wasm_bindgen(getter)]
        #[must_use]
        pub fn id(&self) -> Vec<u8> {
            match &self.engine {
                BrowserVolumeEngine::IndexedDb(volume) => volume.id().into_bytes().to_vec(),
                BrowserVolumeEngine::IndexedDbOpfs(volume) => volume.id().into_bytes().to_vec(),
                BrowserVolumeEngine::Memory(volume) => volume.id().into_bytes().to_vec(),
            }
        }

        /// Returns exact bounded work used to acquire this volume handle.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error when the bounded work receipt cannot be
        /// serialized for JavaScript.
        #[wasm_bindgen(getter, js_name = acquisitionWork)]
        pub fn acquisition_work(&self) -> Result<JsValue, JsValue> {
            serde_wasm_bindgen::to_value(&self.acquisition_work).map_err(js_error)
        }

        /// Computes one bounded Merkle-aware semantic generation diff.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed identities, corrupt
        /// storage, cancellation, allocation, or bounded work.
        #[wasm_bindgen(js_name = diffGenerations)]
        pub async fn diff_generations(
            &self,
            before: Vec<u8>,
            after: Vec<u8>,
            maximum_changes: u32,
        ) -> Result<JsValue, JsValue> {
            let before = acyclic_fs::GenerationId::new(Digest::from_bytes(fixed_32(
                &before,
                "before generation identity",
            )?));
            let after = acyclic_fs::GenerationId::new(Digest::from_bytes(fixed_32(
                &after,
                "after generation identity",
            )?));
            let cancellation = CancellationToken::default();
            let receipt = match &self.engine {
                BrowserVolumeEngine::IndexedDb(volume) => volume
                    .diff_generations(
                        before,
                        after,
                        maximum_changes,
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?,
                BrowserVolumeEngine::IndexedDbOpfs(volume) => volume
                    .diff_generations(
                        before,
                        after,
                        maximum_changes,
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?,
                BrowserVolumeEngine::Memory(volume) => volume
                    .diff_generations(
                        before,
                        after,
                        maximum_changes,
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?,
            };
            encode_generation_diff(receipt.value, receipt.work)
        }

        /// Opens the volume head with explicit access and consistency semantics.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for an invalid mode or authenticated
        /// storage, cancellation, and bounded-work failures.
        pub async fn checkout(&self, options: JsValue) -> Result<BrowserCheckout, JsValue> {
            let options: CheckoutOptions =
                serde_wasm_bindgen::from_value(options).map_err(js_error)?;
            let mode = checkout_mode(&options);
            let cancellation = CancellationToken::default();
            let (engine, acquisition_work) = match &self.engine {
                BrowserVolumeEngine::IndexedDb(volume) => {
                    let receipt = volume
                        .checkout(
                            GenerationSelector::Head,
                            mode,
                            boundary_budget(),
                            &cancellation,
                        )
                        .await
                        .map_err(js_error)?;
                    (
                        BrowserCheckoutEngine::IndexedDb(receipt.value),
                        receipt.work,
                    )
                }
                BrowserVolumeEngine::IndexedDbOpfs(volume) => {
                    let receipt = volume
                        .checkout(
                            GenerationSelector::Head,
                            mode,
                            boundary_budget(),
                            &cancellation,
                        )
                        .await
                        .map_err(js_error)?;
                    (
                        BrowserCheckoutEngine::IndexedDbOpfs(receipt.value),
                        receipt.work,
                    )
                }
                BrowserVolumeEngine::Memory(volume) => {
                    let receipt = volume
                        .checkout(
                            GenerationSelector::Head,
                            mode,
                            boundary_budget(),
                            &cancellation,
                        )
                        .await
                        .map_err(js_error)?;
                    (BrowserCheckoutEngine::Memory(receipt.value), receipt.work)
                }
            };
            Ok(BrowserCheckout {
                engine,
                limits: self.limits,
                acquisition_work,
            })
        }
    }

    #[wasm_bindgen]
    impl BrowserCheckout {
        /// Returns exact bounded work used to acquire this checkout handle.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error when the bounded work receipt cannot be
        /// serialized for JavaScript.
        #[wasm_bindgen(getter, js_name = acquisitionWork)]
        pub fn acquisition_work(&self) -> Result<JsValue, JsValue> {
            serde_wasm_bindgen::to_value(&self.acquisition_work).map_err(js_error)
        }

        /// Applies one ordered sparse mutation batch atomically within this volume.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed operations, rejected semantics,
        /// cancellation, storage, or bounded-work failure.
        #[wasm_bindgen(js_name = applyTransaction)]
        pub async fn apply_transaction(&mut self, operations: JsValue) -> Result<JsValue, JsValue> {
            let authored = decode_authored_transactions(operations, self.limits)?;
            let cancellation = CancellationToken::default();
            let receipt = with_checkout_mut!(&mut self.engine, checkout, {
                checkout
                    .apply_authored_transaction(authored, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?
            });
            serde_wasm_bindgen::to_value(&TransactionResult {
                created_file_ids: receipt
                    .value
                    .created_file_ids
                    .into_iter()
                    .map(|identity| identity.map(|value| value.into_bytes().to_vec()))
                    .collect(),
                work: receipt.work,
            })
            .map_err(js_error)
        }

        /// Builds an immutable candidate generation without publishing authority.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for invalid checkout state, corruption,
        /// cancellation, storage failure, or bounded-work exhaustion.
        #[wasm_bindgen(js_name = checkpoint)]
        pub async fn checkpoint(&self) -> Result<JsValue, JsValue> {
            let cancellation = CancellationToken::default();
            let receipt = match &self.engine {
                BrowserCheckoutEngine::IndexedDb(checkout) => checkout
                    .checkpoint(boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::IndexedDbOpfs(checkout) => checkout
                    .checkpoint(boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::Memory(checkout) => checkout
                    .checkpoint(boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
            };
            encode_checkpoint_result(&receipt)
        }

        /// Explicitly advances a clean manual checkout to the authority head.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for dirty state, storage, cancellation,
        /// authentication, or bounded-work failure.
        #[wasm_bindgen(js_name = refreshHead)]
        pub async fn refresh_head(&mut self) -> Result<JsValue, JsValue> {
            let cancellation = CancellationToken::default();
            let receipt = match &mut self.engine {
                BrowserCheckoutEngine::IndexedDb(checkout) => checkout
                    .refresh_head(boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::IndexedDbOpfs(checkout) => checkout
                    .refresh_head(boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::Memory(checkout) => checkout
                    .refresh_head(boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
            };
            encode_checkpoint_result(&receipt)
        }

        /// Explicitly performs observation-safe synchronization for a live checkout.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for unsupported mode, conflicts, storage,
        /// cancellation, authentication, or bounded-work failure.
        #[wasm_bindgen(js_name = refreshLive)]
        pub async fn refresh_live(&mut self) -> Result<JsValue, JsValue> {
            let cancellation = CancellationToken::default();
            let receipt = match &mut self.engine {
                BrowserCheckoutEngine::IndexedDb(checkout) => checkout
                    .refresh_live(boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::IndexedDbOpfs(checkout) => checkout
                    .refresh_live(boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::Memory(checkout) => checkout
                    .refresh_live(boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
            };
            encode_checkpoint_result(&receipt)
        }

        /// Builds a deterministic complete manifest for resumable transfer.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for checkpoint, closure, authentication,
        /// cancellation, storage, serialization, or bounded work.
        #[wasm_bindgen(js_name = exportManifest)]
        pub async fn export_manifest(&self) -> Result<JsValue, JsValue> {
            let cancellation = CancellationToken::default();
            let receipt = match &self.engine {
                BrowserCheckoutEngine::IndexedDb(checkout) => checkout
                    .export_manifest(boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::IndexedDbOpfs(checkout) => checkout
                    .export_manifest(boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::Memory(checkout) => checkout
                    .export_manifest(boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
            };
            encode_export_manifest(receipt.value, receipt.work)
        }

        /// Prepares a bounded two-parent merge against the current authority head.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed identity, invalid checkout
        /// state, non-head parent, corruption, cancellation, or bounded work.
        #[wasm_bindgen(js_name = prepareMerge)]
        pub async fn prepare_merge(
            &mut self,
            theirs: Vec<u8>,
            maximum_changes: u32,
            maximum_conflicts: u32,
        ) -> Result<JsValue, JsValue> {
            let theirs = acyclic_fs::GenerationId::new(Digest::from_bytes(fixed_32(
                &theirs,
                "merge generation identity",
            )?));
            let cancellation = CancellationToken::default();
            let receipt = match &mut self.engine {
                BrowserCheckoutEngine::IndexedDb(checkout) => checkout
                    .prepare_merge(
                        theirs,
                        maximum_changes,
                        maximum_conflicts,
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::IndexedDbOpfs(checkout) => checkout
                    .prepare_merge(
                        theirs,
                        maximum_changes,
                        maximum_conflicts,
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::Memory(checkout) => checkout
                    .prepare_merge(
                        theirs,
                        maximum_changes,
                        maximum_conflicts,
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?,
            };
            encode_merge_preparation(receipt.value, receipt.work)
        }
        /// Resolves one canonical absolute path without following links.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed paths or authenticated
        /// storage, cancellation, and bounded-work failures.
        #[wasm_bindgen(js_name = lookupNoFollow)]
        pub async fn lookup_no_follow(&mut self, path: String) -> Result<JsValue, JsValue> {
            let portable = PortablePath::parse(&path, self.limits).map_err(js_error)?;
            let path = NamespacePath::from_portable(&portable, self.limits).map_err(js_error)?;
            let cancellation = CancellationToken::default();
            let receipt = match &mut self.engine {
                BrowserCheckoutEngine::IndexedDb(checkout) => checkout
                    .lookup_no_follow(&path, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::IndexedDbOpfs(checkout) => checkout
                    .lookup_no_follow(&path, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::Memory(checkout) => checkout
                    .lookup_no_follow(&path, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
            };
            let record = receipt.value.record;
            serde_wasm_bindgen::to_value(&LookupResult {
                exists: record.is_some(),
                file_id: record.map(|value| value.file_id.into_bytes().to_vec()),
                file_kind: record.map(|value| file_kind(value.kind)),
                resolved_components: receipt.value.resolved_components,
                work: receipt.work,
            })
            .map_err(js_error)
        }

        /// Resolves a bounded path batch with shared authenticated frontiers.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for non-array/excessive/malformed paths,
        /// storage, cancellation, authentication, or bounded-work failure.
        #[wasm_bindgen(js_name = lookupBatchNoFollow)]
        pub async fn lookup_batch_no_follow(&mut self, paths: JsValue) -> Result<JsValue, JsValue> {
            if !js_sys::Array::is_array(&paths) {
                return Err(js_error("lookup paths must be an array"));
            }
            let maximum = self.limits.maximum_paths_per_batch;
            if js_sys::Array::from(&paths).length() > maximum {
                return Err(js_error("lookup path batch exceeds the configured bound"));
            }
            let decoded: Vec<String> = serde_wasm_bindgen::from_value(paths).map_err(js_error)?;
            if decoded.is_empty()
                || decoded.capacity() > usize::try_from(maximum).unwrap_or(usize::MAX)
            {
                return Err(js_error("lookup path batch is empty or excessive"));
            }
            let paths = decoded
                .iter()
                .map(|path| browser_path(path, self.limits))
                .collect::<Result<Vec<_>, _>>()?;
            let cancellation = CancellationToken::default();
            let receipt = with_checkout_mut!(&mut self.engine, checkout, {
                checkout
                    .lookup_batch_no_follow(&paths, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?
            });
            serde_wasm_bindgen::to_value(&BatchLookupResult {
                entries: receipt
                    .value
                    .entries
                    .into_iter()
                    .map(|entry| BatchLookupEntryResult {
                        exists: entry.record.is_some(),
                        file_id: entry
                            .record
                            .map(|record| record.file_id.into_bytes().to_vec()),
                        file_kind: entry.record.map(|record| file_kind(record.kind)),
                        resolved_components: entry.resolved_components,
                    })
                    .collect(),
                retained_allocation_bytes: receipt.value.retained_allocation_bytes.to_string(),
                work: receipt.work,
            })
            .map_err(js_error)
        }

        /// Returns one complete no-follow file record and canonical metadata.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed paths, storage,
        /// authentication, cancellation, encoding, or bounded-work failure.
        #[wasm_bindgen(js_name = statNoFollow)]
        pub async fn stat_no_follow(&mut self, path: String) -> Result<JsValue, JsValue> {
            let path = browser_path(&path, self.limits)?;
            let cancellation = CancellationToken::default();
            let receipt = with_checkout_mut!(&mut self.engine, checkout, {
                checkout
                    .lookup_no_follow_with_metadata(&path, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?
            });
            let (record, metadata_canonical_bytes) = match receipt.value {
                Some(value) => (
                    Some(encode_file_record(value.record)),
                    Some(encode_file_metadata(value.metadata).map_err(js_error)?),
                ),
                None => (None, None),
            };
            serde_wasm_bindgen::to_value(&StatResult {
                exists: record.is_some(),
                record,
                metadata_canonical_bytes,
                work: receipt.work,
            })
            .map_err(js_error)
        }

        /// Reads one complete candidate file record by stable identity.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed identity, authenticated
        /// absence, storage, cancellation, or bounded-work failure.
        #[wasm_bindgen(js_name = readFileRecordById)]
        pub async fn read_file_record_by_id(
            &mut self,
            file_id: Vec<u8>,
        ) -> Result<JsValue, JsValue> {
            let file_id = FileId::from_bytes(fixed_16(&file_id)?);
            let cancellation = CancellationToken::default();
            let receipt = with_checkout_mut!(&mut self.engine, checkout, {
                checkout
                    .read_file_record_by_id(file_id, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?
            });
            serde_wasm_bindgen::to_value(&FileRecordReadResult {
                record: encode_file_record(receipt.value),
                work: receipt.work,
            })
            .map_err(js_error)
        }

        /// Reads complete canonical metadata bytes for one path.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for path, storage, codec, cancellation, or work failure.
        #[wasm_bindgen(js_name = readMetadata)]
        pub async fn read_metadata(&mut self, path: String) -> Result<JsValue, JsValue> {
            let path = browser_path(&path, self.limits)?;
            let cancellation = CancellationToken::default();
            let receipt = with_checkout_mut!(&mut self.engine, checkout, {
                checkout
                    .read_metadata(&path, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?
            });
            serde_wasm_bindgen::to_value(&MetadataResult {
                canonical_bytes: encode_file_metadata(receipt.value).map_err(js_error)?,
                work: receipt.work,
            })
            .map_err(js_error)
        }

        /// Reads complete canonical metadata by stable file identity.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed identity, absence, storage,
        /// authentication, cancellation, encoding, or bounded work.
        #[wasm_bindgen(js_name = readMetadataById)]
        pub async fn read_metadata_by_id(&mut self, file_id: Vec<u8>) -> Result<JsValue, JsValue> {
            let file_id = FileId::from_bytes(fixed_16(&file_id)?);
            let cancellation = CancellationToken::default();
            let receipt = with_checkout_mut!(&mut self.engine, checkout, {
                checkout
                    .read_metadata_by_id(file_id, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?
            });
            serde_wasm_bindgen::to_value(&MetadataResult {
                canonical_bytes: encode_file_metadata(receipt.value).map_err(js_error)?,
                work: receipt.work,
            })
            .map_err(js_error)
        }

        /// Atomically replaces complete canonical metadata for one path.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for path, codec, mutation, cancellation, or work failure.
        #[wasm_bindgen(js_name = setMetadata)]
        pub async fn set_metadata(
            &mut self,
            path: String,
            canonical_bytes: Vec<u8>,
        ) -> Result<JsValue, JsValue> {
            let path = browser_path(&path, self.limits)?;
            let metadata =
                decode_file_metadata(&canonical_bytes, browser_decode_limits(self.limits))
                    .map_err(js_error)?;
            let cancellation = CancellationToken::default();
            let receipt = with_checkout_mut!(&mut self.engine, checkout, {
                checkout
                    .set_metadata(path, metadata, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?
            });
            serde_wasm_bindgen::to_value(&MutationResult {
                file_id: None,
                work: receipt.work,
            })
            .map_err(js_error)
        }

        /// Replaces complete canonical metadata by stable file identity.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed identity/metadata, absence,
        /// storage, cancellation, or bounded work.
        #[wasm_bindgen(js_name = setMetadataById)]
        pub async fn set_metadata_by_id(
            &mut self,
            file_id: Vec<u8>,
            canonical_bytes: Vec<u8>,
        ) -> Result<JsValue, JsValue> {
            let file_id = FileId::from_bytes(fixed_16(&file_id)?);
            let metadata =
                decode_file_metadata(&canonical_bytes, browser_decode_limits(self.limits))
                    .map_err(js_error)?;
            let cancellation = CancellationToken::default();
            let receipt = with_checkout_mut!(&mut self.engine, checkout, {
                checkout
                    .set_metadata_by_id(file_id, metadata, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?
            });
            mutation_value(receipt.work)
        }

        /// Atomically replaces metadata and optional logical size for one path.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed metadata/path, non-regular
        /// resize, storage, cancellation, mutation, or bounded-work failure.
        #[wasm_bindgen(js_name = setAttributes)]
        pub async fn set_attributes(
            &mut self,
            path: String,
            canonical_bytes: Vec<u8>,
            logical_bytes: Option<u64>,
        ) -> Result<JsValue, JsValue> {
            let path = browser_path(&path, self.limits)?;
            let metadata =
                decode_file_metadata(&canonical_bytes, browser_decode_limits(self.limits))
                    .map_err(js_error)?;
            let cancellation = CancellationToken::default();
            let receipt = with_checkout_mut!(&mut self.engine, checkout, {
                checkout
                    .set_attributes(
                        path,
                        metadata,
                        logical_bytes,
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?
            });
            mutation_value(receipt.work)
        }

        /// Atomically replaces metadata and optional logical size by file identity.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed identity/metadata,
        /// non-regular resize, storage, cancellation, or bounded work.
        #[wasm_bindgen(js_name = setAttributesById)]
        pub async fn set_attributes_by_id(
            &mut self,
            file_id: Vec<u8>,
            canonical_bytes: Vec<u8>,
            logical_bytes: Option<u64>,
        ) -> Result<JsValue, JsValue> {
            let file_id = FileId::from_bytes(fixed_16(&file_id)?);
            let metadata =
                decode_file_metadata(&canonical_bytes, browser_decode_limits(self.limits))
                    .map_err(js_error)?;
            let cancellation = CancellationToken::default();
            let receipt = with_checkout_mut!(&mut self.engine, checkout, {
                checkout
                    .set_attributes_by_id(
                        file_id,
                        metadata,
                        logical_bytes,
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?
            });
            mutation_value(receipt.work)
        }

        /// Reads one exact named attribute value.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for class, name, path, storage, cancellation, or work failure.
        #[wasm_bindgen(js_name = readNamedAttribute)]
        pub async fn read_named_attribute(
            &mut self,
            path: String,
            attribute_class: String,
            name: Vec<u8>,
        ) -> Result<JsValue, JsValue> {
            let path = browser_path(&path, self.limits)?;
            let name = browser_attribute_name(&attribute_class, name, self.limits)?;
            let cancellation = CancellationToken::default();
            let receipt = with_checkout_mut!(&mut self.engine, checkout, {
                checkout
                    .read_named_attribute(&path, &name, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?
            });
            serde_wasm_bindgen::to_value(&NamedAttributeResult {
                exists: receipt.value.is_some(),
                bytes: receipt.value.map(|value| value.to_vec()),
                work: receipt.work,
            })
            .map_err(js_error)
        }

        /// Returns one bounded ordered named-attribute page.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for class, cursor, path, storage, cancellation, or work failure.
        #[wasm_bindgen(js_name = listNamedAttributes)]
        pub async fn list_named_attributes(
            &mut self,
            path: String,
            after_class: Option<String>,
            after_name: Option<Vec<u8>>,
            maximum_entries: u32,
        ) -> Result<JsValue, JsValue> {
            let path = browser_path(&path, self.limits)?;
            let after = match (after_class, after_name) {
                (None, None) => None,
                (Some(class), Some(name)) => {
                    Some(browser_attribute_name(&class, name, self.limits)?)
                }
                _ => return Err(JsValue::from_str("named-attribute cursor is incomplete")),
            };
            let cancellation = CancellationToken::default();
            let receipt = with_checkout_mut!(&mut self.engine, checkout, {
                checkout
                    .list_named_attributes(
                        &path,
                        after.as_ref(),
                        maximum_entries,
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?
            });
            serde_wasm_bindgen::to_value(&NamedAttributePageResult {
                entries: receipt
                    .value
                    .entries
                    .into_iter()
                    .map(|entry| NamedAttributeNameResult {
                        attribute_class: browser_attribute_class_name(entry.name.class()),
                        name: entry.name.as_bytes().to_vec(),
                    })
                    .collect(),
                has_more: receipt.value.has_more,
                work: receipt.work,
            })
            .map_err(js_error)
        }

        /// Inserts or replaces one exact named attribute.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for class, name, mode, mutation, cancellation, or work failure.
        #[wasm_bindgen(js_name = writeNamedAttribute)]
        pub async fn write_named_attribute(
            &mut self,
            path: String,
            attribute_class: String,
            name: Vec<u8>,
            bytes: Vec<u8>,
            mode: String,
        ) -> Result<JsValue, JsValue> {
            let path = browser_path(&path, self.limits)?;
            let name = browser_attribute_name(&attribute_class, name, self.limits)?;
            let mode = browser_attribute_write_mode(&mode)?;
            let cancellation = CancellationToken::default();
            let receipt = with_checkout_mut!(&mut self.engine, checkout, {
                checkout
                    .write_named_attribute(
                        path,
                        name,
                        bytes::Bytes::from(bytes),
                        mode,
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?
            });
            serde_wasm_bindgen::to_value(&MutationResult {
                file_id: None,
                work: receipt.work,
            })
            .map_err(js_error)
        }

        /// Removes one exact named attribute.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for class, name, mutation, cancellation, or work failure.
        #[wasm_bindgen(js_name = removeNamedAttribute)]
        pub async fn remove_named_attribute(
            &mut self,
            path: String,
            attribute_class: String,
            name: Vec<u8>,
        ) -> Result<JsValue, JsValue> {
            let path = browser_path(&path, self.limits)?;
            let name = browser_attribute_name(&attribute_class, name, self.limits)?;
            let cancellation = CancellationToken::default();
            let receipt = with_checkout_mut!(&mut self.engine, checkout, {
                checkout
                    .remove_named_attribute(path, name, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?
            });
            serde_wasm_bindgen::to_value(&MutationResult {
                file_id: None,
                work: receipt.work,
            })
            .map_err(js_error)
        }

        /// Reads one exact logical regular-file range.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed paths, invalid ranges,
        /// non-regular files, corruption, cancellation, or bounded work.
        #[wasm_bindgen(js_name = readFileRange)]
        pub async fn read_file_range(
            &mut self,
            path: String,
            offset: u64,
            length: u64,
        ) -> Result<JsValue, JsValue> {
            let portable = PortablePath::parse(&path, self.limits).map_err(js_error)?;
            let path = NamespacePath::from_portable(&portable, self.limits).map_err(js_error)?;
            let cancellation = CancellationToken::default();
            let receipt = match &mut self.engine {
                BrowserCheckoutEngine::IndexedDb(checkout) => checkout
                    .read_file_range(
                        &path,
                        ByteRange { offset, length },
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::IndexedDbOpfs(checkout) => checkout
                    .read_file_range(
                        &path,
                        ByteRange { offset, length },
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::Memory(checkout) => checkout
                    .read_file_range(
                        &path,
                        ByteRange { offset, length },
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?,
            };
            serde_wasm_bindgen::to_value(&FileReadResult {
                bytes: receipt.value.bytes.to_vec(),
                work: receipt.work,
            })
            .map_err(js_error)
        }

        /// Reads one exact logical range by stable file identity.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed identity/range, absence,
        /// non-regular kind, storage, cancellation, or bounded work.
        #[wasm_bindgen(js_name = readFileRangeById)]
        pub async fn read_file_range_by_id(
            &mut self,
            file_id: Vec<u8>,
            offset: u64,
            length: u64,
        ) -> Result<JsValue, JsValue> {
            let file_id = FileId::from_bytes(fixed_16(&file_id)?);
            let cancellation = CancellationToken::default();
            let receipt = with_checkout_mut!(&mut self.engine, checkout, {
                checkout
                    .read_file_range_by_id(
                        file_id,
                        ByteRange { offset, length },
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?
            });
            serde_wasm_bindgen::to_value(&FileReadResult {
                bytes: receipt.value.bytes.to_vec(),
                work: receipt.work,
            })
            .map_err(js_error)
        }

        /// Plans one bounded sparse range without reading file content blobs.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed path/range, invalid bounds,
        /// non-regular kind, storage, cancellation, or bounded work.
        #[wasm_bindgen(js_name = planFileExtents)]
        pub async fn plan_file_extents(
            &mut self,
            path: String,
            offset: u64,
            length: u64,
            maximum_spans: u32,
        ) -> Result<JsValue, JsValue> {
            let path = browser_path(&path, self.limits)?;
            let cancellation = CancellationToken::default();
            let receipt = with_checkout_mut!(&mut self.engine, checkout, {
                checkout
                    .plan_file_extents(
                        &path,
                        ByteRange { offset, length },
                        maximum_spans,
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?
            });
            let result = match receipt.value {
                None => ExtentPlanResult::Inline { work: receipt.work },
                Some(plan) => ExtentPlanResult::Sparse {
                    spans: plan.spans.iter().map(encode_extent_span).collect(),
                    retained_allocation_bytes: plan.retained_allocation_bytes.to_string(),
                    work: receipt.work,
                },
            };
            serde_wasm_bindgen::to_value(&result).map_err(js_error)
        }

        /// Plans one bounded sparse range by stable file identity.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed identity/range, invalid bounds,
        /// non-regular kind, storage, cancellation, or bounded work.
        #[wasm_bindgen(js_name = planFileExtentsById)]
        pub async fn plan_file_extents_by_id(
            &mut self,
            file_id: Vec<u8>,
            offset: u64,
            length: u64,
            maximum_spans: u32,
        ) -> Result<JsValue, JsValue> {
            let file_id = FileId::from_bytes(fixed_16(&file_id)?);
            let cancellation = CancellationToken::default();
            let receipt = with_checkout_mut!(&mut self.engine, checkout, {
                checkout
                    .plan_file_extents_by_id(
                        file_id,
                        ByteRange { offset, length },
                        maximum_spans,
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?
            });
            let result = match receipt.value {
                None => ExtentPlanResult::Inline { work: receipt.work },
                Some(plan) => ExtentPlanResult::Sparse {
                    spans: plan.spans.iter().map(encode_extent_span).collect(),
                    retained_allocation_bytes: plan.retained_allocation_bytes.to_string(),
                    work: receipt.work,
                },
            };
            serde_wasm_bindgen::to_value(&result).map_err(js_error)
        }

        /// Finds the next sparse data or hole boundary without reading file bodies.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed path/offset/target,
        /// non-regular kind, storage, cancellation, or bounded work.
        #[wasm_bindgen(js_name = seekFileExtent)]
        pub async fn seek_file_extent(
            &mut self,
            path: String,
            offset: u64,
            target: String,
        ) -> Result<JsValue, JsValue> {
            let path = browser_path(&path, self.limits)?;
            let target = extent_seek_target(&target)?;
            let cancellation = CancellationToken::default();
            let receipt = with_checkout_mut!(&mut self.engine, checkout, {
                checkout
                    .seek_file_extent(&path, offset, target, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?
            });
            serde_wasm_bindgen::to_value(&ExtentSeekResult {
                offset: receipt.value.map(|value| value.to_string()),
                work: receipt.work,
            })
            .map_err(js_error)
        }

        /// Finds the next sparse boundary by stable file identity.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed identity/offset/target,
        /// non-regular kind, storage, cancellation, or bounded work.
        #[wasm_bindgen(js_name = seekFileExtentById)]
        pub async fn seek_file_extent_by_id(
            &mut self,
            file_id: Vec<u8>,
            offset: u64,
            target: String,
        ) -> Result<JsValue, JsValue> {
            let file_id = FileId::from_bytes(fixed_16(&file_id)?);
            let target = extent_seek_target(&target)?;
            let cancellation = CancellationToken::default();
            let receipt = with_checkout_mut!(&mut self.engine, checkout, {
                checkout
                    .seek_file_extent_by_id(
                        file_id,
                        offset,
                        target,
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?
            });
            serde_wasm_bindgen::to_value(&ExtentSeekResult {
                offset: receipt.value.map(|value| value.to_string()),
                work: receipt.work,
            })
            .map_err(js_error)
        }

        /// Reads one symbolic link's exact opaque target bytes without following it.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed paths, non-links,
        /// corruption, cancellation, storage, or bounded work.
        #[wasm_bindgen(js_name = readSymbolicLink)]
        pub async fn read_symbolic_link(&mut self, path: String) -> Result<JsValue, JsValue> {
            let path = browser_path(&path, self.limits)?;
            let cancellation = CancellationToken::default();
            let receipt = match &mut self.engine {
                BrowserCheckoutEngine::IndexedDb(checkout) => checkout
                    .read_symbolic_link(&path, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::IndexedDbOpfs(checkout) => checkout
                    .read_symbolic_link(&path, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::Memory(checkout) => checkout
                    .read_symbolic_link(&path, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
            };
            serde_wasm_bindgen::to_value(&FileReadResult {
                bytes: receipt.value.to_vec(),
                work: receipt.work,
            })
            .map_err(js_error)
        }

        /// Reads one opaque Windows reparse-point payload without interpreting it.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed paths, wrong file kind,
        /// storage, cancellation, authentication, or bounded work.
        #[wasm_bindgen(js_name = readReparsePoint)]
        pub async fn read_reparse_point(&mut self, path: String) -> Result<JsValue, JsValue> {
            let path = browser_path(&path, self.limits)?;
            let cancellation = CancellationToken::default();
            let receipt = with_checkout_mut!(&mut self.engine, checkout, {
                checkout
                    .read_reparse_point(&path, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?
            });
            serde_wasm_bindgen::to_value(&FileReadResult {
                bytes: receipt.value.to_vec(),
                work: receipt.work,
            })
            .map_err(js_error)
        }

        /// Returns one bounded ordered directory page.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed paths/cursors,
        /// non-directories, corruption, cancellation, or bounded work.
        #[wasm_bindgen(js_name = listDirectory)]
        pub async fn list_directory(
            &mut self,
            path: String,
            after: Option<String>,
            maximum_entries: u32,
        ) -> Result<JsValue, JsValue> {
            let portable = PortablePath::parse(&path, self.limits).map_err(js_error)?;
            let path = NamespacePath::from_portable(&portable, self.limits).map_err(js_error)?;
            let after = after
                .map(|value| {
                    LogicalName::new(
                        NameEncoding::Utf8,
                        value.into_bytes(),
                        self.limits.maximum_component_bytes,
                    )
                })
                .transpose()
                .map_err(js_error)?;
            let cancellation = CancellationToken::default();
            let receipt = match &mut self.engine {
                BrowserCheckoutEngine::IndexedDb(checkout) => checkout
                    .list_directory(
                        &path,
                        after.as_ref(),
                        maximum_entries,
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::IndexedDbOpfs(checkout) => checkout
                    .list_directory(
                        &path,
                        after.as_ref(),
                        maximum_entries,
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::Memory(checkout) => checkout
                    .list_directory(
                        &path,
                        after.as_ref(),
                        maximum_entries,
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?,
            };
            let entries = receipt
                .value
                .entries
                .into_iter()
                .map(|entry| DirectoryEntryResult {
                    name: entry.name.as_bytes().to_vec(),
                    file_id: entry.file_id.into_bytes().to_vec(),
                    file_kind: file_kind(entry.kind),
                })
                .collect();
            serde_wasm_bindgen::to_value(&DirectoryPageResult {
                entries,
                has_more: receipt.value.has_more,
                work: receipt.work,
            })
            .map_err(js_error)
        }

        /// Returns one bounded directory page with records and metadata fetched in batches.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed paths/cursors,
        /// non-directories, corruption, cancellation, or bounded work.
        #[wasm_bindgen(js_name = listDirectoryRecords)]
        pub async fn list_directory_records(
            &mut self,
            path: String,
            after: Option<String>,
            maximum_entries: u32,
        ) -> Result<JsValue, JsValue> {
            let path = browser_path(&path, self.limits)?;
            let after = after
                .map(|value| {
                    LogicalName::new(
                        NameEncoding::Utf8,
                        value.into_bytes(),
                        self.limits.maximum_component_bytes,
                    )
                })
                .transpose()
                .map_err(js_error)?;
            let cancellation = CancellationToken::default();
            let receipt = with_checkout_mut!(&mut self.engine, checkout, {
                checkout
                    .list_directory_records(
                        &path,
                        after.as_ref(),
                        maximum_entries,
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?
            });
            let entries = receipt
                .value
                .entries
                .into_iter()
                .map(|entry| {
                    Ok(DirectoryRecordEntryResult {
                        name: entry.name.as_bytes().to_vec(),
                        record: encode_file_record(entry.record),
                        metadata_canonical_bytes: encode_file_metadata(entry.metadata)
                            .map_err(js_error)?,
                    })
                })
                .collect::<Result<Vec<_>, JsValue>>()?;
            serde_wasm_bindgen::to_value(&DirectoryRecordPageResult {
                entries,
                has_more: receipt.value.has_more,
                work: receipt.work,
            })
            .map_err(js_error)
        }

        /// Creates one regular file in the private COW overlay.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed paths, conflicts,
        /// cancellation, storage, allocation, or bounded work.
        #[wasm_bindgen(js_name = createFile)]
        pub async fn create_file(
            &mut self,
            path: String,
            bytes: Vec<u8>,
        ) -> Result<JsValue, JsValue> {
            let path = browser_path(&path, self.limits)?;
            let cancellation = CancellationToken::default();
            let bytes = bytes::Bytes::from(bytes);
            let receipt = match &mut self.engine {
                BrowserCheckoutEngine::IndexedDb(checkout) => checkout
                    .create_file(path, bytes, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::IndexedDbOpfs(checkout) => checkout
                    .create_file(path, bytes, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::Memory(checkout) => checkout
                    .create_file(path, bytes, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
            };
            serde_wasm_bindgen::to_value(&MutationResult {
                file_id: Some(receipt.value.into_bytes().to_vec()),
                work: receipt.work,
            })
            .map_err(js_error)
        }

        /// Creates one empty directory in the private COW overlay.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed paths, conflicts,
        /// cancellation, storage, allocation, or bounded work.
        #[wasm_bindgen(js_name = createDirectory)]
        pub async fn create_directory(&mut self, path: String) -> Result<JsValue, JsValue> {
            let path = browser_path(&path, self.limits)?;
            let cancellation = CancellationToken::default();
            let receipt = match &mut self.engine {
                BrowserCheckoutEngine::IndexedDb(checkout) => checkout
                    .create_directory(path, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::IndexedDbOpfs(checkout) => checkout
                    .create_directory(path, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::Memory(checkout) => checkout
                    .create_directory(path, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
            };
            serde_wasm_bindgen::to_value(&MutationResult {
                file_id: Some(receipt.value.into_bytes().to_vec()),
                work: receipt.work,
            })
            .map_err(js_error)
        }

        /// Creates one symbolic link with exact opaque target bytes.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed paths, conflicts,
        /// excessive targets, cancellation, storage, or bounded work.
        #[wasm_bindgen(js_name = createSymbolicLink)]
        pub async fn create_symbolic_link(
            &mut self,
            path: String,
            target: Vec<u8>,
        ) -> Result<JsValue, JsValue> {
            let path = browser_path(&path, self.limits)?;
            let cancellation = CancellationToken::default();
            let target = bytes::Bytes::from(target);
            let receipt = match &mut self.engine {
                BrowserCheckoutEngine::IndexedDb(checkout) => checkout
                    .create_symbolic_link(path, target, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::IndexedDbOpfs(checkout) => checkout
                    .create_symbolic_link(path, target, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::Memory(checkout) => checkout
                    .create_symbolic_link(path, target, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
            };
            serde_wasm_bindgen::to_value(&MutationResult {
                file_id: Some(receipt.value.into_bytes().to_vec()),
                work: receipt.work,
            })
            .map_err(js_error)
        }

        /// Creates an exact empty special namespace entry.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed paths/kinds, unsupported
        /// profile semantics, storage, cancellation, or bounded work.
        #[wasm_bindgen(js_name = createSpecial)]
        pub async fn create_special(
            &mut self,
            path: String,
            kind: String,
        ) -> Result<JsValue, JsValue> {
            let path = browser_path(&path, self.limits)?;
            let kind = empty_special_kind(&kind)?;
            let cancellation = CancellationToken::default();
            let receipt = match &mut self.engine {
                BrowserCheckoutEngine::IndexedDb(checkout) => checkout
                    .create_empty_special(path, kind, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::IndexedDbOpfs(checkout) => checkout
                    .create_empty_special(path, kind, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::Memory(checkout) => checkout
                    .create_empty_special(path, kind, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
            };
            mutation_created(&receipt)
        }

        /// Creates an exact POSIX character or block device identity.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed paths/kinds, unsupported
        /// profile semantics, storage, cancellation, or bounded work.
        #[wasm_bindgen(js_name = createDevice)]
        pub async fn create_device(
            &mut self,
            path: String,
            kind: String,
            major: u32,
            minor: u32,
        ) -> Result<JsValue, JsValue> {
            let path = browser_path(&path, self.limits)?;
            let kind = device_kind(&kind)?;
            let cancellation = CancellationToken::default();
            let receipt = match &mut self.engine {
                BrowserCheckoutEngine::IndexedDb(checkout) => checkout
                    .create_device(path, kind, major, minor, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::IndexedDbOpfs(checkout) => checkout
                    .create_device(path, kind, major, minor, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::Memory(checkout) => checkout
                    .create_device(path, kind, major, minor, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
            };
            mutation_created(&receipt)
        }

        /// Creates an opaque exact Windows reparse-point payload.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed paths, unsupported profile
        /// semantics, excessive payload, storage, cancellation, or bounded work.
        #[wasm_bindgen(js_name = createReparsePoint)]
        pub async fn create_reparse_point(
            &mut self,
            path: String,
            payload: Vec<u8>,
        ) -> Result<JsValue, JsValue> {
            let path = browser_path(&path, self.limits)?;
            let cancellation = CancellationToken::default();
            let receipt = match &mut self.engine {
                BrowserCheckoutEngine::IndexedDb(checkout) => checkout
                    .create_reparse_point(
                        path,
                        bytes::Bytes::from(payload),
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::IndexedDbOpfs(checkout) => checkout
                    .create_reparse_point(
                        path,
                        bytes::Bytes::from(payload),
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::Memory(checkout) => checkout
                    .create_reparse_point(
                        path,
                        bytes::Bytes::from(payload),
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?,
            };
            mutation_created(&receipt)
        }

        /// Replaces one logical file range in the private COW overlay.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed paths/offsets,
        /// non-regular files, cancellation, storage, or bounded work.
        #[wasm_bindgen(js_name = writeFile)]
        pub async fn write_file(
            &mut self,
            path: String,
            offset: u64,
            bytes: Vec<u8>,
        ) -> Result<JsValue, JsValue> {
            let path = browser_path(&path, self.limits)?;
            let cancellation = CancellationToken::default();
            let bytes = bytes::Bytes::from(bytes);
            let receipt = match &mut self.engine {
                BrowserCheckoutEngine::IndexedDb(checkout) => checkout
                    .write_file(path, offset, bytes, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::IndexedDbOpfs(checkout) => checkout
                    .write_file(path, offset, bytes, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::Memory(checkout) => checkout
                    .write_file(path, offset, bytes, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
            };
            serde_wasm_bindgen::to_value(&MutationResult {
                file_id: None,
                work: receipt.work,
            })
            .map_err(js_error)
        }

        /// Replaces one logical range by stable file identity.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed identity/offset,
        /// non-regular kind, storage, cancellation, or bounded work.
        #[wasm_bindgen(js_name = writeFileById)]
        pub async fn write_file_by_id(
            &mut self,
            file_id: Vec<u8>,
            offset: u64,
            bytes: Vec<u8>,
        ) -> Result<JsValue, JsValue> {
            let file_id = FileId::from_bytes(fixed_16(&file_id)?);
            let cancellation = CancellationToken::default();
            let bytes = bytes::Bytes::from(bytes);
            let receipt = with_checkout_mut!(&mut self.engine, checkout, {
                checkout
                    .write_file_by_id(file_id, offset, bytes, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?
            });
            mutation_value(receipt.work)
        }

        /// Removes one namespace binding.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed paths/identities,
        /// conflicts, cancellation, storage, or bounded work.
        pub async fn remove(
            &mut self,
            path: String,
            expected_file_id: Option<Vec<u8>>,
        ) -> Result<JsValue, JsValue> {
            let path = browser_path(&path, self.limits)?;
            let expected = expected_file_id
                .as_deref()
                .map(fixed_16)
                .transpose()?
                .map(FileId::from_bytes);
            let cancellation = CancellationToken::default();
            let receipt = match &mut self.engine {
                BrowserCheckoutEngine::IndexedDb(checkout) => checkout
                    .remove(path, expected, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::IndexedDbOpfs(checkout) => checkout
                    .remove(path, expected, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::Memory(checkout) => checkout
                    .remove(path, expected, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
            };
            mutation_value(receipt.work)
        }

        /// Atomically renames one binding within the volume.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed paths, conflicts,
        /// cancellation, storage, or bounded work.
        pub async fn rename(
            &mut self,
            source: String,
            destination: String,
            replace: bool,
        ) -> Result<JsValue, JsValue> {
            let source = browser_path(&source, self.limits)?;
            let destination = browser_path(&destination, self.limits)?;
            let cancellation = CancellationToken::default();
            let receipt = match &mut self.engine {
                BrowserCheckoutEngine::IndexedDb(checkout) => checkout
                    .rename(
                        source,
                        destination,
                        replace,
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::IndexedDbOpfs(checkout) => checkout
                    .rename(
                        source,
                        destination,
                        replace,
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::Memory(checkout) => checkout
                    .rename(
                        source,
                        destination,
                        replace,
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?,
            };
            mutation_value(receipt.work)
        }

        /// Creates one hard link within the volume.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed paths, invalid kinds,
        /// conflicts, cancellation, storage, or bounded work.
        #[wasm_bindgen(js_name = hardLink)]
        pub async fn hard_link(
            &mut self,
            source: String,
            destination: String,
        ) -> Result<JsValue, JsValue> {
            let source = browser_path(&source, self.limits)?;
            let destination = browser_path(&destination, self.limits)?;
            let cancellation = CancellationToken::default();
            let receipt = match &mut self.engine {
                BrowserCheckoutEngine::IndexedDb(checkout) => checkout
                    .hard_link(source, destination, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::IndexedDbOpfs(checkout) => checkout
                    .hard_link(source, destination, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::Memory(checkout) => checkout
                    .hard_link(source, destination, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
            };
            mutation_value(receipt.work)
        }

        /// Changes one regular file's logical length.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed values, invalid kinds,
        /// cancellation, storage, or bounded work.
        #[wasm_bindgen(js_name = resizeFile)]
        pub async fn resize_file(
            &mut self,
            path: String,
            logical_bytes: u64,
        ) -> Result<JsValue, JsValue> {
            let path = browser_path(&path, self.limits)?;
            let cancellation = CancellationToken::default();
            let receipt = match &mut self.engine {
                BrowserCheckoutEngine::IndexedDb(checkout) => checkout
                    .resize_file(path, logical_bytes, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::IndexedDbOpfs(checkout) => checkout
                    .resize_file(path, logical_bytes, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::Memory(checkout) => checkout
                    .resize_file(path, logical_bytes, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
            };
            mutation_value(receipt.work)
        }

        /// Changes logical length by stable file identity.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed identity/length, absence,
        /// invalid kind, storage, cancellation, or bounded work.
        #[wasm_bindgen(js_name = resizeFileById)]
        pub async fn resize_file_by_id(
            &mut self,
            file_id: Vec<u8>,
            logical_bytes: u64,
        ) -> Result<JsValue, JsValue> {
            let file_id = FileId::from_bytes(fixed_16(&file_id)?);
            let cancellation = CancellationToken::default();
            let receipt = with_checkout_mut!(&mut self.engine, checkout, {
                checkout
                    .resize_file_by_id(file_id, logical_bytes, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?
            });
            mutation_value(receipt.work)
        }

        /// Punches a hole or records physically allocated zeros.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed ranges/kinds,
        /// cancellation, storage, or bounded work.
        #[wasm_bindgen(js_name = zeroFileRange)]
        pub async fn zero_file_range(
            &mut self,
            path: String,
            offset: u64,
            length: u64,
            allocated: bool,
            extend: bool,
        ) -> Result<JsValue, JsValue> {
            let path = browser_path(&path, self.limits)?;
            let cancellation = CancellationToken::default();
            let receipt = match &mut self.engine {
                BrowserCheckoutEngine::IndexedDb(checkout) => checkout
                    .zero_file_range(
                        path,
                        ByteRange { offset, length },
                        allocated,
                        extend,
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::IndexedDbOpfs(checkout) => checkout
                    .zero_file_range(
                        path,
                        ByteRange { offset, length },
                        allocated,
                        extend,
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::Memory(checkout) => checkout
                    .zero_file_range(
                        path,
                        ByteRange { offset, length },
                        allocated,
                        extend,
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?,
            };
            mutation_value(receipt.work)
        }

        /// Punches a hole or records allocated zero by stable file identity.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed identity/range, absence,
        /// invalid kind, storage, cancellation, or bounded work.
        #[wasm_bindgen(js_name = zeroFileRangeById)]
        pub async fn zero_file_range_by_id(
            &mut self,
            file_id: Vec<u8>,
            offset: u64,
            length: u64,
            allocated: bool,
            extend: bool,
        ) -> Result<JsValue, JsValue> {
            let file_id = FileId::from_bytes(fixed_16(&file_id)?);
            let cancellation = CancellationToken::default();
            let receipt = with_checkout_mut!(&mut self.engine, checkout, {
                checkout
                    .zero_file_range_by_id(
                        file_id,
                        ByteRange { offset, length },
                        allocated,
                        extend,
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?
            });
            mutation_value(receipt.work)
        }

        /// Allocates sparse holes without replacing existing content.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed values, unsupported
        /// keep-size physical allocation, cancellation, storage, or bounded work.
        #[wasm_bindgen(js_name = preallocateFile)]
        pub async fn preallocate_file(
            &mut self,
            path: String,
            offset: u64,
            length: u64,
            keep_size: bool,
        ) -> Result<JsValue, JsValue> {
            let path = browser_path(&path, self.limits)?;
            let cancellation = CancellationToken::default();
            let receipt = match &mut self.engine {
                BrowserCheckoutEngine::IndexedDb(checkout) => checkout
                    .preallocate_file(
                        path,
                        ByteRange { offset, length },
                        keep_size,
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::IndexedDbOpfs(checkout) => checkout
                    .preallocate_file(
                        path,
                        ByteRange { offset, length },
                        keep_size,
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::Memory(checkout) => checkout
                    .preallocate_file(
                        path,
                        ByteRange { offset, length },
                        keep_size,
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?,
            };
            mutation_value(receipt.work)
        }

        /// Allocates sparse holes by stable file identity.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed identity/range, absence,
        /// unsupported allocation, storage, cancellation, or bounded work.
        #[wasm_bindgen(js_name = preallocateFileById)]
        pub async fn preallocate_file_by_id(
            &mut self,
            file_id: Vec<u8>,
            offset: u64,
            length: u64,
            keep_size: bool,
        ) -> Result<JsValue, JsValue> {
            let file_id = FileId::from_bytes(fixed_16(&file_id)?);
            let cancellation = CancellationToken::default();
            let receipt = with_checkout_mut!(&mut self.engine, checkout, {
                checkout
                    .preallocate_file_by_id(
                        file_id,
                        ByteRange { offset, length },
                        keep_size,
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?
            });
            mutation_value(receipt.work)
        }

        /// Clones one logical range by immutable extent reference.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed paths/ranges,
        /// cancellation, storage, or bounded work.
        #[wasm_bindgen(js_name = cloneFileRange)]
        pub async fn clone_file_range(
            &mut self,
            source: String,
            source_offset: u64,
            destination: String,
            destination_offset: u64,
            length: u64,
        ) -> Result<JsValue, JsValue> {
            let request = FileCloneRequest {
                source: browser_path(&source, self.limits)?,
                source_offset,
                destination: browser_path(&destination, self.limits)?,
                destination_offset,
                length,
            };
            let cancellation = CancellationToken::default();
            let receipt = match &mut self.engine {
                BrowserCheckoutEngine::IndexedDb(checkout) => checkout
                    .clone_file_range(request, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::IndexedDbOpfs(checkout) => checkout
                    .clone_file_range(request, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::Memory(checkout) => checkout
                    .clone_file_range(request, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
            };
            mutation_value(receipt.work)
        }

        /// Clones one logical range between stable file identities.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed identities/ranges, absence,
        /// invalid kinds, storage, cancellation, or bounded work.
        #[wasm_bindgen(js_name = cloneFileRangeById)]
        pub async fn clone_file_range_by_id(
            &mut self,
            source_file_id: Vec<u8>,
            source_offset: u64,
            destination_file_id: Vec<u8>,
            destination_offset: u64,
            length: u64,
        ) -> Result<JsValue, JsValue> {
            let source_file_id = FileId::from_bytes(fixed_16(&source_file_id)?);
            let destination_file_id = FileId::from_bytes(fixed_16(&destination_file_id)?);
            let cancellation = CancellationToken::default();
            let receipt = with_checkout_mut!(&mut self.engine, checkout, {
                checkout
                    .clone_file_range_by_id(
                        source_file_id,
                        source_offset,
                        destination_file_id,
                        destination_offset,
                        length,
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?
            });
            mutation_value(receipt.work)
        }

        /// Checkpoints and conditionally publishes this private overlay.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed operation identity, clean
        /// or read-only checkout, closure failure, cancellation, or bounded work.
        pub async fn commit(&mut self, operation_id: Vec<u8>) -> Result<JsValue, JsValue> {
            let operation_id = OperationId::from_bytes(fixed_16(&operation_id)?);
            let cancellation = CancellationToken::default();
            let receipt = match &mut self.engine {
                BrowserCheckoutEngine::IndexedDb(checkout) => checkout
                    .commit(operation_id, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::IndexedDbOpfs(checkout) => checkout
                    .commit(operation_id, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::Memory(checkout) => checkout
                    .commit(operation_id, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
            };
            commit_value(receipt.value, receipt.work)
        }

        /// Applies and publishes one direct-live transaction with bounded safe retries.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed operations or identity,
        /// wrong checkout mode, unresolved work, cancellation, storage, rebase,
        /// or bounded-work failure.
        #[wasm_bindgen(js_name = mutateLive)]
        pub async fn mutate_live(
            &mut self,
            operations: JsValue,
            operation_id: Vec<u8>,
            maximum_attempts: u32,
            maximum_conflicts: u32,
        ) -> Result<JsValue, JsValue> {
            let authored = decode_authored_transactions(operations, self.limits)?;
            let operation_id = OperationId::from_bytes(fixed_16(&operation_id)?);
            let cancellation = CancellationToken::default();
            let receipt = with_checkout_mut!(&mut self.engine, checkout, {
                checkout
                    .apply_authored_live(
                        authored,
                        operation_id,
                        maximum_attempts,
                        maximum_conflicts,
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?
            });
            authored_live_mutation_value(receipt.value, receipt.work)
        }

        /// Resumes an unresolved direct-live transaction with the same operation identity.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for malformed identity, wrong checkout
        /// mode, absent staged work, cancellation, storage, rebase, or bounds.
        #[wasm_bindgen(js_name = resumeLive)]
        pub async fn resume_live(
            &mut self,
            operation_id: Vec<u8>,
            maximum_attempts: u32,
            maximum_conflicts: u32,
        ) -> Result<JsValue, JsValue> {
            let operation_id = OperationId::from_bytes(fixed_16(&operation_id)?);
            let cancellation = CancellationToken::default();
            let receipt = match &mut self.engine {
                BrowserCheckoutEngine::IndexedDb(checkout) => checkout
                    .resume_live(
                        operation_id,
                        maximum_attempts,
                        maximum_conflicts,
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::IndexedDbOpfs(checkout) => checkout
                    .resume_live(
                        operation_id,
                        maximum_attempts,
                        maximum_conflicts,
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::Memory(checkout) => checkout
                    .resume_live(
                        operation_id,
                        maximum_attempts,
                        maximum_conflicts,
                        boundary_budget(),
                        &cancellation,
                    )
                    .await
                    .map_err(js_error)?,
            };
            live_mutation_value(receipt.value, receipt.work)
        }

        /// Safely advances to head and sparsely replays private mutations.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for unsupported consistency, invalid
        /// bounds, corruption, cancellation, storage, replay, or bounded work.
        #[wasm_bindgen(js_name = rebaseHead)]
        pub async fn rebase_head(&mut self, maximum_conflicts: u32) -> Result<JsValue, JsValue> {
            let cancellation = CancellationToken::default();
            let receipt = match &mut self.engine {
                BrowserCheckoutEngine::IndexedDb(checkout) => checkout
                    .rebase_head(maximum_conflicts, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::IndexedDbOpfs(checkout) => checkout
                    .rebase_head(maximum_conflicts, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::Memory(checkout) => checkout
                    .rebase_head(maximum_conflicts, boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
            };
            let (status, generation_id, conflict_count, truncated) = match receipt.value {
                RebaseDecision::Safe { generation } => (
                    "safe",
                    Some(generation.digest().into_bytes().to_vec()),
                    0,
                    false,
                ),
                RebaseDecision::Conflicted {
                    conflicts,
                    truncated,
                } => (
                    "conflicted",
                    None,
                    u32::try_from(conflicts.len()).unwrap_or(u32::MAX),
                    truncated,
                ),
            };
            serde_wasm_bindgen::to_value(&RebaseResult {
                status,
                generation_id,
                conflict_count,
                truncated,
                work: receipt.work,
            })
            .map_err(js_error)
        }

        /// Discards the private overlay and returns to its immutable base.
        ///
        /// # Errors
        ///
        /// Returns a JavaScript error for cancellation, corruption, storage, or work bounds.
        pub async fn discard(&mut self) -> Result<JsValue, JsValue> {
            let cancellation = CancellationToken::default();
            let receipt = match &mut self.engine {
                BrowserCheckoutEngine::IndexedDb(checkout) => checkout
                    .discard(boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::IndexedDbOpfs(checkout) => checkout
                    .discard(boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
                BrowserCheckoutEngine::Memory(checkout) => checkout
                    .discard(boundary_budget(), &cancellation)
                    .await
                    .map_err(js_error)?,
            };
            mutation_value(receipt.work)
        }
    }

    /// Opens transactional `IndexedDB` correctness storage with optional OPFS acceleration.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for invalid options or unavailable required storage.
    #[wasm_bindgen(js_name = openBrowserFs)]
    pub async fn open_browser_fs(options: JsValue) -> Result<BrowserFs, JsValue> {
        let options: BrowserOptions = serde_wasm_bindgen::from_value(options).map_err(js_error)?;
        if options.database_name.is_empty() || options.maximum_object_bytes == 0 {
            return Err(JsValue::from_str("browser filesystem options are invalid"));
        }
        let authority =
            IndexedDbAuthorityStore::open(&options.database_name, options.maximum_object_bytes)
                .await
                .map_err(js_error)?;
        let object_cache = browser_object_cache_options(&options.object_cache);
        let (engine, immutable_objects) = match options.object_acceleration {
            Acceleration::Indexeddb => {
                let objects = IndexedDbObjectStore::open(
                    &options.database_name,
                    options.maximum_object_bytes,
                )
                .await
                .map_err(js_error)?;
                (
                    BrowserEngine::IndexedDb(Fs::new(
                        authority,
                        cached_objects(objects, object_cache)?,
                        EmbeddedCapabilities { durable: true },
                    )),
                    "indexeddb",
                )
            }
            Acceleration::OpfsRequired | Acceleration::OpfsIfAvailable => {
                match OpfsAcceleratedObjectStore::open(
                    &options.database_name,
                    options.maximum_object_bytes,
                )
                .await
                {
                    Ok(objects) => (
                        BrowserEngine::IndexedDbOpfs(Fs::new(
                            authority,
                            cached_objects(objects, object_cache)?,
                            EmbeddedCapabilities { durable: true },
                        )),
                        "indexeddb-opfs",
                    ),
                    Err(_error)
                        if matches!(options.object_acceleration, Acceleration::OpfsIfAvailable) =>
                    {
                        let objects = IndexedDbObjectStore::open(
                            &options.database_name,
                            options.maximum_object_bytes,
                        )
                        .await
                        .map_err(js_error)?;
                        (
                            BrowserEngine::IndexedDb(Fs::new(
                                authority,
                                cached_objects(objects, object_cache)?,
                                EmbeddedCapabilities { durable: true },
                            )),
                            "indexeddb",
                        )
                    }
                    Err(error) => return Err(js_error(error)),
                }
            }
        };
        Ok(BrowserFs {
            engine: Some(engine),
            capabilities: Capabilities {
                version: env!("CARGO_PKG_VERSION"),
                platform: "browser",
                architecture: "wasm32",
                authority: "indexeddb",
                immutable_objects,
                native_mount: "none",
                writable_native_mount: false,
                native_watch: false,
                native_watch_backend: "none".to_owned(),
                native_watch_persistent_restart: false,
                native_watch_root_identity_fencing: false,
                provider_process_io_observable: false,
            },
        })
    }

    /// Opens the deterministic process-local reference backend.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error when the memory options are invalid.
    #[wasm_bindgen(js_name = openMemoryFs)]
    pub fn open_memory_fs(options: JsValue) -> Result<BrowserFs, JsValue> {
        let options: MemoryOptions = serde_wasm_bindgen::from_value(options).map_err(js_error)?;
        if options.maximum_object_bytes == 0 {
            return Err(JsValue::from_str("memory filesystem options are invalid"));
        }
        Ok(BrowserFs {
            engine: Some(BrowserEngine::Memory(Fs::new(
                MemoryAuthorityStore::default(),
                cached_objects(
                    MemoryObjectStore::new(options.maximum_object_bytes).map_err(js_error)?,
                    browser_object_cache_options(&options.object_cache),
                )?,
                EmbeddedCapabilities::MEMORY,
            ))),
            capabilities: Capabilities {
                version: env!("CARGO_PKG_VERSION"),
                platform: "browser",
                architecture: "wasm32",
                authority: "memory",
                immutable_objects: "memory",
                native_mount: "none",
                writable_native_mount: false,
                native_watch: false,
                native_watch_backend: "none".to_owned(),
                native_watch_persistent_restart: false,
                native_watch_root_identity_fencing: false,
                provider_process_io_observable: false,
            },
        })
    }

    fn js_error(error: impl std::fmt::Display) -> JsValue {
        JsValue::from_str(&error.to_string())
    }

    fn speculation_busy() -> JsValue {
        JsValue::from_str("speculation controller is already executing an operation")
    }

    const fn browser_speculation_options(
        options: &BrowserSpeculationOptions,
    ) -> SpeculationOptions {
        SpeculationOptions {
            residency: ResidencySpeculatorOptions {
                maximum_active_operations: options.residency.maximum_active_operations,
                maximum_active_bytes: options.residency.maximum_active_bytes,
                outcome_window: options.residency.outcome_window,
                traffic_window: options.residency.traffic_window,
                speculative_cost_basis_points: options.residency.speculative_cost_basis_points,
                minimum_usefulness_samples: options.residency.minimum_usefulness_samples,
                minimum_usefulness_basis_points: options.residency.minimum_usefulness_basis_points,
            },
            promotion: PromotionSpeculatorOptions {
                maximum_active_operations: options.promotion.maximum_active_operations,
                maximum_active_bytes: options.promotion.maximum_active_bytes,
                maximum_active_cost_units: options.promotion.maximum_active_cost_units,
                maximum_residency_facts: options.promotion.maximum_residency_facts,
                maximum_destinations: options.promotion.maximum_destinations,
                maximum_accepted_tiers: options.promotion.maximum_accepted_tiers,
                outcome_window: options.promotion.outcome_window,
                minimum_usefulness_samples: options.promotion.minimum_usefulness_samples,
                minimum_usefulness_basis_points: options.promotion.minimum_usefulness_basis_points,
            },
        }
    }

    fn browser_residency_reason(value: &str) -> Result<ResidencyReason, JsValue> {
        match value {
            "directory-successor" => Ok(ResidencyReason::DirectorySuccessor),
            "sequential-range" => Ok(ResidencyReason::SequentialRange),
            "metadata-successor" => Ok(ResidencyReason::MetadataSuccessor),
            "consumer-hint" => Ok(ResidencyReason::ConsumerHint),
            _ => Err(JsValue::from_str("unknown residency speculation reason")),
        }
    }

    const fn browser_residency_rejection(value: ResidencyRejection) -> &'static str {
        match value {
            ResidencyRejection::WrongVolume => "wrong-volume",
            ResidencyRejection::StaleGeneration => "stale-generation",
            ResidencyRejection::InvalidRequest => "invalid-request",
            ResidencyRejection::DuplicateObject => "duplicate-object",
            ResidencyRejection::DuplicateOperation => "duplicate-operation",
            ResidencyRejection::OperationCapacity => "operation-capacity",
            ResidencyRejection::ByteCapacity => "byte-capacity",
            ResidencyRejection::CostBudget => "cost-budget",
            ResidencyRejection::LowUsefulness => "low-usefulness",
        }
    }

    fn browser_storage_tier(value: &str) -> Result<StorageTier, JsValue> {
        match value {
            "process-memory" => Ok(StorageTier::ProcessMemory),
            "node-local" => Ok(StorageTier::NodeLocal),
            "shared-cache" => Ok(StorageTier::SharedCache),
            "durable-origin" => Ok(StorageTier::DurableOrigin),
            _ => Err(JsValue::from_str("unknown speculation storage tier")),
        }
    }

    fn browser_object_residency(
        value: &BrowserObjectResidency,
    ) -> Result<ObjectResidency, JsValue> {
        Ok(ObjectResidency {
            object_id: decode_object_id(&value.object_id)?,
            location_id: StorageLocationId::from_bytes(fixed_16(&value.location_id)?),
            tier: browser_storage_tier(&value.tier)?,
            source_priority: value.source_priority,
        })
    }

    fn browser_promotion_destination(
        value: &BrowserPromotionDestination,
    ) -> Result<PromotionDestination, JsValue> {
        Ok(PromotionDestination {
            location_id: StorageLocationId::from_bytes(fixed_16(&value.location_id)?),
            tier: browser_storage_tier(&value.tier)?,
            writable: value.writable,
            maximum_object_bytes: value.maximum_object_bytes,
            priority: value.priority,
            cost_units_per_byte: value.cost_units_per_byte,
        })
    }

    fn browser_promotion_admission(value: PromotionAdmission) -> BrowserPromotionAdmission {
        match value {
            PromotionAdmission::Satisfied(residency) => BrowserPromotionAdmission {
                status: "satisfied",
                rejection: None,
                operation_id: None,
                object_id: Some(encode_object_id(residency.object_id)),
                source_location_id: Some(residency.location_id.into_bytes().to_vec()),
                destination_location_id: None,
                estimated_cost_units: None,
            },
            PromotionAdmission::Planned(plan) => BrowserPromotionAdmission {
                status: "planned",
                rejection: None,
                operation_id: Some(plan.candidate.operation_id.into_bytes().to_vec()),
                object_id: Some(encode_object_id(plan.candidate.request.object_id)),
                source_location_id: Some(plan.source.location_id.into_bytes().to_vec()),
                destination_location_id: Some(plan.destination.location_id.into_bytes().to_vec()),
                estimated_cost_units: Some(plan.estimated_cost_units.to_string()),
            },
            PromotionAdmission::Rejected(rejection) => BrowserPromotionAdmission {
                status: "rejected",
                rejection: Some(browser_promotion_rejection(rejection)),
                operation_id: None,
                object_id: None,
                source_location_id: None,
                destination_location_id: None,
                estimated_cost_units: None,
            },
        }
    }

    const fn browser_promotion_rejection(value: PromotionRejection) -> &'static str {
        match value {
            PromotionRejection::WrongVolume => "wrong-volume",
            PromotionRejection::StaleGeneration => "stale-generation",
            PromotionRejection::InvalidRequest => "invalid-request",
            PromotionRejection::InputCapacity => "input-capacity",
            PromotionRejection::MissingSource => "missing-source",
            PromotionRejection::DuplicateObject => "duplicate-object",
            PromotionRejection::DuplicateOperation => "duplicate-operation",
            PromotionRejection::ActiveCapacity => "active-capacity",
            PromotionRejection::NoDestination => "no-destination",
            PromotionRejection::LowUsefulness => "low-usefulness",
        }
    }

    fn browser_speculation_preemption(
        value: acyclic_fs::SpeculationPreemption,
    ) -> BrowserSpeculationPreemption {
        BrowserSpeculationPreemption {
            residency_operation_ids: value
                .residency
                .into_iter()
                .map(|operation| operation.into_bytes().to_vec())
                .collect(),
            promotion_operation_ids: value
                .promotion
                .into_iter()
                .map(|operation| operation.into_bytes().to_vec())
                .collect(),
        }
    }

    fn browser_residency_metrics(value: acyclic_fs::ResidencyMetrics) -> BrowserResidencyMetrics {
        BrowserResidencyMetrics {
            candidates: value.candidates.to_string(),
            admitted: value.admitted.to_string(),
            active: value.active.to_string(),
            active_bytes: value.active_bytes.to_string(),
            useful: value.useful.to_string(),
            wasted: value.wasted.to_string(),
            rejected_fence: value.rejected_fence.to_string(),
            rejected_duplicate: value.rejected_duplicate.to_string(),
            rejected_capacity: value.rejected_capacity.to_string(),
            rejected_cost: value.rejected_cost.to_string(),
            rejected_usefulness: value.rejected_usefulness.to_string(),
        }
    }

    fn browser_promotion_metrics(value: acyclic_fs::PromotionMetrics) -> BrowserPromotionMetrics {
        BrowserPromotionMetrics {
            candidates: value.candidates.to_string(),
            satisfied: value.satisfied.to_string(),
            planned: value.planned.to_string(),
            active: value.active.to_string(),
            active_bytes: value.active_bytes.to_string(),
            active_cost_units: value.active_cost_units.to_string(),
            useful: value.useful.to_string(),
            wasted: value.wasted.to_string(),
            rejected: value.rejected.to_string(),
        }
    }

    fn cached_objects<S>(
        objects: S,
        options: ObjectCacheOptions,
    ) -> Result<CachedObjectStore<S>, JsValue> {
        CachedObjectStore::new(objects, options).map_err(js_error)
    }

    const fn browser_object_cache_options(
        options: &BrowserObjectCacheOptions,
    ) -> ObjectCacheOptions {
        ObjectCacheOptions {
            maximum_entries: options.entries,
            maximum_bytes: options.bytes,
            maximum_in_flight: options.in_flight,
            maximum_waiters_per_object: options.waiters_per_object,
        }
    }

    fn browser_path(path: &str, limits: VolumeLimits) -> Result<NamespacePath, JsValue> {
        let portable = PortablePath::parse(path, limits).map_err(js_error)?;
        NamespacePath::from_portable(&portable, limits).map_err(js_error)
    }

    fn browser_decode_limits(limits: VolumeLimits) -> DecodeLimits {
        DecodeLimits {
            maximum_object_bytes: limits.maximum_object_bytes,
            maximum_name_bytes: limits.maximum_component_bytes,
            maximum_page_items: limits.maximum_directory_page_entries,
            maximum_page_bytes: u32::try_from(limits.maximum_object_bytes).unwrap_or(u32::MAX),
            maximum_page_height: limits.maximum_page_height,
            maximum_visited_pages: u32::try_from(limits.maximum_objects_per_generation)
                .unwrap_or(u32::MAX),
        }
    }

    fn browser_attribute_class(value: &str) -> Result<AttributeClass, JsValue> {
        match value {
            "posix-xattr" => Ok(AttributeClass::PosixXattr),
            "windows-stream" => Ok(AttributeClass::WindowsStream),
            "mac-resource-fork" => Ok(AttributeClass::MacResourceFork),
            _ => Err(JsValue::from_str("unknown named-attribute class")),
        }
    }

    const fn browser_attribute_class_name(value: AttributeClass) -> &'static str {
        match value {
            AttributeClass::PosixXattr => "posix-xattr",
            AttributeClass::WindowsStream => "windows-stream",
            AttributeClass::MacResourceFork => "mac-resource-fork",
        }
    }

    fn browser_attribute_name(
        class: &str,
        name: Vec<u8>,
        limits: VolumeLimits,
    ) -> Result<AttributeName, JsValue> {
        AttributeName::new(
            browser_attribute_class(class)?,
            name,
            limits.maximum_component_bytes,
        )
        .map_err(js_error)
    }

    fn browser_attribute_write_mode(value: &str) -> Result<NamedAttributeWriteMode, JsValue> {
        match value {
            "upsert" => Ok(NamedAttributeWriteMode::Upsert),
            "create" => Ok(NamedAttributeWriteMode::Create),
            "replace" => Ok(NamedAttributeWriteMode::Replace),
            _ => Err(JsValue::from_str("unknown named-attribute write mode")),
        }
    }

    fn decode_authored_transactions(
        operations: JsValue,
        limits: VolumeLimits,
    ) -> Result<Vec<AuthoredMutation>, JsValue> {
        if !js_sys::Array::is_array(&operations) {
            return Err(js_error("transaction operations must be an array"));
        }
        let maximum = limits.maximum_mutations_per_batch;
        if js_sys::Array::from(&operations).length() > maximum {
            return Err(js_error(
                "transaction exceeds the configured mutation bound",
            ));
        }
        let decoded: Vec<TransactionOperation> =
            serde_wasm_bindgen::from_value(operations).map_err(js_error)?;
        if decoded.capacity() > usize::try_from(maximum).unwrap_or(usize::MAX) {
            return Err(js_error(
                "transaction exceeds the configured mutation bound",
            ));
        }
        decoded
            .into_iter()
            .map(|operation| authored_transaction(operation, limits))
            .collect()
    }

    #[allow(clippy::too_many_lines)]
    fn authored_transaction(
        operation: TransactionOperation,
        limits: VolumeLimits,
    ) -> Result<AuthoredMutation, JsValue> {
        let metadata = FileMetadata::default();
        Ok(match operation {
            TransactionOperation::CreateFile { path, bytes } => AuthoredMutation::CreateFile {
                path: browser_path(&path, limits)?,
                bytes: bytes::Bytes::from(bytes.into_vec()),
                metadata,
            },
            TransactionOperation::CreateDirectory { path } => AuthoredMutation::CreateDirectory {
                path: browser_path(&path, limits)?,
                metadata,
            },
            TransactionOperation::CreateSymbolicLink { path, target } => {
                AuthoredMutation::CreateSymbolicLink {
                    path: browser_path(&path, limits)?,
                    target: bytes::Bytes::from(target.into_vec()),
                    metadata,
                }
            }
            TransactionOperation::CreateSpecial { path, file_kind } => {
                AuthoredMutation::CreateEmptySpecial {
                    path: browser_path(&path, limits)?,
                    kind: empty_special_kind(&file_kind)?,
                    metadata,
                }
            }
            TransactionOperation::CreateDevice {
                path,
                file_kind,
                major,
                minor,
            } => AuthoredMutation::CreateDevice {
                path: browser_path(&path, limits)?,
                kind: device_kind(&file_kind)?,
                major,
                minor,
                metadata,
            },
            TransactionOperation::CreateReparsePoint { path, payload } => {
                AuthoredMutation::CreateReparsePoint {
                    path: browser_path(&path, limits)?,
                    payload: bytes::Bytes::from(payload.into_vec()),
                    metadata,
                }
            }
            TransactionOperation::Remove {
                path,
                expected_file_id,
            } => AuthoredMutation::Remove {
                path: browser_path(&path, limits)?,
                expected_file_id: expected_file_id
                    .as_ref()
                    .map(serde_bytes::ByteBuf::as_ref)
                    .map(fixed_16)
                    .transpose()?
                    .map(FileId::from_bytes),
            },
            TransactionOperation::Rename {
                source,
                destination,
                replace,
            } => AuthoredMutation::Rename {
                source: browser_path(&source, limits)?,
                destination: browser_path(&destination, limits)?,
                replace,
            },
            TransactionOperation::HardLink {
                source,
                destination,
            } => AuthoredMutation::HardLink {
                source: browser_path(&source, limits)?,
                destination: browser_path(&destination, limits)?,
            },
            TransactionOperation::Write {
                path,
                offset,
                bytes,
            } => AuthoredMutation::Write {
                path: browser_path(&path, limits)?,
                offset,
                bytes: bytes::Bytes::from(bytes.into_vec()),
            },
            TransactionOperation::SetMetadata {
                path,
                canonical_bytes,
            } => AuthoredMutation::SetMetadata {
                path: browser_path(&path, limits)?,
                metadata: decode_file_metadata(
                    canonical_bytes.as_ref(),
                    browser_decode_limits(limits),
                )
                .map_err(js_error)?,
            },
            TransactionOperation::Resize {
                path,
                logical_bytes,
            } => AuthoredMutation::Resize {
                path: browser_path(&path, limits)?,
                logical_bytes,
            },
            TransactionOperation::ZeroRange {
                path,
                offset,
                length,
                allocated,
                extend,
            } => AuthoredMutation::ZeroRange {
                path: browser_path(&path, limits)?,
                range: ByteRange { offset, length },
                allocated,
                extend,
            },
            TransactionOperation::Preallocate {
                path,
                offset,
                length,
                keep_size,
            } => AuthoredMutation::Preallocate {
                path: browser_path(&path, limits)?,
                range: ByteRange { offset, length },
                keep_size,
            },
            TransactionOperation::CloneRange {
                source,
                source_offset,
                destination,
                destination_offset,
                length,
            } => AuthoredMutation::CloneRange(FileCloneRequest {
                source: browser_path(&source, limits)?,
                source_offset,
                destination: browser_path(&destination, limits)?,
                destination_offset,
                length,
            }),
        })
    }

    fn fixed_16(bytes: &[u8]) -> Result<[u8; 16], JsValue> {
        bytes
            .try_into()
            .map_err(|_| JsValue::from_str("identity must be exactly 16 bytes"))
    }

    fn fixed_16_owned(bytes: Vec<u8>) -> Result<[u8; 16], JsValue> {
        bytes
            .try_into()
            .map_err(|_| JsValue::from_str("identity must be exactly 16 bytes"))
    }

    fn fixed_32(bytes: &[u8], label: &str) -> Result<[u8; 32], JsValue> {
        bytes
            .try_into()
            .map_err(|_| JsValue::from_str(&format!("{label} must be exactly 32 bytes")))
    }

    fn fixed_32_owned(bytes: Vec<u8>, label: &str) -> Result<[u8; 32], JsValue> {
        bytes
            .try_into()
            .map_err(|_| JsValue::from_str(&format!("{label} must be exactly 32 bytes")))
    }

    fn closed_error() -> JsValue {
        JsValue::from_str("browser filesystem is closed")
    }

    fn encode_object_id(value: ObjectId) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(33);
        bytes.push(value.kind.canonical_tag());
        bytes.extend_from_slice(value.digest.as_bytes());
        bytes
    }

    fn encode_extent_span(span: &acyclic_fs::kernel::ExtentSlice) -> ExtentSpanResult {
        let (kind, object_id, object_offset) = match &span.kind {
            ExtentKind::Hole => ("hole", None, None),
            ExtentKind::AllocatedZero => ("allocated-zero", None, None),
            ExtentKind::Content {
                object,
                object_offset,
            } => (
                "content",
                Some(encode_object_id(*object)),
                Some(object_offset.to_string()),
            ),
        };
        ExtentSpanResult {
            kind,
            offset: span.offset.to_string(),
            length: span.length.to_string(),
            source_end: span.source_end.to_string(),
            object_id,
            object_offset,
        }
    }

    fn decode_object_id(bytes: &[u8]) -> Result<ObjectId, JsValue> {
        if bytes.len() != 33 {
            return Err(JsValue::from_str(
                "object identity must be exactly 33 bytes",
            ));
        }
        let kind = ObjectKind::from_canonical_tag(bytes[0]).map_err(js_error)?;
        Ok(ObjectId {
            kind,
            digest: Digest::from_bytes(fixed_32(&bytes[1..], "object digest")?),
        })
    }

    fn encode_export_manifest(
        manifest: GenerationExportManifest,
        work: acyclic_fs::WorkCounters,
    ) -> Result<JsValue, JsValue> {
        let manifest_bytes = encode_generation_export_manifest(&manifest).map_err(js_error)?;
        serde_wasm_bindgen::to_value(&ExportManifestResult {
            manifest_bytes,
            objects: manifest.objects.into_iter().map(encode_object_id).collect(),
            work,
        })
        .map_err(js_error)
    }

    fn encode_checkpoint_result(
        receipt: &acyclic_fs::FsReceipt<acyclic_fs::GenerationId>,
    ) -> Result<JsValue, JsValue> {
        serde_wasm_bindgen::to_value(&CheckpointResult {
            generation_id: receipt.value.digest().into_bytes().to_vec(),
            work: receipt.work,
        })
        .map_err(js_error)
    }

    fn encode_generation_diff(
        diff: acyclic_fs::GenerationDiff,
        work: acyclic_fs::WorkCounters,
    ) -> Result<JsValue, JsValue> {
        serde_wasm_bindgen::to_value(&GenerationDiffResult {
            files: diff
                .files
                .into_iter()
                .map(|change| FileRecordChangeResult {
                    file_id: change.file_id.into_bytes().to_vec(),
                    before: change.before.map(encode_file_record),
                    after: change.after.map(encode_file_record),
                })
                .collect(),
            bindings: diff
                .bindings
                .into_iter()
                .map(|change| BindingChangeResult {
                    directory_id: change.directory_id.into_bytes().to_vec(),
                    name: encode_name_component(&change.name),
                    before: change.before.as_ref().map(encode_tree_entry),
                    after: change.after.as_ref().map(encode_tree_entry),
                })
                .collect(),
            truncated: diff.truncated,
            work,
        })
        .map_err(js_error)
    }

    fn browser_join_history(value: &str) -> Result<JoinHistory, JsValue> {
        match value {
            "merge" => Ok(JoinHistory::Merge),
            "rebase" => Ok(JoinHistory::Rebase),
            "squash" => Ok(JoinHistory::Squash),
            "cherry-pick" => Ok(JoinHistory::CherryPick),
            _ => Err(JsValue::from_str(
                "join history must be merge, rebase, squash, or cherry-pick",
            )),
        }
    }

    fn browser_join_result<A, O>(outcome: JoinOutcome<A, O>) -> BrowserJoinResult {
        let generation = |status, generation: Generation<A, O>| BrowserJoinResult {
            status,
            generation_id: Some(serde_bytes::ByteBuf::from(
                generation.id().digest().into_bytes().to_vec(),
            )),
            conflicts: Vec::new(),
            truncated: false,
        };
        match outcome {
            JoinOutcome::Applied(value) => generation("applied", value),
            JoinOutcome::AlreadyApplied(value) => generation("already-applied", value),
            JoinOutcome::NoChanges(value) => generation("no-changes", value),
            JoinOutcome::StaleTarget(value) => generation("stale-target", value),
            JoinOutcome::Conflicted {
                conflicts,
                truncated,
            } => BrowserJoinResult {
                status: "conflicted",
                generation_id: None,
                conflicts: conflicts.into_iter().map(encode_merge_conflict).collect(),
                truncated,
            },
            JoinOutcome::Fenced => BrowserJoinResult {
                status: "fenced",
                generation_id: None,
                conflicts: Vec::new(),
                truncated: false,
            },
            JoinOutcome::IdempotencyConflict => BrowserJoinResult {
                status: "idempotency-conflict",
                generation_id: None,
                conflicts: Vec::new(),
                truncated: false,
            },
        }
    }

    fn browser_workspace_rebase_result<A, O>(
        outcome: WorkspaceRebase<A, O>,
    ) -> BrowserWorkspaceRebaseResult {
        let generation = |status, generation: Generation<A, O>| BrowserWorkspaceRebaseResult {
            status,
            generation_id: Some(serde_bytes::ByteBuf::from(
                generation.id().digest().into_bytes().to_vec(),
            )),
            conflicts: Vec::new(),
            truncated: false,
        };
        match outcome {
            WorkspaceRebase::Rebased(value) => generation("rebased", value),
            WorkspaceRebase::AlreadyRebased(value) => generation("already-rebased", value),
            WorkspaceRebase::Current(value) => generation("current", value),
            WorkspaceRebase::Stale(value) => generation("stale", value),
            WorkspaceRebase::Conflicted {
                conflicts,
                truncated,
            } => BrowserWorkspaceRebaseResult {
                status: "conflicted",
                generation_id: None,
                conflicts: conflicts.into_iter().map(encode_merge_conflict).collect(),
                truncated,
            },
            WorkspaceRebase::Fenced => BrowserWorkspaceRebaseResult {
                status: "fenced",
                generation_id: None,
                conflicts: Vec::new(),
                truncated: false,
            },
            WorkspaceRebase::IdempotencyConflict => BrowserWorkspaceRebaseResult {
                status: "idempotency-conflict",
                generation_id: None,
                conflicts: Vec::new(),
                truncated: false,
            },
        }
    }

    fn encode_file_record(record: FileRecord) -> FileRecordResult {
        let mut result = FileRecordResult {
            file_id: record.file_id.into_bytes().to_vec(),
            file_kind: file_kind(record.kind),
            link_count: record.link_count.to_string(),
            metadata_object: encode_object_id(record.metadata),
            payload_kind: "empty",
            logical_bytes: None,
            payload_object: None,
            inline_bytes: None,
            device_major: None,
            device_minor: None,
        };
        match record.payload {
            FilePayload::InlineRegular(bytes) => {
                result.payload_kind = "inline-regular";
                result.logical_bytes = Some(
                    u64::try_from(bytes.as_bytes().len())
                        .unwrap_or(u64::MAX)
                        .to_string(),
                );
                result.inline_bytes = Some(bytes.as_bytes().to_vec());
            }
            FilePayload::Regular {
                logical_bytes,
                extents,
            } => {
                result.payload_kind = "regular";
                result.logical_bytes = Some(logical_bytes.to_string());
                result.payload_object = Some(encode_object_id(extents));
            }
            FilePayload::Directory { entries } => {
                result.payload_kind = "directory";
                result.payload_object = Some(encode_object_id(entries));
            }
            FilePayload::SymbolicLink {
                target_bytes,
                target,
            } => {
                result.payload_kind = "symbolic-link";
                result.logical_bytes = Some(target_bytes.to_string());
                result.payload_object = Some(encode_object_id(target));
            }
            FilePayload::Empty => {}
            FilePayload::Device { major, minor } => {
                result.payload_kind = "device";
                result.device_major = Some(major);
                result.device_minor = Some(minor);
            }
            FilePayload::ReparsePoint {
                payload_bytes,
                payload,
            } => {
                result.payload_kind = "reparse-point";
                result.logical_bytes = Some(payload_bytes.to_string());
                result.payload_object = Some(encode_object_id(payload));
            }
        }
        result
    }

    fn encode_tree_entry(entry: &TreeEntry) -> TreeEntryResult {
        TreeEntryResult {
            name: encode_name_component(&entry.name),
            file_id: entry.file_id.into_bytes().to_vec(),
            file_kind: file_kind(entry.kind),
        }
    }

    fn encode_name_component(name: &LogicalName) -> NameComponentResult {
        NameComponentResult {
            encoding: match name.encoding() {
                NameEncoding::Utf8 => "utf8",
                NameEncoding::PosixBytes => "posix-bytes",
                NameEncoding::WindowsUtf16Le => "windows-utf16le",
            },
            bytes: name.as_bytes().to_vec(),
        }
    }

    fn encode_merge_preparation(
        preparation: MergePreparation,
        work: acyclic_fs::WorkCounters,
    ) -> Result<JsValue, JsValue> {
        let result = match preparation {
            MergePreparation::Prepared { generation_id } => MergePreparationResult {
                status: "prepared",
                generation_id: Some(generation_id.digest().into_bytes().to_vec()),
                conflicts: Vec::new(),
                truncated: false,
                work,
            },
            MergePreparation::Conflicted {
                conflicts,
                truncated,
            } => MergePreparationResult {
                status: "conflicted",
                generation_id: None,
                conflicts: conflicts.into_iter().map(encode_merge_conflict).collect(),
                truncated,
                work,
            },
        };
        serde_wasm_bindgen::to_value(&result).map_err(js_error)
    }

    fn encode_merge_conflict(conflict: MergeConflict) -> MergeConflictResult {
        match conflict {
            MergeConflict::File(file_id) => MergeConflictResult {
                kind: "file",
                file_id: Some(file_id.into_bytes().to_vec()),
                directory_id: None,
                name: None,
            },
            MergeConflict::Binding { directory_id, name } => MergeConflictResult {
                kind: "binding",
                file_id: None,
                directory_id: Some(directory_id.into_bytes().to_vec()),
                name: Some(encode_name_component(&name)),
            },
        }
    }

    fn decode_export_manifest(
        manifest: &ImportManifest,
    ) -> Result<GenerationExportManifest, JsValue> {
        const MAXIMUM_MANIFEST_BYTES: u64 = 256 * 1024 * 1024;
        const MAXIMUM_MANIFEST_OBJECTS: u64 = 1_000_000;
        let decoded = decode_generation_export_manifest(
            &manifest.manifest_bytes,
            MAXIMUM_MANIFEST_BYTES,
            MAXIMUM_MANIFEST_OBJECTS,
        )
        .map_err(js_error)?;
        if manifest.objects.len() != decoded.objects.len() {
            return Err(JsValue::from_str(
                "manifest object list does not match canonical manifest",
            ));
        }
        for (encoded, expected) in manifest.objects.iter().zip(&decoded.objects) {
            if decode_object_id(encoded)? != *expected {
                return Err(JsValue::from_str(
                    "manifest object list does not match canonical manifest",
                ));
            }
        }
        Ok(decoded)
    }

    fn mutation_value(work: acyclic_fs::WorkCounters) -> Result<JsValue, JsValue> {
        serde_wasm_bindgen::to_value(&MutationResult {
            file_id: None,
            work,
        })
        .map_err(js_error)
    }

    fn mutation_created(receipt: &acyclic_fs::FsReceipt<FileId>) -> Result<JsValue, JsValue> {
        serde_wasm_bindgen::to_value(&MutationResult {
            file_id: Some(receipt.value.into_bytes().to_vec()),
            work: receipt.work,
        })
        .map_err(js_error)
    }

    fn commit_value(
        outcome: CheckoutCommitOutcome,
        work: acyclic_fs::WorkCounters,
    ) -> Result<JsValue, JsValue> {
        let (status, generation_id, epoch, sequence, fingerprint) = match outcome {
            CheckoutCommitOutcome::Committed {
                generation_id,
                head,
            } => (
                "committed",
                Some(generation_id.digest().into_bytes().to_vec()),
                Some(head.epoch.get().to_string()),
                Some(head.sequence.get().to_string()),
                None,
            ),
            CheckoutCommitOutcome::AlreadyCommitted {
                generation_id,
                head,
            } => (
                "already-committed",
                Some(generation_id.digest().into_bytes().to_vec()),
                Some(head.epoch.get().to_string()),
                Some(head.sequence.get().to_string()),
                None,
            ),
            CheckoutCommitOutcome::Conflict { actual } => (
                "conflict",
                None,
                Some(actual.epoch.get().to_string()),
                Some(actual.sequence.get().to_string()),
                None,
            ),
            CheckoutCommitOutcome::Fenced { actual_epoch } => (
                "fenced",
                None,
                Some(actual_epoch.get().to_string()),
                None,
                None,
            ),
            CheckoutCommitOutcome::IdempotencyConflict {
                committed_fingerprint,
            } => (
                "idempotency-conflict",
                None,
                None,
                None,
                Some(committed_fingerprint.into_bytes().to_vec()),
            ),
        };
        serde_wasm_bindgen::to_value(&CommitResult {
            status,
            generation_id,
            epoch,
            sequence,
            committed_fingerprint: fingerprint,
            work,
        })
        .map_err(js_error)
    }

    fn live_mutation_value(
        outcome: LiveMutationOutcome,
        work: acyclic_fs::WorkCounters,
    ) -> Result<JsValue, JsValue> {
        let (status, generation_id, epoch, sequence, conflict_count, truncated, fingerprint) =
            match outcome {
                LiveMutationOutcome::Committed {
                    generation_id,
                    head,
                } => (
                    "committed",
                    Some(generation_id.digest().into_bytes().to_vec()),
                    Some(head.epoch.get().to_string()),
                    Some(head.sequence.get().to_string()),
                    0,
                    false,
                    None,
                ),
                LiveMutationOutcome::AlreadyCommitted {
                    generation_id,
                    head,
                } => (
                    "already-committed",
                    Some(generation_id.digest().into_bytes().to_vec()),
                    Some(head.epoch.get().to_string()),
                    Some(head.sequence.get().to_string()),
                    0,
                    false,
                    None,
                ),
                LiveMutationOutcome::Conflicted {
                    conflicts,
                    truncated,
                } => (
                    "conflicted",
                    None,
                    None,
                    None,
                    u32::try_from(conflicts.len()).unwrap_or(u32::MAX),
                    truncated,
                    None,
                ),
                LiveMutationOutcome::RetryLimit { actual } => (
                    "retry-limit",
                    None,
                    Some(actual.epoch.get().to_string()),
                    Some(actual.sequence.get().to_string()),
                    0,
                    false,
                    None,
                ),
                LiveMutationOutcome::Fenced { actual_epoch } => (
                    "fenced",
                    None,
                    Some(actual_epoch.get().to_string()),
                    None,
                    0,
                    false,
                    None,
                ),
                LiveMutationOutcome::IdempotencyConflict {
                    committed_fingerprint,
                } => (
                    "idempotency-conflict",
                    None,
                    None,
                    None,
                    0,
                    false,
                    Some(committed_fingerprint.into_bytes().to_vec()),
                ),
            };
        serde_wasm_bindgen::to_value(&LiveMutationResult {
            status,
            generation_id,
            epoch,
            sequence,
            conflict_count,
            truncated,
            committed_fingerprint: fingerprint,
            work,
        })
        .map_err(js_error)
    }

    fn authored_live_mutation_value(
        result: acyclic_fs::AuthoredLiveMutationResult,
        work: acyclic_fs::WorkCounters,
    ) -> Result<JsValue, JsValue> {
        let value = live_mutation_value(result.outcome, work)?;
        js_sys::Reflect::set(
            &value,
            &JsValue::from_str("createdFileIds"),
            &serde_wasm_bindgen::to_value(
                &result
                    .created_file_ids
                    .into_iter()
                    .map(|identity| identity.map(|value| value.into_bytes().to_vec()))
                    .collect::<Vec<_>>(),
            )
            .map_err(js_error)?,
        )?;
        Ok(value)
    }

    fn browser_volume_config(options: VolumeOptions) -> Result<VolumeConfig, VolumeConfigError> {
        let limits = options.limits;
        VolumeConfig {
            profile: match options.profile {
                Profile::Portable => FilesystemProfile::Portable,
                Profile::Posix => FilesystemProfile::Posix,
                Profile::Windows => FilesystemProfile::Windows,
                Profile::Browser => FilesystemProfile::Browser,
            },
            concurrency: match options.concurrency {
                Concurrency::ExclusiveWriter => ConcurrencyMode::ExclusiveWriter,
                Concurrency::Optimistic => ConcurrencyMode::Optimistic,
                Concurrency::SerializedAuthority => ConcurrencyMode::SerializedAuthority,
            },
            lifecycle: match options.lifecycle {
                VolumeLifecycle::Ephemeral => Lifecycle::Ephemeral,
                VolumeLifecycle::Durable => Lifecycle::Durable,
            },
            case_sensitivity: match options.case_sensitivity {
                NameCase::Sensitive => CaseSensitivity::Sensitive,
                NameCase::ProfileFolded => CaseSensitivity::ProfileFolded,
            },
            unicode: match options.unicode {
                Unicode::Preserve => UnicodePolicy::Preserve,
                Unicode::RequireNfc => UnicodePolicy::RequireNfc,
            },
            symbolic_links: options.symbolic_links,
            hard_links: options.hard_links,
            sparse_files: options.sparse_files,
            limits: VolumeLimits {
                maximum_path_bytes: limits.maximum_path_bytes,
                maximum_component_bytes: limits.maximum_component_bytes,
                maximum_path_depth: limits.maximum_path_depth,
                maximum_object_bytes: limits.maximum_object_bytes,
                maximum_mutations_per_batch: limits.maximum_mutations_per_batch,
                maximum_paths_per_batch: limits.maximum_paths_per_batch,
                maximum_checkout_dependencies: limits.maximum_checkout_dependencies,
                maximum_directory_page_entries: limits.maximum_directory_page_entries,
                maximum_page_height: limits.maximum_page_height,
                maximum_read_bytes: limits.maximum_read_bytes,
                maximum_files_per_generation: limits.maximum_files_per_generation,
                maximum_objects_per_generation: limits.maximum_objects_per_generation,
                maximum_generation_bytes: limits.maximum_generation_bytes,
            },
        }
        .validate()
    }

    fn checkout_mode(options: &CheckoutOptions) -> CheckoutMode {
        CheckoutMode {
            access: match options.access {
                CheckoutAccess::ReadOnly => AccessMode::ReadOnly,
                CheckoutAccess::ReadWrite => AccessMode::ReadWrite,
            },
            consistency: match options.consistency {
                Consistency::Pinned => ConsistencyMode::Pinned,
                Consistency::TrackingSafe => ConsistencyMode::TrackingSafe,
                Consistency::Live => ConsistencyMode::Live,
                Consistency::Manual => ConsistencyMode::Manual,
            },
            mutations: match options.mutation_mode {
                CheckoutMutationMode::None => MutationMode::None,
                CheckoutMutationMode::PrivateCow => MutationMode::PrivateOverlay,
                CheckoutMutationMode::DirectLive => MutationMode::DirectLive,
            },
        }
    }

    fn file_kind(kind: FileKind) -> &'static str {
        match kind {
            FileKind::Regular => "regular",
            FileKind::Directory => "directory",
            FileKind::SymbolicLink => "symbolic-link",
            FileKind::Fifo => "fifo",
            FileKind::Socket => "socket",
            FileKind::CharacterDevice => "character-device",
            FileKind::BlockDevice => "block-device",
            FileKind::ReparsePoint => "reparse-point",
            FileKind::MountBoundary => "mount-boundary",
        }
    }

    fn empty_special_kind(kind: &str) -> Result<FileKind, JsValue> {
        match kind {
            "fifo" => Ok(FileKind::Fifo),
            "socket" => Ok(FileKind::Socket),
            "mount-boundary" => Ok(FileKind::MountBoundary),
            _ => Err(JsValue::from_str("unknown empty special-file kind")),
        }
    }

    fn device_kind(kind: &str) -> Result<FileKind, JsValue> {
        match kind {
            "character-device" => Ok(FileKind::CharacterDevice),
            "block-device" => Ok(FileKind::BlockDevice),
            _ => Err(JsValue::from_str("unknown device kind")),
        }
    }

    fn extent_seek_target(target: &str) -> Result<ExtentSeekTarget, JsValue> {
        match target {
            "data" => Ok(ExtentSeekTarget::Data),
            "hole" => Ok(ExtentSeekTarget::Hole),
            _ => Err(JsValue::from_str("unknown sparse seek target")),
        }
    }

    fn boundary_budget() -> WorkBudget {
        const OPERATIONS: u64 = 1_000_000;
        const BYTES: u64 = 256 * 1024 * 1024;
        WorkBudget {
            authority_records_read: OPERATIONS,
            authority_records_appended: OPERATIONS,
            authority_bytes_read: BYTES,
            authority_bytes_written: BYTES,
            object_probes: OPERATIONS,
            backend_read_operations: OPERATIONS,
            backend_write_operations: OPERATIONS,
            durability_operations: OPERATIONS,
            page_reads: OPERATIONS,
            page_writes: OPERATIONS,
            object_bytes_read: BYTES,
            object_bytes_written: BYTES,
            bytes_hashed: BYTES,
            bytes_copied: BYTES,
            bytes_encoded: BYTES,
            source_bytes_read: BYTES,
            output_bytes: BYTES,
            items_examined: OPERATIONS,
            items_returned: OPERATIONS,
            allocation_operations: OPERATIONS,
            peak_allocation_bytes: BYTES,
            materializations: OPERATIONS,
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use wasm_bindgen_test::*;

        wasm_bindgen_test_configure!(run_in_browser);

        fn test_browser_fs() -> Result<BrowserFs, JsValue> {
            let objects = MemoryObjectStore::new(1024).map_err(js_error)?;
            let objects = cached_objects(
                objects,
                ObjectCacheOptions {
                    maximum_entries: 8,
                    maximum_bytes: 1024,
                    maximum_in_flight: 2,
                    maximum_waiters_per_object: 2,
                },
            )?;
            Ok(BrowserFs {
                engine: Some(BrowserEngine::Memory(Fs::new(
                    MemoryAuthorityStore::default(),
                    objects,
                    EmbeddedCapabilities::MEMORY,
                ))),
                capabilities: Capabilities {
                    version: env!("CARGO_PKG_VERSION"),
                    platform: "browser",
                    architecture: "wasm32",
                    authority: "memory",
                    immutable_objects: "memory",
                    native_mount: "none",
                    writable_native_mount: false,
                    native_watch: false,
                    native_watch_backend: "none".to_owned(),
                    native_watch_persistent_restart: false,
                    native_watch_root_identity_fencing: false,
                    provider_process_io_observable: false,
                },
            })
        }

        const fn test_speculation_options() -> BrowserSpeculationOptions {
            BrowserSpeculationOptions {
                residency: BrowserResidencySpeculationOptions {
                    maximum_active_operations: 2,
                    maximum_active_bytes: 1024,
                    outcome_window: 8,
                    traffic_window: 8,
                    speculative_cost_basis_points: 10_000,
                    minimum_usefulness_samples: 2,
                    minimum_usefulness_basis_points: 1,
                },
                promotion: BrowserPromotionSpeculationOptions {
                    maximum_active_operations: 2,
                    maximum_active_bytes: 1024,
                    maximum_active_cost_units: 1024,
                    maximum_residency_facts: 4,
                    maximum_destinations: 4,
                    maximum_accepted_tiers: 4,
                    outcome_window: 8,
                    minimum_usefulness_samples: 2,
                    minimum_usefulness_basis_points: 1,
                },
            }
        }

        fn observe_residency(
            speculation: &BrowserSpeculation,
            operation_id: OperationId,
            volume_id: VolumeId,
            generation_id: acyclic_fs::GenerationId,
            object_id: ObjectId,
        ) -> Result<(), JsValue> {
            let observation = BrowserResidencyObservation {
                operation_id: operation_id.into_bytes().to_vec(),
                volume_id: volume_id.into_bytes().to_vec(),
                generation_id: generation_id.digest().into_bytes().to_vec(),
                foreground_bytes: 1024,
                object_id: encode_object_id(object_id),
                maximum_bytes: 16,
                reason: "sequential-range".to_owned(),
            };
            speculation.observe(serde_wasm_bindgen::to_value(&observation)?)?;
            Ok(())
        }

        fn plan_promotion(
            speculation: &BrowserSpeculation,
            operation_id: OperationId,
            object_id: ObjectId,
        ) -> Result<(), JsValue> {
            let request = BrowserPromotionRequest {
                operation_id: operation_id.into_bytes().to_vec(),
                accepted_tiers: vec!["node-local".to_owned()],
                residency: vec![BrowserObjectResidency {
                    object_id: encode_object_id(object_id),
                    location_id: vec![5; 16],
                    tier: "durable-origin".to_owned(),
                    source_priority: 0,
                }],
                destinations: vec![BrowserPromotionDestination {
                    location_id: vec![6; 16],
                    tier: "node-local".to_owned(),
                    writable: true,
                    maximum_object_bytes: 1024,
                    priority: 0,
                    cost_units_per_byte: 1,
                }],
            };
            speculation.plan_promotion(serde_wasm_bindgen::to_value(&request)?)?;
            Ok(())
        }

        #[wasm_bindgen_test]
        fn browser_owner_exposes_both_generation_fenced_speculation_engines() -> Result<(), JsValue>
        {
            let fs = test_browser_fs()?;
            let volume_id = VolumeId::from_bytes([1; 16]);
            let generation_id = acyclic_fs::GenerationId::new(Digest::from_bytes([2; 32]));
            let operation_id = OperationId::from_bytes([3; 16]);
            let object_id = ObjectId {
                kind: ObjectKind::Blob,
                digest: Digest::from_bytes([4; 32]),
            };
            let speculation = fs.create_speculation(
                volume_id.into_bytes().to_vec(),
                generation_id.digest().into_bytes().to_vec(),
                serde_wasm_bindgen::to_value(&test_speculation_options())?,
            )?;
            observe_residency(
                &speculation,
                operation_id,
                volume_id,
                generation_id,
                object_id,
            )?;
            plan_promotion(&speculation, operation_id, object_id)?;
            speculation.finish_promotion(operation_id.into_bytes().to_vec(), true)?;
            speculation.finish_residency(operation_id.into_bytes().to_vec(), true)?;
            speculation.metrics()?;
            Ok(())
        }

        #[wasm_bindgen_test(async)]
        async fn named_workspace_round_trips_binary_data_and_forks_exactly() -> Result<(), JsValue>
        {
            let fs = test_browser_fs()?;
            let workspace = fs.create_workspace("main".to_owned()).await?;
            let payload = vec![0, 255, 1, 0, 128];
            // Match separately awaited JavaScript calls without one giant debug
            // test poll frame consuming half of WebAssembly's default stack.
            let exact = Box::pin(prepare_workspace(&workspace, &payload)).await?;
            Box::pin(mutate_workspace(&workspace, &exact)).await?;
            Box::pin(verify_workspace_fork(&workspace, &payload)).await
        }

        async fn prepare_workspace(
            workspace: &BrowserWorkspace,
            payload: &[u8],
        ) -> Result<BrowserGeneration, JsValue> {
            assert_eq!(workspace.name(), "main");
            assert_eq!(workspace.id().len(), 16);
            let initial = workspace.head().await?;
            assert_eq!(initial.len(), 32);

            workspace
                .write("/binary".to_owned(), payload.to_vec())
                .await?;
            assert_eq!(workspace.read("/binary".to_owned(), 5).await?, payload);
            let exact = workspace.sync().await?;
            workspace.checkpoint("before-output".to_owned()).await?;
            exact.pin("input-generation".to_owned()).await?;
            Ok(exact)
        }

        async fn mutate_workspace(
            workspace: &BrowserWorkspace,
            exact: &BrowserGeneration,
        ) -> Result<(), JsValue> {
            let mut transaction = workspace.begin_transaction(Some(vec![9; 16])).await?;
            transaction
                .create_dir_all("/output/nested".to_owned())
                .await?;
            transaction
                .copy("/binary".to_owned(), "/output/nested/copied".to_owned())
                .await?;
            transaction
                .rename(
                    "/output/nested/copied".to_owned(),
                    "/output/result".to_owned(),
                )
                .await?;
            transaction
                .write("/output/status".to_owned(), b"ready".to_vec())
                .await?;
            transaction.commit().await?;
            assert_eq!(
                workspace.read("/output/status".to_owned(), 5).await?,
                b"ready"
            );
            assert!(exact.read("/output/status".to_owned(), 5).await.is_err());
            let exact_fork = workspace.fork_at("exact-fork".to_owned(), exact).await?;
            assert!(
                exact_fork
                    .read("/output/status".to_owned(), 5)
                    .await
                    .is_err()
            );

            Ok(())
        }

        async fn verify_workspace_fork(
            workspace: &BrowserWorkspace,
            payload: &[u8],
        ) -> Result<(), JsValue> {
            let fork = workspace.fork("fork".to_owned()).await?;
            assert_ne!(fork.id(), workspace.id());
            assert_ne!(fork.head().await?, workspace.head().await?);
            assert_eq!(fork.read("/binary".to_owned(), 5).await?, payload);

            workspace.remove("/binary".to_owned()).await?;
            assert!(workspace.read("/binary".to_owned(), 5).await.is_err());
            assert_eq!(fork.read("/binary".to_owned(), 5).await?, payload);
            Ok(())
        }
    }
}

#[cfg(target_arch = "wasm32")]
pub use bindings::{BrowserCheckout, BrowserFs, BrowserVolume, open_browser_fs, open_memory_fs};
