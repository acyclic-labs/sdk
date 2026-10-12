use super::*;

/// Browser-safe handle backed by the canonical Rust engine.
#[wasm_bindgen]
pub struct BrowserFs {
    pub(in crate::bindings) engine: Option<Fs<BrowserAuthority, CachedObjectStore<BrowserObjects>>>,
    pub(in crate::bindings) capabilities: Capabilities,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
#[allow(
    clippy::struct_excessive_bools,
    reason = "each flag is one independent capability in the serialized report"
)]
pub(in crate::bindings) struct Capabilities {
    pub(in crate::bindings) version: &'static str,
    pub(in crate::bindings) platform: &'static str,
    pub(in crate::bindings) architecture: &'static str,
    pub(in crate::bindings) authority: &'static str,
    pub(in crate::bindings) immutable_objects: &'static str,
    pub(in crate::bindings) native_mount: &'static str,
    pub(in crate::bindings) writable_native_mount: bool,
    pub(in crate::bindings) native_watch: bool,
    pub(in crate::bindings) native_watch_backend: String,
    pub(in crate::bindings) native_watch_persistent_restart: bool,
    pub(in crate::bindings) native_watch_root_identity_fencing: bool,
    pub(in crate::bindings) provider_process_io_observable: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(in crate::bindings) struct BrowserOptions {
    pub(in crate::bindings) database_name: String,
    pub(in crate::bindings) maximum_object_bytes: u64,
    pub(in crate::bindings) object_acceleration: Acceleration,
    pub(in crate::bindings) object_cache: BrowserObjectCacheOptions,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(in crate::bindings) enum Acceleration {
    Indexeddb,
    Opfs,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(in crate::bindings) struct MemoryOptions {
    pub(in crate::bindings) maximum_object_bytes: u64,
    pub(in crate::bindings) maximum_memory_bytes: u64,
    pub(in crate::bindings) object_cache: BrowserObjectCacheOptions,
}

#[wasm_bindgen]
impl BrowserFs {
    /// Exact backend facts selected during open.
    #[wasm_bindgen(getter)]
    pub fn capabilities(&self) -> Result<JsValue, JsValue> {
        serde_wasm_bindgen::to_value(&self.capabilities).map_err(js_error)
    }

    /// Exact process-local immutable-object accelerator telemetry.
    #[wasm_bindgen(js_name = objectCacheStats)]
    pub fn object_cache_stats(&self) -> Result<JsValue, JsValue> {
        let stats = self
            .engine
            .as_ref()
            .ok_or_else(closed_error)?
            .object_cache_stats()
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
    #[wasm_bindgen(js_name = clearObjectCache)]
    pub fn clear_object_cache(&self) -> Result<(), JsValue> {
        self.engine
            .as_ref()
            .ok_or_else(closed_error)?
            .clear_object_cache()
            .map_err(js_error)
    }

    /// Creates both generation-fenced speculation engines over this
    #[wasm_bindgen(js_name = createSpeculation)]
    pub fn create_speculation(
        &self,
        volume_id: &[u8],
        generation_id: &[u8],
        options: JsValue,
    ) -> Result<BrowserSpeculation, JsValue> {
        let options: BrowserSpeculationOptions =
            serde_wasm_bindgen::from_value(options).map_err(js_error)?;
        let controller = SpeculationController::new(
            browser_speculation_options(&options),
            VolumeId::from_bytes(fixed_16(volume_id)?),
            acyclic_fs::GenerationId::new(Digest::from_bytes(fixed_32(
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

    /// Releases browser handles. Durable state remains in the selected browser stores.
    pub fn close(&mut self) {
        self.engine.take();
    }

    /// Creates or idempotently reopens one named workspace.
    #[wasm_bindgen(js_name = createWorkspace)]
    pub async fn create_workspace(&self, name: String) -> Result<BrowserWorkspace, JsValue> {
        let engine = self
            .engine
            .as_ref()
            .ok_or_else(closed_error)?
            .create_workspace(
                name,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| js_error(failure.error))?;
        Ok(BrowserWorkspace { engine })
    }

    /// Opens one existing named workspace.
    #[wasm_bindgen(js_name = openWorkspace)]
    pub async fn open_workspace(&self, name: String) -> Result<BrowserWorkspace, JsValue> {
        let engine = self
            .engine
            .as_ref()
            .ok_or_else(closed_error)?
            .open_workspace(
                name,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| js_error(failure.error))?;
        Ok(BrowserWorkspace { engine })
    }

    /// Creates one independently configured volume.
    #[wasm_bindgen(js_name = createVolume)]
    pub async fn create_volume(&self, options: JsValue) -> Result<BrowserVolume, JsValue> {
        self.create_volume_internal(None, options).await
    }

    /// Idempotently creates one caller-selected volume identity.
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
        let options: BrowserVolumeOptions =
            serde_wasm_bindgen::from_value(options).map_err(js_error)?;
        let config = browser_volume_config(options).map_err(js_error)?;
        let cancellation = CancellationToken::default();
        let (engine, acquisition_work) = {
            let fs = self.engine.as_ref().ok_or_else(closed_error)?;
            let receipt = match volume_id {
                Some(volume_id) => {
                    fs.create_volume_with_id(
                        volume_id,
                        config,
                        acyclic_fs::performance::BOUNDARY_BUDGET,
                        &cancellation,
                    )
                    .await
                }
                None => {
                    fs.create_volume(
                        config,
                        acyclic_fs::performance::BOUNDARY_BUDGET,
                        &cancellation,
                    )
                    .await
                }
            }
            .map_err(js_error)?;
            (receipt.value, receipt.work)
        };
        Ok(BrowserVolume {
            engine,
            acquisition_work,
        })
    }

    /// Opens one previously created or restored persistent volume.
    #[wasm_bindgen(js_name = openVolume)]
    pub async fn open_volume(&self, volume_id: Vec<u8>) -> Result<BrowserVolume, JsValue> {
        let volume_id = VolumeId::from_bytes(fixed_16(&volume_id)?);
        let cancellation = CancellationToken::default();
        let (engine, acquisition_work) = {
            let fs = self.engine.as_ref().ok_or_else(closed_error)?;
            let receipt = fs
                .open_volume(
                    volume_id,
                    acyclic_fs::performance::BOUNDARY_BUDGET,
                    &cancellation,
                )
                .await
                .map_err(js_error)?;
            (receipt.value, receipt.work)
        };
        Ok(BrowserVolume {
            engine,
            acquisition_work,
        })
    }

    /// Exports one exact authenticated immutable object for resumable transfer.
    #[wasm_bindgen(js_name = exportObject)]
    pub async fn export_object(
        &self,
        object_id: Vec<u8>,
        maximum_bytes: u64,
    ) -> Result<Ts<BrowserFileReadResult>, JsValue> {
        let object_id = decode_object_id(&object_id)?;
        let cancellation = CancellationToken::default();
        let receipt = self
            .engine
            .as_ref()
            .ok_or_else(closed_error)?
            .export_object(
                object_id,
                maximum_bytes,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &cancellation,
            )
            .await
            .map_err(js_error)?;
        ts(BrowserFileReadResult {
            bytes: receipt.value.bytes.to_vec(),
            work: browser_work(receipt.work),
        })
    }

    /// Idempotently imports one immutable object under its authenticated identity.
    #[wasm_bindgen(js_name = importObject)]
    pub async fn import_object(
        &self,
        object_id: Vec<u8>,
        bytes: Vec<u8>,
    ) -> Result<Ts<BrowserMutationResult>, JsValue> {
        let object_id = decode_object_id(&object_id)?;
        let bytes = acyclic_stream::OwnedBytes::from(bytes);
        let cancellation = CancellationToken::default();
        let receipt = self
            .engine
            .as_ref()
            .ok_or_else(closed_error)?
            .import_object(
                object_id,
                bytes,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &cancellation,
            )
            .await
            .map_err(js_error)?;
        mutation_value(receipt.work)
    }

    /// Exports one bounded manifest-ordered immutable-object page.
    #[wasm_bindgen(js_name = exportGenerationBatch)]
    pub async fn export_generation_batch(
        &self,
        manifest: JsValue,
        cursor: u64,
        #[wasm_bindgen(unchecked_param_type = "number")] maximum_objects: JsValue,
        maximum_object_bytes: u64,
    ) -> Result<Ts<GenerationTransferBatchResult>, JsValue> {
        let maximum_objects = wasm_u32(&maximum_objects)?;

        let manifest: ImportManifest =
            serde_wasm_bindgen::from_value(manifest).map_err(js_error)?;
        let manifest = decode_export_manifest(&manifest)?;
        let cancellation = CancellationToken::default();
        let receipt = self
            .engine
            .as_ref()
            .ok_or_else(closed_error)?
            .export_generation_batch(
                &manifest,
                TransferCursor::new(cursor),
                maximum_objects,
                maximum_object_bytes,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &cancellation,
            )
            .await
            .map_err(js_error)?;
        ts(GenerationTransferBatchResult {
            first_object: receipt.value.first_object.next_object(),
            next_object: receipt.value.next.map(|next| next.next_object()),
            objects: receipt
                .value
                .objects
                .into_iter()
                .map(|object| object.bytes.to_vec().into())
                .collect(),
            work: browser_work(receipt.work),
        })
    }

    /// Idempotently imports one manifest-aligned immutable-object page.
    #[wasm_bindgen(js_name = importGenerationBatch)]
    pub async fn import_generation_batch(
        &self,
        manifest: JsValue,
        cursor: u64,
        objects: JsValue,
        #[wasm_bindgen(unchecked_param_type = "number")] maximum_objects: JsValue,
    ) -> Result<Ts<GenerationTransferCursorResult>, JsValue> {
        let maximum_objects = wasm_u32(&maximum_objects)?;

        let manifest: ImportManifest =
            serde_wasm_bindgen::from_value(manifest).map_err(js_error)?;
        let manifest = decode_export_manifest(&manifest)?;
        let objects = serde_wasm_bindgen::from_value::<Vec<serde_bytes::ByteBuf>>(objects)
            .map_err(js_error)?
            .into_iter()
            .map(|object| acyclic_stream::OwnedBytes::from(object.into_vec()))
            .collect::<Vec<_>>();
        let cancellation = CancellationToken::default();
        let receipt = self
            .engine
            .as_ref()
            .ok_or_else(closed_error)?
            .import_generation_batch(
                &manifest,
                TransferCursor::new(cursor),
                &objects,
                maximum_objects,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &cancellation,
            )
            .await
            .map_err(js_error)?;
        ts(GenerationTransferCursorResult {
            next_object: receipt.value.next_object(),
            work: browser_work(receipt.work),
        })
    }

    /// Restores authority only after authenticating a complete imported closure.
    #[wasm_bindgen(js_name = restoreVolume)]
    pub async fn restore_volume(
        &self,
        manifest: JsValue,
        operation_id: Vec<u8>,
    ) -> Result<BrowserVolume, JsValue> {
        let manifest: ImportManifest =
            serde_wasm_bindgen::from_value(manifest).map_err(js_error)?;
        let manifest = decode_export_manifest(&manifest)?;
        let operation_id = OperationId::from_bytes(fixed_16(&operation_id)?);
        let cancellation = CancellationToken::default();
        let (engine, acquisition_work) = {
            let fs = self.engine.as_ref().ok_or_else(closed_error)?;
            let receipt = fs
                .restore_volume(
                    &manifest,
                    operation_id,
                    acyclic_fs::performance::BOUNDARY_BUDGET,
                    &cancellation,
                )
                .await
                .map_err(js_error)?;
            (receipt.value, receipt.work)
        };
        Ok(BrowserVolume {
            engine,
            acquisition_work,
        })
    }
}

/// Opens transactional browser storage with explicit `IndexedDB` or OPFS immutable objects.
#[wasm_bindgen(js_name = openBrowserFs)]
pub async fn open_browser_fs(options: JsValue) -> Result<BrowserFs, JsValue> {
    let options: BrowserOptions = serde_wasm_bindgen::from_value(options).map_err(js_error)?;
    if options.database_name.is_empty() || options.maximum_object_bytes == 0 {
        return Err(js_error("browser filesystem options are invalid"));
    }
    let authority =
        IndexedDbAuthorityStore::open(&options.database_name, options.maximum_object_bytes)
            .await
            .map_err(JsValue::from)?;
    let object_cache = browser_object_cache_options(&options.object_cache);
    let (objects, immutable_objects) = match options.object_acceleration {
        Acceleration::Indexeddb => (
            BrowserObjects::IndexedDb(
                IndexedDbObjectStore::open(&options.database_name, options.maximum_object_bytes)
                    .await
                    .map_err(JsValue::from)?,
            ),
            "indexeddb",
        ),
        Acceleration::Opfs => (
            BrowserObjects::Opfs(
                OpfsAcceleratedObjectStore::open(
                    &options.database_name,
                    options.maximum_object_bytes,
                )
                .await
                .map_err(js_error)?,
            ),
            "opfs",
        ),
    };
    let engine = Fs::new(
        BrowserAuthority::IndexedDb(authority),
        CachedObjectStore::new(objects, object_cache).map_err(js_error)?,
        EmbeddedCapabilities { durable: true },
    );
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
#[wasm_bindgen(js_name = openMemoryFs)]
pub fn open_memory_fs(options: JsValue) -> Result<BrowserFs, JsValue> {
    let options: MemoryOptions = serde_wasm_bindgen::from_value(options).map_err(js_error)?;
    if options.maximum_object_bytes == 0
        || options.maximum_memory_bytes < options.maximum_object_bytes
    {
        return Err(js_error("memory filesystem options are invalid"));
    }
    Ok(BrowserFs {
        engine: Some(memory_engine(
            options.maximum_object_bytes,
            options.maximum_memory_bytes,
            browser_object_cache_options(&options.object_cache),
        )?),
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

pub(in crate::bindings) fn memory_engine(
    maximum_object_bytes: u64,
    maximum_memory_bytes: u64,
    cache: ObjectCacheOptions,
) -> Result<Fs<BrowserAuthority, CachedObjectStore<BrowserObjects>>, JsValue> {
    let streams = Arc::new(acyclic_stream::MemoryStream::default());
    if maximum_object_bytes == 0
        || maximum_object_bytes > maximum_memory_bytes
        || usize::try_from(maximum_object_bytes).is_err()
    {
        return Err(js_error("invalid memory object limit"));
    }
    let maximum_bytes = usize::try_from(maximum_memory_bytes).map_err(js_error)?;
    if maximum_bytes == 0 {
        return Err(js_error("invalid memory byte limit"));
    }
    let (objects, bucket) = acyclic_objects::v1::MemoryObjects::with_bucket(
        "acyclic-fs-wasm-memory",
        acyclic_objects::v1::MemoryOptions {
            maximum_bytes,
            maximum_entries: usize::MAX,
        },
    )
    .map_err(js_error)?;
    Ok(Fs::new(
        BrowserAuthority::Memory(StreamAuthorityStore::new(streams)),
        CachedObjectStore::new(
            BrowserObjects::Memory(
                LogicalObjectStore::new(Arc::new(objects), bucket)
                    .with_object_limit(maximum_object_bytes)
                    .map_err(js_error)?,
            ),
            cache,
        )
        .map_err(js_error)?,
        EmbeddedCapabilities::MEMORY,
    ))
}
