use super::*;

/// Exact native companion capabilities returned before any filesystem work.
#[napi(object)]
#[allow(
    clippy::struct_excessive_bools,
    reason = "each flag is one independent capability reported to JavaScript"
)]
pub struct NativeCapabilities {
    pub version: String,
    pub local: bool,
    pub native_watch: bool,
    pub native_watch_backend: String,
    pub native_watch_persistent_restart: bool,
    pub native_watch_root_identity_fencing: bool,
    pub platform: String,
    pub architecture: String,
    pub native_mount: bool,
    pub writable_mount: bool,
    pub provider_process_io_observable: bool,
}

/// Embedded local filesystem owner. No daemon is required.
#[napi]
pub struct NativeFs {
    pub(in crate::bindings) inner: Arc<LocalFs>,
    pub(in crate::bindings) cancellation: CancellationToken,
}

#[napi]
impl NativeFs {
    #[napi(factory)]
    pub async fn open(root: String, object_cache: NativeObjectCacheOptions) -> Result<Self> {
        let mut options = LocalOptions::new(root);
        options.object_cache = native_object_cache_options(object_cache)?;
        let inner = LocalFs::local(options).await.map_err(napi_error)?;
        Ok(Self {
            inner: Arc::new(inner),
            cancellation: CancellationToken::new(),
        })
    }

    #[must_use]
    #[napi(getter)]
    pub fn capabilities(&self) -> NativeCapabilities {
        native_capabilities()
    }

    #[napi]
    pub fn object_cache_stats(&self) -> Result<NativeObjectCacheStats> {
        let stats = self.inner.object_cache_stats().map_err(napi_error)?;
        Ok(NativeObjectCacheStats {
            hits: bigint(stats.hits),
            decoded_hits: bigint(stats.decoded_hits),
            misses: bigint(stats.misses),
            coalesced_reads: bigint(stats.coalesced_reads),
            evictions: bigint(stats.evictions),
            resident_entries: bigint(stats.resident_entries),
            resident_bytes: bigint(stats.resident_bytes),
            resident_canonical_objects: bigint(stats.resident_canonical_objects),
            resident_canonical_bytes: bigint(stats.resident_canonical_bytes),
            resident_decoded_pages: bigint(stats.resident_decoded_pages),
            resident_decoded_bytes: bigint(stats.resident_decoded_bytes),
            in_flight: bigint(stats.in_flight),
        })
    }

    #[napi]
    pub fn clear_object_cache(&self) -> Result<()> {
        self.inner.clear_object_cache().map_err(napi_error)
    }

    #[allow(
        clippy::needless_pass_by_value,
        reason = "N-API owns JavaScript buffers at the ABI boundary"
    )]
    #[napi]
    pub fn create_speculation(
        &self,
        volume_id: Buffer,
        generation_id: Buffer,
        options: NativeSpeculationOptions,
    ) -> Result<NativeSpeculation> {
        let volume_id = VolumeId::from_bytes(fixed_16(&volume_id)?);
        let generation_id = acyclic_fs::GenerationId::new(Digest::from_bytes(fixed_32(
            &generation_id,
            "generation identity",
        )?));
        let controller = SpeculationController::new(
            native_speculation_options(&options)?,
            volume_id,
            generation_id,
        )
        .map_err(napi_error)?;
        Ok(NativeSpeculation {
            fs: Arc::clone(&self.inner),
            controller: tokio::sync::Mutex::new(controller),
            cancellation: CancellationToken::new(),
        })
    }

    #[napi]
    pub fn cancel(&self) {
        self.cancellation.cancel();
    }

    #[must_use]
    #[napi(getter)]
    pub fn cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    #[napi]
    pub async fn create_workspace(&self, name: String) -> Result<NativeWorkspace> {
        sdk_value!(unbounded_future!(self.inner, create_workspace(name)))
            .map(|inner| NativeWorkspace { inner })
    }

    #[napi(js_name = attachDirectory)]
    pub async fn attach_directory(
        &self,
        name: String,
        path: String,
        options: NativeSourceOptions,
    ) -> Result<NativeWorkspace> {
        let mode = match options.mode.as_str() {
            "pinned" => SourceMode::Pinned,
            "tracking" => SourceMode::Tracking,
            _ => {
                return Err(Error::new(
                    Status::InvalidArg,
                    "source mode must be pinned or tracking",
                ));
            }
        };
        Box::pin(
            self.inner.attach_directory(
                name,
                PathBuf::from(path),
                SourceOptions {
                    mode,
                    maximum_paths: options.maximum_paths.into_inner(),
                    maximum_extent_spans: options.maximum_extent_spans.into_inner(),
                    maximum_queued_changes: options.maximum_queued_changes.into_inner(),
                    excluded_paths: options
                        .excluded_paths
                        .unwrap_or_default()
                        .into_iter()
                        .map(|path| {
                            PortablePath::parse(&path, VolumeLimits::default()).map_err(napi_error)
                        })
                        .collect::<Result<Vec<_>>>()?,
                },
            ),
        )
        .await
        .map(|inner| NativeWorkspace { inner })
        .map_err(napi_error)
    }

    #[napi]
    pub async fn open_workspace(&self, name: String) -> Result<NativeWorkspace> {
        sdk_value!(unbounded_future!(self.inner, open_workspace(name)))
            .map(|inner| NativeWorkspace { inner })
    }

    #[napi]
    pub async fn create_volume(&self, options: NativeVolumeOptions) -> Result<NativeVolume> {
        let config = native_volume_config(options)?;
        let receipt = self
            .inner
            .create_volume(
                config,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        Ok(native_volume(receipt))
    }

    #[napi]
    pub async fn create_volume_with_id(
        &self,
        volume_id: Buffer,
        options: NativeVolumeOptions,
    ) -> Result<NativeVolume> {
        let volume_id = VolumeId::from_bytes(fixed_16(&volume_id)?);
        let config = native_volume_config(options)?;
        let receipt = self
            .inner
            .create_volume_with_id(
                volume_id,
                config,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        Ok(native_volume(receipt))
    }

    #[napi]
    pub async fn open_volume(&self, volume_id: Buffer) -> Result<NativeVolume> {
        let volume_id = VolumeId::from_bytes(fixed_16(&volume_id)?);
        let receipt = self
            .inner
            .open_volume(
                volume_id,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        Ok(native_volume(receipt))
    }

    #[napi]
    pub async fn export_object(
        &self,
        object_id: Buffer,
        maximum_bytes: BigInt,
    ) -> Result<NativeFileRead> {
        let receipt = self
            .inner
            .export_object(
                decode_object_id(&object_id)?,
                bigint_u64(&maximum_bytes, "maximumBytes")?,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        Ok(NativeFileRead {
            bytes: Buffer::from(receipt.value.bytes.to_vec()),
            work_json: work_json(&receipt.work)?,
        })
    }

    #[napi]
    pub async fn import_object(
        &self,
        object_id: Buffer,
        bytes: Buffer,
    ) -> Result<NativeMutationResult> {
        let receipt = self
            .inner
            .import_object(
                decode_object_id(&object_id)?,
                bytes.to_vec(),
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        mutation_result(receipt.work)
    }

    #[napi]
    pub async fn export_generation_batch(
        &self,
        manifest: NativeExportManifest,
        cursor: BigInt,
        maximum_objects: NapiU32,
        maximum_object_bytes: BigInt,
    ) -> Result<NativeGenerationTransferBatch> {
        let maximum_objects = maximum_objects.into_inner();

        let manifest = decode_export_manifest(&manifest)?;
        let receipt = self
            .inner
            .export_generation_batch(
                &manifest,
                TransferCursor::new(bigint_u64(&cursor, "cursor")?),
                maximum_objects,
                bigint_u64(&maximum_object_bytes, "maximumObjectBytes")?,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        Ok(NativeGenerationTransferBatch {
            first_object: BigInt::from(receipt.value.first_object.next_object()),
            next_object: receipt
                .value
                .next
                .map(|next| BigInt::from(next.next_object())),
            objects: receipt
                .value
                .objects
                .into_iter()
                .map(|object| Buffer::from(object.bytes.to_vec()))
                .collect(),
            work_json: work_json(&receipt.work)?,
        })
    }

    #[napi]
    pub async fn import_generation_batch(
        &self,
        manifest: NativeExportManifest,
        cursor: BigInt,
        objects: Vec<Buffer>,
        maximum_objects: NapiU32,
    ) -> Result<NativeGenerationTransferCursor> {
        let maximum_objects = maximum_objects.into_inner();

        let manifest = decode_export_manifest(&manifest)?;
        let objects = objects
            .into_iter()
            .map(|object| object.to_vec().into())
            .collect::<Vec<_>>();
        let receipt = self
            .inner
            .import_generation_batch(
                &manifest,
                TransferCursor::new(bigint_u64(&cursor, "cursor")?),
                &objects,
                maximum_objects,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        Ok(NativeGenerationTransferCursor {
            next_object: BigInt::from(receipt.value.next_object()),
            work_json: work_json(&receipt.work)?,
        })
    }

    #[napi]
    pub async fn restore_volume(
        &self,
        manifest: NativeExportManifest,
        operation_id: Buffer,
    ) -> Result<NativeVolume> {
        let manifest = decode_export_manifest(&manifest)?;
        let operation_id = OperationId::from_bytes(fixed_16(&operation_id)?);
        let receipt = self
            .inner
            .restore_volume(
                &manifest,
                operation_id,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        Ok(native_volume(receipt))
    }
}

fn native_volume(receipt: acyclic_fs::OperationReceipt<LocalVolume>) -> NativeVolume {
    NativeVolume {
        inner: receipt.value,
        cancellation: CancellationToken::new(),
        acquisition_work: receipt.work,
    }
}

/// Returns exact native package compatibility facts.
#[napi]
#[must_use]
pub fn native_capabilities() -> NativeCapabilities {
    let mount = probe_native_mount();
    let watch = sdk_native_watch_capabilities();
    NativeCapabilities {
        version: env!("CARGO_PKG_VERSION").to_owned(),
        local: true,
        native_watch: true,
        native_watch_backend: watch.backend.to_owned(),
        native_watch_persistent_restart: watch.persistent_restart,
        native_watch_root_identity_fencing: watch.root_identity_fencing,
        platform: std::env::consts::OS.to_owned(),
        architecture: std::env::consts::ARCH.to_owned(),
        native_mount: mount.available(),
        writable_mount: mount.available(),
        provider_process_io_observable: mount.provider_process_io_observable(),
    }
}
