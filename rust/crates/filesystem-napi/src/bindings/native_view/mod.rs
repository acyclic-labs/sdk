use super::*;

/// Explicit native materialization bounds.
#[napi(object)]
pub struct NativeMaterializeOptions {
    /// Existing empty host directory.
    pub destination: String,
    /// Maximum entries in one authenticated directory page.
    pub maximum_directory_entries: NapiU32,
    /// Maximum spans in one authenticated extent plan.
    pub maximum_extent_spans: NapiU32,
    /// Maximum bytes in one host transfer allocation.
    pub transfer_bytes: BigInt,
}

/// Exact native materialization result.
#[napi(object)]
pub struct NativeMaterializationResult {
    /// Materialized regular-file bindings.
    pub files: BigInt,
    /// Materialized directories excluding the supplied root.
    pub directories: BigInt,
    /// Materialized symbolic links.
    pub symbolic_links: BigInt,
    /// Materialized FIFO, socket, character-device, or block-device bindings.
    pub special_files: BigInt,
    /// Logical regular-file bytes.
    pub logical_file_bytes: BigInt,
    /// Bytes physically written.
    pub written_bytes: BigInt,
    /// Exact machine-readable work receipt.
    pub work_json: String,
}

/// Exact native capture result.
#[napi(object)]
pub struct NativeCaptureResult {
    /// Paths examined.
    pub examined_paths: BigInt,
    /// Paths producing mutations.
    pub changed_paths: BigInt,
    /// File bytes streamed into immutable objects.
    pub staged_file_bytes: BigInt,
    /// Exact machine-readable work receipt.
    pub work_json: String,
}

/// Exact native watcher-capture result and durable acknowledgement boundary.
#[napi(object)]
pub struct NativeWatchCaptureResult {
    /// Watcher epoch authenticated by the captured batch.
    pub epoch: BigInt,
    /// First sequence included in the captured interval.
    pub first_sequence: BigInt,
    /// First sequence not included; safe to persist only after this result.
    pub next_sequence: BigInt,
    /// Paths examined.
    pub examined_paths: BigInt,
    /// Paths producing mutations.
    pub changed_paths: BigInt,
    /// File bytes streamed into immutable objects.
    pub staged_file_bytes: BigInt,
    /// Exact machine-readable work receipt.
    pub work_json: String,
}

/// One watcher-bound baseline reconciliation result.
#[napi(object)]
pub struct NativeWatchReconcileResult {
    /// New baseline epoch.
    pub epoch: BigInt,
    /// Atomic baseline capture receipt.
    pub baseline: NativeCaptureResult,
    /// Exact interval accumulated while the baseline was captured.
    pub post_baseline: NativeWatchBatch,
}

/// One exact native watcher hint.
#[napi(object)]
pub struct NativeWatchChange {
    /// `created`, `modified`, `metadata`, `removed`, or `renamed`.
    pub kind: String,
    /// Single-path hint; absent for renames.
    pub path: Option<NativeNamespacePath>,
    /// Rename source; present only for renames.
    pub from: Option<NativeNamespacePath>,
    /// Rename destination; present only for renames.
    pub to: Option<NativeNamespacePath>,
}

/// One contiguous or invalidated watcher interval.
#[napi(object)]
pub struct NativeWatchBatch {
    /// `changes` or `rescan-required`.
    pub status: String,
    /// Process-local watcher epoch.
    pub epoch: BigInt,
    /// First sequence for a changes batch.
    pub first_sequence: Option<BigInt>,
    /// Cursor safe after a successful capture.
    pub next_sequence: Option<BigInt>,
    /// Exact invalidation reason for a rescan batch.
    pub reason: Option<String>,
    /// Exact bounded hints.
    pub changes: Vec<NativeWatchChange>,
    /// Exact machine-readable polling work receipt.
    pub work_json: String,
}

/// One process-owned exact native watcher over a materialized checkout root.
#[napi]
pub struct NativeWatcher {
    pub(in crate::bindings) inner: std::sync::Mutex<FsNativeWatch>,
    pub(in crate::bindings) pending: std::sync::Mutex<Option<PendingWatchBatch>>,
    pub(in crate::bindings) pending_reconcile: std::sync::Mutex<Option<PendingReconcile>>,
    pub(in crate::bindings) operation: tokio::sync::Mutex<()>,
    pub(in crate::bindings) checkout:
        Arc<SharedCheckout<LocalAuthorityBackend, LocalObjectBackend>>,
    pub(in crate::bindings) source_root: PathBuf,
    pub(in crate::bindings) cancellation: CancellationToken,
}

#[derive(Clone)]
pub(in crate::bindings) struct PendingWatchBatch {
    pub(in crate::bindings) batch: WatchBatch,
    pub(in crate::bindings) poll_work: acyclic_fs::WorkCounters,
    pub(in crate::bindings) capture: Option<acyclic_fs::OperationReceipt<WatchCaptureReceipt>>,
    pub(in crate::bindings) operation_id: OperationId,
}

/// One process-owned native mount over a checkout.
#[napi]
pub struct NativeMount {
    pub(in crate::bindings) inner: std::sync::Mutex<Option<NativeMountSession>>,
    pub(in crate::bindings) mount_id: acyclic_fs::MountId,
    pub(in crate::bindings) destination: String,
}

#[napi]
impl NativeCheckout {
    /// Mounts this checkout through the target's real native namespace driver.
    #[napi]
    pub fn mount(&self, destination: String, writable: bool) -> Result<NativeMount> {
        let source = Arc::new(
            CheckoutMountSource::new(Arc::clone(&self.inner), self.config).map_err(napi_error)?,
        );
        let mount_id = acyclic_fs::MountId::new();
        let session = mount_native(
            NativeMountRequest {
                mount_id,
                volume_id: source.volume_id().map_err(napi_error)?,
                destination: destination.clone().into(),
                writable,
            },
            source,
        )
        .map_err(napi_error)?;
        Ok(NativeMount {
            inner: std::sync::Mutex::new(Some(session)),
            mount_id,
            destination,
        })
    }

    /// Explicitly materializes this checkout; never used as a mount fallback.
    #[napi]
    pub async fn materialize(
        &self,
        options: NativeMaterializeOptions,
    ) -> Result<NativeMaterializationResult> {
        let mut checkout = Box::pin(self.inner.lock()).await;
        let receipt = Box::pin(materialize_checkout(
            &mut checkout,
            &MaterializeOptions {
                destination: options.destination.into(),
                expected_root_identity: None,
                maximum_directory_entries: options.maximum_directory_entries.into_inner(),
                maximum_extent_spans: options.maximum_extent_spans.into_inner(),
                transfer_bytes: bigint_u64(&options.transfer_bytes, "transferBytes")?,
            },
            acyclic_fs::performance::BOUNDARY_BUDGET,
            &self.cancellation,
        ))
        .await
        .map_err(napi_error)?;
        Ok(NativeMaterializationResult {
            files: bigint(receipt.value.files),
            directories: bigint(receipt.value.directories),
            symbolic_links: bigint(receipt.value.symbolic_links),
            special_files: bigint(receipt.value.special_files),
            logical_file_bytes: bigint(receipt.value.logical_file_bytes),
            written_bytes: bigint(receipt.value.written_bytes),
            work_json: work_json(&receipt.work)?,
        })
    }

    /// Captures final host state for a bounded exact path set in one atomic
    #[napi]
    pub async fn capture(
        &self,
        source_root: String,
        paths: Vec<String>,
        maximum_paths: NapiU32,
        maximum_extent_spans: NapiU32,
    ) -> Result<NativeCaptureResult> {
        let maximum_paths = maximum_paths.into_inner();
        let maximum_extent_spans = maximum_extent_spans.into_inner();

        let source_root = PathBuf::from(source_root);
        let expected_root_identity = capture_root_identity(&source_root).map_err(napi_error)?;
        let paths = paths
            .iter()
            .map(|path| native_path(path, self.config))
            .collect::<Result<Vec<_>>>()?;
        let mut checkout = Box::pin(self.inner.lock()).await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = Box::pin(capture_paths(
            &mut checkout,
            &paths,
            &CaptureOptions {
                source_root,
                expected_root_identity,
                maximum_paths,
                maximum_extent_spans,
            },
            acyclic_fs::performance::BOUNDARY_BUDGET,
            &self.cancellation,
        ))
        .await
        .map_err(napi_error)?;
        Ok(NativeCaptureResult {
            examined_paths: bigint(receipt.value.examined_paths),
            changed_paths: bigint(receipt.value.changed_paths),
            staged_file_bytes: bigint(receipt.value.staged_file_bytes),
            work_json: work_json(&receipt.work)?,
        })
    }

    /// Captures the complete bounded host/checkout union in one atomic baseline.
    #[napi]
    pub async fn capture_baseline(
        &self,
        source_root: String,
        maximum_paths: NapiU32,
        maximum_extent_spans: NapiU32,
    ) -> Result<NativeCaptureResult> {
        let maximum_paths = maximum_paths.into_inner();
        let maximum_extent_spans = maximum_extent_spans.into_inner();

        let source_root = PathBuf::from(source_root);
        let expected_root_identity = capture_root_identity(&source_root).map_err(napi_error)?;
        let mut checkout = Box::pin(self.inner.lock()).await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = Box::pin(capture_baseline(
            &mut checkout,
            &CaptureOptions {
                source_root,
                expected_root_identity,
                maximum_paths,
                maximum_extent_spans,
            },
            &acyclic_fs::CapturePolicy::default(),
            acyclic_fs::performance::BOUNDARY_BUDGET,
            &self.cancellation,
        ))
        .await
        .map_err(napi_error)?;
        Ok(NativeCaptureResult {
            examined_paths: bigint(receipt.value.examined_paths),
            changed_paths: bigint(receipt.value.changed_paths),
            staged_file_bytes: bigint(receipt.value.staged_file_bytes),
            work_json: work_json(&receipt.work)?,
        })
    }

    /// Opens an exact native watcher using this volume's path bounds.
    #[napi]
    pub fn watch(
        &self,
        source_root: String,
        maximum_queued_changes: NapiU32,
        recursive: bool,
    ) -> Result<NativeWatcher> {
        let maximum_queued_changes = maximum_queued_changes.into_inner();

        let source_root = PathBuf::from(source_root);
        let watcher = FsNativeWatch::open_with_profile(
            &source_root,
            self.config.profile,
            NativeWatchOptions {
                limits: self.config.limits,
                maximum_queued_changes,
                recursive,
            },
        )
        .map_err(napi_error)?;
        Ok(NativeWatcher {
            inner: std::sync::Mutex::new(watcher),
            pending: std::sync::Mutex::new(None),
            pending_reconcile: std::sync::Mutex::new(None),
            operation: tokio::sync::Mutex::new(()),
            checkout: Arc::clone(&self.inner),
            source_root,
            cancellation: self.cancellation.clone(),
        })
    }
}

impl NativeWatcher {
    fn verify_capture_root(&self) -> Result<()> {
        let root_identity = capture_root_identity(&self.source_root).map_err(napi_error)?;
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .map_err(|(error, _)| napi_error(error))?;
        lock_state(&self.inner, "native watcher state")?
            .verify_root_identity(root_identity, &work)
            .map_err(napi_error)
    }

    async fn capture_reconcile_baseline(
        &self,
        maximum_paths: u32,
        maximum_extent_spans: u32,
    ) -> Result<PendingReconcile> {
        let operation_id = OperationId::new();
        let mut checkout = Box::pin(self.checkout.lock()).await;
        checkout
            .retain_operation_id(operation_id)
            .map_err(napi_error)?;
        let (epoch, expected_root_identity) = {
            let mut watcher = lock_state(&self.inner, "native watcher state")?;
            let expected_root_identity = watcher.root_identity();
            match watcher.begin_rescan() {
                Ok(epoch) => (epoch, expected_root_identity),
                Err(error) => {
                    checkout.clear_retained_operation(operation_id);
                    return Err(napi_error(error));
                }
            }
        };
        let baseline = Box::pin(capture_baseline(
            &mut checkout,
            &CaptureOptions {
                source_root: self.source_root.clone(),
                expected_root_identity,
                maximum_paths,
                maximum_extent_spans,
            },
            &acyclic_fs::CapturePolicy::default(),
            acyclic_fs::performance::BOUNDARY_BUDGET,
            &self.cancellation,
        ))
        .await;
        let baseline = match baseline {
            Ok(receipt) => receipt,
            Err(error) => {
                checkout.clear_retained_operation(operation_id);
                lock_state(&self.inner, "native watcher state")?
                    .abort_rescan(WatchInvalidationReason::NativeRescanRequired)
                    .map_err(napi_error)?;
                return Err(napi_error(error));
            }
        };
        Ok(PendingReconcile {
            operation_id,
            epoch,
            baseline: baseline.value,
            work: baseline.work,
        })
    }
}

#[napi]
impl NativeWatcher {
    /// Establishes an authenticated baseline while preserving events that
    #[napi]
    pub async fn reconcile(
        &self,
        maximum_paths: NapiU32,
        maximum_extent_spans: NapiU32,
    ) -> Result<NativeWatchReconcileResult> {
        let maximum_paths = maximum_paths.into_inner();
        let maximum_extent_spans = maximum_extent_spans.into_inner();

        let _operation = self.operation.lock().await;
        ensure_no_pending_interval(&self.pending)?;
        self.verify_capture_root()?;
        let existing = *lock_state(&self.pending_reconcile, "native watcher state")?;
        let pending = if let Some(pending) = existing {
            pending
        } else {
            let pending =
                Box::pin(self.capture_reconcile_baseline(maximum_paths, maximum_extent_spans))
                    .await?;
            *lock_state(&self.pending_reconcile, "native watcher state")? = Some(pending);
            pending
        };
        {
            let mut checkout = Box::pin(self.checkout.lock()).await;
            checkout
                .retain_operation_id(pending.operation_id)
                .map_err(napi_error)?;
            seal_checkout(
                &mut checkout,
                pending.operation_id,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
            checkout.clear_retained_operation(pending.operation_id);
        }
        *lock_state(&self.pending_reconcile, "native watcher state")? = None;
        let batch = lock_state(&self.inner, "native watcher state")?
            .finish_rescan()
            .map_err(napi_error)?;
        let post_baseline = encode_watch_batch(batch.clone(), acyclic_fs::WorkCounters::default())?;
        *lock_state(&self.pending, "native watcher state")? = Some(PendingWatchBatch {
            batch,
            poll_work: acyclic_fs::WorkCounters::default(),
            capture: None,
            operation_id: OperationId::new(),
        });
        Ok(NativeWatchReconcileResult {
            epoch: bigint(pending.epoch),
            baseline: NativeCaptureResult {
                examined_paths: bigint(pending.baseline.examined_paths),
                changed_paths: bigint(pending.baseline.changed_paths),
                staged_file_bytes: bigint(pending.baseline.staged_file_bytes),
                work_json: work_json(&pending.work)?,
            },
            post_baseline,
        })
    }

    /// Polls and atomically captures one watcher interval. An interval remains
    #[napi]
    pub async fn poll_capture(
        &self,
        maximum_changes: NapiU32,
        maximum_paths: NapiU32,
        maximum_extent_spans: NapiU32,
    ) -> Result<NativeWatchCaptureResult> {
        let maximum_changes = maximum_changes.into_inner();
        let maximum_paths = maximum_paths.into_inner();
        let maximum_extent_spans = maximum_extent_spans.into_inner();

        let _operation = self.operation.lock().await;
        self.verify_capture_root()?;
        let pending = {
            let pending = lock_state(&self.pending, "native watcher state")?;
            pending.clone()
        };
        let pending = if let Some(pending) = pending {
            pending
        } else {
            let receipt = lock_state(&self.inner, "native watcher state")?
                .poll(
                    maximum_changes,
                    acyclic_fs::performance::BOUNDARY_BUDGET,
                    &self.cancellation,
                )
                .map_err(napi_error)?;
            let pending = PendingWatchBatch {
                batch: receipt.value,
                poll_work: receipt.work,
                capture: None,
                operation_id: OperationId::new(),
            };
            *lock_state(&self.pending, "native watcher state")? = Some(pending.clone());
            pending
        };
        {
            let mut checkout = Box::pin(self.checkout.lock()).await;
            checkout
                .retain_operation_id(pending.operation_id)
                .map_err(napi_error)?;
        }
        let receipt = if let Some(capture) = pending.capture {
            capture
        } else {
            let expected_root_identity =
                lock_state(&self.inner, "native watcher state")?.root_identity();
            let mut checkout = Box::pin(self.checkout.lock()).await;
            let receipt = Box::pin(capture_watch_batch(
                &mut checkout,
                pending.batch,
                &CaptureOptions {
                    source_root: self.source_root.clone(),
                    expected_root_identity,
                    maximum_paths,
                    maximum_extent_spans,
                },
                &acyclic_fs::CapturePolicy::default(),
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &self.cancellation,
            ))
            .await
            .map_err(napi_error)?;
            if let Some(retained) = lock_state(&self.pending, "native watcher state")?.as_mut() {
                retained.capture = Some(receipt.clone());
            }
            receipt
        };
        {
            let mut checkout = Box::pin(self.checkout.lock()).await;
            checkout
                .retain_operation_id(pending.operation_id)
                .map_err(napi_error)?;
            seal_checkout(
                &mut checkout,
                pending.operation_id,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
            checkout.clear_retained_operation(pending.operation_id);
        }
        *lock_state(&self.pending, "native watcher state")? = None;
        let work = pending
            .poll_work
            .checked_add(receipt.work)
            .map_err(napi_error)?;
        Ok(NativeWatchCaptureResult {
            epoch: bigint(receipt.value.epoch),
            first_sequence: bigint(receipt.value.first_sequence),
            next_sequence: bigint(receipt.value.next_sequence),
            examined_paths: bigint(receipt.value.capture.examined_paths),
            changed_paths: bigint(receipt.value.capture.changed_paths),
            staged_file_bytes: bigint(receipt.value.capture.staged_file_bytes),
            work_json: work_json(&work)?,
        })
    }
}

#[napi]
impl NativeMount {
    /// Canonical 16-byte mount identity.
    #[must_use]
    #[napi(getter)]
    pub fn id(&self) -> Buffer {
        Buffer::from(self.mount_id.into_bytes().to_vec())
    }

    /// Exact native destination.
    #[must_use]
    #[napi(getter)]
    pub fn destination(&self) -> String {
        self.destination.clone()
    }

    /// Waits until everything the kernel caches through this mount reflects
    #[napi]
    pub fn revalidate(&self) -> Result<()> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .ok_or_else(|| napi_error("mount is stopped"))?
            .revalidate()
            .map_err(napi_error)
    }

    /// Stops this projection exactly once.
    #[napi]
    pub fn stop(&self) -> Result<bool> {
        let mut session = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(mut session) = session.take() else {
            return Ok(false);
        };
        session.stop().map_err(napi_error)
    }
}

pub(in crate::bindings) fn encode_watch_batch(
    batch: WatchBatch,
    work: acyclic_fs::WorkCounters,
) -> Result<NativeWatchBatch> {
    match batch {
        WatchBatch::Changes {
            epoch,
            first_sequence,
            next_sequence,
            changes,
            ..
        } => Ok(NativeWatchBatch {
            status: "changes".to_owned(),
            epoch: bigint(epoch),
            first_sequence: Some(bigint(first_sequence)),
            next_sequence: Some(bigint(next_sequence)),
            reason: None,
            changes: changes.into_iter().map(encode_watch_change).collect(),
            work_json: work_json(&work)?,
        }),
        WatchBatch::RescanRequired { epoch, reason } => Ok(NativeWatchBatch {
            status: "rescan-required".to_owned(),
            epoch: bigint(epoch),
            first_sequence: None,
            next_sequence: None,
            reason: Some(watch_reason(reason).to_owned()),
            changes: Vec::new(),
            work_json: work_json(&work)?,
        }),
    }
}

pub(in crate::bindings) fn encode_watch_change(change: WatchChange) -> NativeWatchChange {
    match change {
        WatchChange::Created(path) => single_watch_change("created", &path),
        WatchChange::Modified(path) => single_watch_change("modified", &path),
        WatchChange::Arrived(path) => single_watch_change("arrived", &path),
        WatchChange::MetadataChanged(path) => single_watch_change("metadata", &path),
        WatchChange::Removed(path) => single_watch_change("removed", &path),
        WatchChange::Renamed { from, to } => NativeWatchChange {
            kind: "renamed".to_owned(),
            path: None,
            from: Some(encode_namespace_path(&from)),
            to: Some(encode_namespace_path(&to)),
        },
    }
}

pub(in crate::bindings) fn single_watch_change(
    kind: &str,
    path: &NamespacePath,
) -> NativeWatchChange {
    NativeWatchChange {
        kind: kind.to_owned(),
        path: Some(encode_namespace_path(path)),
        from: None,
        to: None,
    }
}

pub(in crate::bindings) fn watch_reason(reason: WatchInvalidationReason) -> &'static str {
    match reason {
        WatchInvalidationReason::InitialSnapshotRequired => "initial-snapshot-required",
        WatchInvalidationReason::QueueOverflow => "queue-overflow",
        WatchInvalidationReason::NativeRescanRequired => "native-rescan-required",
        WatchInvalidationReason::BackendError => "backend-error",
        WatchInvalidationReason::UnrepresentablePath => "unrepresentable-path",
        WatchInvalidationReason::AmbiguousRename => "ambiguous-rename",
        WatchInvalidationReason::RootChanged => "root-changed",
    }
}
