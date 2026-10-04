//! Shared physical roots, background collection, and root materialization.

use super::*;

pub(crate) type LocalWorkspace = Workspace<LocalAuthorityBackend, LocalObjectBackend>;
pub(crate) type LocalGeneration = Generation<LocalAuthorityBackend, LocalObjectBackend>;
pub(crate) type LocalDistributedFs =
    DistributedFs<LocalAuthorityBackend, LocalObjectBackend, LocalCoreStateStore>;
pub(crate) type LocalLazySource = FilteredDemandSource<NativeDemandSource>;
pub(crate) type LocalLazyWorkspace =
    LazyWorkspace<LocalAuthorityBackend, LocalObjectBackend, LocalLazySource, LocalCoreStateStore>;
pub(crate) type LocalSingleMount =
    LazyMount<LocalAuthorityBackend, LocalObjectBackend, LocalLazySource, LocalCoreStateStore>;

/// Builds the one source-generation precondition used by every physical
/// writeback route. It captures the requested host paths into an ephemeral
/// exact checkout, so a concurrent user edit is observed before publication
/// and never silently replaced.
pub(crate) fn native_source_precondition(
    workspace: LocalWorkspace,
    source_root: PathBuf,
    expected_root_identity: acyclic_fs::NativeRootIdentity,
    maximum_extent_spans: u32,
) -> acyclic_fs::NativeSourcePrecondition {
    Arc::new(move |generation, paths, budget, cancellation| {
        let workspace = workspace.clone();
        let source_root = source_root.clone();
        Box::pin(async move {
            if paths.is_empty() {
                return Ok(());
            }
            let limits = workspace.limits();
            let namespace_paths = paths
                .iter()
                .map(|path| {
                    let text = format!("/{}", path.to_string_lossy().replace('\\', "/"));
                    let portable =
                        acyclic_fs::path::PortablePath::parse(&text, limits).map_err(|error| {
                            acyclic_fs::NativeTreeMaterializationError::Source(error.to_string())
                        })?;
                    acyclic_fs::kernel::NamespacePath::from_portable_in_profile(
                        &portable,
                        workspace.profile(),
                        limits,
                    )
                    .map_err(|error| {
                        acyclic_fs::NativeTreeMaterializationError::Source(error.to_string())
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let mut checkout = workspace
                .checkout(
                    acyclic_fs::model::GenerationSelector::Exact(generation),
                    acyclic_fs::model::CheckoutMode::tracking_transaction(),
                )
                .await
                .map_err(|error| {
                    acyclic_fs::NativeTreeMaterializationError::Source(error.to_string())
                })?;
            let receipt = acyclic_fs::capture_paths(
                &mut checkout,
                &namespace_paths,
                &acyclic_fs::CaptureOptions {
                    source_root,
                    expected_root_identity,
                    maximum_paths: u32::try_from(namespace_paths.len()).unwrap_or(u32::MAX),
                    maximum_extent_spans,
                },
                budget,
                &cancellation,
            )
            .await
            .map_err(|failure| match failure.error {
                acyclic_fs::CaptureError::RootChanged => {
                    acyclic_fs::NativeTreeMaterializationError::ExternalMutation(
                        "source root identity changed".to_owned(),
                    )
                }
                error => acyclic_fs::NativeTreeMaterializationError::Source(error.to_string()),
            })?;
            if receipt.value.changed_paths == 0 {
                Ok(())
            } else {
                Err(
                    acyclic_fs::NativeTreeMaterializationError::ExternalMutation(
                        "authenticated source generation changed".to_owned(),
                    ),
                )
            }
        })
    })
}

#[derive(Clone, Default)]
pub(crate) struct SharedRootRegistry {
    pub(crate) roots: Arc<AsyncMutex<BTreeMap<PathBuf, Arc<AsyncMutex<SharedRootRegistration>>>>>,
    pub(crate) collection: BackgroundCollection,
}

/// Workspaces deleted before the local store is collected again.
pub(crate) const DELETIONS_PER_COLLECTION: u64 = 32;
/// The longest the local store goes uncollected while the service runs.
pub(crate) const COLLECTION_INTERVAL: std::time::Duration =
    std::time::Duration::from_secs(24 * 60 * 60);

/// Collects the local object store in the background, off every hook's
/// path: once enough workspaces were deleted since the last collection, and
/// at least daily.
#[derive(Clone, Default)]
pub(crate) struct BackgroundCollection {
    pub(crate) deletions: Arc<std::sync::atomic::AtomicU64>,
    pub(crate) wake: Arc<tokio::sync::Notify>,
    pub(crate) task: Arc<CollectionSlot>,
}

/// The running background collection, shared by every registry clone: the
/// last clone to go stops it, so the root it holds is released even when
/// nothing stops it explicitly.
#[derive(Default)]
pub(crate) struct CollectionSlot(pub(crate) Mutex<Option<CollectionTask>>);

/// The running background collection and how to stop it.
pub(crate) struct CollectionTask {
    pub(crate) cancellation: acyclic_fs::CancellationToken,
    pub(crate) task: tokio::task::JoinHandle<()>,
}

impl CollectionSlot {
    pub(crate) fn take(&self) -> Option<CollectionTask> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }
}

impl Drop for CollectionSlot {
    fn drop(&mut self) {
        if let Some(task) = self.take() {
            task.cancellation.cancel();
        }
    }
}

impl BackgroundCollection {
    /// Counts one deleted workspace toward the next collection.
    pub(crate) fn deleted(&self) {
        if self
            .deletions
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
            + 1
            >= DELETIONS_PER_COLLECTION
        {
            self.wake.notify_one();
        }
    }

    pub(crate) fn start(&self, fs: LocalFs, store: LocalCoreStateStore) {
        let collection = LocalDistributedFs::new(fs, store);
        let cancellation = acyclic_fs::CancellationToken::new();
        let deletions = Arc::clone(&self.deletions);
        let wake = Arc::clone(&self.wake);
        let token = cancellation.clone();
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = wake.notified() => {}
                    () = tokio::time::sleep(COLLECTION_INTERVAL) => {}
                    () = token.cancelled() => return,
                }
                deletions.store(0, std::sync::atomic::Ordering::Release);
                // A failed collection sweeps nothing it should not; the next
                // one starts over.
                let _ = collection.collect_garbage(&token).await;
            }
        });
        if let Some(previous) = self
            .task
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .replace(CollectionTask { cancellation, task })
        {
            previous.cancellation.cancel();
        }
    }

    /// Stops collecting and waits for a collection in progress to end.
    pub(crate) async fn stop(&self) {
        if let Some(CollectionTask { cancellation, task }) = self.task.take() {
            cancellation.cancel();
            let _ = task.await;
        }
    }
}

#[derive(Default)]
pub(crate) struct SharedRootRegistration {
    pub(crate) live: Weak<SharedPhysicalRoot>,
    pub(crate) reference: Option<Arc<Mutex<SourceReference>>>,
    pub(crate) native_identity: Option<[u8; 16]>,
}

#[derive(Clone, Copy)]
pub(crate) enum SharedRootAdmission {
    Fresh,
    Resume(SourceReference),
}

impl SharedRootAdmission {
    pub(crate) const fn reference(self) -> Option<SourceReference> {
        match self {
            Self::Fresh => None,
            Self::Resume(reference) => Some(reference),
        }
    }
}

pub(crate) struct SharedPhysicalRoot {
    /// Retained capability used through publication.  Path re-opening is
    /// validation only; callers must pass this handle to filesystem effects.
    pub(crate) root: Arc<HostRoot>,
    pub(crate) source: Arc<LocalLazySource>,
    pub(crate) watcher: Arc<Mutex<NativeWatch>>,
    pub(crate) reference: Arc<Mutex<SourceReference>>,
}

pub(crate) struct SharedRootObservation {
    pub(crate) batch: WatchBatch,
    pub(crate) prior_source: SourceReference,
    pub(crate) source: SourceReference,
    pub(crate) consumed_changes: bool,
}

impl SharedPhysicalRoot {
    pub(crate) async fn validate_path_identity(&self, path: PathBuf) -> Result<(), String> {
        let display_path = path.clone();
        let current =
            tokio::task::spawn_blocking(move || HostRoot::open(&path).map(|root| root.identity()))
                .await
                .map_err(|error| format!("physical root validation worker failed: {error}"))?
                .map_err(display)?;
        if current != self.source.inner().root_identity() {
            return Err(format!(
                "physical root {} changed identity while it was in use",
                display_path.display()
            ));
        }
        Ok(())
    }

    pub(crate) fn observe(&self) -> Result<SharedRootObservation, String> {
        self.observe_with(|| {
            let mut watcher = self
                .watcher
                .lock()
                .map_err(|_| "physical root watcher state is poisoned".to_owned())?;
            watcher.fence(WATCH_FENCE_TIMEOUT).map_err(display)?;
            Ok(watcher
                .poll(65_536, WorkBudget::UNBOUNDED, &CancellationToken::new())
                .map_err(display)?
                .value)
        })
    }

    pub(crate) fn observe_with(
        &self,
        poll: impl FnOnce() -> Result<WatchBatch, String>,
    ) -> Result<SharedRootObservation, String> {
        // Serialize the full observation transaction across sessions sharing this root.
        // A watcher poll and the source epoch it publishes must be indivisible.
        let mut reference = self
            .reference
            .lock()
            .map_err(|_| "physical root reference state is poisoned".to_owned())?;
        let prior_source = self.source.reference();
        let batch = poll()?;
        let consumed_changes =
            !matches!(&batch, WatchBatch::Changes { changes, .. } if changes.is_empty());
        let source = if consumed_changes {
            self.source.inner().invalidate()
        } else {
            prior_source
        };
        *reference = source;
        Ok(SharedRootObservation {
            batch,
            prior_source,
            source,
            consumed_changes,
        })
    }
}

impl SharedRootRegistry {
    pub(crate) async fn acquire(
        &self,
        root: &Path,
        admission: SharedRootAdmission,
    ) -> Result<Arc<SharedPhysicalRoot>, String> {
        let canonical = root.canonicalize().map_err(display)?;
        let registration =
            {
                let mut roots = self.roots.lock().await;
                Arc::clone(roots.entry(canonical.clone()).or_insert_with(|| {
                    Arc::new(AsyncMutex::new(SharedRootRegistration::default()))
                }))
            };
        let mut registration = registration.lock().await;
        if let Some(native_identity) = registration.native_identity {
            let identity_path = canonical.clone();
            let observed_identity = tokio::task::spawn_blocking(move || {
                HostRoot::open(&identity_path).map(|root| root.identity().to_bytes())
            })
            .await
            .map_err(|error| format!("physical root validation worker failed: {error}"))?
            .map_err(display)?;
            let live = registration.live.upgrade();
            if observed_identity == native_identity {
                let retained = *registration
                    .reference
                    .as_ref()
                    .ok_or_else(|| "shared root registration lost its source reference".to_owned())?
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if admission
                    .reference()
                    .is_some_and(|expected| expected.identity != retained.identity)
                {
                    return Err(format!(
                        "physical root {} is already registered with another source identity",
                        canonical.display()
                    ));
                }
                if let Some(shared) = live {
                    shared.validate_path_identity(canonical.clone()).await?;
                    return Ok(shared);
                }
            } else {
                if live.is_some() || matches!(admission, SharedRootAdmission::Resume(_)) {
                    return Err(format!(
                        "physical root {} changed identity while it was in use",
                        canonical.display()
                    ));
                }
                *registration = SharedRootRegistration::default();
            }
        }

        let retained = registration.reference.as_ref().map(|reference| {
            *reference
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        });
        let root_handle = Arc::new(
            tokio::task::spawn_blocking({
                let canonical = canonical.clone();
                move || HostRoot::open(&canonical)
            })
            .await
            .map_err(|error| format!("physical root handle worker failed: {error}"))?
            .map_err(display)?,
        );
        let watcher_path = canonical.clone();
        let watcher = tokio::task::spawn_blocking(move || open_native_watcher(&watcher_path))
            .await
            .map_err(|error| format!("physical root watcher worker failed: {error}"))??;
        let source = open_lazy_source(
            &canonical,
            retained.or(admission.reference()),
            Arc::clone(&watcher) as Arc<dyn DemandDirectoryObserver>,
        )
        .await?;
        let watched_identity = watcher
            .lock()
            .map_err(|_| "physical root watcher state is poisoned".to_owned())?
            .root_identity()
            .to_bytes();
        if source.inner().root_identity().to_bytes() != watched_identity {
            return Err(format!(
                "physical root {} changed identity during admission",
                canonical.display()
            ));
        }
        if matches!(admission, SharedRootAdmission::Resume(_)) {
            source.inner().invalidate();
        }
        let reference = Arc::new(Mutex::new(source.reference()));
        let native_identity = source.inner().root_identity();
        let shared = Arc::new(SharedPhysicalRoot {
            root: root_handle,
            source,
            watcher,
            reference: Arc::clone(&reference),
        });
        *registration = SharedRootRegistration {
            live: Arc::downgrade(&shared),
            reference: Some(reference),
            native_identity: Some(native_identity.to_bytes()),
        };
        Ok(shared)
    }

    pub(crate) async fn prune(&self) {
        self.roots.lock().await.retain(|_, registration| {
            if Arc::strong_count(registration) != 1 {
                return true;
            }
            registration
                .try_lock()
                .map_or(true, |state| state.live.strong_count() != 0)
        });
    }

    #[cfg(test)]
    pub(crate) async fn live_roots(&self) -> usize {
        let registrations = self
            .roots
            .lock()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut live = 0;
        for registration in registrations {
            live += usize::from(registration.lock().await.live.strong_count() > 0);
        }
        live
    }
}

pub(crate) struct LocalMount {
    pub(crate) routes: BTreeMap<String, LocalSingleMount>,
}

impl LocalMount {
    pub(crate) async fn mount(
        routes: impl IntoIterator<Item = (String, LocalLazyWorkspace)>,
        destination: &Path,
    ) -> Result<Self, String> {
        fs::create_dir_all(destination).map_err(display)?;
        let mut mounted = BTreeMap::new();
        for (name, workspace) in routes {
            let path = destination.join(&name);
            fs::create_dir_all(&path).map_err(display)?;
            match workspace
                .mount(
                    &path,
                    MountOptions::read_write().publication(acyclic_fs::MountPublication::Manual),
                )
                .await
            {
                Ok(mount) => {
                    mounted.insert(name, mount);
                }
                Err(error) => {
                    for mount in mounted.values() {
                        let _ = mount.unmount().await;
                    }
                    return Err(display(error));
                }
            }
        }
        Ok(Self { routes: mounted })
    }

    pub(crate) async fn sync_route_with_permit(
        &self,
        name: &[u8],
        permit: PublicationPermit,
    ) -> Result<(), String> {
        let name = std::str::from_utf8(name).map_err(display)?;
        self.routes
            .get(name)
            .ok_or_else(|| "mounted root route is missing".to_owned())?
            .sync_with_permit(permit)
            .await
            .map_err(display)
    }

    pub(crate) async fn sync(&self) -> Result<(), String> {
        for mount in self.routes.values() {
            mount.sync().await.map_err(display)?;
        }
        Ok(())
    }

    pub(crate) async fn advance_to_head(&self) -> Result<(), String> {
        for mount in self.routes.values() {
            mount.advance_to_head().await.map_err(display)?;
        }
        Ok(())
    }

    /// Makes changes made to one route's workspace outside its mount, at
    /// `paths` and beneath them, visible through the mount.
    pub(crate) fn changed_outside(&self, name: &str, paths: &[String]) -> Result<(), String> {
        self.routes
            .get(name)
            .ok_or_else(|| "mounted root route is missing".to_owned())?
            .changed_outside(paths)
            .map_err(display)
    }

    /// Publishes every route, then detaches them all, so a publication
    /// failure leaves every route mounted and publishing again.
    pub(crate) async fn unmount(&self) -> Result<(), String> {
        self.sync().await?;
        // Each route's own unmount is exactly this publication followed by
        // its detach; publishing once per route is enough.
        self.abandon()
    }

    pub(crate) fn abandon(&self) -> Result<(), String> {
        for mount in self.routes.values() {
            mount.abandon().map_err(display)?;
        }
        Ok(())
    }
}
pub(crate) type LocalPublicationPublisher = MaterializingWorkspaceMultiRootPublisher<
    LocalAuthorityBackend,
    LocalObjectBackend,
    LocalDistributedFs,
    PluginRootMaterializer,
>;
pub(crate) type LocalPublicationCoordinator = MultiRootPublicationCoordinator<
    LocalCoreStateStore,
    LocalPublicationPublisher,
    LineageMultiRootPublicationAuthorizer<LocalCoreStateStore>,
>;

#[derive(Clone)]
pub(crate) struct PluginRootMaterializer {
    pub(crate) state: LocalCoreStateStore,
    pub(crate) physical_roots: BTreeMap<WorkspaceRootId, PhysicalRoot>,
    pub(crate) root_writeback_verifier: Arc<dyn acyclic_fs::RootWritebackApprovalVerifier>,
    pub(crate) root_writeback_budget: WorkBudget,
    pub(crate) root_writeback_cancellation: CancellationToken,
}

/// Root writeback remains closed until the Harness composition supplies its
/// durable interaction verifier.  Filesystem does not accept grants or
/// approval records from this plugin layer.
struct DenyRootWriteback;

impl acyclic_fs::RootWritebackApprovalVerifier for DenyRootWriteback {
    fn verify<'a>(
        &'a self,
        _context: acyclic_fs::RootWritebackApprovalContext,
    ) -> futures::future::BoxFuture<'a, Result<(), String>> {
        Box::pin(async {
            Err("root writeback requires Harness durable operator approval".to_owned())
        })
    }
}

pub(crate) fn default_root_writeback_verifier() -> Arc<dyn acyclic_fs::RootWritebackApprovalVerifier>
{
    Arc::new(DenyRootWriteback)
}

pub(crate) fn default_root_writeback_budget() -> WorkBudget {
    let mut budget = WorkBudget::UNBOUNDED;
    budget.authority_records_read = 1_000_000;
    budget.authority_records_appended = 1_000_000;
    budget.authority_bytes_read = 256 * 1024 * 1024;
    budget.authority_bytes_written = 256 * 1024 * 1024;
    budget.object_probes = 1_000_000;
    budget.backend_read_operations = 1_000_000;
    budget.backend_write_operations = 1_000_000;
    budget.durability_operations = 1_000_000;
    budget.page_reads = 1_000_000;
    budget.page_writes = 1_000_000;
    budget.object_bytes_read = 512 * 1024 * 1024;
    budget.object_bytes_written = 512 * 1024 * 1024;
    budget.bytes_hashed = 512 * 1024 * 1024;
    budget.bytes_copied = 512 * 1024 * 1024;
    budget.bytes_encoded = 512 * 1024 * 1024;
    budget.source_bytes_read = 512 * 1024 * 1024;
    budget.source_path_components = 2_000_000;
    budget.source_entries_visited = 2_000_000;
    budget.output_bytes = 512 * 1024 * 1024;
    budget.items_examined = 2_000_000;
    budget.items_returned = 2_000_000;
    budget.allocation_operations = 1_000_000;
    budget.peak_allocation_bytes = 512 * 1024 * 1024;
    budget.materializations = 1_000_000;
    budget
}

#[derive(Clone)]
pub(crate) struct PhysicalRoot {
    pub(crate) workspace_id: acyclic_fs::WorkspaceId,
    pub(crate) path: PathBuf,
    pub(crate) root: Arc<HostRoot>,
}

#[derive(Debug)]
pub(crate) struct PluginRootMaterializerError(pub(crate) String);

impl std::fmt::Display for PluginRootMaterializerError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for PluginRootMaterializerError {}

impl MultiRootMaterializer<LocalAuthorityBackend, LocalObjectBackend> for PluginRootMaterializer {
    type Error = PluginRootMaterializerError;

    async fn materialize(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        workspace: &LocalWorkspace,
        from: acyclic_fs::GenerationId,
        to: acyclic_fs::GenerationId,
    ) -> Result<(), Self::Error> {
        let physical = self.physical_roots.get(&root_id).ok_or_else(|| {
            PluginRootMaterializerError(
                "publication selected a root not configured for physical materialization"
                    .to_owned(),
            )
        })?;
        // Non-root parents are projected directly from their SDK workspace;
        // advancing that mount is the complete materialization operation.
        if workspace.id() != physical.workspace_id {
            return Ok(());
        }
        let directory = root_materialization_directory(&physical.path, operation_id)
            .map_err(PluginRootMaterializerError)?;
        let options = MaterializeOptions {
            destination: directory.join("target"),
            maximum_directory_entries: 4_096,
            maximum_extent_spans: 65_536,
            transfer_bytes: 8 * 1024 * 1024,
        };
        let from_generation = workspace
            .generation(from)
            .await
            .map_err(|error| PluginRootMaterializerError(error.to_string()))?;
        let to_generation = workspace
            .generation(to)
            .await
            .map_err(|error| PluginRootMaterializerError(error.to_string()))?;
        let request = acyclic_fs::HostCheckoutRootWritebackRequest::new_with_options(
            operation_id,
            from,
            to,
            &physical.path,
            &directory,
            &options,
            &[".git"],
        )
        .map_err(|error| PluginRootMaterializerError(error.to_string()))?;
        let intent = request
            .authorize_with(self.root_writeback_verifier.as_ref())
            .await
            .map_err(|error| PluginRootMaterializerError(error.to_string()))?;
        let source_precondition = native_source_precondition(
            workspace.clone(),
            physical.path.clone(),
            physical.root.identity(),
            options.maximum_extent_spans,
        );
        intent
            .publish_native(
                &from_generation,
                &to_generation,
                &self.state,
                &options,
                &[".git"],
                self.root_writeback_budget,
                &self.root_writeback_cancellation,
                self.root_writeback_verifier.as_ref(),
                Arc::clone(&physical.root),
                Some(source_precondition),
            )
            .await
            .map_err(|error| PluginRootMaterializerError(error.to_string()))?;
        Ok(())
    }

    async fn finalize(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
    ) -> Result<(), Self::Error> {
        let physical = self.physical_roots.get(&root_id).ok_or_else(|| {
            PluginRootMaterializerError(
                "publication selected a root not configured for finalization".to_owned(),
            )
        })?;
        self.state
            .remove_materialization_async(operation_id)
            .await
            .map_err(|error| PluginRootMaterializerError(error.to_string()))?;
        let directory = root_materialization_directory(&physical.path, operation_id)
            .map_err(PluginRootMaterializerError)?;
        match fs::remove_dir_all(&directory) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(PluginRootMaterializerError(error.to_string())),
        }
        Ok(())
    }
}

pub(crate) async fn existing_tracked_paths(
    generation: &LocalGeneration,
    candidates: &BTreeSet<String>,
) -> Result<BTreeSet<String>, PluginGitError> {
    let mut tracked = BTreeSet::new();
    for path in candidates {
        match generation.stat(&format!("/{path}")).await {
            Ok(stat) if stat.kind != FileKind::Directory => {
                tracked.insert(path.clone());
            }
            Ok(_) | Err(WorkspaceError::NotFound) => {}
            Err(error) => return Err(PluginGitError(display(error))),
        }
    }
    Ok(tracked)
}
