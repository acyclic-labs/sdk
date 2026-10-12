//! Shared physical roots, background collection, and root materialization.
//! Native acquisition shares the ordinary registry and admission transaction;
//! source references identify observations, never authorize physical-root IO.
//! Native observation and warm reuse share the original-work fence/poll/source
//! invalidation transaction. Failed publication retains a real SDK rescan rather
//! than exposing stale resident source backing to lazy restore.

use super::*;
use acyclic_fs::NativeAllocation;

pub(crate) type LocalWorkspace = Workspace<LocalAuthorityBackend, LocalObjectBackend>;
pub(crate) type LocalGeneration = Generation<LocalAuthorityBackend, LocalObjectBackend>;
pub(crate) type LocalLazySource = FilteredDemandSource<NativeDemandSource>;
pub(crate) type LocalLazyWorkspace =
    LazyWorkspace<LocalAuthorityBackend, LocalObjectBackend, LocalLazySource, LocalCoreStateStore>;
pub(crate) type LocalSingleMount =
    LazyMount<LocalAuthorityBackend, LocalObjectBackend, LocalLazySource, LocalCoreStateStore>;

#[derive(Clone, Default)]
pub(crate) struct SharedRootRegistry {
    pub(crate) roots: Arc<AsyncMutex<BTreeMap<PathBuf, Arc<SharedRootSlot>>>>,
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
                let _ = fs.collect_local_garbage(Some(&store), &token).await;
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
pub(crate) struct SharedRootSlot {
    pub(crate) state: AsyncMutex<SharedRootRegistration>,
    // The canonical map key and this actual slot allocation remain admitted
    // while any registry transaction retains the slot. Map-node allocator
    // bookkeeping is opaque and excluded from logical payload accounting.
    pub(crate) _native_custody: [Option<acyclic_fs::NativeAllocation>; 2],
}

pub(crate) struct SharedSourceReference {
    pub(crate) value: Mutex<SourceReference>,
    // The reference may outlive the physical root through a cold registration.
    pub(crate) _native_custody: Option<acyclic_fs::NativeAllocation>,
}

#[derive(Default)]
pub(crate) struct SharedRootRegistration {
    pub(crate) live: Weak<SharedPhysicalRoot>,
    pub(crate) reference: Option<Arc<SharedSourceReference>>,
    pub(crate) native_identity: Option<[u8; 16]>,
    // A cold admission is not a usable root. Keep real SDK owners when a
    // later admission stage fails, until measured shutdown actually settles.
    pub(crate) pending_raw_watcher: Option<NativeWatch>,
    pub(crate) pending_watcher: Option<Arc<Mutex<NativeWatch>>>,
    pub(crate) pending_source: Option<Arc<LocalLazySource>>,
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
    pub(crate) source: Arc<LocalLazySource>,
    pub(crate) watcher: Arc<Mutex<NativeWatch>>,
    pub(crate) reference: Arc<SharedSourceReference>,
    // SDK source/watch backing carries its own resident custody. This covers
    // only the newly allocated shared physical-root body, and drops last.
    pub(crate) _native_custody: Option<acyclic_fs::NativeAllocation>,
}

pub(crate) struct SharedRootObservation {
    pub(crate) batch: WatchBatch,
    pub(crate) prior_source: SourceReference,
    pub(crate) source: SourceReference,
}

pub(crate) fn observation_consumes_changes(batch: &WatchBatch) -> bool {
    !matches!(batch, WatchBatch::Changes { changes, .. } if changes.is_empty())
}

impl SharedPhysicalRoot {
    pub(crate) async fn validate_path_identity(&self, path: PathBuf) -> Result<(), String> {
        let display_path = path.clone();
        let current = tokio::task::spawn_blocking(move || acyclic_fs::capture_root_identity(&path))
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
        self.observe_transaction(
            || "physical root reference state is poisoned".to_owned(),
            |prior_source, reference| {
                let batch = poll()?;
                let source = if observation_consumes_changes(&batch) {
                    self.source
                        .inner()
                        .invalidate(&mut acyclic_fs::demand::DemandWork::new(
                            acyclic_fs::WorkBudget::UNBOUNDED,
                            &acyclic_fs::CancellationToken::new(),
                        ))
                        .map_err(display)?
                } else {
                    prior_source
                };
                *reference = source;
                Ok(SharedRootObservation {
                    batch,
                    prior_source,
                    source,
                })
            },
        )
    }

    /// Publishes the original native watch batch and its actual new backing
    /// under the caller's original Root/source READ proof. The SDK keeps queued
    /// hint custody in the batch; the returned allocation owns only backing
    /// newly allocated by this observation.
    pub(crate) async fn observe_with_native_work<F, G>(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
        mut guard: G,
    ) -> acyclic_harness::Result<(SharedRootObservation, NativeAllocation)>
    where
        G: FnMut() -> F + Send,
        F: Future<Output = acyclic_harness::Result<()>> + Send,
    {
        use native_root_source::native_work_error;

        work.admit(acyclic_fs::WorkCounters::UNCHARGED)
            .map_err(native_work_error)?;
        guard().await?;
        work.admit(acyclic_fs::WorkCounters::UNCHARGED)
            .map_err(native_work_error)?;
        // No standard mutex guard crosses a proof await. The complete poll /
        // source invalidation / reference update remains one shared transaction.
        let observed = self.observe_native_transaction(work)?;
        // Tuple field order retains the producer guard through this last proof
        // await and drops batch data before its guard on a rejected publication.
        work.admit(acyclic_fs::WorkCounters::UNCHARGED)
            .map_err(native_work_error)?;
        guard().await?;
        work.admit(acyclic_fs::WorkCounters::UNCHARGED)
            .map_err(native_work_error)?;
        Ok(observed)
    }
    /// The shared native publication transaction, also required before a warm
    /// acquisition exposes resident source backing to lazy restore callers.
    /// The caller authenticates before entry and keeps the returned tuple
    /// through its final original-owner proof.
    pub(crate) fn observe_native_transaction(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<(SharedRootObservation, NativeAllocation)> {
        use native_root_source::{native_source_error, native_watch_error};

        self.observe_transaction(
            || acyclic_harness::Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Storage,
                reason: "physical root reference state is poisoned",
            },
            |prior_source, reference| {
                self.source.inner()
                    .validate_current(prior_source, &mut acyclic_fs::demand::NativeSourceWork::from_native_materialization_work(work))
                    .map_err(native_source_error)?;
                let mut watcher = self.watcher.lock().map_err(|_| {
                    acyclic_harness::Error::NativeBoundary {
                        code: acyclic_harness::wire::ErrorCode::Storage,
                        reason: "physical root watcher state is poisoned",
                    }
                })?;
                watcher.fence_with_native_work(WATCH_FENCE_TIMEOUT, work)
                    .map_err(native_watch_error)?;
                let mut source = prior_source;
                let (batch, allocation) = watcher
                    .poll_with_native_work_guarded(65_536, work, |batch| {
                        if observation_consumes_changes(batch) {
                            source = self.source.inner().invalidate(&mut acyclic_fs::demand::NativeSourceWork::from_native_materialization_work(work))?;
                            // The SDK source epoch has really advanced. Keep
                            // shared state aligned even if later publication
                            // fails; the watcher preserves rescan on that error.
                            *reference = source;
                        }
                        Ok(())
                    })
                    .map_err(native_watch_error)?;
                Ok((SharedRootObservation {
                    batch, prior_source, source,
                }, allocation))
            },
        )
    }

    /// Serializes the complete watcher/source publication across every session
    /// sharing this root. Native and ordinary instrumentation use this same
    /// reference transaction rather than independent observation caches.
    fn observe_transaction<T, E>(
        &self,
        poisoned: impl FnOnce() -> E,
        observe: impl FnOnce(SourceReference, &mut SourceReference) -> Result<T, E>,
    ) -> Result<T, E> {
        let mut reference = self.reference.value.lock().map_err(|_| poisoned())?;
        observe(self.source.reference(), &mut reference)
    }
}

impl SharedRootRegistry {
    pub(crate) async fn acquire(
        &self,
        root: &Path,
        admission: SharedRootAdmission,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<Arc<SharedPhysicalRoot>>, String>
    {
        native_root_source::acquire(self, root, admission, budget, cancellation).await
    }

    /// The private Root service supplies actual original owner/creator/grant
    /// verification. Its result is the authenticated held allocation identity,
    /// not permission reconstructed from the path or source reference.
    pub(crate) async fn acquire_with_native_work<F, G>(
        &self,
        root: &Path,
        admission: SharedRootAdmission,
        work: &acyclic_fs::NativeMaterializationWork,
        guard: G,
    ) -> acyclic_harness::Result<Arc<SharedPhysicalRoot>>
    where
        G: FnMut() -> F + Send,
        F: Future<Output = acyclic_harness::Result<acyclic_fs::NativeRootIdentity>> + Send,
    {
        native_root_source::acquire_with_native_work(self, root, admission, work, guard).await
    }

    /// Explicitly settles a failed cold admission using the original source
    /// owner's legitimate cleanup span. False retains the actual SDK owners.
    pub(crate) async fn shutdown_pending_with_native_work<F, G>(
        &self,
        root: &Path,
        work: &acyclic_fs::NativeMaterializationWork,
        guard: G,
    ) -> acyclic_harness::Result<bool>
    where
        G: FnMut() -> F + Send,
        F: Future<Output = acyclic_harness::Result<acyclic_fs::NativeRootIdentity>> + Send,
    {
        native_root_source::shutdown_pending_with_native_work(self, root, work, guard).await
    }

    pub(crate) async fn prune(&self) {
        self.roots.lock().await.retain(|_, registration| {
            if Arc::strong_count(registration) != 1 {
                return true;
            }
            registration.try_lock().map_or(true, |state| {
                state.live.strong_count() != 0
                    || state.pending_raw_watcher.is_some()
                    || state.pending_watcher.is_some()
                    || state.pending_source.is_some()
            })
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
            live += usize::from(registration.state.lock().await.live.strong_count() > 0);
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
        let work = acyclic_fs::NativeMaterializationWork::new(
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .map_err(|(cause, _)| display(cause))?;
        self.routes
            .get(name)
            .ok_or_else(|| "mounted root route is missing".to_owned())?
            .sync_with_permit(permit, &work, |_| {
                std::future::ready(Ok::<(), acyclic_fs::native_mount::MountLifecycleError>(()))
            })
            .await
            .map(|_| ())
            .map_err(|failure| display(failure.cause))
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
    (LocalFs, WorkspaceGraph<LocalCoreStateStore>),
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
}

#[derive(Clone)]
pub(crate) struct PhysicalRoot {
    pub(crate) workspace_id: acyclic_fs::WorkspaceId,
    pub(crate) path: PathBuf,
    pub(crate) source_root_identity: acyclic_fs::NativeRootIdentity,
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
            expected_root_identity: None,
            maximum_directory_entries: 4_096,
            maximum_extent_spans: 65_536,
            transfer_bytes: 8 * 1024 * 1024,
        };
        let cancellation = CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| PluginRootMaterializerError(error.to_string()))?;
        let from_generation = workspace
            .generation(from, &work, &cancellation)
            .await
            .map_err(|error| PluginRootMaterializerError(error.to_string()))?;
        let to_generation = workspace
            .generation(to, &work, &cancellation)
            .await
            .map_err(|error| PluginRootMaterializerError(error.to_string()))?;
        acyclic_fs::publish_native_generation_transition(
            &from_generation,
            &to_generation,
            &self.state,
            acyclic_fs::NativeWorkspacePublication {
                root: &physical.path,
                source_root_identity: physical.source_root_identity,
                operation_directory: &directory,
                operation_id,
                from,
                to,
                excluded_names: &[".git"],
                options: &options,
                budget: WorkBudget::UNBOUNDED,
                cancellation: &cancellation,
            },
            work,
            || std::future::ready(Ok(())),
        )
        .await
        .map_err(|(error, _)| PluginRootMaterializerError(error.to_string()))?;
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
        match generation
            .stat(
                &format!("/{path}"),
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
        {
            Ok(stat) if stat.kind != FileKind::Directory => {
                tracked.insert(path.clone());
            }
            Ok(_) | Err(WorkspaceError::NotFound) => {}
            Err(error) => return Err(PluginGitError(display(error))),
        }
    }
    Ok(tracked)
}
