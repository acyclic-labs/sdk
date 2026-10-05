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
}

#[derive(Clone)]
pub(crate) struct PhysicalRoot {
    pub(crate) workspace_id: acyclic_fs::WorkspaceId,
    pub(crate) path: PathBuf,
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
        let cancellation = CancellationToken::new();
        acyclic_fs::publish_native_workspace_generation(
            workspace,
            &self.state,
            acyclic_fs::NativeWorkspacePublication {
                root: &physical.path,
                operation_directory: &directory,
                operation_id,
                from,
                to,
                options: &options,
                budget: WorkBudget::UNBOUNDED,
                cancellation: &cancellation,
            },
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
