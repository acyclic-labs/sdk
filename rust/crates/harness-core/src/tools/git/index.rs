//! One producer per opened namespace; clients retain only a page and their top-k.
//! Notifications wake readers, but index files are the authoritative snapshot.
use super::*;
use acyclic_fs::{NameIndex, NameIndexManifest, NameIndexRefresh};
use acyclic_stream::StreamProvider as _;
use futures::StreamExt as _;
use std::sync::{OnceLock, Weak};
use tokio::sync::watch;

type Index = NameIndex<LocalAuthorityBackend, LocalObjectBackend>;
// Workspace IDs are scoped to their authority store. Two independent local
// stores can both contain a volume named `main` with the same opaque ID.
type Registry = BTreeMap<(usize, acyclic_fs::WorkspaceId), Arc<AsyncMutex<Weak<Shared>>>>;
static INDEXES: OnceLock<Mutex<Registry>> = OnceLock::new();

struct Shared {
    registration: Arc<AsyncMutex<Weak<Shared>>>,
    latest: AsyncMutex<Option<Arc<Index>>>,
    changed: watch::Sender<u64>,
    failure: Mutex<Option<String>>,
    directory_query: Mutex<Option<String>>,
    refresh: tokio::sync::Notify,
}

impl Shared {
    async fn snapshot(&self) -> Option<Arc<Index>> {
        self.latest.lock().await.clone()
    }

    async fn retire_failed(&self, message: String) {
        let mut registered = self.registration.lock().await;
        *self
            .failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(message);
        if registered
            .upgrade()
            .is_some_and(|current| std::ptr::eq(current.as_ref(), self))
        {
            *registered = Weak::new();
        }
        self.changed.send_modify(|revision| *revision += 1);
    }

    async fn retire_if_unobserved(&self) -> bool {
        // Opening a reader holds this same lock until it subscribes. A
        // resolved closed() future is only a hint: a replacement query may
        // have subscribed since it woke the producer.
        let mut registered = self.registration.lock().await;
        if self.changed.receiver_count() != 0 {
            return false;
        }
        if registered
            .upgrade()
            .is_some_and(|current| std::ptr::eq(current.as_ref(), self))
        {
            *registered = Weak::new();
        }
        true
    }
}

/// A bounded cursor following the shared, file-backed name index.
pub struct IndexedNames {
    shared: Arc<Shared>,
    changed: watch::Receiver<u64>,
    next: u64,
    cut: Option<acyclic_fs::NameCut>,
}

/// One bounded page, or an explicit replacement cut after namespace changes.
pub struct NamePage {
    /// Clears the query's previous candidate fold before accepting more pages.
    pub reset: bool,
    /// Newly available metadata names at this cut.
    pub names: Vec<SearchName>,
}

impl IndexedNames {
    /// Follows existing index pages immediately while one producer fills the
    /// unindexed remainder. Closing one query cannot cancel the shared producer.
    pub async fn open(view: View<'_>) -> crate::Result<Self> {
        let workspace = view.workspace();
        let (provider, _) = workspace.authority_records().map_err(storage_index)?;
        let identity = (Arc::as_ptr(&provider) as usize, workspace.id());
        let entry = {
            let mut registry = INDEXES
                .get_or_init(|| Mutex::new(BTreeMap::new()))
                .lock()
                .map_err(|_| Error::Storage("pathname index registry poisoned".into()))?;
            Arc::clone(
                registry
                    .entry(identity)
                    .or_insert_with(|| Arc::new(AsyncMutex::new(Weak::new()))),
            )
        };
        let registration = Arc::clone(&entry);
        let mut entry = entry.lock().await;
        if let Some(shared) = entry.upgrade() {
            let latest = shared.snapshot().await;
            let valid = if let Some(index) = latest.as_ref() {
                reusable(view, index).await?
            } else {
                true
            };
            if !valid {
                let mut current = shared.latest.lock().await;
                // Reuse checks perform I/O without blocking page readers or
                // the producer. Do not invalidate a newer publication that
                // arrived while this snapshot was being checked.
                if current
                    .as_ref()
                    .zip(latest.as_ref())
                    .is_some_and(|(a, b)| Arc::ptr_eq(a, b))
                {
                    *current = None;
                }
                shared.refresh.notify_one();
            }
            drop(latest);
            return Ok(Self {
                changed: shared.changed.subscribe(),
                shared,
                next: 0,
                cut: None,
            });
        }
        let owned = match view {
            View::Checkout(lazy) => OwnedView::Checkout(
                lazy.open_related(
                    workspace.clone(),
                    &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()),
                )
                .await?,
            ),
            View::Volume(workspace) => OwnedView::Volume(workspace.clone()),
        };
        let native = native_follow(&owned)?;
        let existing = Index::open(workspace)
            .await
            .map_err(super::search::search_error)?;
        let mut existing = if let Some(index) = existing {
            if reusable(view, &index).await? {
                Some(index)
            } else {
                None
            }
        } else {
            None
        };
        let restart_delta = restore_watches(&owned, existing.as_ref()).await?;
        if restart_delta.all || !restart_delta.paths.is_empty() {
            if let OwnedView::Checkout(lazy) = &owned {
                lazy.invalidate_indexed_native_metadata();
            }
            existing = None;
        }
        let (changed, receiver) = watch::channel(0);
        let shared = Arc::new(Shared {
            registration,
            latest: AsyncMutex::new(existing.map(Arc::new)),
            changed,
            failure: Mutex::new(None),
            directory_query: Mutex::new(None),
            refresh: tokio::sync::Notify::new(),
        });
        *entry = Arc::downgrade(&shared);
        let producer = Arc::clone(&shared);
        tokio::spawn(async move {
            if let Err(error) = maintain(&owned, &producer, native, restart_delta).await {
                // Existing followers retain a visibly failed snapshot. They
                // must not keep later queries attached to a dead producer.
                producer.retire_failed(error.to_string()).await;
            }
        });
        Ok(Self {
            shared,
            changed: receiver,
            next: 0,
            cut: None,
        })
    }

    /// Supplies a disposable scheduling hint derived from the followed query
    /// file. The producer and durable index remain shared by all consumers;
    /// query text is never published into the volume's public index files.
    pub fn prefer_directory_names(&self, query: &str) -> crate::Result<()> {
        let mut current = self
            .shared
            .directory_query
            .lock()
            .map_err(|_| Error::Storage("index directory query lock poisoned".into()))?;
        let query = (!query.is_empty()).then_some(query);
        if current.as_deref() != query {
            *current = query.map(str::to_owned);
        }
        Ok(())
    }

    /// Reads at most one committed page; awaits publication if indexing is
    /// incomplete. `None` is returned only for a completed manifest.
    pub async fn next_page(&mut self) -> crate::Result<Option<NamePage>> {
        loop {
            self.changed.borrow_and_update();
            if let Some(message) = self
                .shared
                .failure
                .lock()
                .map_err(|_| Error::Storage("index failure lock poisoned".into()))?
                .clone()
            {
                return Err(Error::Storage(message));
            }
            let latest = self.shared.snapshot().await;
            if let Some(index) = latest.as_ref() {
                if self.cut.as_ref() != Some(&index.manifest.names_cut) {
                    self.cut = Some(index.manifest.names_cut.clone());
                    self.next = 0;
                    return Ok(Some(NamePage {
                        reset: true,
                        names: Vec::new(),
                    }));
                }
                if self.next < index.manifest.pages {
                    let page = index
                        .page(self.next)
                        .await
                        .map_err(super::search::search_error)?;
                    self.next += 1;
                    return Ok(Some(NamePage {
                        reset: false,
                        names: page,
                    }));
                }
                if index.manifest.complete {
                    return Ok(None);
                }
            }
            drop(latest);
            self.changed.changed().await.map_err(storage_index)?;
        }
    }

    /// Unfinished directory count in the latest followed manifest.
    pub async fn pending_directories(&self) -> usize {
        self.shared
            .latest
            .lock()
            .await
            .as_ref()
            .map_or(1, |index| index.manifest.pending_directories)
    }

    /// Unreadable or unrepresentable entries recorded in the durable cut.
    pub async fn unavailable_entries(&self) -> usize {
        self.shared
            .latest
            .lock()
            .await
            .as_ref()
            .map_or(0, |index| index.manifest.checkpoint.unavailable_entries())
    }

    /// Waits for an ordinary index-file revision after a terminal page. The
    /// cursor remains reusable; the next call to `next_page` emits a reset if
    /// the indexed namespace changed. Consumers never poll source directories.
    pub async fn changed(&mut self) -> crate::Result<()> {
        self.changed.changed().await.map_err(storage_index)
    }

    /// Whether the current names describe the caller's pinned content tree.
    /// Index-only transactions may advance authored generations, but source,
    /// overlay, shadow, and public pathname changes cannot be substituted.
    pub async fn matches_tree(&self, tree: GitTreeRef) -> crate::Result<bool> {
        let latest = self.shared.snapshot().await;
        let Some(index) = latest.as_ref() else {
            return Ok(false);
        };
        let (requested, indexed) = match (tree, &index.manifest.cut) {
            (GitTreeRef::Lazy(requested), acyclic_fs::NameCut::Lazy(indexed))
                if requested.workspace_id == indexed.workspace_id
                    && requested.source == indexed.source
                    && requested.overlay == indexed.overlay
                    && requested.shadows == indexed.shadows =>
            {
                (requested.authored_generation, indexed.authored_generation)
            }
            (
                GitTreeRef::Exact(requested),
                acyclic_fs::NameCut::Exact {
                    workspace,
                    generation,
                },
            ) if requested.workspace_id == *workspace => (requested.generation, *generation),
            _ => return Ok(false),
        };
        if requested == indexed {
            return Ok(true);
        }
        let workspace = index.generation().workspace();
        let requested = workspace
            .generation(requested, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|failure| failure.error)?
            .value;
        let observed = workspace
            .generation(
                index.manifest.observed_generation,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| failure.error)?
            .value;
        only_index_changes(&observed, &requested).await
    }
}

fn storage_index(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}

fn native_follow(
    view: &OwnedView,
) -> crate::Result<Option<acyclic_fs::lazy_workspace::NativeNameChanges>> {
    Ok(match view {
        OwnedView::Checkout(lazy) => {
            let cancellation = CancellationToken::new();
            let work =
                acyclic_fs::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
                    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            lazy.follow_native_names(&mut acyclic_fs::kernel::KernelAccounting::NativeOriginal {
                work: &work,
                cancellation: &cancellation,
            })?
        }
        OwnedView::Volume(_) => None,
    })
}

async fn restore_watches(
    view: &OwnedView,
    index: Option<&Index>,
) -> crate::Result<acyclic_fs::lazy_workspace::NativeNameDelta> {
    // A newly opened inotify subscription initially watches only the root.
    // Restore registrations and compare persisted per-directory tokens before
    // declaring a completed index live. Lookup registers each directory and
    // its ancestors; it never lists it or reads candidate content. Offline
    // changes cannot be hidden by reinstalling watches on a stale snapshot.
    let mut delta = acyclic_fs::lazy_workspace::NativeNameDelta::default();
    if let OwnedView::Checkout(lazy) = view
        && let Some(index) = index
        && let acyclic_fs::NameCut::Lazy(snapshot) = &index.manifest.cut
    {
        let cancellation = CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let mut accounting = acyclic_fs::kernel::KernelAccounting::NativeOriginal {
            work: &work,
            cancellation: &cancellation,
        };
        for (path, expected) in index.manifest.checkpoint.source_versions() {
            let observed = lazy
                .observe_indexed_native_metadata(*snapshot, &format!("/{path}"), &mut accounting)
                .await?;
            if observed != expected {
                // Policy content can change without touching the directory's
                // token. Rediscover that policy's subtree, not its siblings.
                metadata_changed(&mut delta, path, index.manifest.complete);
            }
        }
    }
    Ok(delta)
}

async fn maintain(
    view: &OwnedView,
    shared: &Shared,
    mut native: Option<acyclic_fs::lazy_workspace::NativeNameChanges>,
    restart_delta: acyclic_fs::lazy_workspace::NativeNameDelta,
) -> crate::Result<()> {
    let mut native_delta =
        (restart_delta.all || !restart_delta.paths.is_empty()).then_some(restart_delta);
    let probe_metadata = native.is_none() && matches!(view, OwnedView::Checkout(_));
    let mut metadata_probe = MetadataProbe::new();
    let (provider, path) = view
        .workspace()
        .authority_records()
        .map_err(storage_index)?;
    loop {
        // Establish the follow before reading the head. Commits between this
        // watermark and the snapshot are harmless duplicates; commits after
        // the snapshot cannot fall into a subscribe-after-read gap.
        let watermark = provider
            .watermarks(path.clone())
            .await
            .map_err(storage_index)?;
        let mut records = provider
            .follow(
                path.clone(),
                watermark.accepted,
                acyclic_stream::FollowFilter::default(),
            )
            .await
            .map_err(storage_index)?;
        let previous = shared.snapshot().await;
        let published = Index::open_reusing(view.workspace(), previous.as_deref())
            .await
            .map_err(super::search::search_error)?;
        let mut policy = None;
        let published = match published {
            Some(index) if reusable(view.view(), &index).await? => Some(index),
            Some(index) => {
                // The shared index has one volume-owned policy. Query callers
                // cannot replace it, and policy edits invalidate the old fold.
                let ignore = super::search::search_ignore(view.view()).await?;
                let cut = match view.view() {
                    View::Checkout(lazy) => acyclic_fs::NameCut::Lazy(
                        lazy.snapshot(
                            &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()),
                        )
                        .await?,
                    ),
                    View::Volume(workspace) => acyclic_fs::NameCut::Exact {
                        workspace: workspace.id(),
                        generation: workspace
                            .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
                            .await
                            .map_err(|failure| failure.error)?
                            .value
                            .id(),
                    },
                };
                let outcome = match (native_delta.take(), view.view(), cut) {
                    (Some(delta), View::Checkout(lazy), acyclic_fs::NameCut::Lazy(snapshot)) => {
                        index.refresh_native(lazy, snapshot, delta, &ignore).await
                    }
                    (_, _, cut) => index.refresh_authored(view.workspace(), cut, &ignore).await,
                }
                .map_err(super::search::search_error)?;
                policy = Some(ignore);
                match outcome {
                    NameIndexRefresh::Published(index) => Some(index),
                    NameIndexRefresh::Conflict => continue,
                    NameIndexRefresh::Ineligible => None,
                }
            }
            None => None,
        };
        let needs_fill = published
            .as_ref()
            .is_none_or(|index| !index.manifest.complete);
        {
            let mut latest = shared.latest.lock().await;
            let changed = latest.as_ref().map(|index| index.generation().id())
                != published.as_ref().map(|index| index.generation().id());
            *latest = published.map(Arc::new);
            if changed {
                shared.changed.send_modify(|revision| *revision += 1);
            }
        }
        if needs_fill {
            let ignore = match policy {
                Some(ignore) => ignore,
                None => super::search::search_ignore(view.view()).await?,
            };
            tokio::select! {
                biased;
                _ = shared.changed.closed() => {
                    if shared.retire_if_unobserved().await {
                        return Ok(());
                    }
                },
                changed = source_changed(&mut native) => { native_delta = Some(changed?); },
                filled = fill(view, ignore, shared) => {
                    if let Err(error) = filled {
                        if !restart_stale_walk(view, &error) {
                            return Err(error);
                        }
                        native_delta = Some(acyclic_fs::lazy_workspace::NativeNameDelta {
                            all: true,
                            ..Default::default()
                        });
                    }
                },
            }
            // Coalesce our own page publications. Reading a million-name
            // index must not make its producer replay thousands of its own
            // old notifications one by one.
            continue;
        }
        tokio::select! {
            _ = shared.changed.closed() => {
                if shared.retire_if_unobserved().await {
                    return Ok(());
                }
            },
            _ = shared.refresh.notified() => {},
            changed = source_changed(&mut native) => { native_delta = Some(changed?); },
            changed = metadata_changed_without_feed(view, shared, &mut metadata_probe, probe_metadata) => {
                native_delta = Some(changed?);
            },
            record = records.next() => {
                record.ok_or_else(|| Error::Storage("pathname index file follow ended".into()))?
                    .map_err(storage_index)?;
            }
        }
    }
}

fn restart_stale_walk(view: &OwnedView, error: &Error) -> bool {
    let OwnedView::Checkout(lazy) = view else {
        return false;
    };
    let Error::NativeSource { source, .. } = error else {
        return false;
    };
    if !matches!(
        source.semantic_error(),
        acyclic_fs::LazyWorkspaceError::StaleSource
            | acyclic_fs::LazyWorkspaceError::StaleIdentity
            | acyclic_fs::LazyWorkspaceError::StaleCursor
            | acyclic_fs::LazyWorkspaceError::Demand(
                acyclic_fs::demand::DemandError::StaleSource
                    | acyclic_fs::demand::DemandError::StaleVersion
                    | acyclic_fs::demand::DemandError::StaleCursor
            )
    ) {
        return false;
    }
    // A mutation can invalidate a continuation before a native callback is
    // delivered, or on a mount with no feed. Keep discovery live at a new cut
    // instead of leaving every later query on the same unusable checkpoint.
    lazy.invalidate_indexed_native_metadata();
    true
}

fn metadata_changed(
    delta: &mut acyclic_fs::lazy_workspace::NativeNameDelta,
    path: &str,
    complete: bool,
) {
    let root = if path == ".git/info/exclude" {
        ""
    } else if matches!(path.rsplit('/').next(), Some(".gitignore" | ".ignore")) {
        path.rsplit_once('/').map_or("", |(parent, _)| parent)
    } else {
        path
    };
    if root.is_empty() || !complete || delta.paths.len() == 512 {
        delta.all = true;
        delta.paths.clear();
    } else if !delta.all {
        delta.paths.insert(root.to_owned());
    }
}

struct MetadataProbe {
    after: Option<String>,
    next: tokio::time::Instant,
}

impl MetadataProbe {
    fn new() -> Self {
        Self {
            after: None,
            next: tokio::time::Instant::now() + std::time::Duration::from_millis(250),
        }
    }

    async fn wait(&mut self) {
        // This deadline belongs to the producer, not the select! future.
        // Cancelling a wait for an unrelated file revision cannot postpone it.
        tokio::time::sleep_until(self.next).await;
        self.next = tokio::time::Instant::now() + std::time::Duration::from_millis(250);
    }

    async fn scan<'a, F, Fut>(
        &mut self,
        versions: impl Iterator<Item = (&'a str, Option<acyclic_fs::demand::SourceVersion>)>,
        complete: bool,
        mut observe: F,
    ) -> crate::Result<acyclic_fs::lazy_workspace::NativeNameDelta>
    where
        F: FnMut(&'a str) -> Fut,
        Fut: Future<Output = crate::Result<Option<acyclic_fs::demand::SourceVersion>>>,
    {
        let mut count = 0;
        for (path, expected) in versions.take(128) {
            let observed = observe(path).await?;
            // Another authority record can cancel this scan at its next I/O.
            // Retain each completed observation, rather than repeating the
            // first part of a large range after every unrelated revision.
            self.after = Some(path.to_owned());
            count += 1;
            if observed != expected {
                let mut delta = acyclic_fs::lazy_workspace::NativeNameDelta::default();
                metadata_changed(&mut delta, path, complete);
                // Return before another await can discard a detected change.
                return Ok(delta);
            }
        }
        if count < 128 {
            self.after = None;
        }
        Ok(Default::default())
    }
}

async fn metadata_changed_without_feed(
    view: &OwnedView,
    shared: &Shared,
    probe: &mut MetadataProbe,
    enabled: bool,
) -> crate::Result<acyclic_fs::lazy_workspace::NativeNameDelta> {
    let OwnedView::Checkout(lazy) = view else {
        return std::future::pending().await;
    };
    if !enabled {
        return std::future::pending().await;
    }
    loop {
        // Network and externally managed mounts may have no usable native
        // feed. One producer checks a bounded batch of metadata tokens; it
        // never periodically lists the tree, nor allocates a cursor per agent.
        probe.wait().await;
        let Some(index) = shared.snapshot().await else {
            continue;
        };
        let acyclic_fs::NameCut::Lazy(snapshot) = &index.manifest.cut else {
            continue;
        };
        let stop = CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &stop)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let after = probe.after.clone();
        let work = &work;
        let stop = &stop;
        let delta = probe
            .scan(
                index
                    .manifest
                    .checkpoint
                    .source_versions_after(after.as_deref()),
                index.manifest.complete,
                |path| async move {
                    let mut accounting = acyclic_fs::kernel::KernelAccounting::NativeOriginal {
                        work,
                        cancellation: stop,
                    };
                    lazy.observe_indexed_native_metadata(
                        *snapshot,
                        &format!("/{path}"),
                        &mut accounting,
                    )
                    .await
                    .map_err(Error::from)
                },
            )
            .await?;
        if delta.all || !delta.paths.is_empty() {
            lazy.invalidate_indexed_native_metadata();
            return Ok(delta);
        }
    }
}

async fn source_changed(
    native: &mut Option<acyclic_fs::lazy_workspace::NativeNameChanges>,
) -> crate::Result<acyclic_fs::lazy_workspace::NativeNameDelta> {
    match native {
        Some(native) => native.changed().await.map_err(Error::from),
        None => std::future::pending().await,
    }
}

async fn reusable(view: View<'_>, index: &Index) -> crate::Result<bool> {
    let indexed_workspace = match &index.manifest.cut {
        acyclic_fs::NameCut::Lazy(cut) => cut.workspace_id,
        acyclic_fs::NameCut::Exact { workspace, .. } => *workspace,
    };
    if indexed_workspace != view.workspace().id() {
        return Ok(false);
    }
    if let (View::Checkout(lazy), acyclic_fs::NameCut::Lazy(old)) = (view, &index.manifest.cut) {
        let new = lazy
            .snapshot(&mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()))
            .await?;
        if old.source != new.source || old.overlay != new.overlay || old.shadows != new.shadows {
            return Ok(false);
        }
    } else if !matches!(
        (view, &index.manifest.cut),
        (View::Volume(_), acyclic_fs::NameCut::Exact { .. })
    ) {
        return Ok(false);
    }
    // The manifest's observed head is protected by compare-and-publish.
    // Compare that anchor with today's head. `Index::open` pins today's head,
    // which can contain later authored edits, so its generation alone is not
    // proof that the manifest is still current.
    let current = view
        .workspace()
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map_err(|failure| failure.error)?
        .value;
    let observed = view
        .workspace()
        .generation(
            index.manifest.observed_generation,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(|failure| failure.error)?
        .value;
    only_index_changes(&observed, &current).await
}

async fn only_index_changes(
    observed: &LocalGeneration,
    current: &LocalGeneration,
) -> crate::Result<bool> {
    let (diff, _) = match observed
        .diff_to(
            current,
            512,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
    {
        Ok(receipt) => receipt.value,
        Err(failure) if matches!(failure.error, WorkspaceError::JoinLimit) => return Ok(false),
        Err(failure) => return Err(failure.error.into()),
    };
    if diff.changes().truncated {
        return Ok(false);
    }
    let paths = match diff
        .changed_paths(512, WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
    {
        Ok(receipt) => receipt.value.0,
        Err(failure) if matches!(failure.error, WorkspaceError::ChangedPathLimit) => {
            // A bounded diff cannot certify a larger revision as index-only.
            // Rebuild/stream it; a large edit must not kill the producer.
            return Ok(false);
        }
        Err(failure) => return Err(failure.error.into()),
    };
    for path in paths {
        if !index_path(path.path.components()) {
            return Ok(false);
        }
    }
    Ok(true)
}

fn index_path(parts: &[acyclic_fs::kernel::LogicalName]) -> bool {
    fn equals(name: &acyclic_fs::kernel::LogicalName, text: &str) -> bool {
        use acyclic_fs::kernel::NameEncoding;
        match name.encoding() {
            NameEncoding::Utf8 | NameEncoding::PosixBytes => name.as_bytes() == text.as_bytes(),
            NameEncoding::WindowsUtf16Le => {
                let (units, rest) = name.as_bytes().as_chunks::<2>();
                rest.is_empty()
                    && units
                        .iter()
                        .map(|unit| u16::from_le_bytes(*unit))
                        .eq(text.encode_utf16())
            }
        }
    }
    parts.is_empty()
        || (equals(&parts[0], ".acyclic-sdk")
            && (parts.len() == 1 || equals(&parts[1], "name-index")))
}

async fn fill(view: &OwnedView, ignore: GitIgnorePolicy, shared: &Shared) -> crate::Result<()> {
    loop {
        if fill_pass(view, ignore.clone(), shared).await? {
            return Ok(());
        }
        // A concurrent workspace writer or index producer won publication.
        // Follow a compatible winning checkpoint; otherwise start a new cut.
        let current = Index::open(view.workspace())
            .await
            .map_err(super::search::search_error)?;
        let current = match current {
            Some(index) if reusable(view.view(), &index).await? => Some(index),
            _ => None,
        };
        *shared.latest.lock().await = current.map(Arc::new);
        shared.changed.send_modify(|revision| *revision += 1);
        tokio::task::yield_now().await;
    }
}

async fn fill_pass(
    view: &OwnedView,
    ignore: GitIgnorePolicy,
    shared: &Shared,
) -> crate::Result<bool> {
    let workspace = view.workspace();
    let mut prior = shared.snapshot().await;
    // Capture before opening a fresh walk, including its policy/metadata
    // reads. Resumed walks retain the evidence captured by their first page.
    let journal = if prior.is_none() {
        capture_baseline_journal(view)?
    } else {
        None
    };
    let (mut walk, mut pages, mut expected) = if let Some(index) = &prior {
        if index.manifest.complete {
            return Ok(true);
        }
        let walk = match view.view() {
            View::Checkout(lazy) => {
                NameWalk::resume(lazy, index.manifest.checkpoint.clone(), ignore).await
            }
            View::Volume(volume) => {
                NameWalk::resume_exact(volume, index.manifest.checkpoint.clone(), ignore).await
            }
        }
        .map_err(super::search::search_error)?;
        let pages = index.manifest.pages;
        let expected = index.generation();
        (walk, pages, expected)
    } else {
        let expected = workspace
            .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|failure| failure.error)?
            .value;
        let walk = walk_names(view.view(), None, ignore).await?;
        let authored = match walk.cut() {
            acyclic_fs::NameCut::Lazy(cut) => cut.authored_generation,
            acyclic_fs::NameCut::Exact { generation, .. } => generation,
        };
        if authored != expected.id() {
            return Ok(false);
        }
        (walk, 0, expected)
    };
    loop {
        {
            let query = shared
                .directory_query
                .lock()
                .map_err(|_| Error::Storage("index directory query lock poisoned".into()))?;
            walk.prefer_directory_names(query.as_deref());
        }
        let page = walk
            .next_page()
            .await
            .map_err(super::search::search_error)?;
        let complete = page.is_none();
        pages += u64::from(page.is_some());
        let mut checkpoint = walk.checkpoint().map_err(super::search::search_error)?;
        if let Some(journal) = journal {
            checkpoint
                .record_journal_checkpoint(journal)
                .map_err(super::search::search_error)?;
        }
        let manifest = NameIndexManifest {
            schema: acyclic_fs::NameIndexManifest::SCHEMA,
            cut: walk.cut(),
            names_cut: walk.cut(),
            observed_generation: expected.id(),
            pages,
            pending_directories: walk.pending_directories(),
            complete,
            checkpoint,
        };
        let index = Index::publish(
            workspace,
            &expected,
            manifest,
            page.as_deref(),
            prior.as_deref(),
        )
        .await
        .map_err(super::search::search_error)?;
        let Some(index) = index else {
            return Ok(false);
        };
        expected = index.generation();
        prior = Some(Arc::new(index));
        *shared.latest.lock().await = prior.clone();
        shared.changed.send_modify(|revision| *revision += 1);
        if complete {
            return Ok(true);
        }
        tokio::task::yield_now().await;
    }
}

fn capture_baseline_journal(
    view: &OwnedView,
) -> crate::Result<Option<acyclic_fs::NameJournalCheckpoint>> {
    #[cfg(windows)]
    if let OwnedView::Checkout(lazy) = view {
        let stop = CancellationToken::new();
        let mut work = acyclic_fs::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &stop)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        return match lazy.capture_indexed_native_journal(&mut work) {
            Ok(checkpoint) => Ok(Some(checkpoint)),
            Err(acyclic_fs::WindowsUsnError::Demand(error)) => Err(super::search::search_error(
                acyclic_fs::SearchError::Lazy(error.into()),
            )),
            // A mount without a readable native journal still uses the
            // metadata-token observer. No journal continuity is claimed.
            Err(_) => Ok(None),
        };
    }
    let _ = view;
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn volume_search_cancelled_metadata_batch_keeps_progress_and_returns_changes()
    -> crate::Result<()> {
        use futures::future::{Either, pending, ready};
        let paths = ["a", "b", "c"];
        let mut probe = MetadataProbe::new();
        {
            let scanning = probe.scan(paths.into_iter().map(|path| (path, None)), true, |path| {
                if path == "a" {
                    Either::Left(ready(Ok(None)))
                } else {
                    Either::Right(pending())
                }
            });
            tokio::pin!(scanning);
            assert!(futures::poll!(&mut scanning).is_pending());
            // Drop while b's metadata I/O is pending, as select! does when
            // an unrelated authority revision arrives.
        }
        // Negative control: committing only after the entire batch leaves
        // this cursor empty and repeatedly observes a on every cancellation.
        assert_eq!(probe.after.as_deref(), Some("a"));
        let after = probe.after.clone();
        let observed = std::cell::RefCell::new(Vec::new());
        let delta = probe
            .scan(
                paths
                    .into_iter()
                    .filter(|path| Some(*path) > after.as_deref())
                    .map(|path| (path, None)),
                true,
                |path| {
                    observed.borrow_mut().push(path);
                    ready(Ok(Some(acyclic_fs::demand::SourceVersion([1; 32]))))
                },
            )
            .await?;
        assert_eq!(*observed.borrow(), ["b"]);
        assert_eq!(probe.after.as_deref(), Some("b"));
        assert_eq!(delta.paths, BTreeSet::from(["b".to_owned()]));
        assert!(!delta.all);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn volume_search_metadata_deadline_survives_unrelated_file_revisions() {
        let mut probe = MetadataProbe::new();
        for _ in 0..4 {
            let waiting = probe.wait();
            tokio::pin!(waiting);
            assert!(futures::poll!(&mut waiting).is_pending());
            tokio::time::advance(std::time::Duration::from_millis(60)).await;
            // Dropping the wait models another authority record winning select!.
        }
        {
            let waiting = probe.wait();
            tokio::pin!(waiting);
            assert!(futures::poll!(&mut waiting).is_pending());
            tokio::time::advance(std::time::Duration::from_millis(10)).await;
            assert!(futures::poll!(&mut waiting).is_ready());
        }
        // Negative control: the old freshly constructed sleep starts its
        // 250 ms delay again after every record and is not due at this point.
        let restarted = tokio::time::sleep(std::time::Duration::from_millis(250));
        tokio::pin!(restarted);
        assert!(futures::poll!(&mut restarted).is_pending());
        let next = probe.wait();
        tokio::pin!(next);
        assert!(futures::poll!(&mut next).is_pending());
    }

    #[tokio::test]
    async fn volume_search_failed_producer_does_not_poison_later_queries() {
        let registration = Arc::new(AsyncMutex::new(Weak::new()));
        let make = || {
            let (changed, receiver) = watch::channel(0);
            (
                Arc::new(Shared {
                    registration: Arc::clone(&registration),
                    latest: AsyncMutex::new(None),
                    changed,
                    failure: Mutex::new(None),
                    directory_query: Mutex::new(None),
                    refresh: tokio::sync::Notify::new(),
                }),
                receiver,
            )
        };
        let (failed, mut old_reader) = make();
        *registration.lock().await = Arc::downgrade(&failed);
        let retained_old_query = Arc::clone(&failed);
        failed
            .retire_failed("source temporarily unavailable".into())
            .await;
        old_reader.changed().await.unwrap();
        assert!(failed.failure.lock().unwrap().is_some());
        // Negative control: retaining the failed query keeps its Weak alive;
        // waiting for the last client to drop would reuse this dead producer.
        assert!(Arc::downgrade(&retained_old_query).upgrade().is_some());
        assert!(registration.lock().await.upgrade().is_none());
        let (replacement, _new_reader) = make();
        *registration.lock().await = Arc::downgrade(&replacement);
        failed.retire_failed("late old failure".into()).await;
        assert!(Arc::ptr_eq(
            &registration.lock().await.upgrade().unwrap(),
            &replacement
        ));
        assert!(replacement.failure.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn volume_search_replacement_subscription_cancels_idle_retirement() {
        let registration = Arc::new(AsyncMutex::new(Weak::new()));
        let (changed, reader) = watch::channel(0);
        let shared = Arc::new(Shared {
            registration: Arc::clone(&registration),
            latest: AsyncMutex::new(None),
            changed,
            failure: Mutex::new(None),
            directory_query: Mutex::new(None),
            refresh: tokio::sync::Notify::new(),
        });
        *registration.lock().await = Arc::downgrade(&shared);
        drop(reader);
        // Negative control: the old unconditional closed() branch is already
        // ready and would exit even after a replacement query subscribed.
        shared.changed.closed().await;
        let opening = registration.lock().await;
        let retirement = shared.retire_if_unobserved();
        tokio::pin!(retirement);
        assert!(futures::poll!(&mut retirement).is_pending());
        let replacement = shared.changed.subscribe();
        drop(opening);
        assert!(!retirement.await);
        assert!(Arc::ptr_eq(
            &registration.lock().await.upgrade().unwrap(),
            &shared,
        ));
        drop(replacement);
        assert!(shared.retire_if_unobserved().await);
        // Keeping the retired Shared alive cannot make a later opener reuse
        // it: deregistration happens before the producer drops its Arc.
        assert!(registration.lock().await.upgrade().is_none());
        assert_eq!(Arc::strong_count(&shared), 1);
    }

    async fn unwatched_checkout(root: &Path, storage: &Path) -> crate::Result<LocalLazyWorkspace> {
        use acyclic_fs::{Lifecycle, LocalOptions, VolumeConfig};
        acyclic_native_runtime::initialize_native_io_services().map_err(storage_index)?;
        let fs = LocalFs::local(LocalOptions::new(storage.join("fs")))
            .await
            .map_err(storage_index)?;
        let store = LocalCoreStateStore::open_owned(storage.join("core")).map_err(storage_index)?;
        let config = VolumeConfig::native(Lifecycle::Durable);
        let stop = CancellationToken::new();
        let source = NativeDemandSource::open(
            std::borrow::Cow::Borrowed(root),
            config.profile,
            config.limits,
            None,
            None,
            WorkBudget::UNBOUNDED,
            &stop,
        )
        .await
        .map_err(|failure| Error::from(acyclic_fs::LazyWorkspaceError::Demand(failure.error)))?
        .value;
        let (lazy, _) = LazyWorkspace::attach(
            &fs,
            "unwatched",
            Arc::new(source),
            store,
            acyclic_fs::VolumeInitialization::new(config, None),
            OperationId::new(),
            &mut WorkBudget::UNBOUNDED.accounting(&stop),
        )
        .await?;
        Ok(lazy)
    }

    #[tokio::test]
    async fn volume_search_stale_directory_checkpoint_recovers_without_a_feed() -> crate::Result<()>
    {
        let temp = tempfile::tempdir().map_err(storage_index)?;
        let root = temp.path().join("root");
        std::fs::create_dir(&root).map_err(storage_index)?;
        for ordinal in 0..300 {
            std::fs::write(root.join(format!("before-{ordinal:03}.txt")), b"unread")
                .map_err(storage_index)?;
        }
        let lazy = unwatched_checkout(&root, temp.path()).await?;
        let view = Arc::new(OwnedView::Checkout(lazy));
        let ignore = super::super::search::search_ignore(view.view()).await?;
        let mut walk = walk_names(view.view(), None, ignore.clone()).await?;
        let first = loop {
            let page = walk
                .next_page()
                .await
                .map_err(super::super::search::search_error)?
                .expect("unfinished native directory");
            if !page.is_empty() {
                break page;
            }
        };
        let checkpoint = walk
            .checkpoint()
            .map_err(super::super::search::search_error)?;
        let cut = walk.cut();
        let expected = view
            .workspace()
            .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|failure| failure.error)?
            .value;
        let manifest = NameIndexManifest {
            schema: acyclic_fs::NameIndexManifest::SCHEMA,
            cut: cut.clone(),
            names_cut: cut.clone(),
            observed_generation: expected.id(),
            pages: 1,
            pending_directories: walk.pending_directories(),
            complete: false,
            checkpoint: checkpoint.clone(),
        };
        let partial = Index::publish(view.workspace(), &expected, manifest, Some(&first), None)
            .await
            .map_err(super::super::search::search_error)?
            .unwrap();
        std::fs::rename(root.join("before-000.txt"), root.join("after.txt"))
            .map_err(storage_index)?;
        let OwnedView::Checkout(lazy) = view.as_ref() else {
            unreachable!()
        };
        let mut stale = NameWalk::resume(lazy, checkpoint, ignore)
            .await
            .map_err(super::super::search::search_error)?;
        // Negative control: resuming the persisted native cookie at its old
        // directory version fails before the producer's recovery is applied.
        let rejected = stale.next_page().await.err().expect("stale continuation");
        assert!(matches!(
            rejected,
            acyclic_fs::SearchError::Lazy(
                acyclic_fs::LazyWorkspaceError::StaleCursor
                    | acyclic_fs::LazyWorkspaceError::Demand(
                        acyclic_fs::demand::DemandError::StaleCursor
                    )
            )
        ));
        let (changed, receiver) = watch::channel(0);
        let shared = Arc::new(Shared {
            registration: Arc::new(AsyncMutex::new(Weak::new())),
            latest: AsyncMutex::new(Some(Arc::new(partial))),
            changed,
            failure: Mutex::new(None),
            directory_query: Mutex::new(None),
            refresh: tokio::sync::Notify::new(),
        });
        let mut names = IndexedNames {
            shared: Arc::clone(&shared),
            changed: receiver,
            next: 1,
            cut: Some(cut),
        };
        let producer_view = Arc::clone(&view);
        let producer_shared = Arc::clone(&shared);
        let producer = tokio::spawn(async move {
            maintain(&producer_view, &producer_shared, None, Default::default()).await
        });
        let scenario = async {
            let mut found = BTreeSet::new();
            while let Some(page) = names.next_page().await? {
                if page.reset {
                    found.clear();
                }
                found.extend(
                    page.names
                        .into_iter()
                        .filter(|name| name.regular)
                        .map(|name| name.path),
                );
            }
            assert_eq!(found.len(), 300);
            assert!(found.contains("after.txt"));
            assert!(!found.contains("before-000.txt"));
            Ok::<_, Error>(())
        };
        let result = tokio::time::timeout(std::time::Duration::from_secs(2), scenario).await;
        producer.abort();
        result.map_err(storage_index)?
    }

    #[tokio::test]
    async fn volume_search_unwatched_mount_follows_metadata_changes() -> crate::Result<()> {
        let temp = tempfile::tempdir().map_err(storage_index)?;
        let root = temp.path().join("root");
        std::fs::create_dir_all(root.join("nested")).map_err(storage_index)?;
        std::fs::write(root.join("nested/before.txt"), b"unread content").map_err(storage_index)?;
        let lazy = unwatched_checkout(&root, temp.path()).await?;
        let view = Arc::new(OwnedView::Checkout(lazy));
        let (changed, receiver) = watch::channel(0);
        let shared = Arc::new(Shared {
            registration: Arc::new(AsyncMutex::new(Weak::new())),
            latest: AsyncMutex::new(None),
            changed,
            failure: Mutex::new(None),
            directory_query: Mutex::new(None),
            refresh: tokio::sync::Notify::new(),
        });
        let mut names = IndexedNames {
            shared: Arc::clone(&shared),
            changed: receiver,
            next: 0,
            cut: None,
        };
        let producer_view = Arc::clone(&view);
        let producer_shared = Arc::clone(&shared);
        // This is the same producer used when subscribe() returns no feed.
        // Do not install a local watcher as an accidental freshness oracle.
        let producer = tokio::spawn(async move {
            maintain(&producer_view, &producer_shared, None, Default::default()).await
        });
        let scenario = async {
            let mut found_before = false;
            while let Some(page) = names.next_page().await? {
                found_before |= page
                    .names
                    .iter()
                    .any(|name| name.path == "nested/before.txt");
            }
            assert!(found_before);
            let old = shared.snapshot().await.unwrap();
            std::fs::rename(
                root.join("nested/before.txt"),
                root.join("nested/after.txt"),
            )
            .map_err(storage_index)?;
            // Negative control: the source epoch and authored generation have
            // no signal, so the old implementation accepts these stale pages.
            assert!(reusable(view.view(), &old).await?);
            names.changed().await?;
            let mut found_after = false;
            while let Some(page) = names.next_page().await? {
                assert!(
                    !page
                        .names
                        .iter()
                        .any(|name| name.path == "nested/before.txt")
                );
                found_after |= page
                    .names
                    .iter()
                    .any(|name| name.path == "nested/after.txt");
            }
            assert!(found_after);
            Ok::<_, Error>(())
        };
        let result = tokio::time::timeout(std::time::Duration::from_secs(2), scenario).await;
        producer.abort();
        result.map_err(storage_index)?
    }

    #[test]
    fn volume_search_index_only_paths_classify_native_names_without_projection() {
        use acyclic_fs::kernel::{LogicalName, NameEncoding};
        for encoding in [
            NameEncoding::Utf8,
            NameEncoding::PosixBytes,
            NameEncoding::WindowsUtf16Le,
        ] {
            let name = |text: &str| {
                LogicalName::new(
                    encoding,
                    match encoding {
                        NameEncoding::WindowsUtf16Le => {
                            text.encode_utf16().flat_map(u16::to_le_bytes).collect()
                        }
                        _ => text.as_bytes().to_vec(),
                    },
                    255,
                )
                .unwrap()
            };
            assert!(index_path(&[]));
            assert!(index_path(&[name(".acyclic-sdk")]));
            assert!(index_path(&[
                name(".acyclic-sdk"),
                name("name-index"),
                name("page.json")
            ]));
            assert!(!index_path(&[name(".acyclic-sdk"), name("public.txt")]));
            assert!(!index_path(&[name(".acyclic-sdk-other")]));
        }
        let raw = LogicalName::new(NameEncoding::PosixBytes, vec![0xff], 255).unwrap();
        // Negative control: the old projection fails before classifying this
        // ordinary authored edit as a reason to refresh the index.
        assert!(
            raw.unicode_text(WorkBudget::UNBOUNDED, &CancellationToken::new())
                .is_err()
        );
        assert!(!index_path(&[raw]));
    }

    #[tokio::test]
    async fn volume_search_pinned_readers_allow_publication_and_retain_their_page()
    -> crate::Result<()> {
        let temp = tempfile::tempdir().map_err(storage_index)?;
        let fs = LocalFs::local(acyclic_fs::LocalOptions::new(temp.path().join("fs")))
            .await
            .map_err(storage_index)?;
        let stop = CancellationToken::new();
        let workspace = fs
            .create_workspace("snapshot-reader", WorkBudget::UNBOUNDED, &stop)
            .await
            .map_err(|failure| failure.error)?
            .value;
        workspace.write_text("/retained.txt", "content").await?;
        let expected = workspace
            .head(WorkBudget::UNBOUNDED, &stop)
            .await
            .map_err(|failure| failure.error)?
            .value;
        let mut walk = NameWalk::exact(expected.clone(), None, GitIgnorePolicy::default())
            .await
            .map_err(super::super::search::search_error)?;
        let page = walk
            .next_page()
            .await
            .map_err(super::super::search::search_error)?
            .unwrap();
        assert!(
            walk.next_page()
                .await
                .map_err(super::super::search::search_error)?
                .is_none()
        );
        let manifest = NameIndexManifest {
            schema: acyclic_fs::NameIndexManifest::SCHEMA,
            cut: walk.cut(),
            names_cut: walk.cut(),
            observed_generation: expected.id(),
            pages: 1,
            pending_directories: 0,
            complete: true,
            checkpoint: walk
                .checkpoint()
                .map_err(super::super::search::search_error)?,
        };
        let index = Index::publish(&workspace, &expected, manifest, Some(&page), None)
            .await
            .map_err(super::super::search::search_error)?
            .unwrap();
        let (changed, _receiver) = watch::channel(0);
        let shared = Arc::new(Shared {
            registration: Arc::new(AsyncMutex::new(Weak::new())),
            latest: AsyncMutex::new(Some(Arc::new(index))),
            changed,
            failure: Mutex::new(None),
            directory_query: Mutex::new(None),
            refresh: tokio::sync::Notify::new(),
        });
        let reader = shared.snapshot().await.unwrap();
        // A suspended consumer keeps its generation pin, but publication must
        // finish before that consumer resumes and reads its old immutable page.
        let publisher = Arc::clone(&shared);
        tokio::time::timeout(std::time::Duration::from_secs(1), async move {
            *publisher.latest.lock().await = None;
        })
        .await
        .map_err(storage_index)?;
        assert_eq!(
            reader
                .page(0)
                .await
                .map_err(super::super::search::search_error)?[0]
                .path,
            "retained.txt"
        );
        // Negative control: holding the old reader guard prevents publication.
        let held = shared.latest.lock().await;
        assert!(shared.latest.try_lock().is_err());
        drop(held);
        Ok(())
    }
}
