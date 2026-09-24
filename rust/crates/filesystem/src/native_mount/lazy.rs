//! Native projection of one demand-backed lazy workspace.

use super::adapter::{CallbackRuntime, CheckoutDetachedFile};
use super::view_gate::{
    ViewGate as SourceViewGate, ViewReadLease as SourceViewLease,
    ViewWriteLease as SourceMutationLease,
};
use super::view_ledger::{ViewChange, ViewStamp};
use super::{
    CheckoutMountSource, MountAttributePage, MountAttributeWriteMode, MountContentPin,
    MountDirectoryEntry, MountDirectoryPage, MountFilesystem, MountLookup, MountNode,
    MountNodeKind, MountOpenFile, MountPath, MountPublication, MountRangeAllocation,
    MountSeekTarget, MountSourceError, MountViewLease, capture_root_identity,
};
use crate::LazySeekTarget;
use crate::demand::{DemandFile, DemandSource, SourceNode, SourceNodeKind, SourceReference};
use crate::kernel::{
    FileKind, FileMetadata, FileMutation, FilePayload, FileRecord, MetadataField, Mutation,
};
use crate::lazy_workspace::logical_child_path;
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, FileId, IdempotencyKey, LazyDirectoryCursor, LazyLookup,
    LazyWorkspace, LazyWorkspaceError, LazyWorkspaceStore, NativeRootIdentity, WorkspaceMetadata,
};
use bytes::Bytes;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

const MAXIMUM_PROMOTION_BYTES: u64 = u64::MAX;
const MAXIMUM_LAZY_DIRECTORY_CURSORS: usize = 1_024;
const MAXIMUM_PROMOTION_RETRIES: usize = 3;

async fn stage_mount_promotion<A, O, D, S>(
    lazy: &LazyWorkspace<A, O, D, S>,
    authored: &CheckoutMountSource<A, O>,
    path: &str,
    mounted: &MountPath,
    expected_source: FileId,
) -> Result<(), MountSourceError>
where
    A: AsyncAuthorityStore + Send + Sync,
    O: AsyncObjectStore + Send + Sync,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    let promoted = authored.namespace_path(mounted)?;
    for _ in 0..MAXIMUM_PROMOTION_RETRIES {
        let (mut candidate, revision) = authored.shared_checkout().candidate().await?;
        let changed = lazy
            .stage_exact_into_checkout(
                &mut candidate,
                path,
                expected_source,
                MAXIMUM_PROMOTION_BYTES,
                authored.cancellation(),
            )
            .await
            .map_err(lazy_error)?;
        let mut checkout = authored.shared_checkout().lock().await;
        checkout.ensure_publication_resolved()?;
        if checkout.revision() != revision {
            continue;
        }
        if changed {
            checkout.install_candidate(candidate);
            checkout
                .publish_after_mutation(
                    ViewChange::Promoted(&promoted, expected_source),
                    authored.cancellation(),
                )
                .await?;
        }
        return Ok(());
    }
    Err(MountSourceError::Stale)
}

struct StampedCursor<T> {
    generation: (u64, u64),
    cursor: T,
}

struct CursorTable<T> {
    entries: Mutex<BTreeMap<u64, StampedCursor<T>>>,
    next: AtomicU64,
    maximum: usize,
}

impl<T> CursorTable<T> {
    fn new(maximum: usize) -> Self {
        Self {
            entries: Mutex::new(BTreeMap::new()),
            next: AtomicU64::new(1),
            maximum,
        }
    }

    fn remember(&self, generation: (u64, u64), cursor: T) -> Result<Vec<u8>, MountSourceError> {
        let mut token = self.next.fetch_add(1, Ordering::Relaxed);
        if token == 0 {
            token = self.next.fetch_add(1, Ordering::Relaxed);
        }
        let mut entries = self.entries.lock().map_err(|_| MountSourceError::Stale)?;
        while entries.len() >= self.maximum {
            let Some(oldest) = entries.first_key_value().map(|(&key, _)| key) else {
                break;
            };
            entries.remove(&oldest);
        }
        entries.insert(token, StampedCursor { generation, cursor });
        Ok(token.to_le_bytes().to_vec())
    }

    fn take(
        &self,
        generation: (u64, u64),
        cursor: Option<&[u8]>,
    ) -> Result<Option<T>, MountSourceError> {
        let Some(cursor) = cursor else {
            return Ok(None);
        };
        let bytes: [u8; 8] = cursor.try_into().map_err(|_| MountSourceError::Stale)?;
        let stamped = self
            .entries
            .lock()
            .map_err(|_| MountSourceError::Stale)?
            .remove(&u64::from_le_bytes(bytes))
            .ok_or(MountSourceError::Stale)?;
        if stamped.generation != generation {
            return Err(MountSourceError::Stale);
        }
        Ok(Some(stamped.cursor))
    }

    fn clear(&self) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.clear();
        }
    }
}

#[derive(Default)]
struct MountedRemovals {
    paths: BTreeMap<String, FileId>,
    records: BTreeMap<FileId, (FileRecord, FileMetadata)>,
    epoch: u64,
}

impl MountedRemovals {
    fn rebound(&mut self, path: &str) -> bool {
        let Some(file_id) = self.paths.remove(path) else {
            return false;
        };
        if !self.paths.values().any(|pending| *pending == file_id) {
            self.records.remove(&file_id);
        }
        self.epoch = self.epoch.wrapping_add(1);
        true
    }
}

struct DetachedIdentity<A, O> {
    file: Arc<CheckoutDetachedFile<A, O>>,
    ready: bool,
    published_epoch: u64,
    pending_epoch: Option<u64>,
}

type DetachedIdentities<A, O> = Arc<Mutex<BTreeMap<FileId, DetachedIdentity<A, O>>>>;
type OpenIdentityHandles = Arc<Mutex<BTreeMap<FileId, Vec<Weak<dyn MountOpenFile>>>>>;
type DirtyDetachedRecord = (FileId, FileRecord, FileMetadata, u64);

/// One native callback adapter over a source-backed sparse workspace.
///
/// Reads demand only the addressed source facts. Mutations promote the exact
/// node into the authored checkout before delegating to the ordinary checkout
/// adapter, keeping all publication semantics in the SDK.
pub struct LazyMountSource<A, O, D, S> {
    lazy: Arc<LazyWorkspace<A, O, D, S>>,
    authored: Arc<CheckoutMountSource<A, O>>,
    root: String,
    runtime: Arc<CallbackRuntime>,
    cursors: CursorTable<(ViewStamp, LazyDirectoryCursor)>,
    source_view: Arc<SourceViewGate>,
    removals: Mutex<MountedRemovals>,
    detached: DetachedIdentities<A, O>,
    open_sources: OpenIdentityHandles,
    /// The authored checkout's policy, which decides whether a native
    /// durability request (`fsync`) publishes.
    publication: MountPublication,
}

impl<A, O, D, S> LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    /// Composes lazy reads with one ordinary authored checkout adapter.
    pub(super) fn new(
        lazy: Arc<LazyWorkspace<A, O, D, S>>,
        authored: Arc<CheckoutMountSource<A, O>>,
        root: String,
        publication: MountPublication,
    ) -> Result<Self, super::NativeMountError> {
        Ok(Self {
            lazy,
            authored,
            root,
            runtime: Arc::new(CallbackRuntime::create()?),
            cursors: CursorTable::new(MAXIMUM_LAZY_DIRECTORY_CURSORS),
            source_view: Arc::new(SourceViewGate::new()),
            removals: Mutex::new(MountedRemovals::default()),
            detached: Arc::new(Mutex::new(BTreeMap::new())),
            open_sources: Arc::new(Mutex::new(BTreeMap::new())),
            publication,
        })
    }

    /// Cancels the authored callback adapter.
    pub fn cancel(&self) {
        self.cursors.clear();
        self.authored.cancel();
    }

    fn detached_by_id(
        &self,
        file_id: FileId,
    ) -> Result<Option<Arc<CheckoutDetachedFile<A, O>>>, MountSourceError> {
        Ok(self
            .detached
            .lock()
            .map_err(|_| MountSourceError::Stale)?
            .get(&file_id)
            .filter(|entry| entry.ready)
            .map(|entry| Arc::clone(&entry.file)))
    }

    /// A detached identity's live state under the link count of the name
    /// that resolved it, when the named node is detached.
    async fn detached_identity(
        &self,
        named: MountLookup,
    ) -> Result<Option<MountLookup>, MountSourceError> {
        let Some(file) = self.detached_by_id(named.node.file_id)? else {
            return Ok(None);
        };
        let mut current = file.lookup_async().await?;
        current.node.link_count = named.node.link_count;
        Ok(Some(current))
    }

    fn has_open_source_handle(&self, file_id: FileId) -> Result<bool, MountSourceError> {
        let mut open = self
            .open_sources
            .lock()
            .map_err(|_| MountSourceError::Stale)?;
        let Some(handles) = open.get_mut(&file_id) else {
            return Ok(false);
        };
        handles.retain(|handle| handle.strong_count() != 0);
        let present = !handles.is_empty();
        if !present {
            open.remove(&file_id);
        }
        Ok(present)
    }

    fn detached_for_path(
        &self,
        path: &MountPath,
    ) -> Result<Option<Arc<CheckoutDetachedFile<A, O>>>, MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        if self
            .detached
            .lock()
            .map_err(|_| MountSourceError::Stale)?
            .is_empty()
        {
            return Ok(None);
        }
        if let Some(authored) = self.authored.lookup(path)? {
            return self.detached_by_id(authored.node.file_id);
        }
        let text = self.path(path)?;
        if self.is_removed(&text)? {
            return Err(MountSourceError::NotFound);
        }
        let file_id = self.wait(|| async {
            let lookup = self.lazy.lookup(&text).await.map_err(lazy_error)?;
            self.lazy
                .stable_file_id_for_lookup(&text, &lookup)
                .await
                .map_err(lazy_error)
        })?;
        self.detached_by_id(file_id)
    }

    fn finish_detached_publication(&self) -> Result<(), MountSourceError> {
        let mut identities = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
        for (file_id, entry) in identities.iter_mut() {
            if !entry.ready {
                self.authored
                    .record_projection_change(&ViewChange::Node(*file_id));
            }
            entry.ready = true;
            if let Some(epoch) = entry.pending_epoch.take() {
                entry.published_epoch = epoch;
            }
        }
        drop(identities);
        self.prune_detached()
    }

    fn prune_detached(&self) -> Result<(), MountSourceError> {
        // Called only after a successful sync under the stable source-view gate.
        // A source handle re-fetches this identity on every operation, while a
        // shadow open holds the detached file directly. Keep either kind live.
        let mut open = self
            .open_sources
            .lock()
            .map_err(|_| MountSourceError::Stale)?;
        open.retain(|_, handles| {
            handles.retain(|handle| handle.strong_count() != 0);
            !handles.is_empty()
        });
        let mut identities = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
        identities.retain(|id, entry| {
            open.contains_key(id)
                || !entry.ready
                || entry.pending_epoch.is_some()
                || Arc::strong_count(&entry.file) != 1
        });
        Ok(())
    }

    fn abort_detached_publication(&self) -> Result<(), MountSourceError> {
        let mut identities = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
        identities.retain(|_, entry| entry.ready);
        for entry in identities.values_mut() {
            entry.pending_epoch = None;
        }
        Ok(())
    }

    fn dirty_detached_records(&self) -> Result<Vec<DirtyDetachedRecord>, MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let detached = self
            .detached
            .lock()
            .map_err(|_| MountSourceError::Stale)?
            .iter()
            .filter(|(_, entry)| entry.ready && entry.pending_epoch.is_none())
            .map(|(id, entry)| (*id, Arc::clone(&entry.file), entry.published_epoch))
            .collect::<Vec<_>>();
        let mut dirty = Vec::new();
        for (id, file, published_epoch) in detached {
            let (record, metadata, epoch) = file.snapshot()?;
            if epoch > published_epoch {
                dirty.push((id, record, metadata, epoch));
            }
        }
        Ok(dirty)
    }

    fn stage_detached_records(
        &self,
        records: &[(FileRecord, FileMetadata)],
        dirty: &[DirtyDetachedRecord],
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        for (record, metadata) in records {
            let exists = self
                .detached
                .lock()
                .map_err(|_| MountSourceError::Stale)?
                .contains_key(&record.file_id);
            if !exists {
                let file = self
                    .authored
                    .detached_file_from_record(*record, *metadata)?;
                self.detached
                    .lock()
                    .map_err(|_| MountSourceError::Stale)?
                    .insert(
                        record.file_id,
                        DetachedIdentity {
                            file,
                            ready: false,
                            published_epoch: 0,
                            pending_epoch: None,
                        },
                    );
            }
        }
        let mut identities = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
        for (id, _, _, epoch) in dirty {
            let entry = identities.get_mut(id).ok_or(MountSourceError::Stale)?;
            entry.pending_epoch = Some(*epoch);
        }
        Ok(())
    }

    async fn recover_pending_mount_change(
        &self,
        permit: crate::PublicationPermit,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let Some(committed) = self
            .lazy
            .mount_removal_publication()
            .await
            .map_err(lazy_error)?
        else {
            return Ok(());
        };
        // An ambiguous acknowledgement retains the exact checkout candidate.
        let retained = self
            .authored
            .shared_checkout()
            .lock()
            .await
            .has_retained_operation();
        if retained {
            self.authored.sync_async_with_permit_force(permit).await?;
        }
        let published = self
            .lazy
            .finish_mount_removals()
            .await
            .map_err(lazy_error)?;
        if committed || retained {
            if !published {
                return Err(MountSourceError::Stale);
            }
            self.clear_mount_removals()?;
            self.finish_detached_publication()?;
        } else {
            self.abort_detached_publication()?;
        }
        Ok(())
    }

    /// Publishes pending authored writes.
    pub async fn sync_async(&self) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let _mutation = self.source_view.write_stable(None).await?;
        self.sync_staged_with_permit(crate::PublicationPermit::Unrestricted)
            .await
    }

    /// Publishes pending authored writes under one authority lease permit.
    pub async fn sync_async_with_permit(
        &self,
        permit: crate::PublicationPermit,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let _mutation = self.source_view.write_stable(None).await?;
        self.sync_staged_with_permit(permit).await
    }

    /// Publishes pending authored writes on the callback runtime.
    pub fn sync(&self) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        let _mutation = self.mutation_lease(None)?;
        self.wait(|| async {
            self.sync_staged_with_permit(crate::PublicationPermit::Unrestricted)
                .await
        })
    }

    async fn sync_staged_with_permit(
        &self,
        permit: crate::PublicationPermit,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        self.recover_pending_mount_change(permit).await?;
        let (removals, captured_records) = self
            .removals
            .lock()
            .map_err(|_| MountSourceError::Stale)
            .map(|state| (state.paths.clone(), state.records.clone()))?;
        let paths = removals.keys().cloned().collect::<Vec<_>>();
        let dirty = self.dirty_detached_records()?;
        if paths.is_empty() && dirty.is_empty() {
            self.authored.sync_async_with_permit(permit).await?;
            return self.prune_detached();
        }
        let (operation_id, candidate, identity_records) = self
            .prepare_removed_identity_candidate(&removals, &dirty)
            .await?;
        let mut records = captured_records;
        records.extend(
            identity_records
                .into_iter()
                .map(|(record, metadata)| (record.file_id, (record, metadata))),
        );
        records.extend(
            dirty
                .iter()
                .map(|(_, record, metadata, _)| (record.file_id, (*record, *metadata))),
        );
        let identity_records = records.into_values().collect::<Vec<_>>();
        if let Err(error) = self.stage_detached_records(&identity_records, &dirty) {
            self.authored
                .shared_checkout()
                .lock()
                .await
                .clear_retained_operation(operation_id);
            self.abort_detached_publication()?;
            return Err(error);
        }
        let idempotency_key = IdempotencyKey::from_bytes(operation_id.into_bytes());
        if let Err(error) = self
            .lazy
            .prepare_mount_removals(&paths, &identity_records, idempotency_key)
            .await
        {
            self.authored
                .shared_checkout()
                .lock()
                .await
                .clear_retained_operation(operation_id);
            self.abort_detached_publication()?;
            return Err(lazy_error(error));
        }
        {
            let mut checkout = self.authored.shared_checkout().lock().await;
            checkout.install_candidate(candidate);
            checkout.record(&ViewChange::Everything);
        }
        self.authored.sync_async_with_permit_force(permit).await?;
        if !self
            .lazy
            .finish_mount_removals()
            .await
            .map_err(lazy_error)?
        {
            return Err(MountSourceError::Stale);
        }
        self.clear_mount_removals()?;
        self.finish_detached_publication()?;
        Ok(())
    }

    fn clear_mount_removals(&self) -> Result<(), MountSourceError> {
        let mut removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
        removals.paths.clear();
        removals.records.clear();
        Ok(())
    }

    async fn prepare_removed_identity_candidate(
        &self,
        removals: &BTreeMap<String, FileId>,
        dirty: &[DirtyDetachedRecord],
    ) -> Result<
        (
            crate::OperationId,
            crate::Checkout<A, O>,
            Vec<(FileRecord, FileMetadata)>,
        ),
        MountSourceError,
    >
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let mut checkout = self.authored.shared_checkout().lock().await;
        checkout.ensure_publication_resolved()?;
        let mut candidate = checkout.private_candidate();
        let mut identity_records = BTreeMap::new();
        for (path, removed_id) in removals {
            let namespace = self.lazy.namespace_path(path).map_err(lazy_error)?;
            let lookup = candidate
                .lookup_no_follow_with_metadata(
                    &namespace,
                    crate::WorkBudget::UNBOUNDED,
                    self.authored.cancellation(),
                )
                .await
                .map_err(|failure| MountSourceError::Engine(failure.error.to_string()))?;
            let Some(lookup) = lookup.value else {
                continue;
            };
            if lookup.record.file_id != *removed_id {
                continue;
            }
            if lookup.record.kind != FileKind::Regular {
                return Err(MountSourceError::Stale);
            }
            identity_records
                .entry(*removed_id)
                .or_insert((lookup.record, lookup.metadata));
            candidate
                .remove(
                    namespace,
                    Some(*removed_id),
                    crate::WorkBudget::UNBOUNDED,
                    self.authored.cancellation(),
                )
                .await
                .map_err(|failure| MountSourceError::Engine(failure.error.to_string()))?;
        }
        // A detached handle can still write through a removed name while a
        // sibling hard link is already authored. Refresh that live identity
        // directly; resolving it by a surviving path would require a scan and
        // could incorrectly re-read stale source bytes.
        let mut replacements = Vec::new();
        for (file_id, record, _, _) in dirty {
            let live = match candidate
                .read_file_record_by_id(
                    *file_id,
                    crate::WorkBudget::UNBOUNDED,
                    self.authored.cancellation(),
                )
                .await
            {
                Ok(receipt) => Some(receipt.value),
                Err(failure) if matches!(failure.error, crate::FsError::NotFound) => None,
                Err(failure) => {
                    return Err(MountSourceError::Engine(failure.error.to_string()));
                }
            };
            if let Some(live) = live {
                if live.kind != record.kind {
                    return Err(MountSourceError::Stale);
                }
                replacements.push(Mutation::File {
                    file_id: *file_id,
                    mutation: FileMutation::ReplaceRecord { record: *record },
                });
            }
        }
        if !replacements.is_empty() {
            let maximum =
                usize::try_from(candidate.volume_config().limits.maximum_mutations_per_batch)
                    .unwrap_or(usize::MAX)
                    .max(1);
            for chunk in replacements.chunks(maximum) {
                candidate
                    .mutate(
                        chunk.to_vec(),
                        crate::WorkBudget::UNBOUNDED,
                        self.authored.cancellation(),
                    )
                    .await
                    .map_err(|failure| MountSourceError::Engine(failure.error.to_string()))?;
            }
        }
        let identity_records = identity_records.into_values().collect();
        let operation_id = checkout.retained_operation_id();
        Ok((operation_id, candidate, identity_records))
    }

    /// Advances the authored checkout after an external publication.
    pub async fn advance_to_head_async(&self) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let _writer = self.source_view.write().await;
        self.source_view.begin_transition();
        let result = async {
            self.authored.advance_to_head_async().await?;
            self.lazy.rebind_source().await.map_err(lazy_error)?;
            self.cursors.clear();
            Ok(())
        }
        .await;
        if result.is_ok() {
            self.source_view.finish_transition();
        }
        result
    }

    fn path(&self, path: &MountPath) -> Result<String, MountSourceError> {
        let mut value = String::new();
        for component in path.components() {
            value.push('/');
            let text = if cfg!(target_os = "windows") {
                if !component.len().is_multiple_of(2) {
                    return Err(MountSourceError::Invalid(
                        "native UTF-16 name has an odd byte length".to_owned(),
                    ));
                }
                let units = component
                    .chunks_exact(2)
                    .filter_map(|pair| pair.try_into().ok().map(u16::from_le_bytes))
                    .collect::<Vec<_>>();
                String::from_utf16(&units).map_err(|_| {
                    MountSourceError::Unsupported(
                        "lazy portable mount cannot represent an unpaired UTF-16 name".to_owned(),
                    )
                })?
            } else {
                std::str::from_utf8(component)
                    .map_err(|_| {
                        MountSourceError::Unsupported(
                            "lazy portable mount cannot represent a non-UTF-8 name".to_owned(),
                        )
                    })?
                    .to_owned()
            };
            value.push_str(&text);
        }
        if value.is_empty() {
            value.push('/');
        }
        if self.root == "/" {
            Ok(value)
        } else if value == "/" {
            Ok(self.root.clone())
        } else {
            Ok(format!("{}{value}", self.root))
        }
    }

    fn removal_snapshot(
        &self,
        directory: &str,
    ) -> Result<(Vec<String>, bool, u64), MountSourceError> {
        let removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
        let opaque = Self::removed_by(&removals.paths, directory);
        let prefix = if directory == "/" {
            "/".to_owned()
        } else {
            format!("{directory}/")
        };
        let children = removals
            .paths
            .range(prefix.clone()..)
            .take_while(|(path, _)| path.starts_with(&prefix))
            .filter(|(path, _)| {
                path.strip_prefix(&prefix)
                    .is_some_and(|suffix| !suffix.contains('/'))
            })
            .map(|(path, _)| path.clone())
            .collect();
        Ok((children, opaque, removals.epoch))
    }

    fn removed_by(paths: &BTreeMap<String, FileId>, path: &str) -> bool {
        let mut current = path;
        loop {
            if paths.contains_key(current) {
                return true;
            }
            if current == "/" {
                return false;
            }
            current =
                current.rsplit_once('/').map_or(
                    "/",
                    |(parent, _)| if parent.is_empty() { "/" } else { parent },
                );
        }
    }

    fn is_removed(&self, path: &str) -> Result<bool, MountSourceError> {
        let removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
        Ok(Self::removed_by(&removals.paths, path))
    }

    fn removed_identity(&self, path: &str, file_id: FileId) -> Result<bool, MountSourceError> {
        let removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
        Ok(removals
            .paths
            .get(path)
            .is_some_and(|removed| *removed == file_id))
    }

    fn record_removed(&self, path: &MountPath, file_id: FileId) -> Result<(), MountSourceError> {
        let text = self.path(path)?;
        let mut removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
        if removals.paths.insert(text, file_id) != Some(file_id) {
            removals.epoch = removals.epoch.wrapping_add(1);
            self.cursors.clear();
        }
        self.authored.record_projection_change(&ViewChange::Unbound(
            &self.authored.namespace_path(path)?,
            file_id,
        ));
        Ok(())
    }

    fn record_removed_identity(
        &self,
        record: FileRecord,
        metadata: FileMetadata,
        file: Arc<CheckoutDetachedFile<A, O>>,
    ) -> Result<(), MountSourceError> {
        self.detached
            .lock()
            .map_err(|_| MountSourceError::Stale)?
            .entry(record.file_id)
            .or_insert(DetachedIdentity {
                file,
                ready: true,
                published_epoch: 0,
                pending_epoch: None,
            });
        self.removals
            .lock()
            .map_err(|_| MountSourceError::Stale)?
            .records
            .insert(record.file_id, (record, metadata));
        self.authored
            .record_projection_change(&ViewChange::Node(record.file_id));
        Ok(())
    }

    fn record_rebound(&self, path: &str) -> Result<(), MountSourceError> {
        let mut removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
        if removals.rebound(path) {
            self.cursors.clear();
        }
        Ok(())
    }

    async fn promote(&self, mounted: &MountPath) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
        D: DemandSource + 'static,
        S: LazyWorkspaceStore,
    {
        let path = self.path(mounted)?;
        let lookup = self.lazy.lookup(&path).await.map_err(lazy_error)?;
        let expected_source = match lookup {
            LazyLookup::Source(node) => self.lazy.source_file_id(&node),
            LazyLookup::Authored { stat, .. } => stat.file_id,
            LazyLookup::Shadow { record, .. } => record.file_id,
        };
        stage_mount_promotion(&self.lazy, &self.authored, &path, mounted, expected_source).await
    }

    fn promote_parents_locked(&self, path: &MountPath) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
        D: DemandSource + 'static,
        S: LazyWorkspaceStore,
    {
        let components = path.components();
        let mut parent = MountPath::root();
        for component in components.iter().take(components.len().saturating_sub(1)) {
            parent = parent.child((*component).to_vec());
            let authored = self.authored.lookup(&parent)?;
            if authored.is_some() {
                continue;
            }
            self.wait(|| async { self.promote(&parent).await })?;
        }
        Ok(())
    }

    fn wait<T: Send, F>(&self, create: impl FnOnce() -> F + Send) -> Result<T, MountSourceError>
    where
        F: std::future::Future<Output = Result<T, MountSourceError>>,
    {
        self.runtime.wait(create)
    }

    fn remember_cursor(
        &self,
        generation: (u64, u64),
        cursor: (ViewStamp, LazyDirectoryCursor),
    ) -> Result<Vec<u8>, MountSourceError> {
        self.cursors.remember(generation, cursor)
    }

    fn take_cursor(
        &self,
        generation: (u64, u64),
        cursor: Option<&[u8]>,
    ) -> Result<Option<(ViewStamp, LazyDirectoryCursor)>, MountSourceError> {
        self.cursors.take(generation, cursor)
    }
}

struct LazyOpenFile<A, O, D, S> {
    lazy: Arc<LazyWorkspace<A, O, D, S>>,
    authored: Arc<CheckoutMountSource<A, O>>,
    runtime: Arc<CallbackRuntime>,
    path: String,
    mount_path: MountPath,
    expected_source: FileId,
    /// The source file, held open and version-proven since the mount opened
    /// it; unpromoted reads are served only through it.
    source_file: Box<dyn DemandFile>,
    source_node: SourceNode,
    source_generation: u64,
    source_view: Arc<SourceViewGate>,
    detached: DetachedIdentities<A, O>,
    open_sources: OpenIdentityHandles,
    promoted: Mutex<Option<Arc<dyn MountOpenFile>>>,
    checked_authored_epoch: AtomicU64,
}

struct ViewBoundOpenFile<A, O> {
    inner: Arc<dyn MountOpenFile>,
    runtime: Arc<CallbackRuntime>,
    source_view: Arc<SourceViewGate>,
    detached: DetachedIdentities<A, O>,
    open_sources: OpenIdentityHandles,
    file_id: Option<FileId>,
    generation: u64,
}

impl<A, O, D, S> Drop for LazyOpenFile<A, O, D, S> {
    fn drop(&mut self) {
        untrack_open_handle(
            &self.open_sources,
            self.expected_source,
            self as *const Self as *const (),
        );
    }
}

impl<A, O> Drop for ViewBoundOpenFile<A, O> {
    fn drop(&mut self) {
        if let Some(file_id) = self.file_id {
            untrack_open_handle(
                &self.open_sources,
                file_id,
                self as *const Self as *const (),
            );
        }
    }
}

fn untrack_open_handle(handles: &OpenIdentityHandles, file_id: FileId, ptr: *const ()) {
    let Ok(mut open) = handles.lock() else {
        return;
    };
    if let Some(entries) = open.get_mut(&file_id) {
        entries.retain(|entry| entry.as_ptr() as *const () != ptr);
        if entries.is_empty() {
            open.remove(&file_id);
        }
    }
}

impl<A, O> ViewBoundOpenFile<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn current_file(&self) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        let Some(file_id) = self.file_id else {
            return Ok(Arc::clone(&self.inner));
        };
        let detached = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
        Ok(detached
            .get(&file_id)
            .filter(|entry| entry.ready)
            .map_or_else(
                || Arc::clone(&self.inner),
                |entry| Arc::clone(&entry.file) as Arc<dyn MountOpenFile>,
            ))
    }

    fn with_view<T>(
        &self,
        operation: impl FnOnce(&dyn MountOpenFile) -> Result<T, MountSourceError>,
    ) -> Result<T, MountSourceError> {
        let owner = SourceViewGate::callback_owner();
        let _lease = self.runtime.wait(|| {
            let source_view = Arc::clone(&self.source_view);
            let generation = self.generation;
            async move { source_view.read_for_callback(owner, Some(generation)).await }
        })?;
        operation(self.current_file()?.as_ref())
    }

    fn with_mutation<T>(
        &self,
        operation: impl FnOnce(&dyn MountOpenFile) -> Result<T, MountSourceError>,
    ) -> Result<T, MountSourceError> {
        let _lease = self.runtime.wait(|| {
            let source_view = Arc::clone(&self.source_view);
            let generation = self.generation;
            async move { source_view.write_stable(Some(generation)).await }
        })?;
        operation(self.current_file()?.as_ref())
    }
}

impl<A, O> MountOpenFile for ViewBoundOpenFile<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn lookup(&self) -> Result<MountLookup, MountSourceError> {
        self.with_view(MountOpenFile::lookup)
    }

    fn read_range(&self, offset: u64, length: u32) -> Result<Bytes, MountSourceError> {
        self.with_view(|file| file.read_range(offset, length))
    }

    fn read_up_to(&self, offset: u64, maximum_bytes: u32) -> Result<Bytes, MountSourceError> {
        self.with_view(|file| file.read_up_to(offset, maximum_bytes))
    }

    fn seek(&self, offset: u64, target: MountSeekTarget) -> Result<Option<u64>, MountSourceError> {
        self.with_view(|file| file.seek(offset, target))
    }

    fn write_range(&self, offset: u64, bytes: Bytes) -> Result<(), MountSourceError> {
        self.with_mutation(|file| file.write_range(offset, bytes))
    }

    fn resize(&self, logical_bytes: u64) -> Result<(), MountSourceError> {
        self.with_mutation(|file| file.resize(logical_bytes))
    }

    fn allocate_range(
        &self,
        offset: u64,
        length: u64,
        operation: MountRangeAllocation,
    ) -> Result<(), MountSourceError> {
        self.with_mutation(|file| file.allocate_range(offset, length, operation))
    }

    fn set_attributes(
        &self,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
    ) -> Result<(), MountSourceError> {
        self.with_mutation(|file| file.set_attributes(metadata, logical_bytes))
    }

    fn read_attribute(&self, name: &[u8]) -> Result<Option<Bytes>, MountSourceError> {
        self.with_view(|file| file.read_attribute(name))
    }

    fn list_attributes(
        &self,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountAttributePage, MountSourceError> {
        self.with_view(|file| file.list_attributes(cursor, maximum_entries))
    }

    fn write_attribute(
        &self,
        name: &[u8],
        value: Bytes,
        mode: MountAttributeWriteMode,
    ) -> Result<(), MountSourceError> {
        self.with_mutation(|file| file.write_attribute(name, value, mode))
    }

    fn remove_attribute(&self, name: &[u8]) -> Result<(), MountSourceError> {
        self.with_mutation(|file| file.remove_attribute(name))
    }
}

impl<A, O, D, S> LazyOpenFile<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    fn current_authored(&self) -> Result<Option<Arc<dyn MountOpenFile>>, MountSourceError> {
        if let Some(file) = self
            .detached
            .lock()
            .map_err(|_| MountSourceError::Stale)?
            .get(&self.expected_source)
            .filter(|entry| entry.ready)
            .map(|entry| Arc::clone(&entry.file))
        {
            return Ok(Some(file));
        }
        let mut promoted = self.promoted.lock().map_err(|_| MountSourceError::Stale)?;
        if let Some(file) = promoted.as_ref() {
            return Ok(Some(Arc::clone(file)));
        }
        let epoch = self.authored.shared_checkout().revision();
        if self.checked_authored_epoch.load(Ordering::Acquire) == epoch {
            return Ok(None);
        }
        let file = self.authored.attached_file_by_id(self.expected_source)?;
        self.checked_authored_epoch.store(epoch, Ordering::Release);
        if let Some(file) = file {
            *promoted = Some(Arc::clone(&file));
            return Ok(Some(file));
        }
        Ok(None)
    }

    fn read_source(&self, offset: u64, length: u64) -> Result<Bytes, MountSourceError> {
        self.source_file
            .read_range(offset, length, &crate::CancellationToken::new())
            .map(|receipt| receipt.value)
            .map_err(|failure| lazy_error(failure.error.into()))
    }

    fn source_lease(&self) -> Result<SourceViewLease, MountSourceError> {
        let owner = SourceViewGate::callback_owner();
        self.runtime.wait(|| {
            let source_view = Arc::clone(&self.source_view);
            async move {
                source_view
                    .read_for_callback(owner, Some(self.source_generation))
                    .await
            }
        })
    }

    fn with_authored<T>(
        &self,
        operation: impl FnOnce(&dyn MountOpenFile) -> Result<T, MountSourceError>,
    ) -> Result<T, MountSourceError> {
        let _lease = self.runtime.wait(|| {
            let source_view = Arc::clone(&self.source_view);
            async move { source_view.write_stable(Some(self.source_generation)).await }
        })?;
        if let Some(file) = self.current_authored()? {
            return operation(file.as_ref());
        }
        let mut promoted = self.promoted.lock().map_err(|_| MountSourceError::Stale)?;
        if promoted.is_none() {
            self.runtime.wait(|| async {
                stage_mount_promotion(
                    &self.lazy,
                    &self.authored,
                    &self.path,
                    &self.mount_path,
                    self.expected_source,
                )
                .await
            })?;
            *promoted = Some(self.authored.open_file(&self.mount_path)?);
        }
        let file = promoted.as_ref().ok_or(MountSourceError::Stale)?;
        operation(file.as_ref())
    }
}

impl<A, O, D, S> MountOpenFile for LazyOpenFile<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    fn lookup(&self) -> Result<MountLookup, MountSourceError> {
        let _lease = self.source_lease()?;
        if let Some(file) = self.current_authored()? {
            return file.lookup();
        }
        let file_id = self.lazy.source_file_id(&self.source_node);
        Ok(mount_lookup(LazyLookup::Source(self.source_node), file_id))
    }

    fn read_range(&self, offset: u64, length: u32) -> Result<Bytes, MountSourceError> {
        let _lease = self.source_lease()?;
        if let Some(file) = self.current_authored()? {
            return file.read_range(offset, length);
        }
        self.read_source(offset, u64::from(length))
    }

    fn read_up_to(&self, offset: u64, maximum_bytes: u32) -> Result<Bytes, MountSourceError> {
        let _lease = self.source_lease()?;
        if let Some(file) = self.current_authored()? {
            return file.read_up_to(offset, maximum_bytes);
        }
        let length = self
            .source_node
            .logical_bytes
            .ok_or_else(|| MountSourceError::Invalid("source is not regular".to_owned()))?
            .saturating_sub(offset)
            .min(u64::from(maximum_bytes));
        if length == 0 {
            return Ok(Bytes::new());
        }
        self.read_source(offset, length)
    }

    fn seek(&self, offset: u64, target: MountSeekTarget) -> Result<Option<u64>, MountSourceError> {
        let _lease = self.source_lease()?;
        if let Some(file) = self.current_authored()? {
            return file.seek(offset, target);
        }
        let length = self
            .source_node
            .logical_bytes
            .ok_or_else(|| MountSourceError::Invalid("seek requires a regular file".to_owned()))?;
        if offset >= length {
            return Ok(None);
        }
        Ok(Some(match target {
            MountSeekTarget::Data => offset,
            MountSeekTarget::Hole => length,
        }))
    }

    fn write_range(&self, offset: u64, bytes: Bytes) -> Result<(), MountSourceError> {
        self.with_authored(|file| file.write_range(offset, bytes))
    }

    fn resize(&self, logical_bytes: u64) -> Result<(), MountSourceError> {
        self.with_authored(|file| file.resize(logical_bytes))
    }

    fn allocate_range(
        &self,
        offset: u64,
        length: u64,
        operation: MountRangeAllocation,
    ) -> Result<(), MountSourceError> {
        self.with_authored(|file| file.allocate_range(offset, length, operation))
    }

    fn set_attributes(
        &self,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
    ) -> Result<(), MountSourceError> {
        self.with_authored(|file| file.set_attributes(metadata, logical_bytes))
    }

    fn read_attribute(&self, name: &[u8]) -> Result<Option<Bytes>, MountSourceError> {
        self.with_authored(|file| file.read_attribute(name))
    }

    fn list_attributes(
        &self,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountAttributePage, MountSourceError> {
        self.with_authored(|file| file.list_attributes(cursor, maximum_entries))
    }

    fn write_attribute(
        &self,
        name: &[u8],
        value: Bytes,
        mode: MountAttributeWriteMode,
    ) -> Result<(), MountSourceError> {
        self.with_authored(|file| file.write_attribute(name, value, mode))
    }

    fn remove_attribute(&self, name: &[u8]) -> Result<(), MountSourceError> {
        self.with_authored(|file| file.remove_attribute(name))
    }
}

impl<A, O, D, S> MountFilesystem for LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    fn supports_posix_named_attributes(&self) -> bool {
        self.authored.supports_posix_named_attributes()
    }

    fn flush_on_handle_close(&self) -> bool {
        false
    }

    fn view_is_stable(&self) -> bool {
        self.source_view.is_stable()
    }

    fn view_stamp(&self) -> Option<ViewStamp> {
        self.source_view
            .is_stable()
            .then(|| self.authored.view_stamp())
            .flatten()
    }

    fn unchanged_since(&self, path: &MountPath, file_id: Option<FileId>, stamp: ViewStamp) -> bool {
        self.source_view.is_stable() && self.authored.unchanged_since(path, file_id, stamp)
    }

    fn binding_epoch(&self) -> Option<u64> {
        Some(self.source_view.generation())
    }

    fn acquire_view_lease(&self) -> Result<Box<dyn MountViewLease>, MountSourceError> {
        Ok(Box::new(self.view_lease(None)?))
    }

    fn acquire_binding_lease(
        &self,
        expected_epoch: Option<u64>,
    ) -> Result<Box<dyn MountViewLease>, MountSourceError> {
        self.view_lease(expected_epoch)
            .map(|lease| Box::new(lease) as Box<dyn MountViewLease>)
    }

    fn lookup(&self, path: &MountPath) -> Result<Option<MountLookup>, MountSourceError> {
        Ok(self.lookup_pinned(path)?.map(|(lookup, _)| lookup))
    }

    fn lookup_pinned(
        &self,
        path: &MountPath,
    ) -> Result<Option<(MountLookup, Option<MountContentPin>)>, MountSourceError> {
        let path_text = self.path(path)?;
        let owner = SourceViewGate::callback_owner();
        self.wait(|| async move {
            let _lease = self.source_view.read_for_callback(owner, None).await?;
            // Newly authored objects are visible before the operation barrier has
            // published them into the lazy view. This is required for NFS CREATE
            // compounds, which immediately GETATTR the returned filehandle.
            if let Some(lookup) = self.authored.lookup_async(path, owner).await? {
                if self.removed_identity(&path_text, lookup.node.file_id)? {
                    return Ok(None);
                }
                let current = self.detached_identity(lookup).await?;
                return Ok(Some((current.unwrap_or(lookup), None)));
            }
            if self.is_removed(&path_text)? {
                return Ok(None);
            }
            // Inspection never extends the durable observation index. A
            // caller that must later reproduce this exact content keeps the
            // returned pin instead.
            let (lookup, source) = match self.lazy.inspect_unauthored(&path_text, None).await {
                Ok(resolved) => resolved,
                Err(LazyWorkspaceError::NotFound) => return Ok(None),
                Err(error) => return Err(lazy_error(error)),
            };
            let pin = match (&lookup, source) {
                (LazyLookup::Source(node), Some(source))
                    if node.kind == SourceNodeKind::RegularFile =>
                {
                    Some(source_content_pin(source, node))
                }
                _ => None,
            };
            let file_id = self
                .lazy
                .stable_file_id_for_lookup(&path_text, &lookup)
                .await
                .map_err(lazy_error)?;
            let projected = mount_lookup(lookup, file_id);
            if let Some(current) = self.detached_identity(projected).await? {
                return Ok(Some((current, None)));
            }
            Ok(Some((projected, pin)))
        })
    }

    fn open_file(&self, path: &MountPath) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        let lease = self.view_lease(None)?;
        // A create returns an open handle before the operation barrier publishes the authored
        // generation. Consult the authored checkout first so that the just-created file can be
        // written through that handle instead of falling through to the still-unaware lazy view.
        let path_text = self.path(path)?;
        if let Some(authored) = self.authored.lookup(path)? {
            if self.removed_identity(&path_text, authored.node.file_id)? {
                return Err(MountSourceError::NotFound);
            }
            if let Some(file) = self.detached_by_id(authored.node.file_id)? {
                return self.bind_open_file(file, Some(authored.node.file_id), lease.generation);
            }
            return self.authored.open_file(path).and_then(|file| {
                self.bind_open_file(file, Some(authored.node.file_id), lease.generation)
            });
        }
        if self.is_removed(&path_text)? {
            return Err(MountSourceError::NotFound);
        }
        let lookup_path = path_text.clone();
        let (lookup, source) = self.wait(|| async move {
            self.lazy
                .inspect_unauthored(&lookup_path, None)
                .await
                .map_err(lazy_error)
        })?;
        let source_generation = lease.generation;
        match lookup {
            LazyLookup::Authored {
                path: authored_path,
                stat,
            } if authored_path == path_text => self
                .authored
                .open_file(path)
                .and_then(|file| self.bind_open_file(file, Some(stat.file_id), source_generation)),
            LazyLookup::Shadow { record, .. } => {
                if let Some(file) = self.detached_by_id(record.file_id)? {
                    return self.bind_open_file(file, Some(record.file_id), source_generation);
                }
                drop(lease);
                let _mutation = self.mutation_lease(Some(source_generation))?;
                self.promote_locked(path)?;
                self.authored.open_file(path).and_then(|file| {
                    self.bind_open_file(file, Some(record.file_id), source_generation)
                })
            }
            LazyLookup::Authored { stat, .. } => {
                drop(lease);
                let _mutation = self.mutation_lease(Some(source_generation))?;
                self.promote_locked(path)?;
                self.authored.open_file(path).and_then(|file| {
                    self.bind_open_file(file, Some(stat.file_id), source_generation)
                })
            }
            LazyLookup::Source(node) if node.kind == SourceNodeKind::RegularFile => {
                let source = source.ok_or(MountSourceError::Stale)?;
                let source_file = self.wait(|| async {
                    self.lazy
                        .open_source_file(&path_text, source, node)
                        .await
                        .map_err(lazy_error)
                })?;
                let expected_source = self.lazy.source_file_id(&node);
                let file: Arc<dyn MountOpenFile> = Arc::new(LazyOpenFile {
                    lazy: Arc::clone(&self.lazy),
                    authored: Arc::clone(&self.authored),
                    runtime: Arc::clone(&self.runtime),
                    path: path_text,
                    mount_path: path.clone(),
                    expected_source,
                    source_file,
                    source_node: node,
                    source_generation,
                    source_view: Arc::clone(&self.source_view),
                    detached: Arc::clone(&self.detached),
                    open_sources: Arc::clone(&self.open_sources),
                    promoted: Mutex::new(None),
                    // The handle has not yet checked this checkout epoch.
                    // A peer alias may already have staged this file ID.
                    checked_authored_epoch: AtomicU64::new(0),
                });
                self.open_sources
                    .lock()
                    .map_err(|_| MountSourceError::Stale)?
                    .entry(expected_source)
                    .or_default()
                    .push(Arc::downgrade(&file));
                Ok(file)
            }
            LazyLookup::Source(_) => Err(MountSourceError::Invalid(
                "open requires a regular file".to_owned(),
            )),
        }
    }

    fn detach_file(&self, path: &MountPath) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        let lease = self.mutation_lease(None)?;
        self.promote_locked(path)?;
        self.authored
            .detach_file(path)
            .and_then(|file| self.bind_open_file(file, None, lease.generation))
    }

    fn read_link(&self, path: &MountPath) -> Result<Bytes, MountSourceError> {
        let _lease = self.view_lease(None)?;
        if self.authored.lookup(path)?.is_some() {
            return self.authored.read_link(path);
        }
        let path = self.path(path)?;
        if self.is_removed(&path)? {
            return Err(MountSourceError::NotFound);
        }
        self.wait(|| async move { self.lazy.read_link(&path).await.map_err(lazy_error) })
    }

    fn read_range(
        &self,
        path: &MountPath,
        offset: u64,
        length: u32,
    ) -> Result<Bytes, MountSourceError> {
        self.read_content(path, None, offset, length)
    }

    fn read_pinned(
        &self,
        path: &MountPath,
        pin: MountContentPin,
        offset: u64,
        length: u32,
    ) -> Result<Bytes, MountSourceError> {
        self.read_content(path, Some(pin), offset, length)
    }

    fn seek(
        &self,
        path: &MountPath,
        offset: u64,
        target: MountSeekTarget,
    ) -> Result<Option<u64>, MountSourceError> {
        let _lease = self.view_lease(None)?;
        if self.authored.lookup(path)?.is_some() {
            return self.authored.seek(path, offset, target);
        }
        let path = self.path(path)?;
        if self.is_removed(&path)? {
            return Err(MountSourceError::NotFound);
        }
        self.wait(|| async move {
            self.lazy
                .seek(
                    &path,
                    offset,
                    match target {
                        MountSeekTarget::Data => LazySeekTarget::Data,
                        MountSeekTarget::Hole => LazySeekTarget::Hole,
                    },
                )
                .await
                .map_err(lazy_error)
        })
    }

    fn read_directory(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountDirectoryPage, MountSourceError> {
        let text = self.path(path)?;
        let owner = SourceViewGate::callback_owner();
        self.wait(|| async move {
            let lease = self.source_view.read_for_callback(owner, None).await?;
            let (removed_children, opaque, removal_epoch) = self.removal_snapshot(&text)?;
            if opaque && self.authored.lookup_async(path, owner).await?.is_none() {
                return Err(MountSourceError::NotFound);
            }
            let generation = (lease.generation, removal_epoch);
            // A continuation stays exact while its directory's listing does.
            let (stamp, cursor) = match self.take_cursor(generation, cursor)? {
                Some((stamp, cursor)) => (stamp, Some(cursor)),
                None => (ViewStamp::current(), None),
            };
            if !self.authored.unchanged_since(path, None, stamp) {
                return Err(MountSourceError::Stale);
            }
            let (page, state) = {
                let observation = self.authored.shared_checkout().observe(owner).await?;
                observation.ensure_publication_resolved()?;
                self.lazy
                    .list_directory_in_checkout(
                        &mut observation.observer(),
                        &text,
                        cursor,
                        maximum_entries,
                        Some((&removed_children, opaque)),
                    )
                    .await
                    .map_err(lazy_error)?
            };
            let mut entries = Vec::with_capacity(page.entries.len());
            for entry in page.entries {
                let child = logical_child_path(&text, &entry.name).ok_or_else(|| {
                    MountSourceError::Unsupported(
                        "lazy mount cannot address a non-Unicode name".to_owned(),
                    )
                })?;
                let name = super::adapter::native_mount_name(&entry.name)?;
                let mounted_child = path.child(name.clone());
                let lookup = if let Some(node) = entry.source {
                    let lookup = self
                        .lazy
                        .inspect_listed(&state, &child, node)
                        .await
                        .map_err(lazy_error)?;
                    let file_id = self
                        .lazy
                        .stable_file_id_for_lookup(&child, &lookup)
                        .await
                        .map_err(lazy_error)?;
                    mount_lookup(lookup, file_id)
                } else if let Some(authored) =
                    self.authored.lookup_async(&mounted_child, owner).await?
                {
                    if self.removed_identity(&child, authored.node.file_id)? {
                        continue;
                    }
                    authored
                } else {
                    let lookup = self
                        .lazy
                        .inspect_unauthored(&child, Some(&state))
                        .await
                        .map_err(lazy_error)?
                        .0;
                    let file_id = self
                        .lazy
                        .stable_file_id_for_lookup(&child, &lookup)
                        .await
                        .map_err(lazy_error)?;
                    mount_lookup(lookup, file_id)
                };
                entries.push(MountDirectoryEntry {
                    name,
                    node: lookup.node,
                    metadata: lookup.metadata,
                });
            }
            if !self.authored.unchanged_since(path, None, stamp)
                || self.removal_snapshot(&text)?.2 != generation.1
            {
                return Err(MountSourceError::Stale);
            }
            Ok(MountDirectoryPage {
                entries,
                next_cursor: page
                    .next
                    .map(|cursor| self.remember_cursor(generation, (stamp, cursor)))
                    .transpose()?,
            })
        })
    }

    fn create_file(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
    ) -> Result<MountLookup, MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        self.promote_parents_locked(path)?;
        let created = self.authored.create_file(path, metadata)?;
        self.record_rebound(&self.path(path)?)?;
        Ok(created)
    }

    fn create_directory(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
    ) -> Result<MountLookup, MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        self.promote_parents_locked(path)?;
        let created = self.authored.create_directory(path, metadata)?;
        self.record_rebound(&self.path(path)?)?;
        Ok(created)
    }

    fn create_symbolic_link(
        &self,
        path: &MountPath,
        target: Bytes,
        metadata: FileMetadata,
    ) -> Result<MountLookup, MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        self.promote_parents_locked(path)?;
        let created = self.authored.create_symbolic_link(path, target, metadata)?;
        self.record_rebound(&self.path(path)?)?;
        Ok(created)
    }

    fn create_special(
        &self,
        path: &MountPath,
        kind: MountNodeKind,
        device: Option<(u32, u32)>,
        metadata: FileMetadata,
    ) -> Result<MountLookup, MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        self.promote_parents_locked(path)?;
        let created = self.authored.create_special(path, kind, device, metadata)?;
        self.record_rebound(&self.path(path)?)?;
        Ok(created)
    }

    fn set_attributes(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
    ) -> Result<(), MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        if let Some(file) = self.detached_for_path(path)? {
            return file.set_attributes(metadata, logical_bytes);
        }
        self.promote_locked(path)?;
        self.authored.set_attributes(path, metadata, logical_bytes)
    }

    fn read_attribute(
        &self,
        path: &MountPath,
        name: &[u8],
    ) -> Result<Option<Bytes>, MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        if let Some(file) = self.detached_for_path(path)? {
            return file.read_attribute(name);
        }
        self.promote_locked(path)?;
        self.authored.read_attribute(path, name)
    }

    fn list_attributes(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountAttributePage, MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        if let Some(file) = self.detached_for_path(path)? {
            return file.list_attributes(cursor, maximum_entries);
        }
        self.promote_locked(path)?;
        self.authored.list_attributes(path, cursor, maximum_entries)
    }

    fn write_attribute(
        &self,
        path: &MountPath,
        name: &[u8],
        value: Bytes,
        mode: MountAttributeWriteMode,
    ) -> Result<(), MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        if let Some(file) = self.detached_for_path(path)? {
            return file.write_attribute(name, value, mode);
        }
        self.promote_locked(path)?;
        self.authored.write_attribute(path, name, value, mode)
    }

    fn remove_attribute(&self, path: &MountPath, name: &[u8]) -> Result<(), MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        if let Some(file) = self.detached_for_path(path)? {
            return file.remove_attribute(name);
        }
        self.promote_locked(path)?;
        self.authored.remove_attribute(path, name)
    }

    fn write_range(
        &self,
        path: &MountPath,
        offset: u64,
        bytes: Bytes,
    ) -> Result<(), MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        if let Some(file) = self.detached_for_path(path)? {
            return file.write_range(offset, bytes);
        }
        self.promote_locked(path)?;
        self.authored.write_range(path, offset, bytes)
    }

    fn resize(&self, path: &MountPath, logical_bytes: u64) -> Result<(), MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        if let Some(file) = self.detached_for_path(path)? {
            return file.resize(logical_bytes);
        }
        self.promote_locked(path)?;
        self.authored.resize(path, logical_bytes)
    }

    fn allocate_range(
        &self,
        path: &MountPath,
        offset: u64,
        length: u64,
        operation: MountRangeAllocation,
    ) -> Result<(), MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        if let Some(file) = self.detached_for_path(path)? {
            return file.allocate_range(offset, length, operation);
        }
        self.promote_locked(path)?;
        self.authored
            .allocate_range(path, offset, length, operation)
    }

    fn clone_range(
        &self,
        source: &MountPath,
        source_offset: u64,
        destination: &MountPath,
        destination_offset: u64,
        length: u64,
    ) -> Result<(), MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        if self.detached_for_path(source)?.is_some()
            || self.detached_for_path(destination)?.is_some()
        {
            return Err(MountSourceError::Unsupported(
                "range cloning a detached identity requires identity-level clone support"
                    .to_owned(),
            ));
        }
        self.promote_locked(source)?;
        self.promote_locked(destination)?;
        self.authored.clone_range(
            source,
            source_offset,
            destination,
            destination_offset,
            length,
        )
    }

    fn clone_range_by_id(
        &self,
        source_file_id: FileId,
        source_offset: u64,
        destination_file_id: FileId,
        destination_offset: u64,
        length: u64,
    ) -> Result<(), MountSourceError> {
        let _ = (
            source_file_id,
            source_offset,
            destination_file_id,
            destination_offset,
            length,
        );
        Err(MountSourceError::Unsupported(
            "clone-by-identity requires path-backed promotion in a lazy mount".to_owned(),
        ))
    }

    fn remove(&self, path: &MountPath, expected: Option<FileId>) -> Result<(), MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        let text = self.path(path)?;
        let removed_id = if let Some(authored) = self.authored.lookup(path)? {
            if self.removed_identity(&text, authored.node.file_id)? {
                return Err(MountSourceError::NotFound);
            }
            if expected.is_some_and(|expected| expected != authored.node.file_id) {
                return Err(MountSourceError::Stale);
            }
            let removed_open = if self.has_open_source_handle(authored.node.file_id)? {
                let record = self.authored.record_by_id(authored.node.file_id)?;
                let file = self
                    .authored
                    .detached_file_from_record(record, authored.metadata)?;
                Some((record, authored.metadata, file))
            } else {
                None
            };
            self.authored.remove(path, expected)?;
            if let Some((record, metadata, file)) = removed_open {
                self.record_removed_identity(record, metadata, file)?;
            }
            authored.node.file_id
        } else {
            if self.is_removed(&text)? {
                return Err(MountSourceError::NotFound);
            }
            let file_id = self.wait(|| async {
                let lookup = self.lazy.lookup(&text).await.map_err(lazy_error)?;
                let actual = self
                    .lazy
                    .stable_file_id_for_lookup(&text, &lookup)
                    .await
                    .map_err(lazy_error)?;
                if expected.is_some_and(|expected| expected != actual) {
                    return Err(MountSourceError::Stale);
                }
                Ok(actual)
            })?;
            if self.has_open_source_handle(file_id)? {
                self.promote_locked(path)?;
                let record = self.authored.record_by_id(file_id)?;
                let metadata = self
                    .authored
                    .lookup(path)?
                    .ok_or(MountSourceError::Stale)?
                    .metadata;
                let file = self.authored.detached_file_from_record(record, metadata)?;
                self.authored.remove(path, Some(file_id))?;
                self.record_removed_identity(record, metadata, file)?;
            }
            file_id
        };
        self.record_removed(path, removed_id)
    }

    fn rename(
        &self,
        source: &MountPath,
        destination: &MountPath,
        replace: bool,
    ) -> Result<(), MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        let source_text = self.path(source)?;
        let destination_text = self.path(destination)?;
        if self.authored.lookup(source)?.is_none() && self.is_removed(&source_text)? {
            return Err(MountSourceError::NotFound);
        }
        if source_text == destination_text {
            if self.authored.lookup(source)?.is_some() {
                return Ok(());
            }
            return self.wait(|| async {
                self.lazy.lookup(&source_text).await.map_err(lazy_error)?;
                Ok(())
            });
        }
        if self.authored.lookup(source)?.is_none() {
            let source_lookup =
                self.wait(|| async { self.lazy.lookup(&source_text).await.map_err(lazy_error) })?;
            if matches!(
                source_lookup,
                LazyLookup::Source(SourceNode {
                    kind: SourceNodeKind::Directory,
                    ..
                })
            ) {
                return Err(MountSourceError::Unsupported(
                    "renaming an unresolved lazy directory requires a subtree remap".to_owned(),
                ));
            }
        }
        self.promote_locked(source)?;
        let source_id = self
            .authored
            .lookup(source)?
            .ok_or(MountSourceError::Stale)?
            .node
            .file_id;
        self.promote_parents_locked(destination)?;
        if self.authored.lookup(destination)?.is_none() && !self.is_removed(&destination_text)? {
            let source_destination = self.wait(|| async {
                self.lazy
                    .lookup(&destination_text)
                    .await
                    .map_err(lazy_error)
            });
            match source_destination {
                Ok(_) if replace => self.promote_locked(destination)?,
                Ok(_) => return Err(MountSourceError::AlreadyExists),
                Err(MountSourceError::NotFound) => {}
                Err(error) => return Err(error),
            }
        }
        let replaced_open = if replace {
            if let Some(destination_lookup) = self.authored.lookup(destination)? {
                let destination_id = destination_lookup.node.file_id;
                if destination_id != source_id && self.has_open_source_handle(destination_id)? {
                    let record = self.authored.record_by_id(destination_id)?;
                    let file = self
                        .authored
                        .detached_file_from_record(record, destination_lookup.metadata)?;
                    Some((record, destination_lookup.metadata, file))
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };
        self.authored.rename(source, destination, replace)?;
        if let Some((record, metadata, file)) = replaced_open {
            self.record_removed_identity(record, metadata, file)?;
        }
        self.record_removed(source, source_id)?;
        self.record_rebound(&destination_text)
    }

    fn hard_link(
        &self,
        source: &MountPath,
        destination: &MountPath,
    ) -> Result<(), MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        self.promote_locked(source)?;
        self.promote_parents_locked(destination)?;
        self.authored.hard_link(source, destination)?;
        self.record_rebound(&self.path(destination)?)
    }

    fn flush(&self) -> Result<(), MountSourceError> {
        // As at every checkout's native boundary: a manual mount publishes
        // only through `sync` and `unmount`.
        if self.publication == MountPublication::Manual {
            return Ok(());
        }
        let _mutation = self.mutation_lease(None)?;

        self.wait(|| async {
            self.sync_staged_with_permit(crate::PublicationPermit::Unrestricted)
                .await
        })
    }

    fn capture_host_path(
        &self,
        source_root: &Path,
        path: &MountPath,
    ) -> Result<(), MountSourceError> {
        self.capture_host_paths(source_root, std::slice::from_ref(path))
    }

    fn capture_host_paths(
        &self,
        source_root: &Path,
        paths: &[MountPath],
    ) -> Result<(), MountSourceError> {
        if paths.is_empty() {
            return Ok(());
        }
        let expected_root_identity = capture_root_identity(source_root)
            .map_err(|error| MountSourceError::Engine(error.to_string()))?;
        self.capture_host_paths_with_identity(source_root, paths, expected_root_identity)
    }

    fn capture_host_subtree(
        &self,
        source_root: &Path,
        path: &MountPath,
    ) -> Result<(), MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        self.promote_parents_locked(path)?;
        match self.promote_locked(path) {
            Ok(()) | Err(MountSourceError::NotFound) => {}
            Err(error) => return Err(error),
        }
        self.authored.capture_host_subtree(source_root, path)
    }
}

impl<A, O, D, S> LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    pub(crate) fn capture_host_paths_with_identity(
        &self,
        source_root: &Path,
        paths: &[MountPath],
        expected_root_identity: NativeRootIdentity,
    ) -> Result<(), MountSourceError> {
        if paths.is_empty() {
            return Ok(());
        }
        let _mutation = self.mutation_lease(None)?;
        for path in paths {
            self.promote_parents_locked(path)?;
            match self.promote_locked(path) {
                Ok(()) | Err(MountSourceError::NotFound) => {}
                Err(error) => return Err(error),
            }
        }
        self.authored
            .capture_host_paths_with_identity(source_root, paths, expected_root_identity)
    }

    fn bind_open_file(
        &self,
        file: Arc<dyn MountOpenFile>,
        file_id: Option<FileId>,
        generation: u64,
    ) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        let bound: Arc<dyn MountOpenFile> = Arc::new(ViewBoundOpenFile {
            inner: file,
            runtime: Arc::clone(&self.runtime),
            source_view: Arc::clone(&self.source_view),
            detached: Arc::clone(&self.detached),
            open_sources: Arc::clone(&self.open_sources),
            file_id,
            generation,
        });
        if let Some(file_id) = file_id {
            self.open_sources
                .lock()
                .map_err(|_| MountSourceError::Stale)?
                .entry(file_id)
                .or_default()
                .push(Arc::downgrade(&bound));
        }
        Ok(bound)
    }

    fn mutation_lease(
        &self,
        expected: Option<u64>,
    ) -> Result<SourceMutationLease, MountSourceError> {
        self.wait(|| {
            let source_view = Arc::clone(&self.source_view);
            async move { source_view.write_stable(expected).await }
        })
    }

    fn view_lease(&self, expected: Option<u64>) -> Result<SourceViewLease, MountSourceError> {
        let owner = SourceViewGate::callback_owner();
        self.wait(|| {
            let source_view = Arc::clone(&self.source_view);
            async move { source_view.read_for_callback(owner, expected).await }
        })
    }

    fn promote_locked(&self, path: &MountPath) -> Result<(), MountSourceError> {
        if self.authored.lookup(path)?.is_some() {
            return Ok(());
        }
        if self.is_removed(&self.path(path)?)? {
            return Err(MountSourceError::NotFound);
        }
        self.wait(|| self.promote(path))
    }

    /// Reads one range of a path's current content. A pin additionally
    /// requires still source-backed content to be exactly the pinned
    /// version; content authored through the mount since is its newer state.
    fn read_content(
        &self,
        path: &MountPath,
        pin: Option<MountContentPin>,
        offset: u64,
        length: u32,
    ) -> Result<Bytes, MountSourceError> {
        let _lease = self.view_lease(None)?;
        if let Some(file) = self.detached_for_path(path)? {
            return file.read_range(offset, length);
        }
        if self.authored.lookup(path)?.is_some() {
            return self.authored.read_range(path, offset, length);
        }
        let path = self.path(path)?;
        if self.is_removed(&path)? {
            return Err(MountSourceError::NotFound);
        }
        self.wait(|| async move {
            let (lookup, source) = self
                .lazy
                .inspect_unauthored(&path, None)
                .await
                .map_err(lazy_error)?;
            match (lookup, source) {
                (LazyLookup::Source(node), Some(source)) => {
                    if pin.is_some_and(|pin| pin != source_content_pin(source, &node)) {
                        return Err(MountSourceError::Stale);
                    }
                    self.lazy
                        .open_source_file(&path, source, node)
                        .await
                        .and_then(|file| {
                            file.read_range(
                                offset,
                                u64::from(length),
                                &crate::CancellationToken::new(),
                            )
                            .map(|receipt| receipt.value)
                            .map_err(|failure| failure.error.into())
                        })
                }
                _ => self.lazy.read_range(&path, offset, u64::from(length)).await,
            }
            .map_err(lazy_error)
        })
    }
}

/// Binds one source node's exact content version into an opaque pin.
///
/// The source epoch is deliberately excluded: a rebind that leaves a file's
/// version unchanged leaves its content, and every promise about it, intact.
fn source_content_pin(source: SourceReference, node: &SourceNode) -> MountContentPin {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic-fs-lazy-content-pin-v1\0");
    hasher.update(&source.identity);
    hasher.update(&node.file_identity);
    hasher.update(&node.version.0);
    MountContentPin(*hasher.finalize().as_bytes())
}

fn mount_lookup(lookup: LazyLookup, file_id: FileId) -> MountLookup {
    match lookup {
        LazyLookup::Authored { stat, .. } => MountLookup {
            node: MountNode {
                file_id,
                kind: file_kind(stat.kind),
                logical_bytes: stat.logical_bytes.unwrap_or(0),
                link_count: stat.link_count,
                device: None,
            },
            metadata: workspace_metadata(stat.metadata),
        },
        LazyLookup::Shadow {
            record,
            metadata,
            source_link_count,
        } => MountLookup {
            node: MountNode {
                file_id,
                kind: file_kind(record.kind),
                logical_bytes: record_logical_bytes(record.payload),
                link_count: source_link_count,
                device: match record.payload {
                    FilePayload::Device { major, minor } => Some((major, minor)),
                    _ => None,
                },
            },
            metadata: workspace_metadata(metadata),
        },
        LazyLookup::Source(node) => MountLookup {
            node: MountNode {
                file_id,
                kind: source_kind(node.kind),
                logical_bytes: node.logical_bytes.unwrap_or(0),
                link_count: node.link_count.unwrap_or(1),
                device: node.device,
            },
            metadata: source_metadata(node.metadata),
        },
    }
}

const fn source_kind(kind: SourceNodeKind) -> MountNodeKind {
    match kind {
        SourceNodeKind::RegularFile => MountNodeKind::Regular,
        SourceNodeKind::Directory => MountNodeKind::Directory,
        SourceNodeKind::SymbolicLink => MountNodeKind::SymbolicLink,
        SourceNodeKind::Fifo => MountNodeKind::Fifo,
        SourceNodeKind::Socket => MountNodeKind::Socket,
        SourceNodeKind::CharacterDevice => MountNodeKind::CharacterDevice,
        SourceNodeKind::BlockDevice => MountNodeKind::BlockDevice,
        SourceNodeKind::Unsupported => MountNodeKind::Unsupported,
    }
}

fn record_logical_bytes(payload: FilePayload) -> u64 {
    match payload {
        FilePayload::InlineRegular(bytes) => {
            u64::try_from(bytes.as_bytes().len()).unwrap_or(u64::MAX)
        }
        FilePayload::Regular { logical_bytes, .. }
        | FilePayload::SymbolicLink {
            target_bytes: logical_bytes,
            ..
        }
        | FilePayload::ReparsePoint {
            payload_bytes: logical_bytes,
            ..
        } => logical_bytes,
        FilePayload::Directory { .. } | FilePayload::Empty | FilePayload::Device { .. } => 0,
    }
}

const fn file_kind(kind: FileKind) -> MountNodeKind {
    match kind {
        FileKind::Regular => MountNodeKind::Regular,
        FileKind::Directory => MountNodeKind::Directory,
        FileKind::SymbolicLink => MountNodeKind::SymbolicLink,
        FileKind::Fifo => MountNodeKind::Fifo,
        FileKind::Socket => MountNodeKind::Socket,
        FileKind::CharacterDevice => MountNodeKind::CharacterDevice,
        FileKind::BlockDevice => MountNodeKind::BlockDevice,
        FileKind::ReparsePoint | FileKind::MountBoundary => MountNodeKind::Unsupported,
    }
}

fn workspace_metadata(metadata: WorkspaceMetadata) -> FileMetadata {
    FileMetadata {
        posix_mode: field(metadata.posix_mode),
        posix_uid: field(metadata.posix_uid),
        posix_gid: field(metadata.posix_gid),
        posix_flags: field(metadata.posix_flags),
        windows_attributes: field(metadata.windows_attributes),
        created_ns: field(metadata.created_ns),
        modified_ns: field(metadata.modified_ns),
        accessed_ns: field(metadata.accessed_ns),
        changed_ns: field(metadata.changed_ns),
        ..FileMetadata::default()
    }
}

fn source_metadata(metadata: crate::demand::SourceMetadata) -> FileMetadata {
    FileMetadata {
        posix_mode: field(metadata.posix_mode),
        posix_uid: field(metadata.posix_uid),
        posix_gid: field(metadata.posix_gid),
        posix_flags: field(metadata.posix_flags),
        windows_attributes: field(metadata.windows_attributes),
        created_ns: field(
            metadata
                .created_ns
                .and_then(|value| i64::try_from(value).ok()),
        ),
        modified_ns: field(
            metadata
                .modified_ns
                .and_then(|value| i64::try_from(value).ok()),
        ),
        accessed_ns: field(
            metadata
                .accessed_ns
                .and_then(|value| i64::try_from(value).ok()),
        ),
        changed_ns: field(
            metadata
                .changed_ns
                .and_then(|value| i64::try_from(value).ok()),
        ),
        ..FileMetadata::default()
    }
}

fn field<T>(value: Option<T>) -> MetadataField<T> {
    value.map_or(MetadataField::Unavailable, MetadataField::Value)
}

#[allow(clippy::needless_pass_by_value)]
fn lazy_error(error: LazyWorkspaceError) -> MountSourceError {
    match error {
        LazyWorkspaceError::NotFound => MountSourceError::NotFound,
        LazyWorkspaceError::NotRegularFile | LazyWorkspaceError::InvalidPageBound => {
            MountSourceError::Invalid(error.to_string())
        }
        LazyWorkspaceError::UnsupportedNode | LazyWorkspaceError::UnresolvedHardLinks => {
            MountSourceError::Unsupported(error.to_string())
        }
        LazyWorkspaceError::StaleSource
        | LazyWorkspaceError::StaleCursor
        | LazyWorkspaceError::StaleIdentity
        | LazyWorkspaceError::Concurrent => MountSourceError::Stale,
        _ => MountSourceError::Engine(error.to_string()),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn listed_names_are_decoded_by_their_declared_encoding()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::kernel::{LogicalName, NameEncoding};

        let text = "dir-\u{e9}";
        let utf16 = text
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect::<Vec<_>>();
        let windows = LogicalName::new(NameEncoding::WindowsUtf16Le, utf16.clone(), 510)?;
        let portable = LogicalName::new(NameEncoding::Utf8, text.as_bytes().to_vec(), 255)?;
        for name in [&windows, &portable] {
            assert_eq!(
                logical_child_path("/parent", name).as_deref(),
                Some("/parent/dir-\u{e9}")
            );
        }
        if cfg!(windows) {
            for name in [&windows, &portable] {
                assert_eq!(super::super::adapter::native_mount_name(name)?, utf16);
            }
        } else {
            assert_eq!(
                super::super::adapter::native_mount_name(&portable)?,
                text.as_bytes()
            );
        }
        Ok(())
    }

    #[test]
    fn rebound_prunes_only_unreferenced_removed_identities()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::kernel::InlineFileData;
        use crate::{Digest, ObjectId, ObjectKind};

        let first = FileId::from_bytes([1; 16]);
        let second = FileId::from_bytes([2; 16]);
        let payload = InlineFileData::new(b"x")?;
        let record = |file_id| {
            (
                FileRecord {
                    file_id,
                    kind: FileKind::Regular,
                    link_count: 1,
                    metadata: ObjectId {
                        kind: ObjectKind::Metadata,
                        digest: Digest::from_bytes([0; 32]),
                    },
                    payload: FilePayload::InlineRegular(payload),
                },
                FileMetadata::default(),
            )
        };
        let mut removals = MountedRemovals {
            paths: BTreeMap::from([
                ("/a".to_owned(), first),
                ("/b".to_owned(), first),
                ("/c".to_owned(), second),
            ]),
            records: BTreeMap::from([(first, record(first)), (second, record(second))]),
            epoch: 0,
        };
        assert!(removals.rebound("/a"));
        assert!(removals.records.contains_key(&first));
        assert!(removals.rebound("/b"));
        assert!(!removals.records.contains_key(&first));
        assert!(removals.records.contains_key(&second));
        assert!(removals.rebound("/c"));
        assert!(removals.records.is_empty());
        assert!(!removals.rebound("/c"));
        assert_eq!(removals.epoch, 3);
        Ok(())
    }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[tokio::test(flavor = "multi_thread")]
    async fn rebound_does_not_publish_a_stale_removed_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::demand::native::NativeDemandSource;
        use crate::model::{CheckoutMode, FilesystemProfile, GenerationSelector, VolumeLimits};
        use crate::native_mount::{MountPublication, SharedCheckout};
        use crate::{Fs, MemoryLazyWorkspaceStore};

        let source_root = tempfile::tempdir()?;
        std::fs::write(source_root.path().join("a"), b"old-a")?;
        std::fs::write(source_root.path().join("b"), b"old-b")?;
        let demand = Arc::new(
            NativeDemandSource::open(
                source_root.path(),
                FilesystemProfile::Portable,
                VolumeLimits::default(),
            )
            .await?,
        );
        let fs = Fs::memory();
        let lazy = Arc::new(
            LazyWorkspace::attach(
                &fs,
                "mounted-rebound-identity",
                demand,
                MemoryLazyWorkspaceStore::default(),
            )
            .await?,
        );
        let checkout = lazy
            .workspace()
            .engine_checkout(
                GenerationSelector::Head,
                CheckoutMode::tracking_transaction(),
            )
            .await?;
        let config = checkout.volume_config();
        let authored = Arc::new(CheckoutMountSource::new(
            Arc::new(SharedCheckout::with_publication(
                checkout,
                MountPublication::Manual,
            )),
            config,
        )?);
        let view = LazyMountSource::new(
            Arc::clone(&lazy),
            authored,
            "/".to_owned(),
            MountPublication::Manual,
        )?;
        let path = |name: &str| {
            MountPath::root().child(if cfg!(windows) {
                name.encode_utf16().flat_map(u16::to_le_bytes).collect()
            } else {
                name.as_bytes().to_vec()
            })
        };
        let a = path("a");
        let b = path("b");
        let _open_a = view.open_file(&a)?;
        let _open_b = view.open_file(&b)?;
        let old_a = view.lookup(&a)?.expect("source a").node.file_id;
        let old_b = view.lookup(&b)?.expect("source b").node.file_id;
        view.remove(&a, Some(old_a))?;
        view.create_file(&a, FileMetadata::default())?;
        view.write_range(&a, 0, Bytes::from_static(b"new-a"))?;
        view.remove(&b, Some(old_b))?;
        {
            let removals = view.removals.lock().map_err(|_| "poisoned removals")?;
            assert_eq!(removals.paths.len(), 1);
            assert_eq!(removals.records.len(), 1);
            assert!(removals.records.contains_key(&old_b));
        }
        view.sync_async().await?;
        assert_eq!(lazy.read_range("/a", 0, 5).await?.as_ref(), b"new-a");
        assert!(matches!(
            lazy.lookup("/b").await,
            Err(LazyWorkspaceError::NotFound)
        ));
        Ok(())
    }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[tokio::test(flavor = "multi_thread")]
    async fn manual_fsync_publishes_nothing_until_sync() -> Result<(), Box<dyn std::error::Error>> {
        use crate::demand::native::NativeDemandSource;
        use crate::model::{CheckoutMode, FilesystemProfile, GenerationSelector, VolumeLimits};
        use crate::native_mount::SharedCheckout;
        use crate::{Fs, MemoryLazyWorkspaceStore};

        let source_root = tempfile::tempdir()?;
        let demand = Arc::new(
            NativeDemandSource::open(
                source_root.path(),
                FilesystemProfile::Portable,
                VolumeLimits::default(),
            )
            .await?,
        );
        let fs = Fs::memory();
        let lazy = Arc::new(
            LazyWorkspace::attach(
                &fs,
                "manual-fsync",
                demand,
                MemoryLazyWorkspaceStore::default(),
            )
            .await?,
        );
        let checkout = lazy
            .workspace()
            .engine_checkout(
                GenerationSelector::Head,
                CheckoutMode::tracking_transaction(),
            )
            .await?;
        let config = checkout.volume_config();
        let authored = Arc::new(CheckoutMountSource::new(
            Arc::new(SharedCheckout::with_publication(
                checkout,
                MountPublication::Manual,
            )),
            config,
        )?);
        let view = LazyMountSource::new(
            Arc::clone(&lazy),
            authored,
            "/".to_owned(),
            MountPublication::Manual,
        )?;
        let written = MountPath::root().child(if cfg!(windows) {
            "written"
                .encode_utf16()
                .flat_map(u16::to_le_bytes)
                .collect()
        } else {
            b"written".to_vec()
        });
        view.create_file(&written, FileMetadata::default())?;
        view.write_range(&written, 0, Bytes::from_static(b"fsynced"))?;
        view.flush()?;
        assert!(
            matches!(
                lazy.lookup("/written").await,
                Err(LazyWorkspaceError::NotFound)
            ),
            "a native fsync under manual publication publishes nothing"
        );
        view.sync_async().await?;
        assert_eq!(
            lazy.read_range("/written", 0, 7).await?.as_ref(),
            b"fsynced"
        );
        Ok(())
    }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[tokio::test(flavor = "multi_thread")]
    async fn replaced_source_identity_keeps_open_handle_and_surviving_alias()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::demand::native::NativeDemandSource;
        use crate::model::{CheckoutMode, FilesystemProfile, GenerationSelector, VolumeLimits};
        use crate::native_mount::{MountPublication, SharedCheckout};
        use crate::{Fs, MemoryLazyWorkspaceStore};

        let source_root = tempfile::tempdir()?;
        std::fs::write(source_root.path().join("a"), b"source")?;
        std::fs::write(source_root.path().join("c"), b"target")?;
        std::fs::hard_link(source_root.path().join("c"), source_root.path().join("d"))?;
        let demand = Arc::new(
            NativeDemandSource::open(
                source_root.path(),
                FilesystemProfile::Portable,
                VolumeLimits::default(),
            )
            .await?,
        );
        let fs = Fs::memory();
        let lazy = Arc::new(
            LazyWorkspace::attach(
                &fs,
                "mounted-identity-replace",
                demand,
                MemoryLazyWorkspaceStore::default(),
            )
            .await?,
        );
        let checkout = lazy
            .workspace()
            .engine_checkout(
                GenerationSelector::Head,
                CheckoutMode::tracking_transaction(),
            )
            .await?;
        let config = checkout.volume_config();
        let authored = Arc::new(CheckoutMountSource::new(
            Arc::new(SharedCheckout::with_publication(
                checkout,
                MountPublication::Manual,
            )),
            config,
        )?);
        let view = LazyMountSource::new(
            Arc::clone(&lazy),
            authored,
            "/".to_owned(),
            MountPublication::Manual,
        )?;
        let mount_path = |name: &str| {
            let bytes = if cfg!(windows) {
                name.encode_utf16().flat_map(u16::to_le_bytes).collect()
            } else {
                name.as_bytes().to_vec()
            };
            MountPath::root().child(bytes)
        };
        let a = mount_path("a");
        let c = mount_path("c");
        let d = mount_path("d");
        let old = view.open_file(&c)?;
        view.rename(&a, &c, true)?;
        old.write_range(0, Bytes::from_static(b"update"))?;
        assert_eq!(view.read_range(&c, 0, 6)?.as_ref(), b"source");
        assert_eq!(view.read_range(&d, 0, 6)?.as_ref(), b"update");
        view.sync_async().await?;
        assert_eq!(lazy.read_range("/c", 0, 6).await?.as_ref(), b"source");
        assert_eq!(lazy.read_range("/d", 0, 6).await?.as_ref(), b"update");
        old.write_range(0, Bytes::from_static(b"second!"))?;
        view.sync_async().await?;
        assert_eq!(lazy.read_range("/c", 0, 6).await?.as_ref(), b"source");
        assert_eq!(lazy.read_range("/d", 0, 7).await?.as_ref(), b"second!");
        view.allocate_range(&d, 0, 1, MountRangeAllocation::PunchHole)?;
        view.sync_async().await?;
        assert_eq!(lazy.read_range("/d", 0, 7).await?.as_ref(), b"\0econd!");
        let current = view.open_file(&c)?;
        let current_id = current.lookup()?.node.file_id;
        view.remove(&c, Some(current_id))?;
        current.write_range(0, Bytes::from_static(b"orphan"))?;
        assert_eq!(current.read_range(0, 6)?.as_ref(), b"orphan");
        let mut external = lazy
            .workspace()
            .begin_transaction(IdempotencyKey::new())
            .await?;
        external
            .write("/external", Bytes::from_static(b"unrelated"))
            .await?;
        external.commit().await?;
        view.sync_async().await?;
        assert!(matches!(
            lazy.lookup("/c").await,
            Err(LazyWorkspaceError::NotFound)
        ));
        Ok(())
    }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[tokio::test(flavor = "multi_thread")]
    async fn conflicting_mount_removal_releases_publication_intent()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::demand::native::NativeDemandSource;
        use crate::model::{CheckoutMode, FilesystemProfile, GenerationSelector, VolumeLimits};
        use crate::native_mount::{MountPublication, SharedCheckout};
        use crate::{Fs, MemoryLazyWorkspaceStore};

        let source_root = tempfile::tempdir()?;
        std::fs::write(source_root.path().join("file"), b"source")?;
        let demand = Arc::new(
            NativeDemandSource::open(
                source_root.path(),
                FilesystemProfile::Portable,
                VolumeLimits::default(),
            )
            .await?,
        );
        let fs = Fs::memory();
        let lazy = Arc::new(
            LazyWorkspace::attach(
                &fs,
                "conflicting-mount-removal",
                demand,
                MemoryLazyWorkspaceStore::default(),
            )
            .await?,
        );
        let checkout = lazy
            .workspace()
            .engine_checkout(
                GenerationSelector::Head,
                CheckoutMode::tracking_transaction(),
            )
            .await?;
        let config = checkout.volume_config();
        let authored = Arc::new(CheckoutMountSource::new(
            Arc::new(SharedCheckout::with_publication(
                checkout,
                MountPublication::Manual,
            )),
            config,
        )?);
        let view = LazyMountSource::new(
            Arc::clone(&lazy),
            authored,
            "/".to_owned(),
            MountPublication::Manual,
        )?;
        let path = MountPath::root().child(if cfg!(windows) {
            "file".encode_utf16().flat_map(u16::to_le_bytes).collect()
        } else {
            b"file".to_vec()
        });
        let file = view.open_file(&path)?;
        view.remove(&path, Some(file.lookup()?.node.file_id))?;
        let mut conflicting = lazy
            .workspace()
            .begin_transaction(IdempotencyKey::new())
            .await?;
        conflicting
            .write("/file", Bytes::from_static(b"external"))
            .await?;
        conflicting.commit().await?;
        assert!(matches!(
            view.sync_async().await,
            Err(MountSourceError::Stale)
        ));
        assert!(
            !view
                .authored
                .shared_checkout()
                .lock()
                .await
                .has_retained_operation()
        );
        view.recover_pending_mount_change(crate::PublicationPermit::Unrestricted)
            .await?;
        assert!(lazy.mount_removal_publication().await?.is_none());
        Ok(())
    }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[tokio::test(flavor = "multi_thread")]
    async fn source_hard_link_open_unlink_write_preserves_surviving_alias()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::demand::native::NativeDemandSource;
        use crate::model::{CheckoutMode, FilesystemProfile, GenerationSelector, VolumeLimits};
        use crate::native_mount::{MountPublication, SharedCheckout};
        use crate::{Fs, MemoryLazyWorkspaceStore};

        for (write_before_unlink, write_before_first_sync, promote_survivor) in [
            (false, false, false),
            (false, true, false),
            (true, true, false),
            (true, true, true),
        ] {
            let source_root = tempfile::tempdir()?;
            std::fs::write(source_root.path().join("a"), b"source")?;
            std::fs::hard_link(source_root.path().join("a"), source_root.path().join("b"))?;
            let demand = Arc::new(
                NativeDemandSource::open(
                    source_root.path(),
                    FilesystemProfile::Portable,
                    VolumeLimits::default(),
                )
                .await?,
            );
            let fs = Fs::memory();
            let lazy = Arc::new(
                LazyWorkspace::attach(
                    &fs,
                    "mounted-identity-unlink",
                    demand,
                    MemoryLazyWorkspaceStore::default(),
                )
                .await?,
            );
            let checkout = lazy
                .workspace()
                .engine_checkout(
                    GenerationSelector::Head,
                    CheckoutMode::tracking_transaction(),
                )
                .await?;
            let config = checkout.volume_config();
            let authored = Arc::new(CheckoutMountSource::new(
                Arc::new(SharedCheckout::with_publication(
                    checkout,
                    MountPublication::Manual,
                )),
                config,
            )?);
            let view = LazyMountSource::new(
                Arc::clone(&lazy),
                authored,
                "/".to_owned(),
                MountPublication::Manual,
            )?;
            let mount_path = |name: &str| {
                let bytes = if cfg!(windows) {
                    name.encode_utf16().flat_map(u16::to_le_bytes).collect()
                } else {
                    name.as_bytes().to_vec()
                };
                MountPath::root().child(bytes)
            };
            let a = mount_path("a");
            let b = mount_path("b");
            let open = view.open_file(&a)?;
            let identity = open.lookup()?.node.file_id;
            assert!(
                view.prepare_removed_identity_candidate(
                    &BTreeMap::from([("/\0".to_owned(), identity,)]),
                    &[]
                )
                .await
                .is_err()
            );
            assert!(
                !view
                    .authored
                    .shared_checkout()
                    .lock()
                    .await
                    .has_retained_operation()
            );
            assert_eq!(
                view.lookup(&b)?.expect("surviving alias").node.file_id,
                identity
            );
            if write_before_unlink {
                open.write_range(0, Bytes::from_static(b"prime!"))
                    .expect("pre-unlink write");
            }
            let existing_alias = if promote_survivor {
                {
                    let _mutation = view.mutation_lease(None)?;
                    view.promote_locked(&b)?;
                }
                assert!(view.authored.lookup(&b)?.is_some());
                Some(view.open_file(&b)?)
            } else {
                None
            };
            view.remove(&a, Some(identity))
                .expect("unlink source alias");
            if write_before_first_sync {
                open.write_range(0, Bytes::from_static(b"update"))
                    .expect("post-unlink write");
            }
            let first = if write_before_first_sync {
                b"update"
            } else {
                b"source"
            };
            assert!(view.lookup(&a)?.is_none(), "removed name must stay absent");
            assert_eq!(view.open_file(&b)?.read_range(0, 6)?.as_ref(), first);
            if let Some(alias) = &existing_alias {
                assert_eq!(alias.read_range(0, 6)?.as_ref(), first);
            }
            view.sync_async().await?;
            assert!(
                view.lookup(&a)?.is_none(),
                "sync must not restore removed name"
            );
            assert_eq!(view.open_file(&b)?.read_range(0, 6)?.as_ref(), first);
            if promote_survivor {
                assert_eq!(view.authored.read_range(&b, 0, 6)?.as_ref(), first);
            }
            assert!(matches!(
                lazy.lookup("/a").await,
                Err(LazyWorkspaceError::NotFound)
            ));
            assert_eq!(lazy.read_range("/b", 0, 6).await?.as_ref(), first);
            open.write_range(0, Bytes::from_static(b"second!"))?;
            assert_eq!(view.lookup(&b)?.expect("alias").node.logical_bytes, 7);
            assert_eq!(view.read_range(&b, 0, 7)?.as_ref(), b"second!");
            assert_eq!(view.open_file(&b)?.read_range(0, 7)?.as_ref(), b"second!");
            if write_before_unlink {
                view.remove(&b, Some(identity))?;
                open.write_range(0, Bytes::from_static(b"orphan!"))?;
                assert_eq!(open.read_range(0, 7)?.as_ref(), b"orphan!");
                assert!(view.lookup(&b)?.is_none());
            }
            drop(existing_alias);
            drop(open);
            assert!(view.open_sources.lock().expect("open handles").is_empty());
            view.sync_async().await?;
            assert!(
                view.detached
                    .lock()
                    .expect("detached identities")
                    .is_empty()
            );
            if write_before_unlink {
                assert!(matches!(
                    lazy.lookup("/b").await,
                    Err(LazyWorkspaceError::NotFound)
                ));
            } else {
                assert_eq!(lazy.read_range("/b", 0, 7).await?.as_ref(), b"second!");
                assert_eq!(view.open_file(&b)?.read_range(0, 7)?.as_ref(), b"second!");
            }
        }
        Ok(())
    }

    #[test]
    fn abandoned_directory_cursors_are_bounded_and_cleared() {
        let cursors = CursorTable::new(2);
        let first = cursors.remember((2, 1), 1_u8).expect("first cursor");
        let second = cursors.remember((2, 1), 2_u8).expect("second cursor");
        let third = cursors.remember((2, 1), 3_u8).expect("third cursor");

        assert!(matches!(
            cursors.take((2, 1), Some(&first)),
            Err(MountSourceError::Stale)
        ));
        assert_eq!(
            cursors.take((2, 1), Some(&second)).expect("second token"),
            Some(2)
        );
        assert_eq!(
            cursors.take((2, 1), Some(&third)).expect("third token"),
            Some(3)
        );

        let stale = cursors.remember((2, 1), 4_u8).expect("stale cursor");
        assert!(matches!(
            cursors.take((3, 1), Some(&stale)),
            Err(MountSourceError::Stale)
        ));
        let retained = cursors.remember((2, 1), 5_u8).expect("retained cursor");
        cursors.clear();
        assert!(matches!(
            cursors.take((2, 1), Some(&retained)),
            Err(MountSourceError::Stale)
        ));
    }

    #[tokio::test]
    async fn source_view_gate_serializes_rebinds_and_fences_old_handles() {
        let gate = Arc::new(SourceViewGate::new());
        let initial = Arc::clone(&gate).read(None).await.expect("initial lease");
        assert_eq!(initial.generation, SourceViewGate::INITIAL_GENERATION);

        let waiting_gate = Arc::clone(&gate);
        let writer = tokio::spawn(async move { waiting_gate.write().await });
        tokio::task::yield_now().await;
        assert!(
            !writer.is_finished(),
            "a rebind must wait for active readers"
        );
        drop(initial);

        let writer = writer.await.expect("writer task");
        gate.begin_transition();
        assert!(!gate.is_stable());
        gate.finish_transition();
        drop(writer);

        assert!(matches!(
            Arc::clone(&gate)
                .read(Some(SourceViewGate::INITIAL_GENERATION))
                .await,
            Err(MountSourceError::Stale)
        ));
        assert_eq!(
            Arc::clone(&gate)
                .read(None)
                .await
                .expect("new lease")
                .generation,
            SourceViewGate::INITIAL_GENERATION + 2
        );
    }

    #[tokio::test]
    async fn source_view_gate_excludes_authored_mutations_from_read_callbacks() {
        let gate = Arc::new(SourceViewGate::new());
        let reader = Arc::clone(&gate).read(None).await.expect("reader lease");
        let mutation_gate = Arc::clone(&gate);
        let mutation = tokio::spawn(async move {
            mutation_gate
                .write_stable(Some(SourceViewGate::INITIAL_GENERATION))
                .await
        });
        tokio::task::yield_now().await;
        assert!(
            !mutation.is_finished(),
            "an authored mutation must wait for active read callbacks"
        );
        drop(reader);

        let mutation = mutation
            .await
            .expect("mutation task")
            .expect("mutation lease");
        assert_eq!(mutation.generation, SourceViewGate::INITIAL_GENERATION);
        let reader_gate = Arc::clone(&gate);
        let reader = tokio::spawn(async move { reader_gate.read(None).await });
        tokio::task::yield_now().await;
        assert!(
            !reader.is_finished(),
            "a read callback must wait for an authored mutation"
        );
        drop(mutation);
        assert_eq!(
            reader
                .await
                .expect("reader task")
                .expect("reader lease")
                .generation,
            SourceViewGate::INITIAL_GENERATION
        );
    }

    #[tokio::test]
    async fn source_view_gate_allows_nested_callback_reads_while_writer_waits() {
        let gate = Arc::new(SourceViewGate::new());
        let owner = SourceViewGate::callback_owner();
        let outer = Arc::clone(&gate)
            .read_for_callback(owner, None)
            .await
            .expect("outer lease");
        let writer_gate = Arc::clone(&gate);
        let writer = tokio::spawn(async move { writer_gate.write().await });
        tokio::task::yield_now().await;
        assert!(!writer.is_finished(), "writer must wait for outer callback");

        let nested = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            Arc::clone(&gate).read_for_callback(owner, Some(outer.generation)),
        )
        .await
        .expect("a callback must not deadlock when its source method reacquires the view")
        .expect("nested lease");
        drop(nested);
        drop(outer);
        drop(writer.await.expect("writer task"));
    }

    #[tokio::test]
    async fn source_view_gate_gives_a_waiting_writer_priority_over_new_callbacks() {
        let gate = Arc::new(SourceViewGate::new());
        let active = Arc::clone(&gate).read(None).await.expect("active reader");
        let writer_gate = Arc::clone(&gate);
        let writer = tokio::spawn(async move { writer_gate.write().await });
        tokio::task::yield_now().await;
        assert!(!writer.is_finished(), "writer must wait for active reader");

        let later_gate = Arc::clone(&gate);
        let later = tokio::spawn(async move { later_gate.read(None).await });
        tokio::task::yield_now().await;
        assert!(
            !later.is_finished(),
            "a new callback bypassed a waiting writer"
        );

        drop(active);
        let writer = tokio::time::timeout(std::time::Duration::from_secs(1), writer)
            .await
            .expect("writer made no progress")
            .expect("writer task");
        assert!(!later.is_finished(), "reader crossed the active writer");
        drop(writer);
        drop(
            tokio::time::timeout(std::time::Duration::from_secs(1), later)
                .await
                .expect("later reader made no progress")
                .expect("reader task")
                .expect("reader lease"),
        );
    }
}
