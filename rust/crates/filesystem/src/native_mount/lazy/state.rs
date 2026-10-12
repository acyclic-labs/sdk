use super::*;

pub(in crate::native_mount::lazy) enum Resolution {
    Authored(MountLookup),
    Absent,
    Unauthored(LazyLookup, Option<SourceReference>, Option<crate::NativeAllocation>),
}

#[derive(Clone, Copy)]
pub(in crate::native_mount::lazy) struct Deferral {
    pub(in crate::native_mount::lazy) stamp: ViewStamp,
    pub(in crate::native_mount::lazy) source: SourceReference,
    pub(in crate::native_mount::lazy) node: Option<SourceNode>,
}

impl Deferral {
    pub(in crate::native_mount::lazy) fn resolution(self) -> Resolution {
        self.node.map_or(Resolution::Absent, |node| {
            Resolution::Unauthored(LazyLookup::Source(node), Some(self.source), None)
        })
    }
}

#[derive(Default)]
pub(in crate::native_mount::lazy) struct Resolutions(pub(in crate::native_mount::lazy) Mutex<HashMap<MountPath, Deferral>>);

impl Resolutions {
    pub(in crate::native_mount::lazy) fn get(&self, path: &MountPath) -> Option<Deferral> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(path)
            .copied()
    }

    pub(in crate::native_mount::lazy) fn remember(&self, path: &MountPath, deferral: Deferral) {
        let mut entries = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if entries.len() >= MAXIMUM_REMEMBERED_RESOLUTIONS {
            entries.clear();
        }
        entries.insert(path.clone(), deferral);
    }

    pub(in crate::native_mount::lazy) fn forget(&self, path: &MountPath) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(path);
    }
}

#[derive(Default)]
pub(in crate::native_mount::lazy) struct SourceAbsentDirectories(pub(in crate::native_mount::lazy) Mutex<HashMap<MountPath, (ViewStamp, SourceReference)>>);

impl SourceAbsentDirectories {
    pub(in crate::native_mount::lazy) fn get(&self, path: &MountPath) -> Option<(ViewStamp, SourceReference)> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(path)
            .copied()
    }

    pub(in crate::native_mount::lazy) fn remember(&self, path: &MountPath, stamp: ViewStamp, source: SourceReference) {
        let mut entries = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if entries.len() >= MAXIMUM_REMEMBERED_RESOLUTIONS {
            entries.clear();
        }
        entries.insert(path.clone(), (stamp, source));
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(in crate::native_mount::lazy) struct MountedRemoval {
    pub(in crate::native_mount::lazy) file_id: FileId,
    pub(in crate::native_mount::lazy) operation: Option<crate::OperationId>,
}

pub(in crate::native_mount::lazy) struct MountedRemovalPath {
    pub(in crate::native_mount::lazy) text: String,
    pub(in crate::native_mount::lazy) custody: Option<crate::native_mount::NativeMountAllocationCustody>,
    pub(in crate::native_mount::lazy) bytes: u64,
    pub(in crate::native_mount::lazy) native_allocation: Option<crate::NativeAllocation>,
}

impl MountedRemovalPath {
    pub(in crate::native_mount::lazy) fn unmeasured(text: String) -> Self { Self { text, custody: None, bytes: 0, native_allocation: None } }
    pub(in crate::native_mount::lazy) fn capacity(&self) -> usize { self.text.capacity() }
}

impl std::ops::Deref for MountedRemovalPath {
    type Target = str;
    fn deref(&self) -> &str { &self.text }
}

impl std::borrow::Borrow<str> for MountedRemovalPath {
    fn borrow(&self) -> &str { &self.text }
}

impl std::borrow::Borrow<String> for MountedRemovalPath {
    fn borrow(&self) -> &String { &self.text }
}

impl PartialEq for MountedRemovalPath {
    fn eq(&self, other: &Self) -> bool { self.text == other.text }
}

impl Eq for MountedRemovalPath {}

impl PartialOrd for MountedRemovalPath {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) }
}

impl Ord for MountedRemovalPath {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering { self.text.cmp(&other.text) }
}

impl Drop for MountedRemovalPath {
    fn drop(&mut self) {
        drop(std::mem::take(&mut self.text));
        if let Some(custody) = &self.custody {
            custody.retire(self.bytes).expect("the removed path drops its original owned payload first");
        }
    }
}

pub(in crate::native_mount::lazy) type CapturedRemovalData = ((FileRecord, FileMetadata), usize);

pub(in crate::native_mount::lazy) struct CapturedRemovalRecord {
    pub(in crate::native_mount::lazy) value: (FileRecord, FileMetadata),
    pub(in crate::native_mount::lazy) paths: usize,
    pub(in crate::native_mount::lazy) custody: Option<crate::native_mount::NativeMountAllocationCustody>,
    pub(in crate::native_mount::lazy) bytes: u64,
}

impl Drop for CapturedRemovalRecord {
    fn drop(&mut self) {
        if let Some(custody) = &self.custody {
            custody.retire(self.bytes).expect("captured removal data retires only its original owned bytes");
        }
    }
}

pub(in crate::native_mount::lazy) struct PreparedMountedRemoval {
    pub(in crate::native_mount::lazy) key: MountedRemovalPath,
    pub(in crate::native_mount::lazy) file_id: FileId,
    pub(in crate::native_mount::lazy) prior: Option<MountedRemoval>,
    pub(in crate::native_mount::lazy) old_paths: Option<usize>,
    pub(in crate::native_mount::lazy) new_paths: Option<usize>,
    pub(in crate::native_mount::lazy) changed: bool,
    pub(in crate::native_mount::lazy) final_work: crate::WorkCounters,
}

pub(in crate::native_mount::lazy) struct PreparedMountedIdentity<A, O> {
    pub(in crate::native_mount::lazy) record: FileRecord,
    pub(in crate::native_mount::lazy) metadata: FileMetadata,
    pub(in crate::native_mount::lazy) file: Arc<CheckoutDetachedFile<A, O>>,
    pub(in crate::native_mount::lazy) position: Option<usize>,
    pub(in crate::native_mount::lazy) paths: usize,
    pub(in crate::native_mount::lazy) captured: Option<CapturedRemovalRecord>,
    pub(in crate::native_mount::lazy) final_work: crate::WorkCounters,
}

pub(in crate::native_mount::lazy) struct PreparedMountedRebound {
    pub(in crate::native_mount::lazy) removed: Option<MountedRemoval>,
    pub(in crate::native_mount::lazy) paths: Option<usize>,
    pub(in crate::native_mount::lazy) final_work: crate::WorkCounters,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(in crate::native_mount::lazy) struct SelectedImportProof {
    pub(in crate::native_mount::lazy) checkpoint: crate::LazySnapshotRef,
    pub(in crate::native_mount::lazy) source: crate::LazySnapshotRef,
    pub(in crate::native_mount::lazy) source_config: crate::model::VolumeConfig,
    pub(in crate::native_mount::lazy) input: crate::Digest,
}

pub(in crate::native_mount::lazy) struct SelectedMountInput<'a> {
    pub(in crate::native_mount::lazy) destination: crate::LazySnapshotRef,
    pub(in crate::native_mount::lazy) plan: &'a crate::SelectedImportPlan,
    pub(in crate::native_mount::lazy) operation: crate::OperationId,
    pub(in crate::native_mount::lazy) original: &'a crate::NativeMaterializationWork,
}

#[derive(Clone, Copy)]
pub(in crate::native_mount::lazy) struct MountedPublicationCustody {
    pub(in crate::native_mount::lazy) operation: crate::OperationId,
    pub(in crate::native_mount::lazy) published: bool,
    pub(in crate::native_mount::lazy) selected: Option<SelectedImportProof>,
}

#[derive(Default)]
pub(in crate::native_mount::lazy) struct MountedRemovals {
    pub(in crate::native_mount::lazy) paths: BTreeMap<MountedRemovalPath, MountedRemoval>,
    pub(in crate::native_mount::lazy) records: BTreeMap<FileId, CapturedRemovalRecord>,
    pub(in crate::native_mount::lazy) publication: Option<MountedPublicationCustody>,
    pub(in crate::native_mount::lazy) epoch: u64,
    pub(in crate::native_mount::lazy) selected_inputs: Option<crate::lazy_workspace::SelectedImportEffects>,
}

pub(in crate::native_mount::lazy) struct SelectedInputsLoan<'a> {
    pub(in crate::native_mount::lazy) removals: &'a Mutex<MountedRemovals>,
    pub(in crate::native_mount::lazy) operation: crate::OperationId,
    pub(in crate::native_mount::lazy) value: Option<crate::lazy_workspace::SelectedImportEffects>,
}

impl<'a> SelectedInputsLoan<'a> {
    pub(in crate::native_mount::lazy) fn take(
        removals: &'a Mutex<MountedRemovals>, operation: crate::OperationId,
        work: &mut MountSyncWork<'_>,
    ) -> Result<Self, MountSourceError> {
        work.items(1)?;
        let mut retained = removals.lock().map_err(|_| MountSourceError::Stale)?;
        if !retained.publication.is_some_and(|custody| custody.operation == operation) {
            return Err(MountSourceError::Stale);
        }
        Ok(Self { removals, operation, value: retained.selected_inputs.take() })
    }

    pub(in crate::native_mount::lazy) fn discard(mut self) { drop(self.value.take()); }
}

impl Drop for SelectedInputsLoan<'_> {
    fn drop(&mut self) {
        let Some(value) = self.value.take() else { return; };
        let mut retained = self.removals.lock().unwrap_or_else(PoisonError::into_inner);
        if retained.publication.is_some_and(|custody| custody.operation == self.operation) {
            let displaced = retained.selected_inputs.replace(value);
            drop(retained);
            drop(displaced);
        } else {
            drop(retained);
            drop(value);
        }
    }
}

impl MountedRemovals {
    pub(in crate::native_mount::lazy) fn rebound(&mut self, path: &str) -> bool {
        let Some(removed) = self.paths.remove(path) else { return false; };
        if let Some(record) = self.records.get_mut(&removed.file_id) {
            record.paths = record.paths.checked_sub(1).expect("captured identity has a removal owner");
            if record.paths == 0 { self.records.remove(&removed.file_id); }
        }
        self.epoch = self.epoch.wrapping_add(1);
        true
    }
}

pub(in crate::native_mount::lazy) struct PendingDetachedIdentity<A, O> {
    pub(in crate::native_mount::lazy) file: crate::DetachedFile<A, O>,
    pub(in crate::native_mount::lazy) metadata: FileMetadata,
    pub(in crate::native_mount::lazy) epoch: u64,
    pub(in crate::native_mount::lazy) operation: Option<crate::OperationId>,
    // Declared last: the frozen physical collection hold drops first.
    pub(in crate::native_mount::lazy) _custody: crate::native_mount::NativeMountAllocationCustody,
}

pub(in crate::native_mount::lazy) fn retain_pending_detached_with_work<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    file: crate::DetachedFile<A, O>,
    metadata: FileMetadata,
    epoch: u64,
    operation: Option<crate::OperationId>,
    work: &mut crate::demand::DemandWork,
) -> Result<PendingDetachedIdentity<A, O>, MountSourceError> {
    let bytes = file.logical_owned_bytes();
    match crate::native_mount::NativeMountAllocationCustody::from_admitted(bytes, work) {
        Ok(custody) => Ok(PendingDetachedIdentity { file, metadata, epoch, operation, _custody: custody }),
        Err(error) => {
            drop(file);
            work.release(bytes).map_err(mount_demand_error)?;
            Err(mount_demand_error(error))
        }
    }
}

pub(in crate::native_mount::lazy) struct DetachedIdentity<A, O> {
    pub(in crate::native_mount::lazy) file: Arc<CheckoutDetachedFile<A, O>>,
    pub(in crate::native_mount::lazy) ready: bool,
    pub(in crate::native_mount::lazy) published_epoch: u64,
    pub(in crate::native_mount::lazy) pending: Option<PendingDetachedIdentity<A, O>>,
}

pub(in crate::native_mount::lazy) struct DetachedTable<A, O> {
    pub(in crate::native_mount::lazy) entries: Vec<(FileId, DetachedIdentity<A, O>)>,
    pub(in crate::native_mount::lazy) custody: Option<crate::native_mount::NativeMountAllocationCustody>,
    pub(in crate::native_mount::lazy) bytes: u64,
    pub(in crate::native_mount::lazy) native_allocation: Option<crate::NativeAllocation>,
}

impl<A, O> Default for DetachedTable<A, O> {
    fn default() -> Self { Self { entries: Vec::new(), custody: None, bytes: 0, native_allocation: None } }
}

impl<A, O> std::ops::Deref for DetachedTable<A, O> {
    type Target = Vec<(FileId, DetachedIdentity<A, O>)>;
    fn deref(&self) -> &Self::Target { &self.entries }
}

impl<A, O> std::ops::DerefMut for DetachedTable<A, O> {
    fn deref_mut(&mut self) -> &mut Self::Target { &mut self.entries }
}

impl<A, O> Drop for DetachedTable<A, O> {
    fn drop(&mut self) {
        drop(std::mem::take(&mut self.entries));
        if let Some(custody) = &self.custody {
            custody.retire(self.bytes).expect("detached vector storage retires only after its actual drop");
        }
    }
}

pub(in crate::native_mount::lazy) type DetachedIdentities<A, O> = Arc<Mutex<DetachedTable<A, O>>>;

pub(in crate::native_mount::lazy) type OpenIdentityHandles = Arc<Mutex<BTreeMap<FileId, Vec<Weak<dyn MountOpenFile>>>>>;

pub(in crate::native_mount::lazy) type DirtyDetachedRecord = (FileId, FileRecord, FileMetadata, u64);

pub(in crate::native_mount::lazy) struct MountSyncInputs {
    pub(in crate::native_mount::lazy) paths: MountSyncBuffer<Vec<String>>,
    pub(in crate::native_mount::lazy) removed_ids: MountSyncBuffer<Vec<FileId>>,
    pub(in crate::native_mount::lazy) dirty: MountSyncBuffer<Vec<DirtyDetachedRecord>>,
}

pub(in crate::native_mount::lazy) struct PreparedMountSync<A, O> {
    pub(in crate::native_mount::lazy) candidate: CheckoutCandidate<A, O>,
    pub(in crate::native_mount::lazy) epoch: u64,
    pub(in crate::native_mount::lazy) stamp: ViewStamp,
    pub(in crate::native_mount::lazy) inputs: MountSyncInputs,
    pub(in crate::native_mount::lazy) records: MountSyncBuffer<Vec<(FileRecord, FileMetadata)>>,
}

pub(in crate::native_mount::lazy) fn detached_entry<A, O>(entries: &[(FileId, DetachedIdentity<A, O>)], id: FileId) -> Option<&DetachedIdentity<A, O>> {
    entries.binary_search_by_key(&id, |entry| entry.0).ok()
        .and_then(|position| entries.get(position)).map(|entry| &entry.1)
}

pub(in crate::native_mount::lazy) fn detached_position<A, O>(
    entries: &[(FileId, DetachedIdentity<A, O>)],
    id: FileId,
    work: &mut crate::demand::DemandWork,
) -> Result<Result<usize, usize>, MountSourceError> {
    let (mut low, mut high) = (0, entries.len());
    while low < high {
        mount_items(work, 1)?;
        let middle = low + (high - low) / 2;
        let entry = entries.get(middle).ok_or(MountSourceError::Stale)?;
        match entry.0.cmp(&id) {
            std::cmp::Ordering::Less => low = middle + 1,
            std::cmp::Ordering::Greater => high = middle,
            std::cmp::Ordering::Equal => return Ok(Ok(middle)),
        }
    }
    Ok(Err(low))
}
