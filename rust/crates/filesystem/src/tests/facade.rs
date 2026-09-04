use super::*;
use crate::GenerationExportManifestError;
use crate::async_storage::poll_ready;
use crate::kernel::{InlineFileData, LogicalName, NameEncoding};
use crate::memory::MemoryObjectStore as TestMemoryObjectStore;
use crate::model::{
    CaseSensitivity, ConcurrencyMode, FilesystemProfile, UnicodePolicy, VolumeLimits,
};
use crate::storage::{AuthorityFailure, AuthorityResult, AuthorityStore};
use crate::storage::{ObjectFailure, ObjectRead, ObjectReadRequest, ObjectResult, ObjectStore};
use std::sync::atomic::{AtomicUsize, Ordering};

fn config() -> VolumeConfig {
    VolumeConfig {
        profile: FilesystemProfile::Portable,
        concurrency: ConcurrencyMode::Optimistic,
        lifecycle: Lifecycle::Ephemeral,
        case_sensitivity: CaseSensitivity::Sensitive,
        unicode: UnicodePolicy::Preserve,
        symbolic_links: true,
        hard_links: true,
        sparse_files: true,
        limits: VolumeLimits::default(),
    }
}

fn serialized_config() -> VolumeConfig {
    VolumeConfig {
        concurrency: ConcurrencyMode::SerializedAuthority,
        ..config()
    }
}

fn exclusive_config() -> VolumeConfig {
    VolumeConfig {
        concurrency: ConcurrencyMode::ExclusiveWriter,
        ..config()
    }
}

fn pinned() -> CheckoutMode {
    CheckoutMode {
        access: AccessMode::ReadOnly,
        consistency: ConsistencyMode::Pinned,
        mutations: MutationMode::None,
    }
}

fn writable_pinned() -> CheckoutMode {
    CheckoutMode {
        access: AccessMode::ReadWrite,
        consistency: ConsistencyMode::Pinned,
        mutations: MutationMode::PrivateOverlay,
    }
}

fn manual() -> CheckoutMode {
    CheckoutMode {
        access: AccessMode::ReadOnly,
        consistency: ConsistencyMode::Manual,
        mutations: MutationMode::None,
    }
}

fn tracking() -> CheckoutMode {
    CheckoutMode {
        access: AccessMode::ReadOnly,
        consistency: ConsistencyMode::TrackingSafe,
        mutations: MutationMode::None,
    }
}

fn writable_manual() -> CheckoutMode {
    CheckoutMode {
        access: AccessMode::ReadWrite,
        consistency: ConsistencyMode::Manual,
        mutations: MutationMode::PrivateOverlay,
    }
}

fn writable_tracking() -> CheckoutMode {
    CheckoutMode {
        access: AccessMode::ReadWrite,
        consistency: ConsistencyMode::TrackingSafe,
        mutations: MutationMode::PrivateOverlay,
    }
}

fn writable_live() -> CheckoutMode {
    CheckoutMode {
        access: AccessMode::ReadWrite,
        consistency: ConsistencyMode::Live,
        mutations: MutationMode::DirectLive,
    }
}

fn path(value: &str) -> Result<NamespacePath, Box<dyn std::error::Error>> {
    Ok(NamespacePath::new(
        vec![LogicalName::new(
            NameEncoding::Utf8,
            value.as_bytes().to_vec(),
            config().limits.maximum_component_bytes,
        )?],
        config().limits,
    )?)
}

fn path_parts(values: &[&str]) -> Result<NamespacePath, Box<dyn std::error::Error>> {
    let components = values
        .iter()
        .map(|value| {
            LogicalName::new(
                NameEncoding::Utf8,
                value.as_bytes().to_vec(),
                config().limits.maximum_component_bytes,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(NamespacePath::new(components, config().limits)?)
}

fn root_path() -> Result<NamespacePath, Box<dyn std::error::Error>> {
    Ok(NamespacePath::new(Vec::new(), config().limits)?)
}

fn helper_record(file_id: FileId, payload: FilePayload, kind: FileKind) -> FileRecord {
    FileRecord {
        file_id,
        kind,
        link_count: 1,
        metadata: ObjectId {
            kind: ObjectKind::Metadata,
            digest: Digest::from_bytes([9; 32]),
        },
        payload,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CheckoutState {
    base_generation_root: ObjectId,
    generation_root: ObjectId,
    base_file_table: ObjectId,
    base_root: GenerationRoot,
    root: GenerationRoot,
    authority_head: Option<Head>,
    pending_operations: Vec<Mutation>,
    prepared_merge_parent: Option<ObjectId>,
    dependencies: CheckoutDependencies,
    mode: CheckoutMode,
}

fn checkout_state<A, O>(checkout: &Checkout<A, O>) -> CheckoutState {
    CheckoutState {
        base_generation_root: checkout.base_generation_root,
        generation_root: checkout.generation_root,
        base_file_table: checkout.base_file_table,
        base_root: checkout.base_root.clone(),
        root: checkout.root.clone(),
        authority_head: checkout.authority_head,
        pending_operations: checkout.pending_operations.clone(),
        prepared_merge_parent: checkout.prepared_merge_parent,
        dependencies: checkout.dependencies.clone(),
        mode: checkout.mode,
    }
}

struct FaultControl {
    operation: AtomicUsize,
    fail_at: AtomicUsize,
}

impl FaultControl {
    fn disabled() -> Self {
        Self {
            operation: AtomicUsize::new(0),
            fail_at: AtomicUsize::new(usize::MAX),
        }
    }

    fn arm(&self, fail_at: usize) {
        self.operation.store(0, Ordering::Relaxed);
        self.fail_at.store(fail_at, Ordering::Relaxed);
    }

    fn disable(&self) {
        self.operation.store(0, Ordering::Relaxed);
        self.fail_at.store(usize::MAX, Ordering::Relaxed);
    }

    fn should_fail(&self) -> bool {
        self.operation.fetch_add(1, Ordering::Relaxed) + 1 == self.fail_at.load(Ordering::Relaxed)
    }
}

struct FaultObjectStore {
    inner: crate::memory::MemoryObjectStore,
    control: Arc<FaultControl>,
}

struct FaultAuthorityStore {
    inner: crate::memory::MemoryAuthorityStore,
    control: Arc<FaultControl>,
}

struct PostPutObjectStore {
    inner: Arc<crate::memory::MemoryObjectStore>,
    control: Arc<FaultControl>,
}

struct PostAppendAuthorityStore {
    inner: Arc<crate::memory::MemoryAuthorityStore>,
    control: Arc<FaultControl>,
}

impl FaultAuthorityStore {
    fn failure(read: bool) -> AuthorityFailure {
        AuthorityFailure::new(
            AuthorityStoreError::Corrupt("deterministic test fault".to_owned()),
            WorkCounters {
                authority_records_read: u64::from(read),
                authority_records_appended: u64::from(!read),
                ..WorkCounters::default()
            },
        )
    }
}

impl AsyncAuthorityStore for FaultAuthorityStore {
    async fn create_authority(
        &self,
        authority_id: crate::foundation::AuthorityId,
        genesis_epoch: Epoch,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<CreateAuthorityOutcome> {
        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        if self.control.should_fail() {
            return Err(Self::failure(false));
        }
        AuthorityStore::create_authority(&self.inner, authority_id, genesis_epoch, budget)
    }

    async fn head(
        &self,
        authority_id: crate::foundation::AuthorityId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<Head> {
        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        if self.control.should_fail() {
            return Err(Self::failure(true));
        }
        AuthorityStore::head(&self.inner, authority_id, budget)
    }

    async fn compare_and_append(
        &self,
        authority_id: crate::foundation::AuthorityId,
        epoch: Epoch,
        expected: Head,
        commit: ProposedCommit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<AppendOutcome> {
        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        if self.control.should_fail() {
            return Err(Self::failure(false));
        }
        AuthorityStore::compare_and_append(
            &self.inner,
            authority_id,
            epoch,
            expected,
            commit,
            budget,
        )
    }

    async fn replay(
        &self,
        authority_id: crate::foundation::AuthorityId,
        after: Sequence,
        limit: ReplayLimit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<Vec<DurableCommit>> {
        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        if self.control.should_fail() {
            return Err(Self::failure(true));
        }
        AuthorityStore::replay(&self.inner, authority_id, after, limit, budget)
    }

    async fn fence(
        &self,
        authority_id: crate::foundation::AuthorityId,
        expected: Head,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<FenceOutcome> {
        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        if self.control.should_fail() {
            return Err(Self::failure(false));
        }
        AuthorityStore::fence(&self.inner, authority_id, expected, budget)
    }

    async fn find_operation(
        &self,
        authority_id: crate::foundation::AuthorityId,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<Option<DurableCommit>> {
        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        if self.control.should_fail() {
            return Err(Self::failure(true));
        }
        AuthorityStore::find_operation(&self.inner, authority_id, operation_id, budget)
    }
}

impl FaultObjectStore {
    fn failure(read: bool) -> ObjectFailure {
        ObjectFailure::new(
            ObjectStoreError::Corrupt,
            WorkCounters {
                backend_read_operations: u64::from(read),
                backend_write_operations: u64::from(!read),
                ..WorkCounters::default()
            },
        )
    }
}

impl AsyncObjectStore for FaultObjectStore {
    async fn put(
        &self,
        object_id: ObjectId,
        bytes: Bytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        if self.control.should_fail() {
            return Err(Self::failure(false));
        }
        ObjectStore::put(&self.inner, object_id, bytes, budget)
    }

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<ObjectRead> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        if self.control.should_fail() {
            return Err(Self::failure(true));
        }
        ObjectStore::read(&self.inner, object_id, maximum_bytes, budget)
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<Vec<ObjectRead>> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        if self.control.should_fail() {
            return Err(Self::failure(true));
        }
        ObjectStore::read_many(&self.inner, requests, budget)
    }

    async fn contains(
        &self,
        object_id: ObjectId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<bool> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        if self.control.should_fail() {
            return Err(Self::failure(true));
        }
        ObjectStore::contains(&self.inner, object_id, budget)
    }
}

impl AsyncObjectStore for PostPutObjectStore {
    async fn put(
        &self,
        object_id: ObjectId,
        bytes: Bytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        let receipt = ObjectStore::put(&*self.inner, object_id, bytes, budget)?;
        if self.control.should_fail() {
            return Err(ObjectFailure::new(ObjectStoreError::Corrupt, receipt.work));
        }
        Ok(receipt)
    }

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<ObjectRead> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        ObjectStore::read(&*self.inner, object_id, maximum_bytes, budget)
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<Vec<ObjectRead>> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        ObjectStore::read_many(&*self.inner, requests, budget)
    }

    async fn contains(
        &self,
        object_id: ObjectId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<bool> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        ObjectStore::contains(&*self.inner, object_id, budget)
    }
}

impl AsyncAuthorityStore for PostAppendAuthorityStore {
    async fn create_authority(
        &self,
        authority_id: crate::foundation::AuthorityId,
        genesis_epoch: Epoch,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<CreateAuthorityOutcome> {
        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        AuthorityStore::create_authority(&*self.inner, authority_id, genesis_epoch, budget)
    }

    async fn head(
        &self,
        authority_id: crate::foundation::AuthorityId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<Head> {
        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        AuthorityStore::head(&*self.inner, authority_id, budget)
    }

    async fn compare_and_append(
        &self,
        authority_id: crate::foundation::AuthorityId,
        epoch: Epoch,
        expected: Head,
        commit: ProposedCommit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<AppendOutcome> {
        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        let receipt = AuthorityStore::compare_and_append(
            &*self.inner,
            authority_id,
            epoch,
            expected,
            commit,
            budget,
        )?;
        if matches!(receipt.value, AppendOutcome::Committed(_)) && self.control.should_fail() {
            return Err(AuthorityFailure::new(
                AuthorityStoreError::Indeterminate {
                    operation: "compare-and-append",
                    source: std::io::Error::other("deterministic acknowledgement loss"),
                },
                receipt.work,
            ));
        }
        Ok(receipt)
    }

    async fn replay(
        &self,
        authority_id: crate::foundation::AuthorityId,
        after: Sequence,
        limit: ReplayLimit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<Vec<DurableCommit>> {
        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        AuthorityStore::replay(&*self.inner, authority_id, after, limit, budget)
    }

    async fn fence(
        &self,
        authority_id: crate::foundation::AuthorityId,
        expected: Head,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<FenceOutcome> {
        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        AuthorityStore::fence(&*self.inner, authority_id, expected, budget)
    }

    async fn find_operation(
        &self,
        authority_id: crate::foundation::AuthorityId,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<Option<DurableCommit>> {
        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        AuthorityStore::find_operation(&*self.inner, authority_id, operation_id, budget)
    }
}

#[test]
#[allow(clippy::too_many_lines)]
fn facade_helper_state_machines_are_total_and_preserve_typed_failures()
-> Result<(), Box<dyn std::error::Error>> {
    let file_id = FileId::from_bytes([1; 16]);
    let inline = helper_record(
        file_id,
        FilePayload::InlineRegular(InlineFileData::new(b"abcd")?),
        FileKind::Regular,
    );
    assert_eq!(regular_range_regions(None, 0, 1), [None, None]);
    assert_eq!(
        regular_range_regions(Some(inline), 1, 2),
        [
            Some(DependencyRegion::ContentRange {
                file_id,
                offset: 1,
                length: 2,
            }),
            None,
        ]
    );
    assert_eq!(
        regular_range_regions(Some(inline), 3, 4),
        [
            Some(DependencyRegion::ContentRange {
                file_id,
                offset: 3,
                length: 1,
            }),
            Some(DependencyRegion::FileLength(file_id)),
        ]
    );
    let directory = helper_record(
        file_id,
        FilePayload::Directory {
            entries: ObjectId {
                kind: ObjectKind::TreePage,
                digest: Digest::from_bytes([2; 32]),
            },
        },
        FileKind::Directory,
    );
    assert_eq!(
        regular_range_regions(Some(directory), 0, 1),
        [Some(DependencyRegion::FileRecord(file_id)), None]
    );

    assert!(
        validate_planned_file_range(
            4,
            ByteRange {
                offset: 1,
                length: 3
            },
            WorkCounters::default(),
        )
        .is_ok()
    );
    for range in [
        ByteRange {
            offset: 4,
            length: 1,
        },
        ByteRange {
            offset: u64::MAX,
            length: 1,
        },
    ] {
        assert!(matches!(
            validate_planned_file_range(4, range, WorkCounters::default()),
            Err(OperationFailure {
                error: FsError::FileRead(FileRangeReadError::InvalidRange),
                ..
            })
        ));
    }

    assert!(validate_volume_capabilities(config(), EmbeddedCapabilities::MEMORY).is_ok());
    let durable = VolumeConfig {
        lifecycle: Lifecycle::Durable,
        ..config()
    };
    assert!(matches!(
        validate_volume_capabilities(durable, EmbeddedCapabilities::MEMORY),
        Err(OperationFailure {
            error: FsError::UnsupportedDurability,
            ..
        })
    ));
    for admitted in [
        VolumeConfig {
            case_sensitivity: CaseSensitivity::ProfileFolded,
            ..config()
        },
        VolumeConfig {
            unicode: UnicodePolicy::RequireNfc,
            ..config()
        },
    ] {
        // `ProfileFolded`/`RequireNfc` are real, admitted policies (see the
        // `profile_folded_*`/`require_nfc_*` tests below for the folding and
        // normalization behavior itself); volume creation must not reject
        // them.
        assert!(validate_volume_capabilities(admitted, EmbeddedCapabilities::MEMORY).is_ok());
    }
    for mode in [
        pinned(),
        manual(),
        tracking(),
        writable_pinned(),
        writable_manual(),
        writable_tracking(),
    ] {
        assert!(validate_checkout(mode, config()).is_ok());
    }
    assert!(validate_checkout(writable_live(), serialized_config()).is_ok());
    assert!(matches!(
        validate_checkout(writable_live(), config()),
        Err(OperationFailure {
            error: FsError::LiveRequiresSerializedAuthority,
            ..
        })
    ));
    assert!(matches!(
        validate_checkout(
            CheckoutMode {
                access: AccessMode::ReadOnly,
                consistency: ConsistencyMode::Pinned,
                mutations: MutationMode::PrivateOverlay,
            },
            config(),
        ),
        Err(OperationFailure { .. })
    ));

    let replacement = helper_record(
        file_id,
        FilePayload::InlineRegular(InlineFileData::new(b"changed")?),
        FileKind::Regular,
    );
    assert!(matches!(
        file_table_mutation(file_id, None, Some(inline)),
        Some(FileTableMutation::Insert(_))
    ));
    assert!(matches!(
        file_table_mutation(file_id, Some(inline), None),
        Some(FileTableMutation::Remove { .. })
    ));
    assert!(matches!(
        file_table_mutation(file_id, Some(inline), Some(replacement)),
        Some(FileTableMutation::Replace { .. })
    ));
    assert_eq!(file_table_mutation(file_id, None, None), None);
    assert_eq!(directory_entries(None), None);
    assert_eq!(directory_entries(Some(inline)), None);
    assert!(directory_entries(Some(directory)).is_some());

    let logical_name = LogicalName::new(NameEncoding::Utf8, b"name".to_vec(), 255)?;
    let entry = crate::kernel::TreeEntry {
        name: logical_name.clone(),
        file_id,
        kind: FileKind::Regular,
    };
    assert!(matches!(
        tree_mutation(logical_name.clone(), None, Some(entry.clone())),
        Some(TreeMutation::Insert(_))
    ));
    assert!(matches!(
        tree_mutation(logical_name.clone(), Some(entry.clone()), None),
        Some(TreeMutation::Remove { .. })
    ));
    assert!(matches!(
        tree_mutation(logical_name.clone(), Some(entry.clone()), Some(entry)),
        Some(TreeMutation::Replace { .. })
    ));
    assert_eq!(tree_mutation(logical_name, None, None), None);

    assert_eq!(resolve_three(&1, &2, &2), Some(2));
    assert_eq!(resolve_three(&1, &2, &1), Some(2));
    assert_eq!(resolve_three(&1, &1, &2), Some(2));
    assert_eq!(resolve_three(&1, &2, &3), None);
    assert_eq!(
        merge_file_fields(inline, replacement, inline),
        Some(replacement)
    );
    let mut ours_conflicting = replacement;
    ours_conflicting.kind = FileKind::SymbolicLink;
    let mut theirs_conflicting = replacement;
    theirs_conflicting.kind = FileKind::Directory;
    assert_eq!(
        merge_file_fields(inline, ours_conflicting, theirs_conflicting),
        None
    );

    let prior = WorkCounters {
        bytes_copied: 2,
        peak_allocation_bytes: 3,
        ..WorkCounters::default()
    };
    let nested = WorkCounters {
        bytes_hashed: 4,
        peak_allocation_bytes: 5,
        ..WorkCounters::default()
    };
    let merged = merge_simultaneous_work(prior, nested, 7, WorkBudget::UNBOUNDED)?;
    assert_eq!(merged.bytes_copied, 2);
    assert_eq!(merged.bytes_hashed, 4);
    assert_eq!(merged.peak_allocation_bytes, 12);
    let failure = merge_simultaneous_failure(prior, nested, 7, FsError::NotFound);
    assert!(matches!(failure.error, FsError::NotFound));
    assert_eq!(failure.work.peak_allocation_bytes, 12);
    let overflow = merge_simultaneous_failure(
        prior,
        WorkCounters {
            peak_allocation_bytes: u64::MAX,
            ..WorkCounters::default()
        },
        1,
        FsError::NotFound,
    );
    assert!(matches!(overflow.error, FsError::Work(WorkError::Overflow)));
    Ok(())
}

#[test]
fn known_mutation_preconditions_capture_without_backend_rereads()
-> Result<(), Box<dyn std::error::Error>> {
    let store = TestMemoryObjectStore::default();
    let file_id = FileId::from_bytes([91; 16]);
    let record = helper_record(
        file_id,
        FilePayload::InlineRegular(InlineFileData::new(b"direct evidence")?),
        FileKind::Regular,
    );
    let captured = poll_ready(capture_known_dependencies_async(
        &store,
        config(),
        vec![
            (Some(record), DependencyRegion::Metadata(file_id)),
            (Some(record), DependencyRegion::FileLength(file_id)),
            (
                Some(record),
                DependencyRegion::ContentRange {
                    file_id,
                    offset: 0,
                    length: 6,
                },
            ),
        ],
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .ok_or("known dependency capture blocked")??;
    assert_eq!(captured.value.len(), 3);
    assert_eq!(captured.work.backend_read_operations, 0);
    assert!(captured.work.bytes_hashed > 6);

    let mismatch = poll_ready(capture_known_dependencies_async(
        &store,
        config(),
        vec![(
            Some(record),
            DependencyRegion::Metadata(FileId::from_bytes([92; 16])),
        )],
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .ok_or("mismatched dependency capture blocked")?
    .err()
    .ok_or("mismatched record identity unexpectedly succeeded")?;
    assert!(matches!(
        mismatch.error,
        FsError::Probe(AuthenticatedProbeError::RecordIdentityMismatch)
    ));
    assert_eq!(*mismatch.work, WorkCounters::default());
    Ok(())
}

#[test]
fn high_level_file_and_directory_operations_share_the_sparse_kernel()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("create blocked")??
        .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("checkout blocked")??
    .value;
    let original = Bytes::from(vec![b'a'; 100]);
    poll_ready(checkout.create_file(
        path("file")?,
        original,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("create file blocked")??;
    poll_ready(checkout.create_directory(path("directory")?, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("create directory blocked")??;
    poll_ready(checkout.write_file(
        path("file")?,
        40,
        Bytes::from_static(b"changed"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("write blocked")??;
    let read = poll_ready(checkout.read_file_range(
        &path("file")?,
        ByteRange {
            offset: 38,
            length: 12,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("read blocked")??;
    assert_eq!(&read.value.bytes[..], b"aachangedaaa");
    let page = poll_ready(checkout.list_directory(
        &root_path()?,
        None,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("list blocked")??;
    assert_eq!(page.value.entries.len(), 2);
    assert!(!page.value.has_more);
    assert!(checkout.has_pending_mutations());
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn every_object_backend_cut_preserves_facade_atomicity_and_retry()
-> Result<(), Box<dyn std::error::Error>> {
    let control = Arc::new(FaultControl::disabled());
    let fs = Fs::new(
        crate::memory::MemoryAuthorityStore::default(),
        FaultObjectStore {
            inner: crate::memory::MemoryObjectStore::default(),
            control: Arc::clone(&control),
        },
        EmbeddedCapabilities::MEMORY,
    );
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("fault fixture create blocked")??
        .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("fault fixture checkout blocked")??
    .value;

    macro_rules! through_every_object_cut {
        ($operation:expr) => {{
            let original_state = checkout_state(&checkout);
            let mut success = None;
            for cut in 1..=256 {
                control.arm(cut);
                let outcome =
                    poll_ready($operation).ok_or("fault-injected facade future blocked")?;
                match outcome {
                    Ok(receipt) => {
                        success = Some(receipt);
                        break;
                    }
                    Err(failure) => {
                        assert!(
                            failure.work.backend_read_operations
                                + failure.work.backend_write_operations
                                > 0
                        );
                        assert_eq!(checkout_state(&checkout), original_state);
                    }
                }
            }
            control.disable();
            success.ok_or_else(|| {
                format!(
                    "facade operation never passed cut sweep: {}",
                    stringify!($operation)
                )
            })?
        }};
    }

    let large = path("fault-large")?;
    let large_id = through_every_object_cut!(checkout.create_file(
        large.clone(),
        Bytes::from(vec![b'x'; 4_096]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let inline = path("fault-inline")?;
    let inline_id = through_every_object_cut!(checkout.create_file(
        inline.clone(),
        Bytes::from_static(b"inline"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    through_every_object_cut!(checkout.create_directory(
        path("fault-directory")?,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let authored = path("fault-authored-large")?;
    through_every_object_cut!(checkout.apply_authored_transaction(
        vec![AuthoredMutation::CreateFile {
            path: authored.clone(),
            bytes: Bytes::from(vec![b'a'; 4_096]),
            metadata: empty_metadata(),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_object_cut!(checkout.apply_authored_transaction(
        vec![AuthoredMutation::CreateDirectory {
            path: path("fault-authored-directory")?,
            metadata: empty_metadata(),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_object_cut!(checkout.apply_authored_transaction(
        vec![AuthoredMutation::CreateSymbolicLink {
            path: path("fault-authored-link")?,
            target: Bytes::from_static(b"fault-authored-large"),
            metadata: empty_metadata(),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_object_cut!(checkout.apply_authored_transaction(
        vec![AuthoredMutation::Write {
            path: authored.clone(),
            offset: 4_096,
            bytes: Bytes::from_static(b"-fault-swept"),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_object_cut!(checkout.apply_authored_transaction(
        vec![AuthoredMutation::SetMetadata {
            path: authored.clone(),
            metadata: empty_metadata(),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    control.disable();
    let mut staged_source = std::io::Cursor::new(Bytes::from_static(b"staged-fault-content"));
    let staged = poll_ready(checkout.stage_content(
        &mut staged_source,
        64,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("fault fixture content staging blocked")??
    .value;
    through_every_object_cut!(checkout.apply_authored_transaction(
        vec![AuthoredMutation::CreateFileFromContent {
            path: path("fault-authored-content")?,
            content: staged,
            metadata: empty_metadata(),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));

    let range = ByteRange {
        offset: 1,
        length: 64,
    };
    let path_read = through_every_object_cut!(checkout.read_file_range(
        &large,
        range,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(path_read.value.bytes, Bytes::from(vec![b'x'; 64]));
    let identity_read = through_every_object_cut!(checkout.read_file_range_by_id(
        large_id,
        range,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(identity_read.value.bytes, path_read.value.bytes);
    let path_plan = through_every_object_cut!(checkout.plan_file_extents(
        &large,
        range,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let identity_plan = through_every_object_cut!(checkout.plan_file_extents_by_id(
        large_id,
        range,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(
        path_plan.value.as_ref().map(|plan| &plan.spans),
        identity_plan.value.as_ref().map(|plan| &plan.spans)
    );
    assert_eq!(
        path_plan.value.as_ref().map(|plan| plan.spans.len()),
        Some(1)
    );
    let data_seek = through_every_object_cut!(checkout.seek_file_extent(
        &large,
        1,
        ExtentSeekTarget::Data,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(data_seek.value, Some(1));
    let hole_seek = through_every_object_cut!(checkout.seek_file_extent_by_id(
        large_id,
        1,
        ExtentSeekTarget::Hole,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(hole_seek.value, Some(4_096));
    let path_metadata = through_every_object_cut!(checkout.read_metadata(
        &large,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let identity_metadata = through_every_object_cut!(checkout.read_metadata_by_id(
        large_id,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(identity_metadata.value, path_metadata.value);
    let identity_record = through_every_object_cut!(checkout.read_file_record_by_id(
        large_id,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(identity_record.value.file_id, large_id);
    let batch = through_every_object_cut!(checkout.lookup_batch_no_follow(
        &[large.clone(), inline.clone(), path("fault-missing")?],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(batch.value.entries.len(), 3);
    assert!(batch.value.entries[0].record.is_some());
    assert!(batch.value.entries[1].record.is_some());
    assert!(batch.value.entries[2].record.is_none());
    let directory_names = through_every_object_cut!(checkout.list_directory(
        &root_path()?,
        None,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(directory_names.value.entries.len(), 7);
    let directory = through_every_object_cut!(checkout.list_directory_records(
        &root_path()?,
        None,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(directory.value.entries.len(), 7);
    assert!(!directory.value.has_more);
    assert!(
        directory
            .value
            .entries
            .iter()
            .any(|entry| entry.record.file_id == large_id)
    );

    let attribute = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.fault-cut".to_vec(),
        config().limits.maximum_component_bytes,
    )?;
    through_every_object_cut!(checkout.write_named_attribute(
        large.clone(),
        attribute.clone(),
        Bytes::from_static(b"fault-checked-value"),
        NamedAttributeWriteMode::Upsert,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let attribute_value = through_every_object_cut!(checkout.read_named_attribute(
        &large,
        &attribute,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(
        attribute_value.value,
        Some(Bytes::from_static(b"fault-checked-value"))
    );
    let attributes = through_every_object_cut!(checkout.list_named_attributes(
        &large,
        None,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(attributes.value.entries.len(), 1);
    assert_eq!(attributes.value.entries[0].name, attribute);
    assert!(!attributes.value.has_more);
    let identity_attribute = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.identity-fault-cut".to_vec(),
        config().limits.maximum_component_bytes,
    )?;
    through_every_object_cut!(checkout.write_named_attribute_by_id(
        large_id,
        identity_attribute.clone(),
        Bytes::from_static(b"identity-fault-checked-value"),
        NamedAttributeWriteMode::Upsert,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(
        through_every_object_cut!(checkout.read_named_attribute_by_id(
            large_id,
            &identity_attribute,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value,
        Some(Bytes::from_static(b"identity-fault-checked-value"))
    );
    let identity_attributes = through_every_object_cut!(checkout.list_named_attributes_by_id(
        large_id,
        None,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(identity_attributes.value.entries.len(), 2);
    through_every_object_cut!(checkout.remove_named_attribute_by_id(
        large_id,
        identity_attribute.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_object_cut!(checkout.remove_named_attribute(
        large.clone(),
        attribute.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_object_cut!(checkout.write_file_by_id(
        large_id,
        128,
        Bytes::from_static(b"atomic-retry"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_object_cut!(checkout.clone_file_range_by_id(
        large_id,
        0,
        inline_id,
        0,
        4,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let cloned = through_every_object_cut!(checkout.read_file_range_by_id(
        inline_id,
        ByteRange {
            offset: 0,
            length: 6,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(cloned.value.bytes, Bytes::from_static(b"xxxxne"));
    let mut changed_metadata = empty_metadata();
    changed_metadata.modified_ns = MetadataField::Value(101);
    through_every_object_cut!(checkout.set_metadata_by_id(
        large_id,
        changed_metadata,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    changed_metadata.modified_ns = MetadataField::Value(102);
    through_every_object_cut!(checkout.set_attributes_by_id(
        large_id,
        changed_metadata,
        Some(4_224),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_object_cut!(checkout.zero_file_range_by_id(
        large_id,
        ByteRange {
            offset: 4_096,
            length: 32,
        },
        true,
        true,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_object_cut!(checkout.preallocate_file_by_id(
        large_id,
        ByteRange {
            offset: 4_160,
            length: 32,
        },
        true,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_object_cut!(checkout.resize_file_by_id(
        large_id,
        4_192,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    changed_metadata.modified_ns = MetadataField::Value(103);
    through_every_object_cut!(checkout.set_metadata(
        large.clone(),
        changed_metadata,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    changed_metadata.modified_ns = MetadataField::Value(104);
    through_every_object_cut!(checkout.set_attributes(
        large.clone(),
        changed_metadata,
        Some(4_224),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_object_cut!(checkout.zero_file_range(
        large.clone(),
        ByteRange {
            offset: 4_096,
            length: 16,
        },
        false,
        false,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_object_cut!(checkout.preallocate_file(
        large.clone(),
        ByteRange {
            offset: 4_176,
            length: 16,
        },
        true,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_object_cut!(checkout.resize_file(
        large.clone(),
        4_192,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_object_cut!(checkout.clone_file_range(
        FileCloneRequest {
            source: large.clone(),
            source_offset: 0,
            destination: inline.clone(),
            destination_offset: 1,
            length: 3,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let mut detached = through_every_object_cut!(checkout.detach_regular_file(
        &large,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;

    macro_rules! through_every_detached_cut {
        ($operation:expr) => {{
            let original_record = detached.record;
            let mut success = None;
            for cut in 1..=256 {
                control.arm(cut);
                let outcome =
                    poll_ready($operation).ok_or("fault-injected detached future blocked")?;
                match outcome {
                    Ok(receipt) => {
                        success = Some(receipt);
                        break;
                    }
                    Err(failure) => {
                        assert!(
                            failure.work.backend_read_operations
                                + failure.work.backend_write_operations
                                > 0
                        );
                        assert_eq!(detached.record, original_record);
                    }
                }
            }
            control.disable();
            success.ok_or_else(|| {
                format!(
                    "detached operation never passed cut sweep: {}",
                    stringify!($operation)
                )
            })?
        }};
    }

    let detached_read = through_every_detached_cut!(detached.read_range(
        range,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(detached_read.value.bytes, Bytes::from(vec![b'x'; 64]));
    let detached_seek = through_every_detached_cut!(detached.seek(
        1,
        ExtentSeekTarget::Data,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(detached_seek.value, Some(1));
    let mut detached_metadata =
        through_every_detached_cut!(detached.read_metadata(WorkBudget::UNBOUNDED, &cancellation,))
            .value;
    detached_metadata.modified_ns = MetadataField::Value(42);
    through_every_detached_cut!(detached.set_metadata(
        detached_metadata,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_detached_cut!(detached.write_named_attribute(
        attribute.clone(),
        Bytes::from_static(b"detached fault value"),
        NamedAttributeWriteMode::Upsert,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let detached_attribute = through_every_detached_cut!(detached.read_named_attribute(
        &attribute,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(
        detached_attribute.value,
        Some(Bytes::from_static(b"detached fault value"))
    );
    let detached_attributes = through_every_detached_cut!(detached.list_named_attributes(
        None,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(detached_attributes.value.entries.len(), 1);
    assert_eq!(detached_attributes.value.entries[0].name, attribute);
    through_every_detached_cut!(detached.write_range(
        256,
        Bytes::from_static(b"detached atomic retry"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_detached_cut!(detached.zero_range(
        ByteRange {
            offset: 512,
            length: 32,
        },
        true,
        false,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_detached_cut!(detached.preallocate(
        ByteRange {
            offset: 768,
            length: 32,
        },
        false,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_detached_cut!(detached.resize(2_048, WorkBudget::UNBOUNDED, &cancellation,));
    through_every_detached_cut!(detached.remove_named_attribute(
        attribute.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_detached_cut!(detached.set_attributes(
        detached_metadata,
        Some(1_024),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn every_authority_backend_cut_preserves_creation_checkout_and_commit_retry()
-> Result<(), Box<dyn std::error::Error>> {
    let control = Arc::new(FaultControl::disabled());
    let fs = Fs::new(
        FaultAuthorityStore {
            inner: crate::memory::MemoryAuthorityStore::default(),
            control: Arc::clone(&control),
        },
        crate::memory::MemoryObjectStore::default(),
        EmbeddedCapabilities::MEMORY,
    );
    let cancellation = CancellationToken::new();

    macro_rules! through_every_authority_cut {
        ($operation:expr) => {{
            let mut success = None;
            for cut in 1..=64 {
                control.arm(cut);
                let outcome =
                    poll_ready($operation).ok_or("fault-injected authority future blocked")?;
                match outcome {
                    Ok(receipt) => {
                        success = Some(receipt);
                        break;
                    }
                    Err(failure) => assert!(
                        failure
                            .work
                            .authority_records_read
                            .saturating_add(failure.work.authority_records_appended)
                            > 0
                    ),
                }
            }
            control.disable();
            success.ok_or("authority operation never passed the complete cut-point sweep")?
        }};
    }

    let volume_id = VolumeId::from_bytes([31; 16]);
    let volume = through_every_authority_cut!(fs.create_volume_with_id(
        volume_id,
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    assert_eq!(volume.id(), volume_id);
    let reopened = through_every_authority_cut!(fs.open_volume(
        volume_id,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let mut checkout = through_every_authority_cut!(reopened.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    control.disable();
    poll_ready(checkout.create_file(
        path("authority-cut-file")?,
        Bytes::from_static(b"authority cut candidate"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("authority cut candidate creation blocked")??;
    let candidate = checkout.generation_id();
    assert!(checkout.has_pending_mutations());
    let commit_operation = OperationId::from_bytes([33; 16]);
    let mut committed = None;
    for cut in 1..=64 {
        control.arm(cut);
        let outcome =
            poll_ready(checkout.commit(commit_operation, WorkBudget::UNBOUNDED, &cancellation))
                .ok_or("fault-injected commit blocked")?;
        match outcome {
            Ok(receipt) => {
                committed = Some(receipt);
                break;
            }
            Err(failure) => {
                assert!(
                    failure
                        .work
                        .authority_records_read
                        .saturating_add(failure.work.authority_records_appended)
                        > 0
                );
                assert_eq!(checkout.generation_id(), candidate);
                assert!(checkout.has_pending_mutations());
            }
        }
    }
    control.disable();
    let committed = committed.ok_or("commit never passed the complete authority cut sweep")?;
    let committed_generation = match committed.value {
        CheckoutCommitOutcome::Committed { generation_id, .. }
        | CheckoutCommitOutcome::AlreadyCommitted { generation_id, .. } => generation_id,
        CheckoutCommitOutcome::Conflict { .. }
        | CheckoutCommitOutcome::Fenced { .. }
        | CheckoutCommitOutcome::IdempotencyConflict { .. } => {
            return Err("exact retry returned a semantic authority conflict".into());
        }
    };
    assert_eq!(checkout.generation_id(), committed_generation);
    assert!(!checkout.has_pending_mutations());

    let live_id = VolumeId::from_bytes([32; 16]);
    let live_volume = through_every_authority_cut!(fs.create_volume_with_id(
        live_id,
        serialized_config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let mut live = through_every_authority_cut!(live_volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let live_before = live.generation_id();
    control.disable();
    poll_ready(live.create_file(
        path("live-authority-cut")?,
        Bytes::from_static(b"live"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("live candidate mutation blocked")??;
    assert!(live.has_pending_mutations());
    let live_operation = OperationId::from_bytes([34; 16]);
    let mut published = None;
    for cut in 1..=64 {
        control.arm(cut);
        let outcome = poll_ready(live.resume_live(
            live_operation,
            3,
            16,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("fault-injected live publication blocked")?;
        match outcome {
            Ok(receipt) => {
                published = Some(receipt);
                break;
            }
            Err(failure) => {
                assert!(
                    failure
                        .work
                        .authority_records_read
                        .saturating_add(failure.work.authority_records_appended)
                        > 0
                );
                assert_eq!(live.generation_id(), live_before);
                assert!(live.has_pending_mutations());
            }
        }
    }
    control.disable();
    let published = published.ok_or("live publication never passed the authority cut sweep")?;
    assert!(matches!(
        published.value,
        LiveMutationOutcome::Committed { .. } | LiveMutationOutcome::AlreadyCommitted { .. }
    ));
    assert_ne!(live.generation_id(), live_before);
    assert!(!live.has_pending_mutations());
    Ok(())
}

#[test]
fn every_post_put_creation_fault_retries_from_a_complete_authenticated_closure()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let mut reached_clean_completion = false;
    for cut in 1..=32 {
        let control = Arc::new(FaultControl::disabled());
        control.arm(cut);
        let objects = Arc::new(crate::memory::MemoryObjectStore::default());
        let fs = Fs::new(
            crate::memory::MemoryAuthorityStore::default(),
            PostPutObjectStore {
                inner: Arc::clone(&objects),
                control: Arc::clone(&control),
            },
            EmbeddedCapabilities::MEMORY,
        );
        let volume_id = VolumeId::from_bytes(
            [u8::try_from(cut).map_err(|_| "post-put cut does not fit identity")?; 16],
        );
        let first = poll_ready(fs.create_volume_with_id(
            volume_id,
            config(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("post-put creation future blocked")?;
        match first {
            Ok(created) => {
                assert_eq!(created.value.id(), volume_id);
                reached_clean_completion = true;
                break;
            }
            Err(failure) => {
                assert!(
                    matches!(failure.error, FsError::Object(ObjectStoreError::Corrupt)),
                    "unexpected creation failure: {:#?}",
                    failure.error
                );
                assert_eq!(
                    failure.work.backend_write_operations,
                    u64::try_from(cut).map_err(|_| "post-put cut does not fit work counter")?
                );
                control.disable();
                let retried = poll_ready(fs.create_volume_with_id(
                    volume_id,
                    config(),
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                ))
                .ok_or("post-put creation retry blocked")??;
                assert_eq!(retried.value.id(), volume_id);
                let reopened =
                    poll_ready(fs.open_volume(volume_id, WorkBudget::UNBOUNDED, &cancellation))
                        .ok_or("post-put volume reopen blocked")??;
                let checkout = poll_ready(reopened.value.checkout(
                    GenerationSelector::Head,
                    pinned(),
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                ))
                .ok_or("post-put checkout blocked")??;
                assert_eq!(checkout.value.volume_id(), volume_id);
                assert!(!checkout.value.has_pending_mutations());
            }
        }
    }
    assert!(reached_clean_completion);
    Ok(())
}

#[test]
fn every_pre_object_fault_preserves_creation_and_checkout_retry()
-> Result<(), Box<dyn std::error::Error>> {
    let control = Arc::new(FaultControl::disabled());
    let fs = Fs::new(
        crate::memory::MemoryAuthorityStore::default(),
        FaultObjectStore {
            inner: crate::memory::MemoryObjectStore::default(),
            control: Arc::clone(&control),
        },
        EmbeddedCapabilities::MEMORY,
    );
    let cancellation = CancellationToken::new();
    let volume_id = VolumeId::from_bytes([50; 16]);
    let mut volume = None;
    for cut in 1..=64 {
        control.arm(cut);
        match poll_ready(fs.create_volume_with_id(
            volume_id,
            config(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("pre-object creation future blocked")?
        {
            Ok(receipt) => {
                volume = Some(receipt.value);
                break;
            }
            Err(failure) => {
                assert!(
                    matches!(
                        failure.error,
                        FsError::Object(ObjectStoreError::Corrupt)
                            | FsError::Closure(ClosureError::Storage(ObjectStoreError::Corrupt))
                    ),
                    "unexpected creation failure: {:#?}",
                    failure.error
                );
                assert!(failure.work.backend_write_operations > 0);
            }
        }
    }
    control.disable();
    let volume = volume.ok_or("creation never passed the complete object cut sweep")?;
    assert_eq!(volume.id(), volume_id);

    let mut checkout = None;
    for cut in 1..=64 {
        control.arm(cut);
        match poll_ready(volume.checkout(
            GenerationSelector::Head,
            pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("pre-object checkout future blocked")?
        {
            Ok(receipt) => {
                checkout = Some(receipt.value);
                break;
            }
            Err(failure) => {
                assert!(
                    matches!(failure.error, FsError::Object(ObjectStoreError::Corrupt)),
                    "unexpected checkout failure: {:#?}",
                    failure.error
                );
                assert!(failure.work.backend_read_operations > 0);
            }
        }
    }
    control.disable();
    let checkout = checkout.ok_or("checkout never passed the complete object cut sweep")?;
    assert_eq!(checkout.volume_id(), volume_id);
    assert!(!checkout.has_pending_mutations());
    Ok(())
}

#[test]
fn every_object_cut_preserves_sparse_rebase_retry_and_candidate_state()
-> Result<(), Box<dyn std::error::Error>> {
    let control = Arc::new(FaultControl::disabled());
    let fs = Fs::new(
        crate::memory::MemoryAuthorityStore::default(),
        FaultObjectStore {
            inner: crate::memory::MemoryObjectStore::default(),
            control: Arc::clone(&control),
        },
        EmbeddedCapabilities::MEMORY,
    );
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("rebase fixture creation blocked")??
        .value;
    let mut writer = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("writer checkout blocked")??
    .value;
    let mut rebasing = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("tracking checkout blocked")??
    .value;
    poll_ready(rebasing.create_file(
        path("local")?,
        Bytes::from_static(b"local sparse candidate"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("local mutation blocked")??;
    poll_ready(writer.create_file(
        path("remote")?,
        Bytes::from_static(b"remote sparse head"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("remote mutation blocked")??;
    poll_ready(writer.commit(
        OperationId::from_bytes([227; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("remote commit blocked")??;

    let original = checkout_state(&rebasing);
    let mut completed = None;
    for cut in 1..=512 {
        control.arm(cut);
        match poll_ready(rebasing.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("fault-injected rebase blocked")?
        {
            Ok(receipt) => {
                completed = Some(receipt);
                break;
            }
            Err(failure) => {
                assert!(failure.work.backend_read_operations > 0);
                assert_eq!(checkout_state(&rebasing), original);
            }
        }
    }
    control.disable();
    let completed = completed.ok_or("rebase never passed the complete object cut sweep")?;
    assert!(matches!(completed.value, RebaseDecision::Safe { .. }));
    assert!(rebasing.has_pending_mutations());
    assert!(
        poll_ready(rebasing.lookup_no_follow(
            &path("local")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("local lookup blocked")??
        .value
        .record
        .is_some()
    );
    assert!(
        poll_ready(rebasing.lookup_no_follow(
            &path("remote")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("remote lookup blocked")??
        .value
        .record
        .is_some()
    );
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn every_object_cut_preserves_diff_export_and_merge_preparation()
-> Result<(), Box<dyn std::error::Error>> {
    let control = Arc::new(FaultControl::disabled());
    let fs = Fs::new(
        crate::memory::MemoryAuthorityStore::default(),
        FaultObjectStore {
            inner: crate::memory::MemoryObjectStore::default(),
            control: Arc::clone(&control),
        },
        EmbeddedCapabilities::MEMORY,
    );
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("merge fixture creation blocked")??
        .value;
    let base = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("base checkout blocked")??
    .value
    .generation_id();
    let mut ours = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("ours checkout blocked")??
    .value;
    let mut theirs = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("theirs checkout blocked")??
    .value;
    poll_ready(ours.create_file(
        path("ours")?,
        Bytes::from(vec![b'o'; 4_096]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("ours mutation blocked")??;
    poll_ready(theirs.create_file(
        path("theirs")?,
        Bytes::from(vec![b't'; 4_096]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("theirs mutation blocked")??;
    let ours_generation = poll_ready(ours.checkpoint(WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("ours checkpoint blocked")??
        .value;
    let theirs_generation = poll_ready(theirs.checkpoint(WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("theirs checkpoint blocked")??
        .value;
    poll_ready(theirs.commit(
        OperationId::from_bytes([228; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("theirs commit blocked")??;

    macro_rules! sweep_read_only {
        ($operation:expr) => {{
            let original = checkout_state(&ours);
            let mut completed = None;
            for cut in 1..=4_096 {
                control.arm(cut);
                match poll_ready($operation).ok_or("fault-injected operation blocked")? {
                    Ok(receipt) => {
                        completed = Some(receipt);
                        break;
                    }
                    Err(failure) => {
                        assert!(failure.work.backend_read_operations > 0);
                        assert_eq!(checkout_state(&ours), original);
                    }
                }
            }
            control.disable();
            completed.ok_or_else(|| {
                format!(
                    "operation never passed object cut sweep: {}",
                    stringify!($operation)
                )
            })?
        }};
    }

    let diff = sweep_read_only!(volume.diff_generations(
        base,
        ours_generation,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(diff.value.files.len() + diff.value.bindings.len(), 3);
    let manifest = sweep_read_only!(ours.export_manifest(WorkBudget::UNBOUNDED, &cancellation));
    assert_eq!(manifest.value.volume_id, volume.id());
    assert_eq!(
        manifest.value.generation_root,
        ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: ours_generation.digest(),
        }
    );
    let prepared = sweep_read_only!(ours.prepare_merge(
        theirs_generation,
        8,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert!(matches!(prepared.value, MergePreparation::Prepared { .. }));
    assert!(ours.has_pending_mutations());
    for expected in ["ours", "theirs"] {
        assert!(
            poll_ready(ours.lookup_no_follow(
                &path(expected)?,
                WorkBudget::UNBOUNDED,
                &cancellation,
            ))
            .ok_or("merged lookup blocked")??
            .value
            .record
            .is_some()
        );
    }
    Ok(())
}

#[test]
fn every_object_cut_preserves_authenticated_publication_retry()
-> Result<(), Box<dyn std::error::Error>> {
    let control = Arc::new(FaultControl::disabled());
    let fs = Fs::new(
        crate::memory::MemoryAuthorityStore::default(),
        FaultObjectStore {
            inner: crate::memory::MemoryObjectStore::default(),
            control: Arc::clone(&control),
        },
        EmbeddedCapabilities::MEMORY,
    );
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("publication fixture creation blocked")??
        .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("publication checkout blocked")??
    .value;
    poll_ready(checkout.create_file(
        path("publish-large")?,
        Bytes::from(vec![b'p'; 16_384]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("publication mutation blocked")??;
    poll_ready(checkout.write_named_attribute(
        path("publish-large")?,
        AttributeName::new(
            crate::kernel::AttributeClass::PosixXattr,
            b"user.publication".to_vec(),
            config().limits.maximum_component_bytes,
        )?,
        Bytes::from(vec![b'a'; 4_096]),
        NamedAttributeWriteMode::Create,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("publication attribute blocked")??;

    let original = checkout_state(&checkout);
    let operation_id = OperationId::from_bytes([229; 16]);
    let mut completed = None;
    for cut in 1..=4_096 {
        control.arm(cut);
        match poll_ready(checkout.commit(operation_id, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("fault-injected publication blocked")?
        {
            Ok(receipt) => {
                completed = Some(receipt);
                break;
            }
            Err(failure) => {
                assert!(
                    failure.work.backend_read_operations + failure.work.backend_write_operations
                        > 0
                );
                assert_eq!(checkout_state(&checkout), original);
            }
        }
    }
    control.disable();
    let completed = completed.ok_or("publication never passed the complete object cut sweep")?;
    assert!(matches!(
        completed.value,
        CheckoutCommitOutcome::Committed { .. }
    ));
    assert!(!checkout.has_pending_mutations());
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn indeterminate_post_append_retry_resolves_one_durable_publication()
-> Result<(), Box<dyn std::error::Error>> {
    let control = Arc::new(FaultControl::disabled());
    let authority = Arc::new(crate::memory::MemoryAuthorityStore::default());
    let fs = Fs::new(
        PostAppendAuthorityStore {
            inner: Arc::clone(&authority),
            control: Arc::clone(&control),
        },
        crate::memory::MemoryObjectStore::default(),
        EmbeddedCapabilities::MEMORY,
    );
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("indeterminate fixture create blocked")??
        .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("indeterminate fixture checkout blocked")??
    .value;
    poll_ready(checkout.create_file(
        path("indeterminate-file")?,
        Bytes::from_static(b"durable exactly once"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("indeterminate fixture mutation blocked")??;
    let before = checkout_state(&checkout);
    let operation_id = OperationId::from_bytes([49; 16]);
    control.arm(1);
    let failure = poll_ready(checkout.commit(operation_id, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("indeterminate commit blocked")?
        .err()
        .ok_or("post-append acknowledgement loss unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        FsError::Publication(PublicationError::Authority(
            AuthorityStoreError::Indeterminate { .. }
        ))
    ));
    assert_eq!(checkout_state(&checkout), before);
    assert!(
        AuthorityStore::find_operation(
            &*authority,
            volume_authority_id(volume.id()),
            operation_id,
            WorkBudget::UNBOUNDED,
        )?
        .value
        .is_some()
    );

    control.disable();
    let resolved = poll_ready(checkout.commit(operation_id, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("indeterminate commit retry blocked")??;
    assert!(matches!(
        resolved.value,
        CheckoutCommitOutcome::AlreadyCommitted { .. }
    ));
    assert!(!checkout.has_pending_mutations());
    let history = AuthorityStore::replay(
        &*authority,
        volume_authority_id(volume.id()),
        Sequence::GENESIS,
        ReplayLimit {
            records: 8,
            payload_bytes: 64 * 1024,
        },
        WorkBudget::UNBOUNDED,
    )?
    .value;
    assert_eq!(
        history
            .iter()
            .filter(|commit| commit.operation_id == operation_id)
            .count(),
        1
    );
    let mut reopened = poll_ready(volume.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("indeterminate committed checkout blocked")??;
    let bytes = poll_ready(reopened.value.read_file_range(
        &path("indeterminate-file")?,
        ByteRange {
            offset: 0,
            length: 20,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("indeterminate committed read blocked")??;
    assert_eq!(
        bytes.value.bytes,
        Bytes::from_static(b"durable exactly once")
    );
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn detached_open_file_remains_sparse_and_mutable_after_last_binding_removal()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("create blocked")??
        .value;
    let file = path("open-file")?;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("checkout blocked")??
    .value;
    poll_ready(checkout.create_file(
        file.clone(),
        Bytes::from(vec![b'a'; 128]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("create file blocked")??;
    let detached =
        poll_ready(checkout.detach_regular_file(&file, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("detach blocked")??;
    let mut detached = detached.value;
    let file_id = detached.file_id();
    assert_eq!(
        poll_ready(detached.seek(
            0,
            ExtentSeekTarget::Data,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("detached data seek blocked")??
        .value,
        Some(0)
    );
    assert_eq!(
        poll_ready(detached.seek(
            0,
            ExtentSeekTarget::Hole,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("detached hole seek blocked")??
        .value,
        Some(128)
    );
    for (offset, target, expected) in [
        (128, ExtentSeekTarget::Data, None),
        (128, ExtentSeekTarget::Hole, Some(128)),
        (129, ExtentSeekTarget::Data, None),
        (129, ExtentSeekTarget::Hole, None),
    ] {
        assert_eq!(
            poll_ready(detached.seek(offset, target, WorkBudget::UNBOUNDED, &cancellation,))
                .ok_or("detached boundary seek blocked")??
                .value,
            expected
        );
    }

    let attribute = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.detached".to_vec(),
        128,
    )?;
    assert!(
        poll_ready(
            detached.read_named_attribute(&attribute, WorkBudget::UNBOUNDED, &cancellation,)
        )
        .ok_or("detached attribute read blocked")??
        .value
        .is_none()
    );
    assert!(
        poll_ready(detached.list_named_attributes(None, 8, WorkBudget::UNBOUNDED, &cancellation,))
            .ok_or("detached attribute listing blocked")??
            .value
            .entries
            .is_empty()
    );
    let absent_remove = poll_ready(detached.remove_named_attribute(
        attribute.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("attribute-free detached removal blocked")?
    .err()
    .ok_or("attribute-free detached removal unexpectedly succeeded")?;
    assert!(matches!(absent_remove.error, FsError::NotFound));
    let absent_replace = poll_ready(detached.write_named_attribute(
        attribute.clone(),
        Bytes::from_static(b"replacement"),
        NamedAttributeWriteMode::Replace,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("absent detached replacement blocked")?
    .err()
    .ok_or("absent detached replacement unexpectedly succeeded")?;
    assert!(matches!(absent_replace.error, FsError::CreationRejected));
    poll_ready(detached.write_named_attribute(
        attribute.clone(),
        Bytes::from_static(b"first"),
        NamedAttributeWriteMode::Create,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("detached attribute creation blocked")??;
    assert_eq!(
        poll_ready(
            detached.read_named_attribute(&attribute, WorkBudget::UNBOUNDED, &cancellation,)
        )
        .ok_or("detached attribute reread blocked")??
        .value
        .as_deref(),
        Some(b"first".as_slice())
    );
    assert_eq!(
        poll_ready(detached.list_named_attributes(None, 8, WorkBudget::UNBOUNDED, &cancellation,))
            .ok_or("detached populated attribute listing blocked")??
            .value
            .entries
            .len(),
        1
    );
    assert!(
        poll_ready(detached.write_named_attribute(
            attribute.clone(),
            Bytes::from_static(b"duplicate"),
            NamedAttributeWriteMode::Create,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("detached duplicate attribute write blocked")?
        .is_err()
    );
    poll_ready(detached.write_named_attribute(
        attribute.clone(),
        Bytes::from_static(b"second"),
        NamedAttributeWriteMode::Replace,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("detached attribute replacement blocked")??;

    let mut metadata = poll_ready(detached.read_metadata(WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("detached metadata read blocked")??
        .value;
    metadata.posix_mode = MetadataField::Value(0o100_640);
    poll_ready(detached.set_metadata(metadata, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("detached metadata write blocked")??;
    metadata.posix_mode = MetadataField::Value(0o100_600);
    poll_ready(detached.set_attributes(metadata, None, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("detached metadata-only attributes write blocked")??;
    assert_eq!(detached.logical_bytes(), 128);
    poll_ready(detached.set_attributes(metadata, Some(160), WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("detached attributes write blocked")??;
    assert_eq!(detached.logical_bytes(), 160);
    poll_ready(checkout.remove(
        file.clone(),
        Some(file_id),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("remove blocked")??;
    let absent = poll_ready(checkout.lookup_no_follow(&file, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("lookup blocked")??;
    assert!(absent.value.record.is_none());

    poll_ready(detached.write_range(
        0,
        Bytes::from_static(b"open"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("detached write blocked")??;
    poll_ready(detached.write_range(4, Bytes::new(), WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("detached empty write blocked")??;
    poll_ready(detached.preallocate(
        ByteRange {
            offset: 128,
            length: 64,
        },
        false,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("detached preallocation blocked")??;
    poll_ready(detached.zero_range(
        ByteRange {
            offset: 64,
            length: 16,
        },
        false,
        false,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("detached hole blocked")??;
    let read = poll_ready(detached.read_range(
        ByteRange {
            offset: 0,
            length: 80,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("detached read blocked")??;
    assert_eq!(&read.value.bytes[..4], b"open");
    assert_eq!(&read.value.bytes[64..80], &[0; 16]);
    poll_ready(detached.resize(192, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("detached resize blocked")??;
    assert_eq!(detached.logical_bytes(), 192);
    poll_ready(detached.remove_named_attribute(
        attribute.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("detached attribute removal blocked")??;
    assert!(
        poll_ready(detached.remove_named_attribute(
            attribute,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("detached absent attribute removal blocked")?
        .is_err()
    );
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn tracking_reads_capture_only_their_exact_terminal_regions()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([81; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("volume creation blocked")??
    .value;
    let file = path("file")?;
    let mut seed = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("seed checkout blocked")??
    .value;
    poll_ready(seed.create_file(
        file.clone(),
        Bytes::from(vec![b'a'; 128]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("seed create blocked")??;
    poll_ready(seed.commit(
        OperationId::from_bytes([82; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("seed commit blocked")??;

    let mut content_reader = poll_ready(volume.checkout(
        GenerationSelector::Head,
        tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("content reader blocked")??
    .value;
    poll_ready(content_reader.read_file_range(
        &file,
        ByteRange {
            offset: 0,
            length: 8,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("content observation blocked")??;
    let mut metadata_writer = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("metadata writer blocked")??
    .value;
    let mut changed_metadata = empty_metadata();
    changed_metadata.modified_ns = MetadataField::Value(7);
    poll_ready(metadata_writer.set_metadata(
        file.clone(),
        changed_metadata,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("metadata mutation blocked")??;
    poll_ready(metadata_writer.commit(
        OperationId::from_bytes([83; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("metadata commit blocked")??;
    let content_rebase =
        poll_ready(content_reader.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("content rebase blocked")??;
    assert!(matches!(content_rebase.value, RebaseDecision::Safe { .. }));

    let mut metadata_reader = poll_ready(volume.checkout(
        GenerationSelector::Head,
        tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("metadata reader blocked")??
    .value;
    poll_ready(metadata_reader.read_metadata(&file, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("metadata observation blocked")??;
    let mut content_writer = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("content writer blocked")??
    .value;
    poll_ready(content_writer.write_file(
        file.clone(),
        64,
        Bytes::from_static(b"changed"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("content mutation blocked")??;
    poll_ready(content_writer.commit(
        OperationId::from_bytes([84; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("content commit blocked")??;
    let metadata_rebase =
        poll_ready(metadata_reader.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("metadata rebase blocked")??;
    assert!(matches!(metadata_rebase.value, RebaseDecision::Safe { .. }));

    let mut generic_reader = poll_ready(volume.checkout(
        GenerationSelector::Head,
        tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("generic reader blocked")??
    .value;
    poll_ready(generic_reader.lookup_no_follow(&file, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("generic observation blocked")??;
    let mut final_writer = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("final writer blocked")??
    .value;
    poll_ready(final_writer.write_file(
        file,
        96,
        Bytes::from_static(b"different"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("final mutation blocked")??;
    poll_ready(final_writer.commit(
        OperationId::from_bytes([85; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("final commit blocked")??;
    let generic_rebase =
        poll_ready(generic_reader.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("generic rebase blocked")??;
    assert!(matches!(
        generic_rebase.value,
        RebaseDecision::Conflicted { ref conflicts, .. }
            if conflicts.iter().any(|conflict| matches!(
                conflict.region,
                DependencyRegion::FileRecord(_)
            ))
    ));
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn sparse_seek_dependencies_track_base_semantics_and_exact_observed_boundaries()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([101; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("volume creation blocked")??
    .value;
    let file = path("sparse")?;
    let mut seed = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("seed checkout blocked")??
    .value;
    poll_ready(seed.create_file(
        file.clone(),
        Bytes::from(vec![b'a'; 256]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("seed create blocked")??;
    poll_ready(seed.commit(
        OperationId::from_bytes([102; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("seed commit blocked")??;

    let mut local = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("local checkout blocked")??
    .value;
    poll_ready(local.zero_file_range(
        file.clone(),
        ByteRange {
            offset: 128,
            length: 16,
        },
        false,
        false,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("local hole blocked")??;
    let local_observation = poll_ready(local.seek_file_extent(
        &file,
        128,
        ExtentSeekTarget::Hole,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("local seek blocked")??;
    assert_eq!(local_observation.value, Some(128));

    let mut disjoint = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("disjoint checkout blocked")??
    .value;
    poll_ready(disjoint.zero_file_range(
        file.clone(),
        ByteRange {
            offset: 0,
            length: 64,
        },
        false,
        false,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("disjoint hole blocked")??;
    poll_ready(disjoint.commit(
        OperationId::from_bytes([103; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("disjoint commit blocked")??;
    let safe = poll_ready(local.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("local rebase blocked")??;
    assert!(matches!(safe.value, RebaseDecision::Safe { .. }));
    let retained = poll_ready(local.seek_file_extent(
        &file,
        128,
        ExtentSeekTarget::Hole,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("retained local seek blocked")??;
    assert_eq!(retained.value, Some(128));

    let mut reader = poll_ready(volume.checkout(
        GenerationSelector::Head,
        tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("reader checkout blocked")??
    .value;
    let observation = poll_ready(reader.seek_file_extent(
        &file,
        128,
        ExtentSeekTarget::Hole,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("reader seek blocked")??;
    assert_eq!(observation.value, Some(256));

    let mut conflicting = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("conflicting checkout blocked")??
    .value;
    poll_ready(conflicting.zero_file_range(
        file,
        ByteRange {
            offset: 192,
            length: 16,
        },
        false,
        false,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("conflicting hole blocked")??;
    poll_ready(conflicting.commit(
        OperationId::from_bytes([104; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("conflicting commit blocked")??;
    let conflict = poll_ready(reader.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("conflicting rebase blocked")??;
    assert!(matches!(
        conflict.value,
        RebaseDecision::Conflicted { ref conflicts, .. }
            if conflicts.iter().any(|conflict| matches!(
                conflict.region,
                DependencyRegion::SparseSeek {
                    offset: 128,
                    target: ExtentSeekTarget::Hole,
                    ..
                }
            ))
    ));

    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn sparse_local_writes_rebase_across_disjoint_remote_ranges_and_conflict_on_overlap()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([86; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("volume creation blocked")??
    .value;
    let file = path("file")?;
    let mut seed = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("seed checkout blocked")??
    .value;
    poll_ready(seed.create_file(
        file.clone(),
        Bytes::from(vec![b'a'; 128]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("seed create blocked")??;
    poll_ready(seed.commit(
        OperationId::from_bytes([87; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("seed commit blocked")??;

    let mut local = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("local checkout blocked")??
    .value;
    let mut remote = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("remote checkout blocked")??
    .value;
    poll_ready(local.write_file(
        file.clone(),
        0,
        Bytes::from_static(b"LOCAL"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("local write blocked")??;
    poll_ready(remote.write_file(
        file.clone(),
        64,
        Bytes::from_static(b"REMOTE"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("remote write blocked")??;
    poll_ready(remote.commit(
        OperationId::from_bytes([88; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("remote commit blocked")??;
    let safe = poll_ready(local.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("disjoint rebase blocked")??;
    assert!(matches!(safe.value, RebaseDecision::Safe { .. }));
    let local_bytes = poll_ready(local.read_file_range(
        &file,
        ByteRange {
            offset: 0,
            length: 70,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("rebased read blocked")??;
    assert_eq!(&local_bytes.value.bytes[..5], b"LOCAL");
    assert_eq!(&local_bytes.value.bytes[64..70], b"REMOTE");

    let mut overlapping_local = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("overlapping local checkout blocked")??
    .value;
    let mut overlapping_remote = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("overlapping remote checkout blocked")??
    .value;
    poll_ready(overlapping_local.write_file(
        file.clone(),
        10,
        Bytes::from_static(b"local"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("overlapping local write blocked")??;
    poll_ready(overlapping_remote.write_file(
        file,
        12,
        Bytes::from_static(b"remote"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("overlapping remote write blocked")??;
    poll_ready(overlapping_remote.commit(
        OperationId::from_bytes([89; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("overlapping remote commit blocked")??;
    let conflict =
        poll_ready(overlapping_local.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("overlapping rebase blocked")??;
    assert!(matches!(
        conflict.value,
        RebaseDecision::Conflicted { ref conflicts, .. }
            if conflicts.iter().any(|conflict| matches!(
                conflict.region,
                DependencyRegion::ContentRange { .. }
            ))
    ));
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn identity_writes_rebase_without_namespace_lookup_and_conflict_by_exact_range()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([111; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("volume creation blocked")??
    .value;
    let file = path("identity")?;
    let mut seed = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("seed checkout blocked")??
    .value;
    let file_id = poll_ready(seed.create_file(
        file.clone(),
        Bytes::from(vec![b'a'; 128]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("seed create blocked")??
    .value;
    poll_ready(seed.commit(
        OperationId::from_bytes([112; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("seed commit blocked")??;

    let mut local = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("local checkout blocked")??
    .value;
    let mut remote = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("remote checkout blocked")??
    .value;
    poll_ready(local.write_file_by_id(
        file_id,
        0,
        Bytes::from_static(b"LOCAL"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("identity write blocked")??;
    poll_ready(remote.write_file(
        file.clone(),
        64,
        Bytes::from_static(b"REMOTE"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("remote write blocked")??;
    poll_ready(remote.commit(
        OperationId::from_bytes([113; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("remote commit blocked")??;
    let safe = poll_ready(local.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("identity rebase blocked")??;
    assert!(matches!(safe.value, RebaseDecision::Safe { .. }));
    let bytes = poll_ready(local.read_file_range_by_id(
        file_id,
        ByteRange {
            offset: 0,
            length: 70,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("identity read blocked")??;
    assert_eq!(&bytes.value.bytes[..5], b"LOCAL");
    assert_eq!(&bytes.value.bytes[64..70], b"REMOTE");
    let plan = poll_ready(local.plan_file_extents_by_id(
        file_id,
        ByteRange {
            offset: 0,
            length: 70,
        },
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("identity extent plan blocked")??;
    assert!(plan.value.is_some());

    let mut overlapping_local = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("overlapping local checkout blocked")??
    .value;
    let mut overlapping_remote = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("overlapping remote checkout blocked")??
    .value;
    poll_ready(overlapping_local.write_file_by_id(
        file_id,
        10,
        Bytes::from_static(b"local"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("overlapping identity write blocked")??;
    poll_ready(overlapping_remote.write_file(
        file,
        12,
        Bytes::from_static(b"remote"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("overlapping remote write blocked")??;
    poll_ready(overlapping_remote.commit(
        OperationId::from_bytes([114; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("overlapping remote commit blocked")??;
    let conflict =
        poll_ready(overlapping_local.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("overlapping identity rebase blocked")??;
    assert!(matches!(
        conflict.value,
        RebaseDecision::Conflicted { ref conflicts, .. }
            if conflicts.iter().any(|conflict| matches!(
                conflict.region,
                DependencyRegion::ContentRange { file_id: actual, .. } if actual == file_id
            ))
    ));

    let mut extending = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("extending identity checkout blocked")??
    .value;
    poll_ready(extending.write_file_by_id(
        file_id,
        128,
        Bytes::from_static(b"TAIL"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("identity append blocked")??;
    poll_ready(extending.clone_file_range_by_id(
        file_id,
        0,
        file_id,
        140,
        4,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("identity extending clone blocked")??;
    let extended = poll_ready(extending.read_file_range_by_id(
        file_id,
        ByteRange {
            offset: 128,
            length: 16,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("identity extended read blocked")??;
    assert_eq!(&extended.value.bytes[..4], b"TAIL");
    assert_eq!(&extended.value.bytes[4..12], &[0; 8]);
    assert_eq!(&extended.value.bytes[12..], b"aaaa");
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn inline_extent_plans_validate_ranges_and_capture_identity_dependencies()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([115; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("volume creation blocked")??
    .value;
    let file = path("inline")?;
    let mut seed = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("seed checkout blocked")??
    .value;
    let file_id = poll_ready(seed.create_file(
        file.clone(),
        Bytes::from_static(b"abc"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("seed create blocked")??
    .value;
    poll_ready(seed.commit(
        OperationId::from_bytes([116; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("seed commit blocked")??;

    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("tracking checkout blocked")??
    .value;
    let plan = poll_ready(checkout.plan_file_extents_by_id(
        file_id,
        ByteRange {
            offset: 0,
            length: 2,
        },
        1,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("inline identity plan blocked")??;
    assert!(plan.value.is_none());
    assert_eq!(checkout.dependencies.len(), 1);

    let invalid = poll_ready(checkout.plan_file_extents_by_id(
        file_id,
        ByteRange {
            offset: 2,
            length: 2,
        },
        1,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("invalid inline identity plan blocked")?
    .err()
    .ok_or("inline identity plan accepted a range beyond EOF")?;
    assert!(matches!(
        invalid.error,
        FsError::FileRead(FileRangeReadError::InvalidRange)
    ));
    assert_eq!(checkout.dependencies.len(), 1);

    let path_plan = poll_ready(checkout.plan_file_extents(
        &file,
        ByteRange {
            offset: 1,
            length: 2,
        },
        1,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("inline path plan blocked")??;
    assert!(path_plan.value.is_none());
    let invalid_path = poll_ready(checkout.plan_file_extents(
        &file,
        ByteRange {
            offset: 3,
            length: 1,
        },
        1,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("invalid inline path plan blocked")?
    .err()
    .ok_or("inline path plan accepted a range beyond EOF")?;
    assert!(matches!(
        invalid_path.error,
        FsError::FileRead(FileRangeReadError::InvalidRange)
    ));
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn direct_live_mutations_retry_only_across_exactly_safe_regions()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let optimistic = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([90; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("optimistic volume blocked")??
    .value;
    let rejected = poll_ready(optimistic.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("live rejection blocked")?
    .err()
    .ok_or("optimistic volume admitted direct live")?;
    assert!(matches!(
        rejected.error,
        FsError::LiveRequiresSerializedAuthority
    ));

    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([91; 16]),
        serialized_config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("serialized volume blocked")??
    .value;
    let file = path("file")?;
    let mut seed = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("seed checkout blocked")??
    .value;
    poll_ready(seed.create_file(
        file.clone(),
        Bytes::from(vec![b'a'; 128]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("seed create blocked")??;
    poll_ready(seed.commit(
        OperationId::from_bytes([92; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("seed commit blocked")??;

    let mut first = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("first live checkout blocked")??
    .value;
    let mut second = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("second live checkout blocked")??
    .value;
    let first_blob = poll_ready(first.stage_blob(
        Bytes::from_static(b"FIRST"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("first blob blocked")??;
    let second_blob = poll_ready(second.stage_blob(
        Bytes::from_static(b"SECOND"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("second blob blocked")??;
    let first_outcome = poll_ready(first.mutate_live(
        vec![Mutation::Write {
            path: file.clone(),
            offset: 0,
            length: first_blob.value.logical_bytes,
            content: first_blob.value.root,
            content_offset: 0,
        }],
        OperationId::from_bytes([93; 16]),
        2,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("first live mutation blocked")??;
    assert!(matches!(
        first_outcome.value,
        LiveMutationOutcome::Committed { .. }
    ));
    let second_outcome = poll_ready(second.mutate_live(
        vec![Mutation::Write {
            path: file.clone(),
            offset: 64,
            length: second_blob.value.logical_bytes,
            content: second_blob.value.root,
            content_offset: 0,
        }],
        OperationId::from_bytes([94; 16]),
        2,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("second live mutation blocked")??;
    assert!(matches!(
        second_outcome.value,
        LiveMutationOutcome::Committed { .. }
    ));

    let mut overlapping = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("overlap checkout blocked")??
    .value;
    let mut racing = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("racing checkout blocked")??
    .value;
    let overlap_blob = poll_ready(overlapping.stage_blob(
        Bytes::from_static(b"OVERLAP"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("overlap blob blocked")??;
    let racing_blob = poll_ready(racing.stage_blob(
        Bytes::from_static(b"RACING"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("racing blob blocked")??;
    poll_ready(racing.mutate_live(
        vec![Mutation::Write {
            path: file.clone(),
            offset: 10,
            length: racing_blob.value.logical_bytes,
            content: racing_blob.value.root,
            content_offset: 0,
        }],
        OperationId::from_bytes([95; 16]),
        2,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("racing mutation blocked")??;
    let conflict = poll_ready(overlapping.mutate_live(
        vec![Mutation::Write {
            path: file,
            offset: 12,
            length: overlap_blob.value.logical_bytes,
            content: overlap_blob.value.root,
            content_offset: 0,
        }],
        OperationId::from_bytes([96; 16]),
        2,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("overlap mutation blocked")??;
    assert!(matches!(
        conflict.value,
        LiveMutationOutcome::Conflicted { ref conflicts, .. }
            if conflicts.iter().any(|conflict| matches!(
                conflict.region,
                DependencyRegion::ContentRange { .. }
            ))
    ));
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn explicit_live_refresh_advances_only_across_unobserved_regions()
-> Result<(), Box<dyn std::error::Error>> {
    macro_rules! ready {
        ($future:expr) => {
            poll_ready($future).ok_or("filesystem future blocked")??
        };
    }

    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = ready!(fs.create_volume_with_id(
        VolumeId::from_bytes([97; 16]),
        serialized_config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let file = path("refresh")?;
    let mut seed = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    ready!(seed.create_file(
        file.clone(),
        Bytes::from(vec![b'a'; 128]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    ready!(seed.commit(
        OperationId::from_bytes([98; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));

    let mut live = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    assert_eq!(
        ready!(live.read_file_range(
            &file,
            ByteRange {
                offset: 0,
                length: 4,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value
        .bytes
        .as_ref(),
        b"aaaa"
    );

    let mut disjoint = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    ready!(disjoint.write_file(
        file.clone(),
        64,
        Bytes::from_static(b"SAFE"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let disjoint_generation = match ready!(disjoint.commit(
        OperationId::from_bytes([99; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value
    {
        CheckoutCommitOutcome::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected disjoint commit: {outcome:?}").into()),
    };
    assert_eq!(
        ready!(live.refresh_live(WorkBudget::UNBOUNDED, &cancellation)).value,
        disjoint_generation
    );
    assert_eq!(live.generation_id(), disjoint_generation);

    assert_eq!(
        ready!(live.read_file_range(
            &file,
            ByteRange {
                offset: 0,
                length: 4,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value
        .bytes
        .as_ref(),
        b"aaaa"
    );
    let mut overlapping = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    ready!(overlapping.write_file(
        file,
        0,
        Bytes::from_static(b"RACE"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    ready!(overlapping.commit(
        OperationId::from_bytes([100; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let before_conflict = live.generation_id();
    let conflict = poll_ready(live.refresh_live(WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("conflicting live refresh blocked")?
        .err()
        .ok_or("conflicting live refresh unexpectedly advanced")?;
    assert!(matches!(
        conflict.error,
        FsError::LiveConflict { ref conflicts, .. }
            if conflicts.iter().any(|conflict| matches!(
                conflict.region,
                DependencyRegion::ContentRange { .. }
            ))
    ));
    assert_eq!(live.generation_id(), before_conflict);
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn direct_live_resume_is_idempotent_retry_bounded_and_epoch_fenced()
-> Result<(), Box<dyn std::error::Error>> {
    macro_rules! ready {
        ($future:expr) => {
            poll_ready($future).ok_or("filesystem future blocked")??
        };
    }

    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume_id = VolumeId::from_bytes([117; 16]);
    let volume = ready!(fs.create_volume_with_id(
        volume_id,
        serialized_config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;

    let mut winner = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let mut retrying = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    ready!(winner.create_file(
        path("winner")?,
        Bytes::from_static(b"winner"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    ready!(winner.resume_live(
        OperationId::from_bytes([118; 16]),
        1,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));

    ready!(retrying.create_file(
        path("retrying")?,
        Bytes::from_static(b"retrying"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let retry_operation = OperationId::from_bytes([119; 16]);
    let limited =
        ready!(retrying.resume_live(retry_operation, 1, 8, WorkBudget::UNBOUNDED, &cancellation,));
    assert!(matches!(
        limited.value,
        LiveMutationOutcome::RetryLimit { .. }
    ));
    assert!(retrying.has_pending_mutations());
    let mutation_while_unresolved = poll_ready(retrying.create_file(
        path("must-remain-fenced")?,
        Bytes::from_static(b"blocked"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("unresolved live mutation guard blocked")?
    .err()
    .ok_or("unresolved live mutation accepted another mutation")?;
    assert!(matches!(
        mutation_while_unresolved.error,
        FsError::PendingLiveMutation
    ));
    let mismatched_resume = poll_ready(retrying.resume_live(
        OperationId::from_bytes([120; 16]),
        2,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("mismatched live resume blocked")?
    .err()
    .ok_or("mismatched live resume unexpectedly succeeded")?;
    assert!(matches!(
        mismatched_resume.error,
        FsError::PendingLiveMutation
    ));
    let rebase_while_unresolved =
        poll_ready(retrying.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("unresolved live rebase guard blocked")?
            .err()
            .ok_or("unresolved live operation accepted public rebase")?;
    assert!(matches!(
        rebase_while_unresolved.error,
        FsError::PendingLiveMutation
    ));
    let resumed =
        ready!(retrying.resume_live(retry_operation, 2, 8, WorkBudget::UNBOUNDED, &cancellation,));
    assert!(matches!(
        resumed.value,
        LiveMutationOutcome::Committed { .. }
    ));
    assert!(!retrying.has_pending_mutations());
    let clean_resume = poll_ready(retrying.resume_live(
        retry_operation,
        1,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("clean live resume blocked")?
    .err()
    .ok_or("clean live resume unexpectedly succeeded")?;
    assert!(matches!(clean_resume.error, FsError::NoPendingMutations));
    assert_eq!(*clean_resume.work, WorkCounters::default());
    for failure in [
        poll_ready(retrying.resume_live(
            retry_operation,
            0,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("zero-limit live resume blocked")?
        .err()
        .ok_or("zero-limit live resume unexpectedly succeeded")?,
        poll_ready(retrying.mutate_live(
            Vec::new(),
            OperationId::from_bytes([124; 16]),
            0,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("zero-limit live mutation blocked")?
        .err()
        .ok_or("zero-limit live mutation unexpectedly succeeded")?,
    ] {
        assert!(matches!(failure.error, FsError::ZeroLiveRetryLimit));
        assert_eq!(*failure.work, WorkCounters::default());
    }

    let mut pending_guard = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    ready!(pending_guard.create_file(
        path("pending-guard")?,
        Bytes::from_static(b"pending"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let pending_failure = poll_ready(pending_guard.mutate_live(
        Vec::new(),
        OperationId::from_bytes([125; 16]),
        1,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("pending live mutation guard blocked")?
    .err()
    .ok_or("second pending live mutation unexpectedly succeeded")?;
    assert!(matches!(
        pending_failure.error,
        FsError::PendingLiveMutation
    ));
    assert_eq!(*pending_failure.work, WorkCounters::default());
    let authored_pending = poll_ready(pending_guard.apply_authored_live(
        Vec::new(),
        OperationId::from_bytes([126; 16]),
        1,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("pending authored-live guard blocked")?
    .err()
    .ok_or("pending authored-live operation unexpectedly succeeded")?;
    assert!(matches!(
        authored_pending.error,
        FsError::PendingLiveMutation
    ));
    assert_eq!(*authored_pending.work, WorkCounters::default());

    let mut first_retry = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let mut exact_retry = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let root = root_path()?;
    let metadata =
        ready!(first_retry.lookup_no_follow(&root, WorkBudget::UNBOUNDED, &cancellation,))
            .value
            .record
            .ok_or("root record absent")?
            .metadata;
    let identical = Mutation::Create {
        path: path("idempotent")?,
        record: FileRecord {
            file_id: FileId::from_bytes([120; 16]),
            kind: FileKind::Regular,
            link_count: 1,
            metadata,
            payload: FilePayload::InlineRegular(InlineFileData::new(b"same")?),
        },
    };
    ready!(first_retry.mutate(
        vec![identical.clone()],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    ready!(exact_retry.mutate(vec![identical], WorkBudget::UNBOUNDED, &cancellation,));
    let idempotent_operation = OperationId::from_bytes([121; 16]);
    let committed = ready!(first_retry.resume_live(
        idempotent_operation,
        1,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert!(matches!(
        committed.value,
        LiveMutationOutcome::Committed { .. }
    ));
    let repeated = ready!(exact_retry.resume_live(
        idempotent_operation,
        1,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert!(matches!(
        repeated.value,
        LiveMutationOutcome::AlreadyCommitted { .. }
    ));

    let mut conflicting_retry = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    ready!(conflicting_retry.create_file(
        path("different-operation")?,
        Bytes::from_static(b"different"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let conflicted = ready!(conflicting_retry.resume_live(
        idempotent_operation,
        1,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert!(matches!(
        conflicted.value,
        LiveMutationOutcome::IdempotencyConflict { .. }
    ));
    assert!(conflicting_retry.has_pending_mutations());

    let mut fenced = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    ready!(fenced.create_file(
        path("fenced")?,
        Bytes::from_static(b"fenced"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let fence_expected = ready!(AsyncAuthorityStore::head(
        &fs.inner.authority,
        volume_authority_id(volume_id),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let fence = ready!(AsyncAuthorityStore::fence(
        &fs.inner.authority,
        volume_authority_id(volume_id),
        fence_expected,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let FenceOutcome::Advanced(fenced_head) = fence.value else {
        return Err("fresh facade fence conflicted".into());
    };
    let fenced_outcome = ready!(fenced.resume_live(
        OperationId::from_bytes([122; 16]),
        1,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert!(matches!(
        fenced_outcome.value,
        LiveMutationOutcome::Fenced { actual_epoch } if actual_epoch == fenced_head.epoch
    ));

    let mut non_live = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_manual(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    assert!(
        poll_ready(non_live.refresh_live(WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("non-live refresh blocked")?
            .is_err()
    );
    assert!(
        poll_ready(non_live.mutate_live(
            Vec::new(),
            OperationId::from_bytes([123; 16]),
            1,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("non-live mutation blocked")?
        .is_err()
    );
    Ok(())
}

#[test]
fn high_level_operations_stage_one_explicit_direct_live_transaction()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([111; 16]),
        serialized_config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("serialized volume blocked")??
    .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("live checkout blocked")??
    .value;
    let file = path("live.txt")?;
    poll_ready(checkout.create_file(
        file.clone(),
        Bytes::from_static(b"initial"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("high-level live create blocked")??;
    poll_ready(checkout.write_file(
        file.clone(),
        7,
        Bytes::from_static(b"-published"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("high-level live write blocked")??;
    assert!(checkout.has_pending_mutations());

    let published = poll_ready(checkout.resume_live(
        OperationId::from_bytes([112; 16]),
        3,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("live publication blocked")??;
    assert!(matches!(
        published.value,
        LiveMutationOutcome::Committed { .. }
    ));
    assert!(!checkout.has_pending_mutations());
    let bytes = poll_ready(checkout.read_file_range(
        &file,
        ByteRange {
            offset: 0,
            length: 17,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("published read blocked")??;
    assert_eq!(bytes.value.bytes.as_ref(), b"initial-published");
    Ok(())
}

#[test]
fn authored_live_transaction_preserves_create_ids_and_publishes_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([117; 16]),
        serialized_config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("serialized volume blocked")??
    .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("live checkout blocked")??
    .value;
    let file = path("authored-live.txt")?;
    let receipt = poll_ready(checkout.apply_authored_live(
        vec![
            AuthoredMutation::CreateFile {
                path: file.clone(),
                bytes: Bytes::from_static(b"initial"),
                metadata: empty_metadata(),
            },
            AuthoredMutation::Write {
                path: file.clone(),
                offset: 7,
                bytes: Bytes::from_static(b"-published"),
            },
        ],
        OperationId::from_bytes([118; 16]),
        3,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("authored live transaction blocked")??;
    assert!(matches!(
        receipt.value.outcome,
        LiveMutationOutcome::Committed { .. }
    ));
    assert_eq!(receipt.value.created_file_ids.len(), 2);
    assert!(receipt.value.created_file_ids[0].is_some());
    assert!(receipt.value.created_file_ids[1].is_none());
    assert!(!checkout.has_pending_mutations());
    let read = poll_ready(checkout.read_file_range(
        &file,
        ByteRange {
            offset: 0,
            length: 17,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("published authored read blocked")??;
    assert_eq!(read.value.bytes.as_ref(), b"initial-published");
    Ok(())
}

#[test]
fn authored_transaction_is_atomic_and_preserves_result_positions()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([113; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("volume creation blocked")??
    .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("checkout blocked")??
    .value;
    let directory = path("workspace")?;
    let file = path("file.txt")?;
    let transaction = poll_ready(checkout.apply_authored_transaction(
        vec![
            AuthoredMutation::CreateDirectory {
                path: directory,
                metadata: empty_metadata(),
            },
            AuthoredMutation::CreateFile {
                path: file.clone(),
                bytes: Bytes::from_static(b"initial"),
                metadata: empty_metadata(),
            },
            AuthoredMutation::Write {
                path: file.clone(),
                offset: 7,
                bytes: Bytes::from_static(b"-atomic"),
            },
        ],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("authored transaction blocked")??;
    assert_eq!(transaction.value.created_file_ids.len(), 3);
    assert!(transaction.value.created_file_ids[0].is_some());
    assert!(transaction.value.created_file_ids[1].is_some());
    assert!(transaction.value.created_file_ids[2].is_none());
    let read = poll_ready(checkout.read_file_range(
        &file,
        ByteRange {
            offset: 0,
            length: 14,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("transaction read blocked")??;
    assert_eq!(read.value.bytes.as_ref(), b"initial-atomic");

    let failed = poll_ready(checkout.apply_authored_transaction(
        vec![
            AuthoredMutation::CreateFile {
                path: path("orphan")?,
                bytes: Bytes::from_static(b"never-visible"),
                metadata: empty_metadata(),
            },
            AuthoredMutation::Remove {
                path: path("absent")?,
                expected_file_id: None,
            },
        ],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("failing transaction blocked")?;
    assert!(failed.is_err());
    let absent = poll_ready(checkout.lookup_no_follow(
        &path("orphan")?,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("atomicity lookup blocked")??;
    assert!(absent.value.record.is_none());
    Ok(())
}

#[test]
fn inverse_private_mutations_restore_the_base_and_clear_replay_state()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([136; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("inverse volume creation blocked")??
    .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("inverse checkout blocked")??
    .value;
    let base = checkout.generation_id();
    let transient = path("transient")?;
    poll_ready(checkout.create_file(
        transient.clone(),
        Bytes::from_static(b"temporary"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("inverse creation blocked")??;
    assert!(checkout.has_pending_mutations());
    poll_ready(checkout.remove(transient, None, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("inverse removal blocked")??;
    assert!(!checkout.has_pending_mutations());
    assert_eq!(checkout.generation_id(), base);
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn authored_transactions_preflight_expansion_noops_and_byte_bounds()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();

    let mut operation_limited = config();
    operation_limited.limits.maximum_mutations_per_batch = 1;
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([124; 16]),
        operation_limited,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("operation-limited volume blocked")??
    .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("operation-limited checkout blocked")??
    .value;
    let missing_noop = poll_ready(checkout.apply_authored_transaction(
        vec![AuthoredMutation::Write {
            path: path("absent")?,
            offset: 0,
            bytes: Bytes::new(),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("authored missing noop blocked")?
    .err()
    .ok_or("authored missing noop succeeded")?;
    assert!(matches!(
        missing_noop.error,
        FsError::Mutation(GenerationMutationError::MissingSource)
    ));
    assert!(missing_noop.work.page_reads > 0);

    let noop_fs = Fs::memory();
    let noop_volume = poll_ready(noop_fs.create_volume_with_id(
        VolumeId::from_bytes([125; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("noop volume creation blocked")??
    .value;
    let mut noop_checkout = poll_ready(noop_volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("noop checkout blocked")??
    .value;
    let ordered = poll_ready(noop_checkout.apply_authored_transaction(
        vec![
            AuthoredMutation::Write {
                path: path("ordered")?,
                offset: 0,
                bytes: Bytes::new(),
            },
            AuthoredMutation::CreateFile {
                path: path("ordered")?,
                bytes: Bytes::new(),
                metadata: empty_metadata(),
            },
        ],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("ordered noop transaction blocked")?
    .err()
    .ok_or("ordered noop bypassed its missing source")?;
    assert!(matches!(
        ordered.error,
        FsError::Mutation(GenerationMutationError::MissingSource)
    ));
    assert!(
        poll_ready(noop_checkout.lookup_no_follow(
            &path("ordered")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("ordered path lookup blocked")??
        .value
        .record
        .is_none()
    );

    let direct_missing = poll_ready(noop_checkout.write_file(
        path("missing")?,
        0,
        Bytes::new(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("direct missing noop blocked")?
    .err()
    .ok_or("direct missing noop succeeded")?;
    assert!(matches!(
        direct_missing.error,
        FsError::Mutation(GenerationMutationError::MissingSource)
    ));

    let created = poll_ready(noop_checkout.apply_authored_transaction(
        vec![AuthoredMutation::CreateFile {
            path: path("present")?,
            bytes: Bytes::new(),
            metadata: empty_metadata(),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("authored fixture creation blocked")??;
    let file_id = created.value.created_file_ids[0].ok_or("created file id missing")?;
    let generation_before_noop = noop_checkout.generation_id();
    let empty = poll_ready(noop_checkout.apply_authored_transaction(
        Vec::new(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("empty authored transaction blocked")??;
    assert!(empty.value.created_file_ids.is_empty());
    assert_eq!(empty.work, WorkCounters::default());
    assert_eq!(noop_checkout.generation_id(), generation_before_noop);
    let noop = poll_ready(noop_checkout.apply_authored_transaction(
        vec![AuthoredMutation::Write {
            path: path("present")?,
            offset: 0,
            bytes: Bytes::new(),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("authored validated noop blocked")??;
    assert_eq!(noop.value.created_file_ids, vec![None]);
    assert!(noop.work.page_reads > 0);
    assert_eq!(noop_checkout.generation_id(), generation_before_noop);
    let mut empty_source = std::io::Cursor::new(Bytes::new());
    let empty_content = poll_ready(noop_checkout.stage_content(
        &mut empty_source,
        1,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("empty content staging blocked")??
    .value;
    assert_eq!(empty_content.logical_bytes(), 0);
    let content_noop = poll_ready(noop_checkout.apply_authored_transaction(
        vec![AuthoredMutation::WriteFromContent {
            path: path("present")?,
            offset: u64::MAX,
            content: empty_content,
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("empty staged-content write blocked")??;
    assert_eq!(content_noop.value.created_file_ids, vec![None]);
    assert!(content_noop.work.page_reads > 0);
    assert_eq!(noop_checkout.generation_id(), generation_before_noop);
    let direct = poll_ready(noop_checkout.write_file(
        path("present")?,
        u64::MAX,
        Bytes::new(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("direct validated noop blocked")??;
    assert!(direct.work.page_reads > 0);
    assert_eq!(noop_checkout.generation_id(), generation_before_noop);
    let identity = poll_ready(noop_checkout.write_file_by_id(
        file_id,
        u64::MAX,
        Bytes::new(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("identity validated noop blocked")??;
    assert!(identity.work.page_reads > 0);
    assert_eq!(noop_checkout.generation_id(), generation_before_noop);

    let expanded = poll_ready(checkout.apply_authored_transaction(
        vec![AuthoredMutation::CreateFile {
            path: path("large")?,
            bytes: Bytes::from(vec![1; crate::kernel::MAXIMUM_INLINE_FILE_BYTES + 1]),
            metadata: empty_metadata(),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("expanded authored transaction blocked")?
    .err()
    .ok_or("expanded authored transaction unexpectedly succeeded")?;
    assert!(matches!(expanded.error, FsError::TooManyPendingMutations));
    assert_eq!(*expanded.work, WorkCounters::default());
    assert!(
        poll_ready(checkout.lookup_no_follow(
            &path("large")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("expanded path lookup blocked")??
        .value
        .record
        .is_none()
    );

    let mut byte_limited = config();
    byte_limited.limits.maximum_read_bytes = 1;
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([125; 16]),
        byte_limited,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("byte-limited volume blocked")??
    .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("byte-limited checkout blocked")??
    .value;
    for mutation in [
        AuthoredMutation::CreateFile {
            path: path("created-file")?,
            bytes: Bytes::from_static(b"xx"),
            metadata: empty_metadata(),
        },
        AuthoredMutation::Write {
            path: path("file")?,
            offset: 0,
            bytes: Bytes::from_static(b"xx"),
        },
        AuthoredMutation::CreateSymbolicLink {
            path: path("link")?,
            target: Bytes::from_static(b"xx"),
            metadata: empty_metadata(),
        },
    ] {
        let failure = poll_ready(checkout.apply_authored_transaction(
            vec![mutation],
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("byte-limited authored transaction blocked")?
        .err()
        .ok_or("byte-limited authored transaction unexpectedly succeeded")?;
        assert!(matches!(
            failure.error,
            FsError::FileRead(FileRangeReadError::InvalidRange)
        ));
        assert_eq!(*failure.work, WorkCounters::default());
    }
    let direct = poll_ready(checkout.create_symbolic_link(
        path("direct-link")?,
        Bytes::from_static(b"xx"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("byte-limited direct symlink blocked")?
    .err()
    .ok_or("byte-limited direct symlink unexpectedly succeeded")?;
    assert!(matches!(
        direct.error,
        FsError::FileRead(FileRangeReadError::InvalidRange)
    ));
    assert_eq!(*direct.work, WorkCounters::default());
    Ok(())
}

#[test]
fn opaque_payload_reads_enforce_the_volume_output_bound() -> Result<(), Box<dyn std::error::Error>>
{
    let fs = Fs::memory_bounded(config().limits.maximum_object_bytes)?;
    let cancellation = CancellationToken::new();
    let mut limited = config();
    limited.profile = FilesystemProfile::Windows;
    limited.limits.maximum_read_bytes = 1;
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([126; 16]),
        limited,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("limited Windows volume blocked")??
    .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("limited Windows checkout blocked")??
    .value;
    let mut source = std::io::Cursor::new(Bytes::from_static(b"xx"));
    let staged =
        poll_ready(checkout.stage_content(&mut source, 2, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("oversized opaque staging blocked")??
            .value;
    let metadata =
        poll_ready(checkout.stage_metadata(empty_metadata(), WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("opaque metadata staging blocked")??
            .value;
    let symbolic = path("symbolic")?;
    let reparse = path("reparse")?;
    poll_ready(checkout.mutate(
        vec![
            Mutation::Create {
                path: symbolic.clone(),
                record: FileRecord {
                    file_id: FileId::from_bytes([127; 16]),
                    kind: FileKind::SymbolicLink,
                    link_count: 1,
                    metadata,
                    payload: FilePayload::SymbolicLink {
                        target_bytes: staged.logical_bytes(),
                        target: staged.root(),
                    },
                },
            },
            Mutation::Create {
                path: reparse.clone(),
                record: FileRecord {
                    file_id: FileId::from_bytes([128; 16]),
                    kind: FileKind::ReparsePoint,
                    link_count: 1,
                    metadata,
                    payload: FilePayload::ReparsePoint {
                        payload_bytes: staged.logical_bytes(),
                        payload: staged.root(),
                    },
                },
            },
        ],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("opaque raw mutation blocked")??;
    for failure in [
        poll_ready(checkout.read_symbolic_link(&symbolic, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("bounded symlink read blocked")?
            .err()
            .ok_or("oversized symlink read unexpectedly succeeded")?,
        poll_ready(checkout.read_reparse_point(&reparse, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("bounded reparse read blocked")?
            .err()
            .ok_or("oversized reparse read unexpectedly succeeded")?,
    ] {
        assert!(matches!(
            failure.error,
            FsError::FileRead(FileRangeReadError::InvalidRange)
        ));
        assert!(failure.work.object_probes > 0);
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
#[test]
fn authored_transactions_cover_every_portable_operation_without_hidden_paths()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([114; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("volume creation blocked")??
    .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("checkout blocked")??
    .value;

    let mut create_source = std::io::Cursor::new(Bytes::from_static(b"streamed"));
    let staged_create = poll_ready(checkout.stage_content(
        &mut create_source,
        64,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("create staging blocked")??
    .value;
    let mut write_source = std::io::Cursor::new(Bytes::from_static(b"TAIL"));
    let staged_write = poll_ready(checkout.stage_content(
        &mut write_source,
        64,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("write staging blocked")??
    .value;
    assert_eq!(staged_create.logical_bytes(), 8);
    assert_eq!(staged_create.root().kind, ObjectKind::Blob);
    assert_eq!(staged_write.logical_bytes(), 4);
    assert_eq!(staged_write.root().kind, ObjectKind::Blob);

    let source = path("source")?;
    let destination = path("destination")?;
    let streamed = path("streamed")?;
    let hard_link = path("hard-link")?;
    let renamed = path("renamed")?;
    let mut replacement_metadata = empty_metadata();
    replacement_metadata.modified_ns = MetadataField::Value(42);
    let large = Bytes::from(vec![0x5a; crate::kernel::MAXIMUM_INLINE_FILE_BYTES + 1]);
    let transaction = poll_ready(checkout.apply_authored_transaction(
        vec![
            AuthoredMutation::CreateDirectory {
                path: path("directory")?,
                metadata: empty_metadata(),
            },
            AuthoredMutation::CreateFile {
                path: source.clone(),
                bytes: Bytes::from_static(b"abcdefgh"),
                metadata: empty_metadata(),
            },
            AuthoredMutation::CreateFile {
                path: destination.clone(),
                bytes: Bytes::from_static(b"................"),
                metadata: empty_metadata(),
            },
            AuthoredMutation::CreateFile {
                path: path("large")?,
                bytes: large.clone(),
                metadata: empty_metadata(),
            },
            AuthoredMutation::CreateFileFromContent {
                path: streamed.clone(),
                content: staged_create,
                metadata: empty_metadata(),
            },
            AuthoredMutation::CreateSymbolicLink {
                path: path("symbolic")?,
                target: Bytes::from_static(b"../source"),
                metadata: empty_metadata(),
            },
            AuthoredMutation::Write {
                path: source.clone(),
                offset: 0,
                bytes: Bytes::new(),
            },
            AuthoredMutation::Write {
                path: source.clone(),
                offset: 6,
                bytes: Bytes::from_static(b"XY"),
            },
            AuthoredMutation::WriteFromContent {
                path: streamed.clone(),
                offset: 8,
                content: staged_write,
            },
            AuthoredMutation::SetMetadata {
                path: source.clone(),
                metadata: replacement_metadata,
            },
            AuthoredMutation::Resize {
                path: destination.clone(),
                logical_bytes: 24,
            },
            AuthoredMutation::ZeroRange {
                path: source.clone(),
                range: ByteRange {
                    offset: 2,
                    length: 2,
                },
                allocated: false,
                extend: false,
            },
            AuthoredMutation::Preallocate {
                path: destination.clone(),
                range: ByteRange {
                    offset: 16,
                    length: 8,
                },
                keep_size: false,
            },
            AuthoredMutation::CloneRange(FileCloneRequest {
                source: source.clone(),
                source_offset: 0,
                destination: destination.clone(),
                destination_offset: 4,
                length: 8,
            }),
            AuthoredMutation::HardLink {
                source: source.clone(),
                destination: hard_link.clone(),
            },
            AuthoredMutation::Rename {
                source: hard_link,
                destination: renamed.clone(),
                replace: false,
            },
            AuthoredMutation::Remove {
                path: renamed,
                expected_file_id: None,
            },
        ],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("complete authored transaction blocked")??;
    assert_eq!(transaction.value.created_file_ids.len(), 17);
    assert_eq!(
        transaction
            .value
            .created_file_ids
            .iter()
            .filter(|created| created.is_some())
            .count(),
        6
    );
    assert_eq!(
        poll_ready(checkout.read_file_range(
            &streamed,
            ByteRange {
                offset: 0,
                length: 12,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("streamed read blocked")??
        .value
        .bytes
        .as_ref(),
        b"streamedTAIL"
    );
    assert_eq!(
        poll_ready(checkout.read_file_range(
            &path("large")?,
            ByteRange {
                offset: 0,
                length: u64::try_from(large.len())?,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("large read blocked")??
        .value
        .bytes,
        large
    );
    assert_eq!(
        poll_ready(checkout.read_symbolic_link(
            &path("symbolic")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("symbolic-link read blocked")??
        .value
        .as_ref(),
        b"../source"
    );
    assert_eq!(
        poll_ready(checkout.read_metadata(&source, WorkBudget::UNBOUNDED, &cancellation,))
            .ok_or("metadata read blocked")??
            .value
            .modified_ns,
        MetadataField::Value(42)
    );
    Ok(())
}

#[allow(clippy::too_many_lines)]
#[test]
fn path_sdk_exposes_every_sparse_operation_with_one_authenticated_state()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    assert!(Fs::memory_bounded(0).is_err());
    let fs = Fs::memory_bounded(config().limits.maximum_object_bytes)?;
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([117; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("volume creation blocked")??
    .value;
    assert_eq!(volume.id(), VolumeId::from_bytes([117; 16]));
    assert_eq!(volume.config(), config());
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("checkout blocked")??
    .value;
    let source = path("source-path")?;
    let destination = path("destination-path")?;
    let source_id = poll_ready(checkout.create_file(
        source.clone(),
        Bytes::from(vec![b's'; 128]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("source creation blocked")??
    .value;
    poll_ready(checkout.create_file(
        destination.clone(),
        Bytes::from(vec![b'd'; 128]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("destination creation blocked")??;
    let mut metadata = empty_metadata();
    metadata.modified_ns = MetadataField::Value(99);
    poll_ready(checkout.set_metadata(
        source.clone(),
        metadata,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("path metadata write blocked")??;
    poll_ready(checkout.resize_file(
        destination.clone(),
        160,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("path resize blocked")??;
    poll_ready(checkout.zero_file_range(
        source.clone(),
        ByteRange {
            offset: 64,
            length: 16,
        },
        false,
        false,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("path zero blocked")??;
    poll_ready(checkout.preallocate_file(
        destination.clone(),
        ByteRange {
            offset: 128,
            length: 32,
        },
        false,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("path preallocation blocked")??;
    poll_ready(checkout.clone_file_range(
        FileCloneRequest {
            source: source.clone(),
            source_offset: 0,
            destination: destination.clone(),
            destination_offset: 32,
            length: 96,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("path clone blocked")??;
    let alias = path("source-alias")?;
    let renamed = path("source-renamed")?;
    poll_ready(checkout.hard_link(
        source.clone(),
        alias.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("path hard link blocked")??;
    poll_ready(checkout.rename(
        alias,
        renamed.clone(),
        false,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("path rename blocked")??;
    let lookup = poll_ready(checkout.lookup_no_follow_with_metadata(
        &renamed,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("metadata lookup blocked")??
    .value
    .ok_or("renamed link is absent")?;
    assert_eq!(lookup.record.file_id, source_id);
    assert_eq!(lookup.metadata.modified_ns, MetadataField::Value(99));
    let batch = poll_ready(checkout.lookup_batch_no_follow(
        &[source.clone(), renamed.clone(), path("absent-path")?],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("path batch lookup blocked")??;
    assert_eq!(batch.value.entries.len(), 3);
    assert_eq!(
        batch.value.entries[0].record.map(|record| record.file_id),
        Some(source_id)
    );
    assert_eq!(
        batch.value.entries[1].record.map(|record| record.file_id),
        Some(source_id)
    );
    assert!(batch.value.entries[2].record.is_none());
    let records = poll_ready(checkout.list_directory_records(
        &root_path()?,
        None,
        16,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("record listing blocked")??;
    assert_eq!(records.value.entries.len(), 3);
    let plan = poll_ready(checkout.plan_file_extents(
        &source,
        ByteRange {
            offset: 0,
            length: 128,
        },
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("path extent plan blocked")??;
    assert!(plan.value.is_some());
    assert_eq!(
        poll_ready(checkout.seek_file_extent(
            &source,
            64,
            ExtentSeekTarget::Hole,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("path hole seek blocked")??
        .value,
        Some(64)
    );
    assert_eq!(
        poll_ready(checkout.read_metadata(&source, WorkBudget::UNBOUNDED, &cancellation,))
            .ok_or("path metadata read blocked")??
            .value,
        metadata
    );
    poll_ready(checkout.remove(
        renamed,
        Some(source_id),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("path removal blocked")??;

    let mut empty = std::io::Cursor::new(Bytes::new());
    assert!(
        poll_ready(checkout.stage_content(&mut empty, 0, WorkBudget::UNBOUNDED, &cancellation,))
            .ok_or("zero staging bound blocked")?
            .is_err()
    );
    assert!(
        poll_ready(checkout.stage_content(
            &mut empty,
            config().limits.maximum_generation_bytes + 1,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("excessive staging bound blocked")?
        .is_err()
    );
    Ok(())
}

#[allow(clippy::too_many_lines)]
#[test]
fn public_query_and_identity_failures_preserve_the_checkout_candidate()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([118; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("volume creation blocked")??
    .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("checkout blocked")??
    .value;
    let file = path("regular")?;
    let directory = path("directory")?;
    let missing = path("missing")?;
    let file_id = poll_ready(checkout.create_file(
        file.clone(),
        Bytes::from_static(b"data"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("file creation blocked")??
    .value;
    let directory_id = poll_ready(checkout.create_directory(
        directory.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("directory creation blocked")??
    .value;
    let empty_directory = poll_ready(checkout.list_directory_records(
        &directory,
        None,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("empty directory record listing blocked")??;
    assert!(empty_directory.value.entries.is_empty());
    assert!(!empty_directory.value.has_more);
    let unknown_id = FileId::from_bytes([250; 16]);
    let attribute = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.missing".to_vec(),
        128,
    )?;

    let oversized_identity_read = poll_ready(checkout.read_file_range_by_id(
        file_id,
        ByteRange {
            offset: 0,
            length: config().limits.maximum_read_bytes + 1,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("oversized identity read blocked")?
    .err()
    .ok_or("oversized identity read unexpectedly succeeded")?;
    assert!(matches!(
        oversized_identity_read.error,
        FsError::FileRead(FileRangeReadError::InvalidRange)
    ));
    assert_eq!(*oversized_identity_read.work, WorkCounters::default());
    let zero_span_plan = poll_ready(checkout.plan_file_extents_by_id(
        file_id,
        ByteRange {
            offset: 0,
            length: 1,
        },
        0,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("zero-span identity plan blocked")?
    .err()
    .ok_or("zero-span identity plan unexpectedly succeeded")?;
    assert!(matches!(
        zero_span_plan.error,
        FsError::FileRead(FileRangeReadError::InvalidRange)
    ));
    assert_eq!(*zero_span_plan.work, WorkCounters::default());
    assert_eq!(
        poll_ready(checkout.seek_file_extent_by_id(
            file_id,
            0,
            ExtentSeekTarget::Data,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("inline identity data seek blocked")??
        .value,
        Some(0)
    );
    assert_eq!(
        poll_ready(checkout.seek_file_extent_by_id(
            file_id,
            0,
            ExtentSeekTarget::Hole,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("inline identity hole seek blocked")??
        .value,
        Some(4)
    );
    for (offset, target, expected) in [
        (4, ExtentSeekTarget::Data, None),
        (4, ExtentSeekTarget::Hole, Some(4)),
        (5, ExtentSeekTarget::Data, None),
        (5, ExtentSeekTarget::Hole, None),
    ] {
        assert_eq!(
            poll_ready(checkout.seek_file_extent_by_id(
                file_id,
                offset,
                target,
                WorkBudget::UNBOUNDED,
                &cancellation,
            ))
            .ok_or("inline identity boundary seek blocked")??
            .value,
            expected
        );
        assert_eq!(
            poll_ready(checkout.seek_file_extent(
                &file,
                offset,
                target,
                WorkBudget::UNBOUNDED,
                &cancellation,
            ))
            .ok_or("inline path boundary seek blocked")??
            .value,
            expected
        );
    }
    let wrong_kind_seek = poll_ready(checkout.seek_file_extent_by_id(
        directory_id,
        0,
        ExtentSeekTarget::Data,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("wrong-kind identity seek blocked")?
    .err()
    .ok_or("wrong-kind identity seek unexpectedly succeeded")?;
    assert!(matches!(
        wrong_kind_seek.error,
        FsError::FileRead(FileRangeReadError::NotRegular)
    ));
    assert!(
        poll_ready(checkout.read_named_attribute_by_id(
            file_id,
            &attribute,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("attribute-free identity read blocked")??
        .value
        .is_none()
    );
    assert!(
        poll_ready(checkout.list_named_attributes_by_id(
            file_id,
            None,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("attribute-free identity listing blocked")??
        .value
        .entries
        .is_empty()
    );
    let missing_remove = poll_ready(checkout.remove_named_attribute_by_id(
        file_id,
        attribute.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("attribute-free identity removal blocked")?
    .err()
    .ok_or("attribute-free identity removal unexpectedly succeeded")?;
    assert!(matches!(missing_remove.error, FsError::NotFound));
    let missing_replace = poll_ready(checkout.write_named_attribute_by_id(
        file_id,
        attribute.clone(),
        Bytes::from_static(b"first"),
        NamedAttributeWriteMode::Replace,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("missing identity attribute replacement blocked")?
    .err()
    .ok_or("missing identity attribute replacement unexpectedly succeeded")?;
    assert!(matches!(missing_replace.error, FsError::CreationRejected));
    poll_ready(checkout.write_named_attribute_by_id(
        file_id,
        attribute.clone(),
        Bytes::from_static(b"first"),
        NamedAttributeWriteMode::Create,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("identity attribute creation blocked")??;
    let duplicate_create = poll_ready(checkout.write_named_attribute_by_id(
        file_id,
        attribute.clone(),
        Bytes::from_static(b"second"),
        NamedAttributeWriteMode::Create,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("duplicate identity attribute creation blocked")?
    .err()
    .ok_or("duplicate identity attribute creation unexpectedly succeeded")?;
    assert!(matches!(duplicate_create.error, FsError::CreationRejected));
    assert_eq!(
        poll_ready(checkout.read_named_attribute_by_id(
            file_id,
            &attribute,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("identity attribute read blocked")??
        .value
        .as_deref(),
        Some(b"first".as_slice())
    );
    poll_ready(checkout.remove_named_attribute_by_id(
        file_id,
        attribute.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("identity attribute removal blocked")??;
    let repeated_remove = poll_ready(checkout.remove_named_attribute_by_id(
        file_id,
        attribute.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("repeated identity attribute removal blocked")?
    .err()
    .ok_or("repeated identity attribute removal unexpectedly succeeded")?;
    assert!(matches!(repeated_remove.error, FsError::NotFound));

    assert!(
        poll_ready(checkout.list_directory(&file, None, 8, WorkBudget::UNBOUNDED, &cancellation,))
            .ok_or("wrong-kind directory listing blocked")?
            .is_err()
    );
    assert!(
        poll_ready(checkout.list_directory_records(
            &file,
            None,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("wrong-kind record listing blocked")?
        .is_err()
    );
    assert!(
        poll_ready(checkout.read_file_range(
            &directory,
            ByteRange {
                offset: 0,
                length: 1,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("wrong-kind path read blocked")?
        .is_err()
    );
    assert!(
        poll_ready(checkout.read_file_range(
            &file,
            ByteRange {
                offset: 5,
                length: 1,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("invalid path range blocked")?
        .is_err()
    );
    assert!(
        poll_ready(checkout.plan_file_extents(
            &directory,
            ByteRange {
                offset: 0,
                length: 1,
            },
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("wrong-kind path plan blocked")?
        .is_err()
    );
    assert!(
        poll_ready(checkout.seek_file_extent(
            &directory,
            0,
            ExtentSeekTarget::Data,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("wrong-kind path seek blocked")?
        .is_err()
    );
    assert!(
        poll_ready(checkout.read_symbolic_link(&file, WorkBudget::UNBOUNDED, &cancellation,))
            .ok_or("wrong-kind symbolic read blocked")?
            .is_err()
    );
    assert!(
        poll_ready(checkout.read_reparse_point(&file, WorkBudget::UNBOUNDED, &cancellation,))
            .ok_or("wrong-kind reparse read blocked")?
            .is_err()
    );
    assert!(
        poll_ready(checkout.read_metadata(&missing, WorkBudget::UNBOUNDED, &cancellation,))
            .ok_or("missing metadata read blocked")?
            .is_err()
    );
    assert!(
        poll_ready(checkout.read_named_attribute(
            &missing,
            &attribute,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("missing attribute read blocked")?
        .is_err()
    );
    assert!(
        poll_ready(checkout.list_named_attributes(
            &missing,
            None,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("missing attribute listing blocked")?
        .is_err()
    );
    assert!(
        poll_ready(checkout.remove_named_attribute(
            missing.clone(),
            attribute.clone(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("missing attribute removal blocked")?
        .is_err()
    );

    assert!(
        poll_ready(checkout.read_file_record_by_id(
            unknown_id,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("unknown identity read blocked")?
        .is_err()
    );
    assert!(
        poll_ready(checkout.read_file_range_by_id(
            directory_id,
            ByteRange {
                offset: 0,
                length: 1,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("wrong-kind identity read blocked")?
        .is_err()
    );
    assert!(
        poll_ready(checkout.read_file_range_by_id(
            file_id,
            ByteRange {
                offset: 5,
                length: 1,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("invalid identity range blocked")?
        .is_err()
    );
    assert!(
        poll_ready(checkout.plan_file_extents_by_id(
            directory_id,
            ByteRange {
                offset: 0,
                length: 1,
            },
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("wrong-kind identity plan blocked")?
        .is_err()
    );
    assert!(
        poll_ready(checkout.seek_file_extent_by_id(
            directory_id,
            0,
            ExtentSeekTarget::Data,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("wrong-kind identity seek blocked")?
        .is_err()
    );
    assert!(
        poll_ready(checkout.read_metadata_by_id(unknown_id, WorkBudget::UNBOUNDED, &cancellation,))
            .ok_or("unknown identity metadata blocked")?
            .is_err()
    );
    assert!(
        poll_ready(checkout.read_named_attribute_by_id(
            unknown_id,
            &attribute,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("unknown identity attribute blocked")?
        .is_err()
    );
    assert!(
        poll_ready(checkout.list_named_attributes_by_id(
            unknown_id,
            None,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("unknown identity attribute listing blocked")?
        .is_err()
    );
    assert!(
        poll_ready(checkout.write_file_by_id(
            directory_id,
            0,
            Bytes::from_static(b"x"),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("wrong-kind identity write blocked")?
        .is_err()
    );
    assert!(
        poll_ready(checkout.remove_named_attribute_by_id(
            unknown_id,
            attribute,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("unknown identity attribute removal blocked")?
        .is_err()
    );
    assert!(checkout.has_pending_mutations());
    assert_eq!(
        poll_ready(checkout.read_file_range(
            &file,
            ByteRange {
                offset: 0,
                length: 4,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("surviving file read blocked")??
        .value
        .bytes
        .as_ref(),
        b"data"
    );
    Ok(())
}

#[allow(clippy::too_many_lines)]
#[test]
fn public_byte_boundaries_reject_before_allocation_or_backend_work()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut limited = config();
    limited.limits.maximum_read_bytes = 4;
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([189; 16]),
        limited,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("volume creation blocked")??
    .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("checkout blocked")??
    .value;
    let file = path("bounded")?;
    let failure = poll_ready(checkout.create_file(
        file.clone(),
        Bytes::from_static(b"12345"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("oversized create blocked")?
    .err()
    .ok_or("oversized create succeeded")?;
    assert!(matches!(
        failure.error,
        FsError::FileRead(FileRangeReadError::InvalidRange)
    ));
    assert_eq!(*failure.work, WorkCounters::default());

    let file_id = poll_ready(checkout.create_file(
        file.clone(),
        Bytes::from_static(b"1234"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("bounded create blocked")??
    .value;
    for failure in [
        poll_ready(checkout.write_file(
            file.clone(),
            0,
            Bytes::from_static(b"12345"),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("oversized path write blocked")?
        .err()
        .ok_or("oversized path write succeeded")?,
        poll_ready(checkout.write_file_by_id(
            file_id,
            0,
            Bytes::from_static(b"12345"),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("oversized identity write blocked")?
        .err()
        .ok_or("oversized identity write succeeded")?,
    ] {
        assert!(matches!(
            failure.error,
            FsError::FileRead(FileRangeReadError::InvalidRange)
        ));
        assert_eq!(*failure.work, WorkCounters::default());
    }

    poll_ready(checkout.write_file_by_id(
        file_id,
        4,
        Bytes::from_static(b"5"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("bounded extension blocked")??;

    for failure in [
        poll_ready(checkout.read_file_range(
            &file,
            ByteRange {
                offset: 0,
                length: 5,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("oversized path read blocked")?
        .err()
        .ok_or("oversized path read succeeded")?,
        poll_ready(checkout.read_file_range_by_id(
            file_id,
            ByteRange {
                offset: 0,
                length: 5,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("oversized identity read blocked")?
        .err()
        .ok_or("oversized identity read succeeded")?,
    ] {
        assert!(matches!(
            failure.error,
            FsError::FileRead(FileRangeReadError::InvalidRange)
        ));
        assert_eq!(*failure.work, WorkCounters::default());
    }

    let excessive_range = ByteRange {
        offset: 0,
        length: limited.limits.maximum_generation_bytes + 1,
    };
    for failure in [
        poll_ready(checkout.plan_file_extents(
            &file,
            excessive_range,
            1,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("oversized path plan blocked")?
        .err()
        .ok_or("oversized path plan succeeded")?,
        poll_ready(checkout.plan_file_extents_by_id(
            file_id,
            excessive_range,
            1,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("oversized identity plan blocked")?
        .err()
        .ok_or("oversized identity plan succeeded")?,
    ] {
        assert!(matches!(
            failure.error,
            FsError::FileRead(FileRangeReadError::InvalidRange)
        ));
        assert_eq!(*failure.work, WorkCounters::default());
    }

    let symbolic = poll_ready(checkout.create_symbolic_link(
        path("link")?,
        Bytes::from_static(b"12345"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("oversized symbolic link blocked")?
    .err()
    .ok_or("oversized symbolic link succeeded")?;
    assert!(matches!(
        symbolic.error,
        FsError::FileRead(FileRangeReadError::InvalidRange)
    ));
    assert_eq!(*symbolic.work, WorkCounters::default());

    let mut detached =
        poll_ready(checkout.detach_regular_file(&file, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("file detachment blocked")??
            .value;
    let detached_read_failure = poll_ready(detached.read_range(
        ByteRange {
            offset: 0,
            length: 5,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("oversized detached read blocked")?
    .err()
    .ok_or("oversized detached read succeeded")?;
    assert!(matches!(
        detached_read_failure.error,
        FsError::FileRead(FileRangeReadError::InvalidRange)
    ));
    assert_eq!(*detached_read_failure.work, WorkCounters::default());
    let detached_failure = poll_ready(detached.write_range(
        0,
        Bytes::from_static(b"12345"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("oversized detached write blocked")?
    .err()
    .ok_or("oversized detached write succeeded")?;
    assert!(matches!(
        detached_failure.error,
        FsError::FileRead(FileRangeReadError::InvalidRange)
    ));
    assert_eq!(*detached_failure.work, WorkCounters::default());
    Ok(())
}

#[allow(clippy::too_many_lines)]
#[test]
fn checkout_generation_and_mode_guards_reject_without_candidate_damage()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let first = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([119; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("first volume blocked")??
    .value;
    let second = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([120; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("second volume blocked")??
    .value;
    let first_head = poll_ready(first.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("first head blocked")??
    .value
    .generation_id();
    let second_head = poll_ready(second.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("second head blocked")??
    .value
    .generation_id();
    assert!(
        poll_ready(first.checkout(
            GenerationSelector::Exact(first_head),
            writable_pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("writable exact checkout blocked")?
        .is_err()
    );
    assert!(
        poll_ready(first.checkout(
            GenerationSelector::Exact(second_head),
            pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("foreign exact checkout blocked")?
        .is_err()
    );
    assert!(
        poll_ready(first.diff_generations(
            first_head,
            second_head,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("foreign generation diff blocked")?
        .is_err()
    );

    let mut read_only = poll_ready(first.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("read-only checkout blocked")??
    .value;
    assert!(
        poll_ready(read_only.mutate(Vec::new(), WorkBudget::UNBOUNDED, &cancellation,))
            .ok_or("read-only mutation blocked")?
            .is_err()
    );
    assert!(
        poll_ready(read_only.commit(OperationId::new(), WorkBudget::UNBOUNDED, &cancellation,))
            .ok_or("read-only commit blocked")?
            .is_err()
    );
    assert!(
        poll_ready(read_only.discard(WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("read-only discard blocked")?
            .is_err()
    );
    assert!(
        poll_ready(read_only.refresh_head(WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("pinned refresh blocked")?
            .is_err()
    );
    assert!(
        poll_ready(read_only.refresh_live(WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("non-live refresh blocked")?
            .is_err()
    );
    assert!(
        poll_ready(read_only.mutate_live(
            Vec::new(),
            OperationId::new(),
            1,
            1,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("non-live mutation blocked")?
        .is_err()
    );
    assert!(
        poll_ready(read_only.resume_live(
            OperationId::new(),
            1,
            1,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("non-live resume blocked")?
        .is_err()
    );

    let mut writable = poll_ready(first.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("writable checkout blocked")??
    .value;
    let excessive_capacity = Vec::<Mutation>::with_capacity(
        usize::try_from(config().limits.maximum_mutations_per_batch)? + 1,
    );
    let failure =
        poll_ready(writable.mutate(excessive_capacity, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("capacity-guarded mutation blocked")?
            .err()
            .ok_or("excessive mutation capacity unexpectedly succeeded")?;
    assert!(matches!(failure.error, FsError::ExcessiveMutationCapacity));
    assert_eq!(*failure.work, WorkCounters::default());

    let mut clean = poll_ready(first.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("clean writable checkout blocked")??
    .value;
    assert!(
        poll_ready(clean.commit(OperationId::new(), WorkBudget::UNBOUNDED, &cancellation,))
            .ok_or("clean commit blocked")?
            .is_err()
    );
    assert!(
        poll_ready(clean.resume_live(
            OperationId::new(),
            1,
            1,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("clean non-live resume blocked")?
        .is_err()
    );

    let mut manual_checkout = poll_ready(first.checkout(
        GenerationSelector::Head,
        writable_manual(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("manual checkout blocked")??
    .value;
    poll_ready(manual_checkout.create_file(
        path("dirty")?,
        Bytes::from_static(b"dirty"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("manual mutation blocked")??;
    assert!(
        poll_ready(manual_checkout.refresh_head(WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("dirty manual refresh blocked")?
            .is_err()
    );
    assert!(manual_checkout.has_pending_mutations());
    poll_ready(manual_checkout.discard(WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("manual discard blocked")??;
    assert!(!manual_checkout.has_pending_mutations());
    assert_eq!(
        poll_ready(manual_checkout.refresh_head(WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("clean manual refresh blocked")??
            .value,
        first_head
    );
    Ok(())
}

#[allow(clippy::too_many_lines)]
#[test]
fn authored_special_operations_are_profile_exact_and_fail_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut posix = config();
    posix.profile = FilesystemProfile::Posix;
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([115; 16]),
        posix,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("POSIX volume blocked")??
    .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("POSIX checkout blocked")??
    .value;
    let receipt = poll_ready(checkout.apply_authored_transaction(
        vec![
            AuthoredMutation::CreateEmptySpecial {
                path: path("fifo")?,
                kind: FileKind::Fifo,
                metadata: empty_metadata(),
            },
            AuthoredMutation::CreateEmptySpecial {
                path: path("socket")?,
                kind: FileKind::Socket,
                metadata: empty_metadata(),
            },
            AuthoredMutation::CreateEmptySpecial {
                path: path("mount")?,
                kind: FileKind::MountBoundary,
                metadata: empty_metadata(),
            },
            AuthoredMutation::CreateDevice {
                path: path("character")?,
                kind: FileKind::CharacterDevice,
                major: 1,
                minor: 3,
                metadata: empty_metadata(),
            },
            AuthoredMutation::CreateDevice {
                path: path("block")?,
                kind: FileKind::BlockDevice,
                major: 8,
                minor: 0,
                metadata: empty_metadata(),
            },
        ],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("POSIX authored transaction blocked")??;
    assert!(receipt.value.created_file_ids.iter().all(Option::is_some));

    for (mutation, sentinel) in [
        (
            AuthoredMutation::CreateEmptySpecial {
                path: path("invalid-special")?,
                kind: FileKind::Regular,
                metadata: empty_metadata(),
            },
            path("invalid-special")?,
        ),
        (
            AuthoredMutation::CreateDevice {
                path: path("invalid-device")?,
                kind: FileKind::Regular,
                major: 0,
                minor: 0,
                metadata: empty_metadata(),
            },
            path("invalid-device")?,
        ),
    ] {
        let failure = poll_ready(checkout.apply_authored_transaction(
            vec![mutation],
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("invalid authored transaction blocked")?;
        assert!(failure.is_err());
        assert!(
            poll_ready(checkout.lookup_no_follow(&sentinel, WorkBudget::UNBOUNDED, &cancellation,))
                .ok_or("invalid-path lookup blocked")??
                .value
                .record
                .is_none()
        );
    }

    let mut windows = config();
    windows.profile = FilesystemProfile::Windows;
    let windows_volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([116; 16]),
        windows,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("Windows volume blocked")??
    .value;
    let mut windows_checkout = poll_ready(windows_volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("Windows checkout blocked")??
    .value;
    let reparse = path("reparse")?;
    let reparse_payload = Bytes::from_static(b"opaque-reparse-data");
    let receipt = poll_ready(windows_checkout.apply_authored_transaction(
        vec![AuthoredMutation::CreateReparsePoint {
            path: reparse.clone(),
            payload: reparse_payload.clone(),
            metadata: empty_metadata(),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("Windows authored transaction blocked")??;
    assert!(receipt.value.created_file_ids[0].is_some());
    assert_eq!(
        poll_ready(windows_checkout.read_reparse_point(
            &reparse,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("reparse read blocked")??
        .value,
        reparse_payload
    );
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn manifest_transfer_restores_only_after_the_complete_closure_authenticates()
-> Result<(), Box<dyn std::error::Error>> {
    let source = Fs::memory();
    let destination = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(source.create_volume_with_id(
        VolumeId::from_bytes([97; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("source volume blocked")??
    .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("source checkout blocked")??
    .value;
    poll_ready(checkout.create_file(
        path("file")?,
        Bytes::from(vec![42; 4096]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("source file blocked")??;
    let first_checkpoint = poll_ready(checkout.checkpoint(WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("first checkpoint blocked")??;
    let repeated_checkpoint = poll_ready(checkout.checkpoint(WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("repeated checkpoint blocked")??;
    assert_eq!(first_checkpoint.value, repeated_checkpoint.value);
    let manifest = poll_ready(checkout.export_manifest(WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("manifest blocked")??
        .value;
    assert_eq!(manifest.generation_id, first_checkpoint.value);
    assert!(manifest.objects.contains(&manifest.generation_root));

    let mut cursor = TransferCursor::START;
    loop {
        let exported = poll_ready(source.export_generation_batch(
            &manifest,
            cursor,
            2,
            config().limits.maximum_object_bytes,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("object export batch blocked")??;
        let bodies = exported
            .value
            .objects
            .iter()
            .map(|object| object.bytes.clone())
            .collect::<Vec<_>>();
        let imported = poll_ready(destination.import_generation_batch(
            &manifest,
            cursor,
            &bodies,
            2,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("object import batch blocked")??;
        assert_eq!(
            imported.value.next_object(),
            cursor.next_object() + u64::try_from(bodies.len())?
        );
        let Some(next) = exported.value.next else {
            assert_eq!(
                imported.value.next_object(),
                u64::try_from(manifest.objects.len())?
            );
            break;
        };
        assert_eq!(imported.value, next);
        cursor = next;
    }
    let mut wrong_volume = manifest.clone();
    wrong_volume.volume_id = VolumeId::from_bytes([96; 16]);
    let mut wrong_generation = manifest.clone();
    wrong_generation.generation_id = GenerationId::new(Digest::from_bytes([0xee; 32]));
    let mut wrong_objects = manifest.clone();
    wrong_objects
        .objects
        .pop()
        .ok_or("manifest closure is empty")?;
    let mut wrong_file_count = manifest.clone();
    wrong_file_count.file_count = wrong_file_count
        .file_count
        .checked_add(1)
        .ok_or("fixture file count overflow")?;
    for (label, invalid, expected_backend_reads) in [
        ("volume", wrong_volume, true),
        ("generation", wrong_generation, false),
        ("objects", wrong_objects, true),
        ("file-count", wrong_file_count, true),
    ] {
        let failure =
            poll_ready(destination.restore_volume(&invalid, WorkBudget::UNBOUNDED, &cancellation))
                .ok_or("invalid restore blocked")?
                .err()
                .ok_or("invalid manifest unexpectedly restored")?;
        assert!(matches!(failure.error, FsError::InvalidExportManifest));
        assert_eq!(
            failure.work.backend_read_operations > 0,
            expected_backend_reads,
            "unexpected invalid-manifest physical plan for {label}"
        );
        assert_eq!(failure.work.authority_records_appended, 0);
    }
    let restored =
        poll_ready(destination.restore_volume(&manifest, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("restore blocked")??
            .value;
    let mut restored_checkout = poll_ready(restored.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("restored checkout blocked")??
    .value;
    let bytes = poll_ready(restored_checkout.read_file_range(
        &path("file")?,
        ByteRange {
            offset: 0,
            length: 4096,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("restored read blocked")??;
    assert_eq!(bytes.value.bytes.as_ref(), vec![42; 4096]);
    Ok(())
}

#[test]
fn windows_reparse_payload_round_trips_without_interpretation()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut windows = config();
    windows.profile = FilesystemProfile::Windows;
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([98; 16]),
        windows,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("Windows volume blocked")??
    .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("Windows checkout blocked")??
    .value;
    let payload = Bytes::from_static(b"opaque-reparse\0payload");
    poll_ready(checkout.create_reparse_point(
        path("junction")?,
        payload.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("reparse create blocked")??;
    let read = poll_ready(checkout.read_reparse_point(
        &path("junction")?,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("reparse read blocked")??;
    assert_eq!(read.value, payload);

    let empty_reparse = path("empty-reparse")?;
    poll_ready(checkout.create_reparse_point(
        empty_reparse.clone(),
        Bytes::new(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("empty reparse create blocked")??;
    let empty_reparse_read = poll_ready(checkout.read_reparse_point(
        &empty_reparse,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("empty reparse read blocked")??;
    assert!(empty_reparse_read.value.is_empty());

    let empty_link = path("empty-link")?;
    poll_ready(checkout.create_symbolic_link(
        empty_link.clone(),
        Bytes::new(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("empty symbolic-link create blocked")??;
    let empty_link_read =
        poll_ready(checkout.read_symbolic_link(&empty_link, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("empty symbolic-link read blocked")??;
    assert!(empty_link_read.value.is_empty());
    Ok(())
}

#[test]
fn encoded_object_and_backend_buffers_share_one_exact_peak()
-> Result<(), Box<dyn std::error::Error>> {
    let prior = WorkCounters {
        bytes_encoded: 64,
        peak_allocation_bytes: 64,
        ..WorkCounters::default()
    };
    let nested = WorkCounters {
        backend_write_operations: 1,
        peak_allocation_bytes: 32,
        ..WorkCounters::default()
    };
    let mut budget = WorkBudget::UNBOUNDED;
    budget.peak_allocation_bytes = 96;
    let merged =
        merge_simultaneous_work(prior, nested, 64, budget).map_err(|failure| failure.error)?;
    assert_eq!(merged.peak_allocation_bytes, 96);
    assert_eq!(merged.backend_write_operations, 1);

    budget.peak_allocation_bytes = 95;
    let rejected = merge_simultaneous_work(prior, nested, 64, budget)
        .err()
        .ok_or("simultaneous peak unexpectedly admitted")?;
    assert!(matches!(
        rejected.error,
        FsError::Work(WorkError::BudgetExceeded {
            counter: "peak_allocation_bytes",
            observed: 96,
            maximum: 95,
        })
    ));

    let fs = Fs::memory_bounded(config().limits.maximum_object_bytes)?;
    let cancellation = CancellationToken::new();
    let encoded = vec![7_u8; 16];
    let encoded_capacity = u64::try_from(encoded.capacity())?;
    let stored = poll_ready(fs.put_encoded(
        ObjectKind::Metadata,
        encoded.clone(),
        WorkCounters::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("encoded object put blocked")??;
    assert_eq!(stored.1.peak_allocation_bytes, encoded_capacity);
    assert_eq!(stored.1.backend_write_operations, 1);

    let mut one_byte_short = WorkBudget::UNBOUNDED;
    one_byte_short.peak_allocation_bytes = encoded_capacity - 1;
    let failure = poll_ready(fs.put_encoded(
        ObjectKind::Metadata,
        encoded,
        WorkCounters::default(),
        one_byte_short,
        &cancellation,
    ))
    .ok_or("peak-limited encoded put blocked")?
    .err()
    .ok_or("one-byte-short encoded put unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        FsError::Work(WorkError::BudgetExceeded {
            counter: "peak_allocation_bytes",
            observed,
            maximum,
        }) if observed == encoded_capacity && maximum == encoded_capacity - 1
    ));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[test]
fn memory_create_open_and_head_checkout_reconstruct_from_two_stores()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory_bounded(config().limits.maximum_object_bytes)?;
    let cancellation = CancellationToken::new();
    let volume_id = VolumeId::from_bytes([41; 16]);
    let created = poll_ready(fs.create_volume_with_id(
        volume_id,
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("memory create blocked")??;
    let opened = poll_ready(fs.open_volume(volume_id, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("memory open blocked")??;
    assert_eq!(opened.value.config(), config());
    let mut checkout = poll_ready(opened.value.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("memory checkout blocked")??;
    assert_eq!(checkout.value.volume_id(), volume_id);
    assert_eq!(checkout.value.root().parents, Vec::new());
    assert_eq!(checkout.work.object_probes, 1);
    assert_eq!(checkout.work.page_reads, 0);
    assert!(checkout.work.object_bytes_read <= MAXIMUM_GENERATION_ROOT_BYTES);
    let root_path = NamespacePath::new(Vec::new(), config().limits)?;
    let root = poll_ready(checkout.value.lookup_no_follow(
        &root_path,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("memory root lookup blocked")??;
    assert_eq!(
        root.value.record.map(|record| record.kind),
        Some(FileKind::Directory)
    );
    let roots = poll_ready(checkout.value.lookup_batch_no_follow(
        &[root_path.clone(), root_path],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("memory root batch blocked")??;
    assert_eq!(roots.value.entries.len(), 2);
    assert_eq!(roots.value.entries[0], roots.value.entries[1]);
    assert!(created.work.backend_write_operations >= 4);
    Ok(())
}

#[test]
fn create_retry_is_idempotent_and_exact_selector_matches_head()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume_id = VolumeId::from_bytes([42; 16]);
    let first = poll_ready(fs.create_volume_with_id(
        volume_id,
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("first create blocked")??;
    let retry = poll_ready(fs.create_volume_with_id(
        volume_id,
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("retry create blocked")??;
    let head = poll_ready(first.value.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("head checkout blocked")??;
    let exact = poll_ready(retry.value.checkout(
        GenerationSelector::Exact(head.value.generation_id()),
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("exact checkout blocked")??;
    assert_eq!(exact.value.generation_id(), head.value.generation_id());
    Ok(())
}

#[test]
fn unsupported_semantics_reject_before_backend_work() -> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut durable = config();
    durable.lifecycle = Lifecycle::Durable;
    let result = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([43; 16]),
        durable,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("memory create blocked")?;
    let Err(failure) = result else {
        return Err("durable memory volume succeeded".into());
    };
    assert!(matches!(failure.error, FsError::UnsupportedDurability));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[test]
fn conflicting_creation_config_is_rejected_without_changing_opened_config()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume_id = VolumeId::from_bytes([44; 16]);
    poll_ready(fs.create_volume_with_id(volume_id, config(), WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("first create blocked")??;
    let mut conflicting = config();
    conflicting.sparse_files = false;
    let retry = poll_ready(fs.create_volume_with_id(
        volume_id,
        conflicting,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("conflicting create blocked")?;
    let Err(failure) = retry else {
        return Err("conflicting creation succeeded".into());
    };
    assert!(matches!(failure.error, FsError::CreationRejected));
    let opened = poll_ready(fs.open_volume(volume_id, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("open blocked")??;
    assert_eq!(opened.value.config(), config());
    Ok(())
}

#[test]
fn exact_generation_cannot_cross_volume_identity() -> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let first = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([45; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("first volume create blocked")??;
    let second = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([46; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("second volume create blocked")??;
    let second_head = poll_ready(second.value.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("second head blocked")??;
    let crossing = poll_ready(first.value.checkout(
        GenerationSelector::Exact(second_head.value.generation_id()),
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("cross-volume exact checkout blocked")?;
    let Err(failure) = crossing else {
        return Err("cross-volume exact checkout succeeded".into());
    };
    assert!(matches!(failure.error, FsError::VolumeMismatch));
    Ok(())
}

#[test]
fn pre_cancelled_facade_operations_perform_zero_work() -> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let result = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([47; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("cancelled create blocked")?;
    let Err(failure) = result else {
        return Err("cancelled create succeeded".into());
    };
    assert!(matches!(failure.error, FsError::Cancelled(_)));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn pre_cancelled_existing_volume_surfaces_fail_before_visible_work()
-> Result<(), Box<dyn std::error::Error>> {
    macro_rules! cancelled {
        ($future:expr, $label:literal) => {{
            let result = poll_ready($future).ok_or(concat!($label, " blocked"))?;
            let failure = result
                .err()
                .ok_or(concat!($label, " unexpectedly succeeded"))?;
            assert!(
                format!("{:?}", failure.error).contains("Cancelled"),
                "{}: {:?}",
                $label,
                failure.error
            );
            assert_eq!(*failure.work, WorkCounters::default(), "{}", $label);
        }};
    }

    let fs = Fs::memory();
    let active = CancellationToken::new();
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([137; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &active,
    ))
    .ok_or("cancellation fixture volume blocked")??
    .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &active,
    ))
    .ok_or("cancellation fixture checkout blocked")??
    .value;
    let file = path("cancelled-file")?;
    let file_id = poll_ready(checkout.create_file(
        file.clone(),
        Bytes::from_static(b"abcdefgh"),
        WorkBudget::UNBOUNDED,
        &active,
    ))
    .ok_or("cancellation fixture file blocked")??
    .value;
    let mut detached =
        poll_ready(checkout.detach_regular_file(&file, WorkBudget::UNBOUNDED, &active))
            .ok_or("cancellation fixture detach blocked")??
            .value;
    let cancelled_token = CancellationToken::new();
    cancelled_token.cancel();
    let range = ByteRange {
        offset: 0,
        length: 1,
    };
    let attribute = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.cancelled".to_vec(),
        128,
    )?;

    cancelled!(
        volume.checkout(
            GenerationSelector::Head,
            pinned(),
            WorkBudget::UNBOUNDED,
            &cancelled_token,
        ),
        "cancelled checkout"
    );
    cancelled!(
        checkout.lookup_no_follow(&file, WorkBudget::UNBOUNDED, &cancelled_token),
        "cancelled lookup"
    );
    cancelled!(
        checkout.lookup_batch_no_follow(
            std::slice::from_ref(&file),
            WorkBudget::UNBOUNDED,
            &cancelled_token,
        ),
        "cancelled batch lookup"
    );
    cancelled!(
        checkout.list_directory(
            &root_path()?,
            None,
            8,
            WorkBudget::UNBOUNDED,
            &cancelled_token,
        ),
        "cancelled directory listing"
    );
    cancelled!(
        checkout.read_metadata(&file, WorkBudget::UNBOUNDED, &cancelled_token),
        "cancelled metadata"
    );
    cancelled!(
        checkout.read_file_range(&file, range, WorkBudget::UNBOUNDED, &cancelled_token),
        "cancelled path read"
    );
    cancelled!(
        checkout.plan_file_extents(&file, range, 8, WorkBudget::UNBOUNDED, &cancelled_token,),
        "cancelled path plan"
    );
    cancelled!(
        checkout.seek_file_extent(
            &file,
            0,
            ExtentSeekTarget::Data,
            WorkBudget::UNBOUNDED,
            &cancelled_token,
        ),
        "cancelled path seek"
    );
    cancelled!(
        checkout.read_file_record_by_id(file_id, WorkBudget::UNBOUNDED, &cancelled_token),
        "cancelled identity record"
    );
    cancelled!(
        checkout.read_file_range_by_id(file_id, range, WorkBudget::UNBOUNDED, &cancelled_token,),
        "cancelled identity read"
    );
    cancelled!(
        checkout.read_named_attribute(&file, &attribute, WorkBudget::UNBOUNDED, &cancelled_token,),
        "cancelled path attribute"
    );
    cancelled!(
        checkout.write_file(
            file.clone(),
            0,
            Bytes::from_static(b"x"),
            WorkBudget::UNBOUNDED,
            &cancelled_token,
        ),
        "cancelled path write"
    );
    cancelled!(
        checkout.create_file(
            path("cancelled-new-file")?,
            Bytes::from_static(b"must-not-be-encoded"),
            WorkBudget::UNBOUNDED,
            &cancelled_token,
        ),
        "cancelled file creation"
    );
    let mut empty = std::io::Cursor::new(Bytes::new());
    cancelled!(
        checkout.stage_content(&mut empty, 1, WorkBudget::UNBOUNDED, &cancelled_token,),
        "cancelled content staging"
    );
    cancelled!(
        checkout.commit(
            OperationId::from_bytes([138; 16]),
            WorkBudget::UNBOUNDED,
            &cancelled_token,
        ),
        "cancelled commit"
    );
    cancelled!(
        detached.read_range(range, WorkBudget::UNBOUNDED, &cancelled_token),
        "cancelled detached read"
    );
    cancelled!(
        detached.write_range(
            0,
            Bytes::from_static(b"x"),
            WorkBudget::UNBOUNDED,
            &cancelled_token,
        ),
        "cancelled detached write"
    );
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn private_overlay_commit_retry_and_conflict_are_generation_fenced()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory_bounded(config().limits.maximum_object_bytes)?;
    let cancellation = CancellationToken::new();
    let volume_id = VolumeId::from_bytes([49; 16]);
    let volume = poll_ready(fs.create_volume_with_id(
        volume_id,
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("memory create blocked")??
    .value;
    let mut first = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("first writable checkout blocked")??
    .value;
    let mut retry = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("retry writable checkout blocked")??
    .value;
    let mut stale = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("stale writable checkout blocked")??
    .value;
    let root_path = NamespacePath::new(Vec::new(), config().limits)?;
    let metadata =
        poll_ready(first.lookup_no_follow(&root_path, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("root lookup blocked")??
            .value
            .record
            .ok_or("root record absent")?
            .metadata;
    let record = FileRecord {
        file_id: FileId::from_bytes([7; 16]),
        kind: FileKind::Regular,
        link_count: 1,
        metadata,
        payload: FilePayload::InlineRegular(InlineFileData::new(b"hello")?),
    };
    for checkout in [&mut first, &mut retry] {
        poll_ready(checkout.mutate(
            vec![Mutation::Create {
                path: path("a")?,
                record,
            }],
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("overlay mutation blocked")??;
        assert_eq!(
            poll_ready(checkout.lookup_no_follow(
                &path("a")?,
                WorkBudget::UNBOUNDED,
                &cancellation,
            ))
            .ok_or("candidate lookup blocked")??
            .value
            .record
            .map(|value| value.payload),
            Some(record.payload)
        );
    }
    assert!(
        poll_ready(stale.lookup_no_follow(&path("a")?, WorkBudget::UNBOUNDED, &cancellation,))
            .ok_or("base lookup blocked")??
            .value
            .record
            .is_none()
    );
    poll_ready(stale.mutate(
        vec![Mutation::Create {
            path: path("b")?,
            record: FileRecord {
                file_id: FileId::from_bytes([8; 16]),
                ..record
            },
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("stale overlay mutation blocked")??;

    let operation_id = OperationId::from_bytes([9; 16]);
    let committed = poll_ready(first.commit(operation_id, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("first commit blocked")??;
    assert!(matches!(
        committed.value,
        CheckoutCommitOutcome::Committed { .. }
    ));
    assert!(!first.has_pending_mutations());
    let repeated = poll_ready(retry.commit(operation_id, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("idempotent commit blocked")??;
    assert!(matches!(
        repeated.value,
        CheckoutCommitOutcome::AlreadyCommitted { .. }
    ));
    assert!(!retry.has_pending_mutations());
    let conflict = poll_ready(stale.commit(
        OperationId::from_bytes([10; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("stale commit blocked")??;
    assert!(matches!(
        conflict.value,
        CheckoutCommitOutcome::Conflict { .. }
    ));
    assert!(stale.has_pending_mutations());

    let mut head = poll_ready(volume.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("published head checkout blocked")??;
    assert!(
        poll_ready(
            head.value
                .lookup_no_follow(&path("a")?, WorkBudget::UNBOUNDED, &cancellation,)
        )
        .ok_or("published file lookup blocked")??
        .value
        .record
        .is_some()
    );
    let clean_failure = poll_ready(first.commit(
        OperationId::from_bytes([11; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("clean commit blocked")?
    .err()
    .ok_or("clean checkout commit unexpectedly succeeded")?;
    assert!(matches!(clean_failure.error, FsError::NoPendingMutations));
    assert_eq!(*clean_failure.work, WorkCounters::default());

    let mut inverse = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("inverse writable checkout blocked")??
    .value;
    let inverse_record = FileRecord {
        file_id: FileId::from_bytes([15; 16]),
        ..record
    };
    poll_ready(inverse.mutate(
        vec![Mutation::Create {
            path: path("inverse")?,
            record: inverse_record,
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("inverse create blocked")??;
    assert!(inverse.has_pending_mutations());
    poll_ready(inverse.mutate(
        vec![Mutation::Remove {
            path: path("inverse")?,
            expected_file_id: MetadataField::Value(inverse_record.file_id),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("inverse removal blocked")??;
    assert!(!inverse.has_pending_mutations());
    assert!(inverse.dependencies.is_empty());
    let inverse_commit = poll_ready(inverse.commit(
        OperationId::from_bytes([16; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("inverse clean commit blocked")?
    .err()
    .ok_or("inverse clean commit unexpectedly succeeded")?;
    assert!(matches!(inverse_commit.error, FsError::NoPendingMutations));
    assert_eq!(
        *inverse_commit.work,
        WorkCounters {
            backend_read_operations: 1,
            ..WorkCounters::default()
        }
    );
    Ok(())
}

#[test]
fn exclusive_writer_checkout_atomically_fences_every_prior_writer()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([0xe1; 16]),
        exclusive_config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("exclusive volume creation blocked")??
    .value;
    let mut displaced = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("first exclusive checkout blocked")??
    .value;
    let mut owner = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("replacement exclusive checkout blocked")??
    .value;
    poll_ready(displaced.create_file(
        path("displaced")?,
        Bytes::from_static(b"stale"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("displaced mutation blocked")??;
    let rejected = poll_ready(displaced.commit(
        OperationId::from_bytes([0xe2; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("displaced publication blocked")??;
    assert!(matches!(
        rejected.value,
        CheckoutCommitOutcome::Fenced { actual_epoch } if actual_epoch.get() == 3
    ));
    poll_ready(owner.create_file(
        path("owner")?,
        Bytes::from_static(b"current"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("owner mutation blocked")??;
    let committed = poll_ready(owner.commit(
        OperationId::from_bytes([0xe3; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("owner publication blocked")??;
    assert!(matches!(
        committed.value,
        CheckoutCommitOutcome::Committed { head, .. } if head.epoch.get() == 3
    ));
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn manual_refresh_is_explicit_bounded_and_never_discards_mutations()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory_bounded(config().limits.maximum_object_bytes)?;
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([50; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("memory create blocked")??
    .value;
    let mut clean = poll_ready(volume.checkout(
        GenerationSelector::Head,
        manual(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("clean manual checkout blocked")??
    .value;
    let mut dirty = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_manual(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("dirty manual checkout blocked")??
    .value;
    let mut conflicted = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_manual(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("conflicted manual checkout blocked")??
    .value;
    let mut writer = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("writer checkout blocked")??
    .value;
    let mut pinned_checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("pinned checkout blocked")??
    .value;
    let root_path = NamespacePath::new(Vec::new(), config().limits)?;
    let metadata =
        poll_ready(writer.lookup_no_follow(&root_path, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("root lookup blocked")??
            .value
            .record
            .ok_or("root record absent")?
            .metadata;
    let record = FileRecord {
        file_id: FileId::from_bytes([12; 16]),
        kind: FileKind::Regular,
        link_count: 1,
        metadata,
        payload: FilePayload::InlineRegular(InlineFileData::new(b"new-head")?),
    };
    assert!(
        poll_ready(conflicted.lookup_no_follow(
            &path("published")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("negative dependency lookup blocked")??
        .value
        .record
        .is_none()
    );
    poll_ready(conflicted.mutate(
        vec![Mutation::Create {
            path: path("conflicted-local")?,
            record: FileRecord {
                file_id: FileId::from_bytes([16; 16]),
                ..record
            },
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("conflicted local mutation blocked")??;
    poll_ready(dirty.mutate(
        vec![Mutation::Create {
            path: path("private")?,
            record,
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("dirty mutation blocked")??;
    poll_ready(writer.mutate(
        vec![Mutation::Create {
            path: path("published")?,
            record: FileRecord {
                file_id: FileId::from_bytes([13; 16]),
                ..record
            },
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("writer mutation blocked")??;
    poll_ready(writer.commit(
        OperationId::from_bytes([14; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("writer commit blocked")??;
    let conflict = poll_ready(conflicted.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("conflicting rebase blocked")??;
    assert!(matches!(
        conflict.value,
        RebaseDecision::Conflicted {
            ref conflicts,
            truncated: false,
        } if conflicts.iter().any(|value| matches!(
            value.region,
            DependencyRegion::DirectoryName { .. }
        ))
    ));
    assert!(conflicted.has_pending_mutations());

    let pinned_failure =
        poll_ready(pinned_checkout.refresh_head(WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("pinned refresh blocked")?
            .err()
            .ok_or("pinned refresh unexpectedly succeeded")?;
    assert!(matches!(pinned_failure.error, FsError::RefreshNotAllowed));
    assert_eq!(*pinned_failure.work, WorkCounters::default());
    let pinned_rebase =
        poll_ready(pinned_checkout.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("pinned rebase guard blocked")?
            .err()
            .ok_or("pinned rebase unexpectedly succeeded")?;
    assert!(matches!(pinned_rebase.error, FsError::RefreshNotAllowed));
    assert_eq!(*pinned_rebase.work, WorkCounters::default());

    let dirty_failure = poll_ready(dirty.refresh_head(WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("dirty refresh blocked")?
        .err()
        .ok_or("dirty refresh unexpectedly succeeded")?;
    assert!(matches!(
        dirty_failure.error,
        FsError::PendingMutationsRequireRebase
    ));
    assert_eq!(*dirty_failure.work, WorkCounters::default());
    assert!(dirty.has_pending_mutations());
    assert!(
        poll_ready(
            dirty.lookup_no_follow(&path("private")?, WorkBudget::UNBOUNDED, &cancellation,)
        )
        .ok_or("private lookup blocked")??
        .value
        .record
        .is_some()
    );
    let rebased = poll_ready(dirty.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("safe dirty rebase blocked")??;
    assert!(matches!(rebased.value, RebaseDecision::Safe { .. }));
    assert!(dirty.has_pending_mutations());
    assert!(
        poll_ready(dirty.lookup_no_follow(
            &path("published")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("rebased published lookup blocked")??
        .value
        .record
        .is_some()
    );
    assert!(
        poll_ready(
            dirty.lookup_no_follow(&path("private")?, WorkBudget::UNBOUNDED, &cancellation,)
        )
        .ok_or("rebased private lookup blocked")??
        .value
        .record
        .is_some()
    );

    let advanced = poll_ready(clean.refresh_head(WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("manual refresh blocked")??;
    assert_eq!(advanced.value, writer.generation_id());
    assert!(advanced.work.object_probes > 0);
    assert!(
        poll_ready(clean.lookup_no_follow(
            &path("published")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("published lookup blocked")??
        .value
        .record
        .is_some()
    );
    let equal = poll_ready(clean.refresh_head(WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("equal refresh blocked")??;
    assert_eq!(equal.value, advanced.value);
    assert_eq!(equal.work.object_probes, 0);
    assert_eq!(equal.work.backend_read_operations, 3);

    assert!(!dirty.dependencies.is_empty());
    poll_ready(dirty.discard(WorkBudget::UNBOUNDED, &cancellation)).ok_or("discard blocked")??;
    assert!(!dirty.has_pending_mutations());
    assert!(dirty.dependencies.is_empty());
    assert!(
        poll_ready(
            dirty.lookup_no_follow(&path("private")?, WorkBudget::UNBOUNDED, &cancellation,)
        )
        .ok_or("discarded lookup blocked")??
        .value
        .record
        .is_none()
    );
    assert!(
        poll_ready(dirty.lookup_no_follow(
            &path("published")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("discarded published lookup blocked")??
        .value
        .record
        .is_some()
    );
    let after_discard = poll_ready(dirty.refresh_head(WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("post-discard refresh blocked")??;
    assert_eq!(after_discard.value, writer.generation_id());
    Ok(())
}

#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_facade_reopens_durable_volume_and_exact_generation()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let volume_id = VolumeId::from_bytes([48; 16]);
    let cancellation = CancellationToken::new();
    let generation_id;
    let mut durable = config();
    durable.lifecycle = Lifecycle::Durable;
    {
        let fs = Fs::local(LocalOptions::new(directory.path()))?;
        let created = fs
            .create_volume_with_id(volume_id, durable, WorkBudget::UNBOUNDED, &cancellation)
            .await?;
        let checkout = created
            .value
            .checkout(
                GenerationSelector::Head,
                writable_pinned(),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
        generation_id = checkout.value.generation_id();
    }
    let reopened = Fs::local(LocalOptions::new(directory.path()))?;
    let volume = reopened
        .open_volume(volume_id, WorkBudget::UNBOUNDED, &cancellation)
        .await?;
    let exact = volume
        .value
        .checkout(
            GenerationSelector::Exact(generation_id),
            pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    assert_eq!(exact.value.generation_id(), generation_id);
    assert_eq!(exact.value.volume_id(), volume_id);
    Ok(())
}

#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_facade_shares_bounded_object_acceleration_across_handles()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let cancellation = CancellationToken::new();
    let mut durable = config();
    durable.lifecycle = Lifecycle::Durable;
    let fs = Fs::local(LocalOptions::new(directory.path()))?;
    let volume = fs
        .create_volume(durable, WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value;
    let checkout = volume
        .checkout(
            GenerationSelector::Head,
            pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    let root = ObjectId {
        kind: ObjectKind::GenerationRoot,
        digest: checkout.generation_id().digest(),
    };

    fs.clear_object_cache()?;
    let baseline = fs.object_cache_stats()?;
    assert_eq!(baseline.resident_entries, 0);
    let first = fs
        .export_object(
            root,
            durable.limits.maximum_object_bytes,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    let cloned = fs.clone();
    let warm = cloned
        .export_object(
            root,
            durable.limits.maximum_object_bytes,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;

    assert_eq!(first.value.bytes, warm.value.bytes);
    assert_eq!(first.work.backend_read_operations, 1);
    assert_eq!(warm.work.backend_read_operations, 0);
    let stats = fs.object_cache_stats()?;
    assert_eq!(stats.misses - baseline.misses, 1);
    assert_eq!(stats.hits - baseline.hits, 1);
    assert_eq!(stats.resident_entries, 1);
    assert!(stats.resident_bytes > 0);
    Ok(())
}

#[allow(clippy::too_many_lines)]
#[test]
fn public_identity_and_posix_special_surfaces_share_one_candidate()
-> Result<(), Box<dyn std::error::Error>> {
    macro_rules! ready {
        ($future:expr) => {
            poll_ready($future).ok_or("filesystem future blocked")??
        };
    }

    let mut volume_config = config();
    volume_config.profile = FilesystemProfile::Posix;
    let cancellation = CancellationToken::new();
    let fs = Fs::memory();
    let volume = ready!(fs.create_volume(volume_config, WorkBudget::UNBOUNDED, &cancellation,));
    let mut checkout = ready!(volume.value.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));

    let file = path("identity.bin")?;
    let clone = path("clone.bin")?;
    let file_id = ready!(checkout.value.create_file(
        file.clone(),
        Bytes::from_static(b"abcdefgh"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let clone_id = ready!(checkout.value.create_file(
        clone.clone(),
        Bytes::from_static(b"........"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;

    let record = ready!(checkout.value.read_file_record_by_id(
        file_id,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(record.value.file_id, file_id);
    assert_eq!(record.value.kind, FileKind::Regular);
    assert_eq!(
        ready!(checkout.value.read_file_range_by_id(
            file_id,
            ByteRange {
                offset: 2,
                length: 3,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value
        .bytes
        .as_ref(),
        b"cde"
    );
    assert!(
        ready!(checkout.value.plan_file_extents_by_id(
            file_id,
            ByteRange {
                offset: 0,
                length: 8,
            },
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value
        .is_none()
    );
    assert_eq!(
        ready!(checkout.value.seek_file_extent_by_id(
            file_id,
            0,
            ExtentSeekTarget::Data,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value,
        Some(0)
    );
    assert_eq!(
        ready!(checkout.value.seek_file_extent_by_id(
            file_id,
            0,
            ExtentSeekTarget::Hole,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value,
        Some(8)
    );
    assert_eq!(
        ready!(checkout.value.seek_file_extent_by_id(
            file_id,
            9,
            ExtentSeekTarget::Data,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value,
        None
    );
    assert_eq!(
        ready!(checkout.value.seek_file_extent(
            &file,
            9,
            ExtentSeekTarget::Data,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value,
        None
    );

    let mut path_metadata = ready!(checkout.value.read_metadata(
        &file,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    path_metadata.posix_mode = MetadataField::Value(0o100_660);
    ready!(checkout.value.set_attributes(
        file.clone(),
        path_metadata,
        Some(10),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(
        ready!(checkout.value.read_file_range_by_id(
            file_id,
            ByteRange {
                offset: 8,
                length: 2,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value
        .bytes
        .as_ref(),
        &[0, 0]
    );

    let attribute = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.identity".to_vec(),
        128,
    )?;
    assert!(
        ready!(checkout.value.read_named_attribute_by_id(
            file_id,
            &attribute,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value
        .is_none()
    );
    assert!(
        ready!(checkout.value.list_named_attributes_by_id(
            file_id,
            None,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value
        .entries
        .is_empty()
    );
    ready!(checkout.value.write_named_attribute_by_id(
        file_id,
        attribute.clone(),
        Bytes::from_static(b"first"),
        NamedAttributeWriteMode::Create,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(
        ready!(checkout.value.read_named_attribute_by_id(
            file_id,
            &attribute,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value
        .as_deref(),
        Some(b"first".as_slice())
    );
    let maximum_read_bytes = checkout.value.volume.config.limits.maximum_read_bytes;
    checkout.value.volume.config.limits.maximum_read_bytes = 4;
    for bounded in [
        poll_ready(checkout.value.read_named_attribute_by_id(
            file_id,
            &attribute,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("bounded identity attribute read blocked")?,
        poll_ready(checkout.value.read_named_attribute(
            &file,
            &attribute,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("bounded path attribute read blocked")?,
    ] {
        assert!(matches!(
            bounded,
            Err(OperationFailure {
                error: FsError::FileRead(FileRangeReadError::InvalidRange),
                ..
            })
        ));
    }
    checkout.value.volume.config.limits.maximum_read_bytes = maximum_read_bytes;
    assert_eq!(
        ready!(checkout.value.list_named_attributes_by_id(
            file_id,
            None,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value
        .entries
        .len(),
        1
    );
    ready!(checkout.value.write_named_attribute_by_id(
        file_id,
        attribute.clone(),
        Bytes::from_static(b"second"),
        NamedAttributeWriteMode::Replace,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));

    let mut metadata = ready!(checkout.value.read_metadata_by_id(
        file_id,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    metadata.posix_mode = MetadataField::Value(0o100_640);
    ready!(checkout.value.set_metadata_by_id(
        file_id,
        metadata,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    metadata.posix_mode = MetadataField::Value(0o100_600);
    ready!(checkout.value.set_attributes_by_id(
        file_id,
        metadata,
        Some(12),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    ready!(checkout.value.write_file_by_id(
        file_id,
        8,
        Bytes::from_static(b"tail"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    ready!(checkout.value.zero_file_range_by_id(
        file_id,
        ByteRange {
            offset: 4,
            length: 2,
        },
        false,
        false,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    ready!(checkout.value.preallocate_file_by_id(
        file_id,
        ByteRange {
            offset: 12,
            length: 4,
        },
        false,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    ready!(
        checkout
            .value
            .resize_file_by_id(clone_id, 16, WorkBudget::UNBOUNDED, &cancellation,)
    );
    ready!(checkout.value.clone_file_range_by_id(
        file_id,
        0,
        clone_id,
        4,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    ready!(checkout.value.remove_named_attribute_by_id(
        file_id,
        attribute.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert!(
        ready!(checkout.value.read_named_attribute_by_id(
            file_id,
            &attribute,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value
        .is_none()
    );

    let target = Bytes::from_static(b"../identity.bin");
    ready!(checkout.value.create_symbolic_link(
        path("link")?,
        target.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(
        ready!(checkout.value.read_symbolic_link(
            &path("link")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value,
        target
    );
    ready!(checkout.value.create_empty_special(
        path("fifo")?,
        FileKind::Fifo,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    ready!(checkout.value.create_empty_special(
        path("socket")?,
        FileKind::Socket,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    ready!(checkout.value.create_device(
        path("character")?,
        FileKind::CharacterDevice,
        1,
        3,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    ready!(checkout.value.create_device(
        path("block")?,
        FileKind::BlockDevice,
        8,
        0,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert!(
        poll_ready(checkout.value.create_empty_special(
            path("invalid")?,
            FileKind::Regular,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("invalid special blocked")?
        .is_err()
    );
    assert!(
        poll_ready(checkout.value.create_device(
            path("invalid-device")?,
            FileKind::Regular,
            0,
            0,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("invalid device blocked")?
        .is_err()
    );

    ready!(checkout.value.hard_link(
        file.clone(),
        path("hard-link")?,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    ready!(checkout.value.rename(
        path("hard-link")?,
        path("renamed-link")?,
        false,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    ready!(checkout.value.remove(
        path("renamed-link")?,
        Some(file_id),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let listing = ready!(checkout.value.list_directory_records(
        &root_path()?,
        None,
        32,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(listing.value.entries.len(), 7);
    Ok(())
}

#[cfg(feature = "local")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_garbage_collection_authenticates_heads_and_excludes_live_engines()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let options = LocalOptions::new(directory.path());
    let cancellation = CancellationToken::new();
    let volume_id = VolumeId::from_bytes([49; 16]);
    let file = path("retained.txt")?;
    let mut durable = config();
    durable.lifecycle = Lifecycle::Durable;
    let fs = Fs::local(options.clone())?;
    let volume = fs
        .create_volume_with_id(volume_id, durable, WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value;
    let mut checkout = volume
        .checkout(
            GenerationSelector::Head,
            writable_pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    checkout
        .create_file(
            file.clone(),
            Bytes::from_static(b"retained"),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    checkout
        .commit(
            OperationId::from_bytes([49; 16]),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    assert!(
        Fs::collect_local_garbage(
            options.clone(),
            8,
            1_024,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .is_err()
    );
    drop(checkout);
    drop(volume);
    drop(fs);

    let orphan_bytes = Bytes::from_static(b"orphan");
    let orphan = ObjectId {
        kind: ObjectKind::Blob,
        digest: object_digest(ObjectKind::Blob, &orphan_bytes),
    };
    let objects = crate::local::LocalObjectStore::open(directory.path(), 64 * 1024 * 1024)?;
    ObjectStore::put(&objects, orphan, orphan_bytes, WorkBudget::UNBOUNDED)?;
    drop(objects);
    let collected =
        Fs::collect_local_garbage(options, 8, 1_024, WorkBudget::UNBOUNDED, &cancellation).await?;
    assert!(collected.value.removed >= 1);

    let reopened = Fs::local(LocalOptions::new(directory.path()))?;
    let volume = reopened
        .open_volume(volume_id, WorkBudget::UNBOUNDED, &cancellation)
        .await?;
    let mut checkout = volume
        .value
        .checkout(
            GenerationSelector::Head,
            pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    let retained = checkout
        .value
        .read_file_range(
            &file,
            ByteRange {
                offset: 0,
                length: 8,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    assert_eq!(retained.value.bytes.as_ref(), b"retained");
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn named_attributes_are_sparse_bounded_and_atomic_with_metadata()
-> Result<(), Box<dyn std::error::Error>> {
    macro_rules! ready {
        ($future:expr) => {
            poll_ready($future).ok_or("filesystem future blocked")??
        };
    }
    let cancellation = CancellationToken::new();
    let fs = Fs::memory();
    let volume = ready!(fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation,));
    let mut checkout = ready!(volume.value.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let path = path("attributes.bin")?;
    ready!(checkout.value.create_file(
        path.clone(),
        Bytes::new(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let name = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.acyclic".to_vec(),
        128,
    )?;
    assert!(
        ready!(checkout.value.read_named_attribute(
            &path,
            &name,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value
        .is_none()
    );
    let empty_listing = ready!(checkout.value.list_named_attributes(
        &path,
        None,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert!(empty_listing.value.entries.is_empty());
    assert!(!empty_listing.value.has_more);
    let absent_remove = poll_ready(checkout.value.remove_named_attribute(
        path.clone(),
        name.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("attribute-free removal blocked")?
    .err()
    .ok_or("attribute-free removal unexpectedly succeeded")?;
    assert!(matches!(absent_remove.error, FsError::NotFound));
    let absent_replace = poll_ready(checkout.value.write_named_attribute(
        path.clone(),
        name.clone(),
        Bytes::from_static(b"replacement"),
        NamedAttributeWriteMode::Replace,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("absent attribute replacement blocked")?
    .err()
    .ok_or("absent attribute replacement unexpectedly succeeded")?;
    assert!(matches!(absent_replace.error, FsError::CreationRejected));
    ready!(checkout.value.write_named_attribute(
        path.clone(),
        name.clone(),
        Bytes::from_static(b"first"),
        NamedAttributeWriteMode::Create,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let duplicate_create = poll_ready(checkout.value.write_named_attribute(
        path.clone(),
        name.clone(),
        Bytes::from_static(b"duplicate"),
        NamedAttributeWriteMode::Create,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("duplicate attribute creation blocked")?
    .err()
    .ok_or("duplicate attribute creation unexpectedly succeeded")?;
    assert!(matches!(duplicate_create.error, FsError::CreationRejected));
    assert_eq!(
        ready!(checkout.value.read_named_attribute(
            &path,
            &name,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value
        .as_deref(),
        Some(b"first".as_slice())
    );
    let listing = ready!(checkout.value.list_named_attributes(
        &path,
        None,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(listing.value.entries.len(), 1);
    assert_eq!(listing.value.entries[0].name, name);
    assert!(!listing.value.has_more);
    ready!(checkout.value.write_named_attribute(
        path.clone(),
        name.clone(),
        Bytes::from_static(b"second"),
        NamedAttributeWriteMode::Replace,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    ready!(checkout.value.remove_named_attribute(
        path.clone(),
        name.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert!(
        ready!(checkout.value.read_named_attribute(
            &path,
            &name,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value
        .is_none()
    );
    let removed_listing = ready!(checkout.value.list_named_attributes(
        &path,
        None,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert!(removed_listing.value.entries.is_empty());
    assert!(!removed_listing.value.has_more);
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn generation_diff_is_semantic_bounded_and_equal_root_constant_work()
-> Result<(), Box<dyn std::error::Error>> {
    macro_rules! ready {
        ($future:expr) => {
            poll_ready($future).ok_or("filesystem future blocked")??
        };
    }
    let cancellation = CancellationToken::new();
    let fs = Fs::memory();
    let volume = ready!(fs.create_volume_with_id(
        VolumeId::from_bytes([73; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let mut checkout = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let initial = checkout.generation_id();
    ready!(checkout.create_file(
        path("changed")?,
        Bytes::from_static(b"one"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let first =
        match ready!(checkout.commit(OperationId::new(), WorkBudget::UNBOUNDED, &cancellation,))
            .value
        {
            CheckoutCommitOutcome::Committed { generation_id, .. } => generation_id,
            outcome => return Err(format!("unexpected first commit: {outcome:?}").into()),
        };
    let invalid = poll_ready(volume.diff_generations(
        initial,
        first,
        0,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("zero-bound generation diff blocked")?
    .err()
    .ok_or("zero-bound generation diff unexpectedly succeeded")?;
    assert!(matches!(invalid.error, FsError::InvalidDiff));
    assert_eq!(*invalid.work, WorkCounters::default());
    ready!(checkout.write_file(
        path("changed")?,
        0,
        Bytes::from_static(b"two"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let second =
        match ready!(checkout.commit(OperationId::new(), WorkBudget::UNBOUNDED, &cancellation,))
            .value
        {
            CheckoutCommitOutcome::Committed { generation_id, .. } => generation_id,
            outcome => return Err(format!("unexpected second commit: {outcome:?}").into()),
        };

    let created =
        ready!(volume.diff_generations(initial, first, 16, WorkBudget::UNBOUNDED, &cancellation,));
    assert!(!created.value.truncated);
    assert!(created.value.files.len() >= 2);
    assert_eq!(created.value.bindings.len(), 1);
    let bounded =
        ready!(volume.diff_generations(initial, first, 1, WorkBudget::UNBOUNDED, &cancellation,));
    assert!(bounded.value.truncated);
    assert_eq!(bounded.value.files.len(), 1);
    assert!(bounded.value.bindings.is_empty());
    let modified =
        ready!(volume.diff_generations(first, second, 16, WorkBudget::UNBOUNDED, &cancellation,));
    assert_eq!(modified.value.files.len(), 1);
    assert!(modified.value.bindings.is_empty());
    let equal =
        ready!(volume.diff_generations(second, second, 16, WorkBudget::UNBOUNDED, &cancellation,));
    assert!(equal.value.files.is_empty());
    assert_eq!(equal.work, WorkCounters::default());

    let root_metadata =
        ready!(checkout.lookup_no_follow(&root_path()?, WorkBudget::UNBOUNDED, &cancellation,))
            .value
            .record
            .ok_or("diff fixture root is absent")?
            .metadata;
    ready!(checkout.mutate(
        vec![Mutation::Create {
            path: path("ordered-last")?,
            record: FileRecord {
                file_id: FileId::from_bytes([0xff; 16]),
                kind: FileKind::Regular,
                link_count: 1,
                metadata: root_metadata,
                payload: FilePayload::InlineRegular(InlineFileData::new(b"last")?),
            },
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let third =
        match ready!(checkout.commit(OperationId::new(), WorkBudget::UNBOUNDED, &cancellation,))
            .value
        {
            CheckoutCommitOutcome::Committed { generation_id, .. } => generation_id,
            outcome => return Err(format!("unexpected ordered diff commit: {outcome:?}").into()),
        };
    for maximum in [1_u32, 2] {
        let bounded = ready!(volume.diff_generations(
            second,
            third,
            maximum,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ));
        assert!(bounded.value.truncated);
        assert_eq!(
            bounded.value.files.len() + bounded.value.bindings.len(),
            usize::try_from(maximum)?
        );
    }
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn three_way_merge_combines_independent_directory_bindings_and_publishes()
-> Result<(), Box<dyn std::error::Error>> {
    macro_rules! ready {
        ($future:expr) => {
            poll_ready($future).ok_or("filesystem future blocked")??
        };
    }
    let cancellation = CancellationToken::new();
    let fs = Fs::memory();
    let volume = ready!(fs.create_volume_with_id(
        VolumeId::from_bytes([74; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let mut ours = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let mut theirs = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    ready!(ours.create_file(
        path("ours")?,
        Bytes::from_static(b"ours"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    ready!(theirs.create_file(
        path("theirs")?,
        Bytes::from_static(b"theirs"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let theirs_generation = match ready!(theirs.commit(
        OperationId::new(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value
    {
        CheckoutCommitOutcome::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected branch commit: {outcome:?}").into()),
    };
    let prepared = ready!(ours.prepare_merge(
        theirs_generation,
        32,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert!(matches!(prepared.value, MergePreparation::Prepared { .. }));
    let mutation_during_merge =
        poll_ready(ours.mutate(Vec::new(), WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("prepared merge mutation guard blocked")?
            .err()
            .ok_or("prepared merge accepted a mutation")?;
    assert!(matches!(
        mutation_during_merge.error,
        FsError::PreparedMergePending
    ));
    assert!(
        poll_ready(ours.prepare_merge(
            theirs_generation,
            32,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("duplicate merge preparation blocked")?
        .is_err()
    );
    assert!(
        poll_ready(ours.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("prepared merge rebase blocked")?
            .is_err()
    );
    let merged_generation = match ready!(ours.commit(
        OperationId::new(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value
    {
        CheckoutCommitOutcome::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected merge commit: {outcome:?}").into()),
    };
    let mut merged = ready!(volume.checkout(
        GenerationSelector::Exact(merged_generation),
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    assert!(
        ready!(merged.lookup_no_follow(&path("ours")?, WorkBudget::UNBOUNDED, &cancellation,))
            .value
            .record
            .is_some()
    );
    assert!(
        ready!(merged.lookup_no_follow(&path("theirs")?, WorkBudget::UNBOUNDED, &cancellation,))
            .value
            .record
            .is_some()
    );
    assert_eq!(merged.root().parents.len(), 2);

    let mut left = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let mut right = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    ready!(left.hard_link(
        path("ours")?,
        path("left-link")?,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    ready!(right.hard_link(
        path("ours")?,
        path("right-link")?,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let right_generation =
        match ready!(right.commit(OperationId::new(), WorkBudget::UNBOUNDED, &cancellation,)).value
        {
            CheckoutCommitOutcome::Committed { generation_id, .. } => generation_id,
            outcome => return Err(format!("unexpected hard-link commit: {outcome:?}").into()),
        };
    assert!(matches!(
        ready!(left.prepare_merge(
            right_generation,
            32,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value,
        MergePreparation::Prepared { .. }
    ));
    let links_generation = match ready!(left.commit(
        OperationId::new(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value
    {
        CheckoutCommitOutcome::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected hard-link merge: {outcome:?}").into()),
    };
    let mut links = ready!(volume.checkout(
        GenerationSelector::Exact(links_generation),
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let source =
        ready!(links.lookup_no_follow(&path("ours")?, WorkBudget::UNBOUNDED, &cancellation,))
            .value
            .record
            .ok_or("merged hard-link source missing")?;
    assert_eq!(source.link_count, 3);
    for linked in ["left-link", "right-link"] {
        let record =
            ready!(links.lookup_no_follow(&path(linked)?, WorkBudget::UNBOUNDED, &cancellation,))
                .value
                .record
                .ok_or("merged hard link missing")?;
        assert_eq!(record.file_id, source.file_id);
    }
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn three_way_merge_rejects_guards_and_reports_exact_file_conflicts()
-> Result<(), Box<dyn std::error::Error>> {
    macro_rules! ready {
        ($future:expr) => {
            poll_ready($future).ok_or("filesystem future blocked")??
        };
    }
    let cancellation = CancellationToken::new();
    let fs = Fs::memory();
    let volume = ready!(fs.create_volume_with_id(
        VolumeId::from_bytes([124; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let mut seed = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let file = path("conflict.txt")?;
    let file_id = ready!(seed.create_file(
        file.clone(),
        Bytes::from_static(b"base"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let second_file = path("second-conflict.txt")?;
    let second_file_id = ready!(seed.create_file(
        second_file.clone(),
        Bytes::from_static(b"base-two"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let base_generation = match ready!(seed.commit(
        OperationId::from_bytes([125; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value
    {
        CheckoutCommitOutcome::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected seed commit: {outcome:?}").into()),
    };
    let mut ours = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let mut theirs = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let mut read_only = ready!(volume.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    assert!(
        poll_ready(read_only.prepare_merge(
            base_generation,
            8,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("read-only merge preparation blocked")?
        .is_err()
    );
    assert!(
        poll_ready(
            ours.prepare_merge(base_generation, 0, 8, WorkBudget::UNBOUNDED, &cancellation,)
        )
        .ok_or("zero-change merge preparation blocked")?
        .is_err()
    );
    assert!(
        poll_ready(
            ours.prepare_merge(base_generation, 8, 0, WorkBudget::UNBOUNDED, &cancellation,)
        )
        .ok_or("zero-conflict merge preparation blocked")?
        .is_err()
    );
    ready!(ours.write_file(
        file.clone(),
        0,
        Bytes::from_static(b"ours"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    ready!(ours.write_file(
        second_file.clone(),
        0,
        Bytes::from_static(b"ours-two"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    ready!(theirs.write_file(
        file,
        0,
        Bytes::from_static(b"them"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    ready!(theirs.write_file(
        second_file,
        0,
        Bytes::from_static(b"them-two"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let theirs_generation = match ready!(theirs.commit(
        OperationId::from_bytes([126; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value
    {
        CheckoutCommitOutcome::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected competing commit: {outcome:?}").into()),
    };
    assert!(
        poll_ready(
            ours.prepare_merge(base_generation, 8, 8, WorkBudget::UNBOUNDED, &cancellation,)
        )
        .ok_or("stale merge parent check blocked")?
        .is_err()
    );
    let change_limited = poll_ready(ours.prepare_merge(
        theirs_generation,
        1,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("change-limited merge preparation blocked")?
    .err()
    .ok_or("truncated merge inputs unexpectedly prepared")?;
    assert!(matches!(change_limited.error, FsError::MergeChangeLimit));
    assert!(change_limited.work.backend_read_operations > 0);
    let bounded_conflicts = ready!(ours.prepare_merge(
        theirs_generation,
        8,
        1,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert!(matches!(
        bounded_conflicts.value,
        MergePreparation::Conflicted {
            ref conflicts,
            truncated: true,
        } if conflicts.len() == 1
            && matches!(conflicts[0], MergeConflict::File(id) if id == file_id || id == second_file_id)
    ));
    let conflicted = ready!(ours.prepare_merge(
        theirs_generation,
        8,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert!(matches!(
        conflicted.value,
        MergePreparation::Conflicted {
            ref conflicts,
            truncated: false,
        } if conflicts.len() == 2
            && conflicts.contains(&MergeConflict::File(file_id))
            && conflicts.contains(&MergeConflict::File(second_file_id))
    ));
    assert!(ours.has_pending_mutations());
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn directory_merge_helper_covers_scalar_binding_limit_and_invalid_diff_matrix()
-> Result<(), Box<dyn std::error::Error>> {
    let store = crate::memory::MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let limits = DecodeLimits::default();
    let directory_id = FileId::from_bytes([80; 16]);
    let metadata = |byte| ObjectId {
        kind: ObjectKind::Metadata,
        digest: Digest::from_bytes([byte; 32]),
    };
    let put_tree = |page: TreePage| -> Result<ObjectId, Box<dyn std::error::Error>> {
        let bytes = Bytes::from(encode_tree_page(&page, 16)?);
        let object_id = ObjectId {
            kind: ObjectKind::TreePage,
            digest: object_digest(ObjectKind::TreePage, &bytes),
        };
        ObjectStore::put(&store, object_id, bytes, WorkBudget::UNBOUNDED)?;
        Ok(object_id)
    };
    let empty = put_tree(TreePage::Leaf(Vec::new()))?;
    let name = LogicalName::new(NameEncoding::Utf8, b"entry".to_vec(), 128)?;
    let entry = |byte| crate::kernel::TreeEntry {
        name: name.clone(),
        file_id: FileId::from_bytes([byte; 16]),
        kind: FileKind::Regular,
    };
    let base_entries = put_tree(TreePage::Leaf(vec![entry(81)]))?;
    let ours_entries = put_tree(TreePage::Leaf(vec![entry(82)]))?;
    let theirs_entries = put_tree(TreePage::Leaf(vec![entry(83)]))?;
    let directory = |entries, metadata, link_count| FileRecord {
        file_id: directory_id,
        kind: FileKind::Directory,
        link_count,
        metadata,
        payload: FilePayload::Directory { entries },
    };
    let base = directory(base_entries, metadata(84), 1);

    let scalar = poll_ready(merge_directory_record(
        &store,
        directory_id,
        base,
        directory(base_entries, metadata(85), 1),
        directory(base_entries, metadata(86), 1),
        8,
        1,
        limits,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("scalar directory merge blocked")??;
    assert_eq!(
        scalar.value.conflicts,
        vec![MergeConflict::File(directory_id)]
    );
    assert!(scalar.value.record.is_none());
    assert!(!scalar.value.truncated);

    let truncated_scalar = poll_ready(merge_directory_record(
        &store,
        directory_id,
        base,
        directory(base_entries, metadata(84), 2),
        directory(base_entries, metadata(84), 3),
        8,
        0,
        limits,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("truncated scalar directory merge blocked")??;
    assert!(truncated_scalar.value.conflicts.is_empty());
    assert!(truncated_scalar.value.record.is_none());
    assert!(truncated_scalar.value.truncated);

    let invalid = poll_ready(merge_directory_record(
        &store,
        directory_id,
        base,
        helper_record(directory_id, FilePayload::Empty, FileKind::Fifo),
        base,
        8,
        1,
        limits,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("invalid directory merge blocked")?
    .err()
    .ok_or("invalid directory payload merged")?;
    assert!(matches!(invalid.error, MergeGenerationError::InvalidDiff));

    let binding_conflict = poll_ready(merge_directory_record(
        &store,
        directory_id,
        base,
        directory(ours_entries, metadata(84), 1),
        directory(theirs_entries, metadata(84), 1),
        8,
        1,
        limits,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("binding-conflict directory merge blocked")??;
    assert_eq!(
        binding_conflict.value.conflicts,
        vec![MergeConflict::Binding {
            directory_id,
            name: name.clone(),
        }]
    );
    assert!(binding_conflict.value.record.is_none());
    assert!(!binding_conflict.value.truncated);

    let binding_truncated = poll_ready(merge_directory_record(
        &store,
        directory_id,
        base,
        directory(ours_entries, metadata(84), 1),
        directory(theirs_entries, metadata(84), 1),
        8,
        0,
        limits,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("truncated binding directory merge blocked")??;
    assert!(binding_truncated.value.conflicts.is_empty());
    assert!(binding_truncated.value.truncated);

    let unchanged = poll_ready(merge_directory_record(
        &store,
        directory_id,
        directory(empty, metadata(84), 1),
        directory(empty, metadata(84), 1),
        directory(empty, metadata(84), 1),
        8,
        1,
        limits,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("unchanged directory merge blocked")??;
    assert_eq!(
        unchanged
            .value
            .record
            .and_then(|record| directory_entries(Some(record))),
        Some(empty)
    );
    assert!(unchanged.value.conflicts.is_empty());

    let prior = WorkCounters {
        object_probes: 1,
        ..WorkCounters::default()
    };
    let spent = WorkCounters {
        items_examined: 1,
        ..WorkCounters::default()
    };
    let mapped = map_diff_failure(
        OperationFailure::new(PersistentDiffError::WrongRootKind, spent),
        prior,
    );
    assert!(matches!(mapped.error, FsError::InvalidDiff));
    assert_eq!(mapped.work.object_probes, 1);
    assert_eq!(mapped.work.items_examined, 1);
    assert!(matches!(
        map_diff_failure(
            OperationFailure::new(PersistentDiffError::InvalidLimit, spent),
            prior,
        )
        .error,
        FsError::InvalidDiff
    ));
    assert!(matches!(
        map_diff_failure(
            OperationFailure::new(PersistentDiffError::AllocationFailed, spent),
            prior,
        )
        .error,
        FsError::DiffAllocationFailed
    ));
    assert!(matches!(
        map_diff_failure(
            OperationFailure::new(
                PersistentDiffError::Storage(ObjectStoreError::Corrupt),
                spent,
            ),
            prior,
        )
        .error,
        FsError::Object(ObjectStoreError::Corrupt)
    ));
    assert!(matches!(
        map_diff_failure(
            OperationFailure::new(
                PersistentDiffError::Decode(CanonicalDecodeError::TrailingBytes),
                spent,
            ),
            prior,
        )
        .error,
        FsError::Decode(CanonicalDecodeError::TrailingBytes)
    ));
    assert!(matches!(
        map_diff_failure(
            OperationFailure::new(PersistentDiffError::Work(WorkError::Overflow), spent),
            prior,
        )
        .error,
        FsError::Work(WorkError::Overflow)
    ));
    assert!(matches!(
        map_diff_failure(
            OperationFailure::new(PersistentDiffError::Cancelled, spent),
            prior,
        )
        .error,
        FsError::Cancelled(CancellationError)
    ));
    Ok(())
}

fn validation_mutation(value: &str) -> Result<Mutation, Box<dyn std::error::Error>> {
    Ok(Mutation::ValidateRegular { path: path(value)? })
}

#[test]
fn pending_mutation_retention_accounts_transfer_growth_and_bounds()
-> Result<(), Box<dyn std::error::Error>> {
    let baseline = WorkCounters {
        object_probes: 1,
        ..WorkCounters::default()
    };
    let first = validation_mutation("first")?;
    let second = validation_mutation("second")?;

    let mut empty = Vec::new();
    let transferred = retain_pending_operations(
        &mut empty,
        vec![first.clone()],
        4,
        baseline,
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(empty, vec![first.clone()]);
    assert_eq!(transferred, baseline);

    let mut reserved = Vec::with_capacity(4);
    reserved.push(first.clone());
    let retained = retain_pending_operations(
        &mut reserved,
        vec![second.clone()],
        4,
        baseline,
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(reserved, vec![first.clone(), second.clone()]);
    assert_eq!(retained.allocation_operations, 0);
    assert!(retained.bytes_copied > 0);

    let mut growing = Vec::with_capacity(1);
    growing.push(first.clone());
    let grown = retain_pending_operations(
        &mut growing,
        vec![second.clone()],
        4,
        baseline,
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(growing, vec![first.clone(), second.clone()]);
    assert_eq!(grown.allocation_operations, 1);
    assert!(grown.peak_allocation_bytes > 0);
    assert!(grown.bytes_copied > retained.bytes_copied);

    let mut excessive = vec![first.clone()];
    let bounded = retain_pending_operations(
        &mut excessive,
        vec![second.clone()],
        1,
        baseline,
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("pending mutation bound was ignored")?;
    assert!(matches!(bounded.error, FsError::TooManyPendingMutations));
    assert_eq!(excessive, vec![first.clone()]);

    let mut budgeted = Vec::with_capacity(2);
    budgeted.push(first.clone());
    let mut no_copy = WorkBudget::UNBOUNDED;
    no_copy.bytes_copied = 0;
    let rejected = retain_pending_operations(&mut budgeted, vec![second], 4, baseline, no_copy)
        .err()
        .ok_or("pending mutation copy escaped its budget")?;
    assert!(matches!(
        rejected.error,
        FsError::Work(WorkError::BudgetExceeded {
            counter: "bytes_copied",
            ..
        })
    ));
    assert_eq!(budgeted, vec![first]);
    Ok(())
}

#[test]
fn merge_and_transfer_error_adapters_preserve_every_typed_class() {
    let prior = WorkCounters {
        object_probes: 1,
        ..WorkCounters::default()
    };
    let nested = WorkCounters {
        items_examined: 1,
        ..WorkCounters::default()
    };
    let mapped = [
        map_merge_failure(
            OperationFailure::new(MergeGenerationError::InvalidDiff, nested),
            prior,
        ),
        map_merge_failure(
            OperationFailure::new(MergeGenerationError::ChangeLimit, nested),
            prior,
        ),
        map_merge_failure(
            OperationFailure::new(MergeGenerationError::AllocationFailed, nested),
            prior,
        ),
        map_merge_failure(
            OperationFailure::new(
                MergeGenerationError::Object(ObjectStoreError::Corrupt),
                nested,
            ),
            prior,
        ),
        map_merge_failure(
            OperationFailure::new(
                MergeGenerationError::Decode(CanonicalDecodeError::TrailingBytes),
                nested,
            ),
            prior,
        ),
        map_merge_failure(
            OperationFailure::new(
                MergeGenerationError::FileTable(FileTableMutationError::Missing),
                nested,
            ),
            prior,
        ),
        map_merge_failure(
            OperationFailure::new(
                MergeGenerationError::Tree(TreeMutationError::Missing),
                nested,
            ),
            prior,
        ),
        map_merge_failure(
            OperationFailure::new(
                MergeGenerationError::Checkpoint(CheckpointError::DuplicateParent),
                nested,
            ),
            prior,
        ),
        map_merge_failure(
            OperationFailure::new(MergeGenerationError::Cancelled(CancellationError), nested),
            prior,
        ),
        map_merge_failure(
            OperationFailure::new(MergeGenerationError::Work(WorkError::Overflow), nested),
            prior,
        ),
    ];
    assert!(matches!(mapped[0].error, FsError::InvalidDiff));
    assert!(matches!(mapped[1].error, FsError::MergeChangeLimit));
    assert!(matches!(mapped[2].error, FsError::DiffAllocationFailed));
    assert!(matches!(mapped[3].error, FsError::Object(_)));
    assert!(matches!(mapped[4].error, FsError::Decode(_)));
    assert!(matches!(mapped[5].error, FsError::MergeFileTable(_)));
    assert!(matches!(mapped[6].error, FsError::MergeTree(_)));
    assert!(matches!(mapped[7].error, FsError::Checkpoint(_)));
    assert!(matches!(mapped[8].error, FsError::Cancelled(_)));
    assert!(matches!(mapped[9].error, FsError::Work(_)));
    assert!(
        mapped
            .iter()
            .all(|failure| failure.work.object_probes == 1 && failure.work.items_examined == 1)
    );
}

#[test]
fn transfer_error_adapter_preserves_every_typed_class() {
    let transfer = [
        map_transfer_error(GenerationTransferError::Cancelled(CancellationError)),
        map_transfer_error(GenerationTransferError::Manifest(
            GenerationExportManifestError::WrongRootKind,
        )),
        map_transfer_error(GenerationTransferError::ManifestMismatch),
        map_transfer_error(GenerationTransferError::Closure(
            ClosureError::WrongObjectKind,
        )),
        map_transfer_error(GenerationTransferError::Object(ObjectStoreError::Corrupt)),
        map_transfer_error(GenerationTransferError::Work(WorkError::Overflow)),
        map_transfer_error(GenerationTransferError::InvalidCursor),
        map_transfer_error(GenerationTransferError::EmptyBatch),
        map_transfer_error(GenerationTransferError::TooManyObjects),
        map_transfer_error(GenerationTransferError::AllocationFailed),
    ];
    assert!(matches!(transfer[0], FsError::Cancelled(_)));
    assert!(matches!(transfer[1], FsError::InvalidExportManifest));
    assert!(matches!(transfer[2], FsError::InvalidExportManifest));
    assert!(matches!(transfer[3], FsError::Closure(_)));
    assert!(matches!(transfer[4], FsError::Object(_)));
    assert!(matches!(transfer[5], FsError::Work(_)));
    assert!(
        transfer[6..]
            .iter()
            .all(|error| matches!(error, FsError::Transfer(_)))
    );
}

#[test]
fn memory_facade_exposes_shared_cache_control_without_alternate_storage_truth()
-> Result<(), Box<dyn std::error::Error>> {
    let objects = crate::CachedObjectStore::new(
        TestMemoryObjectStore::default(),
        crate::ObjectCacheOptions::default(),
    )?;
    let fs = Fs::new(
        crate::memory::MemoryAuthorityStore::default(),
        objects,
        EmbeddedCapabilities::MEMORY,
    );
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("cached memory volume creation blocked")??
        .value;
    let checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("cached memory checkout blocked")??
    .value;
    let root = ObjectId {
        kind: ObjectKind::GenerationRoot,
        digest: checkout.generation_id().digest(),
    };

    fs.clear_object_cache()?;
    assert_eq!(fs.object_cache_stats()?.resident_entries, 0);
    let first = poll_ready(fs.export_object(
        root,
        config().limits.maximum_object_bytes,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("cold cached export blocked")??;
    let warm = poll_ready(fs.clone().export_object(
        root,
        config().limits.maximum_object_bytes,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("warm cached export blocked")??;
    assert_eq!(first.value.bytes, warm.value.bytes);
    assert_eq!(first.work.backend_read_operations, 1);
    assert_eq!(warm.work.backend_read_operations, 0);
    let populated = fs.object_cache_stats()?;
    assert_eq!(populated.resident_entries, 1);
    assert!(populated.hits >= 1);
    fs.clear_object_cache()?;
    assert_eq!(fs.object_cache_stats()?.resident_entries, 0);
    Ok(())
}

#[test]
fn detached_inline_file_seek_and_attribute_absence_are_exact()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("inline detached volume creation blocked")??
        .value;
    let file = path("inline-detached")?;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("inline detached checkout blocked")??
    .value;
    poll_ready(checkout.create_file(
        file.clone(),
        Bytes::from_static(b"abc"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("inline file creation blocked")??;
    let mut detached =
        poll_ready(checkout.detach_regular_file(&file, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("inline file detachment blocked")??
            .value;
    assert_eq!(detached.logical_bytes(), 3);
    for (offset, target, expected) in [
        (0, ExtentSeekTarget::Data, Some(0)),
        (3, ExtentSeekTarget::Data, None),
        (0, ExtentSeekTarget::Hole, Some(3)),
        (3, ExtentSeekTarget::Hole, Some(3)),
        (4, ExtentSeekTarget::Hole, None),
    ] {
        let receipt =
            poll_ready(detached.seek(offset, target, WorkBudget::UNBOUNDED, &cancellation))
                .ok_or("inline detached seek blocked")??;
        assert_eq!(receipt.value, expected);
        assert_eq!(receipt.work, WorkCounters::default());
    }

    let attribute = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.inline".to_vec(),
        config().limits.maximum_component_bytes,
    )?;
    poll_ready(detached.write_named_attribute(
        attribute.clone(),
        Bytes::from_static(b"value"),
        NamedAttributeWriteMode::Create,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("inline detached attribute creation blocked")??;
    poll_ready(detached.remove_named_attribute(
        attribute.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("inline detached attribute removal blocked")??;
    let absent =
        poll_ready(detached.read_named_attribute(&attribute, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("inline detached absent attribute read blocked")??;
    assert_eq!(absent.value, None);

    detached.record.kind = FileKind::Fifo;
    detached.record.payload = FilePayload::Empty;
    assert_eq!(detached.logical_bytes(), 0);
    let rejected = poll_ready(detached.seek(
        0,
        ExtentSeekTarget::Data,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("non-regular detached seek blocked")?
    .err()
    .ok_or("non-regular detached seek succeeded")?;
    assert!(matches!(
        rejected.error,
        FsError::FileRead(FileRangeReadError::NotRegular)
    ));
    assert_eq!(*rejected.work, WorkCounters::default());
    Ok(())
}

#[test]
fn remaining_facade_guards_are_fast_typed_and_non_mutating()
-> Result<(), Box<dyn std::error::Error>> {
    assert!(matches!(
        validate_checkout(
            CheckoutMode {
                access: AccessMode::ReadWrite,
                consistency: ConsistencyMode::Live,
                mutations: MutationMode::PrivateOverlay,
            },
            serialized_config(),
        ),
        Err(OperationFailure {
            error: FsError::UnsupportedCheckout,
            ..
        })
    ));

    let prior = WorkCounters {
        bytes_copied: u64::MAX,
        peak_allocation_bytes: 1,
        ..WorkCounters::default()
    };
    let overflow = merge_simultaneous_failure(
        prior,
        WorkCounters {
            bytes_copied: 1,
            peak_allocation_bytes: 1,
            ..WorkCounters::default()
        },
        0,
        FsError::NotFound,
    );
    assert!(matches!(overflow.error, FsError::Work(WorkError::Overflow)));
    assert_eq!(*overflow.work, prior);

    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut limited = config();
    limited.limits.maximum_read_bytes = 1;
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([211; 16]),
        limited,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("volume creation blocked")??
    .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("checkout blocked")??
    .value;

    let baseline = WorkCounters {
        object_probes: 1,
        ..WorkCounters::default()
    };
    assert_eq!(
        poll_ready(checkout.observe_base_metadata_ids(
            &[],
            baseline,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("empty metadata observation blocked")??,
        baseline
    );
    assert!(matches!(
        checkout.validate_profile_kind(FileKind::ReparsePoint, baseline),
        Err(OperationFailure {
            error: FsError::UnsupportedFileKind,
            ..
        })
    ));
    assert!(matches!(
        poll_ready(checkout.stage_blob(
            Bytes::from_static(b"xx"),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("oversized blob guard blocked")?,
        Err(OperationFailure {
            error: FsError::FileRead(FileRangeReadError::InvalidRange),
            ..
        })
    ));

    poll_ready(checkout.create_directory(path("directory")?, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("directory creation blocked")??;
    assert!(matches!(
        poll_ready(checkout.detach_regular_file(
            &path("directory")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("directory detach guard blocked")?,
        Err(OperationFailure {
            error: FsError::FileRead(FileRangeReadError::NotRegular),
            ..
        })
    ));
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn assert_authority_volume_identity_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
    let generation_root = ObjectId {
        kind: ObjectKind::GenerationRoot,
        digest: Digest::from_bytes([221; 32]),
    };
    let expected_volume = VolumeId::from_bytes([222; 16]);
    let wrong_volume = VolumeId::from_bytes([223; 16]);
    let creation = DurableCommit {
        epoch: Epoch::new(1)?,
        sequence: Sequence::new(1),
        operation_id: OperationId::from_bytes([224; 16]),
        fingerprint: Digest::from_bytes([225; 32]),
        previous_digest: Digest::ZERO,
        digest: Digest::from_bytes([226; 32]),
        payload: Bytes::from(encode_volume_created(VolumeCreated {
            volume_id: wrong_volume,
            config: config(),
            initial_generation_root: generation_root,
        })?),
    };
    assert!(matches!(
        generation_from_record(&creation, expected_volume, WorkCounters::default()),
        Err(OperationFailure {
            error: FsError::VolumeMismatch,
            ..
        })
    ));
    let publication = DurableCommit {
        sequence: Sequence::new(2),
        payload: {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(b"acyclic-fs-publish-generation-v1\0");
            bytes.extend_from_slice(&1_u16.to_le_bytes());
            bytes.extend_from_slice(&wrong_volume.into_bytes());
            bytes.push(generation_root.kind.canonical_tag());
            bytes.extend_from_slice(generation_root.digest.as_bytes());
            Bytes::from(bytes)
        },
        ..creation
    };
    assert!(matches!(
        generation_from_record(&publication, expected_volume, WorkCounters::default()),
        Err(OperationFailure {
            error: FsError::VolumeMismatch,
            ..
        })
    ));

    let authority = Arc::new(crate::memory::MemoryAuthorityStore::default());
    let fs = Fs::new(
        Arc::clone(&authority),
        crate::memory::MemoryObjectStore::default(),
        EmbeddedCapabilities::MEMORY,
    );
    let expected_authority = volume_authority_id(expected_volume);
    let active = AuthorityStore::create_authority(
        &*authority,
        expected_authority,
        Epoch::GENESIS,
        WorkBudget::UNBOUNDED,
    )?;
    let head = match active.value {
        CreateAuthorityOutcome::Created(head) | CreateAuthorityOutcome::Existing(head) => head,
    };
    let (proposal, _) = creation_commit(
        OperationId::from_bytes([227; 16]),
        encode_volume_created(VolumeCreated {
            volume_id: wrong_volume,
            config: config(),
            initial_generation_root: generation_root,
        })?,
    );
    AuthorityStore::compare_and_append(
        &*authority,
        expected_authority,
        head.epoch,
        Head::genesis(head.epoch),
        proposal,
        WorkBudget::UNBOUNDED,
    )?;
    let cancellation = CancellationToken::new();
    assert!(matches!(
        poll_ready(fs.read_creation(
            expected_volume,
            WorkCounters::default(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("authority identity replay blocked")?,
        Err(OperationFailure {
            error: FsError::VolumeMismatch,
            ..
        })
    ));

    let empty_volume = VolumeId::from_bytes([228; 16]);
    AuthorityStore::create_authority(
        &*authority,
        volume_authority_id(empty_volume),
        Epoch::GENESIS,
        WorkBudget::UNBOUNDED,
    )?;
    let empty = Volume {
        fs,
        id: empty_volume,
        config: config(),
    };
    assert!(matches!(
        poll_ready(empty.resolve_head_generation(WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("empty authority head blocked")?,
        Err(OperationFailure {
            error: FsError::EmptyAuthority,
            ..
        })
    ));
    Ok(())
}

#[test]
fn detached_payload_bounds_and_authority_volume_identity_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("volume creation blocked")??
        .value;
    let file = path("bounded-detached")?;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("checkout blocked")??
    .value;
    poll_ready(checkout.create_file(
        file.clone(),
        Bytes::new(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("file creation blocked")??;
    let attribute = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.bounded".to_vec(),
        config().limits.maximum_component_bytes,
    )?;
    poll_ready(checkout.write_named_attribute(
        file.clone(),
        attribute.clone(),
        Bytes::from_static(b"xx"),
        NamedAttributeWriteMode::Create,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("attribute write blocked")??;
    let mut detached =
        poll_ready(checkout.detach_regular_file(&file, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("file detachment blocked")??
            .value;
    detached.volume.config.limits.maximum_read_bytes = 1;
    assert!(matches!(
        poll_ready(
            detached.read_named_attribute(&attribute, WorkBudget::UNBOUNDED, &cancellation,)
        )
        .ok_or("bounded attribute read blocked")?,
        Err(OperationFailure {
            error: FsError::FileRead(FileRangeReadError::InvalidRange),
            ..
        })
    ));
    assert!(matches!(
        poll_ready(detached.stage_blob(
            Bytes::from_static(b"xx"),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("bounded detached staging blocked")?,
        Err(OperationFailure {
            error: FsError::FileRead(FileRangeReadError::InvalidRange),
            ..
        })
    ));

    assert_authority_volume_identity_fails_closed()
}

// --- Case-folding / Unicode-normalization ---
//
// `CaseSensitivity::ProfileFolded` rejects same-directory siblings whose
// names collide once case-folded (`FsError::NameCollision`) and resolves
// lookups case-insensitively; `UnicodePolicy::RequireNfc` rejects any new
// name that is not canonical NFC (`FsError::NonNormalizedName`). Both are
// admitted at volume creation (pinned above).

fn folded_config() -> VolumeConfig {
    VolumeConfig {
        case_sensitivity: CaseSensitivity::ProfileFolded,
        ..config()
    }
}

fn nfc_config() -> VolumeConfig {
    VolumeConfig {
        unicode: UnicodePolicy::RequireNfc,
        ..config()
    }
}

#[test]
fn profile_folded_volumes_can_be_created_and_checked_out() -> Result<(), Box<dyn std::error::Error>>
{
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume =
        poll_ready(fs.create_volume(folded_config(), WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("create blocked")??
            .value;
    poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("checkout blocked")??;
    Ok(())
}

#[test]
fn require_nfc_volumes_can_be_created_and_checked_out() -> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume(nfc_config(), WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("create blocked")??
        .value;
    poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("checkout blocked")??;
    Ok(())
}

#[test]
fn profile_folded_volume_rejects_case_only_collisions() -> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume =
        poll_ready(fs.create_volume(folded_config(), WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("create blocked")??
            .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("checkout blocked")??
    .value;
    poll_ready(checkout.create_file(
        path("Foo")?,
        Bytes::from_static(b"first"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("first create blocked")??;
    let collision = poll_ready(checkout.create_file(
        path("foo")?,
        Bytes::from_static(b"second"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("second create blocked")?;
    assert!(
        collision.is_err(),
        "creating \"foo\" must collide with the existing \"Foo\" under ProfileFolded"
    );
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn profile_folded_admission_covers_directories_links_and_renames()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume =
        poll_ready(fs.create_volume(folded_config(), WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("create blocked")??
            .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("checkout blocked")??
    .value;

    poll_ready(checkout.create_directory(path("Directory")?, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("directory create blocked")??;
    let directory_collision = poll_ready(checkout.create_directory(
        path("directory")?,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("directory collision blocked")?
    .err()
    .ok_or("directory collision unexpectedly succeeded")?;
    assert!(matches!(directory_collision.error, FsError::NameCollision));

    poll_ready(checkout.create_symbolic_link(
        path("Link")?,
        Bytes::from_static(b"target"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("link create blocked")??;
    let link_collision = poll_ready(checkout.create_symbolic_link(
        path("link")?,
        Bytes::from_static(b"other"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("link collision blocked")?
    .err()
    .ok_or("link collision unexpectedly succeeded")?;
    assert!(matches!(link_collision.error, FsError::NameCollision));

    let source = poll_ready(checkout.create_file(
        path("Source")?,
        Bytes::from_static(b"body"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("source create blocked")??
    .value;
    poll_ready(checkout.rename(
        path("Source")?,
        path("SOURCE")?,
        false,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("case-only rename blocked")??;
    let renamed = poll_ready(checkout.lookup_no_follow(
        &path("source")?,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("renamed lookup blocked")??
    .value
    .record
    .ok_or("renamed source absent")?;
    assert_eq!(renamed.file_id, source);

    poll_ready(checkout.create_file(
        path("Other")?,
        Bytes::from_static(b"other"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("other create blocked")??;
    let rename_collision = poll_ready(checkout.rename(
        path("SOURCE")?,
        path("other")?,
        false,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("rename collision blocked")?
    .err()
    .ok_or("rename collision unexpectedly succeeded")?;
    assert!(matches!(rename_collision.error, FsError::NameCollision));

    let link_destination_collision = poll_ready(checkout.hard_link(
        path("SOURCE")?,
        path("source")?,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("same-fold link blocked")?
    .err()
    .ok_or("same-fold link unexpectedly succeeded")?;
    assert!(matches!(
        link_destination_collision.error,
        FsError::NameCollision
    ));

    poll_ready(checkout.create_file(
        path("Removed")?,
        Bytes::from_static(b"removed"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("removed create blocked")??;
    poll_ready(checkout.remove(path("Removed")?, None, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("removed delete blocked")??;
    poll_ready(checkout.create_file(
        path("removed")?,
        Bytes::from_static(b"reused"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("reused folded name blocked")??;
    Ok(())
}

#[test]
fn profile_folded_authored_batches_reject_intra_batch_collisions()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume =
        poll_ready(fs.create_volume(folded_config(), WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("create blocked")??
            .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("checkout blocked")??
    .value;
    let failure = poll_ready(checkout.apply_authored_transaction(
        vec![
            AuthoredMutation::CreateDirectory {
                path: path("Batch")?,
                metadata: empty_metadata(),
            },
            AuthoredMutation::CreateFile {
                path: path("batch")?,
                bytes: Bytes::from_static(b"body"),
                metadata: empty_metadata(),
            },
        ],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("authored collision blocked")?
    .err()
    .ok_or("authored collision unexpectedly succeeded")?;
    assert!(matches!(failure.error, FsError::NameCollision));
    assert!(!checkout.has_pending_mutations());

    let ordered_aliases = poll_ready(checkout.apply_authored_transaction(
        vec![
            AuthoredMutation::CreateFile {
                path: path("Source")?,
                bytes: Bytes::from_static(b"source"),
                metadata: empty_metadata(),
            },
            AuthoredMutation::Rename {
                source: path("source")?,
                destination: path("Renamed")?,
                replace: false,
            },
        ],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("folded authored alias batch blocked")??;
    let created_id = ordered_aliases.value.created_file_ids[0].ok_or("created id missing")?;
    let renamed = poll_ready(checkout.lookup_no_follow(
        &path("renamed")?,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("folded authored alias lookup blocked")??;
    assert_eq!(
        renamed
            .value
            .record
            .ok_or("renamed record missing")?
            .file_id,
        created_id
    );

    let nested = poll_ready(checkout.apply_authored_transaction(
        vec![
            AuthoredMutation::CreateDirectory {
                path: path("Nested")?,
                metadata: empty_metadata(),
            },
            AuthoredMutation::CreateFile {
                path: path_parts(&["nested", "Child"])?,
                bytes: Bytes::from_static(b"nested"),
                metadata: empty_metadata(),
            },
        ],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("nested folded authored batch blocked")??;
    assert_eq!(nested.value.created_file_ids.len(), 2);
    let nested_read = poll_ready(checkout.read_file_range(
        &path_parts(&["NESTED", "child"])?,
        ByteRange {
            offset: 0,
            length: 6,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("nested folded lookup blocked")??;
    assert_eq!(nested_read.value.bytes.as_ref(), b"nested");
    Ok(())
}

#[test]
fn sensitive_volume_still_permits_case_distinct_siblings() -> Result<(), Box<dyn std::error::Error>>
{
    // Control: default `Sensitive` behavior must not regress while the
    // ProfileFolded tests above are red.
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("create blocked")??
        .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("checkout blocked")??
    .value;
    poll_ready(checkout.create_file(
        path("Foo")?,
        Bytes::from_static(b"first"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("first create blocked")??;
    poll_ready(checkout.create_file(
        path("foo")?,
        Bytes::from_static(b"second"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("second create blocked")??;
    Ok(())
}

#[test]
fn profile_folded_volume_resolves_case_insensitive_lookups()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume =
        poll_ready(fs.create_volume(folded_config(), WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("create blocked")??
            .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("checkout blocked")??
    .value;
    let created = poll_ready(checkout.create_file(
        path("foo")?,
        Bytes::from_static(b"body"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("create blocked")??
    .value;
    let lookup =
        poll_ready(checkout.lookup_no_follow(&path("FOO")?, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("lookup blocked")??
            .value;
    assert_eq!(
        lookup.record.map(|record| record.file_id),
        Some(created),
        "a folded lookup of \"FOO\" must resolve the entry created as \"foo\""
    );
    Ok(())
}

#[test]
fn require_nfc_volume_rejects_non_normalized_names() -> Result<(), Box<dyn std::error::Error>> {
    // "é" spelled two ways: precomposed NFC (U+00E9) and decomposed NFD
    // ('e' U+0065 followed by combining acute accent U+0301). Both encode to
    // the same visible grapheme; only the NFC form is canonical.
    let precomposed = "\u{00e9}";
    let decomposed = "e\u{0301}";
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume(nfc_config(), WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("create blocked")??
        .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("checkout blocked")??
    .value;
    let rejected = poll_ready(checkout.create_file(
        path(decomposed)?,
        Bytes::from_static(b"body"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("nfd create blocked")?;
    assert!(
        rejected.is_err(),
        "a non-normalized (NFD) name must be rejected under RequireNfc"
    );
    poll_ready(checkout.create_file(
        path(precomposed)?,
        Bytes::from_static(b"body"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("nfc create blocked")??;
    Ok(())
}
