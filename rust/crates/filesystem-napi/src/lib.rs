//! Generated-language native embedding boundary for the canonical Rust engine.

use acyclic_fs::kernel::{
    AttributeClass, AttributeName, DecodeLimits, ExtentKind, ExtentSeekTarget, FileKind,
    FileMetadata, FilePayload, FileRecord, LogicalName, NameEncoding, NamespacePath,
    RebaseDecision, TransferCursor, TreeEntry, decode_file_metadata, encode_file_metadata,
};
use acyclic_fs::model::{
    AccessMode, CaseSensitivity, CheckoutMode, ConcurrencyMode, ConsistencyMode, FilesystemProfile,
    GenerationSelector, Lifecycle, MutationMode, UnicodePolicy, VolumeConfig, VolumeLimits,
};
use acyclic_fs::path::PortablePath;
use acyclic_fs::{
    ApplyOptions, AuthoredMutation, ByteRange, CancellationToken, ChangeSet, CheckoutCommitOutcome,
    Digest, FileCloneRequest, FileId, ForkOptions, Generation, GenerationExportManifest,
    IdempotencyKey, JoinHistory, JoinOutcome, JoinPlan, LiveMutationOutcome, LocalAuthorityBackend,
    LocalFs, LocalObjectBackend, LocalOptions, LocalVolume, MergeConflict, MergePreparation,
    NamedAttributeWriteMode, NativeWatch as FsNativeWatch, NativeWatchOptions, ObjectCacheOptions,
    ObjectId, ObjectKind, ObjectReadRequest, ObjectResidency, OperationId, PromotionAdmission,
    PromotionDestination, PromotionRejection, PromotionSpeculatorOptions, ResidencyAdmission,
    ResidencyHint, ResidencyReason, ResidencyRejection, ResidencySpeculatorOptions,
    SpeculationController, SpeculationOptions, StorageLocationId, StorageTier, Transaction,
    TransactionCommit, TransactionConflict, TransactionConflictRegion, TransactionDependencyUse,
    TransactionRebase, TransactionSparseSeek, VolumeId, WatchBatch, WatchChange,
    WatchInvalidationReason, WorkBudget, Workspace, WorkspaceDelete, WorkspaceDirectoryPage,
    WorkspaceExtentKind, WorkspaceExtentPlan, WorkspaceMetadata, WorkspaceRebase, WorkspaceStat,
    decode_generation_export_manifest, encode_generation_export_manifest,
    native_watch_capabilities as sdk_native_watch_capabilities,
};
use acyclic_fs::{
    CaptureOptions, CaptureReceipt, CheckoutMountSource, MaterializeOptions, NativeMountRequest,
    NativeMountSession, SharedCheckout, WatchCaptureReceipt, capture_baseline, capture_paths,
    capture_root_identity, capture_watch_batch, materialize_checkout, mount_native,
    probe_native_mount, seal_checkout,
};
use acyclic_fs::{Mount as WorkspaceMount, MountOptions, MountPublication};
use acyclic_fs::{ReconcileOutcome, SourceMode, SourceOptions, SourceState};
use napi::bindgen_prelude::{AsyncTask, BigInt, Buffer, Error, Result, Status};
use napi::{Env, Task};
use napi_derive::napi;
use std::path::PathBuf;
use std::sync::Arc;

/// Exact native companion capabilities returned before any filesystem work.
#[napi(object)]
#[allow(clippy::struct_excessive_bools)]
pub struct NativeCapabilities {
    /// Canonical package version.
    pub version: String,
    /// Whether durable local authority/object storage is compiled in.
    pub local: bool,
    /// Whether the platform-native watcher is compiled in.
    pub native_watch: bool,
    /// Stable identity of the compiled host notification mechanism.
    pub native_watch_backend: String,
    /// Whether watcher continuity can be proven from a persisted restart cursor.
    pub native_watch_persistent_restart: bool,
    /// Whether active polling fences replacement of the admitted root.
    pub native_watch_root_identity_fencing: bool,
    /// Operating-system target selected at compile time.
    pub platform: String,
    /// CPU architecture selected at compile time.
    pub architecture: String,
    /// Whether the target-native namespace driver is live on this host.
    pub native_mount: bool,
    /// Whether the driver exposes exact writable capture.
    pub writable_mount: bool,
    /// Whether mount I/O from this provider process is observable.
    pub provider_process_io_observable: bool,
}

/// Exact cumulative and resident immutable-object accelerator observations.
#[napi(object)]
pub struct NativeObjectCacheStats {
    /// Complete reads served from resident authenticated bytes.
    pub hits: BigInt,
    /// Authenticated decoded pages reused without canonical decode.
    pub decoded_hits: BigInt,
    /// Reads requiring backing-store work or an in-flight wait.
    pub misses: BigInt,
    /// Reads sharing one concurrent backing-store request.
    pub coalesced_reads: BigInt,
    /// Deterministic least-recently-used removals.
    pub evictions: BigInt,
    /// Current number of resident canonical objects.
    pub resident_entries: BigInt,
    /// Current canonical plus decoded logical bytes.
    pub resident_bytes: BigInt,
    /// Current resident canonical immutable objects.
    pub resident_canonical_objects: BigInt,
    /// Current resident canonical immutable-object bytes.
    pub resident_canonical_bytes: BigInt,
    /// Current resident authenticated decoded pages.
    pub resident_decoded_pages: BigInt,
    /// Current resident decoded-page logical bytes.
    pub resident_decoded_bytes: BigInt,
    /// Current distinct backing-store reads in flight.
    pub in_flight: BigInt,
}

/// Exact hard limits for the process-local immutable-object accelerator.
#[napi(object)]
pub struct NativeObjectCacheOptions {
    /// Maximum resident immutable objects.
    pub maximum_entries: u32,
    /// Maximum resident canonical object bytes.
    pub maximum_bytes: BigInt,
    /// Maximum distinct backing-store reads in flight.
    pub maximum_in_flight: u32,
    /// Maximum followers retained behind one in-flight read.
    pub maximum_waiters_per_object: u32,
}

/// Hard policy for native local-residency prediction.
#[napi(object)]
pub struct NativeResidencySpeculationOptions {
    /// Maximum concurrently admitted residency operations.
    pub maximum_active_operations: u32,
    /// Maximum bytes reserved by active residency operations.
    pub maximum_active_bytes: BigInt,
    /// Number of terminal outcomes retained for usefulness control.
    pub outcome_window: u32,
    /// Number of foreground/speculative traffic samples retained.
    pub traffic_window: u32,
    /// Maximum speculative share of observed foreground traffic.
    pub speculative_cost_basis_points: u32,
    /// Terminal sample count required before usefulness rejection.
    pub minimum_usefulness_samples: u32,
    /// Minimum useful terminal outcome ratio after the sample floor.
    pub minimum_usefulness_basis_points: u32,
}

/// Hard policy for native cross-location promotion planning.
#[napi(object)]
pub struct NativePromotionSpeculationOptions {
    /// Maximum concurrently admitted promotion operations.
    pub maximum_active_operations: u32,
    /// Maximum object bytes reserved by active promotions.
    pub maximum_active_bytes: BigInt,
    /// Maximum estimated cost reserved by active promotions.
    pub maximum_active_cost_units: BigInt,
    /// Maximum exact object-location facts accepted per plan.
    pub maximum_residency_facts: u32,
    /// Maximum destination capabilities accepted per plan.
    pub maximum_destinations: u32,
    /// Maximum acceptable storage tiers supplied per plan.
    pub maximum_accepted_tiers: u32,
    /// Number of terminal outcomes retained for usefulness control.
    pub outcome_window: u32,
    /// Terminal sample count required before usefulness rejection.
    pub minimum_usefulness_samples: u32,
    /// Minimum useful terminal outcome ratio after the sample floor.
    pub minimum_usefulness_basis_points: u32,
}

/// Complete policy for both native speculation engines.
#[napi(object)]
pub struct NativeSpeculationOptions {
    /// Local authenticated-object residency policy.
    pub residency: NativeResidencySpeculationOptions,
    /// Cross-location immutable-object promotion policy.
    pub promotion: NativePromotionSpeculationOptions,
}

/// One authenticated foreground successor supplied to native speculation.
#[napi(object)]
pub struct NativeResidencyObservation {
    /// Stable idempotency identity for the speculative operation.
    pub operation_id: Buffer,
    /// Volume whose immutable generation is being observed.
    pub volume_id: Buffer,
    /// Generation fence under which the hint was derived.
    pub generation_id: Buffer,
    /// Foreground bytes observed before this candidate.
    pub foreground_bytes: BigInt,
    /// Canonical immutable-object identity to warm.
    pub object_id: Buffer,
    /// Maximum bytes the candidate may consume.
    pub maximum_bytes: BigInt,
    /// Exact structural reason for predicting the object.
    pub reason: String,
}

/// Native residency admission result.
#[napi(object)]
pub struct NativeResidencyAdmission {
    /// `admitted`, `duplicate`, or `rejected`.
    pub status: String,
    /// Typed rejection reason when status is `rejected`.
    pub rejection: Option<String>,
}

/// Completed native residency execution.
#[napi(object)]
pub struct NativeResidencyExecution {
    /// Canonical object bytes admitted into authenticated residency.
    pub object_bytes: BigInt,
    /// Serialized exact work counters for the bounded execution.
    pub work_json: String,
}

/// Caller-observed exact object location used by native promotion planning.
#[napi(object)]
pub struct NativeObjectResidency {
    /// Canonical immutable-object identity.
    pub object_id: Buffer,
    /// Stable identity of the location currently holding the object.
    pub location_id: Buffer,
    /// Storage tier of the observed location.
    pub tier: String,
    /// Deterministic source preference; lower values are preferred.
    pub source_priority: u32,
}

/// Writable native promotion destination capability.
#[napi(object)]
pub struct NativePromotionDestination {
    /// Stable identity of the candidate destination.
    pub location_id: Buffer,
    /// Storage tier offered by the destination.
    pub tier: String,
    /// Whether immutable-object publication is currently allowed.
    pub writable: bool,
    /// Largest canonical object accepted by the destination.
    pub maximum_object_bytes: BigInt,
    /// Deterministic destination preference; lower values are preferred.
    pub priority: u32,
    /// Exact estimated cost per canonical byte.
    pub cost_units_per_byte: BigInt,
}

/// Bounded native promotion planning result.
#[napi(object)]
pub struct NativePromotionAdmission {
    /// `admitted`, `duplicate`, or `rejected`.
    pub status: String,
    /// Typed rejection reason when status is `rejected`.
    pub rejection: Option<String>,
    /// Admitted or duplicate operation identity.
    pub operation_id: Option<Buffer>,
    /// Canonical immutable object selected for movement.
    pub object_id: Option<Buffer>,
    /// Deterministically selected source location.
    pub source_location_id: Option<Buffer>,
    /// Deterministically selected destination location.
    pub destination_location_id: Option<Buffer>,
    /// Reserved estimated cost for the admitted promotion.
    pub estimated_cost_units: Option<BigInt>,
}

/// Identities terminalized by native preemption or generation replacement.
#[napi(object)]
pub struct NativeSpeculationPreemption {
    /// Residency operations terminalized by the transition.
    pub residency_operation_ids: Vec<Buffer>,
    /// Promotion operations terminalized by the transition.
    pub promotion_operation_ids: Vec<Buffer>,
}

/// Exact hard limits enforced before externally controlled work.
#[napi(object)]
pub struct NativeVolumeLimits {
    /// Maximum encoded bytes in an absolute path.
    pub maximum_path_bytes: u32,
    /// Maximum encoded bytes in one component.
    pub maximum_component_bytes: u32,
    /// Maximum number of path components.
    pub maximum_path_depth: u32,
    /// Maximum canonical immutable-object bytes.
    pub maximum_object_bytes: BigInt,
    /// Maximum operations in one atomic mutation.
    pub maximum_mutations_per_batch: u32,
    /// Maximum paths in one shared lookup batch.
    pub maximum_paths_per_batch: u32,
    /// Maximum exact dependencies retained by a checkout.
    pub maximum_checkout_dependencies: u32,
    /// Maximum entries returned by one listing page.
    pub maximum_directory_page_entries: u32,
    /// Maximum authenticated tree height.
    pub maximum_page_height: u32,
    /// Maximum bytes returned by one range read.
    pub maximum_read_bytes: BigInt,
    /// Maximum files in one generation closure.
    pub maximum_files_per_generation: BigInt,
    /// Maximum objects in one generation closure.
    pub maximum_objects_per_generation: BigInt,
    /// Maximum canonical bytes in one generation closure.
    pub maximum_generation_bytes: BigInt,
}

/// Complete independently configured volume semantics.
#[napi(object)]
pub struct NativeVolumeOptions {
    /// `portable`, `posix`, `windows`, or `browser`.
    pub profile: String,
    /// `exclusive-writer`, `optimistic`, or `serialized-authority`.
    pub concurrency: String,
    /// `ephemeral` or `durable`.
    pub lifecycle: String,
    /// `sensitive` or `profile-folded`.
    pub case_sensitivity: String,
    /// `preserve` or `require-nfc`.
    pub unicode: String,
    /// Whether symbolic links are representable.
    pub symbolic_links: bool,
    /// Whether multiple names may share one file identity.
    pub hard_links: bool,
    /// Whether holes remain distinct from allocated zeroes.
    pub sparse_files: bool,
    /// Mandatory externally controlled work bounds.
    pub limits: NativeVolumeLimits,
}

/// Checkout behavior selected independently for each volume view.
#[napi(object)]
pub struct NativeCheckoutOptions {
    /// `read-only` or `read-write`.
    pub access: String,
    /// One of `pinned`, `tracking-safe`, `live`, or `manual`.
    pub consistency: String,
    /// One of `none`, `private-cow`, or `direct-live`.
    pub mutation_mode: String,
}

/// Exact no-follow lookup result.
#[napi(object)]
pub struct NativeLookup {
    /// Whether the terminal path exists.
    pub exists: bool,
    /// Stable file identity when present.
    pub file_id: Option<Buffer>,
    /// Canonical file kind when present.
    pub file_kind: Option<String>,
    /// Number of resolved components.
    pub resolved_components: u32,
    /// Exact machine-readable work receipt.
    pub work_json: String,
}

/// One original-order entry from a shared path lookup batch.
#[napi(object)]
pub struct NativeBatchLookupEntry {
    /// Whether the terminal path exists.
    pub exists: bool,
    /// Stable file identity when present.
    pub file_id: Option<Buffer>,
    /// Canonical file kind when present.
    pub file_kind: Option<String>,
    /// Number of path components resolved before terminal absence.
    pub resolved_components: u32,
}

/// One bounded shared-frontier path lookup receipt.
#[napi(object)]
pub struct NativeBatchLookup {
    /// One result per original path, preserving order and duplicates.
    pub entries: Vec<NativeBatchLookupEntry>,
    /// Logical result-vector capacity retained by the caller.
    pub retained_allocation_bytes: BigInt,
    /// Exact machine-readable work receipt.
    pub work_json: String,
}

/// Complete no-follow file record and canonical metadata.
#[napi(object)]
pub struct NativeStat {
    /// Whether the path exists.
    pub exists: bool,
    /// Complete path-independent file record when present.
    pub record: Option<NativeFileRecord>,
    /// Complete canonical metadata bytes when present.
    pub metadata_canonical_bytes: Option<Buffer>,
    /// Exact machine-readable work receipt.
    pub work_json: String,
}

/// One bounded regular-file range.
#[napi(object)]
pub struct NativeFileRead {
    /// Exact logical bytes.
    pub bytes: Buffer,
    /// Exact machine-readable work receipt.
    pub work_json: String,
}

/// One sparse data/hole boundary lookup.
#[napi(object)]
pub struct NativeExtentSeek {
    /// Exact logical offset, or absence when no matching extent remains.
    pub offset: Option<BigInt>,
    /// Exact machine-readable work receipt.
    pub work_json: String,
}

/// One exact logical sparse span.
#[napi(object)]
pub struct NativeExtentSpan {
    /// Physical span class.
    pub kind: String,
    /// Inclusive logical file offset.
    pub offset: BigInt,
    /// Positive logical span length.
    pub length: BigInt,
    /// Exclusive end of the complete canonical source extent.
    pub source_end: BigInt,
    /// Typed immutable object identity for content spans.
    pub object_id: Option<Buffer>,
    /// Byte offset within the immutable object for content spans.
    pub object_offset: Option<BigInt>,
}

/// Bounded sparse range plan without file-body reads.
#[napi(object)]
pub struct NativeExtentPlan {
    /// `inline` for embedded tiny files or `sparse` for an extent plan.
    pub kind: String,
    /// Exact ordered spans for a sparse file.
    pub spans: Vec<NativeExtentSpan>,
    /// Logical allocation retained by the span vector.
    pub retained_allocation_bytes: Option<BigInt>,
    /// Exact machine-readable work receipt.
    pub work_json: String,
}

/// One canonical directory entry.
#[napi(object)]
pub struct NativeDirectoryEntry {
    /// Portable UTF-8 name bytes.
    pub name: Buffer,
    /// Stable file identity.
    pub file_id: Buffer,
    /// Canonical file kind.
    pub file_kind: String,
}

/// One bounded authenticated directory cursor page.
#[napi(object)]
pub struct NativeDirectoryPage {
    /// Ordered entries strictly after the cursor.
    pub entries: Vec<NativeDirectoryEntry>,
    /// Whether another page exists.
    pub has_more: bool,
    /// Exact machine-readable work receipt.
    pub work_json: String,
}

/// One directory binding paired with its complete record and metadata.
#[napi(object)]
pub struct NativeDirectoryRecordEntry {
    /// Exact canonical name bytes.
    pub name: Buffer,
    /// Complete path-independent child record.
    pub record: NativeFileRecord,
    /// Complete canonical metadata bytes referenced by the record.
    pub metadata_canonical_bytes: Buffer,
}

/// One page-efficient directory record enumeration receipt.
#[napi(object)]
pub struct NativeDirectoryRecordPage {
    /// Ordered records strictly after the supplied cursor.
    pub entries: Vec<NativeDirectoryRecordEntry>,
    /// Whether another page exists.
    pub has_more: bool,
    /// Exact machine-readable work receipt.
    pub work_json: String,
}

/// One complete stable file-record read.
#[napi(object)]
pub struct NativeFileRecordRead {
    /// Complete path-independent candidate record.
    pub record: NativeFileRecord,
    /// Exact machine-readable work receipt.
    pub work_json: String,
}

/// Successful authored mutation with exact work evidence.
#[napi(object)]
pub struct NativeMutationResult {
    /// New file identity for create operations.
    pub file_id: Option<Buffer>,
    /// Exact machine-readable work receipt.
    pub work_json: String,
}

/// One strict discriminated operation in an atomic within-volume transaction.
#[napi(object)]
pub struct NativeTransactionOperation {
    /// Stable operation discriminator.
    pub kind: String,
    /// Primary path when required.
    pub path: Option<String>,
    /// Source path when required.
    pub source: Option<String>,
    /// Destination path when required.
    pub destination: Option<String>,
    /// Regular-file body or write bytes.
    pub bytes: Option<Buffer>,
    /// Symbolic-link target bytes.
    pub target: Option<Buffer>,
    /// Reparse-point payload bytes.
    pub payload: Option<Buffer>,
    /// Optional remove identity precondition.
    pub expected_file_id: Option<Buffer>,
    /// Special or device kind.
    pub file_kind: Option<String>,
    /// Primary logical offset.
    pub offset: Option<BigInt>,
    /// Clone source offset.
    pub source_offset: Option<BigInt>,
    /// Clone destination offset.
    pub destination_offset: Option<BigInt>,
    /// Logical range length.
    pub length: Option<BigInt>,
    /// Replacement logical file length.
    pub logical_bytes: Option<BigInt>,
    /// Device major identity.
    pub major: Option<u32>,
    /// Device minor identity.
    pub minor: Option<u32>,
    /// Rename replacement policy.
    pub replace: Option<bool>,
    /// Zero-range physical allocation policy.
    pub allocated: Option<bool>,
    /// Zero-range extension policy.
    pub extend: Option<bool>,
    /// Preallocation logical-size policy.
    pub keep_size: Option<bool>,
    /// Complete canonical metadata replacement bytes.
    pub canonical_bytes: Option<Buffer>,
}

/// Stable result positions for one atomic transaction.
#[napi(object)]
pub struct NativeTransactionResult {
    /// One optional created file identity per input operation.
    pub created_file_ids: Vec<Option<Buffer>>,
    /// Exact machine-readable work receipt.
    pub work_json: String,
}

/// Explicit native materialization bounds.
#[napi(object)]
pub struct NativeMaterializeOptions {
    /// Existing empty host directory.
    pub destination: String,
    /// Maximum entries in one authenticated directory page.
    pub maximum_directory_entries: u32,
    /// Maximum spans in one authenticated extent plan.
    pub maximum_extent_spans: u32,
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

/// Complete path-independent file record crossing the native boundary.
#[napi(object)]
pub struct NativeFileRecord {
    /// Stable 16-byte file identity.
    pub file_id: Buffer,
    /// Canonical file-kind name.
    pub file_kind: String,
    /// Number of namespace bindings referring to this record.
    pub link_count: BigInt,
    /// Canonical 33-byte metadata object identity.
    pub metadata_object: Buffer,
    /// Canonical payload variant name.
    pub payload_kind: String,
    /// Logical payload byte length when the variant has one.
    pub logical_bytes: Option<BigInt>,
    /// Canonical 33-byte payload object identity when externally stored.
    pub payload_object: Option<Buffer>,
    /// Inline regular-file bytes when present.
    pub inline_bytes: Option<Buffer>,
    /// Device major number for device records.
    pub device_major: Option<u32>,
    /// Device minor number for device records.
    pub device_minor: Option<u32>,
}

/// Complete canonical metadata bytes plus exact work.
#[napi(object)]
pub struct NativeMetadataResult {
    /// Versioned canonical metadata record.
    pub canonical_bytes: Buffer,
    /// Exact machine-readable work receipt.
    pub work_json: String,
}

/// Optional exact named-attribute value plus exact work.
#[napi(object)]
pub struct NativeNamedAttributeResult {
    /// Whether the attribute exists.
    pub exists: bool,
    /// Exact bytes when present.
    pub bytes: Option<Buffer>,
    /// Exact machine-readable work receipt.
    pub work_json: String,
}

/// One bounded ordered named-attribute page.
#[napi(object)]
pub struct NativeNamedAttributePage {
    /// Exact canonical class/name pairs.
    pub entries: Vec<NativeNamedAttributeName>,
    /// Whether another canonical page follows.
    pub has_more: bool,
    /// Exact machine-readable work receipt.
    pub work_json: String,
}

/// One exact canonical named-attribute identity.
#[napi(object)]
pub struct NativeNamedAttributeName {
    /// Canonical source-semantics class.
    pub attribute_class: String,
    /// Exact raw name.
    pub name: Buffer,
}

/// One file-record generation change.
#[napi(object)]
pub struct NativeFileRecordChange {
    /// Stable 16-byte file identity.
    pub file_id: Buffer,
    /// Record before the change, absent for creation.
    pub before: Option<NativeFileRecord>,
    /// Record after the change, absent for deletion.
    pub after: Option<NativeFileRecord>,
}

/// One exact name binding generation change.
#[napi(object)]
pub struct NativeBindingChange {
    /// Stable 16-byte parent-directory identity.
    pub directory_id: Buffer,
    /// Exact changed logical name.
    pub name: NativePathComponent,
    /// Binding before the change, absent for creation.
    pub before: Option<NativeTreeEntry>,
    /// Binding after the change, absent for deletion.
    pub after: Option<NativeTreeEntry>,
}

/// One complete directory entry.
#[napi(object)]
pub struct NativeTreeEntry {
    /// Exact logical entry name.
    pub name: NativePathComponent,
    /// Stable 16-byte target file identity.
    pub file_id: Buffer,
    /// Canonical target file-kind name.
    pub file_kind: String,
}

/// Bounded generation diff result.
#[napi(object)]
pub struct NativeGenerationDiff {
    /// Stable-file-record changes.
    pub files: Vec<NativeFileRecordChange>,
    /// Exact directory-binding changes.
    pub bindings: Vec<NativeBindingChange>,
    /// Whether the configured change bound truncated the result.
    pub truncated: bool,
    /// Exact machine-readable work receipt.
    pub work_json: String,
}

/// One typed merge conflict.
#[napi(object)]
pub struct NativeMergeConflict {
    /// `file` or `binding`.
    pub kind: String,
    /// Conflicting stable file identity for a file conflict.
    pub file_id: Option<Buffer>,
    /// Parent-directory identity for a binding conflict.
    pub directory_id: Option<Buffer>,
    /// Exact logical name for a binding conflict.
    pub name: Option<NativePathComponent>,
}

/// Terminal merge preparation result.
#[napi(object)]
pub struct NativeMergePreparation {
    /// `prepared` or `conflicted`.
    pub status: String,
    /// Prepared two-parent generation identity on success.
    pub generation_id: Option<Buffer>,
    /// Bounded exact conflicts when preparation cannot proceed.
    pub conflicts: Vec<NativeMergeConflict>,
    /// Whether the conflict bound truncated the result.
    pub truncated: bool,
    /// Exact machine-readable work receipt.
    pub work_json: String,
}

/// One exact namespace component crossing the native binding boundary.
#[napi(object)]
pub struct NativePathComponent {
    /// `utf8`, `posix-bytes`, or `windows-utf16le`.
    pub encoding: String,
    /// Exact component bytes in the declared representation.
    pub bytes: Buffer,
}

/// One exact absolute volume path; an empty vector denotes the root.
#[napi(object)]
pub struct NativeNamespacePath {
    /// Root-to-leaf exact components.
    pub components: Vec<NativePathComponent>,
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

/// Complete resumable generation-transfer manifest.
#[napi(object)]
pub struct NativeExportManifest {
    /// Canonical, versioned, authenticated transfer descriptor.
    pub manifest_bytes: Buffer,
    /// Stable sorted typed 33-byte complete object closure.
    pub objects: Vec<Buffer>,
    /// Exact machine-readable work receipt.
    pub work_json: String,
}

/// One manifest-ordered immutable-object transfer page.
#[napi(object)]
pub struct NativeGenerationTransferBatch {
    /// Manifest-relative first object position.
    pub first_object: BigInt,
    /// Next position, absent at the canonical end.
    pub next_object: Option<BigInt>,
    /// Ordered immutable object bodies.
    pub objects: Vec<Buffer>,
    /// Exact machine-readable work receipt.
    pub work_json: String,
}

/// Cursor after one idempotently imported transfer page.
#[napi(object)]
pub struct NativeGenerationTransferCursor {
    /// Manifest-relative next object position.
    pub next_object: BigInt,
    /// Exact machine-readable work receipt.
    pub work_json: String,
}

/// Immutable content-addressed candidate built without publishing authority.
#[napi(object)]
pub struct NativeCheckpointResult {
    /// Stable 32-byte generation identity.
    pub generation_id: Buffer,
    /// Exact machine-readable work receipt.
    pub work_json: String,
}

/// Conditional publication outcome with exact authority facts.
#[napi(object)]
pub struct NativeCommitResult {
    /// `committed`, `already-committed`, `conflict`, `fenced`, or `idempotency-conflict`.
    pub status: String,
    /// Candidate generation for successful publication.
    pub generation_id: Option<Buffer>,
    /// Relevant active authority epoch.
    pub epoch: Option<BigInt>,
    /// Relevant authority sequence.
    pub sequence: Option<BigInt>,
    /// Existing fingerprint for an idempotency conflict.
    pub committed_fingerprint: Option<Buffer>,
    /// Exact machine-readable work receipt.
    pub work_json: String,
}

/// Observation-safe head-rebase outcome.
#[napi(object)]
pub struct NativeRebaseResult {
    /// `safe` or `conflicted`.
    pub status: String,
    /// New base generation when safe.
    pub generation_id: Option<Buffer>,
    /// Number of exact changed regions returned.
    pub conflict_count: u32,
    /// Whether additional conflicts exceeded the caller bound.
    pub truncated: bool,
    /// Exact machine-readable work receipt.
    pub work_json: String,
}

/// Terminal result of publishing one explicitly staged direct-live transaction.
#[napi(object)]
pub struct NativeLiveMutationResult {
    /// `committed`, `already-committed`, `conflicted`, `retry-limit`, `fenced`, or `idempotency-conflict`.
    pub status: String,
    /// Published generation when successful.
    pub generation_id: Option<Buffer>,
    /// Relevant authority epoch.
    pub epoch: Option<BigInt>,
    /// Relevant authority sequence.
    pub sequence: Option<BigInt>,
    /// Number of exact conflicts returned.
    pub conflict_count: u32,
    /// Whether additional conflicts exceeded the caller bound.
    pub truncated: bool,
    /// Existing fingerprint for operation-identity reuse with different content.
    pub committed_fingerprint: Option<Buffer>,
    /// Exact machine-readable work receipt.
    pub work_json: String,
}

/// One direct-live authored transaction result with stable create positions.
#[napi(object)]
pub struct NativeAuthoredLiveMutationResult {
    /// One entry per authored operation; only create operations contain identities.
    pub created_file_ids: Vec<Option<Buffer>>,
    /// Terminal publication status.
    pub status: String,
    /// Published generation identity when committed.
    pub generation_id: Option<Buffer>,
    /// Authority epoch when available.
    pub epoch: Option<BigInt>,
    /// Authority sequence when available.
    pub sequence: Option<BigInt>,
    /// Exact bounded conflict count.
    pub conflict_count: u32,
    /// Whether additional conflicts were omitted.
    pub truncated: bool,
    /// Existing fingerprint for idempotency conflict.
    pub committed_fingerprint: Option<Buffer>,
    /// Exact machine-readable work receipt.
    pub work_json: String,
}

/// One independently configured volume owned by the embedded engine.
#[napi]
pub struct NativeVolume {
    inner: LocalVolume,
    cancellation: CancellationToken,
    acquisition_work: acyclic_fs::WorkCounters,
}

/// One generation-fenced checkout. Mutable operations are serialized per handle.
#[napi]
pub struct NativeCheckout {
    inner: Arc<SharedCheckout<LocalAuthorityBackend, LocalObjectBackend>>,
    config: VolumeConfig,
    cancellation: CancellationToken,
    acquisition_work: acyclic_fs::WorkCounters,
}

/// One process-owned exact native watcher over a materialized checkout root.
#[napi]
pub struct NativeWatcher {
    inner: std::sync::Mutex<FsNativeWatch>,
    pending: std::sync::Mutex<Option<PendingWatchBatch>>,
    pending_reconcile: std::sync::Mutex<Option<PendingReconcile>>,
    operation: tokio::sync::Mutex<()>,
    checkout: Arc<SharedCheckout<LocalAuthorityBackend, LocalObjectBackend>>,
    source_root: PathBuf,
    cancellation: CancellationToken,
}

#[derive(Clone)]
struct PendingWatchBatch {
    batch: WatchBatch,
    poll_work: acyclic_fs::WorkCounters,
    capture: Option<WatchCaptureReceipt>,
    operation_id: OperationId,
}

#[derive(Clone, Copy)]
struct PendingReconcile {
    operation_id: OperationId,
    epoch: acyclic_fs::WatchEpoch,
    baseline: CaptureReceipt,
    work: acyclic_fs::WorkCounters,
}

/// One process-owned native mount over a checkout.
#[napi]
pub struct NativeMount {
    inner: std::sync::Mutex<Option<NativeMountSession>>,
    mount_id: acyclic_fs::MountId,
    destination: String,
}

/// Native owner of one volume generation's two canonical speculation engines.
#[napi]
pub struct NativeSpeculation {
    fs: Arc<LocalFs>,
    controller: tokio::sync::Mutex<SpeculationController>,
    cancellation: CancellationToken,
}

#[napi]
impl NativeSpeculation {
    /// Records foreground demand and admits one exact authenticated successor.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed identities, invalid bounds, or a failed
    /// speculation-controller transition.
    #[napi]
    pub async fn observe(
        &self,
        observation: NativeResidencyObservation,
    ) -> Result<NativeResidencyAdmission> {
        let operation_id = OperationId::from_bytes(fixed_16(&observation.operation_id)?);
        let volume_id = VolumeId::from_bytes(fixed_16(&observation.volume_id)?);
        let generation_id = acyclic_fs::GenerationId::new(Digest::from_bytes(fixed_32(
            &observation.generation_id,
            "generation identity",
        )?));
        let hint = ResidencyHint {
            request: ObjectReadRequest {
                object_id: decode_object_id(&observation.object_id)?,
                maximum_bytes: bigint_u64(&observation.maximum_bytes)?,
            },
            reason: native_residency_reason(&observation.reason)?,
        };
        let admission = self
            .controller
            .lock()
            .await
            .observe_hint(
                operation_id,
                volume_id,
                generation_id,
                bigint_u64(&observation.foreground_bytes)?,
                hint,
            )
            .map_err(napi_error)?;
        Ok(match admission {
            ResidencyAdmission::Admitted(_) => NativeResidencyAdmission {
                status: "admitted".to_owned(),
                rejection: None,
            },
            ResidencyAdmission::Rejected(rejection) => NativeResidencyAdmission {
                status: "rejected".to_owned(),
                rejection: Some(native_residency_rejection(rejection).to_owned()),
            },
        })
    }

    /// Executes one active prediction through the owning filesystem's exact
    /// authenticated object backend and shared cache.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation is not active, authenticated object
    /// admission fails, the work budget is exhausted, or execution is cancelled.
    #[napi]
    pub async fn execute_residency(
        &self,
        operation_id: Buffer,
    ) -> Result<NativeResidencyExecution> {
        let operation_id = OperationId::from_bytes(fixed_16(&operation_id)?);
        let controller = self.controller.lock().await;
        let permit = controller
            .active_residency_permit(operation_id)
            .ok_or_else(|| Error::new(Status::InvalidArg, "residency operation is not active"))?;
        let receipt = self
            .fs
            .execute_residency(
                controller.residency(),
                permit,
                boundary_budget(),
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        Ok(NativeResidencyExecution {
            object_bytes: bigint(receipt.value),
            work_json: serde_json::to_string(&receipt.work).map_err(napi_error)?,
        })
    }

    /// Records exact terminal local-residency usefulness.
    ///
    /// # Errors
    ///
    /// Returns an error for a malformed or non-active operation identity, or
    /// when the bounded metrics transition overflows.
    #[napi]
    pub async fn finish_residency(&self, operation_id: Buffer, useful: bool) -> Result<()> {
        self.controller
            .lock()
            .await
            .finish_residency(OperationId::from_bytes(fixed_16(&operation_id)?), useful)
            .map_err(napi_error)
    }

    /// Plans at most one cross-location promotion from an active residency
    /// permit and bounded caller-observed storage facts.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed facts, unsupported tiers, a non-active
    /// residency operation, or a failed bounded controller transition.
    #[napi]
    pub async fn plan_promotion(
        &self,
        operation_id: Buffer,
        accepted_tiers: Vec<String>,
        residency: Vec<NativeObjectResidency>,
        destinations: Vec<NativePromotionDestination>,
    ) -> Result<NativePromotionAdmission> {
        let operation_id = OperationId::from_bytes(fixed_16(&operation_id)?);
        let mut controller = self.controller.lock().await;
        let permit = controller
            .active_residency_permit(operation_id)
            .ok_or_else(|| Error::new(Status::InvalidArg, "residency operation is not active"))?;
        let tiers = accepted_tiers
            .iter()
            .map(|tier| native_storage_tier(tier))
            .collect::<Result<Vec<_>>>()?;
        let residency = residency
            .iter()
            .map(native_object_residency)
            .collect::<Result<Vec<_>>>()?;
        let destinations = destinations
            .iter()
            .map(native_promotion_destination)
            .collect::<Result<Vec<_>>>()?;
        let admission = controller
            .plan_promotion(permit, tiers, &residency, &destinations)
            .map_err(napi_error)?;
        Ok(native_promotion_admission(admission))
    }

    /// Records exact terminal promotion usefulness.
    ///
    /// # Errors
    ///
    /// Returns an error for a malformed or non-active operation identity, or
    /// when the bounded metrics transition overflows.
    #[napi]
    pub async fn finish_promotion(&self, operation_id: Buffer, useful: bool) -> Result<()> {
        self.controller
            .lock()
            .await
            .finish_promotion(OperationId::from_bytes(fixed_16(&operation_id)?), useful)
            .map_err(napi_error)
    }

    /// Atomically preempts both engines before recording foreground bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if terminalizing the bounded active sets or recording
    /// foreground traffic would overflow.
    #[napi]
    pub async fn preempt_for_foreground(
        &self,
        foreground_bytes: BigInt,
    ) -> Result<NativeSpeculationPreemption> {
        let preemption = self
            .controller
            .lock()
            .await
            .preempt_for_foreground(bigint_u64(&foreground_bytes)?)
            .map_err(napi_error)?;
        Ok(native_speculation_preemption(preemption))
    }

    /// Atomically fences both engines onto one new immutable generation.
    ///
    /// # Errors
    ///
    /// Returns an error for a malformed generation identity or if the atomic
    /// bounded controller transition fails.
    #[napi]
    pub async fn replace_generation(
        &self,
        generation_id: Buffer,
    ) -> Result<NativeSpeculationPreemption> {
        let generation_id = acyclic_fs::GenerationId::new(Digest::from_bytes(fixed_32(
            &generation_id,
            "generation identity",
        )?));
        let preemption = self
            .controller
            .lock()
            .await
            .replace_generation(generation_id)
            .map_err(napi_error)?;
        Ok(native_speculation_preemption(preemption))
    }

    /// Returns payload-free exact metrics for both engines.
    ///
    /// # Errors
    ///
    /// Returns an error if the exact metrics cannot be serialized.
    #[napi]
    pub async fn metrics_json(&self) -> Result<String> {
        let metrics = self.controller.lock().await.metrics();
        serde_json::to_string(&serde_json::json!({
            "residency": native_residency_metrics_json(metrics.residency),
            "promotion": native_promotion_metrics_json(metrics.promotion),
        }))
        .map_err(napi_error)
    }

    /// Cooperatively cancels future residency execution owned by this handle.
    #[napi]
    pub fn cancel(&self) {
        self.cancellation.cancel();
    }
}

/// Embedded local filesystem owner. No daemon is required.
#[napi]
pub struct NativeFs {
    inner: Arc<LocalFs>,
    cancellation: CancellationToken,
}

type NativeLocalWorkspace = Workspace<LocalAuthorityBackend, LocalObjectBackend>;
type NativeLocalTransaction = Transaction<LocalAuthorityBackend, LocalObjectBackend>;
type NativeLocalWorkspaceMount = WorkspaceMount<LocalAuthorityBackend, LocalObjectBackend>;
type NativeLocalGeneration = Generation<LocalAuthorityBackend, LocalObjectBackend>;
type NativeLocalChangeSet = ChangeSet<LocalAuthorityBackend, LocalObjectBackend>;
type NativeLocalJoinPlan = JoinPlan<LocalAuthorityBackend, LocalObjectBackend>;

/// One named customer workspace backed by the embedded local engine.
#[napi]
pub struct NativeWorkspace {
    inner: NativeLocalWorkspace,
}

/// One exact immutable customer workspace generation.
#[napi]
pub struct NativeGeneration {
    inner: NativeLocalGeneration,
}

/// One immutable semantic delta between exact generations.
#[napi]
pub struct NativeChangeSet {
    inner: NativeLocalChangeSet,
}

/// One immutable, side-effect-free workspace join plan.
#[napi]
pub struct NativeJoinPlan {
    inner: NativeLocalJoinPlan,
}

/// Exact bounded join planning options.
#[napi(object)]
pub struct NativeJoinOptions {
    /// `merge`, `rebase`, `squash`, or `cherry-pick`.
    pub history: String,
    /// Maximum lineage generations examined while finding an ancestor.
    pub maximum_generations: u32,
    /// Maximum semantic changes admitted by the plan.
    pub maximum_changes: u32,
    /// Maximum exact conflicts returned by application.
    pub maximum_conflicts: u32,
}

/// Terminal result of applying one immutable join plan.
#[napi(object)]
pub struct NativeJoinResult {
    /// `applied`, `already-applied`, `no-changes`, `stale-target`, `conflicted`,
    /// `fenced`, or `idempotency-conflict`.
    pub status: String,
    /// Exact target generation for generation-bearing outcomes.
    pub generation_id: Option<Buffer>,
    /// Stable path-independent conflict regions.
    pub conflicts: Vec<NativeMergeConflict>,
    /// Whether additional conflicts exceeded the retained result bound.
    pub truncated: bool,
}

/// Terminal result of advancing a fork onto its source workspace.
#[napi(object)]
pub struct NativeWorkspaceRebaseResult {
    /// `rebased`, `already-rebased`, `current`, `stale`, `conflicted`, `fenced`,
    /// or `idempotency-conflict`.
    pub status: String,
    /// Exact fork generation for generation-bearing outcomes.
    pub generation_id: Option<Buffer>,
    /// Stable path-independent conflict regions.
    pub conflicts: Vec<NativeMergeConflict>,
    /// Whether additional conflicts exceeded the retained result bound.
    pub truncated: bool,
}

/// One sparse atomic workspace transaction.
#[napi]
pub struct NativeWorkspaceTransaction {
    inner: tokio::sync::Mutex<NativeLocalTransaction>,
}

/// Explicit native workspace mount policy.
#[napi(object)]
pub struct NativeWorkspaceMountOptions {
    /// Whether the mount accepts authored changes.
    pub writable: bool,
    /// Exact workspace directory projected as the mount root.
    pub subdirectory: String,
    /// `close-and-sync`, `per-mutation`, or `manual`.
    pub publication: String,
}

/// Exact bounded native directory source configuration.
#[napi(object)]
pub struct NativeSourceOptions {
    /// `pinned` or `tracking`.
    pub mode: String,
    /// Maximum paths admitted by one baseline or reconciliation.
    pub maximum_paths: u32,
    /// Maximum sparse spans admitted per regular file.
    pub maximum_extent_spans: u32,
    /// Maximum pending native changes before fail-closed rescan.
    pub maximum_queued_changes: u32,
}

/// Current or terminal source reconciliation state.
#[napi(object)]
pub struct NativeSourceResult {
    /// `none`, `clean`, `pending-capture`, `needs-rescan`, `conflict`, or `sealed`.
    pub status: String,
    /// Exact invalidation reason for `needs-rescan`.
    pub reason: Option<String>,
    /// Exact immutable generation selected by a clean terminal operation.
    pub generation_id: Option<Buffer>,
}

/// Retryable owner of one process-local native workspace mount.
#[napi]
pub struct NativeWorkspaceMount {
    inner: Arc<std::sync::Mutex<Option<NativeLocalWorkspaceMount>>>,
    path: String,
}

/// Worker-thread publication of one native workspace mount.
pub struct NativeWorkspaceMountSyncTask {
    inner: Arc<std::sync::Mutex<Option<NativeLocalWorkspaceMount>>>,
}

/// Worker-thread publication and detach of one native workspace mount.
pub struct NativeWorkspaceUnmountTask {
    inner: Arc<std::sync::Mutex<Option<NativeLocalWorkspaceMount>>>,
}

/// Terminal workspace publication outcome.
#[napi(object)]
pub struct NativeWorkspaceCommit {
    /// Stable terminal status.
    pub status: String,
    /// Published or actual generation identity when applicable.
    pub generation_id: Option<Buffer>,
}

/// Observation-safe retained transaction rebase result.
#[napi(object)]
pub struct NativeTransactionRebaseResult {
    /// `rebased` or `conflicted`.
    pub status: String,
    /// New immutable base generation when safe.
    pub generation_id: Option<Buffer>,
    /// Exact bounded changed regions.
    pub conflicts: Vec<NativeTransactionConflict>,
    /// Whether additional conflicts exceeded the caller bound.
    pub truncated: bool,
}

/// One exact transaction dependency conflict.
#[napi(object)]
#[allow(missing_docs)]
pub struct NativeTransactionConflict {
    pub region: String,
    pub file_id: Option<Buffer>,
    pub directory_id: Option<Buffer>,
    pub offset: Option<BigInt>,
    pub length: Option<BigInt>,
    pub sparse_target: Option<String>,
    pub name: Option<NativeWorkspaceName>,
    pub maximum_entries: Option<u32>,
    pub usage: String,
    pub expected: Option<Buffer>,
    pub actual: Option<Buffer>,
}

/// One exact customer-visible path stat without storage topology.
#[napi(object)]
#[allow(missing_docs)]
pub struct NativeWorkspaceStat {
    pub file_id: Buffer,
    pub kind: String,
    pub link_count: BigInt,
    pub logical_bytes: Option<BigInt>,
    pub metadata: NativeWorkspaceMetadata,
}

/// Scalar cross-profile metadata and opaque-payload presence.
#[napi(object)]
#[allow(missing_docs)]
pub struct NativeWorkspaceMetadata {
    pub posix_mode: Option<u32>,
    pub posix_uid: Option<u32>,
    pub posix_gid: Option<u32>,
    pub posix_flags: Option<BigInt>,
    pub windows_attributes: Option<u32>,
    pub created_ns: Option<BigInt>,
    pub modified_ns: Option<BigInt>,
    pub accessed_ns: Option<BigInt>,
    pub changed_ns: Option<BigInt>,
    pub has_named_attributes: bool,
    pub has_acl: bool,
    pub has_security_descriptor: bool,
}

/// One exact encoded directory name.
#[napi(object)]
#[allow(missing_docs)]
pub struct NativeWorkspaceName {
    pub encoding: String,
    pub bytes: Buffer,
}

/// One child in a bounded directory page.
#[napi(object)]
#[allow(missing_docs)]
pub struct NativeWorkspaceDirectoryEntry {
    pub name: NativeWorkspaceName,
    pub file_id: Buffer,
    pub kind: String,
}

/// One bounded authenticated directory page.
#[napi(object)]
#[allow(missing_docs)]
pub struct NativeWorkspaceDirectoryPage {
    pub entries: Vec<NativeWorkspaceDirectoryEntry>,
    pub has_more: bool,
}

/// One topology-free sparse extent span.
#[napi(object)]
#[allow(missing_docs)]
pub struct NativeWorkspaceExtentSpan {
    pub offset: BigInt,
    pub length: BigInt,
    pub source_end: BigInt,
    pub kind: String,
}

/// One bounded topology-free sparse extent plan.
#[napi(object)]
#[allow(missing_docs)]
pub struct NativeWorkspaceExtentPlan {
    pub spans: Vec<NativeWorkspaceExtentSpan>,
}

#[napi]
#[allow(missing_docs)]
#[allow(clippy::missing_errors_doc)]
impl NativeWorkspace {
    /// Canonical customer workspace name.
    #[must_use]
    #[napi(getter)]
    pub fn name(&self) -> String {
        self.inner.name().as_str().to_owned()
    }

    /// Stable opaque workspace identity.
    #[must_use]
    #[napi(getter)]
    pub fn id(&self) -> Buffer {
        Buffer::from(self.inner.id().into_bytes().to_vec())
    }

    /// Current immutable generation identity.
    ///
    /// # Errors
    ///
    /// Returns authority, authentication, or storage failures.
    #[napi]
    pub async fn head(&self) -> Result<Buffer> {
        self.inner
            .head()
            .await
            .map(|generation| Buffer::from(generation.id().digest().into_bytes().to_vec()))
            .map_err(napi_error)
    }

    /// Synchronizes prior workspace operations and returns the exact immutable head.
    ///
    /// # Errors
    ///
    /// Returns authority, authentication, or storage failures.
    #[napi]
    pub async fn sync(&self) -> Result<NativeGeneration> {
        self.inner
            .sync()
            .await
            .map(|receipt| NativeGeneration {
                inner: receipt.into_generation(),
            })
            .map_err(napi_error)
    }

    /// Retains the current generation under one human-readable label.
    ///
    /// # Errors
    ///
    /// Returns invalid label, authority, authentication, or storage failures.
    #[napi]
    pub async fn checkpoint(&self, label: String) -> Result<NativeGeneration> {
        self.inner
            .checkpoint(label)
            .await
            .map(|checkpoint| NativeGeneration {
                inner: checkpoint.generation().clone(),
            })
            .map_err(napi_error)
    }

    /// Retains the current generation under one opaque stable identity.
    ///
    /// # Errors
    ///
    /// Returns invalid identity, conflict, authority, authentication, or storage failures.
    #[napi]
    pub async fn pin(&self, identity: String) -> Result<NativeGeneration> {
        self.inner
            .pin(identity)
            .await
            .map(|pin| NativeGeneration {
                inner: pin.generation().clone(),
            })
            .map_err(napi_error)
    }

    /// Terminally removes this mutable workspace head without invalidating retained state.
    ///
    /// # Errors
    ///
    /// Returns invalid retry identity, authority, or storage failures.
    #[napi]
    pub async fn delete(&self, idempotency_key: Option<Buffer>) -> Result<String> {
        let idempotency_key = native_idempotency_key(idempotency_key)?;
        self.inner
            .delete(idempotency_key)
            .await
            .map(|outcome| {
                match outcome {
                    WorkspaceDelete::Deleted => "deleted",
                    WorkspaceDelete::AlreadyDeleted => "already-deleted",
                    WorkspaceDelete::Conflict => "conflict",
                    WorkspaceDelete::IdempotencyConflict => "idempotency-conflict",
                }
                .to_owned()
            })
            .map_err(napi_error)
    }

    /// Returns the durable semantic state of this workspace's attached source.
    #[napi(js_name = sourceState)]
    pub async fn source_state(&self) -> NativeSourceResult {
        match self.inner.source().cloned() {
            Some(source) => native_source_state(source.state().await),
            None => NativeSourceResult {
                status: "none".to_owned(),
                reason: None,
                generation_id: None,
            },
        }
    }

    /// Captures one contiguous native change interval and conditionally publishes it.
    ///
    /// # Errors
    ///
    /// Returns absence, pinned-mode, native watcher, authority, or storage failures.
    #[napi(js_name = reconcileSource)]
    pub async fn reconcile_source(&self) -> Result<NativeSourceResult> {
        let source =
            self.inner.source().cloned().ok_or_else(|| {
                Error::new(Status::InvalidArg, "workspace has no attached source")
            })?;
        Box::pin(source.reconcile())
            .await
            .map(native_reconcile_outcome)
            .map_err(napi_error)
    }

    /// Rebuilds a complete authenticated source baseline.
    ///
    /// # Errors
    ///
    /// Returns absence, native capture, authority, or storage failures.
    #[napi(js_name = rescanSource)]
    pub async fn rescan_source(&self) -> Result<NativeSourceResult> {
        let source =
            self.inner.source().cloned().ok_or_else(|| {
                Error::new(Status::InvalidArg, "workspace has no attached source")
            })?;
        Box::pin(source.rescan())
            .await
            .map(native_reconcile_outcome)
            .map_err(napi_error)
    }

    /// Captures all remaining source state into an independent immutable generation.
    ///
    /// # Errors
    ///
    /// Returns native capture, authority, authentication, or storage failures.
    #[napi]
    pub async fn seal(&self) -> Result<NativeGeneration> {
        Box::pin(self.inner.seal())
            .await
            .map(|inner| NativeGeneration { inner })
            .map_err(napi_error)
    }

    /// Reads one complete regular file under a caller bound.
    ///
    /// # Errors
    ///
    /// Returns path, kind, bound, authentication, or storage failures.
    #[napi]
    pub async fn read(&self, path: String, maximum_bytes: BigInt) -> Result<Buffer> {
        Box::pin(self.inner.read(&path, bigint_u64(&maximum_bytes)?))
            .await
            .map(|bytes| Buffer::from(bytes.to_vec()))
            .map_err(napi_error)
    }

    #[napi(js_name = readRange)]
    pub async fn read_range(&self, path: String, offset: BigInt, length: BigInt) -> Result<Buffer> {
        Box::pin(
            self.inner
                .read_range(&path, bigint_u64(&offset)?, bigint_u64(&length)?),
        )
        .await
        .map(|bytes| Buffer::from(bytes.to_vec()))
        .map_err(napi_error)
    }

    #[napi]
    pub async fn stat(&self, path: String) -> Result<NativeWorkspaceStat> {
        Box::pin(self.inner.stat(&path))
            .await
            .map(native_workspace_stat)
            .map_err(napi_error)
    }

    #[napi(js_name = listDirectory)]
    pub async fn list_directory(
        &self,
        path: String,
        after: Option<NativeWorkspaceName>,
        maximum_entries: u32,
    ) -> Result<NativeWorkspaceDirectoryPage> {
        let after = after.map(native_workspace_name).transpose()?;
        Box::pin(
            self.inner
                .list_directory(&path, after.as_ref(), maximum_entries),
        )
        .await
        .map(native_workspace_directory_page)
        .map_err(napi_error)
    }

    #[napi(js_name = readSymbolicLink)]
    pub async fn read_symbolic_link(&self, path: String) -> Result<Buffer> {
        Box::pin(self.inner.read_symbolic_link(&path))
            .await
            .map(|bytes| Buffer::from(bytes.to_vec()))
            .map_err(napi_error)
    }

    #[napi(js_name = planExtents)]
    pub async fn plan_extents(
        &self,
        path: String,
        offset: BigInt,
        length: BigInt,
        maximum_spans: u32,
    ) -> Result<NativeWorkspaceExtentPlan> {
        Box::pin(self.inner.plan_extents(
            &path,
            bigint_u64(&offset)?,
            bigint_u64(&length)?,
            maximum_spans,
        ))
        .await
        .map(native_workspace_extent_plan)
        .map_err(napi_error)
    }

    /// Atomically creates or replaces one complete file.
    ///
    /// # Errors
    ///
    /// Returns path, size, conflict, authentication, or storage failures.
    #[napi]
    pub async fn write(&self, path: String, bytes: Buffer) -> Result<NativeWorkspaceCommit> {
        self.inner
            .write(&path, bytes::Bytes::from(bytes.to_vec()))
            .await
            .map(workspace_commit)
            .map_err(napi_error)
    }

    /// Removes one existing path atomically.
    ///
    /// # Errors
    ///
    /// Returns path, absence, conflict, authentication, or storage failures.
    #[napi]
    pub async fn remove(&self, path: String) -> Result<NativeWorkspaceCommit> {
        self.inner
            .remove(&path)
            .await
            .map(workspace_commit)
            .map_err(napi_error)
    }

    /// Creates an independent named workspace at the current generation.
    ///
    /// # Errors
    ///
    /// Returns name, generation, authority, or storage failures.
    #[napi]
    pub async fn fork(
        &self,
        destination: String,
        idempotency_key: Option<Buffer>,
    ) -> Result<NativeWorkspace> {
        let idempotency_key = native_idempotency_key(idempotency_key)?;
        let generation = self.inner.head().await.map_err(napi_error)?;
        self.inner
            .fork(
                destination,
                ForkOptions::from_generation(generation, idempotency_key),
            )
            .await
            .map(|inner| NativeWorkspace { inner })
            .map_err(napi_error)
    }

    /// Creates an independent workspace at one caller-selected exact generation.
    ///
    /// # Errors
    ///
    /// Returns name, foreign-generation, authority, authentication, or storage failures.
    #[napi(js_name = forkAt)]
    pub async fn fork_at(
        &self,
        destination: String,
        generation: &NativeGeneration,
        idempotency_key: Option<Buffer>,
    ) -> Result<NativeWorkspace> {
        let idempotency_key = native_idempotency_key(idempotency_key)?;
        self.inner
            .fork(
                destination,
                ForkOptions::from_generation(generation.inner.clone(), idempotency_key),
            )
            .await
            .map(|inner| NativeWorkspace { inner })
            .map_err(napi_error)
    }

    /// Begins one sparse atomic transaction at the current workspace head.
    ///
    /// # Errors
    ///
    /// Returns invalid retry identity, authority, authentication, or storage failures.
    #[napi(js_name = beginTransaction)]
    pub async fn begin_transaction(
        &self,
        idempotency_key: Option<Buffer>,
    ) -> Result<NativeWorkspaceTransaction> {
        let idempotency_key = native_idempotency_key(idempotency_key)?;
        self.inner
            .begin_transaction(idempotency_key)
            .await
            .map(|inner| NativeWorkspaceTransaction {
                inner: tokio::sync::Mutex::new(inner),
            })
            .map_err(napi_error)
    }

    /// Advances this fork onto its source workspace's current generation.
    #[napi(js_name = liveRebase)]
    pub async fn live_rebase(
        &self,
        idempotency_key: Option<Buffer>,
        maximum_generations: u32,
        maximum_changes: u32,
        maximum_conflicts: u32,
    ) -> Result<NativeWorkspaceRebaseResult> {
        self.inner
            .live_rebase(
                native_idempotency_key(idempotency_key)?,
                maximum_generations,
                maximum_changes,
                maximum_conflicts,
            )
            .await
            .map(native_workspace_rebase_result)
            .map_err(napi_error)
    }

    /// Computes one immutable bounded semantic delta between exact generations.
    ///
    /// # Errors
    ///
    /// Rejects foreign endpoints, invalid bounds, or authenticated storage failures.
    #[napi]
    pub async fn diff(
        &self,
        from: &NativeGeneration,
        to: &NativeGeneration,
        maximum_changes: u32,
    ) -> Result<NativeChangeSet> {
        self.inner
            .diff(&from.inner, &to.inner, maximum_changes)
            .await
            .map(|inner| NativeChangeSet { inner })
            .map_err(napi_error)
    }

    /// Builds one immutable side-effect-free plan for joining this workspace into a target.
    ///
    /// # Errors
    ///
    /// Rejects invalid bounds/history, unrelated workspaces, or authenticated storage failures.
    #[napi(js_name = joinInto)]
    pub async fn join_into(
        &self,
        target: &NativeWorkspace,
        options: NativeJoinOptions,
    ) -> Result<NativeJoinPlan> {
        let history = native_join_history(&options.history)?;
        self.inner
            .join_into(&target.inner)
            .history(history)
            .bounds(
                options.maximum_generations,
                options.maximum_changes,
                options.maximum_conflicts,
            )
            .plan()
            .await
            .map(|inner| NativeJoinPlan { inner })
            .map_err(napi_error)
    }

    /// Mounts this workspace or one exact subtree through the host-native driver.
    ///
    /// # Errors
    ///
    /// Returns invalid policy, workspace, capability, destination, or driver failures.
    #[napi]
    pub async fn mount(
        &self,
        destination: String,
        options: NativeWorkspaceMountOptions,
    ) -> Result<NativeWorkspaceMount> {
        let publication = match options.publication.as_str() {
            "close-and-sync" => MountPublication::CloseAndSync,
            "per-mutation" => MountPublication::PerMutation,
            "manual" => MountPublication::Manual,
            _ => {
                return Err(Error::new(
                    Status::InvalidArg,
                    "mount publication must be close-and-sync, per-mutation, or manual",
                ));
            }
        };
        let mount_options = if options.writable {
            MountOptions::read_write()
        } else {
            MountOptions::read_only()
        }
        .subdirectory(options.subdirectory)
        .publication(publication);
        let mount = self
            .inner
            .mount(PathBuf::from(&destination), mount_options)
            .await
            .map_err(napi_error)?;
        Ok(NativeWorkspaceMount {
            inner: Arc::new(std::sync::Mutex::new(Some(mount))),
            path: destination,
        })
    }
}

#[napi]
impl NativeChangeSet {
    /// Exact immutable base endpoint.
    #[must_use]
    #[napi(getter)]
    pub fn from(&self) -> NativeGeneration {
        NativeGeneration {
            inner: self.inner.from().clone(),
        }
    }

    /// Exact immutable resulting endpoint.
    #[must_use]
    #[napi(getter)]
    pub fn to(&self) -> NativeGeneration {
        NativeGeneration {
            inner: self.inner.to().clone(),
        }
    }

    /// Stable path-independent records and namespace binding changes.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript conversion error if the canonical result cannot be represented.
    pub fn changes(&self) -> Result<NativeGenerationDiff> {
        encode_generation_diff(self.inner.changes().clone(), self.inner.work())
    }

    /// Composes contiguous immutable deltas by diffing their outer endpoints.
    ///
    /// # Errors
    ///
    /// Rejects discontinuous changes, invalid bounds, or authenticated storage failures.
    #[napi]
    pub async fn compose(
        &self,
        next: &NativeChangeSet,
        maximum_changes: u32,
    ) -> Result<NativeChangeSet> {
        self.inner
            .compose(&next.inner, maximum_changes)
            .await
            .map(|inner| NativeChangeSet { inner })
            .map_err(napi_error)
    }
}

#[napi]
impl NativeJoinPlan {
    /// Target generation observed while planning.
    #[must_use]
    #[napi(getter, js_name = targetHead)]
    pub fn target_head(&self) -> Buffer {
        Buffer::from(self.inner.target_head().digest().into_bytes().to_vec())
    }

    /// Exact discovered common ancestor.
    #[must_use]
    #[napi(getter, js_name = commonAncestor)]
    pub fn common_ancestor(&self) -> Buffer {
        Buffer::from(self.inner.common_ancestor().digest().into_bytes().to_vec())
    }

    /// Applies this immutable plan through one exact target-head CAS.
    ///
    /// # Errors
    ///
    /// Rejects malformed identities or authenticated storage and publication failures.
    #[napi]
    pub async fn apply(
        &self,
        if_target: Buffer,
        idempotency_key: Option<Buffer>,
    ) -> Result<NativeJoinResult> {
        let if_target = acyclic_fs::GenerationId::new(Digest::from_bytes(fixed_32(
            &if_target,
            "target generation identity",
        )?));
        let idempotency_key = native_idempotency_key(idempotency_key)?;
        self.inner
            .apply(ApplyOptions {
                if_target,
                idempotency_key,
            })
            .await
            .map(native_join_result)
            .map_err(napi_error)
    }
}

#[napi]
#[allow(missing_docs)]
#[allow(clippy::missing_errors_doc)]
impl NativeGeneration {
    /// Content-addressed generation identity.
    #[must_use]
    #[napi(getter)]
    pub fn id(&self) -> Buffer {
        Buffer::from(self.inner.id().digest().into_bytes().to_vec())
    }

    /// Owning opaque workspace identity.
    #[must_use]
    #[napi(getter, js_name = workspaceId)]
    pub fn workspace_id(&self) -> Buffer {
        Buffer::from(self.inner.workspace_id().into_bytes().to_vec())
    }

    /// Reads one complete file from this exact immutable state.
    ///
    /// # Errors
    ///
    /// Returns path, kind, bound, authentication, or storage failures.
    #[napi]
    pub async fn read(&self, path: String, maximum_bytes: BigInt) -> Result<Buffer> {
        Box::pin(self.inner.read(&path, bigint_u64(&maximum_bytes)?))
            .await
            .map(|bytes| Buffer::from(bytes.to_vec()))
            .map_err(napi_error)
    }

    #[napi(js_name = readRange)]
    pub async fn read_range(&self, path: String, offset: BigInt, length: BigInt) -> Result<Buffer> {
        Box::pin(
            self.inner
                .read_range(&path, bigint_u64(&offset)?, bigint_u64(&length)?),
        )
        .await
        .map(|bytes| Buffer::from(bytes.to_vec()))
        .map_err(napi_error)
    }

    #[napi]
    pub async fn stat(&self, path: String) -> Result<NativeWorkspaceStat> {
        Box::pin(self.inner.stat(&path))
            .await
            .map(native_workspace_stat)
            .map_err(napi_error)
    }

    #[napi(js_name = listDirectory)]
    pub async fn list_directory(
        &self,
        path: String,
        after: Option<NativeWorkspaceName>,
        maximum_entries: u32,
    ) -> Result<NativeWorkspaceDirectoryPage> {
        let after = after.map(native_workspace_name).transpose()?;
        Box::pin(
            self.inner
                .list_directory(&path, after.as_ref(), maximum_entries),
        )
        .await
        .map(native_workspace_directory_page)
        .map_err(napi_error)
    }

    #[napi(js_name = readSymbolicLink)]
    pub async fn read_symbolic_link(&self, path: String) -> Result<Buffer> {
        Box::pin(self.inner.read_symbolic_link(&path))
            .await
            .map(|bytes| Buffer::from(bytes.to_vec()))
            .map_err(napi_error)
    }

    #[napi(js_name = planExtents)]
    pub async fn plan_extents(
        &self,
        path: String,
        offset: BigInt,
        length: BigInt,
        maximum_spans: u32,
    ) -> Result<NativeWorkspaceExtentPlan> {
        Box::pin(self.inner.plan_extents(
            &path,
            bigint_u64(&offset)?,
            bigint_u64(&length)?,
            maximum_spans,
        ))
        .await
        .map(native_workspace_extent_plan)
        .map_err(napi_error)
    }

    /// Retains this exact generation under one opaque identity.
    ///
    /// # Errors
    ///
    /// Returns identity, conflict, authority, authentication, or storage failures.
    #[napi]
    pub async fn pin(&self, identity: String) -> Result<NativeGeneration> {
        self.inner
            .pin(identity)
            .await
            .map(|pin| NativeGeneration {
                inner: pin.generation().clone(),
            })
            .map_err(napi_error)
    }
}

fn native_idempotency_key(value: Option<Buffer>) -> Result<IdempotencyKey> {
    value.map_or_else(
        || Ok(IdempotencyKey::new()),
        |value| {
            value
                .as_ref()
                .try_into()
                .map(IdempotencyKey::from_bytes)
                .map_err(|_| {
                    Error::new(
                        Status::InvalidArg,
                        "idempotency key must be exactly 16 bytes",
                    )
                })
        },
    )
}

fn native_source_state(state: SourceState) -> NativeSourceResult {
    let (status, reason) = match state {
        SourceState::Clean => ("clean", None),
        SourceState::PendingCapture => ("pending-capture", None),
        SourceState::NeedsRescan(reason) => ("needs-rescan", Some(watch_reason(reason).to_owned())),
        SourceState::Conflict => ("conflict", None),
        SourceState::Sealed => ("sealed", None),
    };
    NativeSourceResult {
        status: status.to_owned(),
        reason,
        generation_id: None,
    }
}

fn native_join_history(value: &str) -> Result<JoinHistory> {
    match value {
        "merge" => Ok(JoinHistory::Merge),
        "rebase" => Ok(JoinHistory::Rebase),
        "squash" => Ok(JoinHistory::Squash),
        "cherry-pick" => Ok(JoinHistory::CherryPick),
        _ => Err(Error::new(
            Status::InvalidArg,
            "join history must be merge, rebase, squash, or cherry-pick",
        )),
    }
}

fn native_join_result(
    outcome: JoinOutcome<LocalAuthorityBackend, LocalObjectBackend>,
) -> NativeJoinResult {
    let generation = |status: &str, generation: NativeLocalGeneration| NativeJoinResult {
        status: status.to_owned(),
        generation_id: Some(Buffer::from(generation.id().digest().into_bytes().to_vec())),
        conflicts: Vec::new(),
        truncated: false,
    };
    match outcome {
        JoinOutcome::Applied(value) => generation("applied", value),
        JoinOutcome::AlreadyApplied(value) => generation("already-applied", value),
        JoinOutcome::NoChanges(value) => generation("no-changes", value),
        JoinOutcome::StaleTarget(value) => generation("stale-target", value),
        JoinOutcome::Conflicted {
            conflicts,
            truncated,
        } => NativeJoinResult {
            status: "conflicted".to_owned(),
            generation_id: None,
            conflicts: conflicts.into_iter().map(encode_merge_conflict).collect(),
            truncated,
        },
        JoinOutcome::Fenced => NativeJoinResult {
            status: "fenced".to_owned(),
            generation_id: None,
            conflicts: Vec::new(),
            truncated: false,
        },
        JoinOutcome::IdempotencyConflict => NativeJoinResult {
            status: "idempotency-conflict".to_owned(),
            generation_id: None,
            conflicts: Vec::new(),
            truncated: false,
        },
    }
}

fn native_workspace_rebase_result(
    outcome: WorkspaceRebase<LocalAuthorityBackend, LocalObjectBackend>,
) -> NativeWorkspaceRebaseResult {
    let generation =
        |status: &str, generation: NativeLocalGeneration| NativeWorkspaceRebaseResult {
            status: status.to_owned(),
            generation_id: Some(Buffer::from(generation.id().digest().into_bytes().to_vec())),
            conflicts: Vec::new(),
            truncated: false,
        };
    match outcome {
        WorkspaceRebase::Rebased(value) => generation("rebased", value),
        WorkspaceRebase::AlreadyRebased(value) => generation("already-rebased", value),
        WorkspaceRebase::Current(value) => generation("current", value),
        WorkspaceRebase::Stale(value) => generation("stale", value),
        WorkspaceRebase::Conflicted {
            conflicts,
            truncated,
        } => NativeWorkspaceRebaseResult {
            status: "conflicted".to_owned(),
            generation_id: None,
            conflicts: conflicts.into_iter().map(encode_merge_conflict).collect(),
            truncated,
        },
        WorkspaceRebase::Fenced => NativeWorkspaceRebaseResult {
            status: "fenced".to_owned(),
            generation_id: None,
            conflicts: Vec::new(),
            truncated: false,
        },
        WorkspaceRebase::IdempotencyConflict => NativeWorkspaceRebaseResult {
            status: "idempotency-conflict".to_owned(),
            generation_id: None,
            conflicts: Vec::new(),
            truncated: false,
        },
    }
}

fn native_reconcile_outcome(
    outcome: ReconcileOutcome<LocalAuthorityBackend, LocalObjectBackend>,
) -> NativeSourceResult {
    match outcome {
        ReconcileOutcome::Clean(generation) => NativeSourceResult {
            status: "clean".to_owned(),
            reason: None,
            generation_id: Some(Buffer::from(generation.id().digest().into_bytes().to_vec())),
        },
        ReconcileOutcome::NeedsRescan(reason) => NativeSourceResult {
            status: "needs-rescan".to_owned(),
            reason: Some(watch_reason(reason).to_owned()),
            generation_id: None,
        },
        ReconcileOutcome::Conflict => NativeSourceResult {
            status: "conflict".to_owned(),
            reason: None,
            generation_id: None,
        },
    }
}

#[napi]
impl NativeWorkspaceMount {
    /// Exact native mount path.
    #[must_use]
    #[napi(getter)]
    pub fn path(&self) -> String {
        self.path.clone()
    }

    /// Publishes every pending authored effect without detaching.
    ///
    /// # Errors
    ///
    /// Returns an unmounted-handle or publication failure.
    #[must_use]
    #[napi]
    pub fn sync(&self) -> AsyncTask<NativeWorkspaceMountSyncTask> {
        AsyncTask::new(NativeWorkspaceMountSyncTask {
            inner: Arc::clone(&self.inner),
        })
    }

    /// Publishes pending effects and detaches exactly once.
    ///
    /// A failed publication or detach retains the owner for an exact retry.
    ///
    /// # Errors
    ///
    /// Returns a publication or native detach failure.
    #[must_use]
    #[napi]
    pub fn unmount(&self) -> AsyncTask<NativeWorkspaceUnmountTask> {
        AsyncTask::new(NativeWorkspaceUnmountTask {
            inner: Arc::clone(&self.inner),
        })
    }
}

impl Task for NativeWorkspaceMountSyncTask {
    type Output = ();
    type JsValue = ();

    fn compute(&mut self) -> Result<Self::Output> {
        let guard = self
            .inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "workspace mount lock is poisoned"))?;
        guard
            .as_ref()
            .ok_or_else(|| Error::new(Status::InvalidArg, "workspace mount is detached"))?
            .sync_blocking()
            .map_err(napi_error)
    }

    fn resolve(&mut self, _env: Env, output: Self::Output) -> Result<Self::JsValue> {
        Ok(output)
    }
}

impl Task for NativeWorkspaceUnmountTask {
    type Output = bool;
    type JsValue = bool;

    fn compute(&mut self) -> Result<Self::Output> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "workspace mount lock is poisoned"))?;
        let Some(mount) = guard.as_mut() else {
            return Ok(false);
        };
        mount.unmount_blocking().map_err(napi_error)?;
        guard.take();
        Ok(true)
    }

    fn resolve(&mut self, _env: Env, output: Self::Output) -> Result<Self::JsValue> {
        Ok(output)
    }
}

#[napi]
impl NativeWorkspaceTransaction {
    /// Creates every absent directory on one canonical path.
    ///
    /// # Errors
    ///
    /// Returns path, kind, authentication, or storage failures.
    #[napi(js_name = createDirAll)]
    pub async fn create_dir_all(&self, path: String) -> Result<()> {
        self.inner
            .lock()
            .await
            .create_dir_all(&path)
            .await
            .map_err(napi_error)
    }

    /// Creates exactly one empty directory.
    ///
    /// # Errors
    ///
    /// Returns path, kind, authentication, or storage failures.
    #[napi(js_name = createDirectory)]
    pub async fn create_directory(&self, path: String) -> Result<()> {
        self.inner
            .lock()
            .await
            .create_directory(&path)
            .await
            .map_err(napi_error)
    }

    /// Creates one symbolic link with an opaque target.
    ///
    /// # Errors
    ///
    /// Returns path, profile, authentication, or storage failures.
    #[napi(js_name = createSymbolicLink)]
    pub async fn create_symbolic_link(&self, path: String, target: Buffer) -> Result<()> {
        self.inner
            .lock()
            .await
            .create_symbolic_link(&path, bytes::Bytes::from(target.to_vec()))
            .await
            .map_err(napi_error)
    }

    /// Creates or replaces one complete file inside this transaction.
    ///
    /// # Errors
    ///
    /// Returns path, kind, size, authentication, or storage failures.
    #[napi]
    pub async fn write(&self, path: String, bytes: Buffer) -> Result<()> {
        self.inner
            .lock()
            .await
            .write(&path, bytes::Bytes::from(bytes.to_vec()))
            .await
            .map_err(napi_error)
    }

    /// Removes one existing namespace binding inside this transaction.
    ///
    /// # Errors
    ///
    /// Returns path, absence, authentication, or storage failures.
    #[napi]
    pub async fn remove(&self, path: String) -> Result<()> {
        self.inner
            .lock()
            .await
            .remove(&path)
            .await
            .map_err(napi_error)
    }

    /// Clones one complete regular file without copying its body.
    ///
    /// # Errors
    ///
    /// Returns path, kind, authentication, or storage failures.
    #[napi]
    pub async fn copy(&self, source: String, destination: String) -> Result<()> {
        self.inner
            .lock()
            .await
            .copy(&source, &destination)
            .await
            .map_err(napi_error)
    }

    /// Atomically renames one namespace binding inside this transaction.
    ///
    /// # Errors
    ///
    /// Returns path, absence, authentication, or storage failures.
    #[napi]
    pub async fn rename(&self, source: String, destination: String) -> Result<()> {
        self.inner
            .lock()
            .await
            .rename(&source, &destination)
            .await
            .map_err(napi_error)
    }

    /// Creates one same-workspace hard link.
    ///
    /// # Errors
    ///
    /// Returns path, kind, authentication, or storage failures.
    #[napi(js_name = hardLink)]
    pub async fn hard_link(&self, source: String, destination: String) -> Result<()> {
        self.inner
            .lock()
            .await
            .hard_link(&source, &destination)
            .await
            .map_err(napi_error)
    }

    /// Replaces one sparse regular-file range.
    ///
    /// # Errors
    ///
    /// Returns range, path, kind, authentication, or storage failures.
    #[napi(js_name = writeRange)]
    pub async fn write_range(&self, path: String, offset: BigInt, bytes: Buffer) -> Result<()> {
        self.inner
            .lock()
            .await
            .write_range(
                &path,
                bigint_u64(&offset)?,
                bytes::Bytes::from(bytes.to_vec()),
            )
            .await
            .map_err(napi_error)
    }

    /// Changes one regular file's logical length.
    ///
    /// # Errors
    ///
    /// Returns size, path, kind, authentication, or storage failures.
    #[napi]
    pub async fn resize(&self, path: String, logical_bytes: BigInt) -> Result<()> {
        self.inner
            .lock()
            .await
            .resize(&path, bigint_u64(&logical_bytes)?)
            .await
            .map_err(napi_error)
    }

    /// Punches a hole or installs allocated zeros over one exact range.
    ///
    /// # Errors
    ///
    /// Returns range, capability, authentication, or storage failures.
    #[napi(js_name = zeroRange)]
    pub async fn zero_range(
        &self,
        path: String,
        offset: BigInt,
        length: BigInt,
        allocated: bool,
        extend: bool,
    ) -> Result<()> {
        self.inner
            .lock()
            .await
            .zero_range(
                &path,
                ByteRange {
                    offset: bigint_u64(&offset)?,
                    length: bigint_u64(&length)?,
                },
                allocated,
                extend,
            )
            .await
            .map_err(napi_error)
    }

    /// Preallocates one sparse range without replacing content.
    ///
    /// # Errors
    ///
    /// Returns range, capability, authentication, or storage failures.
    #[napi]
    pub async fn preallocate(
        &self,
        path: String,
        offset: BigInt,
        length: BigInt,
        keep_size: bool,
    ) -> Result<()> {
        self.inner
            .lock()
            .await
            .preallocate(
                &path,
                ByteRange {
                    offset: bigint_u64(&offset)?,
                    length: bigint_u64(&length)?,
                },
                keep_size,
            )
            .await
            .map_err(napi_error)
    }

    /// Clones one immutable range without reading file bytes.
    ///
    /// # Errors
    ///
    /// Returns range, path, kind, authentication, or storage failures.
    #[napi(js_name = cloneRange)]
    pub async fn clone_range(
        &self,
        source: String,
        source_offset: BigInt,
        destination: String,
        destination_offset: BigInt,
        length: BigInt,
    ) -> Result<()> {
        self.inner
            .lock()
            .await
            .clone_range(
                &source,
                bigint_u64(&source_offset)?,
                &destination,
                bigint_u64(&destination_offset)?,
                bigint_u64(&length)?,
            )
            .await
            .map_err(napi_error)
    }

    /// Publishes the complete candidate through one idempotent head CAS.
    ///
    /// # Errors
    ///
    /// Returns closure, authentication, authority, or storage failures.
    #[napi]
    pub async fn commit(&self) -> Result<NativeWorkspaceCommit> {
        self.inner
            .lock()
            .await
            .commit()
            .await
            .map(workspace_commit)
            .map_err(napi_error)
    }

    /// Safely advances this retained candidate and sparsely replays its work.
    ///
    /// # Errors
    ///
    /// Returns dependency-probe, authentication, storage, or replay failures.
    #[napi]
    pub async fn rebase(&self, maximum_conflicts: u32) -> Result<NativeTransactionRebaseResult> {
        self.inner
            .lock()
            .await
            .rebase(maximum_conflicts)
            .await
            .map(native_transaction_rebase)
            .map_err(napi_error)
    }
}

fn native_transaction_rebase(
    outcome: TransactionRebase<LocalAuthorityBackend, LocalObjectBackend>,
) -> NativeTransactionRebaseResult {
    match outcome {
        TransactionRebase::Rebased(generation) => NativeTransactionRebaseResult {
            status: "rebased".to_owned(),
            generation_id: Some(Buffer::from(generation.id().digest().into_bytes().to_vec())),
            conflicts: Vec::new(),
            truncated: false,
        },
        TransactionRebase::Conflicted {
            conflicts,
            truncated,
        } => NativeTransactionRebaseResult {
            status: "conflicted".to_owned(),
            generation_id: None,
            conflicts: conflicts
                .into_iter()
                .map(native_transaction_conflict)
                .collect(),
            truncated,
        },
    }
}

fn native_transaction_conflict(value: TransactionConflict) -> NativeTransactionConflict {
    let mut result = NativeTransactionConflict {
        region: String::new(),
        file_id: None,
        directory_id: None,
        offset: None,
        length: None,
        sparse_target: None,
        name: None,
        maximum_entries: None,
        usage: match value.usage {
            TransactionDependencyUse::Observation => "observation",
            TransactionDependencyUse::Mutation => "mutation",
            TransactionDependencyUse::ObservationAndMutation => "observation-and-mutation",
        }
        .to_owned(),
        expected: value
            .expected
            .map(|digest| Buffer::from(digest.into_bytes().to_vec())),
        actual: value
            .actual
            .map(|digest| Buffer::from(digest.into_bytes().to_vec())),
    };
    match value.region {
        TransactionConflictRegion::FileRecord(file_id) => {
            "file-record".clone_into(&mut result.region);
            result.file_id = Some(Buffer::from(file_id.into_bytes().to_vec()));
        }
        TransactionConflictRegion::Metadata(file_id) => {
            "metadata".clone_into(&mut result.region);
            result.file_id = Some(Buffer::from(file_id.into_bytes().to_vec()));
        }
        TransactionConflictRegion::FileLength(file_id) => {
            "file-length".clone_into(&mut result.region);
            result.file_id = Some(Buffer::from(file_id.into_bytes().to_vec()));
        }
        TransactionConflictRegion::ContentRange {
            file_id,
            offset,
            length,
        } => {
            "content-range".clone_into(&mut result.region);
            result.file_id = Some(Buffer::from(file_id.into_bytes().to_vec()));
            result.offset = Some(bigint(offset));
            result.length = Some(bigint(length));
        }
        TransactionConflictRegion::SparseSeek {
            file_id,
            offset,
            target,
        } => {
            "sparse-seek".clone_into(&mut result.region);
            result.file_id = Some(Buffer::from(file_id.into_bytes().to_vec()));
            result.offset = Some(bigint(offset));
            result.sparse_target = Some(
                match target {
                    TransactionSparseSeek::Data => "data",
                    TransactionSparseSeek::Hole => "hole",
                }
                .to_owned(),
            );
        }
        TransactionConflictRegion::DirectoryName { directory_id, name } => {
            "directory-name".clone_into(&mut result.region);
            result.directory_id = Some(Buffer::from(directory_id.into_bytes().to_vec()));
            result.name = Some(NativeWorkspaceName {
                encoding: native_name_encoding(name.encoding()).to_owned(),
                bytes: Buffer::from(name.as_bytes().to_vec()),
            });
        }
        TransactionConflictRegion::DirectoryRange {
            directory_id,
            after,
            maximum_entries,
        } => {
            "directory-range".clone_into(&mut result.region);
            result.directory_id = Some(Buffer::from(directory_id.into_bytes().to_vec()));
            result.name = after.map(|name| NativeWorkspaceName {
                encoding: native_name_encoding(name.encoding()).to_owned(),
                bytes: Buffer::from(name.as_bytes().to_vec()),
            });
            result.maximum_entries = Some(maximum_entries);
        }
    }
    result
}

fn workspace_commit(
    outcome: TransactionCommit<LocalAuthorityBackend, LocalObjectBackend>,
) -> NativeWorkspaceCommit {
    match outcome {
        TransactionCommit::Committed(generation) => NativeWorkspaceCommit {
            status: "committed".to_owned(),
            generation_id: Some(Buffer::from(generation.id().digest().into_bytes().to_vec())),
        },
        TransactionCommit::AlreadyCommitted(generation) => NativeWorkspaceCommit {
            status: "already-committed".to_owned(),
            generation_id: Some(Buffer::from(generation.id().digest().into_bytes().to_vec())),
        },
        TransactionCommit::Conflict { actual } => NativeWorkspaceCommit {
            status: "conflict".to_owned(),
            generation_id: Some(Buffer::from(actual.id().digest().into_bytes().to_vec())),
        },
        TransactionCommit::Fenced => NativeWorkspaceCommit {
            status: "fenced".to_owned(),
            generation_id: None,
        },
        TransactionCommit::IdempotencyConflict => NativeWorkspaceCommit {
            status: "idempotency-conflict".to_owned(),
            generation_id: None,
        },
    }
}

#[napi]
impl NativeFs {
    /// Opens one bounded embedded local engine rooted at `root`.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error when the durable local stores cannot open.
    #[napi(constructor)]
    pub fn new(root: String, object_cache: NativeObjectCacheOptions) -> Result<Self> {
        let mut options = LocalOptions::new(root);
        options.object_cache = native_object_cache_options(object_cache)?;
        let inner = LocalFs::local(options).map_err(napi_error)?;
        Ok(Self {
            inner: Arc::new(inner),
            cancellation: CancellationToken::new(),
        })
    }

    /// Returns compile-time capability evidence without probing user paths.
    #[must_use]
    #[napi(getter)]
    pub fn capabilities(&self) -> NativeCapabilities {
        native_capabilities()
    }

    /// Returns exact process-local immutable-object accelerator telemetry.
    ///
    /// # Errors
    ///
    /// Fails closed if accelerator synchronization was poisoned.
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

    /// Discards resident accelerator state without changing durable storage.
    ///
    /// # Errors
    ///
    /// Fails closed if accelerator synchronization was poisoned.
    #[napi]
    pub fn clear_object_cache(&self) -> Result<()> {
        self.inner.clear_object_cache().map_err(napi_error)
    }

    /// Creates both canonical speculation engines against one volume
    /// generation while retaining this filesystem's shared object backend.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed identities or invalid speculation bounds.
    #[allow(clippy::needless_pass_by_value)] // N-API owns JavaScript buffers at the ABI boundary.
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

    /// Cooperatively cancels future work owned by this handle.
    #[napi]
    pub fn cancel(&self) {
        self.cancellation.cancel();
    }

    /// Returns whether this handle has been cancelled.
    #[must_use]
    #[napi(getter)]
    pub fn cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    /// Creates an independently cancellable handle over the same embedded stores.
    #[must_use]
    #[napi]
    pub fn fork(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            cancellation: CancellationToken::new(),
        }
    }

    /// Creates or idempotently reopens one durable named workspace.
    ///
    /// # Errors
    ///
    /// Returns invalid-name, authority, or storage failures.
    #[napi]
    pub async fn create_workspace(&self, name: String) -> Result<NativeWorkspace> {
        self.inner
            .create_workspace(name)
            .await
            .map(|inner| NativeWorkspace { inner })
            .map_err(napi_error)
    }

    /// Creates or opens one workspace and attaches an exact native directory source.
    ///
    /// # Errors
    ///
    /// Returns invalid options, unsafe root, watcher, capture, authority, or storage failures.
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
        Box::pin(self.inner.attach_directory(
            name,
            PathBuf::from(path),
            SourceOptions {
                mode,
                maximum_paths: options.maximum_paths,
                maximum_extent_spans: options.maximum_extent_spans,
                maximum_queued_changes: options.maximum_queued_changes,
            },
        ))
        .await
        .map(|inner| NativeWorkspace { inner })
        .map_err(napi_error)
    }

    /// Opens one existing durable named workspace.
    ///
    /// # Errors
    ///
    /// Returns invalid-name, absence, authority, or storage failures.
    #[napi]
    pub async fn open_workspace(&self, name: String) -> Result<NativeWorkspace> {
        self.inner
            .open_workspace(name)
            .await
            .map(|inner| NativeWorkspace { inner })
            .map_err(napi_error)
    }

    /// Creates one independently configured volume.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for unsupported durability, storage failure,
    /// cancellation, authentication failure, or bounded-work exhaustion.
    #[napi]
    pub async fn create_volume(&self, options: NativeVolumeOptions) -> Result<NativeVolume> {
        let config = native_volume_config(options)?;
        let receipt = self
            .inner
            .create_volume(config, boundary_budget(), &self.cancellation)
            .await
            .map_err(napi_error)?;
        Ok(NativeVolume {
            inner: receipt.value,
            cancellation: CancellationToken::new(),
            acquisition_work: receipt.work,
        })
    }

    /// Idempotently creates one caller-selected volume identity.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for malformed identity, incompatible
    /// existing configuration, unsupported semantics, or storage failure.
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
            .create_volume_with_id(volume_id, config, boundary_budget(), &self.cancellation)
            .await
            .map_err(napi_error)?;
        Ok(NativeVolume {
            inner: receipt.value,
            cancellation: CancellationToken::new(),
            acquisition_work: receipt.work,
        })
    }

    /// Reopens one existing volume by its canonical 16-byte identity.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for malformed identity bytes, absent or
    /// corrupt authority, cancellation, or bounded-work exhaustion.
    #[napi]
    pub async fn open_volume(&self, volume_id: Buffer) -> Result<NativeVolume> {
        let volume_id = VolumeId::from_bytes(fixed_16(&volume_id)?);
        let receipt = self
            .inner
            .open_volume(volume_id, boundary_budget(), &self.cancellation)
            .await
            .map_err(napi_error)?;
        Ok(NativeVolume {
            inner: receipt.value,
            cancellation: CancellationToken::new(),
            acquisition_work: receipt.work,
        })
    }

    /// Exports one exact authenticated immutable object for resumable transfer.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for malformed identity, absence, corruption,
    /// cancellation, storage failure, or bounded work.
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
                bigint_u64(&maximum_bytes)?,
                boundary_budget(),
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        Ok(NativeFileRead {
            bytes: Buffer::from(receipt.value.bytes.to_vec()),
            work_json: serde_json::to_string(&receipt.work).map_err(napi_error)?,
        })
    }

    /// Idempotently imports one authenticated immutable object.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for malformed identity, digest mismatch,
    /// cancellation, storage failure, or bounded work.
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
                bytes::Bytes::from(bytes.to_vec()),
                boundary_budget(),
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        mutation_result(receipt.work)
    }

    /// Exports one bounded manifest-ordered immutable-object page.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for malformed manifests, invalid cursors,
    /// cancellation, storage, allocation, or bounded-work failures.
    #[napi]
    pub async fn export_generation_batch(
        &self,
        manifest: NativeExportManifest,
        cursor: BigInt,
        maximum_objects: u32,
        maximum_object_bytes: BigInt,
    ) -> Result<NativeGenerationTransferBatch> {
        let manifest = decode_export_manifest(&manifest)?;
        let receipt = self
            .inner
            .export_generation_batch(
                &manifest,
                TransferCursor::new(bigint_u64(&cursor)?),
                maximum_objects,
                bigint_u64(&maximum_object_bytes)?,
                boundary_budget(),
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
            work_json: serde_json::to_string(&receipt.work).map_err(napi_error)?,
        })
    }

    /// Idempotently imports one manifest-aligned immutable-object page.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for malformed manifests, cursor/body bounds,
    /// cancellation, storage, or bounded-work failures.
    #[napi]
    pub async fn import_generation_batch(
        &self,
        manifest: NativeExportManifest,
        cursor: BigInt,
        objects: Vec<Buffer>,
        maximum_objects: u32,
    ) -> Result<NativeGenerationTransferCursor> {
        let manifest = decode_export_manifest(&manifest)?;
        let objects = objects
            .into_iter()
            .map(|object| bytes::Bytes::from(object.to_vec()))
            .collect::<Vec<_>>();
        let receipt = self
            .inner
            .import_generation_batch(
                &manifest,
                TransferCursor::new(bigint_u64(&cursor)?),
                &objects,
                maximum_objects,
                boundary_budget(),
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        Ok(NativeGenerationTransferCursor {
            next_object: BigInt::from(receipt.value.next_object()),
            work_json: serde_json::to_string(&receipt.work).map_err(napi_error)?,
        })
    }

    /// Restores authority only after authenticating the complete imported closure.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for malformed manifest fields, incomplete or
    /// corrupt closure, conflicting authority, cancellation, or bounded work.
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
                boundary_budget(),
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        Ok(NativeVolume {
            inner: receipt.value,
            cancellation: CancellationToken::new(),
            acquisition_work: receipt.work,
        })
    }

    /// Releases this JavaScript handle. Store ownership remains reference counted.
    #[napi]
    pub fn close(&self) {}
}

#[napi]
impl NativeVolume {
    /// Returns the canonical 16-byte volume identity.
    #[must_use]
    #[napi(getter)]
    pub fn id(&self) -> Buffer {
        Buffer::from(self.inner.id().into_bytes().to_vec())
    }

    /// Returns exact bounded work used to acquire this volume handle.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error if the canonical JSON receipt cannot be encoded.
    #[napi(getter)]
    pub fn acquisition_work_json(&self) -> Result<String> {
        serde_json::to_string(&self.acquisition_work).map_err(napi_error)
    }

    /// Computes one bounded Merkle-aware semantic generation diff.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for malformed identities, foreign/corrupt
    /// generations, cancellation, storage, allocation, or bounded work.
    #[napi]
    pub async fn diff_generations(
        &self,
        before: Buffer,
        after: Buffer,
        maximum_changes: u32,
    ) -> Result<NativeGenerationDiff> {
        let before = acyclic_fs::GenerationId::new(Digest::from_bytes(fixed_32(
            &before,
            "before generation identity",
        )?));
        let after = acyclic_fs::GenerationId::new(Digest::from_bytes(fixed_32(
            &after,
            "after generation identity",
        )?));
        let receipt = self
            .inner
            .diff_generations(
                before,
                after,
                maximum_changes,
                boundary_budget(),
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        encode_generation_diff(receipt.value, receipt.work)
    }

    /// Opens a head checkout with explicit consistency and access semantics.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for invalid modes, unavailable generations,
    /// cancellation, storage failure, or bounded-work exhaustion.
    #[napi]
    pub async fn checkout(&self, options: NativeCheckoutOptions) -> Result<NativeCheckout> {
        let consistency = match options.consistency.as_str() {
            "pinned" => ConsistencyMode::Pinned,
            "tracking-safe" => ConsistencyMode::TrackingSafe,
            "live" => ConsistencyMode::Live,
            "manual" => ConsistencyMode::Manual,
            _ => {
                return Err(Error::new(
                    Status::InvalidArg,
                    "invalid checkout consistency",
                ));
            }
        };
        let access = match options.access.as_str() {
            "read-only" => AccessMode::ReadOnly,
            "read-write" => AccessMode::ReadWrite,
            _ => return Err(Error::new(Status::InvalidArg, "invalid checkout access")),
        };
        let mutations = match options.mutation_mode.as_str() {
            "none" => MutationMode::None,
            "private-cow" => MutationMode::PrivateOverlay,
            "direct-live" => MutationMode::DirectLive,
            _ => {
                return Err(Error::new(
                    Status::InvalidArg,
                    "invalid checkout mutation mode",
                ));
            }
        };
        let mode = CheckoutMode {
            access,
            consistency,
            mutations,
        };
        let receipt = self
            .inner
            .checkout(
                GenerationSelector::Head,
                mode,
                boundary_budget(),
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        Ok(NativeCheckout {
            inner: Arc::new(SharedCheckout::new(receipt.value)),
            config: self.inner.config(),
            cancellation: CancellationToken::new(),
            acquisition_work: receipt.work,
        })
    }
}

#[napi]
impl NativeCheckout {
    /// Returns exact bounded work used to acquire this checkout handle.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error if the canonical JSON receipt cannot be encoded.
    #[napi(getter)]
    pub fn acquisition_work_json(&self) -> Result<String> {
        serde_json::to_string(&self.acquisition_work).map_err(napi_error)
    }

    /// Applies one ordered sparse mutation batch atomically within this volume.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for a malformed operation, incompatible
    /// profile, conflict, storage failure, cancellation, or bounded work.
    #[napi]
    pub async fn apply_transaction(
        &self,
        operations: Vec<NativeTransactionOperation>,
    ) -> Result<NativeTransactionResult> {
        let maximum =
            usize::try_from(self.config.limits.maximum_mutations_per_batch).unwrap_or(usize::MAX);
        if operations.len() > maximum || operations.capacity() > maximum {
            return Err(Error::new(
                Status::InvalidArg,
                "transaction exceeds the configured mutation bound",
            ));
        }
        let authored = operations
            .into_iter()
            .map(|operation| native_authored_transaction(operation, self.config.limits))
            .collect::<Result<Vec<_>>>()?;
        let mut checkout = self.inner.lock().await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = checkout
            .apply_authored_transaction(authored, boundary_budget(), &self.cancellation)
            .await
            .map_err(napi_error)?;
        Ok(NativeTransactionResult {
            created_file_ids: receipt
                .value
                .created_file_ids
                .into_iter()
                .map(|identity| identity.map(|value| Buffer::from(value.into_bytes().to_vec())))
                .collect(),
            work_json: serde_json::to_string(&receipt.work).map_err(napi_error)?,
        })
    }

    /// Builds an immutable candidate generation without publishing authority.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for invalid checkout state, corruption,
    /// cancellation, storage failure, or bounded-work exhaustion.
    #[napi]
    pub async fn checkpoint(&self) -> Result<NativeCheckpointResult> {
        let checkout = self.inner.lock().await;
        let receipt = checkout
            .checkpoint(boundary_budget(), &self.cancellation)
            .await
            .map_err(napi_error)?;
        Ok(NativeCheckpointResult {
            generation_id: Buffer::from(receipt.value.digest().into_bytes().to_vec()),
            work_json: serde_json::to_string(&receipt.work).map_err(napi_error)?,
        })
    }

    /// Explicitly advances a clean manual checkout to the authority head.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for incompatible mode, pending mutations,
    /// authority/storage failure, corruption, cancellation, or bounded work.
    #[napi]
    pub async fn refresh_head(&self) -> Result<NativeCheckpointResult> {
        let mut checkout = self.inner.lock().await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = checkout
            .refresh_head(boundary_budget(), &self.cancellation)
            .await
            .map_err(napi_error)?;
        encode_checkpoint_receipt(&receipt)
    }

    /// Explicitly performs observation-safe synchronization for a live checkout.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for incompatible mode, an observed-region
    /// conflict, authority/storage failure, corruption, cancellation, or bounded work.
    #[napi]
    pub async fn refresh_live(&self) -> Result<NativeCheckpointResult> {
        let mut checkout = self.inner.lock().await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = checkout
            .refresh_live(boundary_budget(), &self.cancellation)
            .await
            .map_err(napi_error)?;
        encode_checkpoint_receipt(&receipt)
    }

    /// Builds a deterministic complete manifest for resumable generation transfer.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for checkpoint, closure, authentication,
    /// cancellation, storage, serialization, or bounded-work failure.
    #[napi]
    pub async fn export_manifest(&self) -> Result<NativeExportManifest> {
        let checkout = self.inner.lock().await;
        let receipt = checkout
            .export_manifest(boundary_budget(), &self.cancellation)
            .await
            .map_err(napi_error)?;
        encode_export_manifest(receipt.value, receipt.work)
    }

    /// Prepares a bounded two-parent merge against the current authority head.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for malformed identity, invalid checkout
    /// state, non-head parent, corrupt storage, cancellation, or bounded work.
    #[napi]
    pub async fn prepare_merge(
        &self,
        theirs: Buffer,
        maximum_changes: u32,
        maximum_conflicts: u32,
    ) -> Result<NativeMergePreparation> {
        let theirs = acyclic_fs::GenerationId::new(Digest::from_bytes(fixed_32(
            &theirs,
            "merge generation identity",
        )?));
        let mut checkout = self.inner.lock().await;
        let receipt = checkout
            .prepare_merge(
                theirs,
                maximum_changes,
                maximum_conflicts,
                boundary_budget(),
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        encode_merge_preparation(receipt.value, receipt.work)
    }
    /// Mounts this checkout through the target's real native namespace driver.
    ///
    /// # Errors
    ///
    /// Returns a typed error for missing platform capability, invalid mount
    /// destination, unavailable writable capture, or driver startup failure.
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
    ///
    /// # Errors
    ///
    /// Returns a typed error for invalid bounds/destination, unsupported exact
    /// host semantics, canonical engine failure, cancellation, or host I/O.
    #[napi]
    pub async fn materialize(
        &self,
        options: NativeMaterializeOptions,
    ) -> Result<NativeMaterializationResult> {
        let mut checkout = self.inner.lock().await;
        let receipt = Box::pin(materialize_checkout(
            &mut checkout,
            &MaterializeOptions {
                destination: options.destination.into(),
                maximum_directory_entries: options.maximum_directory_entries,
                maximum_extent_spans: options.maximum_extent_spans,
                transfer_bytes: bigint_u64(&options.transfer_bytes)?,
            },
            boundary_budget(),
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
            work_json: serde_json::to_string(&receipt.work).map_err(napi_error)?,
        })
    }

    /// Captures final host state for a bounded exact path set in one atomic
    /// checkout transaction.
    ///
    /// # Errors
    ///
    /// Returns a typed error for malformed paths, invalid source root/bounds,
    /// unsupported exact host semantics, source races/I/O, cancellation,
    /// canonical engine failure, or work exhaustion.
    #[napi]
    pub async fn capture(
        &self,
        source_root: String,
        paths: Vec<String>,
        maximum_paths: u32,
        maximum_extent_spans: u32,
    ) -> Result<NativeCaptureResult> {
        let source_root = PathBuf::from(source_root);
        let expected_root_identity = capture_root_identity(&source_root).map_err(napi_error)?;
        let paths = paths
            .iter()
            .map(|path| native_path(path, self.config.limits))
            .collect::<Result<Vec<_>>>()?;
        let mut checkout = self.inner.lock().await;
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
            boundary_budget(),
            &self.cancellation,
        ))
        .await
        .map_err(napi_error)?;
        Ok(NativeCaptureResult {
            examined_paths: bigint(receipt.value.examined_paths),
            changed_paths: bigint(receipt.value.changed_paths),
            staged_file_bytes: bigint(receipt.value.staged_file_bytes),
            work_json: serde_json::to_string(&receipt.work).map_err(napi_error)?,
        })
    }

    /// Captures the complete bounded host/checkout union in one atomic baseline.
    ///
    /// Use this between a native watcher's `beginRescan` and `finishRescan`.
    /// Native links are not followed and exact profile-specific name bytes are
    /// preserved. The checkout is unchanged unless the complete baseline fits
    /// the supplied path and work bounds.
    ///
    /// # Errors
    ///
    /// Returns a typed error for an invalid root/bound, unrepresentable name,
    /// source race or I/O, unsupported host kind, cancellation, canonical
    /// engine failure, or work exhaustion.
    #[napi]
    pub async fn capture_baseline(
        &self,
        source_root: String,
        maximum_paths: u32,
        maximum_extent_spans: u32,
    ) -> Result<NativeCaptureResult> {
        let source_root = PathBuf::from(source_root);
        let expected_root_identity = capture_root_identity(&source_root).map_err(napi_error)?;
        let mut checkout = self.inner.lock().await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = Box::pin(capture_baseline(
            &mut checkout,
            &CaptureOptions {
                source_root,
                expected_root_identity,
                maximum_paths,
                maximum_extent_spans,
            },
            boundary_budget(),
            &self.cancellation,
        ))
        .await
        .map_err(napi_error)?;
        Ok(NativeCaptureResult {
            examined_paths: bigint(receipt.value.examined_paths),
            changed_paths: bigint(receipt.value.changed_paths),
            staged_file_bytes: bigint(receipt.value.staged_file_bytes),
            work_json: serde_json::to_string(&receipt.work).map_err(napi_error)?,
        })
    }

    /// Opens an exact native watcher using this volume's path bounds.
    ///
    /// # Errors
    ///
    /// Returns a typed error for invalid bounds/root or unavailable native
    /// watcher startup.
    #[napi]
    pub fn watch(
        &self,
        source_root: String,
        maximum_queued_changes: u32,
        recursive: bool,
    ) -> Result<NativeWatcher> {
        let source_root = PathBuf::from(source_root);
        let watcher = FsNativeWatch::open(
            &source_root,
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
    /// Resolves one canonical absolute path without following links.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for malformed paths, corrupt authenticated
    /// state, cancellation, storage failure, or bounded-work exhaustion.
    #[napi]
    pub async fn lookup_no_follow(&self, path: String) -> Result<NativeLookup> {
        let portable = PortablePath::parse(&path, self.config.limits).map_err(napi_error)?;
        let path =
            NamespacePath::from_portable(&portable, self.config.limits).map_err(napi_error)?;
        let mut checkout = self.inner.lock().await;
        let receipt = checkout
            .lookup_no_follow(&path, boundary_budget(), &self.cancellation)
            .await
            .map_err(napi_error)?;
        let record = receipt.value.record;
        Ok(NativeLookup {
            exists: record.is_some(),
            file_id: record.map(|value| Buffer::from(value.file_id.into_bytes().to_vec())),
            file_kind: record.map(|value| file_kind(value.kind).to_owned()),
            resolved_components: u32::from(receipt.value.resolved_components),
            work_json: serde_json::to_string(&receipt.work).map_err(napi_error)?,
        })
    }

    /// Resolves an ordered batch of canonical paths through one shared
    /// authenticated traversal frontier without following links.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for an empty or excessive batch, malformed
    /// paths, corrupt authenticated state, cancellation, storage failure, or
    /// bounded-work exhaustion.
    #[napi]
    pub async fn lookup_batch_no_follow(&self, paths: Vec<String>) -> Result<NativeBatchLookup> {
        let maximum =
            usize::try_from(self.config.limits.maximum_paths_per_batch).unwrap_or(usize::MAX);
        if paths.is_empty() || paths.len() > maximum || paths.capacity() > maximum {
            return Err(Error::new(
                Status::InvalidArg,
                "lookup path batch is empty or excessive",
            ));
        }
        let paths = paths
            .iter()
            .map(|path| native_path(path, self.config.limits))
            .collect::<Result<Vec<_>>>()?;
        let mut checkout = self.inner.lock().await;
        let receipt = checkout
            .lookup_batch_no_follow(&paths, boundary_budget(), &self.cancellation)
            .await
            .map_err(napi_error)?;
        let entries = receipt
            .value
            .entries
            .into_iter()
            .map(|entry| {
                let record = entry.record;
                NativeBatchLookupEntry {
                    exists: record.is_some(),
                    file_id: record.map(|value| Buffer::from(value.file_id.into_bytes().to_vec())),
                    file_kind: record.map(|value| file_kind(value.kind).to_owned()),
                    resolved_components: u32::from(entry.resolved_components),
                }
            })
            .collect();
        Ok(NativeBatchLookup {
            entries,
            retained_allocation_bytes: bigint(receipt.value.retained_allocation_bytes),
            work_json: serde_json::to_string(&receipt.work).map_err(napi_error)?,
        })
    }

    /// Returns one complete no-follow file record and canonical metadata.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for malformed paths, corruption, storage
    /// failure, cancellation, serialization, or bounded-work exhaustion.
    #[napi]
    pub async fn stat_no_follow(&self, path: String) -> Result<NativeStat> {
        let path = native_path(&path, self.config.limits)?;
        let mut checkout = self.inner.lock().await;
        let receipt = checkout
            .lookup_no_follow_with_metadata(&path, boundary_budget(), &self.cancellation)
            .await
            .map_err(napi_error)?;
        let (record, metadata_canonical_bytes) = match receipt.value {
            Some(value) => (
                Some(encode_file_record(value.record)),
                Some(Buffer::from(
                    encode_file_metadata(value.metadata).map_err(napi_error)?,
                )),
            ),
            None => (None, None),
        };
        Ok(NativeStat {
            exists: record.is_some(),
            record,
            metadata_canonical_bytes,
            work_json: serde_json::to_string(&receipt.work).map_err(napi_error)?,
        })
    }

    /// Reads one complete file record by stable identity.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for malformed identity bytes, missing or
    /// corrupt authenticated state, cancellation, storage failure, or
    /// bounded-work exhaustion.
    #[napi]
    pub async fn read_file_record_by_id(&self, file_id: Buffer) -> Result<NativeFileRecordRead> {
        let file_id = FileId::from_bytes(fixed_16(&file_id)?);
        let mut checkout = self.inner.lock().await;
        let receipt = checkout
            .read_file_record_by_id(file_id, boundary_budget(), &self.cancellation)
            .await
            .map_err(napi_error)?;
        Ok(NativeFileRecordRead {
            record: encode_file_record(receipt.value),
            work_json: serde_json::to_string(&receipt.work).map_err(napi_error)?,
        })
    }

    /// Reads complete canonical metadata bytes for one path.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for path, storage, codec, cancellation, or work failure.
    #[napi]
    pub async fn read_metadata(&self, path: String) -> Result<NativeMetadataResult> {
        let path = native_path(&path, self.config.limits)?;
        let mut checkout = self.inner.lock().await;
        let receipt = checkout
            .read_metadata(&path, boundary_budget(), &self.cancellation)
            .await
            .map_err(napi_error)?;
        Ok(NativeMetadataResult {
            canonical_bytes: Buffer::from(encode_file_metadata(receipt.value).map_err(napi_error)?),
            work_json: serde_json::to_string(&receipt.work).map_err(napi_error)?,
        })
    }

    /// Reads complete canonical metadata bytes by stable file identity.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for malformed identity, absence, storage,
    /// codec, cancellation, or bounded-work exhaustion.
    #[napi]
    pub async fn read_metadata_by_id(&self, file_id: Buffer) -> Result<NativeMetadataResult> {
        let file_id = FileId::from_bytes(fixed_16(&file_id)?);
        let mut checkout = self.inner.lock().await;
        let receipt = checkout
            .read_metadata_by_id(file_id, boundary_budget(), &self.cancellation)
            .await
            .map_err(napi_error)?;
        Ok(NativeMetadataResult {
            canonical_bytes: Buffer::from(encode_file_metadata(receipt.value).map_err(napi_error)?),
            work_json: serde_json::to_string(&receipt.work).map_err(napi_error)?,
        })
    }

    /// Atomically replaces complete canonical metadata for one path.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for path, codec, mutation, cancellation, or work failure.
    #[napi]
    pub async fn set_metadata(
        &self,
        path: String,
        canonical_bytes: Buffer,
    ) -> Result<NativeMutationResult> {
        let path = native_path(&path, self.config.limits)?;
        let metadata = decode_file_metadata(
            canonical_bytes.as_ref(),
            native_decode_limits(self.config.limits),
        )
        .map_err(napi_error)?;
        let mut checkout = self.inner.lock().await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = checkout
            .set_metadata(path, metadata, boundary_budget(), &self.cancellation)
            .await
            .map_err(napi_error)?;
        native_mutation(None, receipt.work)
    }

    /// Atomically replaces complete canonical metadata by stable file identity.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for malformed identity, codec, mutation,
    /// cancellation, storage, or bounded-work exhaustion.
    #[napi]
    pub async fn set_metadata_by_id(
        &self,
        file_id: Buffer,
        canonical_bytes: Buffer,
    ) -> Result<NativeMutationResult> {
        let file_id = FileId::from_bytes(fixed_16(&file_id)?);
        let metadata = decode_file_metadata(
            canonical_bytes.as_ref(),
            native_decode_limits(self.config.limits),
        )
        .map_err(napi_error)?;
        let mut checkout = self.inner.lock().await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = checkout
            .set_metadata_by_id(file_id, metadata, boundary_budget(), &self.cancellation)
            .await
            .map_err(napi_error)?;
        native_mutation(None, receipt.work)
    }

    /// Atomically replaces metadata and optional logical size for one path.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for malformed metadata/path, non-regular
    /// resize, storage, cancellation, mutation, or bounded-work failure.
    #[napi]
    pub async fn set_attributes(
        &self,
        path: String,
        canonical_bytes: Buffer,
        logical_bytes: Option<BigInt>,
    ) -> Result<NativeMutationResult> {
        let path = native_path(&path, self.config.limits)?;
        let metadata = decode_file_metadata(
            canonical_bytes.as_ref(),
            native_decode_limits(self.config.limits),
        )
        .map_err(napi_error)?;
        let logical_bytes = logical_bytes.as_ref().map(bigint_u64).transpose()?;
        let mut checkout = self.inner.lock().await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = checkout
            .set_attributes(
                path,
                metadata,
                logical_bytes,
                boundary_budget(),
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        native_mutation(None, receipt.work)
    }

    /// Atomically replaces metadata and optional logical size by file identity.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for malformed identity/metadata, non-regular
    /// resize, storage, cancellation, mutation, or bounded-work failure.
    #[napi]
    pub async fn set_attributes_by_id(
        &self,
        file_id: Buffer,
        canonical_bytes: Buffer,
        logical_bytes: Option<BigInt>,
    ) -> Result<NativeMutationResult> {
        let file_id = FileId::from_bytes(fixed_16(&file_id)?);
        let metadata = decode_file_metadata(
            canonical_bytes.as_ref(),
            native_decode_limits(self.config.limits),
        )
        .map_err(napi_error)?;
        let logical_bytes = logical_bytes.as_ref().map(bigint_u64).transpose()?;
        let mut checkout = self.inner.lock().await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = checkout
            .set_attributes_by_id(
                file_id,
                metadata,
                logical_bytes,
                boundary_budget(),
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        native_mutation(None, receipt.work)
    }

    /// Reads one exact named-attribute value.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for class, name, path, storage, cancellation, or work failure.
    #[napi]
    pub async fn read_named_attribute(
        &self,
        path: String,
        attribute_class: String,
        name: Buffer,
    ) -> Result<NativeNamedAttributeResult> {
        let path = native_path(&path, self.config.limits)?;
        let name = native_attribute_name(&attribute_class, name.to_vec(), self.config.limits)?;
        let mut checkout = self.inner.lock().await;
        let receipt = Box::pin(checkout.read_named_attribute(
            &path,
            &name,
            boundary_budget(),
            &self.cancellation,
        ))
        .await
        .map_err(napi_error)?;
        Ok(NativeNamedAttributeResult {
            exists: receipt.value.is_some(),
            bytes: receipt.value.map(|value| Buffer::from(value.to_vec())),
            work_json: serde_json::to_string(&receipt.work).map_err(napi_error)?,
        })
    }

    /// Returns one bounded ordered named-attribute page.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for class, cursor, path, storage, cancellation, or work failure.
    #[napi]
    pub async fn list_named_attributes(
        &self,
        path: String,
        after_class: Option<String>,
        after_name: Option<Buffer>,
        maximum_entries: u32,
    ) -> Result<NativeNamedAttributePage> {
        let path = native_path(&path, self.config.limits)?;
        let after = match (after_class, after_name) {
            (None, None) => None,
            (Some(class), Some(name)) => Some(native_attribute_name(
                &class,
                name.to_vec(),
                self.config.limits,
            )?),
            _ => {
                return Err(Error::new(
                    Status::InvalidArg,
                    "named-attribute cursor is incomplete",
                ));
            }
        };
        let mut checkout = self.inner.lock().await;
        let receipt = Box::pin(checkout.list_named_attributes(
            &path,
            after.as_ref(),
            maximum_entries,
            boundary_budget(),
            &self.cancellation,
        ))
        .await
        .map_err(napi_error)?;
        Ok(NativeNamedAttributePage {
            entries: receipt
                .value
                .entries
                .into_iter()
                .map(|entry| NativeNamedAttributeName {
                    attribute_class: native_attribute_class_name(entry.name.class()).to_owned(),
                    name: Buffer::from(entry.name.as_bytes().to_vec()),
                })
                .collect(),
            has_more: receipt.value.has_more,
            work_json: serde_json::to_string(&receipt.work).map_err(napi_error)?,
        })
    }

    /// Inserts or replaces one exact named attribute.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for class, name, mode, mutation, cancellation, or work failure.
    #[napi]
    pub async fn write_named_attribute(
        &self,
        path: String,
        attribute_class: String,
        name: Buffer,
        bytes: Buffer,
        mode: String,
    ) -> Result<NativeMutationResult> {
        let path = native_path(&path, self.config.limits)?;
        let name = native_attribute_name(&attribute_class, name.to_vec(), self.config.limits)?;
        let mode = native_attribute_write_mode(&mode)?;
        let mut checkout = self.inner.lock().await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = Box::pin(checkout.write_named_attribute(
            path,
            name,
            bytes::Bytes::from(bytes.to_vec()),
            mode,
            boundary_budget(),
            &self.cancellation,
        ))
        .await
        .map_err(napi_error)?;
        native_mutation(None, receipt.work)
    }

    /// Removes one exact named attribute.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for class, name, mutation, cancellation, or work failure.
    #[napi]
    pub async fn remove_named_attribute(
        &self,
        path: String,
        attribute_class: String,
        name: Buffer,
    ) -> Result<NativeMutationResult> {
        let path = native_path(&path, self.config.limits)?;
        let name = native_attribute_name(&attribute_class, name.to_vec(), self.config.limits)?;
        let mut checkout = self.inner.lock().await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = Box::pin(checkout.remove_named_attribute(
            path,
            name,
            boundary_budget(),
            &self.cancellation,
        ))
        .await
        .map_err(napi_error)?;
        native_mutation(None, receipt.work)
    }

    /// Reads one exact logical regular-file range.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for malformed paths or integers, invalid
    /// ranges, non-regular files, corruption, cancellation, or bounded work.
    #[napi]
    pub async fn read_file_range(
        &self,
        path: String,
        offset: BigInt,
        length: BigInt,
    ) -> Result<NativeFileRead> {
        let portable = PortablePath::parse(&path, self.config.limits).map_err(napi_error)?;
        let path =
            NamespacePath::from_portable(&portable, self.config.limits).map_err(napi_error)?;
        let mut checkout = self.inner.lock().await;
        let receipt = checkout
            .read_file_range(
                &path,
                ByteRange {
                    offset: bigint_u64(&offset)?,
                    length: bigint_u64(&length)?,
                },
                boundary_budget(),
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        Ok(NativeFileRead {
            bytes: Buffer::from(receipt.value.bytes.to_vec()),
            work_json: serde_json::to_string(&receipt.work).map_err(napi_error)?,
        })
    }

    /// Reads one exact logical range by stable file identity.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for malformed identity/range, absence,
    /// non-regular kind, storage, cancellation, or bounded work.
    #[napi]
    pub async fn read_file_range_by_id(
        &self,
        file_id: Buffer,
        offset: BigInt,
        length: BigInt,
    ) -> Result<NativeFileRead> {
        let file_id = FileId::from_bytes(fixed_16(&file_id)?);
        let mut checkout = self.inner.lock().await;
        let receipt = checkout
            .read_file_range_by_id(
                file_id,
                ByteRange {
                    offset: bigint_u64(&offset)?,
                    length: bigint_u64(&length)?,
                },
                boundary_budget(),
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        Ok(NativeFileRead {
            bytes: Buffer::from(receipt.value.bytes.to_vec()),
            work_json: serde_json::to_string(&receipt.work).map_err(napi_error)?,
        })
    }

    /// Plans one bounded sparse range without reading file content blobs.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for malformed paths/integers, invalid bounds,
    /// non-regular files, corruption, cancellation, or bounded work.
    #[napi]
    pub async fn plan_file_extents(
        &self,
        path: String,
        offset: BigInt,
        length: BigInt,
        maximum_spans: u32,
    ) -> Result<NativeExtentPlan> {
        let path = native_path(&path, self.config.limits)?;
        let mut checkout = self.inner.lock().await;
        let receipt = checkout
            .plan_file_extents(
                &path,
                ByteRange {
                    offset: bigint_u64(&offset)?,
                    length: bigint_u64(&length)?,
                },
                maximum_spans,
                boundary_budget(),
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        native_extent_plan(receipt)
    }

    /// Plans one bounded sparse range by stable file identity.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for malformed identity/range, invalid bounds,
    /// non-regular kind, storage, cancellation, or bounded work.
    #[napi]
    pub async fn plan_file_extents_by_id(
        &self,
        file_id: Buffer,
        offset: BigInt,
        length: BigInt,
        maximum_spans: u32,
    ) -> Result<NativeExtentPlan> {
        let file_id = FileId::from_bytes(fixed_16(&file_id)?);
        let mut checkout = self.inner.lock().await;
        let receipt = checkout
            .plan_file_extents_by_id(
                file_id,
                ByteRange {
                    offset: bigint_u64(&offset)?,
                    length: bigint_u64(&length)?,
                },
                maximum_spans,
                boundary_budget(),
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        native_extent_plan(receipt)
    }

    /// Finds the next sparse data or hole boundary without reading file bodies.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for malformed paths/offsets/targets,
    /// non-regular files, corruption, cancellation, or bounded work.
    #[napi]
    pub async fn seek_file_extent(
        &self,
        path: String,
        offset: BigInt,
        target: String,
    ) -> Result<NativeExtentSeek> {
        let path = native_path(&path, self.config.limits)?;
        let target = extent_seek_target(&target)?;
        let mut checkout = self.inner.lock().await;
        let receipt = checkout
            .seek_file_extent(
                &path,
                bigint_u64(&offset)?,
                target,
                boundary_budget(),
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        Ok(NativeExtentSeek {
            offset: receipt.value.map(bigint),
            work_json: serde_json::to_string(&receipt.work).map_err(napi_error)?,
        })
    }

    /// Finds the next sparse boundary by stable file identity.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for malformed identity/offset/target,
    /// non-regular kind, storage, cancellation, or bounded work.
    #[napi]
    pub async fn seek_file_extent_by_id(
        &self,
        file_id: Buffer,
        offset: BigInt,
        target: String,
    ) -> Result<NativeExtentSeek> {
        let file_id = FileId::from_bytes(fixed_16(&file_id)?);
        let target = extent_seek_target(&target)?;
        let mut checkout = self.inner.lock().await;
        let receipt = checkout
            .seek_file_extent_by_id(
                file_id,
                bigint_u64(&offset)?,
                target,
                boundary_budget(),
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        Ok(NativeExtentSeek {
            offset: receipt.value.map(bigint),
            work_json: serde_json::to_string(&receipt.work).map_err(napi_error)?,
        })
    }

    /// Reads one symbolic link's exact opaque target bytes without following it.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for malformed paths, non-links, corruption,
    /// cancellation, storage failure, or bounded-work exhaustion.
    #[napi]
    pub async fn read_symbolic_link(&self, path: String) -> Result<NativeFileRead> {
        let path = native_path(&path, self.config.limits)?;
        let mut checkout = self.inner.lock().await;
        let receipt = checkout
            .read_symbolic_link(&path, boundary_budget(), &self.cancellation)
            .await
            .map_err(napi_error)?;
        Ok(NativeFileRead {
            bytes: Buffer::from(receipt.value.to_vec()),
            work_json: serde_json::to_string(&receipt.work).map_err(napi_error)?,
        })
    }

    /// Reads one opaque Windows reparse-point payload without interpreting it.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for malformed paths, non-reparse points,
    /// corruption, cancellation, storage failure, or bounded-work exhaustion.
    #[napi]
    pub async fn read_reparse_point(&self, path: String) -> Result<NativeFileRead> {
        let path = native_path(&path, self.config.limits)?;
        let mut checkout = self.inner.lock().await;
        let receipt = checkout
            .read_reparse_point(&path, boundary_budget(), &self.cancellation)
            .await
            .map_err(napi_error)?;
        Ok(NativeFileRead {
            bytes: Buffer::from(receipt.value.to_vec()),
            work_json: serde_json::to_string(&receipt.work).map_err(napi_error)?,
        })
    }

    /// Returns one bounded ordered directory page.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for malformed paths/cursors, non-directories,
    /// corruption, cancellation, or bounded-work exhaustion.
    #[napi]
    pub async fn list_directory(
        &self,
        path: String,
        after: Option<String>,
        maximum_entries: u32,
    ) -> Result<NativeDirectoryPage> {
        let portable = PortablePath::parse(&path, self.config.limits).map_err(napi_error)?;
        let path =
            NamespacePath::from_portable(&portable, self.config.limits).map_err(napi_error)?;
        let after = after
            .map(|value| {
                LogicalName::new(
                    NameEncoding::Utf8,
                    value.into_bytes(),
                    self.config.limits.maximum_component_bytes,
                )
            })
            .transpose()
            .map_err(napi_error)?;
        let mut checkout = self.inner.lock().await;
        let receipt = checkout
            .list_directory(
                &path,
                after.as_ref(),
                maximum_entries,
                boundary_budget(),
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        let entries = receipt
            .value
            .entries
            .into_iter()
            .map(|entry| NativeDirectoryEntry {
                name: Buffer::from(entry.name.as_bytes().to_vec()),
                file_id: Buffer::from(entry.file_id.into_bytes().to_vec()),
                file_kind: file_kind(entry.kind).to_owned(),
            })
            .collect();
        Ok(NativeDirectoryPage {
            entries,
            has_more: receipt.value.has_more,
            work_json: serde_json::to_string(&receipt.work).map_err(napi_error)?,
        })
    }

    /// Returns a bounded ordered directory page containing complete file
    /// records and canonical metadata bytes.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for malformed paths/cursors, non-directories,
    /// corruption, cancellation, serialization, or bounded-work exhaustion.
    #[napi]
    pub async fn list_directory_records(
        &self,
        path: String,
        after: Option<String>,
        maximum_entries: u32,
    ) -> Result<NativeDirectoryRecordPage> {
        let path = native_path(&path, self.config.limits)?;
        let after = after
            .map(|value| {
                LogicalName::new(
                    NameEncoding::Utf8,
                    value.into_bytes(),
                    self.config.limits.maximum_component_bytes,
                )
            })
            .transpose()
            .map_err(napi_error)?;
        let mut checkout = self.inner.lock().await;
        let receipt = Box::pin(checkout.list_directory_records(
            &path,
            after.as_ref(),
            maximum_entries,
            boundary_budget(),
            &self.cancellation,
        ))
        .await
        .map_err(napi_error)?;
        let entries = receipt
            .value
            .entries
            .into_iter()
            .map(|entry| {
                Ok(NativeDirectoryRecordEntry {
                    name: Buffer::from(entry.name.as_bytes().to_vec()),
                    record: encode_file_record(entry.record),
                    metadata_canonical_bytes: Buffer::from(
                        encode_file_metadata(entry.metadata).map_err(napi_error)?,
                    ),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(NativeDirectoryRecordPage {
            entries,
            has_more: receipt.value.has_more,
            work_json: serde_json::to_string(&receipt.work).map_err(napi_error)?,
        })
    }

    /// Creates one regular file in the private COW overlay.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for malformed paths, conflicting namespace
    /// state, excessive content, cancellation, storage, or bounded work.
    #[napi]
    pub async fn create_file(&self, path: String, bytes: Buffer) -> Result<NativeMutationResult> {
        let path = native_path(&path, self.config.limits)?;
        let mut checkout = self.inner.lock().await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = checkout
            .create_file(
                path,
                bytes::Bytes::from(bytes.to_vec()),
                boundary_budget(),
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        Ok(NativeMutationResult {
            file_id: Some(Buffer::from(receipt.value.into_bytes().to_vec())),
            work_json: serde_json::to_string(&receipt.work).map_err(napi_error)?,
        })
    }

    /// Creates one empty directory in the private COW overlay.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for malformed paths, conflicting namespace
    /// state, cancellation, storage, or bounded work.
    #[napi]
    pub async fn create_directory(&self, path: String) -> Result<NativeMutationResult> {
        let path = native_path(&path, self.config.limits)?;
        let mut checkout = self.inner.lock().await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = checkout
            .create_directory(path, boundary_budget(), &self.cancellation)
            .await
            .map_err(napi_error)?;
        Ok(NativeMutationResult {
            file_id: Some(Buffer::from(receipt.value.into_bytes().to_vec())),
            work_json: serde_json::to_string(&receipt.work).map_err(napi_error)?,
        })
    }

    /// Creates one symbolic link with exact opaque target bytes.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for malformed paths, conflicting namespace
    /// state, excessive targets, cancellation, storage, or bounded work.
    #[napi]
    pub async fn create_symbolic_link(
        &self,
        path: String,
        target: Buffer,
    ) -> Result<NativeMutationResult> {
        let path = native_path(&path, self.config.limits)?;
        let mut checkout = self.inner.lock().await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = checkout
            .create_symbolic_link(
                path,
                bytes::Bytes::from(target.to_vec()),
                boundary_budget(),
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        Ok(NativeMutationResult {
            file_id: Some(Buffer::from(receipt.value.into_bytes().to_vec())),
            work_json: serde_json::to_string(&receipt.work).map_err(napi_error)?,
        })
    }

    /// Creates an exact empty FIFO, socket, or mounted-volume boundary.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for an unknown/incompatible kind, malformed
    /// path, conflict, storage failure, cancellation, or bounded work.
    #[napi]
    pub async fn create_special(&self, path: String, kind: String) -> Result<NativeMutationResult> {
        let path = native_path(&path, self.config.limits)?;
        let kind = empty_special_kind(&kind)?;
        let mut checkout = self.inner.lock().await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = checkout
            .create_empty_special(path, kind, boundary_budget(), &self.cancellation)
            .await
            .map_err(napi_error)?;
        encode_created_mutation(&receipt)
    }

    /// Creates an exact POSIX character or block device identity.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for an unknown/incompatible kind, malformed
    /// path, conflict, storage failure, cancellation, or bounded work.
    #[napi]
    pub async fn create_device(
        &self,
        path: String,
        kind: String,
        major: u32,
        minor: u32,
    ) -> Result<NativeMutationResult> {
        let path = native_path(&path, self.config.limits)?;
        let kind = device_kind(&kind)?;
        let mut checkout = self.inner.lock().await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = checkout
            .create_device(
                path,
                kind,
                major,
                minor,
                boundary_budget(),
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        encode_created_mutation(&receipt)
    }

    /// Creates an opaque exact Windows reparse-point payload.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for an incompatible profile, malformed path,
    /// excessive payload, storage failure, cancellation, or bounded work.
    #[napi]
    pub async fn create_reparse_point(
        &self,
        path: String,
        payload: Buffer,
    ) -> Result<NativeMutationResult> {
        let path = native_path(&path, self.config.limits)?;
        let mut checkout = self.inner.lock().await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = checkout
            .create_reparse_point(
                path,
                bytes::Bytes::from(payload.to_vec()),
                boundary_budget(),
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        encode_created_mutation(&receipt)
    }

    /// Replaces one logical file range in the private COW overlay.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for malformed paths/offsets, non-regular
    /// files, cancellation, storage, or bounded work.
    #[napi]
    pub async fn write_file(
        &self,
        path: String,
        offset: BigInt,
        bytes: Buffer,
    ) -> Result<NativeMutationResult> {
        let path = native_path(&path, self.config.limits)?;
        let mut checkout = self.inner.lock().await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = checkout
            .write_file(
                path,
                bigint_u64(&offset)?,
                bytes::Bytes::from(bytes.to_vec()),
                boundary_budget(),
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        Ok(NativeMutationResult {
            file_id: None,
            work_json: serde_json::to_string(&receipt.work).map_err(napi_error)?,
        })
    }

    /// Replaces one logical file range by stable file identity.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for malformed identity/offset, non-regular
    /// kind, storage, cancellation, or bounded work.
    #[napi]
    pub async fn write_file_by_id(
        &self,
        file_id: Buffer,
        offset: BigInt,
        bytes: Buffer,
    ) -> Result<NativeMutationResult> {
        let file_id = FileId::from_bytes(fixed_16(&file_id)?);
        let mut checkout = self.inner.lock().await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = checkout
            .write_file_by_id(
                file_id,
                bigint_u64(&offset)?,
                bytes::Bytes::from(bytes.to_vec()),
                boundary_budget(),
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        mutation_result(receipt.work)
    }

    /// Removes one namespace binding.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for malformed paths/identities, conflicts,
    /// cancellation, storage, or bounded work.
    #[napi]
    pub async fn remove(
        &self,
        path: String,
        expected_file_id: Option<Buffer>,
    ) -> Result<NativeMutationResult> {
        let expected = expected_file_id
            .as_deref()
            .map(fixed_16)
            .transpose()?
            .map(FileId::from_bytes);
        let mut checkout = self.inner.lock().await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = checkout
            .remove(
                native_path(&path, self.config.limits)?,
                expected,
                boundary_budget(),
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        mutation_result(receipt.work)
    }

    /// Atomically renames one binding within the volume.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for malformed paths, conflicts,
    /// cancellation, storage, or bounded work.
    #[napi]
    pub async fn rename(
        &self,
        source: String,
        destination: String,
        replace: bool,
    ) -> Result<NativeMutationResult> {
        let source = native_path(&source, self.config.limits)?;
        let destination = native_path(&destination, self.config.limits)?;
        let mut checkout = self.inner.lock().await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = checkout
            .rename(
                source,
                destination,
                replace,
                boundary_budget(),
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        mutation_result(receipt.work)
    }

    /// Creates one hard link within the volume.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for malformed paths, invalid kinds,
    /// conflicts, cancellation, storage, or bounded work.
    #[napi]
    pub async fn hard_link(
        &self,
        source: String,
        destination: String,
    ) -> Result<NativeMutationResult> {
        let source = native_path(&source, self.config.limits)?;
        let destination = native_path(&destination, self.config.limits)?;
        let mut checkout = self.inner.lock().await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = checkout
            .hard_link(source, destination, boundary_budget(), &self.cancellation)
            .await
            .map_err(napi_error)?;
        mutation_result(receipt.work)
    }

    /// Changes a regular file's logical length.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for malformed values, invalid kinds,
    /// cancellation, storage, or bounded work.
    #[napi]
    pub async fn resize_file(
        &self,
        path: String,
        logical_bytes: BigInt,
    ) -> Result<NativeMutationResult> {
        let path = native_path(&path, self.config.limits)?;
        let mut checkout = self.inner.lock().await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = checkout
            .resize_file(
                path,
                bigint_u64(&logical_bytes)?,
                boundary_budget(),
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        mutation_result(receipt.work)
    }

    /// Changes a regular file's logical length by stable file identity.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for malformed identity/value, invalid kinds,
    /// cancellation, storage, or bounded-work exhaustion.
    #[napi]
    pub async fn resize_file_by_id(
        &self,
        file_id: Buffer,
        logical_bytes: BigInt,
    ) -> Result<NativeMutationResult> {
        let file_id = FileId::from_bytes(fixed_16(&file_id)?);
        let mut checkout = self.inner.lock().await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = checkout
            .resize_file_by_id(
                file_id,
                bigint_u64(&logical_bytes)?,
                boundary_budget(),
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        mutation_result(receipt.work)
    }

    /// Punches a hole or records physically allocated zeros.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for malformed values, invalid ranges/kinds,
    /// cancellation, storage, or bounded work.
    #[napi]
    pub async fn zero_file_range(
        &self,
        path: String,
        offset: BigInt,
        length: BigInt,
        allocated: bool,
        extend: bool,
    ) -> Result<NativeMutationResult> {
        let path = native_path(&path, self.config.limits)?;
        let mut checkout = self.inner.lock().await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = checkout
            .zero_file_range(
                path,
                ByteRange {
                    offset: bigint_u64(&offset)?,
                    length: bigint_u64(&length)?,
                },
                allocated,
                extend,
                boundary_budget(),
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        mutation_result(receipt.work)
    }

    /// Punches a hole or records allocated zeros by stable file identity.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for malformed identity/range, invalid kinds,
    /// cancellation, storage, or bounded-work exhaustion.
    #[napi]
    pub async fn zero_file_range_by_id(
        &self,
        file_id: Buffer,
        offset: BigInt,
        length: BigInt,
        allocated: bool,
        extend: bool,
    ) -> Result<NativeMutationResult> {
        let file_id = FileId::from_bytes(fixed_16(&file_id)?);
        let mut checkout = self.inner.lock().await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = checkout
            .zero_file_range_by_id(
                file_id,
                ByteRange {
                    offset: bigint_u64(&offset)?,
                    length: bigint_u64(&length)?,
                },
                allocated,
                extend,
                boundary_budget(),
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        mutation_result(receipt.work)
    }

    /// Allocates sparse holes without replacing existing content.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for malformed values, unsupported keep-size
    /// physical allocation, cancellation, storage, or bounded work.
    #[napi]
    pub async fn preallocate_file(
        &self,
        path: String,
        offset: BigInt,
        length: BigInt,
        keep_size: bool,
    ) -> Result<NativeMutationResult> {
        let path = native_path(&path, self.config.limits)?;
        let mut checkout = self.inner.lock().await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = checkout
            .preallocate_file(
                path,
                ByteRange {
                    offset: bigint_u64(&offset)?,
                    length: bigint_u64(&length)?,
                },
                keep_size,
                boundary_budget(),
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        mutation_result(receipt.work)
    }

    /// Allocates sparse holes by stable file identity.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for malformed identity/range, unsupported
    /// keep-size allocation, cancellation, storage, or bounded-work exhaustion.
    #[napi]
    pub async fn preallocate_file_by_id(
        &self,
        file_id: Buffer,
        offset: BigInt,
        length: BigInt,
        keep_size: bool,
    ) -> Result<NativeMutationResult> {
        let file_id = FileId::from_bytes(fixed_16(&file_id)?);
        let mut checkout = self.inner.lock().await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = checkout
            .preallocate_file_by_id(
                file_id,
                ByteRange {
                    offset: bigint_u64(&offset)?,
                    length: bigint_u64(&length)?,
                },
                keep_size,
                boundary_budget(),
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        mutation_result(receipt.work)
    }

    /// Clones one logical range by immutable extent reference.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for malformed paths/values, invalid ranges,
    /// cancellation, storage, or bounded work.
    #[napi]
    pub async fn clone_file_range(
        &self,
        source: String,
        source_offset: BigInt,
        destination: String,
        destination_offset: BigInt,
        length: BigInt,
    ) -> Result<NativeMutationResult> {
        let request = FileCloneRequest {
            source: native_path(&source, self.config.limits)?,
            source_offset: bigint_u64(&source_offset)?,
            destination: native_path(&destination, self.config.limits)?,
            destination_offset: bigint_u64(&destination_offset)?,
            length: bigint_u64(&length)?,
        };
        let mut checkout = self.inner.lock().await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = checkout
            .clone_file_range(request, boundary_budget(), &self.cancellation)
            .await
            .map_err(napi_error)?;
        mutation_result(receipt.work)
    }

    /// Clones one sparse range between stable file identities.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for malformed identities/values, invalid
    /// ranges or kinds, cancellation, storage, or bounded-work exhaustion.
    #[napi]
    pub async fn clone_file_range_by_id(
        &self,
        source_file_id: Buffer,
        source_offset: BigInt,
        destination_file_id: Buffer,
        destination_offset: BigInt,
        length: BigInt,
    ) -> Result<NativeMutationResult> {
        let source_file_id = FileId::from_bytes(fixed_16(&source_file_id)?);
        let destination_file_id = FileId::from_bytes(fixed_16(&destination_file_id)?);
        let mut checkout = self.inner.lock().await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = checkout
            .clone_file_range_by_id(
                source_file_id,
                bigint_u64(&source_offset)?,
                destination_file_id,
                bigint_u64(&destination_offset)?,
                bigint_u64(&length)?,
                boundary_budget(),
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        mutation_result(receipt.work)
    }

    /// Checkpoints and conditionally publishes this private overlay.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for malformed operation identity, clean or
    /// read-only checkouts, closure/authentication failure, cancellation, or work bounds.
    #[napi]
    pub async fn commit(&self, operation_id: Buffer) -> Result<NativeCommitResult> {
        let operation_id = OperationId::from_bytes(fixed_16(&operation_id)?);
        let mut checkout = self.inner.lock().await;
        checkout
            .retain_operation_id(operation_id)
            .map_err(napi_error)?;
        let receipt = checkout
            .commit(operation_id, boundary_budget(), &self.cancellation)
            .await;
        let receipt = match receipt {
            Ok(receipt) => {
                checkout.clear_retained_operation(operation_id);
                receipt
            }
            Err(error) => return Err(napi_error(error)),
        };
        commit_result(receipt.value, receipt.work)
    }

    /// Applies and publishes one direct-live transaction with bounded safe retries.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for malformed operations or identity, wrong
    /// checkout mode, unresolved work, cancellation, storage, or bounded work.
    #[napi]
    pub async fn mutate_live(
        &self,
        operations: Vec<NativeTransactionOperation>,
        operation_id: Buffer,
        maximum_attempts: u32,
        maximum_conflicts: u32,
    ) -> Result<NativeAuthoredLiveMutationResult> {
        let maximum =
            usize::try_from(self.config.limits.maximum_mutations_per_batch).unwrap_or(usize::MAX);
        if operations.len() > maximum || operations.capacity() > maximum {
            return Err(Error::new(
                Status::InvalidArg,
                "transaction exceeds the configured mutation bound",
            ));
        }
        let authored = operations
            .into_iter()
            .map(|operation| native_authored_transaction(operation, self.config.limits))
            .collect::<Result<Vec<_>>>()?;
        let operation_id = OperationId::from_bytes(fixed_16(&operation_id)?);
        let mut checkout = self.inner.lock().await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        checkout
            .retain_operation_id(operation_id)
            .map_err(napi_error)?;
        let receipt = checkout
            .apply_authored_live(
                authored,
                operation_id,
                maximum_attempts,
                maximum_conflicts,
                boundary_budget(),
                &self.cancellation,
            )
            .await;
        match receipt {
            Ok(receipt) => {
                let resolved = live_outcome_resolved(&receipt.value.outcome);
                let created_file_ids = receipt
                    .value
                    .created_file_ids
                    .into_iter()
                    .map(|identity| identity.map(|value| Buffer::from(value.into_bytes().to_vec())))
                    .collect();
                let result = live_mutation_result(receipt.value.outcome, receipt.work)?;
                if resolved {
                    checkout.clear_retained_operation(operation_id);
                }
                Ok(NativeAuthoredLiveMutationResult {
                    created_file_ids,
                    status: result.status,
                    generation_id: result.generation_id,
                    epoch: result.epoch,
                    sequence: result.sequence,
                    conflict_count: result.conflict_count,
                    truncated: result.truncated,
                    committed_fingerprint: result.committed_fingerprint,
                    work_json: result.work_json,
                })
            }
            Err(error) => {
                if !checkout.has_pending_mutations() {
                    checkout.clear_retained_operation(operation_id);
                }
                Err(napi_error(error))
            }
        }
    }

    /// Resumes an unresolved direct-live transaction with bounded safe retries.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for malformed identity, wrong checkout mode,
    /// absent staged work, cancellation, storage, rebase, or bounded-work failure.
    #[napi]
    pub async fn resume_live(
        &self,
        operation_id: Buffer,
        maximum_attempts: u32,
        maximum_conflicts: u32,
    ) -> Result<NativeLiveMutationResult> {
        let operation_id = OperationId::from_bytes(fixed_16(&operation_id)?);
        let mut checkout = self.inner.lock().await;
        checkout
            .retain_operation_id(operation_id)
            .map_err(napi_error)?;
        let receipt = checkout
            .resume_live(
                operation_id,
                maximum_attempts,
                maximum_conflicts,
                boundary_budget(),
                &self.cancellation,
            )
            .await;
        let receipt = match receipt {
            Ok(receipt) => {
                if live_outcome_resolved(&receipt.value) {
                    checkout.clear_retained_operation(operation_id);
                }
                receipt
            }
            Err(error) => return Err(napi_error(error)),
        };
        live_mutation_result(receipt.value, receipt.work)
    }

    /// Safely advances to head and sparsely replays private mutations.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for unsupported consistency, zero bounds,
    /// corruption, cancellation, storage, replay, or bounded-work failure.
    #[napi]
    pub async fn rebase_head(&self, maximum_conflicts: u32) -> Result<NativeRebaseResult> {
        let mut checkout = self.inner.lock().await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = checkout
            .rebase_head(maximum_conflicts, boundary_budget(), &self.cancellation)
            .await
            .map_err(napi_error)?;
        let (status, generation_id, conflict_count, truncated) = match receipt.value {
            RebaseDecision::Safe { generation } => (
                "safe",
                Some(Buffer::from(generation.digest().into_bytes().to_vec())),
                0,
                false,
            ),
            RebaseDecision::Conflicted {
                conflicts,
                truncated,
            } => (
                "conflicted",
                None,
                u32::try_from(conflicts.len()).unwrap_or(u32::MAX),
                truncated,
            ),
        };
        Ok(NativeRebaseResult {
            status: status.to_owned(),
            generation_id,
            conflict_count,
            truncated,
            work_json: serde_json::to_string(&receipt.work).map_err(napi_error)?,
        })
    }

    /// Discards the private overlay and returns to its current immutable base.
    ///
    /// # Errors
    ///
    /// Returns a JavaScript error for cancellation, storage, corruption, or bounded work.
    #[napi]
    pub async fn discard(&self) -> Result<NativeMutationResult> {
        let mut checkout = self.inner.lock().await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = checkout
            .discard(boundary_budget(), &self.cancellation)
            .await
            .map_err(napi_error)?;
        mutation_result(receipt.work)
    }

    /// Cooperatively cancels this checkout's future operations.
    #[napi]
    pub fn cancel(&self) {
        self.cancellation.cancel();
    }
}

impl NativeWatcher {
    fn verify_capture_root(&self) -> Result<()> {
        let root_identity = capture_root_identity(&self.source_root).map_err(napi_error)?;
        self.inner
            .lock()
            .map_err(|_| watcher_poisoned())?
            .verify_root_identity(root_identity)
            .map_err(napi_error)
    }

    async fn capture_reconcile_baseline(
        &self,
        maximum_paths: u32,
        maximum_extent_spans: u32,
    ) -> Result<PendingReconcile> {
        let operation_id = OperationId::new();
        let mut checkout = self.checkout.lock().await;
        checkout
            .retain_operation_id(operation_id)
            .map_err(napi_error)?;
        let (epoch, expected_root_identity) = {
            let mut watcher = self.inner.lock().map_err(|_| watcher_poisoned())?;
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
            boundary_budget(),
            &self.cancellation,
        ))
        .await;
        let baseline = match baseline {
            Ok(receipt) => receipt,
            Err(error) => {
                checkout.clear_retained_operation(operation_id);
                self.inner
                    .lock()
                    .map_err(|_| watcher_poisoned())?
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
    /// arrive during the scan. The watcher owns both checkout and source-root
    /// identity, so callers cannot accidentally reconcile the wrong tree.
    ///
    /// # Errors
    ///
    /// Returns watcher, host-state, engine, cancellation, allocation, or
    /// bounded-work failures. Failed baseline capture leaves the watcher
    /// explicitly invalidated and retryable.
    #[napi]
    pub async fn reconcile(
        &self,
        maximum_paths: u32,
        maximum_extent_spans: u32,
    ) -> Result<NativeWatchReconcileResult> {
        let _operation = self.operation.lock().await;
        ensure_no_pending_interval(&self.pending)?;
        self.verify_capture_root()?;
        let existing = *self
            .pending_reconcile
            .lock()
            .map_err(|_| watcher_poisoned())?;
        let pending = if let Some(pending) = existing {
            pending
        } else {
            let pending =
                Box::pin(self.capture_reconcile_baseline(maximum_paths, maximum_extent_spans))
                    .await?;
            *self
                .pending_reconcile
                .lock()
                .map_err(|_| watcher_poisoned())? = Some(pending);
            pending
        };
        {
            let mut checkout = self.checkout.lock().await;
            checkout
                .retain_operation_id(pending.operation_id)
                .map_err(napi_error)?;
            seal_checkout(
                &mut checkout,
                pending.operation_id,
                boundary_budget(),
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
            checkout.clear_retained_operation(pending.operation_id);
        }
        *self
            .pending_reconcile
            .lock()
            .map_err(|_| watcher_poisoned())? = None;
        let batch = self
            .inner
            .lock()
            .map_err(|_| watcher_poisoned())?
            .finish_rescan()
            .map_err(napi_error)?;
        let post_baseline = encode_watch_batch(batch.clone(), acyclic_fs::WorkCounters::default())?;
        *self.pending.lock().map_err(|_| watcher_poisoned())? = Some(PendingWatchBatch {
            batch,
            poll_work: acyclic_fs::WorkCounters::default(),
            capture: None,
            operation_id: OperationId::new(),
        });
        Ok(NativeWatchReconcileResult {
            epoch: bigint(pending.epoch.get()),
            baseline: NativeCaptureResult {
                examined_paths: bigint(pending.baseline.examined_paths),
                changed_paths: bigint(pending.baseline.changed_paths),
                staged_file_bytes: bigint(pending.baseline.staged_file_bytes),
                work_json: serde_json::to_string(&pending.work).map_err(napi_error)?,
            },
            post_baseline,
        })
    }

    /// Polls and atomically captures one watcher interval. An interval remains
    /// retained across failure and is retried verbatim until capture succeeds.
    ///
    /// # Errors
    ///
    /// Returns invalid bounds, rescan-required, host-state, engine,
    /// cancellation, allocation, watcher, or bounded-work failures.
    #[napi]
    pub async fn poll_capture(
        &self,
        maximum_changes: u32,
        maximum_paths: u32,
        maximum_extent_spans: u32,
    ) -> Result<NativeWatchCaptureResult> {
        let _operation = self.operation.lock().await;
        self.verify_capture_root()?;
        let pending = {
            let pending = self.pending.lock().map_err(|_| watcher_poisoned())?;
            pending.clone()
        };
        let pending = if let Some(pending) = pending {
            pending
        } else {
            let receipt = self
                .inner
                .lock()
                .map_err(|_| watcher_poisoned())?
                .poll(maximum_changes, boundary_budget(), &self.cancellation)
                .map_err(napi_error)?;
            let pending = PendingWatchBatch {
                batch: receipt.value,
                poll_work: receipt.work,
                capture: None,
                operation_id: OperationId::new(),
            };
            *self.pending.lock().map_err(|_| watcher_poisoned())? = Some(pending.clone());
            pending
        };
        {
            let mut checkout = self.checkout.lock().await;
            checkout
                .retain_operation_id(pending.operation_id)
                .map_err(napi_error)?;
        }
        let receipt = if let Some(capture) = pending.capture {
            acyclic_fs::OperationReceipt {
                value: capture,
                work: capture.capture.work,
            }
        } else {
            let expected_root_identity = self
                .inner
                .lock()
                .map_err(|_| watcher_poisoned())?
                .root_identity();
            let mut checkout = self.checkout.lock().await;
            let receipt = Box::pin(capture_watch_batch(
                &mut checkout,
                pending.batch,
                &CaptureOptions {
                    source_root: self.source_root.clone(),
                    expected_root_identity,
                    maximum_paths,
                    maximum_extent_spans,
                },
                boundary_budget(),
                &self.cancellation,
            ))
            .await
            .map_err(napi_error)?;
            if let Some(retained) = self
                .pending
                .lock()
                .map_err(|_| watcher_poisoned())?
                .as_mut()
            {
                retained.capture = Some(receipt.value);
            }
            receipt
        };
        {
            let mut checkout = self.checkout.lock().await;
            checkout
                .retain_operation_id(pending.operation_id)
                .map_err(napi_error)?;
            seal_checkout(
                &mut checkout,
                pending.operation_id,
                boundary_budget(),
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
            checkout.clear_retained_operation(pending.operation_id);
        }
        *self.pending.lock().map_err(|_| watcher_poisoned())? = None;
        let work = pending
            .poll_work
            .checked_add(receipt.work)
            .map_err(napi_error)?;
        Ok(NativeWatchCaptureResult {
            epoch: bigint(receipt.value.epoch.get()),
            first_sequence: bigint(receipt.value.first_sequence.get()),
            next_sequence: bigint(receipt.value.next_sequence.get()),
            examined_paths: bigint(receipt.value.capture.examined_paths),
            changed_paths: bigint(receipt.value.capture.changed_paths),
            staged_file_bytes: bigint(receipt.value.capture.staged_file_bytes),
            work_json: serde_json::to_string(&work).map_err(napi_error)?,
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

    /// Stops this projection exactly once.
    ///
    /// # Errors
    ///
    /// Returns a native driver teardown failure.
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
        native_watch_backend: watch.backend.as_str().to_owned(),
        native_watch_persistent_restart: watch.persistent_restart,
        native_watch_root_identity_fencing: watch.root_identity_fencing,
        platform: std::env::consts::OS.to_owned(),
        architecture: std::env::consts::ARCH.to_owned(),
        native_mount: mount.available,
        writable_mount: mount.writable,
        provider_process_io_observable: mount.provider_process_io_observable,
    }
}

fn napi_error(error: impl std::fmt::Display) -> Error {
    Error::new(Status::GenericFailure, error.to_string())
}

fn watcher_poisoned() -> Error {
    Error::new(Status::GenericFailure, "native watcher state poisoned")
}

fn ensure_no_pending_interval(pending: &std::sync::Mutex<Option<PendingWatchBatch>>) -> Result<()> {
    if pending.lock().map_err(|_| watcher_poisoned())?.is_some() {
        return Err(napi_error(
            "watch interval must be durably acknowledged before reconciliation",
        ));
    }
    Ok(())
}

fn fixed_16(bytes: &[u8]) -> Result<[u8; 16]> {
    bytes.try_into().map_err(|_| {
        Error::new(
            Status::InvalidArg,
            "volume identity must be exactly 16 bytes",
        )
    })
}

fn fixed_32(bytes: &[u8], label: &str) -> Result<[u8; 32]> {
    bytes.try_into().map_err(|_| {
        Error::new(
            Status::InvalidArg,
            format!("{label} must be exactly 32 bytes"),
        )
    })
}

fn encode_object_id(value: ObjectId) -> Buffer {
    let mut bytes = Vec::with_capacity(33);
    bytes.push(value.kind.canonical_tag());
    bytes.extend_from_slice(value.digest.as_bytes());
    Buffer::from(bytes)
}

fn native_extent_span(span: &acyclic_fs::kernel::ExtentSlice) -> NativeExtentSpan {
    let (kind, object_id, object_offset) = match &span.kind {
        ExtentKind::Hole => ("hole", None, None),
        ExtentKind::AllocatedZero => ("allocated-zero", None, None),
        ExtentKind::Content {
            object,
            object_offset,
        } => (
            "content",
            Some(encode_object_id(*object)),
            Some(bigint(*object_offset)),
        ),
    };
    NativeExtentSpan {
        kind: kind.to_owned(),
        offset: bigint(span.offset),
        length: bigint(span.length),
        source_end: bigint(span.source_end),
        object_id,
        object_offset,
    }
}

fn native_extent_plan(
    receipt: acyclic_fs::FsReceipt<Option<acyclic_fs::kernel::ExtentPlan>>,
) -> Result<NativeExtentPlan> {
    let work_json = serde_json::to_string(&receipt.work).map_err(napi_error)?;
    match receipt.value {
        None => Ok(NativeExtentPlan {
            kind: "inline".to_owned(),
            spans: Vec::new(),
            retained_allocation_bytes: None,
            work_json,
        }),
        Some(plan) => Ok(NativeExtentPlan {
            kind: "sparse".to_owned(),
            spans: plan.spans.iter().map(native_extent_span).collect(),
            retained_allocation_bytes: Some(bigint(plan.retained_allocation_bytes)),
            work_json,
        }),
    }
}

fn encode_generation_diff(
    diff: acyclic_fs::GenerationDiff,
    work: acyclic_fs::WorkCounters,
) -> Result<NativeGenerationDiff> {
    Ok(NativeGenerationDiff {
        files: diff
            .files
            .into_iter()
            .map(|change| NativeFileRecordChange {
                file_id: Buffer::from(change.file_id.into_bytes().to_vec()),
                before: change.before.map(encode_file_record),
                after: change.after.map(encode_file_record),
            })
            .collect(),
        bindings: diff
            .bindings
            .into_iter()
            .map(|change| NativeBindingChange {
                directory_id: Buffer::from(change.directory_id.into_bytes().to_vec()),
                name: encode_name_component(&change.name),
                before: change.before.as_ref().map(encode_tree_entry),
                after: change.after.as_ref().map(encode_tree_entry),
            })
            .collect(),
        truncated: diff.truncated,
        work_json: serde_json::to_string(&work).map_err(napi_error)?,
    })
}

fn encode_file_record(record: FileRecord) -> NativeFileRecord {
    let mut result = NativeFileRecord {
        file_id: Buffer::from(record.file_id.into_bytes().to_vec()),
        file_kind: file_kind(record.kind).to_owned(),
        link_count: bigint(record.link_count),
        metadata_object: encode_object_id(record.metadata),
        payload_kind: String::new(),
        logical_bytes: None,
        payload_object: None,
        inline_bytes: None,
        device_major: None,
        device_minor: None,
    };
    match record.payload {
        FilePayload::InlineRegular(bytes) => {
            "inline-regular".clone_into(&mut result.payload_kind);
            result.logical_bytes = Some(bigint(u64::try_from(bytes.as_bytes().len()).unwrap_or(0)));
            result.inline_bytes = Some(Buffer::from(bytes.as_bytes().to_vec()));
        }
        FilePayload::Regular {
            logical_bytes,
            extents,
        } => {
            "regular".clone_into(&mut result.payload_kind);
            result.logical_bytes = Some(bigint(logical_bytes));
            result.payload_object = Some(encode_object_id(extents));
        }
        FilePayload::Directory { entries } => {
            "directory".clone_into(&mut result.payload_kind);
            result.payload_object = Some(encode_object_id(entries));
        }
        FilePayload::SymbolicLink {
            target_bytes,
            target,
        } => {
            "symbolic-link".clone_into(&mut result.payload_kind);
            result.logical_bytes = Some(bigint(target_bytes));
            result.payload_object = Some(encode_object_id(target));
        }
        FilePayload::Empty => "empty".clone_into(&mut result.payload_kind),
        FilePayload::Device { major, minor } => {
            "device".clone_into(&mut result.payload_kind);
            result.device_major = Some(major);
            result.device_minor = Some(minor);
        }
        FilePayload::ReparsePoint {
            payload_bytes,
            payload,
        } => {
            "reparse-point".clone_into(&mut result.payload_kind);
            result.logical_bytes = Some(bigint(payload_bytes));
            result.payload_object = Some(encode_object_id(payload));
        }
    }
    result
}

fn encode_tree_entry(entry: &TreeEntry) -> NativeTreeEntry {
    NativeTreeEntry {
        name: encode_name_component(&entry.name),
        file_id: Buffer::from(entry.file_id.into_bytes().to_vec()),
        file_kind: file_kind(entry.kind).to_owned(),
    }
}

fn encode_name_component(name: &LogicalName) -> NativePathComponent {
    NativePathComponent {
        encoding: match name.encoding() {
            NameEncoding::Utf8 => "utf8",
            NameEncoding::PosixBytes => "posix-bytes",
            NameEncoding::WindowsUtf16Le => "windows-utf16le",
        }
        .to_owned(),
        bytes: Buffer::from(name.as_bytes().to_vec()),
    }
}

fn encode_merge_preparation(
    preparation: MergePreparation,
    work: acyclic_fs::WorkCounters,
) -> Result<NativeMergePreparation> {
    let work_json = serde_json::to_string(&work).map_err(napi_error)?;
    Ok(match preparation {
        MergePreparation::Prepared { generation_id } => NativeMergePreparation {
            status: "prepared".to_owned(),
            generation_id: Some(Buffer::from(generation_id.digest().into_bytes().to_vec())),
            conflicts: Vec::new(),
            truncated: false,
            work_json,
        },
        MergePreparation::Conflicted {
            conflicts,
            truncated,
        } => NativeMergePreparation {
            status: "conflicted".to_owned(),
            generation_id: None,
            conflicts: conflicts.into_iter().map(encode_merge_conflict).collect(),
            truncated,
            work_json,
        },
    })
}

fn encode_merge_conflict(conflict: MergeConflict) -> NativeMergeConflict {
    match conflict {
        MergeConflict::File(file_id) => NativeMergeConflict {
            kind: "file".to_owned(),
            file_id: Some(Buffer::from(file_id.into_bytes().to_vec())),
            directory_id: None,
            name: None,
        },
        MergeConflict::Binding { directory_id, name } => NativeMergeConflict {
            kind: "binding".to_owned(),
            file_id: None,
            directory_id: Some(Buffer::from(directory_id.into_bytes().to_vec())),
            name: Some(encode_name_component(&name)),
        },
    }
}

fn decode_object_id(bytes: &[u8]) -> Result<ObjectId> {
    if bytes.len() != 33 {
        return Err(Error::new(
            Status::InvalidArg,
            "object identity must be exactly 33 bytes",
        ));
    }
    let kind = ObjectKind::from_canonical_tag(bytes[0]).map_err(napi_error)?;
    Ok(ObjectId {
        kind,
        digest: Digest::from_bytes(fixed_32(&bytes[1..], "object digest")?),
    })
}

fn native_speculation_options(options: &NativeSpeculationOptions) -> Result<SpeculationOptions> {
    Ok(SpeculationOptions {
        residency: ResidencySpeculatorOptions {
            maximum_active_operations: options.residency.maximum_active_operations,
            maximum_active_bytes: bigint_u64(&options.residency.maximum_active_bytes)?,
            outcome_window: options.residency.outcome_window,
            traffic_window: options.residency.traffic_window,
            speculative_cost_basis_points: u16_from_u32(
                options.residency.speculative_cost_basis_points,
                "speculativeCostBasisPoints",
            )?,
            minimum_usefulness_samples: options.residency.minimum_usefulness_samples,
            minimum_usefulness_basis_points: u16_from_u32(
                options.residency.minimum_usefulness_basis_points,
                "residency minimumUsefulnessBasisPoints",
            )?,
        },
        promotion: PromotionSpeculatorOptions {
            maximum_active_operations: options.promotion.maximum_active_operations,
            maximum_active_bytes: bigint_u64(&options.promotion.maximum_active_bytes)?,
            maximum_active_cost_units: bigint_u64(&options.promotion.maximum_active_cost_units)?,
            maximum_residency_facts: options.promotion.maximum_residency_facts,
            maximum_destinations: options.promotion.maximum_destinations,
            maximum_accepted_tiers: options.promotion.maximum_accepted_tiers,
            outcome_window: options.promotion.outcome_window,
            minimum_usefulness_samples: options.promotion.minimum_usefulness_samples,
            minimum_usefulness_basis_points: u16_from_u32(
                options.promotion.minimum_usefulness_basis_points,
                "promotion minimumUsefulnessBasisPoints",
            )?,
        },
    })
}

fn native_residency_reason(value: &str) -> Result<ResidencyReason> {
    match value {
        "directory-successor" => Ok(ResidencyReason::DirectorySuccessor),
        "sequential-range" => Ok(ResidencyReason::SequentialRange),
        "metadata-successor" => Ok(ResidencyReason::MetadataSuccessor),
        "consumer-hint" => Ok(ResidencyReason::ConsumerHint),
        _ => Err(Error::new(
            Status::InvalidArg,
            "unknown residency speculation reason",
        )),
    }
}

const fn native_residency_rejection(value: ResidencyRejection) -> &'static str {
    match value {
        ResidencyRejection::WrongVolume => "wrong-volume",
        ResidencyRejection::StaleGeneration => "stale-generation",
        ResidencyRejection::InvalidRequest => "invalid-request",
        ResidencyRejection::DuplicateObject => "duplicate-object",
        ResidencyRejection::DuplicateOperation => "duplicate-operation",
        ResidencyRejection::OperationCapacity => "operation-capacity",
        ResidencyRejection::ByteCapacity => "byte-capacity",
        ResidencyRejection::CostBudget => "cost-budget",
        ResidencyRejection::LowUsefulness => "low-usefulness",
    }
}

fn native_storage_tier(value: &str) -> Result<StorageTier> {
    match value {
        "process-memory" => Ok(StorageTier::ProcessMemory),
        "node-local" => Ok(StorageTier::NodeLocal),
        "shared-cache" => Ok(StorageTier::SharedCache),
        "durable-origin" => Ok(StorageTier::DurableOrigin),
        _ => Err(Error::new(
            Status::InvalidArg,
            "unknown speculation storage tier",
        )),
    }
}

fn native_object_residency(value: &NativeObjectResidency) -> Result<ObjectResidency> {
    Ok(ObjectResidency {
        object_id: decode_object_id(&value.object_id)?,
        location_id: StorageLocationId::from_bytes(fixed_16(&value.location_id)?),
        tier: native_storage_tier(&value.tier)?,
        source_priority: u16_from_u32(value.source_priority, "sourcePriority")?,
    })
}

fn native_promotion_destination(
    value: &NativePromotionDestination,
) -> Result<PromotionDestination> {
    Ok(PromotionDestination {
        location_id: StorageLocationId::from_bytes(fixed_16(&value.location_id)?),
        tier: native_storage_tier(&value.tier)?,
        writable: value.writable,
        maximum_object_bytes: bigint_u64(&value.maximum_object_bytes)?,
        priority: u16_from_u32(value.priority, "promotion priority")?,
        cost_units_per_byte: bigint_u64(&value.cost_units_per_byte)?,
    })
}

fn native_promotion_admission(value: PromotionAdmission) -> NativePromotionAdmission {
    match value {
        PromotionAdmission::Satisfied(residency) => NativePromotionAdmission {
            status: "satisfied".to_owned(),
            rejection: None,
            operation_id: None,
            object_id: Some(encode_object_id(residency.object_id)),
            source_location_id: Some(Buffer::from(residency.location_id.into_bytes().to_vec())),
            destination_location_id: None,
            estimated_cost_units: None,
        },
        PromotionAdmission::Planned(plan) => NativePromotionAdmission {
            status: "planned".to_owned(),
            rejection: None,
            operation_id: Some(Buffer::from(
                plan.candidate.operation_id.into_bytes().to_vec(),
            )),
            object_id: Some(encode_object_id(plan.candidate.request.object_id)),
            source_location_id: Some(Buffer::from(plan.source.location_id.into_bytes().to_vec())),
            destination_location_id: Some(Buffer::from(
                plan.destination.location_id.into_bytes().to_vec(),
            )),
            estimated_cost_units: Some(bigint(plan.estimated_cost_units)),
        },
        PromotionAdmission::Rejected(rejection) => NativePromotionAdmission {
            status: "rejected".to_owned(),
            rejection: Some(native_promotion_rejection(rejection).to_owned()),
            operation_id: None,
            object_id: None,
            source_location_id: None,
            destination_location_id: None,
            estimated_cost_units: None,
        },
    }
}

const fn native_promotion_rejection(value: PromotionRejection) -> &'static str {
    match value {
        PromotionRejection::WrongVolume => "wrong-volume",
        PromotionRejection::StaleGeneration => "stale-generation",
        PromotionRejection::InvalidRequest => "invalid-request",
        PromotionRejection::InputCapacity => "input-capacity",
        PromotionRejection::MissingSource => "missing-source",
        PromotionRejection::DuplicateObject => "duplicate-object",
        PromotionRejection::DuplicateOperation => "duplicate-operation",
        PromotionRejection::ActiveCapacity => "active-capacity",
        PromotionRejection::NoDestination => "no-destination",
        PromotionRejection::LowUsefulness => "low-usefulness",
    }
}

fn native_speculation_preemption(
    value: acyclic_fs::SpeculationPreemption,
) -> NativeSpeculationPreemption {
    NativeSpeculationPreemption {
        residency_operation_ids: value
            .residency
            .into_iter()
            .map(|value| Buffer::from(value.into_bytes().to_vec()))
            .collect(),
        promotion_operation_ids: value
            .promotion
            .into_iter()
            .map(|value| Buffer::from(value.into_bytes().to_vec()))
            .collect(),
    }
}

fn native_residency_metrics_json(value: acyclic_fs::ResidencyMetrics) -> serde_json::Value {
    serde_json::json!({
        "candidates": value.candidates.to_string(),
        "admitted": value.admitted.to_string(),
        "active": value.active.to_string(),
        "activeBytes": value.active_bytes.to_string(),
        "useful": value.useful.to_string(),
        "wasted": value.wasted.to_string(),
        "rejectedFence": value.rejected_fence.to_string(),
        "rejectedDuplicate": value.rejected_duplicate.to_string(),
        "rejectedCapacity": value.rejected_capacity.to_string(),
        "rejectedCost": value.rejected_cost.to_string(),
        "rejectedUsefulness": value.rejected_usefulness.to_string(),
    })
}

fn native_promotion_metrics_json(value: acyclic_fs::PromotionMetrics) -> serde_json::Value {
    serde_json::json!({
        "candidates": value.candidates.to_string(),
        "satisfied": value.satisfied.to_string(),
        "planned": value.planned.to_string(),
        "active": value.active.to_string(),
        "activeBytes": value.active_bytes.to_string(),
        "activeCostUnits": value.active_cost_units.to_string(),
        "useful": value.useful.to_string(),
        "wasted": value.wasted.to_string(),
        "rejected": value.rejected.to_string(),
    })
}

fn encode_checkpoint_receipt(
    receipt: &acyclic_fs::FsReceipt<acyclic_fs::GenerationId>,
) -> Result<NativeCheckpointResult> {
    Ok(NativeCheckpointResult {
        generation_id: Buffer::from(receipt.value.digest().into_bytes().to_vec()),
        work_json: serde_json::to_string(&receipt.work).map_err(napi_error)?,
    })
}

fn encode_created_mutation(
    receipt: &acyclic_fs::FsReceipt<FileId>,
) -> Result<NativeMutationResult> {
    Ok(NativeMutationResult {
        file_id: Some(Buffer::from(receipt.value.into_bytes().to_vec())),
        work_json: serde_json::to_string(&receipt.work).map_err(napi_error)?,
    })
}

fn encode_export_manifest(
    manifest: GenerationExportManifest,
    work: acyclic_fs::WorkCounters,
) -> Result<NativeExportManifest> {
    let manifest_bytes = encode_generation_export_manifest(&manifest).map_err(napi_error)?;
    Ok(NativeExportManifest {
        manifest_bytes: Buffer::from(manifest_bytes),
        objects: manifest.objects.into_iter().map(encode_object_id).collect(),
        work_json: serde_json::to_string(&work).map_err(napi_error)?,
    })
}

fn decode_export_manifest(manifest: &NativeExportManifest) -> Result<GenerationExportManifest> {
    const MAXIMUM_MANIFEST_BYTES: u64 = 256 * 1024 * 1024;
    const MAXIMUM_MANIFEST_OBJECTS: u64 = 1_000_000;
    decode_generation_export_manifest(
        &manifest.manifest_bytes,
        MAXIMUM_MANIFEST_BYTES,
        MAXIMUM_MANIFEST_OBJECTS,
    )
    .map_err(napi_error)
}

fn encode_namespace_path(path: &NamespacePath) -> NativeNamespacePath {
    NativeNamespacePath {
        components: path
            .components()
            .iter()
            .map(|component| NativePathComponent {
                encoding: match component.encoding() {
                    NameEncoding::Utf8 => "utf8",
                    NameEncoding::PosixBytes => "posix-bytes",
                    NameEncoding::WindowsUtf16Le => "windows-utf16le",
                }
                .to_owned(),
                bytes: Buffer::from(component.as_bytes().to_vec()),
            })
            .collect(),
    }
}

fn encode_watch_batch(
    batch: WatchBatch,
    work: acyclic_fs::WorkCounters,
) -> Result<NativeWatchBatch> {
    match batch {
        WatchBatch::Changes {
            epoch,
            first_sequence,
            next_sequence,
            changes,
        } => Ok(NativeWatchBatch {
            status: "changes".to_owned(),
            epoch: bigint(epoch.get()),
            first_sequence: Some(bigint(first_sequence.get())),
            next_sequence: Some(bigint(next_sequence.get())),
            reason: None,
            changes: changes.into_iter().map(encode_watch_change).collect(),
            work_json: serde_json::to_string(&work).map_err(napi_error)?,
        }),
        WatchBatch::RescanRequired { epoch, reason } => Ok(NativeWatchBatch {
            status: "rescan-required".to_owned(),
            epoch: bigint(epoch.get()),
            first_sequence: None,
            next_sequence: None,
            reason: Some(watch_reason(reason).to_owned()),
            changes: Vec::new(),
            work_json: serde_json::to_string(&work).map_err(napi_error)?,
        }),
    }
}

fn encode_watch_change(change: WatchChange) -> NativeWatchChange {
    match change {
        WatchChange::Created(path) => single_watch_change("created", &path),
        WatchChange::Modified(path) => single_watch_change("modified", &path),
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

fn single_watch_change(kind: &str, path: &NamespacePath) -> NativeWatchChange {
    NativeWatchChange {
        kind: kind.to_owned(),
        path: Some(encode_namespace_path(path)),
        from: None,
        to: None,
    }
}

fn watch_reason(reason: WatchInvalidationReason) -> &'static str {
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

fn native_path(path: &str, limits: VolumeLimits) -> Result<NamespacePath> {
    let portable = PortablePath::parse(path, limits).map_err(napi_error)?;
    NamespacePath::from_portable(&portable, limits).map_err(napi_error)
}

#[allow(clippy::too_many_lines)]
fn native_authored_transaction(
    operation: NativeTransactionOperation,
    limits: VolumeLimits,
) -> Result<AuthoredMutation> {
    let NativeTransactionOperation {
        kind,
        path,
        source,
        destination,
        bytes,
        target,
        payload,
        expected_file_id,
        file_kind,
        offset,
        source_offset,
        destination_offset,
        length,
        logical_bytes,
        major,
        minor,
        replace,
        allocated,
        extend,
        keep_size,
        canonical_bytes,
    } = operation;
    let metadata = FileMetadata::default();
    Ok(match kind.as_str() {
        "create-file" => AuthoredMutation::CreateFile {
            path: native_path(&required(path, "path")?, limits)?,
            bytes: bytes::Bytes::from(required(bytes, "bytes")?.to_vec()),
            metadata,
        },
        "create-directory" => AuthoredMutation::CreateDirectory {
            path: native_path(&required(path, "path")?, limits)?,
            metadata,
        },
        "create-symbolic-link" => AuthoredMutation::CreateSymbolicLink {
            path: native_path(&required(path, "path")?, limits)?,
            target: bytes::Bytes::from(required(target, "target")?.to_vec()),
            metadata,
        },
        "create-special" => AuthoredMutation::CreateEmptySpecial {
            path: native_path(&required(path, "path")?, limits)?,
            kind: empty_special_kind(&required(file_kind, "fileKind")?)?,
            metadata,
        },
        "create-device" => AuthoredMutation::CreateDevice {
            path: native_path(&required(path, "path")?, limits)?,
            kind: device_kind(&required(file_kind, "fileKind")?)?,
            major: required(major, "major")?,
            minor: required(minor, "minor")?,
            metadata,
        },
        "create-reparse-point" => AuthoredMutation::CreateReparsePoint {
            path: native_path(&required(path, "path")?, limits)?,
            payload: bytes::Bytes::from(required(payload, "payload")?.to_vec()),
            metadata,
        },
        "remove" => AuthoredMutation::Remove {
            path: native_path(&required(path, "path")?, limits)?,
            expected_file_id: expected_file_id
                .as_deref()
                .map(fixed_16)
                .transpose()?
                .map(FileId::from_bytes),
        },
        "rename" => AuthoredMutation::Rename {
            source: native_path(&required(source, "source")?, limits)?,
            destination: native_path(&required(destination, "destination")?, limits)?,
            replace: required(replace, "replace")?,
        },
        "hard-link" => AuthoredMutation::HardLink {
            source: native_path(&required(source, "source")?, limits)?,
            destination: native_path(&required(destination, "destination")?, limits)?,
        },
        "write" => AuthoredMutation::Write {
            path: native_path(&required(path, "path")?, limits)?,
            offset: required_bigint(offset, "offset")?,
            bytes: bytes::Bytes::from(required(bytes, "bytes")?.to_vec()),
        },
        "set-metadata" => AuthoredMutation::SetMetadata {
            path: native_path(&required(path, "path")?, limits)?,
            metadata: decode_file_metadata(
                &required(canonical_bytes, "canonicalBytes")?,
                native_decode_limits(limits),
            )
            .map_err(napi_error)?,
        },
        "resize" => AuthoredMutation::Resize {
            path: native_path(&required(path, "path")?, limits)?,
            logical_bytes: required_bigint(logical_bytes, "logicalBytes")?,
        },
        "zero-range" => AuthoredMutation::ZeroRange {
            path: native_path(&required(path, "path")?, limits)?,
            range: ByteRange {
                offset: required_bigint(offset, "offset")?,
                length: required_bigint(length, "length")?,
            },
            allocated: required(allocated, "allocated")?,
            extend: required(extend, "extend")?,
        },
        "preallocate" => AuthoredMutation::Preallocate {
            path: native_path(&required(path, "path")?, limits)?,
            range: ByteRange {
                offset: required_bigint(offset, "offset")?,
                length: required_bigint(length, "length")?,
            },
            keep_size: required(keep_size, "keepSize")?,
        },
        "clone-range" => AuthoredMutation::CloneRange(FileCloneRequest {
            source: native_path(&required(source, "source")?, limits)?,
            source_offset: required_bigint(source_offset, "sourceOffset")?,
            destination: native_path(&required(destination, "destination")?, limits)?,
            destination_offset: required_bigint(destination_offset, "destinationOffset")?,
            length: required_bigint(length, "length")?,
        }),
        _ => {
            return Err(Error::new(
                Status::InvalidArg,
                "unknown transaction operation",
            ));
        }
    })
}

fn required<T>(value: Option<T>, field: &str) -> Result<T> {
    value.ok_or_else(|| Error::new(Status::InvalidArg, format!("transaction requires {field}")))
}

fn required_bigint(value: Option<BigInt>, field: &str) -> Result<u64> {
    bigint_u64(&required(value, field)?)
}

fn native_decode_limits(limits: VolumeLimits) -> DecodeLimits {
    DecodeLimits {
        maximum_object_bytes: limits.maximum_object_bytes,
        maximum_name_bytes: limits.maximum_component_bytes,
        maximum_page_items: limits.maximum_directory_page_entries,
        maximum_page_bytes: u32::try_from(limits.maximum_object_bytes).unwrap_or(u32::MAX),
        maximum_page_height: limits.maximum_page_height,
        maximum_visited_pages: u32::try_from(limits.maximum_objects_per_generation)
            .unwrap_or(u32::MAX),
    }
}

fn native_attribute_class(value: &str) -> Result<AttributeClass> {
    match value {
        "posix-xattr" => Ok(AttributeClass::PosixXattr),
        "windows-stream" => Ok(AttributeClass::WindowsStream),
        "mac-resource-fork" => Ok(AttributeClass::MacResourceFork),
        _ => Err(Error::new(
            Status::InvalidArg,
            "unknown named-attribute class",
        )),
    }
}

const fn native_attribute_class_name(value: AttributeClass) -> &'static str {
    match value {
        AttributeClass::PosixXattr => "posix-xattr",
        AttributeClass::WindowsStream => "windows-stream",
        AttributeClass::MacResourceFork => "mac-resource-fork",
    }
}

fn native_attribute_name(
    class: &str,
    name: Vec<u8>,
    limits: VolumeLimits,
) -> Result<AttributeName> {
    AttributeName::new(
        native_attribute_class(class)?,
        name,
        limits.maximum_component_bytes,
    )
    .map_err(napi_error)
}

fn native_attribute_write_mode(value: &str) -> Result<NamedAttributeWriteMode> {
    match value {
        "upsert" => Ok(NamedAttributeWriteMode::Upsert),
        "create" => Ok(NamedAttributeWriteMode::Create),
        "replace" => Ok(NamedAttributeWriteMode::Replace),
        _ => Err(Error::new(
            Status::InvalidArg,
            "unknown named-attribute write mode",
        )),
    }
}

fn native_mutation(
    file_id: Option<FileId>,
    work: acyclic_fs::WorkCounters,
) -> Result<NativeMutationResult> {
    Ok(NativeMutationResult {
        file_id: file_id.map(|value| Buffer::from(value.into_bytes().to_vec())),
        work_json: serde_json::to_string(&work).map_err(napi_error)?,
    })
}

fn bigint_u64(value: &BigInt) -> Result<u64> {
    let (negative, value, lossless) = value.get_u64();
    if negative || !lossless {
        return Err(Error::new(
            Status::InvalidArg,
            "filesystem offset and length must be lossless unsigned 64-bit integers",
        ));
    }
    Ok(value)
}

fn u16_from_u32(value: u32, field: &str) -> Result<u16> {
    u16::try_from(value).map_err(|_| {
        Error::new(
            Status::InvalidArg,
            format!("{field} must fit an unsigned 16-bit integer"),
        )
    })
}

fn native_volume_config(options: NativeVolumeOptions) -> Result<VolumeConfig> {
    let profile = match options.profile.as_str() {
        "portable" => FilesystemProfile::Portable,
        "posix" => FilesystemProfile::Posix,
        "windows" => FilesystemProfile::Windows,
        "browser" => FilesystemProfile::Browser,
        _ => return Err(Error::new(Status::InvalidArg, "invalid filesystem profile")),
    };
    let concurrency = match options.concurrency.as_str() {
        "exclusive-writer" => ConcurrencyMode::ExclusiveWriter,
        "optimistic" => ConcurrencyMode::Optimistic,
        "serialized-authority" => ConcurrencyMode::SerializedAuthority,
        _ => return Err(Error::new(Status::InvalidArg, "invalid concurrency mode")),
    };
    let lifecycle = match options.lifecycle.as_str() {
        "ephemeral" => Lifecycle::Ephemeral,
        "durable" => Lifecycle::Durable,
        _ => return Err(Error::new(Status::InvalidArg, "invalid lifecycle")),
    };
    let case_sensitivity = match options.case_sensitivity.as_str() {
        "sensitive" => CaseSensitivity::Sensitive,
        "profile-folded" => CaseSensitivity::ProfileFolded,
        _ => return Err(Error::new(Status::InvalidArg, "invalid case sensitivity")),
    };
    let unicode = match options.unicode.as_str() {
        "preserve" => UnicodePolicy::Preserve,
        "require-nfc" => UnicodePolicy::RequireNfc,
        _ => return Err(Error::new(Status::InvalidArg, "invalid Unicode policy")),
    };
    let limits = options.limits;
    let maximum_path_depth = u16::try_from(limits.maximum_path_depth)
        .map_err(|_| Error::new(Status::InvalidArg, "maximum path depth exceeds u16"))?;
    let maximum_page_height = u16::try_from(limits.maximum_page_height)
        .map_err(|_| Error::new(Status::InvalidArg, "maximum page height exceeds u16"))?;
    VolumeConfig {
        profile,
        concurrency,
        lifecycle,
        case_sensitivity,
        unicode,
        symbolic_links: options.symbolic_links,
        hard_links: options.hard_links,
        sparse_files: options.sparse_files,
        limits: VolumeLimits {
            maximum_path_bytes: limits.maximum_path_bytes,
            maximum_component_bytes: limits.maximum_component_bytes,
            maximum_path_depth,
            maximum_object_bytes: bigint_u64(&limits.maximum_object_bytes)?,
            maximum_mutations_per_batch: limits.maximum_mutations_per_batch,
            maximum_paths_per_batch: limits.maximum_paths_per_batch,
            maximum_checkout_dependencies: limits.maximum_checkout_dependencies,
            maximum_directory_page_entries: limits.maximum_directory_page_entries,
            maximum_page_height,
            maximum_read_bytes: bigint_u64(&limits.maximum_read_bytes)?,
            maximum_files_per_generation: bigint_u64(&limits.maximum_files_per_generation)?,
            maximum_objects_per_generation: bigint_u64(&limits.maximum_objects_per_generation)?,
            maximum_generation_bytes: bigint_u64(&limits.maximum_generation_bytes)?,
        },
    }
    .validate()
    .map_err(napi_error)
}

fn mutation_result(work: acyclic_fs::WorkCounters) -> Result<NativeMutationResult> {
    Ok(NativeMutationResult {
        file_id: None,
        work_json: serde_json::to_string(&work).map_err(napi_error)?,
    })
}

fn bigint(value: u64) -> BigInt {
    BigInt {
        sign_bit: false,
        words: vec![value],
    }
}

fn native_object_cache_options(options: NativeObjectCacheOptions) -> Result<ObjectCacheOptions> {
    let converted = ObjectCacheOptions {
        maximum_entries: options.maximum_entries,
        maximum_bytes: bigint_u64(&options.maximum_bytes)?,
        maximum_in_flight: options.maximum_in_flight,
        maximum_waiters_per_object: options.maximum_waiters_per_object,
    };
    drop(options);
    Ok(converted)
}

fn commit_result(
    outcome: CheckoutCommitOutcome,
    work: acyclic_fs::WorkCounters,
) -> Result<NativeCommitResult> {
    let (status, generation_id, epoch, sequence, fingerprint) = match outcome {
        CheckoutCommitOutcome::Committed {
            generation_id,
            head,
        } => (
            "committed",
            Some(Buffer::from(generation_id.digest().into_bytes().to_vec())),
            Some(bigint(head.epoch.get())),
            Some(bigint(head.sequence.get())),
            None,
        ),
        CheckoutCommitOutcome::AlreadyCommitted {
            generation_id,
            head,
        } => (
            "already-committed",
            Some(Buffer::from(generation_id.digest().into_bytes().to_vec())),
            Some(bigint(head.epoch.get())),
            Some(bigint(head.sequence.get())),
            None,
        ),
        CheckoutCommitOutcome::Conflict { actual } => (
            "conflict",
            None,
            Some(bigint(actual.epoch.get())),
            Some(bigint(actual.sequence.get())),
            None,
        ),
        CheckoutCommitOutcome::Fenced { actual_epoch } => {
            ("fenced", None, Some(bigint(actual_epoch.get())), None, None)
        }
        CheckoutCommitOutcome::IdempotencyConflict {
            committed_fingerprint,
        } => (
            "idempotency-conflict",
            None,
            None,
            None,
            Some(Buffer::from(committed_fingerprint.into_bytes().to_vec())),
        ),
    };
    Ok(NativeCommitResult {
        status: status.to_owned(),
        generation_id,
        epoch,
        sequence,
        committed_fingerprint: fingerprint,
        work_json: serde_json::to_string(&work).map_err(napi_error)?,
    })
}

fn live_mutation_result(
    outcome: LiveMutationOutcome,
    work: acyclic_fs::WorkCounters,
) -> Result<NativeLiveMutationResult> {
    let (status, generation_id, epoch, sequence, conflict_count, truncated, fingerprint) =
        match outcome {
            LiveMutationOutcome::Committed {
                generation_id,
                head,
            } => (
                "committed",
                Some(Buffer::from(generation_id.digest().into_bytes().to_vec())),
                Some(bigint(head.epoch.get())),
                Some(bigint(head.sequence.get())),
                0,
                false,
                None,
            ),
            LiveMutationOutcome::AlreadyCommitted {
                generation_id,
                head,
            } => (
                "already-committed",
                Some(Buffer::from(generation_id.digest().into_bytes().to_vec())),
                Some(bigint(head.epoch.get())),
                Some(bigint(head.sequence.get())),
                0,
                false,
                None,
            ),
            LiveMutationOutcome::Conflicted {
                conflicts,
                truncated,
            } => (
                "conflicted",
                None,
                None,
                None,
                u32::try_from(conflicts.len()).unwrap_or(u32::MAX),
                truncated,
                None,
            ),
            LiveMutationOutcome::RetryLimit { actual } => (
                "retry-limit",
                None,
                Some(bigint(actual.epoch.get())),
                Some(bigint(actual.sequence.get())),
                0,
                false,
                None,
            ),
            LiveMutationOutcome::Fenced { actual_epoch } => (
                "fenced",
                None,
                Some(bigint(actual_epoch.get())),
                None,
                0,
                false,
                None,
            ),
            LiveMutationOutcome::IdempotencyConflict {
                committed_fingerprint,
            } => (
                "idempotency-conflict",
                None,
                None,
                None,
                0,
                false,
                Some(Buffer::from(committed_fingerprint.into_bytes().to_vec())),
            ),
        };
    Ok(NativeLiveMutationResult {
        status: status.to_owned(),
        generation_id,
        epoch,
        sequence,
        conflict_count,
        truncated,
        committed_fingerprint: fingerprint,
        work_json: serde_json::to_string(&work).map_err(napi_error)?,
    })
}

fn live_outcome_resolved(outcome: &LiveMutationOutcome) -> bool {
    matches!(
        outcome,
        LiveMutationOutcome::Committed { .. }
            | LiveMutationOutcome::AlreadyCommitted { .. }
            | LiveMutationOutcome::IdempotencyConflict { .. }
    )
}

fn file_kind(kind: FileKind) -> &'static str {
    match kind {
        FileKind::Regular => "regular",
        FileKind::Directory => "directory",
        FileKind::SymbolicLink => "symbolic-link",
        FileKind::Fifo => "fifo",
        FileKind::Socket => "socket",
        FileKind::CharacterDevice => "character-device",
        FileKind::BlockDevice => "block-device",
        FileKind::ReparsePoint => "reparse-point",
        FileKind::MountBoundary => "mount-boundary",
    }
}

fn signed_bigint(value: i64) -> BigInt {
    BigInt {
        sign_bit: value.is_negative(),
        words: vec![value.unsigned_abs()],
    }
}

fn native_workspace_metadata(value: WorkspaceMetadata) -> NativeWorkspaceMetadata {
    NativeWorkspaceMetadata {
        posix_mode: value.posix_mode,
        posix_uid: value.posix_uid,
        posix_gid: value.posix_gid,
        posix_flags: value.posix_flags.map(bigint),
        windows_attributes: value.windows_attributes,
        created_ns: value.created_ns.map(signed_bigint),
        modified_ns: value.modified_ns.map(signed_bigint),
        accessed_ns: value.accessed_ns.map(signed_bigint),
        changed_ns: value.changed_ns.map(signed_bigint),
        has_named_attributes: value.has_named_attributes,
        has_acl: value.has_acl,
        has_security_descriptor: value.has_security_descriptor,
    }
}

#[allow(clippy::needless_pass_by_value)]
fn native_workspace_stat(value: WorkspaceStat) -> NativeWorkspaceStat {
    NativeWorkspaceStat {
        file_id: Buffer::from(value.file_id.into_bytes().to_vec()),
        kind: file_kind(value.kind).to_owned(),
        link_count: bigint(value.link_count),
        logical_bytes: value.logical_bytes.map(bigint),
        metadata: native_workspace_metadata(value.metadata),
    }
}

fn native_name_encoding(value: NameEncoding) -> &'static str {
    match value {
        NameEncoding::Utf8 => "utf8",
        NameEncoding::PosixBytes => "posix-bytes",
        NameEncoding::WindowsUtf16Le => "windows-utf16le",
    }
}

#[allow(clippy::needless_pass_by_value)]
fn native_workspace_name(value: NativeWorkspaceName) -> Result<LogicalName> {
    let encoding = match value.encoding.as_str() {
        "utf8" => NameEncoding::Utf8,
        "posix-bytes" => NameEncoding::PosixBytes,
        "windows-utf16le" => NameEncoding::WindowsUtf16Le,
        _ => return Err(Error::new(Status::InvalidArg, "unknown name encoding")),
    };
    LogicalName::new(encoding, value.bytes.to_vec(), u32::MAX).map_err(napi_error)
}

fn native_workspace_directory_page(value: WorkspaceDirectoryPage) -> NativeWorkspaceDirectoryPage {
    NativeWorkspaceDirectoryPage {
        entries: value
            .entries
            .into_iter()
            .map(|entry| NativeWorkspaceDirectoryEntry {
                name: NativeWorkspaceName {
                    encoding: native_name_encoding(entry.name.encoding()).to_owned(),
                    bytes: Buffer::from(entry.name.as_bytes().to_vec()),
                },
                file_id: Buffer::from(entry.file_id.into_bytes().to_vec()),
                kind: file_kind(entry.kind).to_owned(),
            })
            .collect(),
        has_more: value.has_more,
    }
}

fn native_workspace_extent_plan(value: WorkspaceExtentPlan) -> NativeWorkspaceExtentPlan {
    NativeWorkspaceExtentPlan {
        spans: value
            .spans
            .into_iter()
            .map(|span| NativeWorkspaceExtentSpan {
                offset: bigint(span.offset),
                length: bigint(span.length),
                source_end: bigint(span.source_end),
                kind: match span.kind {
                    WorkspaceExtentKind::Hole => "hole",
                    WorkspaceExtentKind::AllocatedZero => "allocated-zero",
                    WorkspaceExtentKind::Content => "content",
                }
                .to_owned(),
            })
            .collect(),
    }
}

fn empty_special_kind(kind: &str) -> Result<FileKind> {
    match kind {
        "fifo" => Ok(FileKind::Fifo),
        "socket" => Ok(FileKind::Socket),
        "mount-boundary" => Ok(FileKind::MountBoundary),
        _ => Err(Error::new(
            Status::InvalidArg,
            "unknown empty special-file kind",
        )),
    }
}

fn device_kind(kind: &str) -> Result<FileKind> {
    match kind {
        "character-device" => Ok(FileKind::CharacterDevice),
        "block-device" => Ok(FileKind::BlockDevice),
        _ => Err(Error::new(Status::InvalidArg, "unknown device kind")),
    }
}

fn extent_seek_target(target: &str) -> Result<ExtentSeekTarget> {
    match target {
        "data" => Ok(ExtentSeekTarget::Data),
        "hole" => Ok(ExtentSeekTarget::Hole),
        _ => Err(Error::new(Status::InvalidArg, "unknown sparse seek target")),
    }
}

fn boundary_budget() -> WorkBudget {
    const OPERATIONS: u64 = 1_000_000;
    const BYTES: u64 = 256 * 1024 * 1024;
    WorkBudget {
        authority_records_read: OPERATIONS,
        authority_records_appended: OPERATIONS,
        authority_bytes_read: BYTES,
        authority_bytes_written: BYTES,
        object_probes: OPERATIONS,
        backend_read_operations: OPERATIONS,
        backend_write_operations: OPERATIONS,
        durability_operations: OPERATIONS,
        page_reads: OPERATIONS,
        page_writes: OPERATIONS,
        object_bytes_read: BYTES,
        object_bytes_written: BYTES,
        bytes_hashed: BYTES,
        bytes_copied: BYTES,
        bytes_encoded: BYTES,
        source_bytes_read: BYTES,
        output_bytes: BYTES,
        items_examined: OPERATIONS,
        items_returned: OPERATIONS,
        allocation_operations: OPERATIONS,
        peak_allocation_bytes: BYTES,
        materializations: OPERATIONS,
    }
}

#[cfg(test)]
#[allow(clippy::large_futures)]
mod tests {
    use super::*;

    fn test_config() -> VolumeConfig {
        VolumeConfig {
            profile: FilesystemProfile::Portable,
            concurrency: ConcurrencyMode::Optimistic,
            lifecycle: Lifecycle::Durable,
            case_sensitivity: CaseSensitivity::Sensitive,
            unicode: UnicodePolicy::Preserve,
            symbolic_links: true,
            hard_links: true,
            sparse_files: true,
            limits: VolumeLimits::default(),
        }
    }

    #[test]
    fn capability_identity_matches_build_target() {
        let facts = native_capabilities();
        assert_eq!(facts.version, env!("CARGO_PKG_VERSION"));
        assert_eq!(facts.platform, std::env::consts::OS);
        assert_eq!(facts.architecture, std::env::consts::ARCH);
        assert!(facts.local);
        assert!(facts.native_watch);
    }

    #[test]
    fn native_owner_exposes_and_clears_shared_object_acceleration()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let fs = NativeFs::new(
            root.path().to_string_lossy().into_owned(),
            NativeObjectCacheOptions {
                maximum_entries: 8,
                maximum_bytes: bigint(1024),
                maximum_in_flight: 2,
                maximum_waiters_per_object: 2,
            },
        )?;
        assert_eq!(fs.object_cache_stats()?.resident_entries.get_u64().1, 0);
        fs.clear_object_cache()?;
        assert_eq!(fs.object_cache_stats()?.resident_bytes.get_u64().1, 0);
        Ok(())
    }

    #[test]
    fn native_owner_rejects_zero_or_lossy_object_cache_bounds()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        assert!(
            NativeFs::new(
                root.path().to_string_lossy().into_owned(),
                NativeObjectCacheOptions {
                    maximum_entries: 0,
                    maximum_bytes: bigint(1024),
                    maximum_in_flight: 2,
                    maximum_waiters_per_object: 2,
                },
            )
            .is_err()
        );
        assert!(
            NativeFs::new(
                root.path().to_string_lossy().into_owned(),
                NativeObjectCacheOptions {
                    maximum_entries: 8,
                    maximum_bytes: BigInt {
                        sign_bit: false,
                        words: vec![0, 1],
                    },
                    maximum_in_flight: 2,
                    maximum_waiters_per_object: 2,
                },
            )
            .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn native_owner_exposes_both_generation_fenced_speculation_engines()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let fs = NativeFs::new(
            root.path().to_string_lossy().into_owned(),
            NativeObjectCacheOptions {
                maximum_entries: 8,
                maximum_bytes: bigint(1024),
                maximum_in_flight: 2,
                maximum_waiters_per_object: 2,
            },
        )?;
        let volume_id = VolumeId::new();
        let generation_id = acyclic_fs::GenerationId::new(Digest::from_bytes([7_u8; 32]));
        let speculation = fs.create_speculation(
            Buffer::from(volume_id.into_bytes().to_vec()),
            Buffer::from(generation_id.digest().into_bytes().to_vec()),
            NativeSpeculationOptions {
                residency: NativeResidencySpeculationOptions {
                    maximum_active_operations: 2,
                    maximum_active_bytes: bigint(1024),
                    outcome_window: 8,
                    traffic_window: 8,
                    speculative_cost_basis_points: 10_000,
                    minimum_usefulness_samples: 2,
                    minimum_usefulness_basis_points: 1,
                },
                promotion: NativePromotionSpeculationOptions {
                    maximum_active_operations: 2,
                    maximum_active_bytes: bigint(1024),
                    maximum_active_cost_units: bigint(1024),
                    maximum_residency_facts: 4,
                    maximum_destinations: 4,
                    maximum_accepted_tiers: 4,
                    outcome_window: 8,
                    minimum_usefulness_samples: 2,
                    minimum_usefulness_basis_points: 1,
                },
            },
        )?;
        let operation_id = OperationId::new();
        let object_id = ObjectId {
            kind: ObjectKind::Blob,
            digest: Digest::from_bytes([9_u8; 32]),
        };
        let admission = speculation
            .observe(NativeResidencyObservation {
                operation_id: Buffer::from(operation_id.into_bytes().to_vec()),
                volume_id: Buffer::from(volume_id.into_bytes().to_vec()),
                generation_id: Buffer::from(generation_id.digest().into_bytes().to_vec()),
                foreground_bytes: bigint(1024),
                object_id: encode_object_id(object_id),
                maximum_bytes: bigint(16),
                reason: "sequential-range".to_owned(),
            })
            .await?;
        assert_eq!(admission.status, "admitted");

        let promotion = speculation
            .plan_promotion(
                Buffer::from(operation_id.into_bytes().to_vec()),
                vec!["node-local".to_owned()],
                vec![NativeObjectResidency {
                    object_id: encode_object_id(object_id),
                    location_id: Buffer::from([1_u8; 16].to_vec()),
                    tier: "durable-origin".to_owned(),
                    source_priority: 0,
                }],
                vec![NativePromotionDestination {
                    location_id: Buffer::from([2_u8; 16].to_vec()),
                    tier: "node-local".to_owned(),
                    writable: true,
                    maximum_object_bytes: bigint(1024),
                    priority: 0,
                    cost_units_per_byte: bigint(1),
                }],
            )
            .await?;
        assert_eq!(promotion.status, "planned");
        speculation
            .finish_promotion(Buffer::from(operation_id.into_bytes().to_vec()), true)
            .await?;
        speculation
            .finish_residency(Buffer::from(operation_id.into_bytes().to_vec()), true)
            .await?;
        let metrics = speculation.metrics_json().await?;
        assert!(metrics.contains("\"residency\""));
        assert!(metrics.contains("\"promotion\""));
        Ok(())
    }

    async fn exercise_native_arbitrary_shape_transaction(
        transaction: &NativeWorkspaceTransaction,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        transaction.create_directory("/shapes".to_owned()).await?;
        transaction
            .write(
                "/shapes/source".to_owned(),
                Buffer::from(b"abcdef".to_vec()),
            )
            .await?;
        transaction
            .write(
                "/shapes/destination".to_owned(),
                Buffer::from(b"......".to_vec()),
            )
            .await?;
        transaction
            .create_symbolic_link(
                "/shapes/symlink".to_owned(),
                Buffer::from(b"source".to_vec()),
            )
            .await?;
        transaction
            .hard_link("/shapes/source".to_owned(), "/shapes/hard-link".to_owned())
            .await?;
        transaction
            .write_range(
                "/shapes/source".to_owned(),
                bigint(1),
                Buffer::from(b"Z".to_vec()),
            )
            .await?;
        transaction
            .zero_range(
                "/shapes/source".to_owned(),
                bigint(2),
                bigint(2),
                false,
                false,
            )
            .await?;
        transaction
            .preallocate("/shapes/source".to_owned(), bigint(0), bigint(6), true)
            .await?;
        transaction
            .clone_range(
                "/shapes/source".to_owned(),
                bigint(0),
                "/shapes/destination".to_owned(),
                bigint(1),
                bigint(5),
            )
            .await?;
        transaction
            .resize("/shapes/destination".to_owned(), bigint(8))
            .await?;
        Ok(())
    }

    async fn assert_native_arbitrary_shape_files(
        workspace: &NativeWorkspace,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        for path in ["/shapes/source", "/shapes/hard-link"] {
            assert_eq!(
                workspace.read(path.to_owned(), bigint(8)).await?.as_ref(),
                b"aZ\0\0ef"
            );
        }
        assert_eq!(
            workspace
                .read("/shapes/destination".to_owned(), bigint(8))
                .await?
                .as_ref(),
            b".aZ\0\0e\0\0"
        );
        Ok(())
    }

    async fn assert_native_bounded_reads(
        workspace: &NativeWorkspace,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            workspace
                .read_range("/output/status".to_owned(), bigint(1), bigint(3))
                .await?
                .as_ref(),
            b"ead"
        );
        let source_stat = workspace.stat("/shapes/source".to_owned()).await?;
        let hard_link_stat = workspace.stat("/shapes/hard-link".to_owned()).await?;
        assert_eq!(bigint_u64(&source_stat.link_count)?, 2);
        assert_eq!(
            source_stat.file_id.as_ref(),
            hard_link_stat.file_id.as_ref()
        );
        assert_eq!(source_stat.kind, "regular");
        assert_eq!(
            workspace
                .read_symbolic_link("/shapes/symlink".to_owned())
                .await?
                .as_ref(),
            b"source"
        );
        let first_page = workspace
            .list_directory("/shapes".to_owned(), None, 1)
            .await?;
        assert!(first_page.has_more);
        let remaining_page = workspace
            .list_directory(
                "/shapes".to_owned(),
                Some(NativeWorkspaceName {
                    encoding: first_page.entries[0].name.encoding.clone(),
                    bytes: Buffer::from(first_page.entries[0].name.bytes.as_ref().to_vec()),
                }),
                16,
            )
            .await?;
        assert_eq!(remaining_page.entries.len(), 3);
        let extents = workspace
            .plan_extents("/shapes/source".to_owned(), bigint(0), bigint(6), 8)
            .await?;
        assert!(extents.spans.iter().any(|span| span.kind == "content"));
        assert!(
            extents
                .spans
                .iter()
                .any(|span| span.kind == "allocated-zero")
        );
        Ok(())
    }

    async fn assert_native_transaction_rebase(
        workspace: &NativeWorkspace,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let first = workspace
            .begin_transaction(Some(Buffer::from(vec![31; 16])))
            .await?;
        let disjoint = workspace
            .begin_transaction(Some(Buffer::from(vec![32; 16])))
            .await?;
        first
            .write("/native-race-a".to_owned(), Buffer::from(vec![1]))
            .await?;
        disjoint
            .write("/native-race-b".to_owned(), Buffer::from(vec![2]))
            .await?;
        assert_eq!(first.commit().await?.status, "committed");
        assert_eq!(disjoint.commit().await?.status, "conflict");
        let safe = disjoint.rebase(16).await?;
        assert_eq!(safe.status, "rebased");
        assert!(safe.conflicts.is_empty());
        assert_eq!(disjoint.commit().await?.status, "committed");

        let winner = workspace
            .begin_transaction(Some(Buffer::from(vec![33; 16])))
            .await?;
        let loser = workspace
            .begin_transaction(Some(Buffer::from(vec![34; 16])))
            .await?;
        winner
            .write("/native-race-a".to_owned(), Buffer::from(vec![3]))
            .await?;
        loser
            .write("/native-race-a".to_owned(), Buffer::from(vec![4]))
            .await?;
        assert_eq!(winner.commit().await?.status, "committed");
        assert_eq!(loser.commit().await?.status, "conflict");
        let conflict = loser.rebase(16).await?;
        assert_eq!(conflict.status, "conflicted");
        assert!(!conflict.conflicts.is_empty());
        assert!(!conflict.truncated);
        Ok(())
    }

    #[tokio::test]
    async fn native_named_workspace_is_durable_binary_and_fork_exact()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let fs = NativeFs::new(
            root.path().to_string_lossy().into_owned(),
            NativeObjectCacheOptions {
                maximum_entries: 8,
                maximum_bytes: bigint(1024 * 1024),
                maximum_in_flight: 2,
                maximum_waiters_per_object: 2,
            },
        )?;
        let workspace = fs.create_workspace("main".to_owned()).await?;
        assert_eq!(workspace.name(), "main");
        let committed = workspace
            .write("/value.bin".to_owned(), Buffer::from(vec![0, 1, 2, 255]))
            .await?;
        assert_eq!(committed.status, "committed");
        assert_eq!(
            workspace
                .read("/value.bin".to_owned(), bigint(16))
                .await?
                .as_ref(),
            &[0, 1, 2, 255]
        );
        let exact = workspace.sync().await?;
        workspace.checkpoint("before-output".to_owned()).await?;
        exact.pin("input-generation".to_owned()).await?;
        let transaction = workspace
            .begin_transaction(Some(Buffer::from(vec![7; 16])))
            .await?;
        transaction
            .create_dir_all("/output/nested".to_owned())
            .await?;
        transaction
            .copy("/value.bin".to_owned(), "/output/nested/copied".to_owned())
            .await?;
        transaction
            .rename(
                "/output/nested/copied".to_owned(),
                "/output/result".to_owned(),
            )
            .await?;
        transaction
            .write("/output/status".to_owned(), Buffer::from(b"ready".to_vec()))
            .await?;
        exercise_native_arbitrary_shape_transaction(&transaction).await?;
        assert_eq!(transaction.commit().await?.status, "committed");
        assert_eq!(
            workspace
                .read("/output/status".to_owned(), bigint(5))
                .await?
                .as_ref(),
            b"ready"
        );
        Box::pin(assert_native_arbitrary_shape_files(&workspace)).await?;
        Box::pin(assert_native_bounded_reads(&workspace)).await?;
        Box::pin(assert_native_transaction_rebase(&workspace)).await?;
        assert!(
            exact
                .read("/output/status".to_owned(), bigint(5))
                .await
                .is_err()
        );
        let exact_fork = workspace
            .fork_at("exact-agent".to_owned(), &exact, None)
            .await?;
        assert!(
            exact_fork
                .read("/output/status".to_owned(), bigint(5))
                .await
                .is_err()
        );
        let fork = workspace.fork("agent".to_owned(), None).await?;
        assert_eq!(
            fork.read("/value.bin".to_owned(), bigint(16))
                .await?
                .as_ref(),
            &[0, 1, 2, 255]
        );
        drop(fs);

        let reopened = NativeFs::new(
            root.path().to_string_lossy().into_owned(),
            NativeObjectCacheOptions {
                maximum_entries: 8,
                maximum_bytes: bigint(1024 * 1024),
                maximum_in_flight: 2,
                maximum_waiters_per_object: 2,
            },
        )?
        .open_workspace("main".to_owned())
        .await?;
        assert_eq!(
            reopened
                .read("/value.bin".to_owned(), bigint(16))
                .await?
                .as_ref(),
            &[0, 1, 2, 255]
        );
        Ok(())
    }

    #[tokio::test]
    async fn native_attached_workspace_rescans_and_seals_one_source()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let source = tempfile::tempdir()?;
        std::fs::write(source.path().join("value.bin"), [1_u8, 2, 3])?;
        let fs = NativeFs::new(
            root.path().to_string_lossy().into_owned(),
            NativeObjectCacheOptions {
                maximum_entries: 8,
                maximum_bytes: bigint(1024 * 1024),
                maximum_in_flight: 2,
                maximum_waiters_per_object: 2,
            },
        )?;
        let workspace = fs
            .attach_directory(
                "attached".to_owned(),
                source.path().to_string_lossy().into_owned(),
                NativeSourceOptions {
                    mode: "tracking".to_owned(),
                    maximum_paths: 128,
                    maximum_extent_spans: 128,
                    maximum_queued_changes: 128,
                },
            )
            .await?;
        assert_eq!(workspace.source_state().await.status, "clean");
        assert_eq!(
            workspace
                .read("/value.bin".to_owned(), bigint(8))
                .await?
                .as_ref(),
            &[1, 2, 3]
        );
        std::fs::write(source.path().join("value.bin"), [4_u8, 5])?;
        assert_eq!(workspace.rescan_source().await?.status, "clean");
        assert_eq!(
            workspace
                .read("/value.bin".to_owned(), bigint(8))
                .await?
                .as_ref(),
            &[4, 5]
        );
        let sealed = workspace.seal().await?;
        assert_eq!(
            sealed
                .read("/value.bin".to_owned(), bigint(8))
                .await?
                .as_ref(),
            &[4, 5]
        );
        assert_eq!(workspace.source_state().await.status, "sealed");
        Ok(())
    }

    #[tokio::test]
    async fn native_workspace_change_sets_compose_and_join_atomically()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let fs = NativeFs::new(
            root.path().to_string_lossy().into_owned(),
            NativeObjectCacheOptions {
                maximum_entries: 32,
                maximum_bytes: bigint(1024 * 1024),
                maximum_in_flight: 4,
                maximum_waiters_per_object: 4,
            },
        )?;
        let main = fs.create_workspace("main".to_owned()).await?;
        main.write("/base".to_owned(), Buffer::from(vec![1_u8]))
            .await?;
        let agent = main.fork("agent".to_owned(), None).await?;
        let base = agent.sync().await?;
        agent
            .write("/first".to_owned(), Buffer::from(vec![2_u8]))
            .await?;
        let middle = agent.sync().await?;
        agent
            .write("/second".to_owned(), Buffer::from(vec![3_u8]))
            .await?;
        let end = agent.sync().await?;
        let first = agent.diff(&base, &middle, 32).await?;
        let second = agent.diff(&middle, &end, 32).await?;
        let composed = first.compose(&second, 32).await?;
        assert_eq!(composed.from().id().as_ref(), base.id().as_ref());
        assert_eq!(composed.to().id().as_ref(), end.id().as_ref());
        assert!(!composed.changes()?.files.is_empty());

        let plan = agent
            .join_into(
                &main,
                NativeJoinOptions {
                    history: "merge".to_owned(),
                    maximum_generations: 64,
                    maximum_changes: 64,
                    maximum_conflicts: 16,
                },
            )
            .await?;
        let result = plan.apply(plan.target_head(), None).await?;
        assert_eq!(result.status, "applied");
        assert_eq!(
            main.read("/second".to_owned(), bigint(8)).await?.as_ref(),
            &[3]
        );
        Ok(())
    }

    #[tokio::test]
    async fn shared_publication_fence_blocks_napi_mutation_and_reconcile_recovers()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let source_root = tempfile::tempdir()?;
        let cancellation = CancellationToken::new();
        let fs = LocalFs::local(LocalOptions::new(root.path()))?;
        let volume = fs
            .create_volume(test_config(), WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value;
        let checkout = volume
            .checkout(
                GenerationSelector::Head,
                CheckoutMode {
                    access: AccessMode::ReadWrite,
                    consistency: ConsistencyMode::Pinned,
                    mutations: MutationMode::PrivateOverlay,
                },
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value;
        let shared = Arc::new(SharedCheckout::new(checkout));
        let native = NativeCheckout {
            inner: Arc::clone(&shared),
            config: test_config(),
            cancellation: CancellationToken::new(),
            acquisition_work: acyclic_fs::WorkCounters::default(),
        };
        let unresolved = OperationId::new();
        shared.lock().await.retain_operation_id(unresolved)?;
        assert!(
            native
                .create_file("/blocked.txt".to_owned(), Buffer::from(b"blocked".to_vec()))
                .await
                .is_err()
        );
        assert!(!shared.lock().await.has_pending_mutations());

        let watcher = NativeWatcher {
            inner: std::sync::Mutex::new(FsNativeWatch::open(
                source_root.path(),
                NativeWatchOptions {
                    limits: test_config().limits,
                    maximum_queued_changes: 64,
                    recursive: true,
                },
            )?),
            pending: std::sync::Mutex::new(None),
            pending_reconcile: std::sync::Mutex::new(None),
            operation: tokio::sync::Mutex::new(()),
            checkout: Arc::clone(&shared),
            source_root: source_root.path().to_path_buf(),
            cancellation: CancellationToken::new(),
        };
        assert!(Box::pin(watcher.reconcile(64, 64)).await.is_err());
        shared.lock().await.clear_retained_operation(unresolved);
        Box::pin(watcher.reconcile(64, 64)).await?;
        Ok(())
    }
}
