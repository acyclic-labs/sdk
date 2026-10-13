//! Thin embedded `Fs`/`Volume`/`Checkout` composition over the two durable stores.

use acyclic_stream::ProviderTask;

use crate::async_storage::{AsyncAuthorityStore, AsyncObjectStore};

use crate::cancellation::{CancellationError, CancellationToken};

use crate::foundation::{
    AuthorityId, Digest, DurableCommit, Epoch, FileId, GenerationId, Head, OperationId,
    ProposedCommit, Sequence, VolumeId,
};

use crate::heap_future::in_heap;

use crate::kernel::{
    contextual_publication_fingerprint_with_accounting, list_attributes_async_with_accounting,
    lookup_file_record_async_with_accounting, lookup_path_async_with_accounting, lookup_paths,
    lookup_paths_async_with_accounting,
    merge_generation_with_borrowed_map_resolutions_with_accounting,
    merge_generation_with_borrowed_resolutions_with_accounting,
    plan_extent_range_with_accounting, prove_record_closure_async, read_blob_range_with_accounting,
    read_generation_root_with_accounting, retire_allocation, types,
};

#[cfg(not(target_arch = "wasm32"))]
use crate::kernel::{
    prove_generation_closure_with_native_work, prove_record_closure_with_native_work,
    publish_generation_async_guarded_with_native_work,
};

use crate::kernel::allocation::{AllocationLedger, KernelAccounting, KernelOwnedAllocation};

use crate::kernel::{Owned, ProofDataClaim, ProofDataCustody};

use crate::workspace::WorkspaceError;

use crate::foundation::usize_to_u64;

#[cfg(not(target_arch = "wasm32"))]
use crate::{NativeAllocation, NativeMaterializationWork, NativeTreeMaterializationError};

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
use crate::kernel::decode_retention_created;

use crate::kernel::{
    AsyncBlobSource, AttributeEntry, AttributeListing, AttributeLookupError, AttributeMutation,
    AttributeMutationError, AttributeName, AttributePage, AuthenticatedGenerationProbe,
    AuthenticatedProbeError, BlobBuildError, BlobBuildOptions, BlobReadError, CanonicalDecodeError,
    CheckoutDependencies, CheckpointError, CheckpointRequest, ClosureError, ClosureLimits,
    DecodeLimits, Dependency, DependencyError, DependencyRegion, DependencyState, DirectoryPage,
    DirectoryReadError, ExtentPlan, ExtentRangeRequest, ExtentSeekRequest, ExtentSeekTarget,
    FileKind, FileMetadata, FileMutation, FilePayload, FileRangeRead, FileRangeReadError,
    FileRangeRequest, FileRecord, FileRecordReadError, FileTableMutationError,
    GenerationExportManifest, GenerationMutationError, GenerationRoot, GenerationTransferBatch,
    GenerationTransferError, InlineFileData, InlineFileDataError, LiveRetryAction, LiveRetryError,
    LiveRetryState, LogicalName, MergeConflictResolution, MergeConflictSide, MergeGenerationError,
    MergeGenerationOutcome, MergeGenerationRequest, MetadataField, Mutation, NameEncoding,
    NamespacePath, PathBatchLookup, PathLookup, PathLookupError, ProbeLimits, PublicationError,
    PublicationIntent, PublicationReceipt, PublishGenerationRequest, RebaseConflict,
    RebaseDecision, RebaseError, RegularMutation, RegularMutationError, RetentionCreated,
    RetentionCreatedError, RetentionKind, TransferCursor, TreeMutationError, TreePage,
    VolumeCreated, VolumeCreatedError, apply_regular_mutation_async,
    authenticate_generation_export_manifest_async, build_blob,
    build_generation_export_manifest_async, contextual_publication_fingerprint,
    decode_workspace_deleted, export_generation_batch_async, import_generation_batch_async,
    list_attributes, lookup_file_record, lookup_file_records, merge_generation_async,
    plan_extent_range, prove_generation_closure, publish_generation,
    publish_generation_async_with_permit, read_blob_range, read_file_range_async,
    retention_authority_id, volume_authority_id,
};

#[cfg(test)]
use crate::kernel::merge_directory_record_async as merge_directory_record;

pub use crate::kernel::{LiveMutationOutcome, LivePublicationObservation, MergeConflict};

#[cfg(all(
    feature = "local",
    feature = "native-watch",
    not(target_arch = "wasm32")
))]
use crate::kernel::{decode_source_volume, source_authority_id};

use crate::model::{
    AccessMode, CaseSensitivity, CheckoutMode, CheckoutModeError, ConcurrencyMode, ConsistencyMode,
    GenerationSelector, Lifecycle, MutationMode, UnicodePolicy, VolumeConfig, VolumeConfigError,
};

use crate::performance::{
    MeasuredResult, OperationFailure, OperationReceipt, WorkBudget, WorkCounters, WorkError,
};

use crate::storage::{
    AppendOutcome, AuthorityStoreError, ByteRange, FenceOutcome, HashedObject,
    OBJECT_DIGEST_ENVELOPE_BYTES, ObjectId, ObjectKind, ObjectReadRequest, ObjectStoreError,
    PublicationPermit, ReplayLimit, object_digest,
};

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
use acyclic_native_runtime::OwnershipAnchor;

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
use acyclic_objects::v1::ObjectsProvider as _;

use bytes::Bytes;

use futures::{StreamExt as _, stream};

use serde::{Deserialize, Serialize};

use std::collections::BTreeMap;

use std::mem::size_of;

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
use std::path::{Path, PathBuf};

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
use std::sync::{OnceLock, Weak};

use thiserror::Error;

mod fork_selection;
mod fork_selection_work;

#[cfg(all(test, feature = "memory"))]
#[path = "../tests/facade/mod.rs"]
mod tests;

mod attributes;
mod authored_compile;
mod authored_model;
mod authored_records;
mod candidates;
mod checkout;
mod collection;
mod commit;
mod content;
mod content_model;
mod detached;
mod diff;
mod directory;
mod error;
mod extents;
pub(crate) use extents::seek_record_extent_with_accounting;
mod fork;
mod fs;
mod generation;
mod grouped;
mod join_publication;
mod join_request;
mod join_witness;
mod lineage;
mod live;
mod local;
mod merge;
mod merge_content;
mod metadata;
mod mutation_regions;
mod mutations;
mod names;
mod namespace;
mod proof;
mod publication;
mod read;
mod read_accounting;
mod reader;
mod rebase;
mod resolved_read;
mod retention;
mod retirement;
mod staging;
mod transactions;
mod transfer;
mod tree;
mod volume_creation;
mod work;
mod workspace_creation;

pub use attributes::NamedAttributeWriteMode;
use authored_compile::clone_authored_path;
use authored_compile::clone_borrowed_authored_descriptor;
use authored_compile::push_authored_operation;
pub use authored_model::AuthoredMutation;
#[cfg(feature = "native-mount")]
pub(crate) use candidates::PreparedCandidateAdoption;
use candidates::PreparedCheckoutRoots;
use candidates::root_parent_buffer;
use candidates::root_parent_bytes;
pub use checkout::Checkout;
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub use collection::LocalGarbageCollection;
pub use content_model::ContentChange;
pub use content_model::ContentTimes;
pub use content_model::FileCloneRequest;
pub use content_model::GroupedChange;
pub use content_model::GroupedOutcome;
use content_model::content_input_path_bytes;
pub(crate) use content_model::{ContentAddress, ContentFile};
pub use detached::DetachedFile;
pub use diff::DirectoryBindingChange;
pub use diff::FileRecordChange;
pub use diff::GenerationDiff;
pub use diff::MergePreparation;
pub use directory::DirectoryPageRequest;
pub use directory::DirectoryRecordEntry;
pub use directory::DirectoryRecordPage;
pub use directory::PathMetadataLookup;
pub use directory::ResolvedDirectoryEntry;
pub use directory::ResolvedDirectoryPage;
pub(crate) use directory::list_directory_records_to_file_root_with_accounting;
pub use error::FsError;
use error::map_merge_failure;
use error::map_transfer_error;
use error::validate_checkout;
use error::validate_volume_capabilities;
use extents::regular_range_regions;
pub use fs::EmbeddedCapabilities;
pub use fs::Fs;
use fs::FsInner;
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub use fs::LocalAuthorityBackend;
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub use fs::LocalFs;
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub use fs::LocalObjectBackend;
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub use fs::LocalOperationWindowStore;
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub use fs::LocalVolume;
#[cfg(all(feature = "memory", feature = "distributed"))]
pub use fs::MemoryAuthorityBackend;
#[cfg(all(test, feature = "memory", feature = "distributed"))]
pub(crate) use fs::MemoryCheckout;
#[cfg(all(feature = "memory", feature = "distributed"))]
pub use fs::MemoryFs;
#[cfg(all(feature = "memory", feature = "distributed"))]
pub use fs::MemoryObjectBackend;
use generation::MAXIMUM_VOLUME_EVENT_BYTES;
pub use generation::Volume;
use generation::generation_from_record;
pub(crate) use generation::read_generation_root;
use generation::record_logical_bytes;
use join_publication::WorkspaceJoinPublication;
use join_publication::checkout_commit_outcome;
use join_request::WORKSPACE_JOIN_OPERATION_DOMAIN;
pub(crate) use join_request::WorkspaceJoinOutcome;
pub(crate) use join_request::WorkspaceJoinRequest;
pub(crate) use join_request::WorkspaceJoinResolutions;
use join_request::hash_join_resolutions;
use join_request::workspace_join_conflict_bytes;
use join_request::workspace_join_context;
use join_request::workspace_join_scalar;
pub use join_witness::JoinCommitWitness;
use join_witness::hash_merge_resolution;
use lineage::RebaseLineage;
pub(crate) use lineage::WorkspaceCommonAncestor;
pub(crate) use lineage::WorkspaceRebaseRequest;
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub use local::LocalOptions;
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
use local::run_local_initialization;
use metadata::AuthoredMetadataCache;
pub(crate) use metadata::read_record_metadata_to_file_root_with_accounting;
use proof::DependencyLedger;
use proof::Proof;
use publication::LastCommit;
pub(crate) use publication::PublicationClosure;
pub(crate) use publication::VerifiedPublication;
use publication::closure_limits;
use publication::durable_head;
#[cfg(not(target_arch = "wasm32"))]
use read_accounting::native_pinned_allocation;
use read_accounting::readonly_cleanup_standalone;
use read_accounting::readonly_standalone_live;
pub use reader::PinnedReader;
use rebase::probe_limits;
pub use resolved_read::FileDescription;
pub use resolved_read::ResolvedFile;
pub use resolved_read::ResolvedFileRangeReadRequest;
use resolved_read::order_batch_results;
pub use staging::StagedContent;
pub(crate) use staging::stage_content_for_volume_with_accounting;
pub use transactions::AuthoredLiveMutationResult;
pub use transactions::AuthoredTransactionResult;
use transactions::mutation_path_owned_bytes;
use tree::directory_entries;
use tree::list_directory_to_file_root_with_accounting;
use volume_creation::CREATION_FINGERPRINT_DOMAIN;
use volume_creation::creation_commit;
use volume_creation::creation_fingerprint;
use volume_creation::derived_digest;
use volume_creation::merge_creation_backend_work;
pub use work::FsResult;
pub use work::FsWork;
use work::add;
pub(crate) use work::measured;
use work::merge_simultaneous_failure;
use work::merge_simultaneous_work;
use work::remaining;
use work::remaining_owned;
use work::{authority_work, record_authority_work};
use workspace_creation::decode_first_creation_record;

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "../tests/opaque_authority.rs"]
pub(crate) mod test_authority;

use crate::kernel::{decode_published_generation, decode_volume_created};
