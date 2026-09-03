//! Canonical authenticated filesystem page formats and sparse algorithms.

mod allocation;
mod attribute;
mod attribute_access;
mod attribute_list;
mod attribute_mutation;
mod blob;
mod checkpoint;
mod closure;
mod codec;
mod export;
mod extent;
mod extent_clone;
mod extent_mutation;
mod file_read;
mod file_table;
mod file_table_mutation;
mod frontier;
mod generation;
mod generation_mutation;
mod list;
mod live;
mod merge;
mod metadata;
mod mutation;
mod namespace_path;
mod path_access;
mod persistent_batch;
mod persistent_btree;
mod persistent_diff;
mod persistent_io;
mod persistent_pagination;
mod probe;
mod publication;
mod range;
mod read;
mod rebase;
mod regular_mutation;
mod retention;
#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
mod source_state;
mod transfer;
mod tree;
mod tree_mutation;
mod types;
mod volume;

pub use attribute::{
    AttributeChild, AttributeClass, AttributeEntry, AttributeError, AttributeName, AttributePage,
    attribute_page_id, decode_attribute_page, encode_attribute_page,
};
pub use attribute_access::{
    AttributeBatchLookup, AttributeLookup, AttributeLookupError, AttributeLookupFailure,
    lookup_attribute, lookup_attribute_async, lookup_attributes, lookup_attributes_async,
};
pub use attribute_list::{
    AttributeListError, AttributeListFailure, AttributeListing, list_attributes,
    list_attributes_async,
};
pub use attribute_mutation::{
    AttributeMutation, AttributeMutationError, AttributeMutationFailure, AttributeMutationReceipt,
    AttributeSemanticError, apply_attribute_mutations, apply_attribute_mutations_async,
};
pub use blob::{
    AsyncBlobSource, BlobBuild, BlobBuildError, BlobBuildFailure, BlobBuildOptions, BlobChild,
    BlobChunkRef, BlobNode, BlobPage, BlobRead, BlobReadError, BlobReadFailure, blob_page_id,
    build_blob, build_blob_async, decode_blob_page, encode_blob_page, read_blob_range,
    read_blob_range_async,
};
pub use checkpoint::{
    CheckpointError, CheckpointFailure, CheckpointReceipt, CheckpointRequest, build_checkpoint,
    build_checkpoint_async,
};
pub use closure::{
    ClosureError, ClosureLimits, GenerationProof, GenerationProofFailure, prove_generation_closure,
    prove_generation_closure_async,
};
pub use codec::{CanonicalDecodeError, DecodeLimits};
pub use export::{
    GenerationExportManifest, GenerationExportManifestError, decode_generation_export_manifest,
    encode_generation_export_manifest, validate_generation_export_manifest,
};
pub use extent::{decode_extent_page, encode_extent_page, extent_page_id};
pub use extent_clone::{
    ExtentCloneError, ExtentCloneFailure, ExtentCloneReceipt, ExtentCloneRequest,
    clone_extent_range, clone_extent_range_async,
};
pub use extent_mutation::{
    ExtentMutation, ExtentMutationError, ExtentMutationFailure, ExtentMutationOptions,
    ExtentMutationReceipt, apply_extent_mutations, apply_extent_mutations_async,
};
pub use file_read::{
    FileRangeRead, FileRangeReadError, FileRangeReadFailure, FileRangeRequest,
    read_file_range_async,
};
pub use file_table::{
    FilePayload, FileRecord, FileRecordBatchLookup, FileRecordLookup, FileRecordReadError,
    FileRecordReadFailure, FileTableChild, FileTableError, FileTablePage, InlineFileData,
    InlineFileDataError, MAXIMUM_INLINE_FILE_BYTES, decode_file_table_page, encode_file_table_page,
    file_table_page_id, lookup_file_record, lookup_file_record_async, lookup_file_records,
    lookup_file_records_async,
};
pub use file_table_mutation::{
    FileTableMutation, FileTableMutationError, FileTableMutationFailure, FileTableMutationReceipt,
    FileTableSemanticError, apply_file_table_mutations, apply_file_table_mutations_async,
};
pub(crate) use generation::generation_root_parent_count;
pub use generation::{
    GenerationRoot, MAXIMUM_GENERATION_ROOT_BYTES, decode_generation_root, encode_generation_root,
    generation_root_id,
};
pub(crate) use generation_mutation::apply_generation_mutations_retaining_async;
pub use generation_mutation::{
    GenerationMutationError, GenerationMutationFailure, GenerationMutationReceipt,
    apply_generation_mutations, apply_generation_mutations_async,
};
pub use list::{
    DirectoryPage, DirectoryReadError, DirectoryReadFailure, list_tree_entries,
    list_tree_entries_async,
};
pub use live::{
    LiveMutationOutcome, LivePublicationObservation, LiveRetryAction, LiveRetryError,
    LiveRetryState,
};
pub use merge::{
    MergeConflict, MergeGenerationError, MergeGenerationOutcome, MergeGenerationRequest,
    MergeGenerationResult, merge_generation_async,
};
#[cfg(test)]
pub(crate) use merge::{
    file_table_mutation, merge_directory_record_async, merge_file_fields, resolve_three,
    tree_mutation,
};
pub use metadata::{
    FileMetadata, MetadataField, decode_file_metadata, encode_file_metadata, file_metadata_id,
};
pub use mutation::{FileMutation, Mutation, MutationPlan, MutationPlanError, MutationPlanFailure};
pub use namespace_path::{NamespacePath, NamespacePathError};
pub(crate) use path_access::observe_path_edges_async;
pub use path_access::{
    ObservedPathLookup, PathBatchEntry, PathBatchLookup, PathLookup, PathLookupError,
    PathLookupFailure, lookup_path, lookup_path_async, lookup_path_refs, lookup_path_refs_async,
    lookup_paths, lookup_paths_async, observe_path_async,
};
pub(crate) use persistent_diff::{
    DiffError as PersistentDiffError, diff_file_records_async, diff_tree_entries_async,
};
pub use probe::{
    AuthenticatedGenerationProbe, AuthenticatedProbeError, ProbeLimits, capture_content_range_bytes,
};
pub use publication::{
    PublicationError, PublicationFailure, PublicationReceipt, PublishGenerationRequest,
    PublishedGeneration, decode_published_generation, publish_generation, publish_generation_async,
};
pub use range::{
    ExtentPlan, ExtentRangeRequest, ExtentReadError, ExtentReadFailure, ExtentSeekRequest,
    ExtentSeekTarget, ExtentSlice, plan_extent_range, plan_extent_range_async, seek_extent,
    seek_extent_async,
};
pub use read::{
    TreeBatchLookup, TreeLookup, TreeReadError, TreeReadFailure, lookup_tree_entries,
    lookup_tree_entries_async, lookup_tree_entry, lookup_tree_entry_async,
};
pub use rebase::{
    AsyncRebaseProbe, CheckoutDependencies, Dependency, DependencyError, DependencyRegion,
    DependencyState, DependencyUse, ProbeReceipt, RebaseConflict, RebaseDecision, RebaseError,
    RebaseProbe, RebaseReceipt, classify_rebase, classify_rebase_async,
};
pub use regular_mutation::RegularMutationError;
pub(crate) use regular_mutation::{RegularMutation, apply_regular_mutation_async};
pub use retention::{
    RetentionCreated, RetentionCreatedError, RetentionKind, decode_retention_created,
    decode_workspace_deleted, encode_retention_created, encode_workspace_deleted,
    retention_authority_id,
};
#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
pub(crate) use source_state::{
    DurableSourceMode, DurableSourceState, SourceFact, SourceInvalidation, decode_source_fact,
    encode_source_fact,
};
#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
pub(crate) use source_state::{decode_source_volume, source_authority_id};
pub use transfer::{
    GenerationTransferBatch, GenerationTransferError, GenerationTransferResult, TransferCursor,
    authenticate_generation_export_manifest_async, build_generation_export_manifest_async,
    export_generation_batch_async, import_generation_batch_async,
};
pub use tree::{decode_tree_page, encode_tree_page, tree_page_id};
pub use tree_mutation::{
    TreeMutation, TreeMutationError, TreeMutationFailure, TreeMutationReceipt,
    apply_tree_mutations, apply_tree_mutations_async,
};
pub use types::{
    Extent, ExtentChild, ExtentKind, ExtentPage, ExtentPageError, FileKind, LogicalName,
    NameEncoding, TreeChild, TreeEntry, TreePage, TreePageError,
};
pub use volume::{
    VolumeCreated, VolumeCreatedError, decode_volume_created, encode_volume_created,
    volume_authority_id,
};
