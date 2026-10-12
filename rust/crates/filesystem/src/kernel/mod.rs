//! Canonical authenticated filesystem page formats and sparse algorithms.

pub(crate) mod allocation;
pub use allocation::KernelAccounting;
mod attribute;
mod attribute_access;
mod attribute_list;
mod attribute_mutation;
mod blob;
mod checkpoint;
mod closure;
pub(crate) mod codec;
mod export;
mod extent;
mod extent_clone;
mod extent_mutation;
mod file_read;
mod file_table;
mod file_table_mutation;
mod generation;
mod historical_evidence;
mod generation_mutation;
mod list;
mod live;
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
mod mark;
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
mod persistent_point;
mod probe;
mod publication;
mod publication_evidence;
mod range;
mod read;
mod rebase;
mod regular_mutation;
mod retention;
mod search;
#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
mod source_state;
mod transfer;
mod tree;
mod tree_mutation;
pub(crate) mod types;
mod volume;

pub use persistent_btree::Child;

pub(crate) use attribute_list::list_attributes_async_with_accounting;
pub(crate) use attribute_mutation::apply_attribute_mutations_with_accounting;
pub(crate) use blob::{
    build_blob_with_accounting, read_blob_range_with_accounting};
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use blob::PreparedBlobDemandFailure;
pub(crate) use checkpoint::build_checkpoint_with_accounting;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use checkpoint::build_checkpoint_with_native_work;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use closure::{
    prove_generation_closure_with_native_work, prove_record_closure_with_native_work};
pub(crate) use file_read::{
    OriginalReadAccounting, ReadDecodeError, read_object_with_accounting};
pub(crate) use file_read::{decode_owned, owned_read_bytes};
pub(crate) use generation_mutation::{
    apply_generation_mutations_retaining_with_accounting, clone_generation_root_with_accounting};
pub(crate) use list::{
    list_tree_entries_async_with_accounting, list_tree_entries_at_or_after_async_with_accounting};
pub(crate) use merge::{
    MergeGenerationBorrowedRequest, merge_generation_with_accounting, merge_generation_with_borrowed_map_resolutions_with_accounting, merge_generation_with_borrowed_resolutions_with_accounting};
pub(crate) use namespace_path::NamespacePathStep;
pub(crate) use persistent_diff::{
    ValueChange, diff_file_records_with_accounting, diff_tree_entries_with_accounting};
pub(crate) use persistent_point::split_allocation;
pub(crate) use publication::{
    contextual_publication_fingerprint_with_accounting};
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use publication::publish_generation_async_guarded_with_native_work;
pub(crate) use range::plan_extent_range_with_accounting;
pub(crate) use rebase::{
    ClassifyRebaseFailure, classify_rebase_with_accounting};
pub(crate) use regular_mutation::apply_regular_mutation_with_accounting;
pub(crate) use search::binary_search_with_accounting;
pub(crate) use types::lowercase_unicode_with_accounting;

pub use attribute::{
    AttributeClass, AttributeEntry, AttributeError, AttributeName, AttributePage, attribute_page_id, decode_attribute_page, encode_attribute_page};
pub use attribute_access::{
    AttributeBatchLookup, AttributeLookup, AttributeLookupError, AttributeLookupFailure, lookup_attribute, lookup_attributes};
pub(crate) use attribute_access::{
    lookup_attribute_async_with_accounting};
pub use attribute_list::{
    AttributeListError, AttributeListFailure, AttributeListing, list_attributes};
pub use attribute_mutation::{
    AttributeMutation, AttributeMutationError, AttributeMutationFailure, AttributeMutationReceipt, apply_attribute_mutations};
pub use blob::{
    AsyncBlobSource, OwnedSourceChunk, BlobBuild, BlobBuildError, BlobBuildFailure, BlobBuildOptions, BlobChild, BlobChunkRef, BlobNode, BlobPage, BlobRead, BlobReadError, BlobReadFailure, blob_page_id, build_blob, build_blob_async, decode_blob_page, encode_blob_page, read_blob_range, read_blob_range_async};
pub use checkpoint::{
    CheckpointError, CheckpointFailure, CheckpointReceipt, CheckpointRequest, build_checkpoint, build_checkpoint_async};
#[cfg(feature = "s3-http")]
pub(crate) use closure::prove_blob_closure_async;
pub(crate) use closure::prove_record_closure_async;
pub use closure::{
    ClosureError, ClosureLimits, GenerationProof, GenerationProofFailure, prove_generation_closure, prove_generation_closure_async};
pub use codec::{
    CanonicalDecodeError, DecodeLimits, EncodeError, EncodeAllocationStep, EncodeWorkStep, ReadAllocationStep, encode};
pub(crate) use codec::{
    CanonicalInvariant, Decoder, Encoder};
pub use historical_evidence::{
    HistoricalGenerationEvidence, HistoricalGenerationProjection, decode_historical_generation_evidence, encode_original_generation_evidence};
pub use publication_evidence::{
    GenerationPublicationEvidence, GenerationPublicationProjection, PublicationEvidenceError, decode_generation_publication_evidence, encode_generation_publication_evidence};
pub use export::{
    GenerationExportManifest, GenerationExportManifestError, decode_generation_export_manifest, encode_generation_export_manifest, validate_generation_export_manifest};
pub use extent::{
    decode_extent_page, encode_extent_page, extent_page_id};
pub use extent_clone::{
    ExtentCloneError, ExtentCloneFailure, ExtentCloneReceipt, ExtentCloneRequest, clone_extent_range};
pub use extent_mutation::{
    ExtentMutation, ExtentMutationError, ExtentMutationFailure, ExtentMutationReceipt, apply_extent_mutations};
pub use file_read::{
    FileRangeRead, FileRangeReadError, FileRangeReadFailure, FileRangeRequest, read_file_range_async};
pub(crate) use file_read::{
    ReadOwned, read_file_range_with_accounting, read_objects_with_accounting, retained_object_reads_bytes};
pub use file_table::{
    FilePayload, FilePayloadKind, FileRecord, FileRecordBatchLookup, FileRecordLookup, FileRecordReadError, FileRecordReadFailure, FileTableError, FileTablePage, InlineFileData, InlineFileDataError, MAXIMUM_INLINE_FILE_BYTES, decode_file_table_page, encode_file_table_page, file_table_page_id, lookup_file_record, lookup_file_record_async, lookup_file_records, lookup_file_records_async};
pub(crate) use file_table::{
    decode_file_record, encode_file_record, encode_file_table_leaf_records, lookup_file_record_async_with_custody, lookup_file_records_async_with_custody};
pub(crate) use file_table::{
    lookup_file_record_async_with_accounting, lookup_file_records_async_with_accounting};
pub use file_table_mutation::{
    FileTableMutation, FileTableMutationError, FileTableMutationFailure, FileTableMutationReceipt, apply_file_table_mutations};
pub(crate) use file_table_mutation::apply_file_table_mutations_with_accounting;
pub(crate) use generation::{
    read_generation_root_with_accounting, read_generation_root_with_limits_and_accounting};
pub use generation::{
    GenerationRoot, GenerationRootDecodeStep, MAXIMUM_GENERATION_ROOT_BYTES, decode_generation_root, encode_generation_root, generation_root_id};
pub(crate) use generation_mutation::apply_generation_mutations_retaining_async;
pub use generation_mutation::{
    GenerationMutationError, GenerationMutationFailure, GenerationMutationReceipt, apply_generation_mutations, apply_generation_mutations_async};
pub(crate) use list::{
    list_tree_entries_async_with_custody};
pub use list::{
    DirectoryPage, DirectoryReadError, DirectoryReadFailure, list_tree_entries, list_tree_entries_async};
pub use live::{
    LiveMutationOutcome, LivePublicationObservation, LiveRetryAction, LiveRetryError, LiveRetryState};
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub use mark::MarkError;
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub(crate) use mark::Marker;
pub use merge::{
    MergeConflict, MergeConflictResolution, MergeConflictSide, MergeGenerationError, MergeGenerationOutcome, MergeGenerationRequest, MergeGenerationResult, merge_generation_async};
#[cfg(test)]
pub(crate) use merge::{
    file_table_mutation, merge_directory_record_async, merge_file_fields, resolve_three, tree_mutation};
pub use metadata::{
    FileMetadata, MetadataField, decode_file_metadata, encode_file_metadata, file_metadata_id};
pub use mutation::{
    FileMutation, Mutation, MutationPlan, MutationPlanError, MutationPlanFailure};
pub use namespace_path::{
    NamespacePath, NamespacePathError};
pub(crate) use path_access::{
    lookup_path_async_with_custody, lookup_paths_selected_async, observe_path_edges_async};
pub(crate) use path_access::{
    AccountedPath, lookup_path_async_with_accounting, lookup_path_refs_async_with_accounting, lookup_paths_async_with_accounting, lookup_paths_selected_async_with_accounting, observe_path_async_with_accounting, observe_path_refs_async_with_accounting, observe_paths_async_with_accounting};
pub use path_access::{
    ObservedPathBatch, ObservedPathLookup, PathBatchEntry, PathBatchLookup, PathLookup, PathLookupError, PathLookupFailure, lookup_path, lookup_path_async, lookup_path_refs, lookup_path_refs_async, lookup_paths, lookup_paths_async, observe_path_async, observe_paths_async};
pub(crate) use persistent_point::{
    Owned, retire_allocation};
pub(crate) use persistent_pagination::{
    paginate_async_with_accounting, paginate_async_at_or_after_with_accounting};
pub use probe::{
    AuthenticatedGenerationProbe, AuthenticatedProbeError, ProbeLimits, capture_content_range_bytes};
pub(crate) use publication::contextual_publication_fingerprint;
#[cfg(test)]
pub(crate) use publication::encode_publication_payload;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use publication::NativePublicationPayload;
pub use publication::{
    PublicationError, PublicationFailure, PublicationReceipt, PublishGenerationRequest, PublishedGeneration, decode_published_generation, publish_generation, publish_generation_async};
pub(crate) use publication::{
    PublicationIntent, permit_context_with_accounting, publish_generation_async_guarded, publish_generation_async_with_permit, BorrowedPublicationEvidence, decode_publication_evidence_with_accounting, contextual_publication_fingerprint_from_payload_with_accounting};
pub use range::{
    ExtentPlan, ExtentRangeRequest, ExtentReadError, ExtentReadFailure, ExtentSeekRequest, ExtentSeekTarget, ExtentSlice, plan_extent_range, plan_extent_range_async, seek_extent, seek_extent_async};
pub(crate) use range::seek_extent_with_accounting;
pub use read::{
    TreeBatchLookup, TreeLookup, TreeReadError, TreeReadFailure, lookup_tree_entries, lookup_tree_entries_async, lookup_tree_entry, lookup_tree_entry_async};
pub(crate) use read::lookup_tree_entry_async_with_custody;
pub(crate) use read::{
    lookup_tree_entries_async_with_accounting, lookup_tree_entry_async_with_accounting};
pub(crate) use rebase::{
    ProofDataClaim, ProofDataCustody};
pub use rebase::{
    AsyncRebaseProbe, CheckoutDependencies, Dependency, DependencyError, DependencyRegion, DependencyState, DependencyUse, ProbeReceipt, RebaseConflict, RebaseDecision, RebaseError, RebaseProbe, RebaseReceipt, classify_rebase, classify_rebase_async};
pub use regular_mutation::RegularMutationError;
pub(crate) use regular_mutation::{
    RegularMutation, apply_regular_mutation_async};
pub use retention::{
    RetentionCreated, RetentionCreatedError, RetentionKind, decode_retention_created, decode_workspace_deleted, encode_workspace_deleted, retention_authority_id};
pub(crate) use retention::{
    encode_retention_created};
#[cfg(all(
    feature = "local",
    feature = "native-watch",
    not(target_arch = "wasm32")
))]
pub(crate) use source_state::decode_source_volume;
#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
pub(crate) use source_state::source_authority_id;
#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
pub(crate) use source_state::{
    DurableSourceMode, DurableSourceState, SourceFact, SourceInvalidation, decode_source_fact, encode_source_fact};
pub use transfer::{
    GenerationTransferBatch, GenerationTransferError, GenerationTransferResult, TransferCursor, authenticate_generation_export_manifest_async, build_generation_export_manifest_async, export_generation_batch_async, import_generation_batch_async};
pub use tree::{
    decode_tree_page, encode_tree_page, tree_page_id};
pub use tree_mutation::{
    TreeMutation, TreeMutationError, TreeMutationFailure, TreeMutationReceipt, apply_tree_mutations};
pub(crate) use tree_mutation::apply_tree_mutations_with_accounting;
pub use types::{
    Extent, ExtentChild, ExtentKind, ExtentPage, ExtentPageError, FileKind, LogicalName, NameEncoding, TreeEntry, TreePage, TreePageError};
pub use volume::{
    VolumeCreated, VolumeCreatedError, decode_volume_created, volume_authority_id};
pub(crate) use volume::{
    CONFIG_ENCODED_BYTES, decode_config, encode_config, encode_volume_created};
