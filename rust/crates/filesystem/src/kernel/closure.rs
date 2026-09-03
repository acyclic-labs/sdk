//! Bounded whole-generation authentication before authority publication.

use super::{
    AttributeName, AttributePage, BlobNode, DecodeLimits, ExtentKind, ExtentPage, FilePayload,
    FileRecord, FileTablePage, GenerationRoot, LogicalName, MetadataField, TreeEntry, TreePage,
    decode_attribute_page, decode_blob_page, decode_extent_page, decode_file_metadata,
    decode_file_table_page, decode_generation_root, decode_tree_page,
};
use crate::foundation::{FileId, GenerationId};
use crate::model::FilesystemProfile;
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use crate::storage::{ObjectId, ObjectKind, ObjectStoreError};
use crate::{CancellationToken, async_storage};
use bytes::Bytes;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::mem::size_of;
use thiserror::Error;

/// Hard closure-walk limits admitted before generation publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClosureLimits {
    /// Canonical decoder/page limits.
    pub decode: DecodeLimits,
    /// Maximum distinct immutable objects read by the proof.
    pub maximum_objects: u64,
    /// Maximum path-independent file records.
    pub maximum_files: u64,
    /// Maximum cumulative canonical bytes read.
    pub maximum_object_bytes: u64,
    /// Exact namespace profile whose representable kinds are admitted.
    pub profile: FilesystemProfile,
    /// Whether symbolic-link records are admitted.
    pub symbolic_links: bool,
    /// Whether more than one namespace binding may reference a non-directory.
    pub hard_links: bool,
    /// Whether holes or allocated-zero extents are admitted.
    pub sparse_files: bool,
}

/// Successful whole-generation proof.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GenerationProof {
    /// Decoded immutable root.
    pub root: GenerationRoot,
    /// Content-addressed generation identity.
    pub generation_id: GenerationId,
    /// Distinct authenticated objects read.
    pub object_count: u64,
    /// Reachable path-independent file records.
    pub file_count: u64,
    /// Stable sorted identities in the complete authenticated closure.
    pub objects: Vec<ObjectId>,
    /// Exact proof work.
    pub work: WorkCounters,
}

/// Generation-closure failure retaining all work spent before rejection.
pub type GenerationProofFailure = OperationFailure<ClosureError>;

/// Proves the complete immutable object closure and namespace/file-table consistency.
///
/// # Errors
///
/// Fails closed on missing/corrupt objects, malformed canonical bytes, routing
/// forgery, graph cycles/aliases, unreachable records, kind/link mismatches,
/// out-of-bounds content spans, or admitted resource limits.
pub fn prove_generation_closure<S: crate::ImmediateObjectStore>(
    store: &S,
    generation_root: ObjectId,
    limits: ClosureLimits,
    budget: WorkBudget,
) -> Result<GenerationProof, GenerationProofFailure> {
    let cancellation = CancellationToken::new();
    let mut context = ProofContext::new(store, limits, budget, &cancellation);
    async_storage::poll_immediate(context.prove(generation_root))
        .map_err(|error| OperationFailure::new(error, context.work))
}

/// Asynchronously proves a complete immutable generation closure.
///
/// Browser, remote, and native execution share this one canonical proof
/// machine; only storage polling differs.
///
/// # Errors
///
/// Returns the same fail-closed proof errors as [`prove_generation_closure`],
/// including asynchronous cancellation.
pub async fn prove_generation_closure_async<S: crate::AsyncObjectStore>(
    store: &S,
    generation_root: ObjectId,
    limits: ClosureLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<GenerationProof, GenerationProofFailure> {
    let mut context = ProofContext::new(store, limits, budget, cancellation);
    context
        .prove(generation_root)
        .await
        .map_err(|error| OperationFailure::new(error, context.work))
}

struct ProofContext<'a, S> {
    store: &'a S,
    limits: ClosureLimits,
    budget: WorkBudget,
    work: WorkCounters,
    objects: HashSet<ObjectId>,
    blob_lengths: HashMap<ObjectId, u64>,
    cancellation: &'a CancellationToken,
}

impl<'a, S: crate::AsyncObjectStore> ProofContext<'a, S> {
    fn new(
        store: &'a S,
        limits: ClosureLimits,
        budget: WorkBudget,
        cancellation: &'a CancellationToken,
    ) -> Self {
        Self {
            store,
            limits,
            budget,
            work: WorkCounters::default(),
            objects: HashSet::new(),
            blob_lengths: HashMap::new(),
            cancellation,
        }
    }

    async fn prove(&mut self, generation_root: ObjectId) -> Result<GenerationProof, ClosureError> {
        Self::require_kind(generation_root, ObjectKind::GenerationRoot)?;
        let root_bytes = self.read_object(generation_root, false).await?;
        let root = decode_generation_root(&root_bytes, self.limits.decode)?;
        let generation_id = GenerationId::new(generation_root.digest);
        let records = self.prove_file_table(root.file_table).await?;
        let file_count = u64::try_from(records.len()).unwrap_or(u64::MAX);
        if file_count > self.limits.maximum_files {
            return Err(ClosureError::TooManyFiles {
                observed: file_count,
                maximum: self.limits.maximum_files,
            });
        }
        self.prove_records_and_namespace(root.root_file_id, &records)
            .await?;
        let mut objects: Vec<ObjectId> = self.objects.iter().copied().collect();
        objects.sort_unstable_by(|left, right| {
            left.kind
                .canonical_tag()
                .cmp(&right.kind.canonical_tag())
                .then_with(|| left.digest.as_bytes().cmp(right.digest.as_bytes()))
        });
        let object_bytes = u64::try_from(objects.capacity())
            .unwrap_or(u64::MAX)
            .saturating_mul(u64::try_from(size_of::<ObjectId>()).unwrap_or(u64::MAX));
        self.work = self
            .work
            .checked_add(WorkCounters {
                items_examined: u64::try_from(objects.len()).unwrap_or(u64::MAX),
                bytes_copied: u64::try_from(objects.len())
                    .unwrap_or(u64::MAX)
                    .saturating_mul(u64::try_from(size_of::<ObjectId>()).unwrap_or(u64::MAX)),
                allocation_operations: u64::from(!objects.is_empty()),
                peak_allocation_bytes: object_bytes,
                ..WorkCounters::default()
            })
            .map_err(ClosureError::Work)?;
        self.work.verify(self.budget).map_err(ClosureError::Work)?;
        Ok(GenerationProof {
            root,
            generation_id,
            object_count: u64::try_from(self.objects.len()).unwrap_or(u64::MAX),
            file_count,
            objects,
            work: self.work,
        })
    }

    async fn read_object(&mut self, object: ObjectId, page: bool) -> Result<Bytes, ClosureError> {
        self.objects.insert(object);
        let count = u64::try_from(self.objects.len()).unwrap_or(u64::MAX);
        if count > self.limits.maximum_objects {
            return Err(ClosureError::TooManyObjects {
                observed: count,
                maximum: self.limits.maximum_objects,
            });
        }
        let semantic = WorkCounters {
            page_reads: u64::from(page),
            ..WorkCounters::default()
        };
        let prospective = self.work.checked_add(semantic)?;
        let remaining = prospective.remaining(self.budget)?;
        let receipt = crate::AsyncObjectStore::read(
            self.store,
            object,
            self.limits.decode.maximum_object_bytes,
            remaining,
            self.cancellation,
        )
        .await
        .map_err(|failure| match prospective.checked_add(*failure.work) {
            Ok(combined) => {
                self.work = combined;
                ClosureError::Storage(failure.error)
            }
            Err(error) => {
                self.work = prospective;
                ClosureError::Work(error)
            }
        })?;
        self.work = prospective.checked_add(receipt.work)?;
        self.work.verify(self.budget)?;
        if self.work.object_bytes_read > self.limits.maximum_object_bytes {
            return Err(ClosureError::ClosureBytesExceeded {
                observed: self.work.object_bytes_read,
                maximum: self.limits.maximum_object_bytes,
            });
        }
        Ok(receipt.value.bytes)
    }

    async fn prove_file_table(
        &mut self,
        root: ObjectId,
    ) -> Result<BTreeMap<FileId, FileRecord>, ClosureError> {
        Self::require_kind(root, ObjectKind::FileTablePage)?;
        let mut records = BTreeMap::new();
        let mut pending = vec![(root, None, None)];
        let mut pages = HashSet::new();
        while let Some((page_id, lower, upper)) = pending.pop() {
            if !pages.insert(page_id) {
                return Err(ClosureError::RoutingAlias);
            }
            let bytes = self.read_object(page_id, true).await?;
            match decode_file_table_page(&bytes, self.limits.decode)? {
                FileTablePage::Leaf(entries) => {
                    validate_file_bounds(&entries, lower, upper)?;
                    for record in entries {
                        if records.insert(record.file_id, record).is_some() {
                            return Err(ClosureError::DuplicateFileRecord(record.file_id));
                        }
                    }
                }
                FileTablePage::Internal(children) => {
                    validate_file_child_bounds(&children, lower, upper)?;
                    for index in (0..children.len()).rev() {
                        let child = children[index];
                        let child_upper = children
                            .get(index + 1)
                            .map(|next| next.first_file_id)
                            .or(upper);
                        pending.push((child.page, Some(child.first_file_id), child_upper));
                    }
                }
            }
        }
        Ok(records)
    }

    async fn prove_records_and_namespace(
        &mut self,
        root_file_id: FileId,
        records: &BTreeMap<FileId, FileRecord>,
    ) -> Result<(), ClosureError> {
        let root = records
            .get(&root_file_id)
            .ok_or(ClosureError::MissingRootFile)?;
        if !matches!(root.payload, FilePayload::Directory { .. }) {
            return Err(ClosureError::RootIsNotDirectory);
        }
        let mut directory_entries = BTreeMap::new();
        for record in records.values() {
            if !record.kind.is_supported_by_profile(self.limits.profile)
                || (record.kind == super::FileKind::SymbolicLink && !self.limits.symbolic_links)
                || (record.link_count > 1 && !self.limits.hard_links)
            {
                return Err(ClosureError::UnsupportedVolumeSemantics);
            }
            self.prove_metadata(record.metadata).await?;
            match record.payload {
                FilePayload::Regular {
                    logical_bytes,
                    extents,
                } => {
                    self.prove_extent_tree(extents, logical_bytes).await?;
                }
                FilePayload::Directory { entries } => {
                    directory_entries.insert(record.file_id, self.prove_tree(entries).await?);
                }
                FilePayload::SymbolicLink {
                    target_bytes,
                    target,
                }
                | FilePayload::ReparsePoint {
                    payload_bytes: target_bytes,
                    payload: target,
                } => {
                    if self.prove_blob(target).await? != target_bytes {
                        return Err(ClosureError::BlobLengthMismatch);
                    }
                }
                FilePayload::InlineRegular(_) | FilePayload::Empty | FilePayload::Device { .. } => {
                }
            }
        }
        Self::validate_namespace(root_file_id, records, &directory_entries)
    }

    async fn prove_metadata(&mut self, metadata_id: ObjectId) -> Result<(), ClosureError> {
        Self::require_kind(metadata_id, ObjectKind::Metadata)?;
        // Metadata identities are immutable and authenticated. Large trees commonly
        // share one canonical metadata object across many records, so proving an
        // identity already present in this closure must not issue another backend
        // read or decode. Metadata objects enter `objects` only through this method.
        if self.objects.contains(&metadata_id) {
            return Ok(());
        }
        let bytes = self.read_object(metadata_id, false).await?;
        let metadata = decode_file_metadata(&bytes, self.limits.decode)?;
        if let MetadataField::Value(attributes) = metadata.named_attributes {
            self.prove_attributes(attributes).await?;
        }
        for blob in [metadata.acl, metadata.security_descriptor] {
            if let MetadataField::Value(blob) = blob {
                self.prove_blob(blob).await?;
            }
        }
        Ok(())
    }

    async fn prove_tree(&mut self, root: ObjectId) -> Result<Vec<TreeEntry>, ClosureError> {
        Self::require_kind(root, ObjectKind::TreePage)?;
        let mut entries = Vec::new();
        let mut pending = vec![(root, None, None)];
        let mut pages = HashSet::new();
        while let Some((page_id, lower, upper)) = pending.pop() {
            if !pages.insert(page_id) {
                return Err(ClosureError::RoutingAlias);
            }
            let bytes = self.read_object(page_id, true).await?;
            match decode_tree_page(&bytes, self.limits.decode)? {
                TreePage::Leaf(values) => {
                    validate_tree_bounds(&values, lower.as_ref(), upper.as_ref())?;
                    entries.extend(values);
                }
                TreePage::Internal(children) => {
                    validate_tree_child_bounds(&children, lower.as_ref(), upper.as_ref())?;
                    for index in (0..children.len()).rev() {
                        let child = children[index].clone();
                        let child_upper = children
                            .get(index + 1)
                            .map(|next| next.first_name.clone())
                            .or_else(|| upper.clone());
                        pending.push((child.page, Some(child.first_name), child_upper));
                    }
                }
            }
        }
        Ok(entries)
    }

    async fn prove_extent_tree(&mut self, root: ObjectId, size: u64) -> Result<(), ClosureError> {
        Self::require_kind(root, ObjectKind::ExtentPage)?;
        let mut pending = vec![(root, 0_u64, size)];
        let mut pages = HashSet::new();
        while let Some((page_id, first, end)) = pending.pop() {
            if !pages.insert(page_id) {
                return Err(ClosureError::RoutingAlias);
            }
            let bytes = self.read_object(page_id, true).await?;
            match decode_extent_page(&bytes, self.limits.decode)? {
                ExtentPage::Leaf(extents) => {
                    validate_extent_bounds(&extents, first, end)?;
                    for extent in extents {
                        if !self.limits.sparse_files
                            && !matches!(extent.kind, ExtentKind::Content { .. })
                        {
                            return Err(ClosureError::UnsupportedVolumeSemantics);
                        }
                        if let ExtentKind::Content {
                            object,
                            object_offset,
                        } = extent.kind
                        {
                            let content_end = object_offset
                                .checked_add(extent.length)
                                .ok_or(ClosureError::RangeOverflow)?;
                            if content_end > self.prove_blob(object).await? {
                                return Err(ClosureError::BlobSpanOutsideObject);
                            }
                        }
                    }
                }
                ExtentPage::Internal(children) => {
                    if children.first().map(|child| child.first_offset) != Some(first)
                        || children.last().map(|child| child.end_offset) != Some(end)
                    {
                        return Err(ClosureError::RoutingBoundsMismatch);
                    }
                    for child in children.into_iter().rev() {
                        pending.push((child.page, child.first_offset, child.end_offset));
                    }
                }
            }
        }
        Ok(())
    }

    async fn prove_blob(&mut self, root: ObjectId) -> Result<u64, ClosureError> {
        Self::require_kind(root, ObjectKind::Blob)?;
        if let Some(length) = self.blob_lengths.get(&root) {
            return Ok(*length);
        }
        let mut pending = vec![(root, 0_u64, None)];
        let mut pages = HashSet::new();
        let mut root_length = None;
        while let Some((page_id, first, end)) = pending.pop() {
            if !pages.insert(page_id) {
                return Err(ClosureError::RoutingAlias);
            }
            let bytes = self.read_object(page_id, true).await?;
            let page = decode_blob_page(&bytes, self.limits.decode)?;
            if page.first_offset != first || end.is_some_and(|value| page.end_offset != value) {
                return Err(ClosureError::RoutingBoundsMismatch);
            }
            if root_length.is_none() {
                root_length = Some(page.end_offset);
            }
            match page.node {
                BlobNode::Internal(children) => {
                    for child in children.into_iter().rev() {
                        pending.push((child.page, child.first_offset, Some(child.end_offset)));
                    }
                }
                BlobNode::Leaf(chunks) => {
                    for chunk in chunks {
                        let expected = chunk.end_offset - chunk.first_offset;
                        Self::require_kind(chunk.chunk, ObjectKind::BlobChunk)?;
                        let value = self.read_object(chunk.chunk, false).await?;
                        if u64::try_from(value.len()).unwrap_or(u64::MAX) != expected {
                            return Err(ClosureError::BlobLengthMismatch);
                        }
                    }
                }
            }
        }
        let length = root_length.ok_or(ClosureError::RoutingBoundsMismatch)?;
        self.blob_lengths.insert(root, length);
        Ok(length)
    }

    async fn prove_attributes(&mut self, root: ObjectId) -> Result<(), ClosureError> {
        Self::require_kind(root, ObjectKind::AttributePage)?;
        let mut pending = vec![(root, None, None)];
        let mut pages = HashSet::new();
        while let Some((page_id, lower, upper)) = pending.pop() {
            if !pages.insert(page_id) {
                return Err(ClosureError::RoutingAlias);
            }
            let bytes = self.read_object(page_id, true).await?;
            match decode_attribute_page(&bytes, self.limits.decode)? {
                AttributePage::Leaf(entries) => {
                    validate_attribute_bounds(&entries, lower.as_ref(), upper.as_ref())?;
                    for entry in entries {
                        if self.prove_blob(entry.value).await? != entry.value_bytes {
                            return Err(ClosureError::BlobLengthMismatch);
                        }
                    }
                }
                AttributePage::Internal(children) => {
                    validate_attribute_child_bounds(&children, lower.as_ref(), upper.as_ref())?;
                    for index in (0..children.len()).rev() {
                        let child = children[index].clone();
                        let child_upper = children
                            .get(index + 1)
                            .map(|next| next.first_name.clone())
                            .or_else(|| upper.clone());
                        pending.push((child.page, Some(child.first_name), child_upper));
                    }
                }
            }
        }
        Ok(())
    }

    fn validate_namespace(
        root: FileId,
        records: &BTreeMap<FileId, FileRecord>,
        directories: &BTreeMap<FileId, Vec<TreeEntry>>,
    ) -> Result<(), ClosureError> {
        let mut links: BTreeMap<FileId, u64> = BTreeMap::new();
        links.insert(root, 1);
        for entries in directories.values() {
            for entry in entries {
                let record = records
                    .get(&entry.file_id)
                    .ok_or(ClosureError::MissingFileRecord(entry.file_id))?;
                if record.kind != entry.kind {
                    return Err(ClosureError::NamespaceKindMismatch(entry.file_id));
                }
                let count = links.entry(entry.file_id).or_default();
                *count = count.checked_add(1).ok_or(ClosureError::RangeOverflow)?;
            }
        }
        for record in records.values() {
            if links.get(&record.file_id).copied().unwrap_or(0) != record.link_count {
                return Err(ClosureError::LinkCountMismatch(record.file_id));
            }
            if matches!(record.payload, FilePayload::Directory { .. }) && record.link_count != 1 {
                return Err(ClosureError::DirectoryHardLink(record.file_id));
            }
        }
        let mut reachable = HashSet::new();
        let mut visited_directories = HashSet::new();
        let mut pending = vec![root];
        while let Some(file_id) = pending.pop() {
            if !visited_directories.insert(file_id) {
                return Err(ClosureError::DirectoryHardLink(file_id));
            }
            reachable.insert(file_id);
            let entries = directories
                .get(&file_id)
                .ok_or(ClosureError::MissingDirectoryTree(file_id))?;
            for entry in entries {
                reachable.insert(entry.file_id);
                if directories.contains_key(&entry.file_id) {
                    pending.push(entry.file_id);
                }
            }
        }
        if reachable.len() != records.len() {
            return Err(ClosureError::UnreachableFileRecords);
        }
        Ok(())
    }

    fn require_kind(object: ObjectId, expected: ObjectKind) -> Result<(), ClosureError> {
        if object.kind == expected {
            Ok(())
        } else {
            Err(ClosureError::WrongObjectKind)
        }
    }
}

fn validate_file_bounds(
    records: &[FileRecord],
    lower: Option<FileId>,
    upper: Option<FileId>,
) -> Result<(), ClosureError> {
    if lower.is_some_and(|value| records.first().map(|record| record.file_id) != Some(value))
        || upper.is_some_and(|value| records.last().is_some_and(|record| record.file_id >= value))
    {
        return Err(ClosureError::RoutingBoundsMismatch);
    }
    Ok(())
}

fn validate_file_child_bounds(
    children: &[super::FileTableChild],
    lower: Option<FileId>,
    upper: Option<FileId>,
) -> Result<(), ClosureError> {
    if lower.is_some_and(|value| children.first().map(|child| child.first_file_id) != Some(value))
        || upper.is_some_and(|value| {
            children
                .last()
                .is_some_and(|child| child.first_file_id >= value)
        })
    {
        return Err(ClosureError::RoutingBoundsMismatch);
    }
    Ok(())
}

fn validate_tree_bounds(
    entries: &[TreeEntry],
    lower: Option<&LogicalName>,
    upper: Option<&LogicalName>,
) -> Result<(), ClosureError> {
    if lower.is_some_and(|value| entries.first().map(|entry| &entry.name) != Some(value))
        || upper.is_some_and(|value| entries.last().is_some_and(|entry| entry.name >= *value))
    {
        return Err(ClosureError::RoutingBoundsMismatch);
    }
    Ok(())
}

fn validate_tree_child_bounds(
    children: &[super::TreeChild],
    lower: Option<&LogicalName>,
    upper: Option<&LogicalName>,
) -> Result<(), ClosureError> {
    if lower.is_some_and(|value| children.first().map(|child| &child.first_name) != Some(value))
        || upper.is_some_and(|value| {
            children
                .last()
                .is_some_and(|child| child.first_name >= *value)
        })
    {
        return Err(ClosureError::RoutingBoundsMismatch);
    }
    Ok(())
}

fn validate_extent_bounds(
    extents: &[super::Extent],
    first: u64,
    end: u64,
) -> Result<(), ClosureError> {
    if first == end && extents.is_empty() {
        return Ok(());
    }
    if extents.first().map(|extent| extent.offset) != Some(first)
        || extents
            .last()
            .and_then(|extent| extent.offset.checked_add(extent.length))
            != Some(end)
    {
        return Err(ClosureError::RoutingBoundsMismatch);
    }
    Ok(())
}

fn validate_attribute_bounds(
    entries: &[super::AttributeEntry],
    lower: Option<&AttributeName>,
    upper: Option<&AttributeName>,
) -> Result<(), ClosureError> {
    if lower.is_some_and(|value| entries.first().map(|entry| &entry.name) != Some(value))
        || upper.is_some_and(|value| entries.last().is_some_and(|entry| entry.name >= *value))
    {
        return Err(ClosureError::RoutingBoundsMismatch);
    }
    Ok(())
}

fn validate_attribute_child_bounds(
    children: &[super::AttributeChild],
    lower: Option<&AttributeName>,
    upper: Option<&AttributeName>,
) -> Result<(), ClosureError> {
    if lower.is_some_and(|value| children.first().map(|child| &child.first_name) != Some(value))
        || upper.is_some_and(|value| {
            children
                .last()
                .is_some_and(|child| child.first_name >= *value)
        })
    {
        return Err(ClosureError::RoutingBoundsMismatch);
    }
    Ok(())
}

/// Whole-generation proof failures.
#[derive(Debug, Error)]
pub enum ClosureError {
    /// Typed object reference has the wrong class.
    #[error("generation closure object has the wrong kind")]
    WrongObjectKind,
    /// Same object was encountered twice where the graph requires a tree.
    #[error("routing graph contains a cycle or alias")]
    RoutingAlias,
    /// Parent lower/upper bounds do not match authenticated child content.
    #[error("routing bounds do not match authenticated child content")]
    RoutingBoundsMismatch,
    /// Closure exceeds its object-count bound.
    #[error("generation closure has {observed} objects; maximum is {maximum}")]
    TooManyObjects {
        /// Distinct objects encountered.
        observed: u64,
        /// Admitted maximum.
        maximum: u64,
    },
    /// Closure exceeds its file-count bound.
    #[error("generation closure has {observed} files; maximum is {maximum}")]
    TooManyFiles {
        /// File records encountered.
        observed: u64,
        /// Admitted maximum.
        maximum: u64,
    },
    /// Closure exceeds its cumulative canonical-byte bound.
    #[error("generation closure read {observed} bytes; maximum is {maximum}")]
    ClosureBytesExceeded {
        /// Cumulative canonical bytes read.
        observed: u64,
        /// Admitted maximum.
        maximum: u64,
    },
    /// File table contains the same stable identity twice.
    #[error("duplicate file record {0:?}")]
    DuplicateFileRecord(FileId),
    /// Root file record is absent.
    #[error("root file record is missing")]
    MissingRootFile,
    /// Root file is not a directory.
    #[error("root file record is not a directory")]
    RootIsNotDirectory,
    /// A record or extent exceeds the volume's declared semantic capabilities.
    #[error("generation closure uses semantics unsupported by its volume configuration")]
    UnsupportedVolumeSemantics,
    /// Namespace references an absent file record.
    #[error("namespace references missing file record {0:?}")]
    MissingFileRecord(FileId),
    /// Namespace entry kind differs from its file record.
    #[error("namespace kind differs from file record {0:?}")]
    NamespaceKindMismatch(FileId),
    /// Recorded hard-link count differs from exact namespace references.
    #[error("file link count is incorrect for {0:?}")]
    LinkCountMismatch(FileId),
    /// Directories cannot have hard links.
    #[error("directory has multiple namespace links {0:?}")]
    DirectoryHardLink(FileId),
    /// Directory record has no proven namespace tree.
    #[error("directory record has no namespace tree {0:?}")]
    MissingDirectoryTree(FileId),
    /// File table contains records unreachable from the root.
    #[error("file table contains unreachable records")]
    UnreachableFileRecords,
    /// Blob logical length differs from its declared use.
    #[error("blob logical length does not match its declaration")]
    BlobLengthMismatch,
    /// Extent references bytes outside its blob.
    #[error("extent content span exceeds its blob")]
    BlobSpanOutsideObject,
    /// Offset/count arithmetic overflowed.
    #[error("generation closure range arithmetic overflowed")]
    RangeOverflow,
    /// Immutable object backend failed.
    #[error(transparent)]
    Storage(#[from] ObjectStoreError),
    /// Canonical object failed decoding or semantic validation.
    #[error(transparent)]
    Decode(#[from] super::CanonicalDecodeError),
    /// Exact work exceeded or overflowed its budget.
    #[error(transparent)]
    Work(#[from] WorkError),
}

#[cfg(all(test, feature = "memory"))]
#[path = "tests/closure.rs"]
mod tests;
