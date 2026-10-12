//! Bounded whole-generation authentication before authority publication.

use crate::kernel::Child;
use super::{
    AttributeName, AttributePage, BlobNode, DecodeLimits, ExtentKind, ExtentPage, FilePayload,
    FileRecord, FileTablePage, GenerationRoot, LogicalName, MetadataField, TreeEntry, TreePage,
    decode_file_metadata,
};
use crate::foundation::{FileId, GenerationId, usize_to_u64};
use crate::model::FilesystemProfile;
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use crate::storage::{ObjectId, ObjectKind, ObjectStoreError};
use crate::CancellationToken;
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
    /// Logical regular-file bytes in this generation, counting each file identity once even when it has multiple hard links. Sparse holes count.
    pub logical_file_bytes: u128,
    /// Stable sorted identities in the complete authenticated closure.
    pub objects: Vec<ObjectId>,
    /// Exact proof work.
    pub work: WorkCounters,
}

/// Generation-closure failure retaining all work spent before rejection.
pub type GenerationProofFailure = OperationFailure<ClosureError>;

/// Proves the complete immutable object closure and namespace/file-table consistency.
pub async fn prove_generation_closure<S: crate::AsyncObjectStore>(
    store: &S,
    generation_root: ObjectId,
    limits: ClosureLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<GenerationProof, GenerationProofFailure> {
    let (proof, guard) = prove_generation_closure_owned_async(
        store, generation_root, limits, budget, cancellation,
    ).await?;
    drop(guard);
    Ok(proof)
}

pub(crate) async fn prove_generation_closure_owned_async<S: crate::AsyncObjectStore>(
    store: &S,
    generation_root: ObjectId,
    limits: ClosureLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<(GenerationProof, ProofGuard), GenerationProofFailure> {
    let meter = ProofMeter::standalone(budget, cancellation)
        .map_err(|error| OperationFailure::new(closure_error(error), WorkCounters::default()))?;
    let mut context = ProofContext::new(store, limits, meter, cancellation);
    context.prove(generation_root).await
        .map_err(|error| OperationFailure::new(closure_error(error), context.meter.work()))
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn prove_generation_closure_with_native_work<S: crate::AsyncObjectStore>(
    store: &S,
    generation_root: ObjectId,
    limits: ClosureLimits,
    original: &crate::NativeMaterializationWork,
    cancellation: &CancellationToken,
) -> Result<(GenerationProof, crate::NativeAllocation), crate::FsError> {
    let meter = ProofMeter::native(original, cancellation);
    let mut context = ProofContext::new(store, limits, meter, cancellation);
    let mut owned = context.prove(generation_root).await?;
    let allocation = owned.1.take_native()?;
    Ok((owned.0, allocation))
}

#[cfg(feature = "s3-http")]
pub(crate) async fn prove_blob_closure_async<S: crate::AsyncObjectStore>(
    store: &S,
    root: ObjectId,
    expected_bytes: u64,
    limits: ClosureLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<(Vec<ObjectId>, WorkCounters), GenerationProofFailure> {
    let meter = ProofMeter::standalone(budget, cancellation)
        .map_err(|error| OperationFailure::new(closure_error(error), WorkCounters::default()))?;
    let mut context = ProofContext::new(store, limits, meter, cancellation);
    let logical_bytes = context.prove_blob(root).await
        .map_err(|error| OperationFailure::new(closure_error(error), context.meter.work()))?;
    if logical_bytes != expected_bytes {
        return Err(OperationFailure::new(
            ClosureError::BlobLengthMismatch, context.meter.work(),
        ));
    }
    let (objects, guard) = context.take_objects()
        .map_err(|error| OperationFailure::new(closure_error(error), context.meter.work()))?;
    drop(guard);
    Ok((objects, context.meter.work()))
}

pub(crate) async fn prove_record_closure_async<S: crate::AsyncObjectStore>(
    store: &S,
    records: &[FileRecord],
    limits: ClosureLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<(Vec<ObjectId>, WorkCounters), GenerationProofFailure> {
    let meter = ProofMeter::standalone(budget, cancellation)
        .map_err(|error| OperationFailure::new(closure_error(error), WorkCounters::default()))?;
    let mut context = ProofContext::new(store, limits, meter, cancellation);
    let (objects, guard) = context.prove_record_closure(records).await
        .map_err(|error| OperationFailure::new(closure_error(error), context.meter.work()))?;
    drop(guard);
    Ok((objects, context.meter.work()))
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn prove_record_closure_with_native_work<S: crate::AsyncObjectStore>(
    store: &S,
    records: &[FileRecord],
    limits: ClosureLimits,
    original: &crate::NativeMaterializationWork,
    cancellation: &CancellationToken,
) -> Result<(Vec<ObjectId>, crate::NativeAllocation), crate::FsError> {
    let meter = ProofMeter::native(original, cancellation);
    let mut context = ProofContext::new(store, limits, meter, cancellation);
    let mut owned = context.prove_record_closure(records).await?;
    let allocation = owned.1.take_native()?;
    Ok((owned.0, allocation))
}

struct ProofContext<'a, S> {
    store: &'a S,
    limits: ClosureLimits,
    objects: ProofVec<'a, ObjectId>,
    // ObjectId includes its kind: complete blob and extent proofs can share
    // one invocation-owned length table without a second allocation domain.
    proven_lengths: ProofVec<'a, (ObjectId, u64)>,
    meter: ProofMeter<'a>,
    object_bytes: u64,
    cancellation: &'a CancellationToken,
}

impl<'a, S: crate::AsyncObjectStore> ProofContext<'a, S> {
    fn new(
        store: &'a S,
        limits: ClosureLimits,
        meter: ProofMeter<'a>,
        cancellation: &'a CancellationToken,
    ) -> Self {
        Self {
            store,
            limits,
            objects: ProofVec::new(&meter),
            proven_lengths: ProofVec::new(&meter),
            object_bytes: 0,
            meter,
            cancellation,
        }
    }

    async fn prove_record_closure(
        &mut self,
        records: &[FileRecord],
    ) -> Result<(Vec<ObjectId>, ProofGuard), crate::FsError> {
        for record in records {
            self.meter.charge(WorkCounters::UNCHARGED)?;
            self.prove_record(record).await?;
        }
        self.take_objects()
    }

    fn examine(&self, count: usize) -> Result<(), crate::FsError> {
        self.meter.charge(WorkCounters {
            items_examined: usize_to_u64(count),
            ..WorkCounters::default()
        })
    }

    fn take_objects(&mut self) -> Result<(Vec<ObjectId>, ProofGuard), crate::FsError> {
        let objects = std::mem::replace(&mut self.objects, ProofVec::new(&self.meter));
        objects.into_parts()
    }

    async fn prove(
        &mut self,
        generation_root: ObjectId,
    ) -> Result<(GenerationProof, ProofGuard), crate::FsError> {
        Self::require_kind(generation_root, ObjectKind::GenerationRoot)?;
        let root_bytes = self.read_object(generation_root, false).await?;
        let root = self.meter.decode_root(&root_bytes.0, self.limits.decode)?;
        drop(root_bytes);
        let generation_id = GenerationId::new(generation_root.digest);
        let records = self.prove_file_table(root.0.file_table).await?;
        let file_count = usize_to_u64(records.len());
        if file_count > self.limits.maximum_files {
            return Err(ClosureError::TooManyFiles {
                observed: file_count, maximum: self.limits.maximum_files,
            }.into());
        }
        let mut logical_file_bytes = 0_u128;
        for record in records.iter().map(|(_, value)| value) {
            self.examine(1)?;
            let bytes = match record.payload {
                FilePayload::InlineRegular(data) => data.as_bytes().len() as u128,
                FilePayload::Regular { logical_bytes, .. } => u128::from(logical_bytes),
                _ => 0,
            };
            logical_file_bytes = logical_file_bytes.checked_add(bytes)
                .ok_or(ClosureError::LogicalBytesOverflow)?;
        }
        self.prove_records_and_namespace(root.0.root_file_id, &records).await?;
        drop(records);
        let object_count = usize_to_u64(self.objects.len());
        let objects = self.take_objects()?;
        let mut root = root;
        let mut output = (GenerationProof {
            root: root.0, generation_id, object_count, file_count, logical_file_bytes,
            objects: objects.0, work: self.meter.work(),
        }, objects.1);
        output.1.absorb(&mut root.1)?;
        Ok(output)
    }

    async fn read_object(
        &mut self,
        object: ObjectId,
        page: bool,
    ) -> Result<(crate::storage::ObjectRead, ProofGuard), crate::FsError> {
        let count = usize_to_u64(self.objects.len())
            .checked_add(u64::from(self.objects.values.binary_search(&object).is_err()))
            .ok_or(WorkError::Overflow)?;
        if count > self.limits.maximum_objects {
            return Err(ClosureError::TooManyObjects {
                observed: count, maximum: self.limits.maximum_objects,
            }.into());
        }
        set_insert(&mut self.objects, object)?;
        self.meter.charge(WorkCounters {
            page_reads: u64::from(page), ..WorkCounters::default()
        })?;
        let read = self.meter.read(
            self.store, object, self.limits.decode.maximum_object_bytes, self.cancellation,
        ).await?;
        self.object_bytes = self.object_bytes.checked_add(usize_to_u64(read.0.bytes.len()))
            .ok_or(WorkError::Overflow)?;
        let observed = self.object_bytes;
        if observed > self.limits.maximum_object_bytes {
            return Err(ClosureError::ClosureBytesExceeded {
                observed, maximum: self.limits.maximum_object_bytes,
            }.into());
        }
        Ok(read)
    }

    async fn prove_file_table(
        &mut self,
        root: ObjectId,
    ) -> Result<ProofVec<'a, (FileId, FileRecord)>, crate::FsError> {
        Self::require_kind(root, ObjectKind::FileTablePage)?;
        let mut records = ProofVec::new(&self.meter);
        let mut pending = ProofVec::new(&self.meter);
        pending.push((root, None, None))?;
        let mut pages = ProofVec::new(&self.meter);
        while let Some((page_id, lower, upper)) = pending.pop() {
            self.examine(1)?;
            if !set_insert(&mut pages, page_id)? {
                return Err(ClosureError::RoutingAlias.into());
            }
            let bytes = self.read_object(page_id, true).await?;
            let page = self.meter.decode_file_table(&bytes.0, self.limits.decode)?;
            drop(bytes);
            match &page.0 {
                FileTablePage::Leaf(entries) => {
                    validate_file_bounds(entries, lower, upper)?;
                    for record in entries {
                        self.examine(1)?;
                        if map_insert(&mut records, record.file_id, *record)?.is_some() {
                            return Err(ClosureError::DuplicateFileRecord(record.file_id).into());
                        }
                    }
                }
                FileTablePage::Internal(children) => {
                    validate_file_child_bounds(children, lower, upper)?;
                    for (index, child) in children.iter().enumerate().rev() {
                        self.examine(1)?;
                        let child_upper = children.get(index + 1)
                            .map(|next| next.first).or(upper);
                        pending.push((child.page, Some(child.first), child_upper))?;
                    }
                }
            }
        }
        Ok(records)
    }

    async fn prove_records_and_namespace(
        &mut self,
        root_file_id: FileId,
        records: &ProofVec<'a, (FileId, FileRecord)>,
    ) -> Result<(), crate::FsError> {
        let root = map_get(&records, &root_file_id).ok_or(ClosureError::MissingRootFile)?;
        if !matches!(root.payload, FilePayload::Directory { .. }) {
            return Err(ClosureError::RootIsNotDirectory.into());
        }
        let mut directory_entries = ProofVec::new(&self.meter);
        for record in records.iter().map(|(_, value)| value) {
            self.examine(1)?;
            if let Some(entries) = self.prove_record(record).await? {
                map_insert(&mut directory_entries, record.file_id, entries)?;
            }
        }
        self.validate_namespace(root_file_id, records, &directory_entries)
    }

    async fn prove_record(
        &mut self,
        record: &FileRecord,
    ) -> Result<Option<ProofVec<'a, (TreePage, ProofGuard)>>, crate::FsError> {
        if !record.kind.is_supported_by_profile(self.limits.profile)
            || (record.kind == super::FileKind::SymbolicLink && !self.limits.symbolic_links)
            || (record.link_count > 1 && !self.limits.hard_links)
        {
            return Err(ClosureError::UnsupportedVolumeSemantics.into());
        }
        self.prove_metadata(record.metadata).await?;
        match record.payload {
            FilePayload::Regular { logical_bytes, extents } => {
                self.prove_extent_tree(extents, logical_bytes).await?;
            }
            FilePayload::Directory { entries } => return Ok(Some(self.prove_tree(entries).await?)),
            FilePayload::SymbolicLink { target_bytes, target }
            | FilePayload::ReparsePoint { payload_bytes: target_bytes, payload: target } => {
                if self.prove_blob(target).await? != target_bytes {
                    return Err(ClosureError::BlobLengthMismatch.into());
                }
            }
            FilePayload::InlineRegular(_) | FilePayload::Empty | FilePayload::Device { .. } => {}
        }
        Ok(None)
    }

    async fn prove_metadata(&mut self, metadata_id: ObjectId) -> Result<(), crate::FsError> {
        Self::require_kind(metadata_id, ObjectKind::Metadata)?;
        // Immutable metadata is authenticated once even when many records share it.
        if self.objects.values.binary_search(&metadata_id).is_ok() {
            return Ok(());
        }
        let bytes = self.read_object(metadata_id, false).await?;
        self.examine(1)?;
        let metadata = decode_file_metadata(&bytes.0.bytes, self.limits.decode, &mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
        drop(bytes);
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

    async fn prove_tree(
        &mut self,
        root: ObjectId,
    ) -> Result<ProofVec<'a, (TreePage, ProofGuard)>, crate::FsError> {
        Self::require_kind(root, ObjectKind::TreePage)?;
        let mut leaves = ProofVec::new(&self.meter);
        let mut pending: ProofVec<'a, (
            ObjectId, Option<(LogicalName, ProofGuard)>, Option<(LogicalName, ProofGuard)>,
        )> = ProofVec::new(&self.meter);
        pending.push((root, None, None))?;
        let mut pages = ProofVec::new(&self.meter);
        while let Some((page_id, lower, upper)) = pending.pop() {
            self.examine(1)?;
            if !set_insert(&mut pages, page_id)? {
                return Err(ClosureError::RoutingAlias.into());
            }
            let bytes = self.read_object(page_id, true).await?;
            let page = self.meter.decode_tree(&bytes.0, self.limits.decode)?;
            drop(bytes);
            match &page.0 {
                TreePage::Leaf(values) => {
                    validate_tree_bounds(values, lower.as_ref().map(|key| &key.0), upper.as_ref().map(|key| &key.0))?;
                    leaves.push(page)?;
                }
                TreePage::Internal(children) => {
                    validate_tree_child_bounds(children, lower.as_ref().map(|key| &key.0), upper.as_ref().map(|key| &key.0))?;
                    for (index, child) in children.iter().enumerate().rev() {
                        self.examine(1)?;
                        let child_lower = self.meter.clone_key::<super::tree_mutation::TreeFormat>(
                            &child.first, self.limits.decode.maximum_name_bytes,
                        )?;
                        let upper_key = children.get(index + 1)
                            .map(|next| &next.first).or(upper.as_ref().map(|key| &key.0));
                        let child_upper = upper_key.map(|key| {
                            self.meter.clone_key::<super::tree_mutation::TreeFormat>(
                                key, self.limits.decode.maximum_name_bytes,
                            )
                        }).transpose()?;
                        pending.push((child.page, Some(child_lower), child_upper))?;
                    }
                }
            }
        }
        Ok(leaves)
    }

    async fn prove_extent_tree(&mut self, root: ObjectId, size: u64) -> Result<(), crate::FsError> {
        Self::require_kind(root, ObjectKind::ExtentPage)?;
        // Reuse only a complete proof in this invocation. The declared length is
        // part of the proof: a shared root cannot authenticate a different span.
        if let Some(length) = map_get(&self.proven_lengths, &root) {
            return if *length == size {
                Ok(())
            } else {
                Err(ClosureError::RoutingBoundsMismatch.into())
            };
        }
        let mut pending = ProofVec::new(&self.meter);
        pending.push((root, 0_u64, size))?;
        let mut pages = ProofVec::new(&self.meter);
        while let Some((page_id, first, end)) = pending.pop() {
            self.examine(1)?;
            if !set_insert(&mut pages, page_id)? {
                return Err(ClosureError::RoutingAlias.into());
            }
            let bytes = self.read_object(page_id, true).await?;
            let page = self.meter.decode_extent(&bytes.0, self.limits.decode)?;
            drop(bytes);
            match &page.0 {
                ExtentPage::Leaf(extents) => {
                    validate_extent_bounds(extents, first, end)?;
                    for extent in extents {
                        self.examine(1)?;
                        if !self.limits.sparse_files
                            && !matches!(extent.kind, ExtentKind::Content { .. })
                        {
                            return Err(ClosureError::UnsupportedVolumeSemantics.into());
                        }
                        if let ExtentKind::Content { object, object_offset } = extent.kind {
                            let content_end = object_offset.checked_add(extent.length)
                                .ok_or(ClosureError::RangeOverflow)?;
                            if content_end > self.prove_blob(object).await? {
                                return Err(ClosureError::BlobSpanOutsideObject.into());
                            }
                        }
                    }
                }
                ExtentPage::Internal(children) => {
                    if children.first().map(|child| child.first_offset) != Some(first)
                        || children.last().map(|child| child.end_offset) != Some(end)
                    {
                        return Err(ClosureError::RoutingBoundsMismatch.into());
                    }
                    for child in children.iter().rev() {
                        self.examine(1)?;
                        pending.push((child.page, child.first_offset, child.end_offset))?;
                    }
                }
            }
        }
        map_insert(&mut self.proven_lengths, root, size)?;
        Ok(())
    }

    async fn prove_blob(&mut self, root: ObjectId) -> Result<u64, crate::FsError> {
        Self::require_kind(root, ObjectKind::Blob)?;
        if let Some(length) = map_get(&self.proven_lengths, &root) {
            return Ok(*length);
        }
        let mut pending = ProofVec::new(&self.meter);
        pending.push((root, 0_u64, None))?;
        let mut pages = ProofVec::new(&self.meter);
        let mut root_length = None;
        while let Some((page_id, first, end)) = pending.pop() {
            self.examine(1)?;
            if !set_insert(&mut pages, page_id)? {
                return Err(ClosureError::RoutingAlias.into());
            }
            let bytes = self.read_object(page_id, true).await?;
            let page = self.meter.decode_blob(&bytes.0, self.limits.decode)?;
            drop(bytes);
            if page.0.first_offset != first || end.is_some_and(|value| page.0.end_offset != value) {
                return Err(ClosureError::RoutingBoundsMismatch.into());
            }
            if root_length.is_none() {
                root_length = Some(page.0.end_offset);
            }
            match &page.0.node {
                BlobNode::Internal(children) => {
                    for child in children.iter().rev() {
                        self.examine(1)?;
                        pending.push((child.page, child.first_offset, Some(child.end_offset)))?;
                    }
                }
                BlobNode::Leaf(chunks) => {
                    for chunk in chunks {
                        self.examine(1)?;
                        let expected = chunk.end_offset - chunk.first_offset;
                        Self::require_kind(chunk.chunk, ObjectKind::BlobChunk)?;
                        let value = self.read_object(chunk.chunk, false).await?;
                        if usize_to_u64(value.0.bytes.len()) != expected {
                            return Err(ClosureError::BlobLengthMismatch.into());
                        }
                    }
                }
            }
        }
        let length = root_length.ok_or(ClosureError::RoutingBoundsMismatch)?;
        map_insert(&mut self.proven_lengths, root, length)?;
        Ok(length)
    }

    async fn prove_attributes(&mut self, root: ObjectId) -> Result<(), crate::FsError> {
        Self::require_kind(root, ObjectKind::AttributePage)?;
        let mut pending: ProofVec<'a, (
            ObjectId, Option<(AttributeName, ProofGuard)>, Option<(AttributeName, ProofGuard)>,
        )> = ProofVec::new(&self.meter);
        pending.push((root, None, None))?;
        let mut pages = ProofVec::new(&self.meter);
        while let Some((page_id, lower, upper)) = pending.pop() {
            self.examine(1)?;
            if !set_insert(&mut pages, page_id)? {
                return Err(ClosureError::RoutingAlias.into());
            }
            let bytes = self.read_object(page_id, true).await?;
            let page = self.meter.decode_attributes(&bytes.0, self.limits.decode)?;
            drop(bytes);
            match &page.0 {
                AttributePage::Leaf(entries) => {
                    validate_attribute_bounds(entries, lower.as_ref().map(|key| &key.0), upper.as_ref().map(|key| &key.0))?;
                    for entry in entries {
                        self.examine(1)?;
                        if self.prove_blob(entry.value).await? != entry.value_bytes {
                            return Err(ClosureError::BlobLengthMismatch.into());
                        }
                    }
                }
                AttributePage::Internal(children) => {
                    validate_attribute_child_bounds(children, lower.as_ref().map(|key| &key.0), upper.as_ref().map(|key| &key.0))?;
                    for (index, child) in children.iter().enumerate().rev() {
                        self.examine(1)?;
                        let child_lower = self.meter.clone_key::<super::attribute_mutation::AttributeFormat>(
                            &child.first, self.limits.decode.maximum_name_bytes,
                        )?;
                        let upper_key = children.get(index + 1)
                            .map(|next| &next.first).or(upper.as_ref().map(|key| &key.0));
                        let child_upper = upper_key.map(|key| {
                            self.meter.clone_key::<super::attribute_mutation::AttributeFormat>(
                                key, self.limits.decode.maximum_name_bytes,
                            )
                        }).transpose()?;
                        pending.push((child.page, Some(child_lower), child_upper))?;
                    }
                }
            }
        }
        Ok(())
    }

    fn tree_entries<'b>(
        pages: &'b ProofVec<'a, (TreePage, ProofGuard)>,
    ) -> impl Iterator<Item = &'b TreeEntry> + 'b {
        pages.iter().flat_map(|page| match &page.0 {
            TreePage::Leaf(entries) => entries.as_slice(),
            TreePage::Internal(_) => &[],
        })
    }

    fn validate_namespace(
        &self,
        root: FileId,
        records: &ProofVec<'a, (FileId, FileRecord)>,
        directories: &ProofVec<'a, (FileId, ProofVec<'a, (TreePage, ProofGuard)>)>,
    ) -> Result<(), crate::FsError> {
        let mut links = ProofVec::new(&self.meter);
        map_insert(&mut links, root, 1_u64)?;
        for pages in directories.iter().map(|(_, value)| value) {
            for entry in Self::tree_entries(pages) {
                self.examine(1)?;
                let record = map_get(&records, &entry.file_id)
                    .ok_or(ClosureError::MissingFileRecord(entry.file_id))?;
                if record.kind != entry.kind {
                    return Err(ClosureError::NamespaceKindMismatch(entry.file_id).into());
                }
                let count = map_get(&links, &entry.file_id).copied().unwrap_or(0_u64)
                    .checked_add(1).ok_or(ClosureError::RangeOverflow)?;
                map_insert(&mut links, entry.file_id, count)?;
            }
        }
        for record in records.iter().map(|(_, value)| value) {
            self.examine(1)?;
            if map_get(&links, &record.file_id).copied().unwrap_or(0) != record.link_count {
                return Err(ClosureError::LinkCountMismatch(record.file_id).into());
            }
            if matches!(record.payload, FilePayload::Directory { .. }) && record.link_count != 1 {
                return Err(ClosureError::DirectoryHardLink(record.file_id).into());
            }
        }
        let mut reachable = ProofVec::new(&self.meter);
        let mut visited_directories = ProofVec::new(&self.meter);
        let mut pending = ProofVec::new(&self.meter);
        pending.push(root)?;
        while let Some(file_id) = pending.pop() {
            self.examine(1)?;
            if !set_insert(&mut visited_directories, file_id)? {
                return Err(ClosureError::DirectoryHardLink(file_id).into());
            }
            set_insert(&mut reachable, file_id)?;
            let pages = map_get(&directories, &file_id)
                .ok_or(ClosureError::MissingDirectoryTree(file_id))?;
            for entry in Self::tree_entries(pages) {
                self.examine(1)?;
                set_insert(&mut reachable, entry.file_id)?;
                if map_get(&directories, &entry.file_id).is_some() {
                    pending.push(entry.file_id)?;
                }
            }
        }
        if reachable.len() != records.len() {
            return Err(ClosureError::UnreachableFileRecords.into());
        }
        Ok(())
    }

    fn require_kind(object: ObjectId, expected: ObjectKind) -> Result<(), crate::FsError> {
        if object.kind == expected {
            Ok(())
        } else {
            Err(ClosureError::WrongObjectKind.into())
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
    children: &[Child<FileId>],
    lower: Option<FileId>,
    upper: Option<FileId>,
) -> Result<(), ClosureError> {
    if lower.is_some_and(|value| children.first().map(|child| child.first) != Some(value))
        || upper.is_some_and(|value| {
            children
                .last()
                .is_some_and(|child| child.first >= value)
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
    children: &[Child<LogicalName>],
    lower: Option<&LogicalName>,
    upper: Option<&LogicalName>,
) -> Result<(), ClosureError> {
    if lower.is_some_and(|value| children.first().map(|child| &child.first) != Some(value))
        || upper.is_some_and(|value| {
            children
                .last()
                .is_some_and(|child| child.first >= *value)
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
    children: &[Child<AttributeName>],
    lower: Option<&AttributeName>,
    upper: Option<&AttributeName>,
) -> Result<(), ClosureError> {
    if lower.is_some_and(|value| children.first().map(|child| &child.first) != Some(value))
        || upper.is_some_and(|value| {
            children
                .last()
                .is_some_and(|child| child.first >= *value)
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
    /// The sum of logical regular-file sizes cannot be represented.
    #[error("generation logical file byte total overflowed")]
    LogicalBytesOverflow,
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
    /// A native original accounting leaf failed with its original typed cause.
    #[cfg(not(target_arch = "wasm32"))]
    #[error(transparent)]
    Native(#[from] crate::NativeTreeMaterializationError),
    /// An authenticated extent decoder retained its typed read failure.
    #[error(transparent)]
    FileRead(#[from] super::FileRangeReadError),
    /// An authenticated blob decoder retained its typed read failure.
    #[error(transparent)]
    BlobRead(#[from] super::BlobReadError),
}

fn closure_error(error: crate::FsError) -> ClosureError {
    match error {
        crate::FsError::Closure(error) => error,
        crate::FsError::Object(error) => ClosureError::Storage(error),
        crate::FsError::Decode(error) => ClosureError::Decode(error),
        crate::FsError::Work(error) => ClosureError::Work(error),
        crate::FsError::Cancelled(_) => ClosureError::Storage(ObjectStoreError::Cancelled),
        #[cfg(not(target_arch = "wasm32"))]
        crate::FsError::NativeKernel(error) => ClosureError::Native(error),
        crate::FsError::FileRead(super::FileRangeReadError::Extent(super::ExtentReadError::Decode(error))) =>
            ClosureError::Decode(error),
        crate::FsError::BlobRead(super::BlobReadError::Decode(error)) => ClosureError::Decode(error),
        crate::FsError::FileRead(error) => ClosureError::FileRead(error),
        crate::FsError::BlobRead(error) => ClosureError::BlobRead(error),
        _ => unreachable!("closure accounting emitted an unrelated filesystem error"),
    }
}

// Shared standalone state is borrowed only by synchronous accounting leaves.
// Native mode keeps the actual original meter, never a reconstructed receipt.
struct StandaloneProofState {
    allocations: super::allocation::AllocationLedger,
    work: WorkCounters,
    budget: WorkBudget,
}

fn proof_standalone_control_bytes() -> u64 {
    usize_to_u64(size_of::<std::sync::Mutex<StandaloneProofState>>() + 2 * size_of::<usize>())
}

#[derive(Clone)]
enum ProofMeterKind<'a> {
    Standalone(std::sync::Arc<std::sync::Mutex<StandaloneProofState>>, std::marker::PhantomData<&'a ()>),
    #[cfg(not(target_arch = "wasm32"))]
    Native(&'a crate::NativeMaterializationWork),
}

#[derive(Clone)]
struct ProofMeter<'a> {
    kind: ProofMeterKind<'a>,
    cancellation: &'a CancellationToken,
}

impl<'a> ProofMeter<'a> {
    fn standalone(budget: WorkBudget, cancellation: &'a CancellationToken) -> Result<Self, crate::FsError> {
        cancellation.check()?;
        let mut state = StandaloneProofState {
            allocations: super::allocation::AllocationLedger::default(),
            work: WorkCounters::UNCHARGED,
            budget,
        };
        let controls = proof_standalone_control_bytes();
        state.allocations.claim_bytes(controls, 1, &mut state.work, budget)
            .map_err(proof_allocation_error)?;
        Ok(Self {
            kind: ProofMeterKind::Standalone(
                std::sync::Arc::new(std::sync::Mutex::new(state)), std::marker::PhantomData,
            ),
            cancellation,
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native(original: &'a crate::NativeMaterializationWork, cancellation: &'a CancellationToken) -> Self {
        Self { kind: ProofMeterKind::Native(original), cancellation }
    }

    fn with_accounting<T>(
        &self,
        operation: impl FnOnce(&mut super::allocation::KernelAccounting<'_>) -> Result<T, crate::FsError>,
    ) -> Result<T, crate::FsError> {
        match &self.kind {
            ProofMeterKind::Standalone(state, _) => {
                let mut state = state.lock().map_err(|_| WorkError::Overflow)?;
                let StandaloneProofState { allocations, work, budget } = &mut *state;
                operation(&mut super::allocation::KernelAccounting::borrowed_standalone(
                    allocations, work, *budget, self.cancellation,
                ))
            }
            #[cfg(not(target_arch = "wasm32"))]
            ProofMeterKind::Native(original) => operation(
                &mut super::allocation::KernelAccounting::native(original, self.cancellation),
            ),
        }
    }

    // Producers return exact retained capacity; failed producers destroy their
    // data and release that capacity themselves, never a global ledger delta.
    // Tuple field order destroys physical data before retiring its guard.
    fn owned<T>(
        &self,
        operation: impl FnOnce(&mut super::allocation::KernelAccounting<'_>)
            -> Result<(T, super::allocation::KernelOwnedAllocation, u64), crate::FsError>,
    ) -> Result<(T, ProofGuard), crate::FsError> {
        match &self.kind {
            ProofMeterKind::Standalone(state, _) => {
                let mut state_lock = state.lock().map_err(|_| WorkError::Overflow)?;
                let StandaloneProofState { allocations, work, budget } = &mut *state_lock;
                let (value, _, bytes) = operation(&mut super::allocation::KernelAccounting::borrowed_standalone(
                    allocations, work, *budget, self.cancellation,
                ))?;
                Ok((value, ProofGuard::Standalone {
                        state: std::sync::Arc::clone(state), bytes,
                    }))
            }
            #[cfg(not(target_arch = "wasm32"))]
            ProofMeterKind::Native(original) => {
                let (value, allocation, _) = operation(
                    &mut super::allocation::KernelAccounting::native(original, self.cancellation),
                )?;
                match allocation {
                    super::allocation::KernelOwnedAllocation::NativeOriginal(guard) =>
                        Ok((value, ProofGuard::Native(guard))),
                    super::allocation::KernelOwnedAllocation::Standalone =>
                        Err(crate::NativeTreeMaterializationError::InvalidLayout.into()),
                }
            }
        }
    }

    fn empty_guard(&self) -> Result<ProofGuard, crate::FsError> {
        self.owned(|accounting| Ok(((), accounting.claim_owned(0, 0)?, 0)))
            .map(|owned| owned.1)
    }

    fn work(&self) -> WorkCounters {
        match &self.kind {
            ProofMeterKind::Standalone(state, _) => state.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner).work,
            #[cfg(not(target_arch = "wasm32"))]
            ProofMeterKind::Native(original) => original.receipt(),
        }
    }

    fn charge(&self, delta: WorkCounters) -> Result<(), crate::FsError> {
        self.with_accounting(|accounting| accounting.charge(delta))
    }

    fn record_completed(&self, delta: WorkCounters) -> Result<(), crate::FsError> {
        self.with_accounting(|accounting| accounting.record_completed(delta))
    }

    fn admit(&self, delta: WorkCounters) -> Result<(), crate::FsError> {
        self.with_accounting(|accounting| accounting.admit(delta))
    }

    fn remaining(&self) -> Result<WorkBudget, crate::FsError> {
        self.with_accounting(|accounting| accounting.remaining())
    }

    async fn read<S: crate::AsyncObjectStore>(
        &self, store: &S, object: ObjectId, maximum: u64, cancellation: &CancellationToken,
    ) -> Result<(crate::storage::ObjectRead, ProofGuard), crate::FsError> {
        self.cancellation.check()?;
        cancellation.check()?;
        #[cfg(not(target_arch = "wasm32"))]
        if let ProofMeterKind::Native(original) = &self.kind {
            let read = store.read_with_native_work(object, maximum, original, cancellation).await
                .map_err(|error| match error {
                    ObjectStoreError::Native(error) => crate::FsError::NativeKernel(error),
                    error => error.into(),
                })?;
            // The returned Bytes keeps the provider's real original guard.
            return Ok((read, self.empty_guard()?));
        }
        let result = store.read(object, maximum, self.remaining()?, cancellation).await;
        self.owned(|accounting| {
            super::file_read::accept_standalone_object_read(result, accounting)
                .map(|(read, bytes)| (read, super::allocation::KernelOwnedAllocation::Standalone, bytes))
        })
    }

    fn decode_root(
        &self, read: &crate::storage::ObjectRead, limits: DecodeLimits,
    ) -> Result<(GenerationRoot, ProofGuard), crate::FsError> {
        self.decode_steps(|observe| {
            super::generation::decode_generation_root(&read.bytes, limits, observe)
        })
    }

    fn decode_steps<T>(
        &self,
        decode: impl FnOnce(&mut dyn FnMut(super::codec::ReadAllocationStep)
            -> Result<(), crate::FsError>)
            -> Result<T, crate::FsError>,
    ) -> Result<(T, ProofGuard), crate::FsError> {
        self.owned(|accounting| {
            let mut collector = super::file_read::OriginalReadAccounting::new(accounting)?;
            match decode(&mut |step| collector.observe(step)) {
                Ok(value) => {
                    let (allocation, bytes) = collector.into_parts();
                    Ok((value, allocation, bytes))
                }
                Err(error) => {
                    collector.release_failed()?;
                    Err(error)
                }
            }
        })
    }

    fn decode_file_table(&self, read: &crate::storage::ObjectRead, limits: DecodeLimits)
        -> Result<(FileTablePage, ProofGuard), crate::FsError> {
        self.decode_steps(|observe| super::file_table::decode_file_table_page(&read.bytes, limits, &mut |step| observe(step)))
    }

    fn decode_tree(&self, read: &crate::storage::ObjectRead, limits: DecodeLimits)
        -> Result<(TreePage, ProofGuard), crate::FsError> {
        self.decode_steps(|observe| super::tree::decode_tree_page(&read.bytes, limits, &mut |step| observe(step)))
    }

    fn decode_attributes(&self, read: &crate::storage::ObjectRead, limits: DecodeLimits)
        -> Result<(AttributePage, ProofGuard), crate::FsError> {
        self.decode_steps(|observe| super::attribute::decode_attribute_page(&read.bytes, limits, &mut |step| observe(step)))
    }

    fn decode_extent(&self, read: &crate::storage::ObjectRead, limits: DecodeLimits)
        -> Result<(ExtentPage, ProofGuard), crate::FsError> {
        self.decode_steps(|observe| super::extent::decode_extent_page(
            &read.bytes, limits, &mut |step| observe(step),
        ))
    }

    fn decode_blob(&self, read: &crate::storage::ObjectRead, limits: DecodeLimits)
        -> Result<(super::BlobPage, ProofGuard), crate::FsError> {
        self.decode_steps(|observe| super::blob::decode_blob_page(
            &read.bytes, limits, &mut |step| observe(step),
        ))
    }

    fn clone_key<F: super::persistent_btree::Format>(
        &self, key: &F::Key, maximum: u32,
    ) -> Result<(F::Key, ProofGuard), crate::FsError> {
        self.decode_steps(|observe| F::try_clone_key(
            key, maximum, &mut |step| observe(step),
        ))
    }
}

fn proof_allocation_error(error: super::allocation::AllocationError) -> crate::FsError {
    error.into_work_or(crate::FsError::Decode(super::CanonicalDecodeError::AllocationFailed))
}

fn proof_capacity_bytes<T>(capacity: usize) -> Result<u64, crate::FsError> {
    capacity.checked_mul(size_of::<T>()).map(usize_to_u64)
        .ok_or_else(|| WorkError::Overflow.into())
}


pub(crate) enum ProofGuard {
    Standalone {
        state: std::sync::Arc<std::sync::Mutex<StandaloneProofState>>,
        bytes: u64,
    },
    #[cfg(not(target_arch = "wasm32"))]
    Native(crate::NativeAllocation),
}

impl ProofGuard {
    // A retained standalone result also keeps this proof's shared meter alive.
    // Count its one admitted Arc body, never the original native root state.
    pub(crate) fn retained_bytes(&self) -> u64 {
        match self {
            ProofGuard::Standalone { bytes, .. } => bytes
                .checked_add(proof_standalone_control_bytes())
                .expect("retained proof control ownership invariant"),
            #[cfg(not(target_arch = "wasm32"))]
            ProofGuard::Native(guard) => guard.bytes(),
        }
    }

    fn absorb(&mut self, incoming: &mut Self) -> Result<(), crate::FsError> {
        match (self, incoming) {
            (ProofGuard::Standalone { state, bytes }, ProofGuard::Standalone { state: source, bytes: added })
                if std::sync::Arc::ptr_eq(state, source) => {
                    *bytes = bytes.checked_add(*added).ok_or(WorkError::Overflow)?;
                    *added = 0;
                    Ok(())
                }
            #[cfg(not(target_arch = "wasm32"))]
            (ProofGuard::Native(target), ProofGuard::Native(source)) => target.absorb(source).map_err(Into::into),
            _ => Err(WorkError::Overflow.into()),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn take_native(&mut self) -> Result<crate::NativeAllocation, crate::FsError> {
        match self {
            ProofGuard::Native(guard) => guard.split_off(guard.bytes()).map_err(Into::into),
            ProofGuard::Standalone { .. } => Err(crate::NativeTreeMaterializationError::InvalidLayout.into()),
        }
    }

}

impl Drop for ProofGuard {
    fn drop(&mut self) {
        if let Self::Standalone { state, bytes } = self {
            if *bytes != 0 {
                state.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
                    .allocations.release(*bytes).expect("proof allocation ownership invariant");
            }
        }
    }
}

struct ProofVec<'a, T> {
    values: Vec<T>,
    guard: Option<ProofGuard>,
    meter: ProofMeter<'a>,
}

impl<'a, T> ProofVec<'a, T> {
    fn new(meter: &ProofMeter<'a>) -> Self {
        Self { values: Vec::new(), guard: None, meter: meter.clone() }
    }

    fn ensure_capacity(&mut self) -> Result<(), crate::FsError> {
        if self.values.len() < self.values.capacity() { return Ok(()); }
        let capacity = self.values.len().checked_add(1).ok_or(WorkError::Overflow)?
            .checked_next_power_of_two().ok_or(WorkError::Overflow)?;
        let mut replacement = self.meter.owned(|accounting| {
            let requested = proof_capacity_bytes::<T>(capacity)?;
            let mut allocation = accounting.claim_owned(requested, u64::from(requested != 0))?;
            let mut values = Vec::new();
            if let Err(error) = values.try_reserve_exact(capacity) {
                drop(values);
                accounting.release_owned(&mut allocation, requested)?;
                #[cfg(not(target_arch = "wasm32"))]
                if matches!(&self.meter.kind, ProofMeterKind::Native(_)) {
                    return Err(crate::NativeTreeMaterializationError::AllocationFailed(error).into());
                }
                let _ = error;
                return Err(super::CanonicalDecodeError::AllocationFailed.into());
            }
            let actual = proof_capacity_bytes::<T>(values.capacity())?;
            if let Err(error) = accounting.reconcile_owned(&mut allocation, requested, actual) {
                drop(values);
                accounting.release_owned(&mut allocation, actual)?;
                return Err(error);
            }
            Ok((values, allocation, actual))
        })?;
        let copied = WorkCounters {
            bytes_copied: proof_capacity_bytes::<T>(self.values.len())?,
            ..WorkCounters::UNCHARGED
        };
        self.meter.admit(copied)?;
        replacement.0.append(&mut self.values);
        self.meter.record_completed(copied)?;
        let (values, guard) = replacement;
        let old_values = std::mem::replace(&mut self.values, values);
        let old_guard = self.guard.replace(guard);
        drop(old_values);
        drop(old_guard);
        Ok(())
    }

    fn push(&mut self, value: T) -> Result<(), crate::FsError> {
        self.ensure_capacity()?;
        let copied = WorkCounters {
            bytes_copied: usize_to_u64(size_of::<T>()), ..WorkCounters::UNCHARGED
        };
        self.meter.admit(copied)?;
        self.values.push(value);
        self.meter.record_completed(copied)?;
        Ok(())
    }

    fn insert(&mut self, index: usize, value: T) -> Result<(), crate::FsError> {
        self.ensure_capacity()?;
        let moved = self.values.len().checked_sub(index).ok_or(WorkError::Overflow)?
            .checked_add(1).ok_or(WorkError::Overflow)?;
        let copied = WorkCounters {
            bytes_copied: proof_capacity_bytes::<T>(moved)?, ..WorkCounters::UNCHARGED
        };
        self.meter.admit(copied)?;
        self.values.insert(index, value);
        self.meter.record_completed(copied)?;
        Ok(())
    }

    fn pop(&mut self) -> Option<T> { self.values.pop() }
    fn len(&self) -> usize { self.values.len() }
    fn iter(&self) -> std::slice::Iter<'_, T> { self.values.iter() }
    fn into_parts(mut self) -> Result<(Vec<T>, ProofGuard), crate::FsError> {
        let guard = match self.guard.take() {
            Some(guard) => guard,
            None => self.meter.empty_guard()?,
        };
        Ok((std::mem::take(&mut self.values), guard))
    }
}

impl<T> Drop for ProofVec<'_, T> {
    fn drop(&mut self) {
        drop(std::mem::take(&mut self.values));
        drop(self.guard.take());
    }
}

#[cfg(all(test, feature = "memory"))]
#[path = "tests/closure.rs"]
mod tests;

fn map_get<'v, K: Ord, V>(entries: &'v ProofVec<'_, (K, V)>, key: &K) -> Option<&'v V> {
    entries.values.binary_search_by(|entry| entry.0.cmp(key)).ok()
        .map(|index| &entries.values[index].1)
}

fn map_insert<K: Ord, V>(entries: &mut ProofVec<'_, (K, V)>, key: K, value: V) -> Result<Option<V>, crate::FsError> {
    match entries.values.binary_search_by(|entry| entry.0.cmp(&key)) {
        Ok(index) => {
            let copied = WorkCounters { bytes_copied: usize_to_u64(size_of::<V>()), ..WorkCounters::UNCHARGED };
            entries.meter.admit(copied)?;
            let previous = std::mem::replace(&mut entries.values[index].1, value);
            entries.meter.record_completed(copied)?;
            Ok(Some(previous))
        }
        Err(index) => { entries.insert(index, (key, value))?; Ok(None) }
    }
}

fn set_insert<K: Ord>(entries: &mut ProofVec<'_, K>, key: K) -> Result<bool, crate::FsError> {
    match entries.values.binary_search(&key) {
        Ok(_) => Ok(false),
        Err(index) => { entries.insert(index, key)?; Ok(true) }
    }
}
