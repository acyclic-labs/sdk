//! Marks every object a set of roots reaches, for a collection.
//!
//! Unlike a closure proof, marking authenticates nothing beyond decoding: it
//! only has to find every reachable object, once. Generations share most of
//! their pages, so a page already marked is not read again, and content
//! chunks are marked by identity without being read.

use std::collections::HashSet;

use super::{
    AttributePage, BlobNode, DecodeLimits, ExtentKind, ExtentPage, FilePayload, FileRecord,
    FileTablePage, GenerationRoot, MetadataField, TreePage, decode_attribute_page,
    decode_blob_page, decode_extent_page, decode_file_metadata, decode_file_table_page,
    decode_generation_root, decode_tree_page,
};
use crate::CancellationToken;
use crate::foundation::GenerationId;
use crate::performance::WorkBudget;
use crate::storage::{ObjectId, ObjectKind, ObjectStoreError};

/// Accumulates the objects a collection keeps.
pub(crate) struct Marker<'a, S> {
    store: &'a S,
    limits: DecodeLimits,
    cancellation: &'a CancellationToken,
    marked: HashSet<ObjectId>,
    /// Generations whose whole closure is marked, with their parents.
    generations: std::collections::HashMap<GenerationId, Vec<GenerationId>>,
}

/// A marking failure: an object a root reaches is missing or undecodable,
/// so nothing may be swept.
#[derive(Debug, thiserror::Error)]
pub enum MarkError {
    /// Storage failed or an object is missing.
    #[error(transparent)]
    Storage(#[from] ObjectStoreError),
    /// An object does not decode.
    #[error("a reachable object does not decode: {0}")]
    Decode(String),
}

fn decode<T, E: std::fmt::Display>(result: Result<T, E>) -> Result<T, MarkError> {
    result.map_err(|error| MarkError::Decode(error.to_string()))
}

impl<'a, S: crate::AsyncObjectStore> Marker<'a, S> {
    pub(crate) fn new(store: &'a S, cancellation: &'a CancellationToken) -> Self {
        Self {
            store,
            limits: DecodeLimits::default(),
            cancellation,
            marked: HashSet::new(),
            generations: std::collections::HashMap::new(),
        }
    }

    /// Decodes what is marked next under `limits`, those of its volume.
    pub(crate) fn set_limits(&mut self, limits: DecodeLimits) {
        self.limits = limits;
    }

    /// Keeps `objects` without walking them.
    #[cfg(feature = "s3-http")]
    pub(crate) fn keep(&mut self, objects: impl IntoIterator<Item = ObjectId>) {
        self.marked.extend(objects);
    }

    /// Marks the root object of every generation a fully marked one names as
    /// an ancestor, which lineage walks read, without their closures. A root
    /// already gone ends its branch: nothing can restore it.
    pub(crate) async fn lineage_roots(&mut self) -> Result<(), MarkError> {
        let mut pending = self
            .generations
            .values()
            .flatten()
            .copied()
            .collect::<Vec<_>>();
        let mut seen = HashSet::new();
        while let Some(generation) = pending.pop() {
            if self.generations.contains_key(&generation) || !seen.insert(generation) {
                continue;
            }
            match self.generation_root(generation).await {
                Ok(root) => pending.extend(root.parents),
                Err(MarkError::Storage(ObjectStoreError::Missing)) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    /// Everything marked so far.
    pub(crate) const fn marked(&self) -> &HashSet<ObjectId> {
        &self.marked
    }

    /// Reads `object` unless it is already marked, marking it.
    async fn visit(&mut self, object: ObjectId) -> Result<Option<bytes::Bytes>, MarkError> {
        if !self.marked.insert(object) {
            return Ok(None);
        }
        self.cancellation
            .check()
            .map_err(|_| MarkError::Storage(ObjectStoreError::Cancelled))?;
        let read = self
            .store
            .read(
                object,
                self.limits.maximum_object_bytes,
                WorkBudget::UNBOUNDED,
                self.cancellation,
            )
            .await
            .map_err(|failure| MarkError::Storage(failure.error))?;
        Ok(Some(read.value.bytes))
    }

    /// Marks the complete closure of one generation; returns its parents.
    pub(crate) async fn generation(
        &mut self,
        generation: GenerationId,
    ) -> Result<Vec<GenerationId>, MarkError> {
        if let Some(parents) = self.generations.get(&generation) {
            return Ok(parents.clone());
        }
        let root = self.generation_root(generation).await?;
        self.working(&root).await?;
        self.generations.insert(generation, root.parents.clone());
        Ok(root.parents)
    }

    /// Marks only the root object of one generation, which lineage walks
    /// read; returns its parents.
    pub(crate) async fn generation_root(
        &mut self,
        generation: GenerationId,
    ) -> Result<GenerationRoot, MarkError> {
        let object = ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: generation.digest(),
        };
        self.marked.insert(object);
        let read = self
            .store
            .read(
                object,
                self.limits.maximum_object_bytes,
                WorkBudget::UNBOUNDED,
                self.cancellation,
            )
            .await
            .map_err(|failure| MarkError::Storage(failure.error))?;
        decode(decode_generation_root(&read.value.bytes, self.limits))
    }

    /// Marks everything a root reaches, whether or not it is stored, such
    /// as a checkout's working tree.
    pub(crate) async fn working(&mut self, root: &GenerationRoot) -> Result<(), MarkError> {
        let mut pending = vec![root.file_table];
        while let Some(page) = pending.pop() {
            let Some(bytes) = self.visit(page).await? else {
                continue;
            };
            match decode(decode_file_table_page(&bytes, self.limits))? {
                FileTablePage::Leaf(records) => {
                    for record in records {
                        self.record(&record).await?;
                    }
                }
                FileTablePage::Internal(children) => {
                    pending.extend(children.into_iter().map(|child| child.page));
                }
            }
        }
        Ok(())
    }

    /// Marks every object one file record reaches.
    pub(crate) async fn record(&mut self, record: &FileRecord) -> Result<(), MarkError> {
        self.metadata(record.metadata).await?;
        match record.payload {
            FilePayload::Regular { extents, .. } => self.extents(extents).await,
            FilePayload::Directory { entries } => self.tree(entries).await,
            FilePayload::SymbolicLink { target, .. }
            | FilePayload::ReparsePoint {
                payload: target, ..
            } => self.blob(target).await,
            FilePayload::InlineRegular(_) | FilePayload::Empty | FilePayload::Device { .. } => {
                Ok(())
            }
        }
    }

    async fn metadata(&mut self, metadata: ObjectId) -> Result<(), MarkError> {
        let Some(bytes) = self.visit(metadata).await? else {
            return Ok(());
        };
        let metadata = decode(decode_file_metadata(&bytes, self.limits))?;
        if let MetadataField::Value(attributes) = metadata.named_attributes {
            self.attributes(attributes).await?;
        }
        for blob in [metadata.acl, metadata.security_descriptor] {
            if let MetadataField::Value(blob) = blob {
                self.blob(blob).await?;
            }
        }
        Ok(())
    }

    async fn tree(&mut self, root: ObjectId) -> Result<(), MarkError> {
        let mut pending = vec![root];
        while let Some(page) = pending.pop() {
            let Some(bytes) = self.visit(page).await? else {
                continue;
            };
            if let TreePage::Internal(children) = decode(decode_tree_page(&bytes, self.limits))? {
                pending.extend(children.into_iter().map(|child| child.page));
            }
        }
        Ok(())
    }

    async fn extents(&mut self, root: ObjectId) -> Result<(), MarkError> {
        let mut pending = vec![root];
        while let Some(page) = pending.pop() {
            let Some(bytes) = self.visit(page).await? else {
                continue;
            };
            match decode(decode_extent_page(&bytes, self.limits))? {
                ExtentPage::Leaf(extents) => {
                    for extent in extents {
                        if let ExtentKind::Content { object, .. } = extent.kind {
                            self.blob(object).await?;
                        }
                    }
                }
                ExtentPage::Internal(children) => {
                    pending.extend(children.into_iter().map(|child| child.page));
                }
            }
        }
        Ok(())
    }

    async fn blob(&mut self, root: ObjectId) -> Result<(), MarkError> {
        let mut pending = vec![root];
        while let Some(page) = pending.pop() {
            let Some(bytes) = self.visit(page).await? else {
                continue;
            };
            match decode(decode_blob_page(&bytes, self.limits))?.node {
                BlobNode::Internal(children) => {
                    pending.extend(children.into_iter().map(|child| child.page));
                }
                BlobNode::Leaf(chunks) => {
                    self.marked
                        .extend(chunks.into_iter().map(|chunk| chunk.chunk));
                }
            }
        }
        Ok(())
    }

    async fn attributes(&mut self, root: ObjectId) -> Result<(), MarkError> {
        let mut pending = vec![root];
        while let Some(page) = pending.pop() {
            let Some(bytes) = self.visit(page).await? else {
                continue;
            };
            match decode(decode_attribute_page(&bytes, self.limits))? {
                AttributePage::Leaf(entries) => {
                    for entry in entries {
                        self.blob(entry.value).await?;
                    }
                }
                AttributePage::Internal(children) => {
                    pending.extend(children.into_iter().map(|child| child.page));
                }
            }
        }
        Ok(())
    }
}
