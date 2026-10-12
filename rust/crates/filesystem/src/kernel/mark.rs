//! Marks every object a set of roots reaches, for a collection.

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

pub(crate) struct Marker<'a, S> {
    store: &'a S,
    limits: DecodeLimits,
    cancellation: &'a CancellationToken,
    marked: HashSet<ObjectId>,
    /// Generations whose whole closure is marked, with their parents.
    generations: std::collections::HashMap<GenerationId, Vec<GenerationId>>,
}

/// A marking failure: an object a root reaches is missing or undecodable, so nothing may be swept.
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

    pub(crate) fn set_limits(&mut self, limits: DecodeLimits) {
        self.limits = limits;
    }

    #[cfg(feature = "s3-http")]
    pub(crate) fn keep(&mut self, objects: impl IntoIterator<Item = ObjectId>) {
        self.marked.extend(objects);
    }

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

    pub(crate) const fn marked(&self) -> &HashSet<ObjectId> {
        &self.marked
    }

    async fn visit(&mut self, object: ObjectId) -> Result<Option<acyclic_stream::OwnedBytes>, MarkError> {
        if !self.marked.insert(object) {
            return Ok(None);
        }
        self.cancellation
            .check()
            .map_err(|_| MarkError::Storage(ObjectStoreError::Cancelled))?;
        let read = {
            let __object_args = (
                object,
                self.limits.maximum_object_bytes,
                WorkBudget::UNBOUNDED,
                self.cancellation,
            );
            let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
            let __object_result = self
                .store
                .read(__object_args.0, __object_args.1, &mut __object_work)
                .await;
            __object_work.receipt(__object_result)
        }
        .map_err(|failure| MarkError::Storage(failure.error))?;
        Ok(Some(read.value.bytes))
    }

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

    pub(crate) async fn generation_root(
        &mut self,
        generation: GenerationId,
    ) -> Result<GenerationRoot, MarkError> {
        let object = ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: generation.digest(),
        };
        self.marked.insert(object);
        let read = {
            let __object_args = (
                object,
                self.limits.maximum_object_bytes,
                WorkBudget::UNBOUNDED,
                self.cancellation,
            );
            let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
            let __object_result = self
                .store
                .read(__object_args.0, __object_args.1, &mut __object_work)
                .await;
            __object_work.receipt(__object_result)
        }
        .map_err(|failure| MarkError::Storage(failure.error))?;
        decode(decode_generation_root(&read.value.bytes, self.limits, |_| Ok::<_, crate::kernel::CanonicalDecodeError>(())))
    }

    pub(crate) async fn working(&mut self, root: &GenerationRoot) -> Result<(), MarkError> {
        let mut pending = vec![root.file_table];
        while let Some(page) = pending.pop() {
            let Some(bytes) = self.visit(page).await? else {
                continue;
            };
            match decode(decode_file_table_page(&bytes, self.limits, &mut |_| {
                Ok::<_, crate::kernel::CanonicalDecodeError>(())
            }))? {
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
        let metadata = decode(decode_file_metadata(&bytes, self.limits, &mut |_| {
            Ok::<_, crate::kernel::CanonicalDecodeError>(())
        }))?;
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
            if let TreePage::Internal(children) = decode(decode_tree_page(&bytes, self.limits, &mut |_| {
                    Ok::<_, crate::kernel::CanonicalDecodeError>(())
                }))? {
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
            match decode(decode_extent_page(&bytes, self.limits, &mut |_| {
                Ok::<_, crate::kernel::CanonicalDecodeError>(())
            }))? {
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
            match decode(decode_blob_page(&bytes, self.limits, &mut |_| {
                Ok::<_, crate::kernel::CanonicalDecodeError>(())
            }))?.node {
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
            match decode(decode_attribute_page(&bytes, self.limits, &mut |_| {
                Ok::<_, crate::kernel::CanonicalDecodeError>(())
            }))? {
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
