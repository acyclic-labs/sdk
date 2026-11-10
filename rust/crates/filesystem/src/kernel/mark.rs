//! Marks every object a set of roots reaches, for a collection.

use std::collections::HashSet;

use super::allocation::KernelAccounting;
use super::{
    DecodeLimits, FileRecord, GenerationRoot, decode_generation_root, visit_object_references,
    visit_record_references,
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

    async fn visit(
        &mut self,
        object: ObjectId,
    ) -> Result<Option<acyclic_stream::OwnedBytes>, MarkError> {
        if !self.marked.insert(object) {
            return Ok(None);
        }
        self.cancellation
            .check()
            .map_err(|_| MarkError::Storage(ObjectStoreError::Cancelled))?;
        let read = {
            let mut work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, self.cancellation);
            let result = self
                .store
                .read(object, self.limits.maximum_object_bytes, &mut work)
                .await;
            work.receipt(result)
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
        let object = generation.object_id();
        self.marked.insert(object);
        let read = {
            let mut work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, self.cancellation);
            let result = self
                .store
                .read(object, self.limits.maximum_object_bytes, &mut work)
                .await;
            work.receipt(result)
        }
        .map_err(|failure| MarkError::Storage(failure.error))?;
        decode(decode_generation_root(
            &read.value.bytes,
            self.limits,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
        ))
    }

    pub(crate) async fn working(&mut self, root: &GenerationRoot) -> Result<(), MarkError> {
        self.closure(vec![root.file_table]).await
    }

    pub(crate) async fn record(&mut self, record: &FileRecord) -> Result<(), MarkError> {
        let mut pending = Vec::new();
        visit_record_references(record, |child| {
            pending.push(child);
            Ok::<_, MarkError>(())
        })?;
        self.closure(pending).await
    }

    /// Marks everything `pending` reaches through the same immediate
    /// references the store records at write, so every child a marked object
    /// pins is itself marked. Chunks carry no references and are not read.
    async fn closure(&mut self, mut pending: Vec<ObjectId>) -> Result<(), MarkError> {
        while let Some(object) = pending.pop() {
            if object.kind == ObjectKind::BlobChunk {
                self.marked.insert(object);
                continue;
            }
            let Some(bytes) = self.visit(object).await? else {
                continue;
            };
            let mut work = KernelAccounting::standalone(WorkBudget::UNBOUNDED, self.cancellation);
            decode(visit_object_references(
                object,
                &bytes,
                self.limits,
                &mut work,
                |child| {
                    pending.push(child);
                    Ok(())
                },
            ))?;
        }
        Ok(())
    }
}
