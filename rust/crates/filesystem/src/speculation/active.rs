use super::{PromotionPlan, ResidencyCandidate};
use crate::foundation::OperationId;
use crate::storage::ObjectId;
use std::collections::{BTreeMap, BTreeSet};

pub(super) trait Entry {
    fn operation_id(&self) -> OperationId;
    fn object_id(&self) -> ObjectId;
}

impl Entry for ResidencyCandidate {
    fn operation_id(&self) -> OperationId {
        self.operation_id
    }

    fn object_id(&self) -> ObjectId {
        self.request.object_id
    }
}

impl Entry for PromotionPlan {
    fn operation_id(&self) -> OperationId {
        self.candidate.operation_id
    }

    fn object_id(&self) -> ObjectId {
        self.candidate.request.object_id
    }
}

/// The operation owns its value; the object index prevents duplicate work
/// without scanning all other active operations.
#[derive(Clone)]
pub(super) struct Active<T> {
    operations: BTreeMap<OperationId, T>,
    objects: BTreeSet<ObjectId>,
}

impl<T> Default for Active<T> {
    fn default() -> Self {
        Self {
            operations: BTreeMap::new(),
            objects: BTreeSet::new(),
        }
    }
}

impl<T: Entry> Active<T> {
    pub(super) fn contains_key(&self, operation: &OperationId) -> bool {
        self.operations.contains_key(operation)
    }

    pub(super) fn contains_object(&self, object: &ObjectId) -> bool {
        self.objects.contains(object)
    }

    pub(super) fn get(&self, operation: &OperationId) -> Option<&T> {
        self.operations.get(operation)
    }

    pub(super) fn values(&self) -> impl Iterator<Item = &T> {
        self.operations.values()
    }

    pub(super) fn insert(&mut self, value: T) {
        self.objects.insert(value.object_id());
        self.operations.insert(value.operation_id(), value);
    }

    pub(super) fn remove(&mut self, operation: &OperationId) -> Option<T> {
        let value = self.operations.remove(operation)?;
        self.objects.remove(&value.object_id());
        Some(value)
    }

    pub(super) fn into_keys(self) -> impl Iterator<Item = OperationId> {
        self.operations.into_keys()
    }

    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }
}
