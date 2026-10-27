//! Process-local custody of provider-issued connection handles.
use super::SandboxInstance;
use crate::ProviderError;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, PoisonError},
};

#[derive(Clone)]
pub(super) struct Instances<T>(Arc<Mutex<BTreeMap<String, T>>>);

impl<T> Default for Instances<T> {
    fn default() -> Self {
        Self(Arc::default())
    }
}

impl<T: Clone> Instances<T> {
    pub fn get(&self, instance: &SandboxInstance) -> Result<T, ProviderError> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&instance.id)
            .cloned()
            .ok_or_else(|| ProviderError::NotFound(instance.id.clone()))
    }

    pub fn insert(&self, id: String, handle: T) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(id, handle);
    }

    pub fn remove(&self, instance: &SandboxInstance) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&instance.id);
    }
}
