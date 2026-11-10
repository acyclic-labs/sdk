//! Host-held routing; selections in agent records contain no credentials.
use super::{Model, ModelProvider};
use crate::Result;
use std::sync::Arc;

/// A model and the provider that serves it.
pub type ModelBinding = (Model, Arc<dyn ModelProvider>);

/// Resolves durable provider/model selections through the host's catalog.
pub trait ModelResolver: acyclic_stream::ProviderPlatform {
    /// Unknown or unavailable selections fail without using another provider.
    fn resolve(&self, selection: &str) -> Result<ModelBinding>;
}

/// Checks the public provider/model address, allowing slashes in model names.
pub fn validate(selection: &str) -> Result<()> {
    match selection.split_once('/') {
        Some((provider, model))
            if !provider.trim().is_empty()
                && !model.trim().is_empty()
                && selection.trim() == selection =>
        {
            Ok(())
        }
        _ => Err(crate::Error::Invalid("expected provider/model".into())),
    }
}
