//! Host-held routing; selections in agent records contain no credentials.
use super::{Model, ModelProvider};
use crate::Result;
use std::sync::Arc;

/// A model selection's bound provider.
pub type ModelBinding = (Model, Arc<dyn ModelProvider>);

/// One immutable protected selection and its lazily admitted native declarations.
/// Retaining this owner preserves the exact provider and declarations across
/// later catalog replacements, including replacements at the same model address.
pub struct ResolvedModel {
    binding: ModelBinding,
    native: once_cell::sync::OnceCell<Option<Arc<crate::tools::native::NativeTools>>>,
}

impl ResolvedModel {
    /// Retains a host-admitted binding without decoding unused declarations.
    pub fn new(binding: ModelBinding) -> Self {
        Self {
            binding,
            native: once_cell::sync::OnceCell::new(),
        }
    }

    pub(crate) fn native_tools(&self) -> Result<Option<Arc<crate::tools::native::NativeTools>>> {
        self.native
            .get_or_try_init(|| self.binding.1.native_tools(&self.binding.0))
            .cloned()
    }
}

impl std::ops::Deref for ResolvedModel {
    type Target = ModelBinding;

    fn deref(&self) -> &ModelBinding {
        &self.binding
    }
}

/// Resolves model selections to providers once: every composition resolving through one
/// resolver reuses the provider of a selection it already resolved; an unseen selection,
/// or a new resolver over a changed catalog, resolves anew.
pub struct ModelResolver {
    default: String,
    resolve: Arc<Resolve>,
    resolved: std::sync::Mutex<std::collections::BTreeMap<String, Arc<ResolvedModel>>>,
}

#[cfg(not(target_arch = "wasm32"))]
type Resolve = dyn Fn(&str) -> Result<ModelBinding> + Send + Sync;
#[cfg(target_arch = "wasm32")]
type Resolve = dyn Fn(&str) -> Result<ModelBinding>;

impl ModelResolver {
    /// The catalog default used when the root configuration selects no model.
    pub fn default_model(&self) -> &str {
        &self.default
    }

    /// Resolves through `resolve`, selecting `default` when nothing else selects a model.
    pub fn new(
        default: impl Into<String>,
        resolve: impl Fn(&str) -> Result<ModelBinding> + acyclic_stream::ProviderPlatform + 'static,
    ) -> Self {
        Self {
            default: default.into(),
            resolve: Arc::new(resolve),
            resolved: Default::default(),
        }
    }

    /// A new immutable catalog snapshot containing an explicitly bound model.
    /// Existing compositions retain their original bindings and calibration.
    pub fn with_binding(&self, binding: ModelBinding) -> Result<Self> {
        binding.0.validate()?;
        let mut resolved = self
            .resolved
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        resolved.insert(
            format!("{}/{}", binding.0.provider, binding.0.name),
            Arc::new(ResolvedModel::new(binding)),
        );
        Ok(Self {
            default: self.default.clone(),
            resolve: self.resolve.clone(),
            resolved: std::sync::Mutex::new(resolved),
        })
    }

    /// Reuses the original protected provider binding for this selection.
    pub fn resolve(&self, selection: &str) -> Result<ModelBinding> {
        Ok(self.resolve_shared(selection)?.binding.clone())
    }

    /// Retains the exact immutable model/provider pair without copying model
    /// metadata. Consumers can retain this owner across calls and catalog
    /// replacements; a later catalog cannot change an admitted binding.
    pub fn resolve_shared(&self, selection: &str) -> Result<Arc<ResolvedModel>> {
        let mut resolved = self
            .resolved
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(model) = resolved.get(selection) {
            return Ok(Arc::clone(model));
        }
        let model = Arc::new(ResolvedModel::new((self.resolve)(selection)?));
        resolved.insert(selection.into(), Arc::clone(&model));
        Ok(model)
    }
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
