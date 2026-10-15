use super::*;

/// One tool assembled from three independently replaceable values.
#[derive(Clone)]
pub struct Tool {
    /// Additional lifecycle-hook names declared by this tool's implementation.
    pub hook_names: &'static [&'static str],
    /// Model-visible contract.
    pub definition: ToolDefinition,
    /// Execution implementation.
    pub executor: Arc<dyn ToolExecutor>,
    /// Context projection implementation.
    pub projection: Arc<dyn ToolProjection>,
}

#[derive(Clone, PartialEq, Eq)]
enum ModelSelection {
    Revision(String),
    Ambiguous,
    Hidden,
}

/// Retains pinned revisions and one model selection per logical name.
#[derive(Clone, Default)]
pub struct ToolRegistry {
    versions: BTreeMap<(String, String), Tool>,
    selected: BTreeMap<String, ModelSelection>,
    catalog_revisions: BTreeMap<String, [u8; 32]>,
}

impl ToolRegistry {
    /// Adds a disjoint registry by moving its entries; collisions reject the whole assembly.
    pub fn extend(mut self, additional: Self) -> Result<Self> {
        if additional
            .versions
            .keys()
            .any(|key| self.versions.contains_key(key))
            || additional
                .selected
                .keys()
                .any(|key| self.selected.contains_key(key))
            || additional
                .catalog_revisions
                .keys()
                .any(|key| self.catalog_revisions.contains_key(key))
        {
            return Err(Error::Conflict("tool registries overlap".into()));
        }
        self.versions.extend(additional.versions);
        self.selected.extend(additional.selected);
        self.catalog_revisions.extend(additional.catalog_revisions);
        Ok(self)
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod native;
mod registration;
mod selection;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
const OWNERSHIP_OVERFLOW: Error = Error::NativeBoundary {
    code: crate::wire::ErrorCode::Invalid,
    reason: "tool registry ownership overflow",
};
