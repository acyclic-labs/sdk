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

/// Immutable shared snapshots of pinned revisions and the model selection.
/// Cloning shares storage; changing a snapshot copies metadata and retains payloads.
#[derive(Clone, Default)]
pub struct ToolRegistry {
    data: Arc<RegistryData>,
}

#[derive(Clone, Default)]
struct RegistryData {
    versions: BTreeMap<(String, String), Arc<Tool>>,
    selected: BTreeMap<String, ModelSelection>,
    catalog_revisions: BTreeMap<String, [u8; 32]>,
}

impl ToolRegistry {
    /// Adds a disjoint registry by moving its entries; collisions reject the whole assembly.
    pub fn extend(mut self, additional: Self) -> Result<Self> {
        if additional
            .data
            .versions
            .keys()
            .any(|key| self.data.versions.contains_key(key))
            || additional
                .data
                .selected
                .keys()
                .any(|key| self.data.selected.contains_key(key))
            || additional
                .data
                .catalog_revisions
                .keys()
                .any(|key| self.data.catalog_revisions.contains_key(key))
        {
            return Err(Error::Conflict("tool registries overlap".into()));
        }
        let data = Arc::make_mut(&mut self.data);
        let additional = Arc::unwrap_or_clone(additional.data);
        data.versions.extend(additional.versions);
        data.selected.extend(additional.selected);
        data.catalog_revisions.extend(additional.catalog_revisions);
        Ok(self)
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
const REGISTRY_BYTES: usize = std::mem::size_of::<ToolRegistry>()
    + std::mem::size_of::<RegistryData>()
    + 2 * std::mem::size_of::<usize>();
#[cfg(feature = "filesystem")]
const VERSION_SLOT_BYTES: usize = std::mem::size_of::<(String, String)>()
    + std::mem::size_of::<Arc<Tool>>()
    + std::mem::size_of::<Tool>()
    - std::mem::size_of::<ToolDefinition>()
    + 2 * std::mem::size_of::<usize>();

mod lookup;
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
