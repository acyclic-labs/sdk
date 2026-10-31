use super::*;

/// One tool assembled from three independently replaceable values.
#[derive(Clone)]
pub struct Tool {
    /// Admitted skill provenance, retained on this same pinned registry entry.
    pub skill_source: Option<super::SkillSource>,
    /// Additional lifecycle-hook names declared by this tool's implementation.
    pub hook_names: &'static [&'static str],
    /// Model-visible contract.
    pub definition: ToolDefinition,
    /// Execution implementation.
    pub executor: Arc<dyn ToolExecutor>,
    /// Context projection implementation.
    pub projection: Arc<dyn ToolProjection>,
}

impl Tool {
    /// Filesystem skills share the fixed call capability; their source and
    /// exact definition remain the original invocation's approval identity.
    pub(crate) fn capability_name(&self) -> &str {
        if self.skill_source.is_some() {
            "call"
        } else {
            &self.definition.name
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
enum ModelSelection {
    Revision(String),
    /// Pinned callable skill omitted from the fixed model declaration prefix.
    CallableRevision(String),
    Ambiguous,
    Hidden,
}

impl ModelSelection {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn retained_bytes(&self) -> u64 {
        match self {
            Self::Revision(revision) | Self::CallableRevision(revision) => {
                revision.capacity() as u64
            }
            Self::Ambiguous | Self::Hidden => 0,
        }
    }
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
    /// Exact retained entries, excluding the fixed registry frame. Every
    /// mutation updates this with its admitted payload and metadata delta.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    retained_bytes: u64,
}

mod merge;
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
#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod tests;

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
const OWNERSHIP_OVERFLOW: Error = Error::NativeBoundary {
    code: crate::wire::ErrorCode::Invalid,
    reason: "tool registry ownership overflow",
};
