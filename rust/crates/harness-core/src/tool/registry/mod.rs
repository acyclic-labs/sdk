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
    /// A capability the model sees as these native declarations, each
    /// selected under its own name. Its grant still names the capability.
    Native {
        revision: Option<String>,
        names: Vec<String>,
    },
}

impl ModelSelection {
    fn owned_bytes(&self) -> u64 {
        match self {
            Self::Revision(revision) => revision.capacity() as u64,
            Self::Native { revision, names } => names.iter().fold(
                (names.capacity() * std::mem::size_of::<String>()) as u64
                    + revision
                        .as_ref()
                        .map_or(0, |revision| revision.capacity() as u64),
                |bytes, name| bytes.saturating_add(name.capacity() as u64),
            ),
            Self::Ambiguous | Self::Hidden => 0,
        }
    }
}

/// Immutable shared snapshots of pinned revisions and the model selection.
/// Cloning shares storage; changing a snapshot copies metadata and retains payloads.
#[derive(Clone, Default)]
pub struct ToolRegistry {
    data: Arc<RegistryData>,
    sources: Vec<Arc<dyn ToolSource>>,
    source_ceiling: Option<Arc<[String]>>,
}

/// A shared, event-invalidated tool catalog. Refresh runs before model dispatch;
/// snapshots retain their executors while an admitted call is in flight.
pub trait ToolSource: acyclic_stream::ProviderPlatform {
    /// Refresh only sources invalidated by their watch.
    fn refresh(&self) -> BoxFuture<'_, Result<()>>;
    /// The last completely admitted catalog.
    fn snapshot(&self) -> Result<ToolRegistry>;
    /// Names previously published by this source and subsequently revoked.
    fn was_removed(&self, _name: &str) -> Result<bool> {
        Ok(false)
    }
    /// The host's grant for tools supplied by this source.
    fn capability(&self) -> Option<&str> {
        None
    }
    /// Rebind an admitted revision to its original producer for replay or
    /// reconciliation. This never makes a removed tool callable again.
    fn recover(
        &self,
        _definition: &ToolDefinition,
        _io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Option<Arc<Tool>>> {
        Ok(None)
    }
}

impl ToolRegistry {
    pub(crate) fn restrict_sources(&mut self, allowed: &[String]) {
        self.source_ceiling = Some(
            allowed
                .iter()
                .filter(|name| self.source_allowed(name))
                .cloned()
                .collect(),
        );
    }

    fn source_allowed(&self, name: &str) -> bool {
        self.source_ceiling
            .as_ref()
            .is_none_or(|allowed| allowed.iter().any(|allowed| allowed == name))
    }


    pub(crate) fn fixed_definitions(
        &self,
        grants: &crate::Capabilities,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Vec<ToolDefinition>> {
        Self {
            data: self.data.clone(),
            sources: Vec::new(),
            source_ceiling: None,
        }
        .definitions_for_grants(grants, io)
    }
    pub(crate) fn source_granted(
        &self,
        tool: &ToolDefinition,
        grants: &crate::Capabilities,
    ) -> Result<bool> {
        if !self.source_allowed(&tool.name) { return Ok(false); }
        for source in &self.sources {
            let snapshot = source.snapshot()?;
            if source
                .capability()
                .is_some_and(|grant| grants.contains(grant))
                && (snapshot.resolve(&tool.name, None)?.is_some()
                    || source.recover(tool, None)?.is_some())
                || snapshot.source_granted(tool, grants)?
            {
                return Ok(true);
            }
        }
        Ok(false)
    }
    pub(crate) fn recover(
        &self,
        definition: &ToolDefinition,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Option<Arc<Tool>>> {
        if let Some(tool) = self.get_shared(&definition.name, Some(&definition.revision), io)? {
            return Ok(Some(Arc::clone(tool)));
        }
        for source in &self.sources {
            if let Some(tool) = source.recover(definition, io)? {
                return Ok(Some(tool));
            }
            if let Some(tool) = source.snapshot()?.recover(definition, io)? {
                return Ok(Some(tool));
            }
        }
        Ok(None)
    }
    pub(crate) fn was_removed(&self, name: &str) -> Result<bool> {
        for source in &self.sources {
            if source.was_removed(name)? {
                return Ok(true);
            }
        }
        Ok(false)
    }
    /// Attach one host-owned live source, shared by all registry clones.
    pub fn with_source(mut self, source: Arc<dyn ToolSource>) -> Self {
        self.sources.push(source);
        self
    }

    pub(crate) fn retain_sources(&mut self, mut keep: impl FnMut(&dyn ToolSource) -> bool) {
        self.sources.retain(|source| keep(source.as_ref()));
    }

    pub(crate) async fn refresh(&self) -> Result<()> {
        for source in &self.sources {
            source.refresh().await?;
        }
        Ok(())
    }

    /// Resolve the current live selection, retaining its executor through a call.
    pub fn resolve(
        &self,
        name: &str,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Option<Arc<Tool>>> {
        for source in &self.sources {
            if let Some(tool) = source.snapshot()?.resolve(name, io)? {
                return Ok(self.source_allowed(name).then_some(tool));
            }
        }
        Ok(self.get_shared(name, None, io)?.cloned())
    }

    pub(crate) fn has_sources(&self) -> bool {
        !self.sources.is_empty()
    }
}

#[derive(Clone, Default)]
struct RegistryData {
    versions: BTreeMap<(String, String), Arc<Tool>>,
    selected: BTreeMap<String, ModelSelection>,
    catalog_revisions: BTreeMap<String, [u8; 32]>,
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
