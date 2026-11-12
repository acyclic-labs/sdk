use super::*;
use std::collections::BTreeSet;

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
    source_ceiling: Option<Arc<BTreeSet<String>>>,
}

/// A shared, event-invalidated tool catalog. Refresh runs before model dispatch;
/// snapshots retain their executors while an admitted call is in flight.
pub trait ToolSource: acyclic_stream::ProviderPlatform {
    /// Refresh only sources invalidated by their watch.
    fn refresh(&self) -> BoxFuture<'_, Result<()>>;
    /// The last completely admitted catalog.
    fn snapshot(&self) -> Result<ToolRegistry>;
    /// Whether this source serves file-authored aliases on demand. Such aliases
    /// are discovered through ordinary paged directory reads, not model schemas.
    fn has_lazy_tools(&self) -> bool {
        false
    }
    /// Load just one authored alias from its original producer. The caller must
    /// validate its input and retain its canonical invocation before dispatch.
    /// This observation grants no execution authority.
    fn load<'a>(&'a self, _name: &'a str) -> BoxFuture<'a, Result<Option<Arc<Tool>>>> {
        Box::pin(async { Ok(None) })
    }
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
    pub(crate) fn has_lazy_tools(&self) -> bool {
        self.sources.iter().any(|source| source.has_lazy_tools())
    }

    /// Resolve one file-authored alias without materializing a catalog. Fixed
    /// definitions keep precedence, and a template ceiling is checked before
    /// any source read.
    pub async fn load(&self, name: &str) -> Result<Option<Arc<Tool>>> {
        let mut selected = None;
        for source in &self.sources {
            if source.has_lazy_tools()
                && (self.source_allowed(name)
                    || source
                        .capability()
                        .and_then(|grant| grant.strip_prefix("tool:call:"))
                        .is_some_and(|capability| self.source_allowed(capability)))
                && let Some(tool) = source.load(name).await?
            {
                if selected.is_some() || self.resolve(name, None)?.is_some() {
                    return Err(Error::Conflict("tool sources overlap".into()));
                }
                selected = Some(tool);
            }
        }
        Ok(selected)
    }
    pub(crate) fn restrict_sources(&mut self, allowed: &[String]) {
        let allowed = allowed
            .iter()
            .filter(|name| self.source_allowed(name))
            .cloned()
            .collect();
        self.source_ceiling = Some(Arc::new(allowed));
    }

    pub(super) fn source_allowed(&self, name: &str) -> bool {
        self.source_ceiling
            .as_ref()
            .is_none_or(|allowed| allowed.contains(name))
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
        if !self.source_allowed(&tool.name) {
            return Ok(false);
        }
        for source in &self.sources {
            let snapshot = source.snapshot()?;
            if source
                .capability()
                .is_some_and(|grant| grants.contains(grant))
                && (snapshot.resolve(&tool.name, None)?.is_some()
                    || source.recover(tool, None)?.is_some()
                    || snapshot.recover(tool, None)?.is_some())
                || snapshot.source_granted(tool, grants)?
            {
                return Ok(true);
            }
        }
        Ok(false)
    }
    /// Fixed, granted declarations are pinned by the executor configuration.
    /// Source overlap is rejected when preparing their original model request.
    pub(crate) fn fixed_for_grants(
        &self,
        name: &str,
        grants: &crate::Capabilities,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Option<Arc<Tool>>> {
        Ok(self
            .get_shared(name, None, io)?
            .filter(|tool| {
                let capability = tool.executor.capability().unwrap_or(tool);
                grants.iter().any(|grant| {
                    grant.strip_prefix("tool:call:") == Some(capability.definition.name.as_str())
                })
            })
            .cloned())
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
    /// An explicit revision must still be selected; retained revisions remain
    /// available through `get` and recovery without authorizing fresh calls.
    pub fn resolve(
        &self,
        name: &str,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Option<Arc<Tool>>> {
        for source in &self.sources {
            if let Some(tool) = source.snapshot()?.resolve(name, io)?
                && self.source_allowed(&tool.definition.name)
            {
                return Ok(Some(tool));
            }
        }
        Ok(self
            .get_shared(name, None, io)?
            .filter(|tool| {
                matches!(
                    self.data.selected.get(&tool.definition.name),
                    Some(ModelSelection::Revision(revision)
                        | ModelSelection::Native { revision: Some(revision), .. })
                        if revision == &tool.definition.revision
                )
            })
            .cloned())
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
#[cfg(not(target_arch = "wasm32"))]
const REGISTRY_BYTES: usize = std::mem::size_of::<ToolRegistry>()
    + std::mem::size_of::<RegistryData>()
    + 2 * std::mem::size_of::<usize>();
const VERSION_SLOT_BYTES: usize = std::mem::size_of::<(String, String)>()
    + std::mem::size_of::<Arc<Tool>>()
    + std::mem::size_of::<Tool>()
    - std::mem::size_of::<ToolDefinition>()
    + 2 * std::mem::size_of::<usize>();

mod lookup;
#[cfg(not(target_arch = "wasm32"))]
mod native;
mod registration;
mod selection;
#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod tests;

#[cfg(not(target_arch = "wasm32"))]
const OWNERSHIP_OVERFLOW: Error = Error::NativeBoundary {
    code: crate::wire::ErrorCode::Invalid,
    reason: "tool registry ownership overflow",
};
