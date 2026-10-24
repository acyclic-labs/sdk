//! Pi bindings on the existing route and preparation records.
use super::*;
use acyclic_harness::{
    AgentId, TaskId,
    conversation::{VolumeClass, VolumeOwner, VolumeRef},
    resources::{GenerationRef, ProviderRef},
};

/// Persisted host discriminator.
#[derive(Clone, Debug, Default, Serialize, Deserialize, acyclic_harness::HeapLayout)]
#[heap_layout(crate = acyclic_harness)]
#[serde(
    tag = "kind",
    content = "binding",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(crate) enum PluginTaskBinding<B = Box<PiTaskBinding>> {
    #[default]
    HostManaged,
    Pi(B),
}

impl PluginTaskBinding {
    pub(crate) fn require_host_managed_recovery(&self) -> Result<(), String> {
        match self {
            Self::HostManaged => Ok(()),
            Self::Pi(_) => {
                Err("Pi sessions must recover through their owning task runtime".to_owned())
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, acyclic_harness::HeapLayout)]
#[heap_layout(crate = acyclic_harness)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiTaskBinding {
    pub(crate) native_session_id: String,
    pub(crate) task_id: TaskId,
    pub(crate) agent_id: AgentId,
    pub(crate) project_volumes: BTreeMap<String, VolumeRef>,
    pub(crate) scratch: PiScratchBinding,
    pub(crate) results: Option<PiScratchBinding>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, acyclic_harness::HeapLayout)]
#[heap_layout(crate = acyclic_harness)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiScratchBinding {
    pub(crate) volume: VolumeRef,
    pub(crate) allocation: PiScratchAllocation,
}

/// Explicit progress; an omitted allocation is never preparing.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, acyclic_harness::HeapLayout)]
#[heap_layout(crate = acyclic_harness)]
#[serde(
    tag = "state",
    content = "creation",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(crate) enum PiScratchAllocation {
    Preparing,
    Created(PiScratchCreation),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, acyclic_harness::HeapLayout)]
#[heap_layout(crate = acyclic_harness)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiScratchCreation {
    pub(crate) workspace_id: [u8; 16],
    pub(crate) generation: GenerationRef,
}

impl PiScratchCreation {
    pub(crate) fn validate(&self, volume: &VolumeRef) -> acyclic_harness::Result<()> {
        self.generation.validate(&mut None)?;
        if self.workspace_id == [0; 16]
            || self.generation.as_resource().provider() != volume.provider()
            || self.generation.as_resource().key().len() != 32
            || self.generation.as_resource().version().is_some()
        {
            return Err(invalid_binding("Pi scratch creation is invalid"));
        }
        Ok(())
    }
}

/// Reject malformed selections before reopening workspaces.
pub(crate) fn validate_pi_bindings<'s>(state: &'s AdapterState) -> Result<(), String> {
    let validate = || -> acyclic_harness::Result<()> {
        if matches!(state.task_binding, PluginTaskBinding::HostManaged) {
            return if state
                .routes
                .values()
                .any(|route| matches!(route.task_binding, PluginTaskBinding::Pi(_)))
                || state
                    .pending
                    .iter()
                    .any(|pending| matches!(pending.task_binding, PluginTaskBinding::Pi(_)))
            {
                Err(invalid_binding(
                    "host-managed session contains a Pi task binding",
                ))
            } else {
                Ok(())
            };
        }
        let mut tasks = BTreeSet::new();
        let mut agents = BTreeSet::new();
        let mut sessions = BTreeSet::new();
        let mut unique = |binding: &'s PluginTaskBinding| -> acyclic_harness::Result<()> {
            let PluginTaskBinding::Pi(binding) = binding else {
                return Err(invalid_binding(
                    "Pi session contains a host-managed task binding",
                ));
            };
            if !tasks.insert(binding.task_id.into_bytes())
                || !agents.insert(binding.agent_id.into_bytes())
                || !sessions.insert(binding.native_session_id.as_str())
            {
                return Err(invalid_binding(
                    "Pi task, agent or native session identity is duplicated",
                ));
            }
            Ok(())
        };
        unique(&state.task_binding)?;
        let PluginTaskBinding::Pi(root) = &state.task_binding else {
            unreachable!()
        };
        root.validate(
            Some(&state.root_session_id),
            state.roots.keys().map(|key| key.as_str()),
            state.pending_root_registration,
        )?;
        let mut aliases = BTreeSet::from([
            "root",
            state.root_agent_id.as_str(),
            state.root_session_id.as_str(),
        ]);
        for (key, route) in &state.routes {
            if key != &route.agent_id || !aliases.insert(key.as_str()) {
                return Err(invalid_binding(
                    "Pi route key differs from its original alias or collides with another route",
                ));
            }
            unique(&route.task_binding)?;
            let PluginTaskBinding::Pi(binding) = &route.task_binding else {
                unreachable!()
            };
            binding.validate(None, route.roots.keys().map(|key| key.as_str()), false)?;
        }
        for pending in &state.pending {
            let alias = pending
                .agent_id
                .as_deref()
                .ok_or_else(|| invalid_binding("Pi pending spawn has no original agent alias"))?;
            if alias.is_empty()
                || alias.len() > 128
                || !aliases.insert(alias)
                || !alias
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
            {
                return Err(invalid_binding(
                    "Pi pending agent alias is invalid or duplicated",
                ));
            }
            unique(&pending.task_binding)?;
            let PluginTaskBinding::Pi(binding) = &pending.task_binding else {
                unreachable!()
            };
            let preparing = matches!(
                pending.lifecycle,
                PendingSpawnLifecycle::Preparing | PendingSpawnLifecycle::Discarding
            ) && pending.roots.is_empty();
            if preparing {
                binding.validate(
                    None,
                    binding.project_volumes.keys().map(|key| key.as_str()),
                    true,
                )?;
            } else {
                binding.validate(None, pending.roots.keys().map(|key| key.as_str()), false)?;
            }
        }
        Ok(())
    };
    validate().map_err(|error| match error {
        acyclic_harness::Error::NativeBoundary { reason, .. } => reason.to_owned(),
        error => display(error),
    })
}

impl PiTaskBinding {
    pub(crate) fn validate<'a>(
        &self,
        session: Option<&str>,
        roots: impl IntoIterator<Item = &'a str>,
        preparing: bool,
    ) -> acyclic_harness::Result<()> {
        let id = self.native_session_id.as_bytes();
        if id.is_empty()
            || id.len() > 256
            || id.first().is_none_or(|byte| !byte.is_ascii_alphanumeric())
            || id.last().is_none_or(|byte| !byte.is_ascii_alphanumeric())
            || id
                .iter()
                .any(|byte| !byte.is_ascii_alphanumeric() && !matches!(byte, b'-' | b'_' | b'.'))
            || session.is_some_and(|expected| expected != self.native_session_id)
            || self.task_id.into_bytes() == [0; 16]
            || self.agent_id.into_bytes() == [0; 16]
        {
            return Err(invalid_binding(
                "Pi binding has an invalid task, agent or native session identity",
            ));
        }
        if self.project_volumes.len() > MAXIMUM_ADAPTER_ROOTS
            || !self
                .project_volumes
                .keys()
                .map(|key| key.as_str())
                .eq(roots)
        {
            return Err(invalid_binding(
                "Pi project selection differs from its retained root map",
            ));
        }
        let scratch = &self.scratch.volume;
        validate_pi_provider(scratch.provider())?;
        if scratch.class() != VolumeClass::AgentPrivate
            || scratch.owner() != &VolumeOwner::Agent(self.agent_id)
        {
            return Err(invalid_binding(
                "Pi scratch is not private to its admitted agent",
            ));
        }
        for (index, volume) in self.project_volumes.values().enumerate() {
            if volume.class() != VolumeClass::Project
                || !matches!(volume.owner(), VolumeOwner::Project(_))
                || volume.provider() != scratch.provider()
                || self
                    .project_volumes
                    .values()
                    .take(index)
                    .any(|prior| prior == volume)
            {
                return Err(invalid_binding(
                    "Pi project volume selection has an invalid role, provider or alias",
                ));
            }
        }
        match &self.scratch.allocation {
            PiScratchAllocation::Created(creation) => creation.validate(scratch)?,
            PiScratchAllocation::Preparing if preparing => {}
            _ => return Err(invalid_binding("Pi scratch creation is missing or invalid")),
        }
        if let Some(results) = &self.results {
            if results.volume == *scratch
                || results.volume.class() != VolumeClass::AgentPrivate
                || results.volume.owner() != &VolumeOwner::Agent(self.agent_id)
                || results.volume.provider() != scratch.provider()
                || self
                    .project_volumes
                    .values()
                    .any(|volume| volume == &results.volume)
            {
                return Err(invalid_binding(
                    "Pi result storage must be distinct and private to its admitted agent",
                ));
            }
            match &results.allocation {
                PiScratchAllocation::Created(creation) => creation.validate(&results.volume)?,
                PiScratchAllocation::Preparing if preparing => {}
                _ => {
                    return Err(invalid_binding(
                        "Pi result storage has no authenticated creation",
                    ));
                }
            }
        }
        Ok(())
    }
}

fn validate_pi_provider(provider: &ProviderRef) -> acyclic_harness::Result<()> {
    provider.validate(&mut None)?;
    if [provider.namespace(), provider.family(), provider.version()]
        .iter()
        .any(|part| part.len() > 256)
        || provider.family() != "filesystem"
    {
        return Err(invalid_binding(
            "Pi volume provider is invalid or exceeds its identity bound",
        ));
    }
    Ok(())
}

fn invalid_binding(reason: &'static str) -> acyclic_harness::Error {
    acyclic_harness::Error::NativeBoundary {
        code: acyclic_harness::wire::ErrorCode::Invalid,
        reason,
    }
}

#[cfg(test)]
#[path = "pi_binding_tests.rs"]
pub(crate) mod tests;
