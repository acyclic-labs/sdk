//! Pi task binding contracts and retained selection validation.

use super::*;
use acyclic_harness::{
    AgentId, TaskId,
    conversation::{VolumeClass, VolumeOwner, VolumeRef},
    resources::{GenerationRef, ProviderRef},
};

/// Required persisted discriminator; absent historical fields are rejected.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "binding",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(crate) enum PluginTaskBinding {
    #[default]
    HostManaged,
    Pi(Box<PiTaskBinding>),
}

impl PluginTaskBinding {
    /// The host-managed reopen path has no retained task owner and therefore
    /// cannot authorize Pi workspace access, allocation or publication.
    pub(crate) fn require_host_managed_recovery(&self) -> Result<(), String> {
        match self {
            Self::HostManaged => Ok(()),
            Self::Pi(_) => {
                Err("Pi sessions must recover through their owning task runtime".to_owned())
            }
        }
    }
}

/// References on the existing route/prepare record, never authority to dispatch.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiTaskBinding {
    pub(crate) native_session_id: String,
    pub(crate) task_id: TaskId,
    pub(crate) agent_id: AgentId,
    /// Same root keys as the existing physical context; no second agent index.
    pub(crate) project_volumes: BTreeMap<String, VolumeRef>,
    pub(crate) scratch: PiScratchBinding,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiScratchBinding {
    pub(crate) volume: VolumeRef,
    pub(crate) allocation: PiScratchAllocation,
}

/// Explicit allocation progress; an omitted field is never a preparing state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiScratchCreation {
    pub(crate) workspace_id: [u8; 16],
    /// Original allocation/fork operation generation, never the mutable head.
    pub(crate) generation: GenerationRef,
}

impl PiScratchCreation {
    pub(crate) fn validate(&self, volume: &VolumeRef) -> Result<(), String> {
        self.generation.validate().map_err(display)?;
        if self.workspace_id == [0; 16]
            || self.generation.as_resource().provider() != volume.provider()
            || self.generation.as_resource().key().len() != 32
            || self.generation.as_resource().version().is_some()
        {
            return Err("Pi scratch creation is invalid".to_owned());
        }
        Ok(())
    }
}

/// Reject malformed retained selections before reopening any workspace. This
/// checks references only; dispatch still needs the admitted task owner and its
/// exact grants, and workspace IDs must be verified against the owning store.
pub(crate) fn validate_pi_bindings(state: &AdapterState) -> Result<(), String> {
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
            Err("host-managed session contains a Pi task binding".to_owned())
        } else {
            Ok(())
        };
    }
    let mut tasks = BTreeSet::new();
    let mut agents = BTreeSet::new();
    let mut sessions = BTreeSet::new();
    let mut validate = |binding: &PluginTaskBinding,
                        session: Option<&str>,
                        roots: Vec<&str>,
                        preparing: bool|
     -> Result<(), String> {
        let PluginTaskBinding::Pi(binding) = binding else {
            return Err("Pi session contains a host-managed task binding".to_owned());
        };
        binding.validate(session, roots, preparing)?;
        if !tasks.insert(binding.task_id.into_bytes())
            || !agents.insert(binding.agent_id.into_bytes())
            || !sessions.insert(binding.native_session_id.clone())
        {
            return Err("Pi task, agent or native session identity is duplicated".to_owned());
        }
        Ok(())
    };
    validate(
        &state.task_binding,
        Some(&state.root_session_id),
        state.roots.keys().map(String::as_str).collect(),
        state.pending_root_registration,
    )?;
    for (key, route) in &state.routes {
        if key != &route.agent_id {
            return Err("Pi route key differs from its native agent identity".to_owned());
        }
        validate(
            &route.task_binding,
            Some(&route.agent_id),
            route.roots.keys().map(String::as_str).collect(),
            false,
        )?;
    }
    for pending in &state.pending {
        let preparing = matches!(
            pending.lifecycle,
            PendingSpawnLifecycle::Preparing | PendingSpawnLifecycle::Discarding
        ) && pending.roots.is_empty();
        let roots = if preparing {
            if pending.parent_agent_id == state.root_agent_id {
                state.roots.keys().map(String::as_str).collect()
            } else {
                state
                    .routes
                    .get(&pending.parent_agent_id)
                    .ok_or_else(|| "Pi pending spawn has no retained parent route".to_owned())?
                    .roots
                    .keys()
                    .map(String::as_str)
                    .collect()
            }
        } else {
            pending.roots.keys().map(String::as_str).collect()
        };
        validate(&pending.task_binding, None, roots, preparing)?;
    }
    Ok(())
}

impl PiTaskBinding {
    pub(crate) fn validate(
        &self,
        session: Option<&str>,
        roots: Vec<&str>,
        preparing: bool,
    ) -> Result<(), String> {
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
            return Err(
                "Pi binding has an invalid task, agent or native session identity".to_owned(),
            );
        }
        if self.project_volumes.len() > MAXIMUM_ADAPTER_ROOTS
            || !self.project_volumes.keys().map(String::as_str).eq(roots)
        {
            return Err("Pi project selection differs from its retained root map".to_owned());
        }
        let scratch = &self.scratch.volume;
        validate_pi_provider(scratch.provider())?;
        if scratch.class() != VolumeClass::AgentPrivate
            || scratch.owner() != &VolumeOwner::Agent(self.agent_id)
        {
            return Err("Pi scratch is not private to its admitted agent".to_owned());
        }
        let mut volumes = BTreeSet::new();
        for volume in self.project_volumes.values() {
            if volume.class() != VolumeClass::Project
                || !matches!(volume.owner(), VolumeOwner::Project(_))
                || volume.provider() != scratch.provider()
                || !volumes.insert(volume.storage_name().map_err(display)?)
            {
                return Err(
                    "Pi project volume selection has an invalid role, provider or alias".to_owned(),
                );
            }
        }
        match &self.scratch.allocation {
            PiScratchAllocation::Created(creation) => creation.validate(scratch)?,
            PiScratchAllocation::Preparing if preparing => {}
            _ => return Err("Pi scratch creation is missing or invalid".to_owned()),
        }
        Ok(())
    }
}

fn validate_pi_provider(provider: &ProviderRef) -> Result<(), String> {
    provider.validate().map_err(display)?;
    if [provider.namespace(), provider.family(), provider.version()]
        .iter()
        .any(|part| part.len() > 256)
        || provider.family() != "filesystem"
    {
        return Err("Pi volume provider is invalid or exceeds its identity bound".to_owned());
    }
    Ok(())
}

#[cfg(test)]
#[path = "pi_binding_tests.rs"]
mod tests;
