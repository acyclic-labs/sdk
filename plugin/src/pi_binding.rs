//! Pi task binding contracts and retained selection validation.

use super::*;
use acyclic_harness::{
    AgentId, TaskId,
    conversation::{VolumeClass, VolumeOwner, VolumeRef},
    resources::{GenerationRef, ProviderRef},
};

/// Required persisted discriminator; absent historical fields are rejected.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "binding",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(crate) enum PluginTaskBinding<B = Box<PiTaskBinding>> {
    HostManaged,
    Pi(B),
}

impl Default for PluginTaskBinding {
    fn default() -> Self {
        Self::HostManaged
    }
}

impl PluginTaskBinding {
    /// The host-managed reopen path has no retained task owner and therefore cannot authorize Pi workspace access, allocation or publication.
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
    /// Explicit unmounted result storage.
    pub(crate) results: Option<PiScratchBinding>,
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
    pub(crate) fn validate(&self, volume: &VolumeRef) -> acyclic_harness::Result<()> {
        self.validate_inner(volume, None)
    }

    pub(crate) fn validate_with_native_work(
        &self,
        volume: &VolumeRef,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<()> {
        self.validate_inner(volume, Some(work))
    }

    fn validate_inner(
        &self,
        volume: &VolumeRef,
        work: Option<&acyclic_fs::NativeMaterializationWork>,
    ) -> acyclic_harness::Result<()> {
        binding_validation_step(work, 1)?;
        match work {
            Some(work) => {
                self.generation
                    .validate(&mut Some(&mut acyclic_harness::crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(work)?)))?
            }
            None => self.generation.validate(&mut None)?,
        }
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

/// Reject malformed retained selections before reopening any workspace.
pub(crate) fn validate_pi_bindings(state: &AdapterState) -> Result<(), String> {
    validate_pi_bindings_inner(state).map_err(|error| match error {
        acyclic_harness::Error::NativeBoundary { reason, .. } => reason.to_owned(),
        error => display(error),
    })
}

fn validate_pi_bindings_inner<'s>(state: &'s AdapterState) -> acyclic_harness::Result<()> {
    if matches!(state.task_binding, PluginTaskBinding::HostManaged) {
        for route in state.routes.values() {
            if matches!(route.task_binding, PluginTaskBinding::Pi(_)) {
                return Err(invalid_binding(
                    "host-managed session contains a Pi task binding",
                ));
            }
        }
        for pending in &state.pending {
            if matches!(pending.task_binding, PluginTaskBinding::Pi(_)) {
                return Err(invalid_binding(
                    "host-managed session contains a Pi task binding",
                ));
            }
        }
        return Ok(());
    }
    let mut tasks = BTreeSet::new();
    let mut agents = BTreeSet::new();
    let mut sessions = BTreeSet::new();
    let mut unique =
        |binding: &'s PluginTaskBinding| -> acyclic_harness::Result<&'s PiTaskBinding> {
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
            Ok(binding)
        };
    unique(&state.task_binding)?.validate(
        Some(&state.root_session_id),
        state.roots.keys().map(String::as_str),
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
        unique(&route.task_binding)?.validate(
            None,
            route.roots.keys().map(String::as_str),
            false,
        )?;
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
        let preparing = matches!(
            pending.lifecycle,
            PendingSpawnLifecycle::Preparing | PendingSpawnLifecycle::Discarding
        ) && pending.roots.is_empty();
        let binding = unique(&pending.task_binding)?;
        if preparing {
            binding.validate(
                None,
                binding.project_volumes.keys().map(String::as_str),
                preparing,
            )?;
        } else {
            binding.validate(None, pending.roots.keys().map(String::as_str), preparing)?;
        }
    }
    Ok(())
}

impl PiTaskBinding {
    pub(crate) fn validate<'a>(
        &self,
        session: Option<&str>,
        roots: impl IntoIterator<Item = &'a str>,
        preparing: bool,
    ) -> acyclic_harness::Result<()> {
        self.validate_inner(session, roots, preparing, None)
    }

    pub(crate) fn validate_with_native_work<'a>(
        &self,
        session: Option<&str>,
        roots: impl IntoIterator<Item = &'a str>,
        preparing: bool,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<()> {
        self.validate_inner(session, roots, preparing, Some(work))
    }

    fn validate_inner<'a>(
        &self,
        session: Option<&str>,
        roots: impl IntoIterator<Item = &'a str>,
        preparing: bool,
        work: Option<&acyclic_fs::NativeMaterializationWork>,
    ) -> acyclic_harness::Result<()> {
        binding_validation_step(work, self.native_session_id.len() as u64 + 1)?;
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
        binding_validation_step(work, self.project_volumes.len() as u64)?;
        if self.project_volumes.len() > MAXIMUM_ADAPTER_ROOTS
            || !self.project_volumes.keys().map(String::as_str).eq(roots)
        {
            return Err(invalid_binding(
                "Pi project selection differs from its retained root map",
            ));
        }
        let scratch = &self.scratch.volume;
        validate_pi_provider(scratch.provider(), work)?;
        if scratch.class() != VolumeClass::AgentPrivate
            || scratch.owner() != &VolumeOwner::Agent(self.agent_id)
        {
            return Err(invalid_binding(
                "Pi scratch is not private to its admitted agent",
            ));
        }
        for (index, volume) in self.project_volumes.values().enumerate() {
            binding_validation_step(work, index as u64 + 1)?;
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
            PiScratchAllocation::Created(creation) => creation.validate_inner(scratch, work)?,
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
                PiScratchAllocation::Created(creation) => {
                    creation.validate_inner(&results.volume, work)?
                }
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

fn validate_pi_provider(
    provider: &ProviderRef,
    work: Option<&acyclic_fs::NativeMaterializationWork>,
) -> acyclic_harness::Result<()> {
    match work {
        Some(work) => provider.validate(&mut Some(&mut acyclic_harness::crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(work)?)))?,
        None => provider.validate(&mut None)?,
    }
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

fn binding_validation_step(
    work: Option<&acyclic_fs::NativeMaterializationWork>,
    items: u64,
) -> acyclic_harness::Result<()> {
    if let Some(work) = work {
        work.admit(acyclic_fs::WorkCounters {
            items_examined: items,
            ..acyclic_fs::WorkCounters::UNCHARGED
        })?;
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
