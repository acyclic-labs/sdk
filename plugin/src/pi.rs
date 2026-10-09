//! Pi bindings and installation over the existing task and configuration owners.

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

/// References on the existing route/prepare record, never authority to dispatch.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiTaskBinding {
    pub(crate) native_session_id: String,
    pub(crate) task_id: TaskId,
    pub(crate) agent_id: AgentId,
    /// Same root keys as the existing physical context; no second agent index.
    pub(crate) project_volumes: BTreeMap<String, VolumeRef>,
    pub(crate) scratch: PiScratchBinding,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiScratchBinding {
    pub(crate) volume: VolumeRef,
    pub(crate) allocation: PiScratchAllocation,
}

/// Explicit allocation progress; an omitted field is never a preparing state.
#[derive(Clone, Debug, Serialize, Deserialize)]
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

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiScratchCreation {
    pub(crate) workspace_id: [u8; 16],
    /// Original allocation/fork operation generation, never the mutable head.
    pub(crate) generation: GenerationRef,
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
    fn validate(
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
            PiScratchAllocation::Created(creation)
                if creation.workspace_id != [0; 16]
                    && creation.generation.as_resource().provider() == scratch.provider()
                    && creation.generation.as_resource().key().len() == 32
                    && creation.generation.as_resource().version().is_none() => {}
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

pub(crate) fn pi_settings_path(project: bool) -> Result<PathBuf, String> {
    if project {
        Ok(env::current_dir()
            .map_err(display)?
            .join(".pi/settings.json"))
    } else {
        let directory = match env::var_os("PI_CODING_AGENT_DIR") {
            Some(directory) => PathBuf::from(directory),
            None => home_directory()?.join(".pi/agent"),
        };
        Ok(directory.join("settings.json"))
    }
}

fn pi_extension_asset(assets: &Path, executable: &Path) -> Result<(PathBuf, String), String> {
    let source = include_str!("../pi/index.ts").replace(
        "\"__ACYCLIC_EXECUTABLE__\"",
        &serde_json::to_string(executable).map_err(display)?,
    );
    // Content-addressing lets settings retain a pinned asset across upgrades.
    // A preexisting different asset is corruption, never something to overwrite.
    let extension = assets.join(format!("{}.ts", blake3::hash(source.as_bytes()).to_hex()));
    Ok((extension, source))
}

fn verify_pi_extension_asset(extension: &Path, source: &str) -> Result<(), String> {
    let mut installed_bytes = Vec::new();
    fs::File::open(extension)
        .and_then(|file| {
            file.take(source.len() as u64 + 1)
                .read_to_end(&mut installed_bytes)
        })
        .map_err(|error| {
            format!(
                "cannot read Pi extension asset at {}: {error}",
                extension.display()
            )
        })?;
    if installed_bytes != source.as_bytes() {
        return Err(format!(
            "Pi extension asset at {} differs from its content identity",
            extension.display()
        ));
    }
    Ok(())
}

pub(crate) fn install_pi_at(
    settings: &Path,
    assets: &Path,
    executable: &Path,
) -> Result<(), String> {
    let (extension, source) = pi_extension_asset(assets, executable)?;
    fs::create_dir_all(assets).map_err(display)?;
    let temporary = assets.join(format!("{}.next", uuid::Uuid::new_v4()));
    match fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
    {
        Ok(mut file) => {
            let publish = (|| {
                file.write_all(source.as_bytes())?;
                sync_file(&file, Durability::Full)?;
                drop(file);
                durable_rename(&temporary, &extension, RenameMode::NoReplace)
            })();
            let _ = fs::remove_file(&temporary);
            if let Err(error) = publish
                && error.kind() != io::ErrorKind::AlreadyExists
            {
                return Err(display(error));
            }
        }
        Err(error) => return Err(display(error)),
    }
    verify_pi_extension_asset(&extension, &source)?;
    install_owned_json(settings, "pi-extension", "Pi settings", |prior| {
        let mut document = prior.cloned().unwrap_or_else(|| json!({}));
        let object = document
            .as_object_mut()
            .ok_or_else(|| "Pi settings must contain an object".to_owned())?;
        let extensions = object
            .entry("extensions")
            .or_insert_with(|| json!([]))
            .as_array_mut()
            .ok_or_else(|| "Pi extensions must be an array".to_owned())?;
        let path = serde_json::to_value(&extension).map_err(display)?;
        if !extensions.contains(&path) {
            extensions.push(path);
        }
        Ok(document)
    })
}

pub(crate) fn pi_installation_check() -> Value {
    let inspect = || -> Result<bool, String> {
        let path = pi_settings_path(false)?;
        let Some(ownership) = read_mcp_ownership(&mcp_ownership_path(&path, "pi-extension"))?
        else {
            return Ok(false);
        };
        if ownership.version != 1
            || ownership.config_path != path
            || ownership.section != "pi-extension"
            || read_optional_json(&path, "Pi settings")?.as_ref() != Some(&ownership.installed)
        {
            return Err("Pi settings differ from the owned installation".to_owned());
        }
        let (extension, source) = pi_extension_asset(
            &default_data_directory().join("pi-extension"),
            &current_executable()?,
        )?;
        let selected = serde_json::to_value(&extension).map_err(display)?;
        if !ownership
            .installed
            .get("extensions")
            .and_then(Value::as_array)
            .is_some_and(|extensions| extensions.contains(&selected))
        {
            return Err(format!(
                "Pi settings do not reference the pinned extension at {}",
                extension.display()
            ));
        }
        verify_pi_extension_asset(&extension, &source)?;
        Ok(true)
    };
    match inspect() {
        Ok(true) => doctor_check(
            "pi-install",
            "warn",
            "Pi diagnostics extension installed; recursive execution and platform qualification pending",
        ),
        Ok(false) => doctor_check(
            "pi-install",
            "warn",
            "no owned per-user Pi extension installation",
        ),
        Err(error) => doctor_check("pi-install", "fail", error),
    }
}

#[cfg(test)]
mod binding_tests {
    use super::*;

    fn binding(session: &str, identity: u8) -> PiTaskBinding {
        let provider = ProviderRef::new("acyclic", "filesystem", "1").unwrap();
        let agent_id = AgentId::from_bytes([identity; 16]);
        PiTaskBinding {
            native_session_id: session.to_owned(),
            task_id: TaskId::from_bytes([identity; 16]),
            agent_id,
            project_volumes: BTreeMap::new(),
            scratch: PiScratchBinding {
                volume: VolumeRef::new(
                    provider.clone(),
                    "scratch",
                    VolumeClass::AgentPrivate,
                    VolumeOwner::Agent(agent_id),
                )
                .unwrap(),
                allocation: PiScratchAllocation::Created(PiScratchCreation {
                    workspace_id: [identity; 16],
                    generation: GenerationRef::new(provider, [3; 32], None).unwrap(),
                }),
            },
        }
    }

    #[test]
    fn roles_providers_and_original_creation_are_checked() {
        let original = binding("native-session", 1);
        original
            .validate(Some("native-session"), vec![], false)
            .unwrap();
        assert!(
            original
                .validate(Some("other-session"), vec![], false)
                .is_err()
        );
        assert!(
            original
                .validate(None, vec!["missing-root"], false)
                .is_err()
        );
        let mut changed = original.clone();
        changed.scratch.allocation = PiScratchAllocation::Preparing;
        assert!(changed.validate(None, vec![], false).is_err());
        changed.validate(None, vec![], true).unwrap();
        let mut changed = original.clone();
        changed.scratch.volume = VolumeRef::new(
            original.scratch.volume.provider().clone(),
            "scratch",
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([2; 16])),
        )
        .unwrap();
        assert!(changed.validate(None, vec![], false).is_err());
        let mut changed = original.clone();
        let PiScratchAllocation::Created(creation) = &mut changed.scratch.allocation else {
            panic!("fixture must have a created scratch volume");
        };
        creation.generation = GenerationRef::new(
            ProviderRef::new("other", "filesystem", "1").unwrap(),
            [3; 32],
            None,
        )
        .unwrap();
        assert!(changed.validate(None, vec![], false).is_err());
        let mut changed = original;
        let PiScratchAllocation::Created(creation) = &mut changed.scratch.allocation else {
            panic!("fixture must have a created scratch volume");
        };
        creation.workspace_id = [0; 16];
        assert!(changed.validate(None, vec![], false).is_err());
    }

    #[test]
    fn scratch_allocation_requires_an_explicit_current_variant() {
        let original = binding("native-session", 1);
        let current = serde_json::to_value(&original).unwrap();
        assert_eq!(
            current.pointer("/scratch/allocation/state"),
            Some(&json!("created"))
        );
        let mut preparing = original;
        preparing.scratch.allocation = PiScratchAllocation::Preparing;
        let prepared = serde_json::to_value(&preparing).unwrap();
        let decoded: PiTaskBinding = serde_json::from_value(prepared).unwrap();
        decoded.validate(None, vec![], true).unwrap();
        assert!(decoded.validate(None, vec![], false).is_err());
        for replacement in [
            serde_json::Value::Null,
            json!({}),
            json!({"state":"unknown"}),
            json!({"state":"created"}),
        ] {
            let mut invalid = current.clone();
            *invalid.pointer_mut("/scratch/allocation").unwrap() = replacement;
            assert!(serde_json::from_value::<PiTaskBinding>(invalid).is_err());
        }
        let mut omitted = current;
        omitted
            .get_mut("scratch")
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove("allocation");
        assert!(serde_json::from_value::<PiTaskBinding>(omitted).is_err());
    }

    #[test]
    fn project_selection_rejects_aliases_and_non_project_roles() {
        let mut selected = binding("native-session", 1);
        let project = VolumeRef::new(
            selected.scratch.volume.provider().clone(),
            "project",
            VolumeClass::Project,
            VolumeOwner::Project("project-owner".to_owned()),
        )
        .unwrap();
        selected
            .project_volumes
            .insert("a".to_owned(), project.clone());
        selected.validate(None, vec!["a"], false).unwrap();
        selected.project_volumes.insert("b".to_owned(), project);
        assert!(selected.validate(None, vec!["a", "b"], false).is_err());
        selected.project_volumes.remove("b");
        selected
            .project_volumes
            .insert("a".to_owned(), selected.scratch.volume.clone());
        assert!(selected.validate(None, vec!["a"], false).is_err());
    }

    #[test]
    fn retained_bindings_reject_mixed_hosts_and_duplicate_task_identities() {
        let root = binding("root-session", 1);
        let mut state = AdapterState {
            task_binding: PluginTaskBinding::Pi(Box::new(root.clone())),
            root_session_id: "root-session".to_owned(),
            root_agent_id: "root:root-session".to_owned(),
            ..AdapterState::default()
        };
        validate_pi_bindings(&state).unwrap();
        let route = Route {
            task_binding: PluginTaskBinding::HostManaged,
            agent_id: "child-session".to_owned(),
            turn_id: "turn".to_owned(),
            context_id: [2; 16],
            root_id: [2; 16],
            parent_agent_id: state.root_agent_id.clone(),
            roots: BTreeMap::new(),
            mount_path: PathBuf::new(),
            lifecycle: RouteLifecycle::Active,
        };
        state.routes.insert("child-session".to_owned(), route);
        assert!(validate_pi_bindings(&state).is_err());
        let mut child = binding("child-session", 2);
        state.routes.get_mut("child-session").unwrap().task_binding =
            PluginTaskBinding::Pi(Box::new(child.clone()));
        validate_pi_bindings(&state).unwrap();
        child.task_id = root.task_id;
        state.routes.get_mut("child-session").unwrap().task_binding =
            PluginTaskBinding::Pi(Box::new(child));
        assert!(validate_pi_bindings(&state).is_err());
        state.task_binding = PluginTaskBinding::HostManaged;
        assert!(validate_pi_bindings(&state).is_err());
    }
}
