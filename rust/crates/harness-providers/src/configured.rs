//! The native provider and harness composition used by every local host.
use crate::{
    AlloySelection, AlloySpec, AlloyTurn, ModelFeatures, Registry, ResolvedAlloy, ResolvedModel,
};
use acyclic_harness::{
    Error, OperationId, Result,
    agent::AgentProfilePin,
    approvals::ApprovalPolicy,
    local::LocalHarnessConfig,
    model::{Model, ModelProvider},
    preset::Preset,
    profile::{ModelFiles, ModelProfile, ProfileModels, Profiles, ProjectLayer, RoleFile, models},
};
use std::{
    collections::HashMap,
    path::{Component, Path},
    sync::Arc,
};

const EMPTY: &str = "---\nversion: 1\n---\n";
const RENDERER: &str = "provider-v1";
const ROLES: [(&str, &str); 4] = [
    (
        "orchestrator",
        include_str!("../../harness-core/src/profile/roles/orchestrator.md"),
    ),
    (
        "advisor",
        include_str!("../../harness-core/src/profile/roles/advisor.md"),
    ),
    (
        "worker",
        include_str!("../../harness-core/src/profile/roles/worker.md"),
    ),
    (
        "reviewer",
        include_str!("../../harness-core/src/profile/roles/reviewer.md"),
    ),
];

struct Providers {
    single: HashMap<String, Arc<ResolvedModel>>,
    alloys: HashMap<String, ResolvedAlloy>,
}

impl ProfileModels for Providers {
    fn select(
        &self,
        profile: &AgentProfilePin,
        operation: OperationId,
        ordinal: u64,
    ) -> Result<(Model, Arc<dyn ModelProvider>)> {
        let provider = if let Some(alloy) = self.alloys.get(&profile.model) {
            alloy
                .select(AlloyTurn {
                    operation_id: operation,
                    ordinal,
                    role: &profile.role,
                })
                .clone()
        } else {
            self.single
                .get(&profile.model)
                .cloned()
                .ok_or_else(|| crate::invalid("role model has no admitted provider"))?
        };
        Ok((provider.model().clone(), provider))
    }
}

impl Registry {
    /// Binds the single native harness composition. Ordinary files under
    /// `.harness` select roles, model profiles, project capacity and endpoints;
    /// the caller supplies only credentials and the owner's approval policy.
    pub fn configure(
        mut self,
        workspace: &Path,
        policy: Option<ApprovalPolicy>,
    ) -> Result<LocalHarnessConfig> {
        let root = workspace.join(".harness");
        if let Some(catalog) = read_optional(&root.join("providers.json"))? {
            self.load_catalog_bytes(catalog.as_bytes())?;
        }
        let mut roles = Vec::with_capacity(ROLES.len());
        for (name, builtin) in ROLES {
            let source = read_optional(&root.join("roles").join(format!("{name}.md")))?
                .unwrap_or_else(|| builtin.into());
            let role = RoleFile::parse(&source)?;
            if role.name() != name {
                return Err(crate::invalid("role file identity differs from its path"));
            }
            roles.push(role);
        }
        let mut files = HashMap::new();
        for role in &roles {
            load_files(&root, role.model(), &mut files)?;
        }
        let mut providers = Providers {
            single: HashMap::new(),
            alloys: HashMap::new(),
        };
        let mut admitted = Vec::<ModelProfile>::with_capacity(files.len());
        for (key, model) in files {
            let capacity = if let Some(members) = model.members() {
                let alloy = self.resolve_alloy(
                    AlloySpec {
                        models: members.to_vec(),
                        select: AlloySelection::RoundRobin,
                    },
                    ModelFeatures {
                        tools: true,
                        images: false,
                    },
                )?;
                let capacity = alloy.capacity();
                providers.alloys.insert(key, alloy);
                capacity
            } else {
                let provider = Arc::new(self.resolve(&key)?);
                let capacity = provider.context_capacity(provider.model())?;
                providers.single.insert(key, provider);
                capacity
            };
            admitted.push(model.admit(capacity, RENDERER)?);
        }
        let mut preset = Preset::graphcoder();
        if let Some(policy) = policy {
            preset.approval_policy = policy;
        }
        let project = read_optional(&root.join("profile.md"))?.unwrap_or_else(|| EMPTY.into());
        let profiles = Arc::new(Profiles::new(
            admitted,
            roles,
            ProjectLayer::parse(&project)?,
            EMPTY,
            preset.approval_policy.bind()?,
        )?);
        let mut config = LocalHarnessConfig::configured(workspace, profiles, Arc::new(providers))?
            .preset(&preset);
        for selection in preset.approval_policy.models() {
            let provider = Arc::new(self.resolve(selection)?);
            config = config.approval_model(selection, (provider.model().clone(), provider));
        }
        config.working_directory(&std::env::current_dir().map_err(Error::process_io)?)
    }
}

fn read_optional(path: &Path) -> Result<Option<String>> {
    match std::fs::read_to_string(path) {
        Ok(source) => Ok(Some(source)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(Error::process_io(error)),
    }
}

fn load_files(root: &Path, key: &str, files: &mut HashMap<String, ModelFiles>) -> Result<()> {
    if files.contains_key(key) {
        return Ok(());
    }
    let (family, name) = key
        .split_once('/')
        .ok_or_else(|| crate::invalid("profile selection requires family/model"))?;
    if [family, name].iter().any(|part| {
        part.is_empty()
            || Path::new(part)
                .components()
                .any(|component| !matches!(component, Component::Normal(_)))
    }) {
        return Err(crate::invalid(
            "profile identity must remain under its models directory",
        ));
    }
    let builtin = models::sources(key).ok();
    let family_source = read_optional(&root.join("models").join(family).join("_family.md"))?
        .or_else(|| builtin.map(|(family, _)| family.into()))
        .ok_or_else(|| crate::invalid("model family profile is absent"))?;
    let model_source = read_optional(&root.join("models").join(family).join(format!("{name}.md")))?
        .or_else(|| builtin.map(|(_, model)| model.into()))
        .ok_or_else(|| crate::invalid("model profile is absent"))?;
    let model = ModelFiles::parse(&family_source, &model_source)?;
    if model.key() != key {
        return Err(crate::invalid(
            "model profile identity differs from its path",
        ));
    }
    let members = model.members().map(<[String]>::to_vec).unwrap_or_default();
    files.insert(key.into(), model);
    for member in members {
        load_files(root, &member, files)?;
        if files[&member].members().is_some() {
            return Err(crate::invalid("nested alloy profiles are not supported"));
        }
    }
    Ok(())
}
