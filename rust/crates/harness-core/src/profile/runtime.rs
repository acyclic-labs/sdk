//! An immutable epoch's profile files, shared by every executing actor.

use super::*;
use std::collections::HashMap;
use std::sync::Arc;

/// The original provider registry selects a role's admitted model at its
/// durable turn identity. Alloy member selection uses this same boundary.
pub trait ProfileModels: acyclic_stream::ProviderPlatform + 'static {
    /// Returns an already admitted provider; never creates a second model loop.
    fn select(
        &self,
        profile: &AgentProfilePin,
        operation: crate::OperationId,
        ordinal: u64,
    ) -> Result<(crate::model::Model, Arc<dyn crate::model::ModelProvider>)>;
}

/// Parsed profile files and their original owner policy. File discovery and
/// provider admission precede construction; no actor performs a directory scan.
/// Replacing files constructs the next epoch rather than editing this snapshot.
pub struct Profiles {
    models: HashMap<String, ModelProfile>,
    roles: HashMap<String, RoleFile>,
    pins: HashMap<String, AgentProfilePin>,
    resolved: HashMap<String, HashMap<String, Resolved>>,
    project: ProjectLayer,
    agent: File<Override>,
    approvals: ApprovalGate,
}

struct Resolved {
    identities: ([u8; 32], [u8; 32]),
    tools: HashMap<String, (String, bool)>,
}

impl Resolved {
    fn new(profile: &AgentProfile<'_>) -> Self {
        Self {
            identities: (profile.digest(), profile.prefix_digest()),
            tools: profile
                .prefix()
                .tools
                .iter()
                .map(|tool| {
                    let available = profile.overlay().allowed_tools.is_none_or(|allowed| {
                        allowed
                            .iter()
                            .any(|capability| capability == &tool.capability)
                    });
                    (tool.variant.clone(), (tool.revision.clone(), available))
                })
                .collect(),
        }
    }
}

impl Profiles {
    /// Retains already admitted model files and the versioned role files.
    /// Every role must resolve here, before any root or child can be created.
    pub fn new(
        models: impl IntoIterator<Item = ModelProfile>,
        roles: impl IntoIterator<Item = RoleFile>,
        project: ProjectLayer,
        agent: &str,
        approvals: ApprovalGate,
    ) -> Result<Self> {
        let mut selected = Self {
            models: HashMap::new(),
            roles: HashMap::new(),
            pins: HashMap::new(),
            resolved: HashMap::new(),
            project,
            agent: File::<Override>::parse(agent)?,
            approvals,
        };
        version(selected.agent.metadata.version)?;
        for model in models {
            let key = model
                .model
                .metadata
                .key
                .as_ref()
                .ok_or_else(|| invalid("model key is absent"))?;
            if selected.models.contains_key(key) {
                return Err(invalid("model profile identity is repeated"));
            }
            selected.models.insert(key.clone(), model);
        }
        for role in roles {
            if selected.roles.contains_key(&role.0.metadata.name) {
                return Err(invalid("role profile identity is repeated"));
            }
            selected.roles.insert(role.0.metadata.name.clone(), role);
        }
        for role in selected.roles.values() {
            let profile = selected.resolve(&role.0.metadata.name)?;
            let mut resolved = HashMap::new();
            resolved.insert(role.model().to_owned(), Resolved::new(&profile));
            let mut pin = AgentProfilePin {
                role: role.0.metadata.name.clone(),
                model: role.model().into(),
                digest: profile.digest(),
                context: role.context(),
            };
            if let Some(members) = selected.models[role.model()].members() {
                let mut digest = blake3::Hasher::new();
                digest.update(b"acyclic.profile-alloy.v1");
                digest.update(&pin.digest);
                for member in members {
                    let model = selected
                        .models
                        .get(member)
                        .ok_or_else(|| invalid("alloy member profile is absent"))?;
                    if model.members().is_some() {
                        return Err(invalid("nested alloy profiles are not supported"));
                    }
                    let member_profile = resolve_layers(
                        model,
                        role,
                        &selected.project,
                        &selected.agent,
                        &selected.approvals,
                    )?;
                    digest.update(&member_profile.digest());
                    resolved.insert(member.clone(), Resolved::new(&member_profile));
                }
                pin.digest = *digest.finalize().as_bytes();
            }
            for child in &role.0.metadata.may_spawn {
                let target = selected
                    .roles
                    .get(child)
                    .ok_or_else(|| invalid("spawn role is absent"))?;
                role.admit_child(target)?;
            }
            selected.pins.insert(pin.role.clone(), pin);
            selected.resolved.insert(role.name().to_owned(), resolved);
        }
        Ok(selected)
    }

    /// Resolves by immutable file identity, independent of history and agents.
    pub fn resolve(&self, role: &str) -> Result<AgentProfile<'_>> {
        let role = self
            .roles
            .get(role)
            .ok_or_else(|| invalid("role is absent"))?;
        let model = self
            .models
            .get(role.model())
            .ok_or_else(|| invalid("role model has no admitted profile"))?;
        let identities = self
            .resolved
            .get(role.name())
            .and_then(|models| models.get(role.model()))
            .map(|resolved| resolved.identities);
        resolve_cached(
            model,
            role,
            &self.project,
            &self.agent,
            &self.approvals,
            identities,
        )
    }

    /// The default top-level role is data in the orchestrator file. It cannot
    /// be replaced by a caller's tool list or a raw model selection.
    pub fn root(&self) -> Result<AgentProfilePin> {
        let pin = self.pin("orchestrator")?;
        if pin.context != SpawnContext::TopLevel {
            return Err(invalid("orchestrator role must be top level"));
        }
        Ok(pin)
    }

    /// Validates retained custody before using a pin from an actor checkpoint.
    pub fn pinned(&self, pin: &AgentProfilePin) -> Result<AgentProfile<'_>> {
        self.check_pin(pin)?;
        self.resolve(&pin.role)
    }

    /// Selects the member's own immutable prefix after the original provider
    /// registry chooses it at the durable turn. Foreign reasoning projection
    /// belongs to the renderer; this never edits conversation history.
    pub fn for_model(&self, pin: &AgentProfilePin, key: &str) -> Result<AgentProfile<'_>> {
        self.check_pin(pin)?;
        let role = &self.roles[&pin.role];
        let logical = &self.models[role.model()];
        let _ = logical;
        let resolved = self
            .resolved
            .get(role.name())
            .and_then(|models| models.get(key))
            .ok_or_else(|| invalid("selected model is outside the pinned role profile"))?;
        let model = self
            .models
            .get(key)
            .ok_or_else(|| invalid("selected member profile is absent"))?;
        resolve_cached(
            model,
            role,
            &self.project,
            &self.agent,
            &self.approvals,
            Some(resolved.identities),
        )
    }

    /// Constant-time availability lookup over declarations admitted once at
    /// epoch construction. This is additional policy, never tool authority.
    pub fn tool_available(
        &self,
        pin: &AgentProfilePin,
        model: &str,
        name: &str,
        revision: &str,
    ) -> Result<bool> {
        self.check_pin(pin)?;
        let resolved = self
            .resolved
            .get(&pin.role)
            .and_then(|models| models.get(model))
            .ok_or_else(|| invalid("selected model is outside the pinned role profile"))?;
        Ok(resolved
            .tools
            .get(name)
            .is_some_and(|(expected, available)| expected == revision && *available))
    }

    fn check_pin(&self, pin: &AgentProfilePin) -> Result<()> {
        if self.pins.get(&pin.role) != Some(pin) {
            return Err(invalid("actor profile differs from its pinned epoch"));
        }
        Ok(())
    }

    fn pin(&self, name: &str) -> Result<AgentProfilePin> {
        self.pins
            .get(name)
            .cloned()
            .ok_or_else(|| invalid("role is absent"))
    }
}

impl SpawnProfiles for Profiles {
    fn child(&self, parent: &AgentProfilePin, role: &str) -> Result<AgentProfilePin> {
        self.check_pin(parent)?;
        let parent = self
            .roles
            .get(&parent.role)
            .ok_or_else(|| invalid("parent role is absent"))?;
        let child = self
            .roles
            .get(role)
            .ok_or_else(|| invalid("child role is absent"))?;
        parent.admit_child(child)?;
        self.pin(role)
    }
}
