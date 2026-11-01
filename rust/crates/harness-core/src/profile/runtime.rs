//! The host's role profiles: one immutable snapshot shared by every turn.

use super::{RoleFile, SpawnContext, invalid};
use crate::{
    Error, Result,
    agent::{AgentProfilePin, SpawnProfiles},
    context::Context,
    model::{ModelContent, ModelMessage, ModelRole},
};
use std::{collections::HashMap, sync::Arc};

/// The role that runs every stock root agent.
pub const ROOT_ROLE: &str = "orchestrator";

const BUILTIN: [&str; 4] = [
    include_str!("roles/orchestrator.md"),
    include_str!("roles/advisor.md"),
    include_str!("roles/worker.md"),
    include_str!("roles/reviewer.md"),
];

struct Role {
    file: RoleFile,
    pin: AgentProfilePin,
    directive: Arc<Context>,
}

/// Parsed role files, each admitted with its pin and directive. Replacing a
/// file builds the next snapshot; an agent's pin then refuses this one, so a
/// role change is a new fork rather than an edit of a running agent.
pub struct Profiles {
    roles: HashMap<String, Role>,
}

impl Profiles {
    /// The built-in orchestrator, advisor, worker and reviewer roles, admitted
    /// once per process and shared by every stock host.
    #[must_use]
    pub fn builtin() -> Arc<Self> {
        static BUILTIN_PROFILES: std::sync::LazyLock<Arc<Profiles>> =
            std::sync::LazyLock::new(|| {
                let roles = BUILTIN
                    .map(RoleFile::parse)
                    .into_iter()
                    .collect::<Result<Vec<_>>>();
                // The shipped files are constants; the profile tests admit them.
                Arc::new(
                    roles
                        .and_then(Profiles::new)
                        .expect("built-in role files are admitted"),
                )
            });
        Arc::clone(&BUILTIN_PROFILES)
    }

    /// Admits `roles`: names are unique, [`ROOT_ROLE`] is top level, and every
    /// spawn target names an admitted role its parent may spawn.
    ///
    /// # Errors
    /// When a file repeats a role, names an absent or top-level spawn target,
    /// or the root role is absent or not top level.
    pub fn new(roles: impl IntoIterator<Item = RoleFile>) -> Result<Self> {
        let mut admitted = HashMap::new();
        for file in roles {
            let name = file.name().to_owned();
            let directive = Arc::new(Context {
                messages: (!file.0.body.trim().is_empty())
                    .then(|| ModelMessage {
                        role: ModelRole::System,
                        content: ModelContent::Text(file.0.body.trim().to_owned()),
                    })
                    .into_iter()
                    .collect(),
                ..Context::default()
            });
            let pin = AgentProfilePin {
                role: name.clone(),
                digest: file.0.digest,
                context: file.context(),
            };
            let role = Role {
                file,
                pin,
                directive,
            };
            if admitted.insert(name, role).is_some() {
                return Err(invalid("role profile identity is repeated"));
            }
        }
        for role in admitted.values() {
            for child in &role.file.0.metadata.may_spawn {
                let target = admitted
                    .get(child)
                    .ok_or_else(|| invalid("spawn role is absent"))?;
                role.file.admit_child(&target.file)?;
            }
        }
        let profiles = Self { roles: admitted };
        profiles.root()?;
        Ok(profiles)
    }

    /// The pin every stock root agent is created with.
    ///
    /// # Errors
    /// When the root role is absent or not top level.
    pub fn root(&self) -> Result<AgentProfilePin> {
        let role = self
            .roles
            .get(ROOT_ROLE)
            .ok_or_else(|| invalid("orchestrator role is absent"))?;
        if role.pin.context != SpawnContext::TopLevel {
            return Err(invalid("orchestrator role must be top level"));
        }
        Ok(role.pin.clone())
    }

    /// The role file `pin` names, refused when the agent was adopted under
    /// another revision of it.
    ///
    /// # Errors
    /// When the role is absent or its file changed since the pin.
    pub fn pinned(&self, pin: &AgentProfilePin) -> Result<&RoleFile> {
        self.role(pin).map(|role| &role.file)
    }

    /// The role's directive, prepended to each of the agent's model requests.
    pub(crate) fn directive(&self, pin: &AgentProfilePin) -> Result<Arc<Context>> {
        self.role(pin).map(|role| Arc::clone(&role.directive))
    }

    fn role(&self, pin: &AgentProfilePin) -> Result<&Role> {
        self.roles
            .get(&pin.role)
            .filter(|role| role.pin == *pin)
            .ok_or_else(|| invalid("agent profile differs from its pinned role file"))
    }
}

impl SpawnProfiles for Profiles {
    fn root(&self) -> Result<AgentProfilePin> {
        Profiles::root(self)
    }

    fn child(&self, parent: &AgentProfilePin, role: &str) -> Result<AgentProfilePin> {
        let parent = self.role(parent)?;
        let child = self.roles.get(role).ok_or_else(|| {
            Error::Invalid(format!(
                "{role} names no role; a {} may spawn {}",
                parent.pin.role,
                parent.file.0.metadata.may_spawn.join(", ")
            ))
        })?;
        parent.file.admit_child(&child.file).map_err(|_| {
            Error::Invalid(format!(
                "a {} may not spawn a {role}; it may spawn {}",
                parent.pin.role,
                parent.file.0.metadata.may_spawn.join(", ")
            ))
        })?;
        Ok(child.pin.clone())
    }
}
