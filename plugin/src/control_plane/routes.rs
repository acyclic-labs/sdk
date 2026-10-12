//! Retained routes and child preparations.

use super::*;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct RouteRoot {
    pub(crate) root_id: [u8; 16],
    pub(crate) repository_workspace_id: [u8; 16],
    pub(crate) published_generation: [u8; 32],
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Route {
    pub(crate) task_binding: PluginTaskBinding,
    pub(crate) agent_id: String,
    pub(crate) turn_id: String,
    pub(crate) context_id: [u8; 16],
    pub(crate) root_id: [u8; 16],
    pub(crate) parent_agent_id: String,
    pub(crate) roots: BTreeMap<String, RouteRoot>,
    pub(crate) mount_path: PathBuf,
    pub(crate) lifecycle: RouteLifecycle,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RouteLifecycle {
    #[default]
    Mounting,
    Active,
    StopRequested,
    Frozen,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct PendingSpawn {
    pub(crate) task_binding: PluginTaskBinding,
    /// Pi's original user alias is known before admission. Native Codex/Claude
    /// hosts assign their agent identity only during the startup handshake.
    pub(crate) agent_id: Option<String>,
    pub(crate) parent_agent_id: String,
    pub(crate) tool_use_id: String,
    pub(crate) active_root_id: Option<[u8; 16]>,
    pub(crate) expires_at_millis: u64,
    pub(crate) workspace_name: String,
    pub(crate) fork_key: [u8; 16],
    pub(crate) roots: BTreeMap<String, RouteRoot>,
    pub(crate) mount_path: PathBuf,
    pub(crate) lifecycle: PendingSpawnLifecycle,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PendingSpawnLifecycle {
    #[default]
    Preparing,
    Mounting,
    Prepared,
    Discarding,
}

impl RouteRoot {
    pub(crate) fn id(&self) -> WorkspaceRootId {
            WorkspaceRootId::from_bytes(self.root_id)
        }

    pub(crate) fn mount_name(&self) -> String {
            route_name(self.id())
        }

}

impl Route {
    pub(crate) fn active_path(&self) -> Result<PathBuf, String> {
            let active = self
                .roots
                .get(&root_key(WorkspaceRootId::from_bytes(self.root_id)))
                .ok_or_else(|| "persisted route is missing its active root".to_owned())?;
            Ok(self.mount_path.join(active.mount_name()))
        }

}

impl RouteLifecycle {
    pub(crate) fn accepts_tools(self) -> bool {
            self == Self::Active
        }

    pub(crate) fn needs_mount(self) -> bool {
            self != Self::Frozen
        }

    pub(crate) fn is_frozen(self) -> bool {
            self == Self::Frozen
        }

}

impl PendingSpawn {
    pub(crate) fn context_id(&self) -> WorkspaceContextId {
            WorkspaceContextId::from_bytes(self.fork_key)
        }

    pub(crate) fn mount_name(&self) -> String {
            compact_id(&self.fork_key)
        }

}
