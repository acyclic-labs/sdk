//! Session catalog and durable state.

use super::*;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct DiscardWorkspace {
    pub(crate) name: String,
    pub(crate) delete_key: [u8; 16],
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct DiscardAgent {
    pub(crate) agent_id: String,
    pub(crate) path: PathBuf,
    pub(crate) repository_workspace_ids: Vec<[u8; 16]>,
    pub(crate) mount_detached: bool,
    pub(crate) workspaces: VecDeque<DiscardWorkspace>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct PendingDiscard {
    pub(crate) parent_context_id: [u8; 16],
    pub(crate) child_context_id: [u8; 16],
    pub(crate) context_discarded: bool,
    pub(crate) agents: VecDeque<DiscardAgent>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct AdapterState {
    pub(crate) task_binding: PluginTaskBinding,
    pub(crate) version: u32,
    pub(crate) root_session_id: String,
    pub(crate) active: bool,
    pub(crate) root_agent_id: String,
    pub(crate) root_turns: BTreeSet<String>,
    pub(crate) root_context_id: [u8; 16],
    pub(crate) root_id: [u8; 16],
    pub(crate) roots: BTreeMap<String, RootBinding>,
    pub(crate) pending_root_registration: bool,
    pub(crate) pending_root_adoptions: BTreeSet<String>,
    pub(crate) routes: BTreeMap<String, Route>,
    pub(crate) turns: BTreeMap<String, String>,
    pub(crate) pending: VecDeque<PendingSpawn>,
    pub(crate) leases: BTreeMap<String, LeaseRecord>,
    pub(crate) pending_discards: BTreeMap<String, PendingDiscard>,
}

impl AdapterState {
    pub(crate) fn require_created_roots(&self) -> Result<(), String> {
            for root in self.roots.values() {
                root.created()?;
            }
            Ok(())
        }

}

impl ControlPlane {
    pub(crate) fn persist(&mut self) -> Result<(), String> {
            // A save that survives a power loss survives it with every authority
            // write it may rest on.
            self.fs.flush_deferred_authority().map_err(display)?;
            self.slots
                .save(&self.data, &self.state, Survives::PowerLoss)?;
            // A flushed save is a whole snapshot, so it covers any unflushed one.
            self.unflushed = false;
            Ok(())
        }

    /// Saves the last transition of a request without flushing it; see
        /// [`Survives::ServiceCrash`].
        pub(crate) fn persist_unflushed(&mut self) -> Result<(), String> {
            self.slots
                .save(&self.data, &self.state, Survives::ServiceCrash)?;
            self.unflushed = true;
            Ok(())
        }

    /// Flushes an unflushed save and the authority writes of the requests
        /// before it. Every request starts here, so nothing ever acts on a
        /// transition that a power loss could still undo.
        pub(crate) async fn make_durable(&mut self) -> Result<(), String> {
            let fs = self.fs.clone();
            tokio::task::spawn_blocking(move || fs.flush_deferred_authority())
                .await
                .map_err(display)?
                .map_err(display)?;
            if !self.unflushed {
                return Ok(());
            }
            #[cfg(test)]
            if std::mem::take(&mut self.fail_next_flush) {
                return Err("injected adapter-state flush failure".to_owned());
            }
            let data = self.data.clone();
            let mut slots = self.slots;
            self.slots = tokio::task::spawn_blocking(move || {
                slots.flush_unflushed(&data)?;
                Ok::<_, String>(slots)
            })
            .await
            .map_err(display)??;
            self.unflushed = false;
            Ok(())
        }

}
