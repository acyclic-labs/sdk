//! Versioned run-start delegation policy shared by all agent hosts.
use crate::{
    Error, Result,
    agent::{AgentPath, CreationConfig, SpawnTemplates, VolumeBinding},
};
use acyclic_stream::BoxProviderFuture;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Immutable policy selected before a run starts and retained in its stream.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Topology {
    /// Persisted format version, currently one.
    pub version: u32,
    /// Forced arrangement and its user-supplied parameters.
    #[serde(flatten)]
    pub mode: Mode,
}

/// Forced model delegation policy.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "kebab-case", deny_unknown_fields)]
pub enum Mode {
    /// One agent; delegation is absent from its declarations.
    Single,
    /// Root with a lifetime reservation for exactly this many worker slots.
    Team {
        /// Lifetime worker admissions selected by the caller.
        workers: u64,
    },
    /// Unbounded delegation tree.
    Swarm,
}

impl Topology {
    /// Immutable run instructions appended to the selected role directive.
    #[must_use]
    pub fn instruction(&self) -> String {
        match self.mode {
            Mode::Single => "Forced topology: single. Complete the task as one agent. Delegation is unavailable.".into(),
            Mode::Team { workers } => format!("Forced topology: team:{workers}. The root may delegate to at most {workers} workers total over this run. Workers complete their assigned tasks without delegation; depth is one. Assign independent work and integrate their results."),
            Mode::Swarm => "Forced topology: swarm. Delegate and coordinate work as an unrestricted tree, choosing its width and depth for the task.".into(),
        }
    }
    /// Rejects unsupported persisted formats before execution.
    pub fn validate(&self) -> Result<()> {
        if self.version != 1 {
            return Err(Error::Invalid(
                "unsupported topology version; use version 1".into(),
            ));
        }
        Ok(())
    }

    /// Parses the thin CLI selector; execution remains a harness concern.
    pub fn select(mode: &str) -> Result<Self> {
        let mode = match mode {
            "single" => Mode::Single,
            "swarm" => Mode::Swarm,
            mode if mode.starts_with("team:") => Mode::Team {
                workers: mode
                    .strip_prefix("team:")
                    .unwrap_or_default()
                    .parse()
                    .map_err(|_| {
                        Error::Invalid("team requires a nonnegative worker count: team:N".into())
                    })?,
            },
            _ => {
                return Err(Error::Invalid(
                    "topology must be single, team:N or swarm".into(),
                ));
            }
        };
        let topology = Self { version: 1, mode };
        topology.validate()?;
        Ok(topology)
    }

    /// Run-start delegation capability; never changes as slots fill.
    #[must_use]
    pub fn offers_spawn(&self, root: bool) -> bool {
        match self.mode {
            Mode::Swarm => true,
            Mode::Team { .. } => root,
            Mode::Single => false,
        }
    }

    /// Lifetime child reservation, distinct from concurrency backpressure.
    #[must_use]
    pub fn spawn_limit(&self, root: bool) -> Option<u64> {
        match self.mode {
            Mode::Swarm => None,
            Mode::Team { workers } if root => Some(workers),
            _ => Some(0),
        }
    }
}

/// Policy adapter over the one authoritative role/template selector.
pub(crate) struct Templates {
    pub inner: Arc<dyn SpawnTemplates>,
    pub topology: Topology,
}

impl SpawnTemplates for Templates {
    fn topology(&self) -> Option<&Topology> {
        Some(&self.topology)
    }

    fn child<'a>(
        &'a self,
        parent: &'a CreationConfig,
        path: &'a AgentPath,
        volumes: &'a [VolumeBinding],
        template: &'a str,
        ceiling: Option<&'a crate::Capabilities>,
    ) -> BoxProviderFuture<'a, Result<CreationConfig>> {
        Box::pin(async move {
            if let Some(topology) = &parent.topology
                && !topology.offers_spawn(path.parent().is_none())
            {
                return Err(Error::Invalid(
                    "forced topology does not admit delegation by this agent".into(),
                ));
            }
            let mut child = self
                .inner
                .child(parent, path, volumes, template, ceiling)
                .await?;
            child.topology.clone_from(&parent.topology);
            Ok(child)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::{AgentId, AgentRecord as _, ForkAdopted, InboxRecord};

    #[test]
    fn policies_survive_original_adoption_codecs_and_refuse_an_unknown_version()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        for mode in ["single", "team:0", "team:2", "swarm"] {
            let topology = Topology::select(mode)?;
            let record = InboxRecord::ForkAdopted(Box::new(ForkAdopted {
                agent: AgentId::from_bytes([31; 16]),
                parent: None,
                profile: Some(crate::templates::root_with_topology(Some(&topology))?),
                forked_at: 0,
                conversation_tail: 0,
                checkpoint: None,
                volumes: Vec::new(),
                brief: None,
                policy: None,
            }));
            assert_eq!(InboxRecord::decode(&record.encode()?)?, record);
            let mut invalid = record;
            let InboxRecord::ForkAdopted(adoption) = &mut invalid else {
                unreachable!()
            };
            adoption
                .profile
                .as_mut()
                .unwrap()
                .topology
                .as_mut()
                .unwrap()
                .version = 2;
            assert!(
                invalid.encode().is_err(),
                "unsupported topology reached committed adoption"
            );
        }
        Ok(())
    }
}
