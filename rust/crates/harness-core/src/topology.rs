//! Versioned run-start delegation policy shared by all agent hosts.
use crate::{
    Error, Result,
    agent::{AgentPath, CreationConfig, SpawnTemplates, WorkspaceEntry},
};
use acyclic_stream::BoxProviderFuture;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Immutable policy selected before a run starts and retained in its stream.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Topology {
    /// Persisted format version, currently one.
    pub version: u32,
    /// Caller-selected spawn capability and its parameters.
    #[serde(flatten)]
    pub mode: Mode,
}

/// Run-start spawn admission capability.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "kebab-case", deny_unknown_fields)]
pub enum Mode {
    /// One agent; delegation is absent from its declarations.
    Single {
        /// Maximum open children per agent; absent means unbounded.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_children: Option<u64>,
        /// Maximum agent depth, counting the root as zero; absent means unbounded.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_depth: Option<u64>,
    },
    /// Root with a lifetime reservation for exactly this many worker slots.
    Team {
        /// Lifetime worker admissions selected by the caller.
        workers: u64,
        /// Maximum open children per agent; absent means unbounded.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_children: Option<u64>,
        /// Maximum agent depth, counting the root as zero; absent means unbounded.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_depth: Option<u64>,
    },
    /// Unbounded delegation tree.
    Swarm {
        /// Maximum open children per agent; absent means unbounded.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_children: Option<u64>,
        /// Maximum agent depth, counting the root as zero; absent means unbounded.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_depth: Option<u64>,
    },
}

impl Topology {
    /// Whether this run exposes an agent coordination tool to an agent at
    /// `depth` (the root is depth zero).
    #[must_use]
    pub fn offers_tool(&self, name: &str, depth: usize) -> bool {
        if matches!(self.mode, Mode::Single { .. }) && crate::tools::AGENT_TOOLS.contains(&name) {
            false
        } else {
            name != "spawn" || self.offers_spawn(depth)
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
            "single" => Mode::Single {
                max_children: None,
                max_depth: None,
            },
            "swarm" => Mode::Swarm {
                max_children: None,
                max_depth: None,
            },
            mode if mode.starts_with("team:") => Mode::Team {
                workers: mode
                    .strip_prefix("team:")
                    .unwrap_or_default()
                    .parse()
                    .map_err(|_| {
                        Error::Invalid("team requires a nonnegative worker count: team:N".into())
                    })?,
                max_children: None,
                max_depth: None,
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

    /// Maximum open children per agent; absent means unbounded.
    #[must_use]
    pub fn max_children(&self) -> Option<u64> {
        match &self.mode {
            Mode::Single { max_children, .. }
            | Mode::Team { max_children, .. }
            | Mode::Swarm { max_children, .. } => *max_children,
        }
    }

    /// Maximum agent depth, counting the root as zero; absent means unbounded.
    #[must_use]
    pub fn max_depth(&self) -> Option<u64> {
        match &self.mode {
            Mode::Single { max_depth, .. }
            | Mode::Team { max_depth, .. }
            | Mode::Swarm { max_depth, .. } => *max_depth,
        }
    }

    /// Whether `open` children stay within the configured width cap.
    #[must_use]
    pub fn admits_children(&self, open: usize) -> bool {
        self.max_children()
            .is_none_or(|limit| usize::try_from(limit).map_or(true, |limit| open < limit))
    }

    /// Selects the unbounded-or-explicit child and depth caps.
    #[must_use]
    pub fn with_limits(mut self, max_children: Option<u64>, max_depth: Option<u64>) -> Self {
        match &mut self.mode {
            Mode::Single {
                max_children: children,
                max_depth: depth,
            }
            | Mode::Team {
                max_children: children,
                max_depth: depth,
                ..
            }
            | Mode::Swarm {
                max_children: children,
                max_depth: depth,
            } => {
                *children = max_children;
                *depth = max_depth;
            }
        }
        self
    }

    /// Whether an agent at `depth` may spawn: false when the run's mode or
    /// depth cap puts its children out of bounds. The caller must omit the
    /// spawn tool, rather than refusing the call at run time.
    #[must_use]
    pub fn offers_spawn(&self, depth: usize) -> bool {
        let within_depth = self
            .max_depth()
            .is_none_or(|max| u64::try_from(depth).is_ok_and(|depth| depth < max));
        within_depth
            && match self.mode {
                Mode::Swarm { .. } => true,
                Mode::Team { .. } => depth == 0,
                Mode::Single { .. } => false,
            }
    }

    /// Lifetime child reservation, distinct from concurrency backpressure.
    #[must_use]
    pub fn spawn_limit(&self, root: bool) -> Option<u64> {
        match self.mode {
            Mode::Swarm { .. } => None,
            Mode::Team { workers, .. } if root => Some(workers),
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
        volumes: &'a [WorkspaceEntry],
        template: &'a str,
        ceiling: Option<&'a crate::Capabilities>,
    ) -> BoxProviderFuture<'a, Result<CreationConfig>> {
        Box::pin(async move {
            if let Some(topology) = &parent.topology
                && !topology.offers_spawn(path.depth())
            {
                return Err(Error::Invalid(
                    "the selected spawn capability does not admit children for this agent".into(),
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

    #[test]
    fn depth_cap_removes_spawn_at_and_below_the_cap()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let capped = Topology::select("swarm")?.with_limits(None, Some(2));
        assert!(capped.offers_spawn(0));
        assert!(capped.offers_spawn(1));
        assert!(!capped.offers_spawn(2));
        assert!(capped.offers_tool("spawn", 1));
        assert!(!capped.offers_tool("spawn", 2));
        assert!(capped.offers_tool("send", 2));
        let width = Topology::select("swarm")?.with_limits(Some(2), None);
        assert!(width.admits_children(1));
        assert!(!width.admits_children(2));
        assert!(Topology::select("swarm")?.admits_children(usize::MAX));
        let single = Topology::select("single")?;
        assert!(!single.offers_spawn(0));
        assert!(!single.offers_tool("spawn", 0));
        assert!(!single.offers_tool("wait", 0));
        let unbounded = Topology::select("swarm")?;
        assert!(unbounded.offers_spawn(usize::MAX));
        Ok(())
    }

    #[test]
    fn limits_default_to_unbounded_and_round_trip_as_top_level_fields()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let default: Topology = serde_json::from_value(serde_json::json!({
            "version": 1, "mode": "swarm"
        }))?;
        assert_eq!((default.max_children(), default.max_depth()), (None, None));
        let capped: Topology = serde_json::from_value(serde_json::json!({
            "version": 1, "mode": "swarm", "max_children": 2, "max_depth": 3
        }))?;
        assert_eq!(
            (capped.max_children(), capped.max_depth()),
            (Some(2), Some(3))
        );
        assert_eq!(
            serde_json::to_value(&capped)?,
            serde_json::json!({
                "version": 1, "mode": "swarm", "max_children": 2, "max_depth": 3
            })
        );
        assert!(
            serde_json::from_value::<Topology>(serde_json::json!({
                "version": 1, "mode": "swarm", "unknown": 1
            }))
            .is_err()
        );
        Ok(())
    }
}
