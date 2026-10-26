//! The marginal resource oracle shared by harness and native-host tests.
use super::ProcessResources;
use std::io;

/// A per-agent quantity in thousandths.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct PerAgent(pub u64);

impl PerAgent {
    /// Divides one measured total over its agents, rounding up so positive
    /// growth can never disappear into a zero-resource bound.
    pub fn of(total: u64, agents: u64) -> Self {
        let units = (u128::from(total) * 1000).div_ceil(u128::from(agents.max(1)));
        Self(u64::try_from(units).unwrap_or(u64::MAX))
    }

    /// The cost of each additional agent; process constants cancel.
    pub fn marginal(small: u64, large: u64, additional: u64) -> Self {
        Self::of(large.saturating_sub(small), additional)
    }

    /// An integral ceiling in the same units.
    pub const fn whole(value: u64) -> Self {
        Self(value * 1000)
    }
}

impl std::fmt::Display for PerAgent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}.{}", self.0 / 1000, self.0 % 1000 / 100)
    }
}

/// The harness's retained-descriptor ratchet, including Windows watch handles.
#[cfg(not(windows))]
pub const IDLE_DESCRIPTORS: PerAgent = PerAgent::whole(1);
/// The harness's retained-descriptor ratchet, including Windows watch handles.
#[cfg(windows)]
pub const IDLE_DESCRIPTORS: PerAgent = PerAgent::whole(2);
/// Shared routing and native pools add no thread per idle view.
pub const IDLE_THREADS: PerAgent = PerAgent::whole(0);
/// A view is a namespace; it adds no native mount.
pub const VIEW_MOUNTS: PerAgent = PerAgent::whole(0);

/// Process counters at one retained-view population.
#[derive(Clone, Copy, Debug)]
pub struct ViewResources {
    /// Number of retained views, excluding the root.
    pub agents: u64,
    /// Counts of the process that owns the views, excluding host descendants.
    pub process: ProcessResources,
}

impl ViewResources {
    /// Resource slopes from N to 2N. Process constants cancel; no warm-up
    /// population, allowance or measured resource is subtracted.
    pub fn exceeded(self, larger: Self) -> io::Result<Vec<&'static str>> {
        if self.agents == 0 || larger.agents != self.agents.checked_mul(2).unwrap_or(0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "scale requires N and 2N views",
            ));
        }
        let additional = larger.agents - self.agents;
        let values = [
            (
                "idle descriptors",
                self.process.descriptors,
                larger.process.descriptors,
                IDLE_DESCRIPTORS,
            ),
            (
                "idle threads",
                self.process.threads,
                larger.process.threads,
                IDLE_THREADS,
            ),
            (
                "native mounts",
                self.process.mounts,
                larger.process.mounts,
                VIEW_MOUNTS,
            ),
        ];
        Ok(values
            .into_iter()
            .filter_map(|(name, small, large, limit)| {
                (PerAgent::marginal(small, large, additional) > limit).then_some(name)
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_view_oracle_rejects_per_child_mounts_and_threads() -> io::Result<()> {
        let small = ViewResources {
            agents: 4,
            process: ProcessResources {
                descriptors: 30,
                threads: 12,
                mounts: 7,
                ..ProcessResources::default()
            },
        };
        let shared = ViewResources {
            agents: 8,
            process: small.process,
        };
        assert!(small.exceeded(shared)?.is_empty());
        let per_child = ViewResources {
            agents: 8,
            process: ProcessResources {
                descriptors: 42,
                threads: 20,
                mounts: 11,
                ..small.process
            },
        };
        assert_eq!(
            small.exceeded(per_child)?,
            vec!["idle descriptors", "idle threads", "native mounts"]
        );
        assert!(
            small
                .exceeded(ViewResources {
                    agents: 5,
                    ..shared
                })
                .is_err()
        );
        let many = ViewResources {
            agents: 2000,
            ..small
        };
        let one_extra = ViewResources {
            agents: 4000,
            process: ProcessResources {
                threads: many.process.threads + 1,
                ..many.process
            },
        };
        assert_eq!(many.exceeded(one_extra)?, vec!["idle threads"]);
        assert_eq!(PerAgent::marginal(5, 6, 2000), PerAgent(1));
        Ok(())
    }
}
