//! Lazy, immutable measurements of one returned child wait.
use super::{AgentId, ChildWake, Children, Error};
use acyclic_stream::StreamProvider;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// One returned wait's immutable measurement cut. Checkpoints and statuses
/// retain a root and count, never one inline measurement per child.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
#[serde(deny_unknown_fields)]
pub struct ChildWakes {
    #[cfg_attr(
        feature = "wasm",
        tsify(type = "{ root: number[] | null; count: number }")
    )]
    settlements: Children,
    woke_at_micros: u64,
}

impl ChildWakes {
    /// Bind the original completion cut to the clock where its wait returned.
    #[must_use]
    pub fn new(settlements: Children, woke_at_micros: u64) -> Self {
        Self {
            settlements,
            woke_at_micros,
        }
    }

    /// Number of observed children, without loading a measurement.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.settlements.len()
    }

    /// Whether this status reports a returned child wait.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.settlements.is_empty()
    }

    /// Read at most `limit` original measurements after `after`. Continue
    /// from the last returned child; neither the checkpoint nor a page scans
    /// the whole wait. The provider must be the owner of the status stream.
    pub async fn page<P: StreamProvider + ?Sized>(
        &self,
        provider: Arc<P>,
        after: Option<&AgentId>,
        limit: usize,
    ) -> Result<Vec<ChildWake>, Error> {
        let mut settlements = self.settlements.clone();
        settlements.bind(provider);
        settlements
            .entries_page(after, limit)
            .await?
            .into_iter()
            .map(|(child, settled_at)| {
                Ok(ChildWake {
                    child,
                    settled_at_micros: settled_at
                        .ok_or(Error::Record("child wake has no original completion time"))?,
                    woke_at_micros: self.woke_at_micros,
                })
            })
            .collect()
    }

    pub(super) async fn persist<P: StreamProvider + ?Sized>(
        &mut self,
        provider: Arc<P>,
    ) -> Result<(), Error> {
        self.settlements.bind(provider);
        self.settlements.persist().await
    }
}
