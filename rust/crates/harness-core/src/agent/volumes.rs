//! Spawned volumes: each child's volumes, created in the parent step's one commit.

use super::{AgentState, Error, Out};
use acyclic_actors::guest::Composition;

/// Binds the volumes of every child a step spawns and joins their creation to
/// the step's commit. Bindings are a pure function of the parent and child, so
/// a replayed step names the same volumes; the returned participant forks or
/// creates them inside the commit that adopts the children, all-or-none.
pub trait SpawnVolumes: acyclic_stream::ProviderPlatform + 'static {
    /// Fills the volumes of each `Out::Spawn` in `outs`, and returns the
    /// participant that creates them, or `None` when `outs` spawns nothing.
    fn bind(
        &self,
        parent: &AgentState,
        outs: &mut [Out],
    ) -> Result<Option<Box<dyn Composition>>, Error>;
}
