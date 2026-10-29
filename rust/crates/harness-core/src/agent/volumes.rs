//! Spawned volumes: each child's volumes, created in the parent step's one commit.

use super::{AgentState, Error, Out};
use acyclic_actors::guest::{Composer, Participant};

/// Binds the volumes of every child a step spawns and joins their creation to
/// the step's commit. Bindings are a pure function of the parent and child, so
/// a replayed step names the same volumes; the returned participant request
/// forks or creates them inside the commit that adopts the children,
/// all-or-none. The request is plain data a guest may return: as a
/// [`Composer`], the host re-derives it from the parent's state before composing.
pub trait SpawnVolumes: Composer {
    /// Fills the volumes of each `Out::Spawn` in `outs`, and returns the
    /// participant request that creates them, or `None` when `outs` spawns nothing.
    fn bind(&self, parent: &AgentState, outs: &mut [Out]) -> Result<Option<Participant>, Error>;
}
