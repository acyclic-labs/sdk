//! Spawned volumes: each child's volumes, created in the parent step's one commit.

use super::{AgentPath, AgentState, Error, Out, VolumeBinding};
use crate::conversation::FileRef;
use acyclic_actors::guest::Composition;
use acyclic_stream::BoxProviderFuture;

/// Binds the volumes of every child a step spawns and joins their creation to
/// the step's commit. Bindings are a pure function of the parent and child, so
/// a replayed step names the same volumes; the returned participant forks or
/// creates them inside the commit that adopts the children, all-or-none.
pub trait SpawnVolumes: acyclic_stream::ProviderPlatform + 'static {
    /// Fills the volumes of each `Out::Spawn` in `outs`, and returns the
    /// participant that creates them, or `None` when `outs` spawns nothing.
    /// A spawn's brief is created inside the child's private volume.
    fn bind(
        &self,
        parent: &AgentState,
        outs: &mut [Out],
    ) -> Result<Option<Box<dyn Composition>>, Error>;

    /// The brief `child`'s private volume was created holding, if any, staged
    /// as the child's own retained file.
    ///
    /// The creating generation exists only once the spawn commit seals, after
    /// the adoption record is written, and retention may later retire it; the
    /// child stages its brief at adoption instead, under one operation per
    /// child, so every replay names the same file.
    fn brief<'a>(
        &'a self,
        child: &'a AgentPath,
        volumes: &'a [VolumeBinding],
    ) -> BoxProviderFuture<'a, Result<Option<FileRef>, Error>>;
}
