//! Spawn names resolve against immutable, terminal setup failures. No index or
//! generation counter is persisted: the recorded intent is the replay identity.

use super::{AgentPath, Error, IntentId, TurnEffect, checkpoint_state, handler::Agents};
use std::sync::Arc;

pub(crate) const SPAWN: &str = "spawn:";
/// Largest generated spawn-name base, before its generation suffix.
const MAX_SPAWN_INTENT_BYTES: usize = 128;
const SEP: char = '\u{1f}';

/// First available generation, or the first child that has not released its name.
#[derive(Debug, PartialEq, Eq)]
pub enum SpawnResolution {
    /// No child has been created at this generation.
    Free(u64),
    /// This generation owns the name, even after completing observable work.
    Live(u64, AgentPath),
}

impl SpawnResolution {
    /// The child currently owning the name, if any.
    pub fn live(self) -> Option<AgentPath> {
        match self {
            Self::Free(_) => None,
            Self::Live(_, path) => Some(path),
        }
    }
}

/// Validates a model-supplied name before constructing any internal suffix.
pub(crate) fn spawn_intent(name: &str, generation: u64) -> Result<IntentId, Error> {
    let base = format!("{SPAWN}{name}");
    if name.is_empty()
        || base.len() > MAX_SPAWN_INTENT_BYTES
        || !name.bytes().all(|byte| byte.is_ascii_graphic())
    {
        return Err(Error::Record(
            "spawn name must be printable ASCII without spaces",
        ));
    }
    IntentId::try_from(base.clone())?;
    IntentId::try_from(if generation == 0 {
        base
    } else {
        format!("{base}{SEP}{generation}")
    })
}

pub(crate) fn spawn_name(intent: &IntentId) -> Result<&str, Error> {
    intent
        .as_str()
        .strip_prefix(SPAWN)
        .map(|name| name.split_once(SEP).map_or(name, |(name, _)| name))
        .ok_or(Error::Record("tool spawn intent has no spawn prefix"))
}

/// Canonical generated intents extend the existing name grammar; external names
/// cannot contain SEP, so no two (name, generation) pairs share an identity.
pub(crate) fn generated_intent(value: &str) -> bool {
    let Some((base, generation)) = value.split_once(SEP) else {
        return false;
    };
    base.starts_with(SPAWN)
        && base.len() > SPAWN.len()
        && base.len() <= MAX_SPAWN_INTENT_BYTES
        && base.bytes().all(|byte| byte.is_ascii_graphic())
        && generation
            .parse::<u64>()
            .is_ok_and(|g| g != 0 && g.to_string() == generation)
}

/// Resolves a spawn name in the provider's original actor namespace. Each
/// generation costs exactly one state-head read, including a newly adopted
/// child: no checkpoint means it is live, so adoption needs no second read.
/// Released generations are terminal and can never reclaim the name.
pub async fn resolve_spawn_name<P: crate::store::JournalProvider + ?Sized + 'static>(
    provider: &Arc<P>,
    within: &acyclic_actors::addressing::ActorReference,
    parent: &AgentPath,
    name: &str,
) -> Result<SpawnResolution, Error> {
    let store = super::actor_store(provider)?;
    let mut generation = 0u64;
    loop {
        let child = TurnEffect::child(parent, &spawn_intent(name, generation)?)?;
        let actor = Agents::<P>::actor(within, &child.id())?;
        let snapshot = store
            .load(&actor)
            .await
            .map_err(|error| Error::Actor(Box::new(error)))?;
        let Some(state) = snapshot.state() else {
            return Ok(SpawnResolution::Free(generation));
        };
        let released = match state.checkpoint() {
            Some(checkpoint) => {
                let state = checkpoint_state(checkpoint)?;
                if state.path != child || state.parent.as_ref() != Some(parent) {
                    return Err(Error::Record("spawn state binding differs"));
                }
                state.released()
            }
            None => false,
        };
        if !released {
            return Ok(SpawnResolution::Live(generation, child));
        }
        generation = generation
            .checked_add(1)
            .ok_or(Error::Record("spawn generations exhausted"))?;
    }
}
