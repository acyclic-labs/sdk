//! Original step resource observations, committed with the actor checkpoint.
//!
//! These are semantic commits and conversation bytes, not backend attempts or
//! physical durability operations. CPU, memory and other measurements remain
//! unknown unless an original producer supplies evidence. Model usage and price
//! records remain their accounting authorities. No observation reads history,
//! visits ancestors, waits, or discovers machine topology.

use super::{AgentId, Error};
use serde::{Deserialize, Serialize};

/// Resource snapshot streams live outside forked agent subtrees. A child's
/// counters start at zero; inherited conversation bytes are never new work.
pub const RESOURCES: &str = "harness/resources";

/// Original host observations of one execution location. Every absent field is
/// unknown. Digests identify immutable host/region facts, not routing authority.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceLocation {
    /// Immutable host identity, shared by colocated agents.
    pub host: Option<[u8; 32]>,
    /// Original Machine identity, when execution is machine-backed.
    pub machine: Option<[u8; 16]>,
    /// Immutable region identity, when the host knows it.
    pub region: Option<[u8; 32]>,
    /// Execution core of this placement, not a sampled process CPU number.
    pub core: Option<u32>,
    /// NUMA node assigned by the host.
    pub numa: Option<u32>,
    /// Executor shard assigned by the host.
    pub shard: Option<u32>,
}

/// A fixed placement fact supplied by the original host at binding or movement.
/// Local and remote hosts use the same representation and leave unknown fields
/// absent. This observation never changes execution routing.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourcePlacement {
    /// Digest of the original filesystem provider identity, if bound.
    pub filesystem_provider: Option<[u8; 32]>,
    /// Actor placement, if known.
    pub actor: ResourceLocation,
    /// Executor shell placement, which can differ from the actor's location.
    pub shell: ResourceLocation,
}

impl ResourcePlacement {
    /// Observes the existing provider identity without discovering a host or
    /// attributing shared-machine usage to an actor. Hashing is paid once when
    /// the host binds, never once per step.
    #[must_use]
    pub fn filesystem(provider: &crate::resources::ProviderRef) -> Self {
        let mut hash = blake3::Hasher::new_derive_key("acyclic.harness.resource-provider.v1");
        for part in [provider.namespace(), provider.family(), provider.version()] {
            hash.update(&(part.len() as u64).to_be_bytes());
            hash.update(part.as_bytes());
        }
        Self {
            filesystem_provider: Some(hash.finalize().into()),
            ..Self::default()
        }
    }
}

/// A placement hashed once at the original host boundary. Every input is fixed
/// size; no allocation, provider call, clock read or machine query is required.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObservedResourcePlacement {
    digest: [u8; 32],
    fact: ResourcePlacement,
}

impl ObservedResourcePlacement {
    /// Binds the exact typed fact to its digest. Explicit presence tags keep an
    /// unknown field distinct from a measured zero.
    #[must_use]
    pub fn new(fact: ResourcePlacement) -> Self {
        fn optional<const N: usize>(hash: &mut blake3::Hasher, value: Option<[u8; N]>) {
            hash.update(&[u8::from(value.is_some())]);
            if let Some(value) = value {
                hash.update(&value);
            }
        }
        fn location(hash: &mut blake3::Hasher, value: ResourceLocation) {
            optional(hash, value.host);
            optional(hash, value.machine);
            optional(hash, value.region);
            optional(hash, value.core.map(u32::to_be_bytes));
            optional(hash, value.numa.map(u32::to_be_bytes));
            optional(hash, value.shard.map(u32::to_be_bytes));
        }
        let mut hash = blake3::Hasher::new_derive_key("acyclic.harness.resource-placement.v1");
        optional(&mut hash, fact.filesystem_provider);
        location(&mut hash, fact.actor);
        location(&mut hash, fact.shell);
        Self {
            digest: hash.finalize().into(),
            fact,
        }
    }

    /// Identity of this exact placement fact.
    #[must_use]
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }

    /// The original host observation.
    #[must_use]
    pub const fn fact(&self) -> ResourcePlacement {
        self.fact
    }
}

/// Fixed cumulative coverage of this agent's own settled steps. Subtree totals
/// are a projection of member snapshots, never writes to every ancestor.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentResourceCounters {
    /// Original actor step commits; retries of one commit do not add a step.
    pub step_commits: u64,
    /// Conversation records newly appended by this agent's steps.
    pub conversation_records: u64,
    /// Exact encoded bytes of those conversation records, excluding inherited
    /// content, this observation and the actor checkpoint.
    pub conversation_bytes: u64,
}

impl AgentResourceCounters {
    /// One pure checked transition. Its input is counted while the step already
    /// visits its effects; there is no second serialization or history scan.
    pub fn step(self, records: u64, bytes: u64) -> Result<Self, Error> {
        let overflow = || Error::Record("agent resource counters overflow");
        Ok(Self {
            step_commits: self.step_commits.checked_add(1).ok_or_else(overflow)?,
            conversation_records: self
                .conversation_records
                .checked_add(records)
                .ok_or_else(overflow)?,
            conversation_bytes: self
                .conversation_bytes
                .checked_add(bytes)
                .ok_or_else(overflow)?,
        })
    }
}

/// Research checkpoint contribution. Its fixed size is independent of swarm
/// size, history, child count and depth. A new child has no contribution yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentResourceState {
    /// This agent's own cumulative measurements.
    pub counters: AgentResourceCounters,
    /// Last observed placement identity, or unknown.
    pub placement: Option<[u8; 32]>,
}

/// Version-one observation committed with its checkpoint and source cursor.
/// Exporters stream these cumulative observations with their original source
/// sequence; a query selects the latest per agent instead of summing snapshots.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentResourceSnapshot {
    /// Persisted format version, currently one.
    pub version: u32,
    /// Fixed owning agent identity; no root agent is implied.
    pub agent: [u8; 16],
    /// Original consumed inbox cut in this step's commit.
    pub inbox_cursor: u64,
    /// The exact accumulator encoded in the same checkpoint.
    pub state: AgentResourceState,
    /// Original fact on first observation or a move. Other steps retain only
    /// its digest. An unknown placement clears the previous identity explicitly.
    pub placement: Option<ResourcePlacement>,
}

impl AgentResourceSnapshot {
    /// Decode a whole version-one record. Unsupported formats never become
    /// plausible measurements and have no migration fallback.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() > acyclic_stream::MAX_RECORD_BYTES {
            return Err(Error::Record(
                "agent resource snapshot exceeds stream record size",
            ));
        }
        let snapshot: Self = serde_json::from_slice(bytes)
            .map_err(|_| Error::Record("agent resource snapshot is malformed"))?;
        if snapshot.version != 1 {
            return Err(Error::Record("unsupported agent resource snapshot version"));
        }
        if snapshot.state.counters.step_commits == 0 {
            return Err(Error::Record(
                "agent resource snapshot has no committed step",
            ));
        }
        if let Some(fact) = snapshot.placement
            && snapshot.state.placement != Some(ObservedResourcePlacement::new(fact).digest())
        {
            return Err(Error::Record("agent resource placement digest differs"));
        }
        Ok(snapshot)
    }
}

/// This agent's resource stream, outside the forked subtree. Construction and
/// observation cost depend only on its fixed identity, not ancestry depth.
pub fn resource_stream(agent: &AgentId) -> Result<acyclic_stream::StreamPath, Error> {
    acyclic_stream::StreamPath::new(format!("{RESOURCES}/{}", agent.as_str()))
        .map_err(|_| Error::Path("agent resource stream is invalid"))
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
