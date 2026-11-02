//! Immutable outcomes of client spawns, addressed by their inbox sequence.
//!
//! The consuming step appends its outcome in the same commit as the child and
//! checkpoint. Observation follows that one record, independent of the parent's
//! later steps and children; it never decodes or searches actor history.

use super::{AgentPath, Error, SpawnOutcome};
use acyclic_stream::{CommitMutation, StreamPath};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    version: u32,
    outcome: SpawnOutcome,
}

pub(super) fn path(parent: &AgentPath, sequence: u64) -> Result<StreamPath, Error> {
    StreamPath::new(format!("harness/spawns/{}/{sequence}", parent.id())).map_err(Error::Stream)
}

pub(super) fn append(
    parent: &AgentPath,
    sequence: u64,
    outcome: SpawnOutcome,
) -> Result<CommitMutation, Error> {
    let value = serde_json::to_vec(&Receipt {
        version: 1,
        outcome,
    })
    .map_err(|_| Error::Record("spawn outcome does not encode"))?;
    Ok(CommitMutation::Append {
        path: path(parent, sequence)?,
        records: vec![value.into()],
    })
}

pub(super) fn decode(
    parent: &AgentPath,
    sequence: u64,
    value: &[u8],
) -> Result<SpawnOutcome, Error> {
    let receipt: Receipt =
        serde_json::from_slice(value).map_err(|_| Error::Record("spawn outcome is malformed"))?;
    if receipt.version != 1 {
        return Err(Error::Record("unsupported spawn outcome version"));
    }
    if let SpawnOutcome::Spawned { child } = &receipt.outcome
        && *child != super::requested_child(parent, sequence)?.1
    {
        return Err(Error::Record("spawn outcome names another child"));
    }
    Ok(receipt.outcome)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::AgentId;

    #[test]
    fn a_spawn_outcome_rejects_another_version_or_child() {
        let parent = AgentPath::root(&AgentId::from_bytes([1; 16]));
        let sequence = 4;
        let (_, child) = crate::agent::requested_child(&parent, sequence).expect("child");
        let another_parent = AgentPath::root(&AgentId::from_bytes([2; 16]));
        let another_child = another_parent.child(&child.id()).expect("child");
        for (version, child) in [(2, child), (1, another_child)] {
            let value = serde_json::to_vec(&Receipt {
                version,
                outcome: SpawnOutcome::Spawned { child },
            })
            .expect("record");
            assert!(matches!(
                decode(&parent, sequence, &value),
                Err(Error::Record(_))
            ));
        }
    }
}
