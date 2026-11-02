//! Immutable outcomes of client spawns, addressed by their inbox sequence.
//!
//! The consuming step appends its outcome in the same commit as the child and
//! checkpoint. Observation follows that one record, independent of the parent's
//! later steps and children; it never decodes or searches actor history.

use super::{AgentPath, Error, SpawnOutcome, SpawnRefusal};
use acyclic_stream::{CommitMutation, StreamPath};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    version: u32,
    outcome: Outcome,
}

// The child is determined by the parent and inbox sequence. Persisting its
// address again would copy lineage into an otherwise fixed-size outcome.
#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Outcome {
    Spawned {},
    Refused { reason: SpawnRefusal },
}

pub(super) fn path(parent: &AgentPath, sequence: u64) -> Result<StreamPath, Error> {
    StreamPath::new(format!("harness/spawns/{}/{sequence}", parent.id_text()))
        .map_err(Error::Stream)
}

pub(super) fn append(
    parent: &AgentPath,
    sequence: u64,
    outcome: SpawnOutcome,
) -> Result<CommitMutation, Error> {
    let value = serde_json::to_vec(&Receipt {
        version: 1,
        outcome: match outcome {
            SpawnOutcome::Spawned { .. } => Outcome::Spawned {},
            SpawnOutcome::Refused { reason } => Outcome::Refused { reason },
        },
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
    match receipt.outcome {
        Outcome::Spawned {} => Ok(SpawnOutcome::Spawned {
            child: super::requested_child(parent, sequence)?.1,
        }),
        Outcome::Refused { reason } => Ok(SpawnOutcome::Refused { reason }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::AgentId;

    #[test]
    fn receipt_publication_does_not_copy_ancestry() {
        let mut baseline = None;
        let mut previous_copy = 0;
        for depth in [0, 16, 63] {
            let mut parent = AgentPath::root(&AgentId::from_bytes([1; 16]));
            for _ in 0..depth {
                parent = parent.child(&AgentId::from_bytes([2; 16])).expect("parent");
            }
            let (_, child) = super::super::requested_child(&parent, 4).expect("child");
            let outcome = SpawnOutcome::Spawned { child };
            // Negative control: encoding the address again pays for every
            // ancestor, even though the receipt's key already determines it.
            let copying = acyclic_test_support::measure(|| {
                std::hint::black_box(serde_json::to_vec(&outcome).expect("outcome"));
            });
            assert!(copying.bytes_total > previous_copy);
            previous_copy = copying.bytes_total;

            let mut published = None;
            let work = acyclic_test_support::measure(|| {
                published = Some(append(&parent, 4, outcome).expect("outcome"));
            });
            let CommitMutation::Append { records, .. } = published.expect("published") else {
                panic!("outcome must append");
            };
            let current = (work.count_total, work.bytes_total, records[0].value.len());
            match baseline {
                None => baseline = Some(current),
                Some(baseline) => assert_eq!(current, baseline),
            }
        }
    }

    #[test]
    fn a_spawn_outcome_rejects_another_version_or_an_embedded_address() {
        let parent = AgentPath::root(&AgentId::from_bytes([1; 16]));
        let sequence = 4;
        let value = serde_json::to_value(Receipt {
            version: 1,
            outcome: Outcome::Spawned {},
        })
        .expect("record");
        for (key, replacement) in [
            ("version", serde_json::json!(2)),
            (
                "outcome",
                serde_json::json!({"kind": "spawned", "child": parent}),
            ),
        ] {
            let mut changed = value.clone();
            changed[key] = replacement;
            let value = serde_json::to_vec(&changed).expect("record");
            assert!(matches!(
                decode(&parent, sequence, &value),
                Err(Error::Record(_))
            ));
        }
    }
}
