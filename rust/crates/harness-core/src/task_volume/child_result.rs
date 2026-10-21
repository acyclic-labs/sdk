//! Immutable answers and failures for one child mailbox turn.
use super::{Error, Result};
use crate::{Outcome, TaskId, executor::TurnOutput};
use serde::{Deserialize, Serialize};

/// One settled child outcome retained independently of its next receive.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChildTurnResult {
    /// Original admitted child identity.
    pub task: TaskId,
    /// Gapless input sequence completed by this turn.
    pub turn: u64,
    /// Answer, failure, or acknowledged cancellation.
    pub outcome: Outcome<TurnOutput>,
}

impl ChildTurnResult {
    /// Decodes a complete result; uncertain outcomes cannot acknowledge a turn.
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        let result: Self = serde_json::from_slice(bytes).map_err(|_| Error::InvalidSegment)?;
        result.validate()?;
        Ok(result)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.turn == 0 || matches!(self.outcome, Outcome::Indeterminate { .. }) {
            return Err(Error::InvalidSegment);
        }
        if let Outcome::Succeeded(output) = &self.outcome {
            for attachment in &output.attachments {
                attachment
                    .validate(&mut None)
                    .map_err(|_| Error::InvalidSegment)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[cfg(not(acyclic_harness_loom))]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[test]
        fn terminal_intent_allows_draining_results_until_the_settled_acknowledgement(
            task in any::<[u8;16]>(), archive in any::<bool>(),
        ) {
            use crate::task_volume::{ChildFile, ChildState, ChildStatus, Component, Control, FileKey, FileValue};
            use super::super::{Index, apply, validate_file};
            let mut index = Index::default();
            let reference = Component::new("worker").unwrap();
            let key = |file| FileKey::Child { reference: reference.clone(), file };
            let status = ChildStatus { task: TaskId::from_bytes(task), state: ChildState::Running, settled: false, inputs: 2 };
            let started = FileValue::new(key(ChildFile::Status), serde_json::to_vec(&status).unwrap());
            apply(&mut index, started, 1);
            let control = if archive { Control::Archive } else { Control::Cancel };
            let intent = FileValue::new(key(ChildFile::Control), control.bytes().to_vec());
            prop_assert!(validate_file(&index, &intent).is_ok());
            apply(&mut index, intent.clone(), 2);
            prop_assert!(validate_file(&index, &intent).is_ok());
            let result = ChildTurnResult { task: status.task, turn: 1, outcome: Outcome::Cancelled };
            let drained = FileValue::new(key(ChildFile::Result(1)), serde_json::to_vec(&result).unwrap());
            prop_assert!(validate_file(&index, &drained).is_ok());
            apply(&mut index, drained, 3);
            let inbox = FileValue::new(key(ChildFile::Inbox(Component::new("late").unwrap())), b"late".to_vec());
            prop_assert!(validate_file(&index, &inbox).is_err());
            let resume = FileValue::new(key(ChildFile::Control), Control::Resume.bytes().to_vec());
            prop_assert!(validate_file(&index, &resume).is_err());
            let settled = FileValue::new(key(ChildFile::Status), serde_json::to_vec(&ChildStatus {
                state: ChildState::Running.apply(control), settled: true, ..status
            }).unwrap());
            prop_assert!(validate_file(&index, &settled).is_ok());
            apply(&mut index, settled.clone(), 4);
            prop_assert!(validate_file(&index, &settled).is_ok());
            let late = ChildTurnResult { turn: 2, ..result };
            let late = FileValue::new(key(ChildFile::Result(2)), serde_json::to_vec(&late).unwrap());
            prop_assert!(validate_file(&index, &late).is_err());
        }

        #[test]
        fn publication_is_gapless_immutable_and_bound_to_the_admitted_task(
            task in any::<[u8;16]>(), text in ".{0,128}", turns in 1u64..9,
        ) {
            use crate::task_volume::{ChildFile, ChildState, ChildStatus, Component, FileKey, FileValue};
            use super::super::{Index, apply, validate_file};
            let reference = Component::new("worker").unwrap();
            let task = TaskId::from_bytes(task);
            let mut index = Index::default();
            let key = |file| FileKey::Child { reference: reference.clone(), file };
            let status = FileValue::new(key(ChildFile::Status), serde_json::to_vec(&ChildStatus {
                task, state: ChildState::Running, settled: false, inputs: turns,
            }).unwrap());
            apply(&mut index, status, 1);
            for turn in 1..=turns {
                let result = ChildTurnResult { task, turn, outcome: Outcome::Failed { message: text.clone() } };
                let file = FileValue::new(key(ChildFile::Result(turn)), serde_json::to_vec(&result).unwrap());
                let mut foreign = result.clone();
                let mut owner = task.into_bytes(); owner[0] ^= 1;
                foreign.task = TaskId::from_bytes(owner);
                prop_assert!(validate_file(&index, &FileValue::new(file.key.clone(), serde_json::to_vec(&foreign).unwrap())).is_err());
                let future = ChildTurnResult { turn: turns + 1, ..result.clone() };
                prop_assert!(validate_file(&index, &FileValue::new(key(ChildFile::Result(turns + 1)), serde_json::to_vec(&future).unwrap())).is_err());
                if turn < turns {
                    let skipped = ChildTurnResult { turn: turn + 1, ..result.clone() };
                    prop_assert!(validate_file(&index, &FileValue::new(key(ChildFile::Result(turn + 1)), serde_json::to_vec(&skipped).unwrap())).is_err());
                }
                prop_assert!(validate_file(&index, &file).is_ok());
                apply(&mut index, file.clone(), turn + 1);
                prop_assert!(validate_file(&index, &file).is_ok());
                let changed = ChildTurnResult { outcome: Outcome::Cancelled, ..result };
                prop_assert!(validate_file(&index, &FileValue::new(file.key.clone(), serde_json::to_vec(&changed).unwrap())).is_err());
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[test]
        fn settled_result_and_path_round_trip(
            task in any::<[u8;16]>(), turn in 1u64..u64::MAX,
            outcome in 0u8..3, text in ".{0,128}",
        ) {
            let outcome = match outcome {
                0 => serde_json::json!({"Succeeded":{"text":text,"metadata":null,"steps":1,"attachments":[]}}),
                1 => serde_json::json!({"Failed":{"message":text}}),
                _ => serde_json::json!("Cancelled"),
            };
            let result = ChildTurnResult { task: TaskId::from_bytes(task), turn,
                outcome: serde_json::from_value(outcome).unwrap() };
            let bytes = serde_json::to_vec(&result).unwrap();
            prop_assert_eq!(ChildTurnResult::parse(&bytes).unwrap(), result.clone());
            let key = crate::task_volume::FileKey::parse(&format!("agents/worker/results/{turn}.json")).unwrap();
            prop_assert_eq!(key.owner(), crate::task_volume::Owner::Harness);
            prop_assert!(!key.mutable());
            let keys = std::collections::BTreeSet::from([key.clone()]);
            for query in [crate::task_volume::Query::Children,
                crate::task_volume::Query::Child(crate::task_volume::Component::new("worker").unwrap())] {
                prop_assert!(query.matches(&key));
                prop_assert_eq!(query.ranges().into_iter().map(|range| keys.range(range).count()).sum::<usize>(), 1);
            }

            prop_assert_eq!(crate::task_volume::FileKey::parse(&key.path()).unwrap(), key);
            let mut invalid = result;
            invalid.turn = 0;
            prop_assert!(ChildTurnResult::parse(&serde_json::to_vec(&invalid).unwrap()).is_err());
            invalid.turn = turn;
            invalid.outcome = Outcome::Indeterminate { operation_id: crate::OperationId::from_bytes([3;16]), cause: crate::IndeterminateCause::Unobserved };
            prop_assert!(ChildTurnResult::parse(&serde_json::to_vec(&invalid).unwrap()).is_err());
            prop_assert!(crate::task_volume::FileKey::parse(&format!("agents/worker/results/0{turn}.json")).is_err(), "leading-zero result accepted");
        }
    }
}
