use crate::{OperationId, workflow::WorkflowCommand};
use serde::{Deserialize, Serialize};

/// Ordinary local request head. Goal status/content remains exclusively GOAL.md.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[doc(hidden)]
pub struct Turn {
    pub operation: OperationId,
    pub root: OperationId,
    pub automatic_turns: u32,
    /// GOAL.md write publication observed with this retained count.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub goal_revision: Option<OperationId>,
    pub receive: WorkflowCommand,
    pub model: WorkflowCommand,
}

#[doc(hidden)]
pub fn decode(bytes: &[u8]) -> serde_json::Result<Turn> {
    serde_json::from_slice(bytes)
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn goal_revision_corpus_preserves_legacy_heads_and_rejects_malformed_metadata() {
        let legacy = decode(include_bytes!("../../../harness-fuzz/corpus/records/turn")).unwrap();
        assert_eq!(legacy.goal_revision, None);
        let linked = decode(include_bytes!(
            "../../../harness-fuzz/corpus/records/turn-goal-revision"
        ))
        .unwrap();
        assert_eq!(
            linked.goal_revision,
            Some(OperationId::from_bytes(std::array::from_fn(
                |index| index as u8
            )))
        );
        assert_eq!(linked.operation, legacy.operation);
        assert_eq!(linked.automatic_turns, legacy.automatic_turns);
        assert!(
            decode(include_bytes!(
                "../../../harness-fuzz/corpus/records/turn-goal-revision-invalid"
            ))
            .is_err()
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn turn_head_round_trips(operation in any::<[u8;16]>(), root in any::<[u8;16]>(), count in any::<u32>(), revision in prop::option::of(any::<[u8;16]>()), kind in ".{0,64}") {
            let seed = include_bytes!("../../../harness-fuzz/corpus/records/turn");
            let mut turn = decode(seed).unwrap();
            turn.operation = OperationId::from_bytes(operation);
            turn.root = OperationId::from_bytes(root);
            turn.automatic_turns = count;
            turn.goal_revision = revision.map(OperationId::from_bytes);
            turn.model.kind = kind;
            let bytes = serde_json::to_vec(&turn).unwrap();
            prop_assert_eq!(decode(&bytes).unwrap(), turn);
        }
    }
}
