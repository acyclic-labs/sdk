use super::{OperationId, WorkflowCommand};
use serde::{Deserialize, Serialize};

/// Ordinary local request head. Goal status/content remains exclusively GOAL.md.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Turn {
    pub operation: OperationId,
    pub root: OperationId,
    pub automatic_turns: u32,
    pub receive: WorkflowCommand,
    pub model: WorkflowCommand,
}

pub(super) fn decode(bytes: &[u8]) -> serde_json::Result<Turn> {
    serde_json::from_slice(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn turn_head_round_trips(operation in any::<[u8;16]>(), root in any::<[u8;16]>(), count in any::<u32>(), kind in ".{0,64}") {
            let seed = include_bytes!("../../../../harness-fuzz/corpus/records/turn");
            let mut turn = decode(seed).unwrap();
            turn.operation = OperationId::from_bytes(operation);
            turn.root = OperationId::from_bytes(root);
            turn.automatic_turns = count;
            turn.model.kind = kind;
            let bytes = serde_json::to_vec(&turn).unwrap();
            prop_assert_eq!(decode(&bytes).unwrap(), turn);
        }
    }
}
