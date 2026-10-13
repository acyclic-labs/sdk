use super::*;

pub(super) fn plan(case: u8) -> MaterializationPlan {
    MaterializationPlan {
        operation_id: OperationId::from_bytes([case; 16]),
        intent: MaterializationIntent::GenerationTransition {
            from: generation(1),
            to: generation(2),
        },
        edits: vec![
            MaterializationEdit::Install {
                path: "a".to_owned(),
                image: b"new-a".to_vec(),
            },
            MaterializationEdit::Remove {
                path: "b".to_owned(),
            },
            MaterializationEdit::Rename {
                from: "from".to_owned(),
                to: "to".to_owned(),
            },
        ],
    }
}
