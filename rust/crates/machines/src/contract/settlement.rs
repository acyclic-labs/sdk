use super::*;

/// Exact successful result and its canonical Streams publication identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SettledMutation {
    pub operation: OperationId,
    pub settlement: acyclic_stream::CommitId,
    pub outcome: MutationOutcome,
}

/// Terminal canonical publication. Transport errors never stand in for this state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationSettlement {
    pub operation: OperationObservation,
    pub settlement: acyclic_stream::CommitId,
    pub outcome: Option<MutationOutcome>,
}

impl OperationSettlement {
    pub fn result(self, key: IdempotencyKey) -> Result<SettledMutation, ProviderError> {
        match (self.operation.phase, self.outcome) {
            (OperationPhase::Succeeded, Some(outcome)) => Ok(SettledMutation {
                operation: self.operation.id, settlement: self.settlement, outcome,
            }),
            (OperationPhase::Failed, None) => Err(ProviderError::Failed),
            (OperationPhase::Cancelled, None) => Err(ProviderError::Cancelled),
            (OperationPhase::Pending | OperationPhase::Indeterminate, None) => Err(ProviderError::Indeterminate(key)),
            _ => Err(ProviderError::Rejected("operation settlement has contradictory terminal state; recovery: repair the canonical settlement encoding".into())),
        }
    }
}
