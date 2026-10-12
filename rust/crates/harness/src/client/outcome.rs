use super::*;

/// Evidence establishes correspondence, rather than UI equality.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Correspondence {
    /// Prediction matches canonical outcome under the pinned adapter.
    Match,
    /// Canonical result replaces the prediction.
    Different,
}

/// Provider operation outcome, independent of prediction status.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum OperationOutcome {
    /// No evidence of admission or completion.
    Unknown,
    /// Admission is not confirmation.
    Admitted,
    /// Effect outcome cannot yet be established.
    Indeterminate,
    /// Authoritative terminal success, with canonical projection.
    Completed,
    /// Authoritative rejection (not an uncertain effect).
    Rejected,
    /// Authoritative cancellation; adapter must establish actual termination.
    Cancelled,
}

impl OperationOutcome {
    pub(super) fn terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Rejected | Self::Cancelled)
    }
}

/// Status of a local prediction, not an authority or operation receipt.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PredictionOutcome {
    /// Eligible for explicit overlay selection.
    Pending,
    /// Canonical correspondence established.
    Confirmed,
    /// Canonical result differs.
    Replaced,
    /// A pinned basis or dependency ceased to hold.
    Invalidated,
    /// Definitive rejection or cancellation.
    Removed,
}
