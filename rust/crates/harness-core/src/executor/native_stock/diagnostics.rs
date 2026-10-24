use super::*;

pub(in crate::executor) const fn stock_serde_diagnostic_label_bytes() -> u64 {
    crate::model::MODEL_DIAGNOSTIC_LABEL_BYTES + 1098
}

#[cfg(feature = "filesystem")]
macro_rules! stock_records {
    ($($record:ty),+ $(,)?) => {$(impl PreparationRecord for $record {
        fn diagnostic_label_bytes() -> u64 { stock_serde_diagnostic_label_bytes() }
    })+};
}
#[cfg(feature = "filesystem")]
stock_records!(
    String,
    ExecutionEvent,
    TurnInput,
    crate::scheduler::LeaseFence,
    crate::conversation::ModelContextSelection,
    crate::context::CompactionRetention,
    crate::context::CompactionPolicy,
    crate::context::ModelTokenCount,
    Vec<ModelEvent>,
    crate::registry::ComponentIdentity
);
