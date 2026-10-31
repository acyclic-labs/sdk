//! Startup planning uses the sealed selection; input admission counts the actual request.
use super::*;
use crate::context::{CompactionPolicy, ThresholdCompaction};

pub(super) fn select(
    selected: Option<CompactionPolicy>,
    model: &Model,
    provider: &dyn ModelProvider,
) -> Result<CompactionPolicy> {
    if matches!(selected, Some(CompactionPolicy::Disabled)) {
        return Ok(CompactionPolicy::Disabled);
    }
    let capacity = provider.context_capacity(model)?;
    let output = provider
        .model_profile(model)?
        .and_then(|profile| profile.max_output_tokens());
    let reservation = capacity.output_reservation(output)?;
    match selected {
        Some(CompactionPolicy::Threshold(policy)) => {
            policy.validate(capacity, output)?;
            Ok(CompactionPolicy::Threshold(policy))
        }
        _ => Ok(CompactionPolicy::Threshold(
            ThresholdCompaction::for_capacity(
                crate::context::ModelContextCapacity {
                    output_tokens: Some(reservation),
                    ..capacity
                },
                0,
            )?,
        )),
    }
}
