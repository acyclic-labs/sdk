//! Automatic budgets use the provider's fixed tool/options token accounting.
use super::*;
use crate::{
    context::{CompactionPolicy, ThresholdCompaction},
    model::{ModelMessage, ModelRequest, ModelRole, PreparedModelRequest},
};

pub(super) async fn select(
    selected: Option<CompactionPolicy>,
    model: &Model,
    provider: &dyn ModelProvider,
    tools: &ToolRegistry,
    limits: Limits,
) -> Result<CompactionPolicy> {
    match selected {
        Some(CompactionPolicy::Disabled) => Ok(CompactionPolicy::Disabled),
        Some(CompactionPolicy::Threshold(policy)) => {
            policy.validate(provider.context_capacity(model)?, None)?;
            Ok(CompactionPolicy::Threshold(policy))
        }
        None => {
            let capacity = provider.context_capacity(model)?;
            let reserve = ThresholdCompaction::for_capacity(capacity, 0)?.response_reserve_tokens;
            let request = PreparedModelRequest::prepare(
                ModelRequest {
                    model: model.clone(),
                    messages: vec![ModelMessage {
                        role: ModelRole::User,
                        content: ModelContent::Text("token accounting".into()),
                    }],
                    tools: tools.definitions(None)?,
                    max_output_tokens: Some(reserve),
                },
                limits,
                None,
            )?;
            let count = provider.count_tokens(&request).await?;
            let fixed = u32::try_from(count.validate(&request, &mut None)?).map_err(|_| {
                Error::Invalid("fixed tool context exceeds provider capacity".into())
            })?;
            Ok(CompactionPolicy::Threshold(
                ThresholdCompaction::for_capacity(capacity, fixed)?,
            ))
        }
    }
}
