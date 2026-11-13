//! Admitted effort overlays use the existing appended configuration item.
use super::*;

impl StockExecution<'_> {
    pub(super) fn configuration_delta(
        &self,
        messages: &[ModelMessage],
    ) -> Result<Option<crate::model::ConfigurationUpdate>> {
        if self.configuration.is_none()
            && self
                .selected
                .as_ref()
                .is_none_or(|binding| binding.effort.is_none())
        {
            return Ok(None);
        }
        let effort = self.selected.as_ref().and_then(|binding| binding.effort);
        let Some(mut profile) = self
            .selected_provider()
            .model_profile(self.selected_model())?
        else {
            return if effort.is_none() {
                Ok(None)
            } else {
                Err(stock_boundary(
                    self.io,
                    crate::wire::ErrorCode::Unsupported,
                    "selected model has no admitted reasoning baseline",
                ))
            };
        };
        if let Some(effort) = effort {
            if profile.baseline_effort().is_none() {
                return Err(stock_boundary(
                    self.io,
                    crate::wire::ErrorCode::Unsupported,
                    "selected model has no admitted reasoning baseline",
                ));
            }
            profile.overlay_effort = Some(match effort {
                crate::config::Effort::Low => acyclic_model_render::Effort::Low,
                crate::config::Effort::Medium => acyclic_model_render::Effort::Medium,
                crate::config::Effort::High => acyclic_model_render::Effort::High,
            });
        }
        let mut previous = None;
        for message in messages.iter().rev() {
            for part in message.content.parts() {
                crate::model::charge_items(self.io, 1)?;
                if let ModelContentPart::Configuration { prefix, effort } = part
                    && *prefix == profile.prefix_digest()
                {
                    previous = Some(*effort);
                }
            }
            if previous.is_some() {
                break;
            }
        }
        Ok(profile.configuration_delta(previous))
    }
}
