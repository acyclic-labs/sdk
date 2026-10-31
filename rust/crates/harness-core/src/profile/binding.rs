//! Executable profile declarations borrow the registry's original revisions.
use super::{AgentProfile, invalid};
use crate::{
    Result,
    executor::PreparationIo,
    tool::{Tool, ToolRegistry},
};

/// One ordered profile capability bound to its original executor and schemas.
/// Profile availability is additional policy, never a replacement for grants,
/// approval admission or the executor's original authorization boundary.
pub struct ToolBinding<'a> {
    capability: &'a str,
    tool: &'a Tool,
    available: bool,
}

impl<'a> ToolBinding<'a> {
    /// Logical capability selected by the profile.
    pub fn capability(&self) -> &'a str {
        self.capability
    }
    /// Original registry entry, including its executor and result projection.
    pub fn tool(&self) -> &'a Tool {
        self.tool
    }
    /// Whether this role may attempt the capability under its original grants.
    pub fn available(&self) -> bool {
        self.available
    }
}

impl<'a> AgentProfile<'a> {
    /// Pins provider-facing settings to the original selected model and hard
    /// capacities. Only the parsed profile can construct this sealed projection.
    pub fn bind_model(
        &self,
        model: &crate::model::Model,
        capacity: crate::context::ModelContextCapacity,
    ) -> Result<crate::model::ModelProfileBinding> {
        let selected = self
            .prefix()
            .key
            .strip_prefix(&model.provider)
            .and_then(|suffix| suffix.strip_prefix('/'));
        if selected != Some(model.name.as_str()) || capacity != self.prefix().capacity.model {
            return Err(invalid(
                "profile differs from the selected model or hard capacity",
            ));
        }
        Ok(crate::model::ModelProfileBinding {
            digest: self.digest(),
            prefix: self.prefix_digest(),
            reasoning: self.prefix().reasoning,
            cache: self.prefix().cache,
            max_output_tokens: self.overlay().max_output_tokens,
            capacity: crate::context::ModelContextCapacity {
                context_tokens: self.prefix().capacity.budget_tokens,
                output_tokens: capacity
                    .output_tokens
                    .map(|hard| hard.min(self.prefix().capacity.budget_tokens)),
            },
            overlay_effort: self.overlay().effort,
        })
    }

    /// Borrows declarations in profile order without allocating a binding vector.
    /// Each item resolves its exact registry revision once. Consumers must finish
    /// admission before dispatch; a missing variant never substitutes another tool.
    pub fn bind_tools<'b>(
        &'b self,
        registry: &'b ToolRegistry,
        io: Option<&'b PreparationIo>,
    ) -> impl ExactSizeIterator<Item = Result<ToolBinding<'b>>> + 'b {
        self.prefix().tools.iter().map(move |revision| {
            crate::model::charge_items(io, 1)?;
            let tool = registry
                .get(&revision.variant, Some(&revision.revision), io)?
                .ok_or_else(|| {
                    crate::executor::stock_boundary(
                        io,
                        crate::wire::ErrorCode::NotFound,
                        "profile tool variant/revision has no executable binding",
                    )
                })?;
            let available = self.overlay().allowed_tools.is_none_or(|allowed| {
                allowed
                    .iter()
                    .any(|capability| capability == &revision.capability)
            });
            Ok(ToolBinding {
                capability: &revision.capability,
                tool,
                available,
            })
        })
    }
}
