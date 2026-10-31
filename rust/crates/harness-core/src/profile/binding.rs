//! Executable profile declarations borrow the registry's original revisions.
use super::{AgentProfile, invalid};
use crate::{
    Result,
    executor::PreparationIo,
    model::ModelRequest,
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

/// One epoch's ordered bindings. Construction reads only the admitted profile
/// and immutable registry; indexed use performs no lookup, allocation or I/O.
pub struct BoundTools<'a> {
    bindings: Vec<ToolBinding<'a>>,
    profile: [u8; 32],
    prefix: [u8; 32],
    max_output_tokens: Option<u32>,
    model_key: &'a str,
    // The vector retires before its original reservation.
    _custody: Option<crate::conversation::ContentHold<'a>>,
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

    /// Resolves every declaration once, preserving profile order and role
    /// restrictions independently. A missing native variant is refused; a
    /// similarly named function or another revision is never substituted.
    pub fn bind_tools<'b>(
        &'b self,
        registry: &'b ToolRegistry,
        io: Option<&'b PreparationIo>,
    ) -> Result<BoundTools<'b>> {
        let bindings = crate::model::vec_with_capacity(self.prefix().tools.len(), io)?;
        let custody = io
            .map(|io| {
                io.guard_owned(crate::core::owned_buffer::<ToolBinding<'b>>(
                    bindings.capacity(),
                )?)
            })
            .transpose()?;
        let mut bound = BoundTools {
            bindings,
            profile: self.digest(),
            prefix: self.prefix_digest(),
            max_output_tokens: self.overlay().max_output_tokens,
            model_key: self.prefix().key,
            _custody: custody,
        };
        for revision in self.prefix().tools {
            crate::model::charge_items(io, 1)?;
            let tool = registry
                .get(&revision.variant, Some(&revision.revision), io)?
                .ok_or_else(|| {
                    invalid("profile tool variant/revision has no executable binding")
                })?;
            let available = self.overlay().allowed_tools.is_none_or(|allowed| {
                allowed
                    .iter()
                    .any(|capability| capability == &revision.capability)
            });
            bound.bindings.push(ToolBinding {
                capability: &revision.capability,
                tool,
                available,
            });
        }
        Ok(bound)
    }
}

impl<'a> BoundTools<'a> {
    /// Admits the current profile-bound request through the existing request
    /// constructor. It does not implement another encoder or model loop.
    pub fn prepare(
        &self,
        request: ModelRequest,
        limits: crate::conversation::Limits,
        io: Option<&PreparationIo>,
    ) -> Result<crate::model::PreparedModelRequest> {
        self.validate_request(&request)?;
        crate::model::PreparedModelRequest::prepare(request, limits, io)
    }

    /// Immutable profile identity for the existing epoch/checkpoint owner.
    pub fn profile_digest(&self) -> [u8; 32] {
        self.profile
    }
    /// Frozen prefix identity, independent of role availability.
    pub fn prefix_digest(&self) -> [u8; 32] {
        self.prefix
    }
    /// All declarations in frozen order, including unavailable capabilities.
    pub fn declarations(&self) -> &[ToolBinding<'a>] {
        &self.bindings
    }

    /// Checks the current request before its existing preparation/dispatch.
    /// This bridge does not build another request or copy tool definitions.
    /// The original manifest admission still pins executors and grants.
    pub fn validate_request(&self, request: &ModelRequest) -> Result<()> {
        let selected = self
            .model_key
            .strip_prefix(&request.model.provider)
            .and_then(|suffix| suffix.strip_prefix('/'));
        if selected != Some(request.model.name.as_str())
            || request.max_output_tokens != self.max_output_tokens
            || request.tools.len() != self.bindings.len()
            || request
                .tools
                .iter()
                .zip(&self.bindings)
                .any(|(definition, binding)| definition != &binding.tool.definition)
        {
            return Err(invalid(
                "request differs from its executable profile bindings",
            ));
        }
        Ok(())
    }
}
