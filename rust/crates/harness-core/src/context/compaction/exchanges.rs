use super::{Context, quota_vector, selection::compaction_error};
use crate::{
    Result,
    model::{ModelContentPart, ModelMessage, ModelRole},
};
use acyclic_collections::CompiledPrefixError;
pub(super) use acyclic_model_render::exchanges::Group;
use acyclic_model_render::exchanges::{self, Message, Part};

impl Message for ModelMessage {
    fn assistant(&self) -> bool {
        self.role == ModelRole::Assistant
    }

    fn parts(&self) -> impl Iterator<Item = Part<'_>> {
        self.content.parts().iter().map(|part| match part {
            ModelContentPart::Reasoning { block } if !block.continuity.is_empty() => Part::Native,
            ModelContentPart::Text {
                native: Some(_), ..
            } => Part::Native,
            ModelContentPart::ToolCall(call) => Part::Call {
                id: &call.call_id,
                native: call.native.is_some(),
            },
            ModelContentPart::ToolResult { call_id, .. } => Part::Result { id: call_id },
            _ => Part::Other,
        })
    }
}

pub(super) fn exchange_groups(
    context: &Context,
    quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<Vec<Group>> {
    let mut groups = quota_vector::<Group>(context.messages.len(), quota)?;
    let original = crate::wire_codec::BorrowedIndexWork::new(quota.work());
    match exchanges::exchange_groups(&context.messages, &mut groups, &mut &original) {
        Ok(()) => Ok(groups),
        Err(CompiledPrefixError::Work(error)) => Err(error),
        Err(CompiledPrefixError::Allocation(cause)) => original.with(|work| {
            Err(match work.as_deref_mut() {
                Some(io) => io.allocation_failure(cause),
                None => crate::Error::Invalid(cause.to_string()),
            })
        }),
        Err(CompiledPrefixError::Invariant) => original.with(|work| {
            Err(compaction_error(
                work,
                "tool dependency index is inconsistent",
            ))
        }),
        Err(CompiledPrefixError::Capacity | CompiledPrefixError::Order(_)) => {
            original.with(|work| {
                Err(compaction_error(
                    work,
                    "tool dependency index cannot be represented",
                ))
            })
        }
    }
}
