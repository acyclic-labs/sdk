use super::selection::{compaction_error, compaction_step};
use super::{Context, ModelContentPart, quota_vector, retire_vector};
pub(super) use acyclic_model_render::dependencies::Group;
use acyclic_model_render::dependencies::{DependencyBudget, DependencyPart, DependencySource};

impl DependencySource for Context {
    fn len(&self) -> usize {
        self.messages.len()
    }
    fn part_count(&self) -> Option<usize> {
        self.messages.iter().try_fold(0usize, |count, message| {
            count.checked_add(message.content.parts().len())
        })
    }
    fn assistant(&self, position: usize) -> bool {
        self.messages[position].role == crate::model::ModelRole::Assistant
    }
    fn irreducible(&self, position: usize) -> bool {
        let message = &self.messages[position];
        self.current_input_index.map(|index| index as usize) == Some(position)
            || message.role == crate::model::ModelRole::System
            || message.content.contains_project_instructions()
            || message.content.contains_native_media()
            || message
                .content
                .parts()
                .iter()
                .any(|part| matches!(part, ModelContentPart::Configuration { .. }))
    }
    fn parts(&self, position: usize) -> impl Iterator<Item = DependencyPart<'_>> {
        self.messages[position]
            .content
            .parts()
            .iter()
            .map(|part| match part {
                ModelContentPart::Reasoning { block } if !block.continuity.is_empty() => {
                    DependencyPart::Continuity
                }
                ModelContentPart::Text {
                    native: Some(_), ..
                } => DependencyPart::Native,
                ModelContentPart::ToolCall(call) => DependencyPart::Call {
                    id: &call.call_id,
                    native: call.native.is_some(),
                },
                ModelContentPart::ToolResult { call_id, .. } => {
                    DependencyPart::Result { id: call_id }
                }
                _ => DependencyPart::Other,
            })
    }
}

impl DependencyBudget for crate::wire_codec::CodecQuota<'_, '_> {
    type Error = crate::Error;
    fn step(&mut self) -> crate::Result<()> {
        compaction_step(self)
    }
    fn vector<T>(&mut self, capacity: usize) -> crate::Result<Vec<T>> {
        quota_vector(capacity, self)
    }
    fn retire<T>(&mut self, values: Vec<T>) -> crate::Result<()> {
        retire_vector(values, self)
    }
    fn invalid(&mut self, reason: &'static str) -> Self::Error {
        compaction_error(self.work(), reason)
    }
}

pub(super) fn exchange_groups(
    context: &Context,
    quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> crate::Result<Vec<Group>> {
    acyclic_model_render::dependencies::exchange_groups(context, quota)
}

pub(super) fn mandatory_selection(
    context: &Context,
    covered: usize,
    groups: &[Group],
    quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> crate::Result<Vec<bool>> {
    acyclic_model_render::dependencies::mandatory_flags(context, covered, groups, quota)
}
