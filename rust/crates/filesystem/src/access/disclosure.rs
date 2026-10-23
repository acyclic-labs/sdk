use super::{AccessView, InvalidPolicy, PathTrie};

impl AccessView {
    pub(crate) fn confirms_disclosure(
        &self,
        selected: &PathTrie,
        observed: Option<(
            crate::GenerationId,
            crate::kernel::MetadataField<crate::ObjectId>,
        )>,
        now: u64,
    ) -> Result<bool, InvalidPolicy> {
        let current = self
            .current
            .read()
            .map_err(|_| InvalidPolicy("access view publication failed"))?;
        Ok(std::ptr::eq(current.as_ref(), selected)
            && observed.is_some_and(|(generation, revision)| {
                selected.matches_policy_revision(generation, revision)
            })
            && selected.valid_at(now))
    }
}
