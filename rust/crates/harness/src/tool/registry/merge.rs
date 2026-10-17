use super::*;

impl ToolRegistry {
    /// Adds a disjoint registry, retaining every pinned revision and model selection.
    pub fn extend(&mut self, other: Self) -> Result<()> {
        if other
            .data
            .versions
            .keys()
            .any(|key| self.data.versions.contains_key(key))
            || other
                .data
                .selected
                .keys()
                .any(|key| self.data.selected.contains_key(key))
            || other
                .data
                .catalog_revisions
                .keys()
                .any(|key| self.data.catalog_revisions.contains_key(key))
        {
            return Err(Error::Conflict("tool registries overlap".into()));
        }
        let data = Arc::make_mut(&mut self.data);
        let other = Arc::unwrap_or_clone(other.data);
        data.versions.extend(other.versions);
        data.selected.extend(other.selected);
        data.catalog_revisions.extend(other.catalog_revisions);
        Ok(())
    }
}
