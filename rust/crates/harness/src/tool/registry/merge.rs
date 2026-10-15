use super::*;

impl ToolRegistry {
    /// Adds a disjoint registry, retaining every pinned revision and model selection.
    pub fn extend(&mut self, mut other: Self) -> Result<()> {
        if self
            .selected
            .keys()
            .any(|name| other.selected.contains_key(name))
        {
            return Err(Error::Conflict(
                "tool registries contain the same logical name".into(),
            ));
        }
        if self
            .catalog_revisions
            .keys()
            .any(|name| other.catalog_revisions.contains_key(name))
        {
            return Err(Error::Conflict(
                "tool registries contain the same catalog namespace".into(),
            ));
        }
        self.versions.append(&mut other.versions);
        self.selected.append(&mut other.selected);
        self.catalog_revisions.append(&mut other.catalog_revisions);
        Ok(())
    }
}
