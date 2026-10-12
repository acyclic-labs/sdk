use super::*;

impl ToolRegistry {
    pub(super) fn native_owned_bytes(&self) -> Result<u64> {
        use crate::filesystem::UploadRecord as _;
        let mut bytes = std::mem::size_of::<Self>() as u64;
        for ((name, revision), tool) in &self.versions {
            let definition_deep = tool.definition.logical_owned_bytes()?;
            let row = (std::mem::size_of::<((String, String), Tool)>() as u64)
                .checked_add(name.capacity() as u64)
                .and_then(|bytes| bytes.checked_add(revision.capacity() as u64))
                .and_then(|bytes| bytes.checked_add(definition_deep));
            bytes = bytes
                .checked_add(row.ok_or(OWNERSHIP_OVERFLOW)?)
                .ok_or(OWNERSHIP_OVERFLOW)?;
        }
        for (name, selection) in &self.selected {
            let deep = match selection {
                ModelSelection::Revision(revision) => revision.capacity() as u64,
                _ => 0,
            };
            bytes = bytes
                .checked_add(std::mem::size_of::<(String, ModelSelection)>() as u64)
                .and_then(|bytes| bytes.checked_add(name.capacity() as u64))
                .and_then(|bytes| bytes.checked_add(deep))
                .ok_or(OWNERSHIP_OVERFLOW)?;
        }
        for name in self.catalog_revisions.keys() {
            bytes = bytes
                .checked_add(std::mem::size_of::<(String, [u8; 32])>() as u64)
                .and_then(|bytes| bytes.checked_add(name.capacity() as u64))
                .ok_or(OWNERSHIP_OVERFLOW)?;
        }
        Ok(bytes)
    }

    /// Copies registry metadata through its original native producers.
    pub(crate) fn clone_with_native_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        use crate::filesystem::UploadRecord as _;
        let mut custody = work.claim(
            std::mem::size_of::<Self>() as u64,
            0,
            acyclic_fs::WorkCounters::UNCHARGED,
        )?;
        let mut copied = Self::default();
        for ((name, revision), tool) in &self.versions {
            let io = crate::filesystem::PreparationIo::from_native_work(work)?;
            io.retain_owned(std::mem::size_of::<ToolDefinition>() as u64)?;
            let definition = crate::model::clone_tool_definition(&tool.definition, Some(&io))?;
            let retained = (std::mem::size_of::<ToolDefinition>() as u64)
                .checked_add(definition.logical_owned_bytes()?)
                .ok_or(OWNERSHIP_OVERFLOW)?;
            let mut definition_hold = io.into_native_custody(retained)?;
            let mut name_hold;
            let (name, hold) = work.display(name)?;
            name_hold = hold;
            let mut revision_hold;
            let (revision, hold) = work.display(revision)?;
            revision_hold = hold;
            custody.absorb(&mut definition_hold)?;
            custody.absorb(&mut name_hold)?;
            custody.absorb(&mut revision_hold)?;
            let slots = std::mem::size_of::<(String, String)>() as u64
                + std::mem::size_of::<Arc<dyn ToolExecutor>>() as u64
                + std::mem::size_of::<Arc<dyn ToolProjection>>() as u64;
            custody.resize(
                custody
                    .bytes()
                    .checked_add(slots)
                    .ok_or(OWNERSHIP_OVERFLOW)?,
            )?;
            copied.versions.insert(
                (name, revision),
                Tool {
                    definition,
                    executor: tool.executor.clone(),
                    projection: tool.projection.clone(),
                },
            );
        }
        for (name, selection) in &self.selected {
            let mut name_hold;
            let (name, hold) = work.display(name)?;
            name_hold = hold;
            custody.absorb(&mut name_hold)?;
            let selection = match selection {
                ModelSelection::Revision(revision) => {
                    let mut hold;
                    let (revision, owned) = work.display(revision)?;
                    hold = owned;
                    custody.absorb(&mut hold)?;
                    ModelSelection::Revision(revision)
                }
                ModelSelection::Ambiguous => ModelSelection::Ambiguous,
                ModelSelection::Hidden => ModelSelection::Hidden,
            };
            custody.resize(
                custody
                    .bytes()
                    .checked_add(std::mem::size_of::<(String, ModelSelection)>() as u64)
                    .ok_or(OWNERSHIP_OVERFLOW)?,
            )?;
            copied.selected.insert(name, selection);
        }
        for (name, revision) in &self.catalog_revisions {
            let mut hold;
            let (name, owned) = work.display(name)?;
            hold = owned;
            custody.absorb(&mut hold)?;
            custody.resize(
                custody
                    .bytes()
                    .checked_add(std::mem::size_of::<(String, [u8; 32])>() as u64)
                    .ok_or(OWNERSHIP_OVERFLOW)?,
            )?;
            copied.catalog_revisions.insert(name, *revision);
        }
        Ok((copied, custody))
    }
}
