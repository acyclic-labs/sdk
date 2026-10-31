use super::*;

impl ToolRegistry {
    pub(super) fn native_owned_bytes(&self) -> Result<u64> {
        (REGISTRY_BYTES as u64)
            .checked_add(self.data.retained_bytes)
            .ok_or(OWNERSHIP_OVERFLOW)
    }

    /// Copies registry metadata on its original ledger.
    #[cfg(test)]
    pub(crate) fn clone_with_native_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        io.retain_owned(REGISTRY_BYTES as u64)?;
        let mut copied = Self::default();
        let data = Arc::make_mut(&mut copied.data);
        for ((name, revision), tool) in &self.data.versions {
            io.retain_owned((VERSION_SLOT_BYTES + std::mem::size_of::<ToolDefinition>()) as u64)?;
            let key = (
                crate::executor::stock_string_copy(name, Some(&io))?,
                crate::executor::stock_string_copy(revision, Some(&io))?,
            );
            let definition = crate::model::clone_tool_definition(&tool.definition, Some(&io))?;
            if tool.skill_source.is_some() {
                io.retain_owned(crate::executor::filesystem_skill_marker_bytes())?;
            }
            let tool = Arc::new(Tool {
                skill_source: tool
                    .skill_source
                    .as_ref()
                    .map(|source| source.copy(Some(&io)))
                    .transpose()?,
                hook_names: tool.hook_names,
                definition,
                executor: tool.executor.clone(),
                projection: tool.projection.clone(),
            });
            use crate::filesystem::UploadRecord as _;
            let source = tool
                .skill_source
                .as_ref()
                .map(|source| {
                    source.logical_owned_bytes().and_then(|bytes| {
                        bytes
                            .checked_add(crate::executor::filesystem_skill_marker_bytes())
                            .ok_or(OWNERSHIP_OVERFLOW)
                    })
                })
                .transpose()?
                .unwrap_or(0);
            let definition_bytes = tool.definition.logical_owned_bytes()?;
            let added = (VERSION_SLOT_BYTES as u64)
                .checked_add(key.0.capacity() as u64)
                .and_then(|bytes| bytes.checked_add(key.1.capacity() as u64))
                .and_then(|bytes| bytes.checked_add(std::mem::size_of::<ToolDefinition>() as u64))
                .and_then(|bytes| bytes.checked_add(definition_bytes))
                .and_then(|bytes| bytes.checked_add(source))
                .ok_or(OWNERSHIP_OVERFLOW)?;
            data.retained_bytes = data
                .retained_bytes
                .checked_add(added)
                .ok_or(OWNERSHIP_OVERFLOW)?;
            data.versions.insert(key, tool);
        }
        for (name, selection) in &self.data.selected {
            io.retain_owned(std::mem::size_of::<(String, ModelSelection)>() as u64)?;
            let name = crate::executor::stock_string_copy(name, Some(&io))?;
            let selection = match selection {
                ModelSelection::Revision(revision) => ModelSelection::Revision(
                    crate::executor::stock_string_copy(revision, Some(&io))?,
                ),
                ModelSelection::CallableRevision(revision) => ModelSelection::CallableRevision(
                    crate::executor::stock_string_copy(revision, Some(&io))?,
                ),
                ModelSelection::Ambiguous => ModelSelection::Ambiguous,
                ModelSelection::Hidden => ModelSelection::Hidden,
            };
            let added = (std::mem::size_of::<(String, ModelSelection)>() as u64)
                .checked_add(name.capacity() as u64)
                .and_then(|bytes| bytes.checked_add(selection.retained_bytes()))
                .ok_or(OWNERSHIP_OVERFLOW)?;
            data.retained_bytes = data
                .retained_bytes
                .checked_add(added)
                .ok_or(OWNERSHIP_OVERFLOW)?;
            data.selected.insert(name, selection);
        }
        for (name, revision) in &self.data.catalog_revisions {
            io.retain_owned(std::mem::size_of::<(String, [u8; 32])>() as u64)?;
            let name = crate::executor::stock_string_copy(name, Some(&io))?;
            let added = (std::mem::size_of::<(String, [u8; 32])>() as u64)
                .checked_add(name.capacity() as u64)
                .ok_or(OWNERSHIP_OVERFLOW)?;
            data.retained_bytes = data
                .retained_bytes
                .checked_add(added)
                .ok_or(OWNERSHIP_OVERFLOW)?;
            data.catalog_revisions.insert(name, *revision);
        }
        let retained = copied.native_owned_bytes()?;
        Ok((copied, io.into_native_custody(retained)?))
    }
}
