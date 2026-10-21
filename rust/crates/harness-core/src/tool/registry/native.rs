use super::*;

impl ToolRegistry {
    pub(super) fn native_owned_bytes(&self) -> Result<u64> {
        use crate::filesystem::UploadRecord as _;
        let mut bytes = REGISTRY_BYTES as u64;
        for ((name, revision), tool) in &self.data.versions {
            let definition_deep = tool.definition.logical_owned_bytes()?;
            let row = ((VERSION_SLOT_BYTES + std::mem::size_of::<ToolDefinition>()) as u64)
                .checked_add(name.capacity() as u64)
                .and_then(|bytes| bytes.checked_add(revision.capacity() as u64))
                .and_then(|bytes| bytes.checked_add(definition_deep));
            bytes = bytes
                .checked_add(row.ok_or(OWNERSHIP_OVERFLOW)?)
                .ok_or(OWNERSHIP_OVERFLOW)?;
        }
        for (name, selection) in &self.data.selected {
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
        for name in self.data.catalog_revisions.keys() {
            bytes = bytes
                .checked_add(std::mem::size_of::<(String, [u8; 32])>() as u64)
                .and_then(|bytes| bytes.checked_add(name.capacity() as u64))
                .ok_or(OWNERSHIP_OVERFLOW)?;
        }
        Ok(bytes)
    }

    /// Copies registry metadata on its original ledger.
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
            data.versions.insert(
                key,
                Arc::new(Tool {
                    hook_names: tool.hook_names,
                    definition,
                    executor: tool.executor.clone(),
                    projection: tool.projection.clone(),
                }),
            );
        }
        for (name, selection) in &self.data.selected {
            io.retain_owned(std::mem::size_of::<(String, ModelSelection)>() as u64)?;
            let name = crate::executor::stock_string_copy(name, Some(&io))?;
            let selection = match selection {
                ModelSelection::Revision(revision) => ModelSelection::Revision(
                    crate::executor::stock_string_copy(revision, Some(&io))?,
                ),
                ModelSelection::Ambiguous => ModelSelection::Ambiguous,
                ModelSelection::Hidden => ModelSelection::Hidden,
            };
            data.selected.insert(name, selection);
        }
        for (name, revision) in &self.data.catalog_revisions {
            io.retain_owned(std::mem::size_of::<(String, [u8; 32])>() as u64)?;
            data.catalog_revisions.insert(
                crate::executor::stock_string_copy(name, Some(&io))?,
                *revision,
            );
        }
        let retained = copied.native_owned_bytes()?;
        Ok((copied, io.into_native_custody(retained)?))
    }
}
