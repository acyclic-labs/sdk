use super::*;

/// Immutable registry of exact resumable tool revisions.
#[derive(Clone, Default)]
pub struct ResumableToolRegistry(Vec<ResumableTool>);

impl ResumableToolRegistry {
    /// Registers one version-pinned tool state machine.
    pub fn register(&mut self, tool: ResumableTool) -> Result<()> {
        tool.validate(None)?;
        let position = self.0.binary_search_by(|existing| {
            (&existing.definition.name, &existing.definition.revision)
                .cmp(&(&tool.definition.name, &tool.definition.revision))
        });
        match position {
            Ok(_) => Err(Error::Conflict(format!(
                "resumable tool {}@{} is already registered",
                tool.definition.name, tool.definition.revision,
            ))),
            Err(position) => {
                self.0.insert(position, tool);
                Ok(())
            }
        }
    }

    /// Resolves only the exact admitted revision.
    pub(crate) fn get(
        &self,
        name: &str,
        revision: &str,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Option<&ResumableTool>> {
        if let Some(work) = work.as_deref_mut() {
            work.check_cancellation()?;
        }
        let mut lower = 0;
        let mut upper = self.0.len();
        while lower < upper {
            if let Some(work) = work.as_deref_mut() {
                work.codec_charge(crate::executor::json::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
            }
            let middle = lower + (upper - lower) / 2;
            let tool = &self.0[middle];
            match (
                tool.definition.name.as_str(),
                tool.definition.revision.as_str(),
            )
                .cmp(&(name, revision))
            {
                std::cmp::Ordering::Less => lower = middle + 1,
                std::cmp::Ordering::Greater => upper = middle,
                std::cmp::Ordering::Equal => return Ok(Some(tool)),
            }
        }
        Ok(None)
    }

    /// Iterates registered definitions in deterministic identity order.
    pub fn definitions(&self) -> impl Iterator<Item = &ToolDefinition> {
        self.0.iter().map(|tool| &tool.definition)
    }

    /// Validates installed revisions on the original native ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn validate(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        work.remaining()?;
        // Reject an unaudited installed provider before any schema/registry allocation for other rows.
        for tool in &self.0 {
            work.charge(acyclic_fs::WorkCounters {
                items_examined: 1,
                ..Default::default()
            })?;
            if !tool.machine.supports_native_work() {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unsupported,
                    reason: "resumable registry contains an unaudited native machine",
                });
            }
            tool.machine.identity_with_native_work(work)?;
        }
        for tool in &self.0 {
            tool.validate(Some(work))?;
        }
        Ok(())
    }

    /// Copies installed definitions while retaining the same machine implementations.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn clone_with_native_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        use crate::filesystem::UploadRecord as _;
        self.validate(work)?;
        let io = crate::executor::PreparationIo::from_native_work(work)?;
        io.retain_owned(std::mem::size_of::<Self>() as u64)?;
        let mut copied = Self::default();
        let capacity = self
            .0
            .len()
            .checked_mul(std::mem::size_of::<ResumableTool>())
            .ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "resumable registry ownership overflow",
            })? as u64;
        io.retain_owned(capacity)?;
        if capacity != 0 {
            io.allocation_attempt(capacity)?;
        }
        copied
            .0
            .try_reserve_exact(self.0.len())
            .map_err(Error::Allocation)?;
        let backing = (copied.0.capacity() * std::mem::size_of::<ResumableTool>()) as u64;
        io.retain_owned(backing - capacity)?;
        for tool in &self.0 {
            let definition = crate::model::clone_tool_definition(&tool.definition, Some(&io))?;
            copied.0.push(ResumableTool {
                definition,
                machine: tool.machine.clone(),
            });
        }
        let mut retained = (std::mem::size_of::<Self>() as u64)
            .checked_add(backing)
            .ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "resumable registry ownership overflow",
            })?;
        for tool in &copied.0 {
            work.charge(acyclic_fs::WorkCounters {
                items_examined: 1,
                ..Default::default()
            })?;
            let definition_bytes = tool.definition.logical_owned_bytes()?;
            retained = retained
                .checked_add(definition_bytes)
                .ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "resumable registry ownership overflow",
                })?;
        }
        let custody = io.into_native_custody(retained)?;
        Ok((copied, custody))
    }
}
