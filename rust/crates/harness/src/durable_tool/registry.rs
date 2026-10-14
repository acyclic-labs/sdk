use super::*;
/// Registry of exact resumable tool revisions.
#[derive(Clone, Default)]
pub struct ResumableToolRegistry(Vec<ResumableTool>);
impl ResumableToolRegistry {
    /// Registers an unambiguous pinned tool.
    pub fn register(&mut self, tool: ResumableTool) -> Result<()> {
        tool.validate(None)?;
        match self.0.binary_search_by(|existing| {
            (&existing.definition.name, &existing.definition.revision)
                .cmp(&(&tool.definition.name, &tool.definition.revision))
        }) {
            Ok(_) => Err(Error::Conflict(
                "resumable tool is already registered".into(),
            )),
            Err(index) => {
                self.0.insert(index, tool);
                Ok(())
            }
        }
    }
    pub(crate) fn get(
        &self,
        name: &str,
        revision: &str,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<Option<&ResumableTool>> {
        if let Some(work) = work.as_deref_mut() {
            work.check_cancellation()?;
        }
        let (mut lower, mut upper) = (0, self.0.len());
        while lower < upper {
            if let Some(work) = work.as_deref_mut() {
                work.codec_charge(crate::executor::json::JsonCodecCharge::Work {
                    bytes: 0,
                    items: 1,
                })?;
            }
            let middle = lower + (upper - lower) / 2;
            let tool = self
                .0
                .get(middle)
                .ok_or_else(|| Error::Invalid("resumable tool index is invalid".into()))?;
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
    /// Iterates definitions in identity order.
    pub fn definitions(&self) -> impl Iterator<Item = &ToolDefinition> {
        self.0.iter().map(|tool| &tool.definition)
    }
    /// Copies installed definitions on the original ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn clone_with_native_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        use crate::filesystem::UploadRecord as _;
        // Check every producer before allocating any copied schema.
        work.remaining()?;
        for tool in &self.0 {
            work.charge(acyclic_fs::WorkCounters {
                items_examined: 1,
                ..Default::default()
            })?;
            if !tool.machine.supports_native_work() {
                return Err(Error::NativeBoundary {
                    code: Unsupported,
                    reason: "registry has no native machine producer",
                });
            }
            tool.machine.identity_with_native_work(work)?;
        }
        for tool in &self.0 {
            tool.validate(Some(work))?;
        }
        let io = PreparationIo::from_native_work(work)?;
        io.retain_owned(std::mem::size_of::<Self>() as u64)?;
        let capacity = self
            .0
            .len()
            .checked_mul(std::mem::size_of::<ResumableTool>())
            .ok_or_else(|| boundary(Some(&io), Invalid, "registry size overflow"))?
            as u64;
        io.retain_owned(capacity)?;
        if capacity != 0 {
            io.allocation_attempt(capacity)?;
        }
        let mut copied = Self::default();
        copied
            .0
            .try_reserve_exact(self.0.len())
            .map_err(Error::Allocation)?;
        let backing = (copied.0.capacity() * std::mem::size_of::<ResumableTool>()) as u64;
        io.retain_owned(backing - capacity)?;
        for tool in &self.0 {
            copied.0.push(ResumableTool {
                definition: crate::model::clone_tool_definition(&tool.definition, Some(&io))?,
                machine: tool.machine.clone(),
            });
        }
        let mut retained = (std::mem::size_of::<Self>() as u64)
            .checked_add(backing)
            .ok_or_else(|| boundary(Some(&io), Invalid, "registry size overflow"))?;
        for tool in &copied.0 {
            work.charge(acyclic_fs::WorkCounters {
                items_examined: 1,
                ..Default::default()
            })?;
            retained = retained
                .checked_add(tool.definition.logical_owned_bytes()?)
                .ok_or_else(|| boundary(Some(&io), Invalid, "registry size overflow"))?;
        }
        Ok((copied, io.into_native_custody(retained)?))
    }
}
