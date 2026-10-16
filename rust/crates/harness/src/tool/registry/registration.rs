use super::*;

impl ToolRegistry {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    /// Creates an empty registry with funded metadata.
    pub fn new(
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        let custody = work.claim(
            REGISTRY_BYTES as u64,
            1,
            acyclic_fs::WorkCounters::UNCHARGED,
        )?;
        Ok((Self::default(), custody))
    }

    /// Registers a tool using its original payload and registry custody.
    pub fn register(
        &mut self,
        tool: (Tool, crate::NativeCustody),
        custody: &mut crate::NativeCustody,
        work: crate::NativeWork<'_>,
    ) -> Result<()> {
        self.register_checked(tool, custody, work, &|| Ok(()))
    }

    pub(crate) fn register_checked(
        &mut self,
        tool: (Tool, crate::NativeCustody),
        custody: &mut crate::NativeCustody,
        work: crate::NativeWork<'_>,
        check: &impl Fn() -> Result<()>,
    ) -> Result<()> {
        check()?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut tool = tool;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let (original, definition_bytes, original_quota) =
            self.admit_registration(&mut tool, custody, work)?;
        let mut published = false;
        let result = (|| {
            let tool = tool.0;
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let owned_io = work
                .map(crate::filesystem::PreparationIo::from_native_work)
                .transpose()?;
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let io = owned_io.as_ref();
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            let io: Option<&crate::executor::PreparationIo> = None;
            crate::executor::with_history_io(io, |work| {
                tool.definition.validate_checked(work, check)
            })?;
            check()?;
            self.admit_version_slot(io)?;
            let key = (
                crate::executor::stock_string_copy(&tool.definition.name, io)?,
                crate::executor::stock_string_copy(&tool.definition.revision, io)?,
            );
            if self.data.versions.contains_key(&key) {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                if work.is_some() {
                    return Err(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Conflict,
                        reason: "tool revision is already registered",
                    });
                }
                return Err(Error::Conflict(format!(
                    "tool {}@{} is already registered",
                    key.0, key.1
                )));
            }
            let previous = self.data.selected.contains_key(&key.0);
            let selection = if previous {
                None
            } else {
                #[cfg(feature = "filesystem")]
                if let Some(io) = io {
                    io.retain_owned(std::mem::size_of::<(String, ModelSelection)>() as u64)?;
                }
                let name = crate::executor::stock_string_copy(&key.0, io)?;
                let selection = if previous {
                    ModelSelection::Ambiguous
                } else {
                    ModelSelection::Revision(crate::executor::stock_string_copy(&key.1, io)?)
                };
                Some((name, selection))
            };
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let retained = if let Some(owned_io) = owned_io {
                let (extra, retained) = self.registration_bytes(
                    &key,
                    &selection,
                    previous,
                    original,
                    definition_bytes,
                )?;
                let mut held = owned_io.into_native_custody(extra)?;
                custody
                    .as_mut()
                    .ok_or(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Storage,
                        reason: "tool registry custody is missing",
                    })?
                    .absorb(&mut held)?;
                retained
            } else {
                0
            };
            let data = Arc::make_mut(&mut self.data);
            if let Some((name, selection)) = selection {
                data.selected.insert(name, selection);
            } else if let Some(selection) = data.selected.get_mut(&key.0)
                && *selection != ModelSelection::Hidden
            {
                *selection = ModelSelection::Ambiguous;
            }
            data.versions.insert(key, Arc::new(tool));
            published = true;
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.is_some() {
                // Replaced selection storage is gone before its refund.
                reconcile_registry_custody(custody, retained)?;
            }
            Ok(())
        })();
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if result.is_err() && !published && work.is_some() {
            // The closure's input and prepared buffers have dropped before refund.
            reconcile_registry_custody(custody, original_quota)?;
        }
        let _ = (published, custody, work);
        result
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn admit_registration(
        &mut self,
        tool: &mut (Tool, crate::NativeCustody),
        custody: &mut crate::NativeCustody,
        work: crate::NativeWork<'_>,
    ) -> Result<(u64, u64, u64)> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let (original, definition_bytes, original_quota) = if let Some(work) = work {
            use crate::filesystem::UploadRecord as _;
            work.remaining()?;
            if Arc::get_mut(&mut self.data).is_none() {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "shared native registry mutation requires a funded registry copy",
                });
            }
            let original = self.native_owned_bytes()?;
            let definition_bytes = (std::mem::size_of::<ToolDefinition>() as u64)
                .checked_add(tool.0.definition.logical_owned_bytes()?)
                .ok_or(OWNERSHIP_OVERFLOW)?;
            let registry = custody.as_mut().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "tool registration has no original registry custody",
            })?;
            let definition = tool.1.as_mut().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "tool registration has no original payload custody",
            })?;
            if !registry.work().shares_allocation_context(work)
                || registry.bytes() < original
                || !definition.work().shares_allocation_context(work)
                || definition.bytes() < definition_bytes
            {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "tool registration has no original payload custody",
                });
            }
            tool.0.executor.supports_native_execution(work)?;
            tool.0.projection.supports_native_projection(Some(work))?;
            let original_quota = registry.bytes();
            registry.absorb(definition)?;
            (original, definition_bytes, original_quota)
        } else {
            (0, 0, 0)
        };
        Ok((original, definition_bytes, original_quota))
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn registration_bytes(
        &self,
        key: &(String, String),
        selection: &Option<(String, ModelSelection)>,
        previous: bool,
        original: u64,
        definition_bytes: u64,
    ) -> Result<(u64, u64)> {
        let selection_bytes = match &selection {
            Some((name, selected)) => [
                std::mem::size_of::<(String, ModelSelection)>() as u64,
                name.capacity() as u64,
                match selected {
                    ModelSelection::Revision(revision) => revision.capacity() as u64,
                    _ => 0,
                },
            ]
            .into_iter()
            .try_fold(0_u64, |total, bytes| total.checked_add(bytes))
            .ok_or(OWNERSHIP_OVERFLOW)?,
            None => 0,
        };
        let extra = [
            VERSION_SLOT_BYTES as u64,
            key.0.capacity() as u64,
            key.1.capacity() as u64,
            selection_bytes,
        ]
        .into_iter()
        .try_fold(0_u64, |total, bytes| total.checked_add(bytes))
        .ok_or(OWNERSHIP_OVERFLOW)?;
        let released = if previous && selection.is_none() {
            match self.data.selected.get(&key.0) {
                Some(ModelSelection::Revision(revision)) => revision.capacity() as u64,
                _ => 0,
            }
        } else {
            0
        };
        let retained = original
            .checked_add(definition_bytes)
            .and_then(|bytes| bytes.checked_add(extra))
            .and_then(|bytes| bytes.checked_sub(released))
            .ok_or(OWNERSHIP_OVERFLOW)?;
        Ok((extra, retained))
    }

    fn admit_version_slot(&self, io: Option<&crate::executor::PreparationIo>) -> Result<()> {
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            io.retain_owned(VERSION_SLOT_BYTES as u64)?;
            let counters = acyclic_fs::WorkCounters {
                items_examined: self.data.versions.len() as u64,
                ..Default::default()
            };
            io.preflight(counters)?;
            io.charge(counters)?;
        }
        let _ = io;
        Ok(())
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn reconcile_registry_custody(custody: &mut crate::NativeCustody, retained: u64) -> Result<()> {
    let custody = custody.as_mut().ok_or(Error::NativeBoundary {
        code: crate::wire::ErrorCode::Storage,
        reason: "tool registry custody is missing",
    })?;
    custody.release(custody.bytes().checked_sub(retained).ok_or(
        acyclic_fs::NativeTreeMaterializationError::Work(acyclic_fs::WorkError::Overflow),
    )?)?;
    Ok(())
}
