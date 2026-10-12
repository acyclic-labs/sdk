use super::*;

impl ToolRegistry {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    /// Creates an empty registry with funded metadata.
    pub fn new(
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        let custody = work.claim(
            std::mem::size_of::<Self>() as u64,
            0,
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
        let mut tool = tool;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let (original, definition_bytes, original_quota) = if let Some(work) = work {
            use crate::filesystem::UploadRecord as _;
            work.remaining()?;
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
            crate::executor::with_history_io(io, |work| tool.definition.validate(work))?;
            #[cfg(feature = "filesystem")]
            if let Some(io) = io {
                io.retain_owned(
                    (std::mem::size_of::<(String, String)>()
                        + std::mem::size_of::<Arc<dyn ToolExecutor>>()
                        + std::mem::size_of::<Arc<dyn ToolProjection>>())
                        as u64,
                )?;
                let counters = acyclic_fs::WorkCounters {
                    items_examined: self.versions.len() as u64,
                    ..Default::default()
                };
                io.preflight(counters)?;
                io.charge(counters)?;
            }
            let key = (
                crate::executor::stock_string_copy(&tool.definition.name, io)?,
                crate::executor::stock_string_copy(&tool.definition.revision, io)?,
            );
            if self.versions.contains_key(&key) {
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
            let previous = self.versions.keys().any(|(logical, _)| logical == &key.0);
            let selection = if previous && self.selected.contains_key(&key.0) {
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
            let retained = if io.is_some() {
                let extra = (std::mem::size_of::<(String, String)>() as u64)
                    .checked_add(std::mem::size_of::<Arc<dyn ToolExecutor>>() as u64)
                    .and_then(|bytes| {
                        bytes.checked_add(std::mem::size_of::<Arc<dyn ToolProjection>>() as u64)
                    })
                    .and_then(|bytes| bytes.checked_add(key.0.capacity() as u64))
                    .and_then(|bytes| bytes.checked_add(key.1.capacity() as u64))
                    .and_then(|bytes| match &selection {
                        Some((name, selection)) => bytes
                            .checked_add(std::mem::size_of::<(String, ModelSelection)>() as u64)
                            .and_then(|bytes| bytes.checked_add(name.capacity() as u64))
                            .and_then(|bytes| {
                                bytes.checked_add(match selection {
                                    ModelSelection::Revision(revision) => {
                                        revision.capacity() as u64
                                    }
                                    _ => 0,
                                })
                            }),
                        None => Some(bytes),
                    })
                    .ok_or(OWNERSHIP_OVERFLOW)?;
                let released = if previous && selection.is_none() {
                    match self.selected.get(&key.0) {
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
                let mut held = owned_io
                    .expect("native registration ledger")
                    .into_native_custody(extra)?;
                custody
                    .as_mut()
                    .expect("checked registry custody")
                    .absorb(&mut held)?;
                retained
            } else {
                0
            };
            if let Some((name, selection)) = selection {
                self.selected.insert(name, selection);
            } else if let Some(selection) = self.selected.get_mut(&key.0) {
                if *selection != ModelSelection::Hidden {
                    *selection = ModelSelection::Ambiguous;
                }
            }
            self.versions.insert(key, tool);
            published = true;
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.is_some() {
                // Replaced selection storage is gone before its refund.
                let custody = custody.as_mut().expect("checked registry custody");
                custody.release(custody.bytes().checked_sub(retained).ok_or(
                    acyclic_fs::NativeTreeMaterializationError::Work(
                        acyclic_fs::WorkError::Overflow,
                    ),
                )?)?;
            }
            Ok(())
        })();
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if result.is_err() && !published && work.is_some() {
            // The closure's input and prepared buffers have dropped before refund.
            let custody = custody.as_mut().expect("checked registry custody");
            custody.release(custody.bytes().checked_sub(original_quota).ok_or(
                acyclic_fs::NativeTreeMaterializationError::Work(acyclic_fs::WorkError::Overflow),
            )?)?;
        }
        let _ = (published, custody, work);
        result
    }
}
