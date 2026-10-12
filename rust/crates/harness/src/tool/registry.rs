use super::*;

/// One tool assembled from three independently replaceable values.
#[derive(Clone)]
pub struct Tool {
    /// Model-visible contract.
    pub definition: ToolDefinition,
    /// Execution implementation.
    pub executor: Arc<dyn ToolExecutor>,
    /// Context projection implementation.
    pub projection: Arc<dyn ToolProjection>,
}

#[derive(Clone, PartialEq, Eq)]
enum ModelSelection {
    Revision(String),
    Ambiguous,
    Hidden,
}

/// Deterministic registry retaining every pinned revision. A model request
/// exposes exactly one selected revision per logical tool name.
#[derive(Clone, Default)]
pub struct ToolRegistry {
    versions: BTreeMap<(String, String), Tool>,
    selected: BTreeMap<String, ModelSelection>,
    catalog_revisions: BTreeMap<String, [u8; 32]>,
}

impl ToolRegistry {
    /// Creates an empty registry with funded metadata.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
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

    /// Registers a tool, retaining paid definitions on their original ledger.
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
                .ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "tool definition ownership overflow",
                })?;
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
                    .ok_or(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Invalid,
                        reason: "tool registration ownership overflow",
                    })?;
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
                    .ok_or(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Invalid,
                        reason: "tool registry ownership overflow",
                    })?;
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
                custody
                    .as_mut()
                    .expect("checked registry custody")
                    .substitute(retained)?;
            }
            Ok(())
        })();
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if result.is_err() && !published && work.is_some() {
            // The closure's input and prepared buffers have dropped before refund.
            custody
                .as_mut()
                .expect("checked registry custody")
                .substitute(original_quota)?;
        }
        let _ = (published, custody, work);
        result
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn native_owned_bytes(&self) -> Result<u64> {
        use crate::filesystem::UploadRecord as _;
        let mut bytes = std::mem::size_of::<Self>() as u64;
        for ((name, revision), tool) in &self.versions {
            let definition_deep = tool.definition.logical_owned_bytes()?;
            let row = (std::mem::size_of::<((String, String), Tool)>() as u64)
                .checked_add(name.capacity() as u64)
                .and_then(|bytes| bytes.checked_add(revision.capacity() as u64))
                .and_then(|bytes| bytes.checked_add(definition_deep));
            bytes = bytes
                .checked_add(row.ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "tool registry ownership overflow",
                })?)
                .ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "tool registry ownership overflow",
                })?;
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
                .ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "tool registry ownership overflow",
                })?;
        }
        for name in self.catalog_revisions.keys() {
            bytes = bytes
                .checked_add(std::mem::size_of::<(String, [u8; 32])>() as u64)
                .and_then(|bytes| bytes.checked_add(name.capacity() as u64))
                .ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "tool registry ownership overflow",
                })?;
        }
        Ok(bytes)
    }

    /// Selects the one revision visible to a model under this logical name.
    pub fn select_model_version(&mut self, name: &str, revision: &str) -> Result<()> {
        if !self
            .versions
            .contains_key(&(name.to_owned(), revision.to_owned()))
        {
            return Err(Error::NotFound(format!("tool {name}@{revision}")));
        }
        self.selected.insert(
            name.to_owned(),
            ModelSelection::Revision(revision.to_owned()),
        );
        Ok(())
    }

    /// Pins complete dynamic installation identity, including empty visibility.
    pub(crate) fn pin_catalog_revision(
        &mut self,
        namespace: &str,
        expected: Option<[u8; 32]>,
        revision: [u8; 32],
    ) -> Result<()> {
        if self.catalog_revisions.get(namespace).copied() != expected {
            return Err(Error::Conflict(
                "dynamic catalog installation changed".into(),
            ));
        }
        self.catalog_revisions.insert(namespace.into(), revision);
        Ok(())
    }

    /// Removes a logical tool from future model catalogs without deleting its immutable versions.
    pub fn remove_from_model(&mut self, name: &str) -> Result<()> {
        if !self.versions.keys().any(|(logical, _)| logical == name) {
            return Err(Error::NotFound(format!("tool {name}")));
        }
        self.selected
            .insert(name.to_owned(), ModelSelection::Hidden);
        Ok(())
    }

    /// Installs an agent-selected tool after exact scope and durable approval checks.
    pub async fn install_scoped(
        &mut self,
        tool: Tool,
        installation_id: OperationId,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        interaction_id: InteractionId,
        approval: &dyn ToolApprovalVerifier,
    ) -> Result<()> {
        verifier.verify(scope, &mut None)?;
        if !scope
            .capabilities()
            .contains(&format!("tool:install:{}", tool.definition.name))
        {
            return Err(Error::Unauthorized(
                "scope does not permit this tool installation".into(),
            ));
        }
        let digest = tool.definition.digest()?;
        approval
            .verify(interaction_id, installation_id, digest)
            .await?;
        self.register((tool, None), &mut None, None)
    }

    /// Returns one assembled tool.
    pub(crate) fn get(
        &self,
        name: &str,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Option<&Tool>> {
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            io.charge(acyclic_fs::WorkCounters {
                items_examined: name.len() as u64,
                ..Default::default()
            })?;
        }
        if let Some((logical, revision)) = name.rsplit_once('@') {
            return self.get_version(logical, revision, io);
        }
        let Some(ModelSelection::Revision(revision)) = self.selected.get(name) else {
            return Ok(None);
        };
        self.get_version(name, revision, io)
    }

    /// Resolves a pinned revision independently of the model-visible choice.
    pub(crate) fn get_version(
        &self,
        name: &str,
        revision: &str,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Option<&Tool>> {
        let key = (
            crate::executor::stock_string_copy(name, io)?,
            crate::executor::stock_string_copy(revision, io)?,
        );
        Ok(self.versions.get(&key))
    }

    /// Copies only SDK-owned registry metadata on the original ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
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
                .ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "tool definition ownership overflow",
                })?;
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
            custody.resize(custody.bytes().checked_add(slots).ok_or(
                Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "native tool registry ownership overflow",
                },
            )?)?;
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
                    .ok_or(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Invalid,
                        reason: "native tool selection ownership overflow",
                    })?,
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
                    .ok_or(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Invalid,
                        reason: "native tool catalog ownership overflow",
                    })?,
            )?;
            copied.catalog_revisions.insert(name, *revision);
        }
        Ok((copied, custody))
    }

    /// Returns the unambiguous model-visible selection in canonical order.
    pub fn definitions(
        &self,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Vec<ToolDefinition>> {
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            io.retain_owned(std::mem::size_of::<Vec<ToolDefinition>>() as u64)?;
        }
        let mut definitions = crate::model::vec_with_capacity(self.selected.len(), io)?;
        for (name, selection) in &self.selected {
            let revision = match selection {
                ModelSelection::Revision(revision) => revision,
                ModelSelection::Hidden => continue,
                ModelSelection::Ambiguous => {
                    return Err(crate::executor::stock_boundary(
                        io,
                        crate::wire::ErrorCode::Conflict,
                        "tool requires an explicit model revision",
                    ));
                }
            };
            let tool = self.get_version(name, revision, io)?.ok_or_else(|| {
                crate::executor::stock_boundary(
                    io,
                    crate::wire::ErrorCode::Storage,
                    "selected tool revision is not registered",
                )
            })?;
            definitions.push(crate::model::clone_tool_definition(&tool.definition, io)?);
        }
        Ok(definitions)
    }
}
