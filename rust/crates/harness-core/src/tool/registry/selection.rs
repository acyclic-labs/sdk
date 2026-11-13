use super::*;

impl ToolRegistry {
    /// The selected canonical executor, including when presented through native
    /// declarations. Hidden capabilities remain unavailable. Native adapters all
    /// retain the same originally selected capability and revision.
    pub(crate) fn capability(
        &self,
        name: &str,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Option<&Tool>> {
        let selected = match self.data.selected.get(name) {
            // A family can replace the canonical spelling entirely. Every
            // declaration retains the capability selected before binding.
            Some(ModelSelection::Native { names, .. }) => match names.first() {
                Some(declaration) => self.get(declaration, None, io)?,
                None => None,
            },
            _ => self.get(name, None, io)?,
        };
        Ok(selected.map(|tool| tool.executor.capability().map(Arc::as_ref).unwrap_or(tool)))
    }

    pub(crate) fn pin_model_definitions(
        &mut self,
        definitions: &[ToolDefinition],
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        self.pin_definitions(definitions, io, false)
    }

    /// Epochs pin registered capabilities while live sources retain their
    /// event-driven catalog generations and announce changes after the prefix.
    pub(crate) fn pin_epoch_definitions(
        &mut self,
        definitions: &[ToolDefinition],
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        self.pin_definitions(definitions, io, true)
    }

    fn pin_definitions(
        &mut self,
        definitions: &[ToolDefinition],
        io: Option<&crate::executor::PreparationIo>,
        fixed_only: bool,
    ) -> Result<()> {
        use crate::{executor::stock_boundary, wire::ErrorCode};
        // A manifest pins canonical revisions; native declarations bind to
        // what it pins, so they are bound after it and never pinned.
        if self
            .data
            .selected
            .values()
            .any(|selection| matches!(selection, ModelSelection::Native { .. }))
        {
            return Err(Error::Conflict(
                "native declarations bind after the manifest pins".into(),
            ));
        }
        crate::executor::with_history_io(io, |work| -> Result<()> {
            let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
            let mut selected =
                crate::core::reserve_copy_vector::<(&str, &str)>(definitions.len(), &mut quota)?;
            for definition in definitions {
                if fixed_only && !self.data.selected.contains_key(&definition.name) {
                    continue;
                }
                let tool = lookup::get(&self.data.versions, &definition.name, &definition.revision)
                    .ok_or_else(|| {
                        stock_boundary(
                            io,
                            ErrorCode::NotFound,
                            "manifest tool revision is not registered",
                        )
                    })?;
                if tool.definition != *definition {
                    return Err(stock_boundary(
                        io,
                        ErrorCode::Conflict,
                        "manifest tool definition differs from its executor",
                    ));
                }
                selected.push((definition.name.as_str(), definition.revision.as_str()));
            }
            selected.sort_unstable_by(|a, b| a.0.cmp(b.0));
            if selected.windows(2).any(|pair| pair[0].0 == pair[1].0) {
                return Err(stock_boundary(
                    io,
                    ErrorCode::Conflict,
                    "manifest repeats a tool name",
                ));
            }
            for (name, selection) in &mut Arc::make_mut(&mut self.data).selected {
                let replacement = match selected
                    .binary_search_by(|(candidate, _)| candidate.cmp(&name.as_str()))
                {
                    Ok(index) => ModelSelection::Revision(crate::executor::stock_string_copy(
                        selected[index].1,
                        io,
                    )?),
                    Err(_) => ModelSelection::Hidden,
                };
                let old_bytes = match selection {
                    ModelSelection::Revision(revision) => revision.capacity(),
                    _ => 0,
                };
                *selection = replacement;
                if let Some(io) = io {
                    io.release(old_bytes as u64)?;
                }
            }
            Ok(())
        })
    }

    /// Selects one revision for future model requests.
    pub fn select_model_version(&mut self, name: &str, revision: &str) -> Result<()> {
        if self.get_shared(name, Some(revision), None)?.is_none() {
            return Err(Error::NotFound(format!("tool {name}@{revision}")));
        }
        if matches!(self.data.selected.get(name), Some(ModelSelection::Revision(selected)) if selected == revision)
        {
            return Ok(());
        }
        Arc::make_mut(&mut self.data).selected.insert(
            name.to_owned(),
            ModelSelection::Revision(revision.to_owned()),
        );
        Ok(())
    }

    /// Pins the complete dynamic catalog installation.
    pub(crate) fn pin_catalog_revision(
        &mut self,
        namespace: &str,
        expected: Option<[u8; 32]>,
        revision: [u8; 32],
    ) -> Result<()> {
        if self.data.catalog_revisions.get(namespace).copied() != expected {
            return Err(Error::Conflict(
                "dynamic catalog installation changed".into(),
            ));
        }
        Arc::make_mut(&mut self.data)
            .catalog_revisions
            .insert(namespace.into(), revision);
        Ok(())
    }

    /// Presents capability `name` to the model as `natives`, declarations
    /// already registered and selected under their own names. Calls under the
    /// capability's own name are no longer offered, unless included among the
    /// declarations; its selected revision then stays callable.
    pub(crate) fn present_natively(&mut self, name: &str, natives: Vec<String>) -> Result<()> {
        let selection = Arc::make_mut(&mut self.data)
            .selected
            .get_mut(name)
            .ok_or_else(|| Error::NotFound(format!("tool {name}")))?;
        if natives.len() == 1 && natives[0] == name {
            return Ok(());
        }
        let revision = if natives.iter().any(|native| native == name) {
            match std::mem::replace(selection, ModelSelection::Hidden) {
                ModelSelection::Revision(revision) => Some(revision),
                previous => {
                    *selection = previous;
                    return Err(Error::Conflict(format!(
                        "native declaration {name} requires a selected revision"
                    )));
                }
            }
        } else {
            None
        };
        *selection = ModelSelection::Native {
            revision,
            names: natives,
        };
        Ok(())
    }

    /// Hides a logical tool while retaining admitted revisions.
    pub fn remove_from_model(&mut self, name: &str) -> Result<()> {
        let selection = Arc::make_mut(&mut self.data)
            .selected
            .get_mut(name)
            .ok_or_else(|| Error::NotFound(format!("tool {name}")))?;
        *selection = ModelSelection::Hidden;
        Ok(())
    }

    /// Installs a tool after scope and exact approval checks.
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

    /// Resolves an explicit pinned revision or the model-visible selection.
    pub fn get(
        &self,
        name: &str,
        revision: Option<&str>,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Option<&Tool>> {
        self.get_shared(name, revision, io)
            .map(|tool| tool.map(Arc::as_ref))
    }

    /// Resolves the same selection while retaining the original executor owner.
    pub(crate) fn get_shared(
        &self,
        name: &str,
        revision: Option<&str>,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Option<&Arc<Tool>>> {
        if revision.is_none()
            && let Some(io) = io
        {
            io.charge(acyclic_fs::WorkCounters {
                items_examined: name.len() as u64,
                ..Default::default()
            })?;
        }
        let (name, revision) = match revision {
            Some(revision) => (name, revision),
            None => match name.rsplit_once('@') {
                Some(pinned) => pinned,
                None => match self.data.selected.get(name) {
                    Some(ModelSelection::Revision(revision))
                    | Some(ModelSelection::Native {
                        revision: Some(revision),
                        ..
                    }) => (name, revision.as_str()),
                    _ => return Ok(None),
                },
            },
        };
        crate::executor::with_history_io(io, |work| {
            if let Some(work) = work.as_deref_mut() {
                work.check_cancellation()?;
            }
            Ok(lookup::get(&self.data.versions, name, revision))
        })
    }

    /// Returns the unambiguous model selection in canonical order.
    pub fn definitions(
        &self,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Vec<ToolDefinition>> {
        self.definitions_in_namespace("", io)
    }

    /// Returns only the selected namespace, without resolving unrelated tools.
    pub fn definitions_in_namespace(
        &self,
        prefix: &str,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Vec<ToolDefinition>> {
        let mut definitions = self.definitions_for_names(
            || {
                self.data
                    .selected
                    .range::<str, _>((
                        std::ops::Bound::Included(prefix),
                        std::ops::Bound::Unbounded,
                    ))
                    .map(|(name, _)| name.as_str())
                    .take_while(move |name| name.starts_with(prefix))
            },
            false,
            io,
        )?;
        for source in &self.sources {
            for definition in source.snapshot()?.definitions_in_namespace(prefix, io)? {
                if self.source_allowed(&definition.name) {
                    crate::executor::stock_vec_push(&mut definitions, definition, io)?;
                }
            }
        }
        definitions.sort_unstable_by(|a, b| a.name.cmp(&b.name));
        if definitions
            .windows(2)
            .any(|pair| pair[0].name == pair[1].name)
        {
            return Err(Error::Conflict("tool sources overlap".into()));
        }
        Ok(definitions)
    }

    pub(crate) fn definitions_for_grants(
        &self,
        grants: &crate::Capabilities,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Vec<ToolDefinition>> {
        let mut definitions = self.definitions_for_names(
            || {
                grants
                    .iter()
                    .filter_map(|grant| grant.strip_prefix("tool:call:"))
            },
            true,
            io,
        )?;
        for source in &self.sources {
            let snapshot = source.snapshot()?;
            if source
                .capability()
                .is_some_and(|grant| grants.contains(grant))
            {
                for definition in snapshot.definitions(io)? {
                    if self.source_allowed(&definition.name) {
                        crate::executor::stock_vec_push(&mut definitions, definition, io)?;
                    }
                }
            } else {
                for definition in snapshot.definitions_for_grants(grants, io)? {
                    if self.source_allowed(&definition.name) {
                        crate::executor::stock_vec_push(&mut definitions, definition, io)?;
                    }
                }
            }
        }
        definitions.sort_unstable_by(|a, b| a.name.cmp(&b.name));
        if definitions
            .windows(2)
            .any(|pair| pair[0].name == pair[1].name)
        {
            return Err(Error::Conflict("tool sources overlap".into()));
        }
        Ok(definitions)
    }

    /// `capabilities` reads each name as a granted capability, offered as its
    /// native declarations when it has them.
    fn definitions_for_names<'a, I: Iterator<Item = &'a str>>(
        &'a self,
        names: impl Fn() -> I,
        capabilities: bool,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Vec<ToolDefinition>> {
        if let Some(io) = io {
            io.retain_owned(std::mem::size_of::<Vec<ToolDefinition>>() as u64)?;
        }
        let admitted = || {
            names()
                .flat_map(move |name| {
                    let natives = match self.data.selected.get(name) {
                        Some(ModelSelection::Native { names, .. }) if capabilities => Some(names),
                        _ => None,
                    };
                    let own = natives.is_none().then_some(name);
                    natives.into_iter().flatten().map(String::as_str).chain(own)
                })
                .filter_map(move |name| {
                    if let Err(error) = crate::model::charge_items(io, 1) {
                        return Some(Err(error));
                    }
                    match self.data.selected.get(name) {
                        Some(ModelSelection::Revision(revision))
                        | Some(ModelSelection::Native {
                            revision: Some(revision),
                            ..
                        }) => Some(Ok((name, revision))),
                        Some(
                            ModelSelection::Hidden | ModelSelection::Native { revision: None, .. },
                        )
                        | None => None,
                        Some(ModelSelection::Ambiguous) => {
                            Some(Err(crate::executor::stock_boundary(
                                io,
                                crate::wire::ErrorCode::Conflict,
                                "tool requires an explicit model revision",
                            )))
                        }
                    }
                })
        };
        let count = admitted().try_fold(0usize, |count, selection| selection.map(|_| count + 1))?;
        let mut definitions = crate::model::vec_with_capacity(count, io)?;
        for selection in admitted() {
            let (name, revision) = selection?;
            let tool = self.get(name, Some(revision), io)?.ok_or_else(|| {
                crate::executor::stock_boundary(
                    io,
                    crate::wire::ErrorCode::Storage,
                    "selected tool revision is not registered",
                )
            })?;
            let definition = crate::model::clone_tool_definition(&tool.definition, io)?;
            definitions.push(definition);
        }
        Ok(definitions)
    }
}
