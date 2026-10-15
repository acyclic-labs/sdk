use super::*;

impl ToolRegistry {
    pub(crate) fn pin_model_definitions(&mut self, definitions: &[ToolDefinition]) -> Result<()> {
        let mut selected = BTreeMap::new();
        for definition in definitions {
            let tool = self
                .get(&definition.name, Some(&definition.revision), None)?
                .ok_or_else(|| {
                    Error::NotFound("manifest tool revision is not registered".into())
                })?;
            if tool.definition != *definition {
                return Err(Error::Conflict(
                    "manifest tool definition differs from its executor".into(),
                ));
            }
            if selected
                .insert(
                    definition.name.clone(),
                    ModelSelection::Revision(definition.revision.clone()),
                )
                .is_some()
            {
                return Err(Error::Conflict("manifest repeats a tool name".into()));
            }
        }
        Arc::make_mut(&mut self.data).selected = selected;
        Ok(())
    }

    /// Selects one revision for future model requests.
    pub fn select_model_version(&mut self, name: &str, revision: &str) -> Result<()> {
        if !self
            .data
            .versions
            .contains_key(&(name.to_owned(), revision.to_owned()))
        {
            return Err(Error::NotFound(format!("tool {name}@{revision}")));
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

    /// Hides a logical tool while retaining admitted revisions.
    pub fn remove_from_model(&mut self, name: &str) -> Result<()> {
        if !self.data.selected.contains_key(name)
            && !self
                .data
                .versions
                .keys()
                .any(|(logical, _)| logical == name)
        {
            return Err(Error::NotFound(format!("tool {name}")));
        }
        Arc::make_mut(&mut self.data)
            .selected
            .insert(name.to_owned(), ModelSelection::Hidden);
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
        #[cfg(feature = "filesystem")]
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
                    Some(ModelSelection::Revision(revision)) => (name, revision.as_str()),
                    _ => return Ok(None),
                },
            },
        };
        Ok(self
            .data
            .versions
            .get(&(
                crate::executor::stock_string_copy(name, io)?,
                crate::executor::stock_string_copy(revision, io)?,
            ))
            .map(Arc::as_ref))
    }

    /// Returns the unambiguous model selection in canonical order.
    pub fn definitions(
        &self,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Vec<ToolDefinition>> {
        self.definitions_for_names(self.data.selected.keys().map(String::as_str), io)
    }

    pub(crate) fn definitions_for_grants(
        &self,
        grants: &crate::Capabilities,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Vec<ToolDefinition>> {
        self.definitions_for_names(
            grants
                .iter()
                .filter_map(|grant| grant.strip_prefix("tool:call:")),
            io,
        )
    }

    fn definitions_for_names<'a>(
        &self,
        names: impl Iterator<Item = &'a str>,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Vec<ToolDefinition>> {
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            io.retain_owned(std::mem::size_of::<Vec<ToolDefinition>>() as u64)?;
        }
        let mut definitions = Vec::new();
        for name in names {
            let revision = match self.data.selected.get(name) {
                Some(ModelSelection::Revision(revision)) => revision,
                Some(ModelSelection::Hidden) | None => continue,
                Some(ModelSelection::Ambiguous) => {
                    return Err(crate::executor::stock_boundary(
                        io,
                        crate::wire::ErrorCode::Conflict,
                        "tool requires an explicit model revision",
                    ));
                }
            };
            let tool = self.get(name, Some(revision), io)?.ok_or_else(|| {
                crate::executor::stock_boundary(
                    io,
                    crate::wire::ErrorCode::Storage,
                    "selected tool revision is not registered",
                )
            })?;
            let definition = crate::model::clone_tool_definition(&tool.definition, io)?;
            crate::executor::stock_vec_push(&mut definitions, definition, io)?;
        }
        Ok(definitions)
    }
}
