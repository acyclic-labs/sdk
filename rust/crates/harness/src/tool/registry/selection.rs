use super::*;

impl ToolRegistry {
    /// Selects one revision for future model requests.
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

    /// Pins the complete dynamic catalog installation.
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

    /// Hides a logical tool while retaining admitted revisions.
    pub fn remove_from_model(&mut self, name: &str) -> Result<()> {
        if !self.versions.keys().any(|(logical, _)| logical == name) {
            return Err(Error::NotFound(format!("tool {name}")));
        }
        self.selected
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
                None => match self.selected.get(name) {
                    Some(ModelSelection::Revision(revision)) => (name, revision.as_str()),
                    _ => return Ok(None),
                },
            },
        };
        Ok(self.versions.get(&(
            crate::executor::stock_string_copy(name, io)?,
            crate::executor::stock_string_copy(revision, io)?,
        )))
    }

    /// Returns the unambiguous model selection in canonical order.
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
            let tool = self.get(name, Some(revision), io)?.ok_or_else(|| {
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
