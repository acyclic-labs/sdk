use super::*;

impl ToolRegistry {
    pub(crate) fn pin_model_definitions(
        &mut self,
        definitions: &[ToolDefinition],
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        use crate::{executor::stock_boundary, wire::ErrorCode};
        crate::executor::with_history_io(io, |work| -> Result<()> {
            let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
            let mut selected =
                crate::core::reserve_copy_vector::<(&str, &str)>(definitions.len(), &mut quota)?;
            for definition in definitions {
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
                #[cfg(feature = "filesystem")]
                let old_bytes = match selection {
                    ModelSelection::Revision(revision) => revision.capacity(),
                    _ => 0,
                };
                *selection = replacement;
                #[cfg(feature = "filesystem")]
                if let Some(io) = io {
                    io.release(old_bytes as u64)?;
                }
            }
            Ok(())
        })
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
        if io.is_none() {
            return Ok(lookup::get(&self.data.versions, name, revision).map(Arc::as_ref));
        }
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
        self.definitions_for_names(|| self.data.selected.keys().map(String::as_str), io)
    }

    pub(crate) fn definitions_for_grants(
        &self,
        grants: &crate::Capabilities,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Vec<ToolDefinition>> {
        self.definitions_for_names(
            || {
                grants
                    .iter()
                    .filter_map(|grant| grant.strip_prefix("tool:call:"))
                    .filter(|name| {
                        !crate::tools::children::family(name) || grants.contains("tool:call:spawn")
                    })
            },
            io,
        )
    }

    fn definitions_for_names<'a, I: Iterator<Item = &'a str>>(
        &self,
        names: impl Fn() -> I,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Vec<ToolDefinition>> {
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            io.retain_owned(std::mem::size_of::<Vec<ToolDefinition>>() as u64)?;
        }
        let admitted = || {
            names().filter_map(move |name| {
                if let Err(error) = crate::model::charge_items(io, 1) {
                    return Some(Err(error));
                }
                match self.data.selected.get(name) {
                    Some(ModelSelection::Revision(revision)) => Some(Ok((name, revision))),
                    Some(ModelSelection::Hidden) | None => None,
                    Some(ModelSelection::Ambiguous) => Some(Err(crate::executor::stock_boundary(
                        io,
                        crate::wire::ErrorCode::Conflict,
                        "tool requires an explicit model revision",
                    ))),
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
