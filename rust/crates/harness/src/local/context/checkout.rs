use super::*;

fn archive_path(name: &str, bytes: &[u8]) -> String {
    format!("inputs/context/{}/{name}", blake3::hash(bytes))
}

impl LocalHarness {
    pub(super) async fn checkout_instructions(
        &self,
        workspace: &LocalWorkspace,
        names: &[String],
        maximum: u64,
        publisher: &dyn ContentPublisher,
    ) -> Result<DiscoveredContext> {
        // Bound archive paths before asking the source for instruction bodies.
        if names
            .iter()
            .any(|name| "inputs/context/".len() + 64 + 1 + name.len() > self.limits.path_bytes)
        {
            return Err(Error::Invalid(
                "instruction archive exceeds path bound".into(),
            ));
        }
        let (selected, pending) = workspace.selected_view().await?;
        if let Some((operation, _)) = pending {
            return Err(Error::Indeterminate(OperationId::from_bytes(
                operation.into_bytes(),
            )));
        }
        let view = selected.as_ref().unwrap_or_else(|| workspace.view());
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::BOUNDARY_BUDGET,
            &cancellation,
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let mut accounting = acyclic_fs::kernel::KernelAccounting::NativeOriginal {
            work: &work,
            cancellation: &cancellation,
        };
        let mut discovery = DiscoveredContext::default();
        for name in names {
            let bytes = match view
                .read_latest(&format!("/{name}"), maximum, &mut accounting)
                .await
            {
                Ok(bytes) => bytes,
                Err(acyclic_fs::LazyWorkspaceError::NotFound) => continue,
                Err(error) => return Err(error.into()),
            };
            #[cfg(test)]
            if let Some(observer) = self.context_read_observer {
                observer(self, name).await?;
            }
            ContextDiscoveryLimits {
                instruction_bytes: maximum,
                ..Default::default()
            }
            .validate_instruction(bytes.bytes.as_ref())?;
            // Content-addressed private copies retain exact input bytes without
            // authoring over the checkout or duplicating unchanged bodies.
            let path = archive_path(name, bytes.bytes.as_ref());
            let operation =
                publication_operation(b"acyclic.local.context-source/v1\0", path.as_bytes());
            let file = publisher
                .stage_once(
                    operation,
                    &path,
                    bytes.bytes.as_ref(),
                    "text/markdown",
                    name,
                    None,
                )?
                .await?
                .0;
            discovery
                .instructions
                .push(crate::model::ProjectInstruction::Captured {
                    file,
                    source: crate::model::InstructionIdentity::new(
                        self.project.clone(),
                        name.clone(),
                    )?,
                });
        }
        Ok(discovery)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        // Property: archives depend only on the selected name and exact bytes;
        // every generated name/body round-trips its path and content identity.
        #[test]
        fn archive_identity(name in prop::sample::select(vec!["AGENTS.md", "CLAUDE.md"]), bytes in prop::collection::vec(any::<u8>(), 0..128)) {
            let path = archive_path(name, &bytes);
            crate::conversation::validate_content_path(&path, &mut None)?;
            prop_assert!(path.len() <= Limits::default().path_bytes);
            let suffix = path.strip_prefix("inputs/context/").unwrap();
            let (digest, retained_name) = suffix.split_once('/').unwrap();
            prop_assert_eq!(retained_name, name);
            let expected = blake3::hash(&bytes).to_hex();
            prop_assert_eq!(digest, expected.as_str());
            prop_assert_eq!(path, archive_path(name, &bytes));
        }
    }
}
