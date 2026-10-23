use super::*;

fn archive_path(name: &str, bytes: &[u8]) -> String {
    format!("inputs/context/{}/{name}", blake3::hash(bytes))
}

impl LocalHarness {
    async fn authorize_checkout_context(
        &self,
        workspace: &acyclic_fs::Workspace<LocalAuthorityBackend, LocalObjectBackend>,
        io: &crate::filesystem::PreparationIo,
    ) -> Result<()> {
        crate::filesystem::require_unrestricted_workspace_read(
            workspace,
            &self.verifier,
            &self.signed,
            io,
            "checkout context",
        )
        .await
    }

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
        let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
        let mut discovery = DiscoveredContext::default();
        for name in names {
            self.authorize_checkout_context(view.workspace(), &io)
                .await?;
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
            self.authorize_checkout_context(view.workspace(), &io)
                .await?;
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
                    crate::conversation::ContentAccess::default(),
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

    #[tokio::test]
    async fn checkout_instructions_reject_restrictive_authority_before_disclosing_bytes()
    -> Result<()> {
        use acyclic_fs::access::{Action, Capability, Generations, Grant, ResourcePattern};
        let directory = tempfile::tempdir().map_err(storage)?;
        let project = crate::local::tests::volume(directory.path()).await?;
        let provider = Arc::new(crate::test_support::ScriptedModelProvider::new([]));
        let mut harness = LocalHarness::open(
            crate::local::tests::volume_config(directory.path(), project, provider)?
                .without_default_tools(),
        )
        .await?;
        let workspace = harness
            .workspace
            .as_ref()
            .expect("fixture workspace")
            .clone();
        std::fs::write(workspace.path.join("AGENTS.md"), b"private instructions")
            .map_err(storage)?;
        let names = ["AGENTS.md".to_owned()];
        let found = harness
            .checkout_instructions(
                &workspace,
                &names,
                1024,
                harness.input_publisher()?.as_ref(),
            )
            .await?;
        assert_eq!(found.instructions.len(), 1);
        let label = Capability {
            layers: vec![vec![Grant {
                actions: Action::ALL.into_iter().collect(),
                resource: ResourcePattern {
                    lineage_root: None,
                    volume: None,
                    generations: Generations::All,
                    path: "public/**".into(),
                },
            }]],
            expires_at: None,
        }
        .to_scope_label()
        .map_err(storage)?;
        let issuer = AuthorityIssuer::new(
            "context-test",
            [19; 32],
            harness.verifier.audience().clone(),
        );
        harness.signed = issuer.issue(
            "context-test",
            &Capabilities::new(harness.signed.capabilities().iter().chain([label.as_str()])),
            harness.signed.agent(),
            None,
            &mut None,
        )?;
        harness.verifier = issuer.verifier(&mut None)?;
        assert!(matches!(
            harness
                .checkout_instructions(
                    &workspace,
                    &names,
                    1024,
                    harness.input_publisher()?.as_ref(),
                )
                .await,
            Err(Error::FilesystemAccessConfiguration("checkout context"))
        ));
        Ok(())
    }

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
