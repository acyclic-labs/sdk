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

    #[cfg(test)]
    pub(super) async fn checkout_instructions(
        &self,
        workspace: &LocalWorkspace,
        names: &[String],
        maximum: u64,
        publisher: &dyn ContentPublisher,
    ) -> Result<DiscoveredContext> {
        let mut bounds = crate::context::DiscoveryWork {
            limits: ContextDiscoveryLimits {
                instruction_bytes: maximum,
                ..Default::default()
            },
            ..Default::default()
        };
        self.checkout_instructions_with_work(workspace, names, publisher, &mut bounds)
            .await
    }

    pub(super) async fn checkout_instructions_with_work(
        &self,
        workspace: &LocalWorkspace,
        names: &[String],
        publisher: &dyn ContentPublisher,
        bounds: &mut crate::context::DiscoveryWork,
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
        self.authorize_checkout_context(view.workspace(), &io)
            .await?;
        let cut = view.snapshot(&mut accounting).await?;
        let mut source = CheckoutInstructions {
            harness: self,
            publisher,
            directory: crate::context::native_discovery::LazyDiscovery {
                view,
                cut,
                work: &work,
            },
        };
        let instructions = crate::context::capture_instructions(
            &mut source,
            "",
            &self.active_directory,
            names,
            bounds,
            Some(&cut),
        )
        .await?;
        self.authorize_checkout_context(view.workspace(), &io)
            .await?;
        let discovery = DiscoveredContext {
            instructions,
            skills: Vec::new(),
        };
        Ok(discovery)
    }
}

struct CheckoutInstructions<'a> {
    harness: &'a LocalHarness,
    publisher: &'a dyn ContentPublisher,
    directory: crate::context::native_discovery::LazyDiscovery<'a>,
}
impl crate::context::DiscoverySource for CheckoutInstructions<'_> {
    type Revision = acyclic_fs::LazySnapshotRef;
    type Cursor = acyclic_fs::LazyDirectoryCursor;
    fn page<'a>(
        &'a mut self,
        path: &'a str,
        revision: Option<&'a Self::Revision>,
        cursor: Option<Self::Cursor>,
        maximum: u32,
    ) -> acyclic_stream::BoxProviderFuture<
        'a,
        Result<crate::context::DiscoveryPage<Self::Revision, Self::Cursor>>,
    > {
        self.directory.page(path, revision, cursor, maximum)
    }
}
impl crate::context::InstructionReader for CheckoutInstructions<'_> {
    fn instruction<'a>(
        &'a mut self,
        path: &'a str,
        cut: &'a Self::Revision,
        maximum: u64,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<crate::model::ProjectInstruction>> {
        Box::pin(async move {
            let archive_length = "inputs/context/".len() + 64 + 1 + path.len();
            if archive_length > self.harness.limits.path_bytes {
                return Err(Error::Invalid(
                    "instruction archive exceeds path bound".into(),
                ));
            }
            let mut accounting = acyclic_fs::kernel::KernelAccounting::NativeOriginal {
                work: self.directory.work,
                cancellation: self.directory.work.cancellation(),
            };
            let (bytes, _allocation) = self
                .directory
                .view
                .read_at_snapshot(*cut, &format!("/{path}"), None, maximum, &mut accounting)
                .await?;
            ContextDiscoveryLimits {
                instruction_bytes: maximum,
                ..Default::default()
            }
            .validate_instruction(bytes.bytes.as_ref())?;
            #[cfg(test)]
            if let Some(observer) = self.harness.context_read_observer {
                observer(self.harness, path).await?;
            }
            let archived = archive_path(path, bytes.bytes.as_ref());
            let operation =
                publication_operation(b"acyclic.local.context-source/v1\0", archived.as_bytes());
            let file = self
                .publisher
                .stage_once(
                    operation,
                    &archived,
                    bytes.bytes.as_ref(),
                    "text/markdown",
                    path.rsplit('/').next().unwrap_or(path),
                    crate::conversation::ContentAccess::default(),
                )?
                .await?
                .0;
            Ok(crate::model::ProjectInstruction::Captured {
                file,
                source: crate::model::InstructionIdentity::new(
                    self.harness.project.clone(),
                    path.into(),
                )?,
            })
        })
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
