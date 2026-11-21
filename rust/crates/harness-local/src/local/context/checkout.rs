use super::*;

fn archive_path(name: &str, bytes: &[u8]) -> String {
    format!(
        "{}/{}/{name}",
        ContextEpoch::INSTRUCTIONS_DIRECTORY,
        blake3::hash(bytes)
    )
}

impl LocalHarness {
    /// Admits reads of the primary subtree at exactly `cut`, the one snapshot
    /// every checkout context read uses, whatever the head has become since.
    pub(super) async fn authorize_checkout_context(
        &self,
        physical: &LocalWorkspace,
        workspace: &acyclic_fs::Workspace<LocalAuthorityBackend, LocalObjectBackend>,
        cut: &acyclic_fs::LazySnapshotRef,
        io: &crate::filesystem::PreparationIo,
        authority: (&AuthorityVerifier, &Scope),
    ) -> Result<()> {
        let primary = physical.mounts.named(None)?.1;
        crate::filesystem::require_workspace_subtree_read(
            workspace,
            &primary.view.volume,
            &primary.view.root,
            cut.authored_generation,
            authority,
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
        self.checkout_instructions_with_work(
            workspace,
            names,
            publisher,
            &mut bounds,
            (&self.verifier, &self.signed),
        )
        .await
    }

    pub(super) async fn checkout_instructions_with_work(
        &self,
        workspace: &LocalWorkspace,
        names: &[String],
        publisher: &dyn ContentPublisher,
        bounds: &mut crate::context::DiscoveryWork,
        authority: (&AuthorityVerifier, &Scope),
    ) -> Result<DiscoveredContext> {
        // Bound archive paths before asking the source for instruction bodies.
        if names.iter().any(|name| {
            self.limits.path_bytes.is_some_and(|maximum| {
                ContextEpoch::INSTRUCTIONS_DIRECTORY.len() + 1 + 64 + 1 + name.len() > maximum.get()
            })
        }) {
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
            acyclic_fs_work::performance::BOUNDARY_BUDGET,
            &cancellation,
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let mut accounting = acyclic_fs::kernel::KernelAccounting::native(&work, &cancellation);
        let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
        let cut = view.snapshot(&mut accounting).await?;
        self.authorize_checkout_context(workspace, view.workspace(), &cut, &io, authority)
            .await?;
        let mut source = CheckoutInstructions {
            harness: self,
            volume: &workspace.mounts.named(None)?.1.view.volume,
            publisher,
            directory: crate::context::native_discovery::LazyDiscovery {
                view,
                cut: cut.clone(),
                cancellation: &cancellation,
            },
        };
        let instructions = crate::context::capture_instructions(
            &mut source,
            &workspace.mounts.named(None)?.1.view.root,
            &self.active_directory,
            names,
            bounds,
            Some(&cut),
        )
        .await?;
        self.authorize_checkout_context(workspace, view.workspace(), &cut, &io, authority)
            .await?;
        let discovery = DiscoveredContext {
            instructions,
            skills: Vec::new(),
        };
        Ok(discovery)
    }
    /// Each of `paths` present in the selected checkout view, at one
    /// snapshot under the checkout context's read authority: its index and
    /// bytes, at most `maximum` of each.
    pub(super) async fn checkout_files(
        &self,
        workspace: &LocalWorkspace,
        paths: &[String],
        maximum: Option<u64>,
    ) -> Result<Vec<(usize, Vec<u8>)>> {
        let (selected, pending) = workspace.selected_view().await?;
        if let Some((operation, _)) = pending {
            return Err(Error::Indeterminate(OperationId::from_bytes(
                operation.into_bytes(),
            )));
        }
        let view = selected.as_ref().unwrap_or_else(|| workspace.view());
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs_work::performance::BOUNDARY_BUDGET,
            &cancellation,
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let mut accounting = acyclic_fs::kernel::KernelAccounting::native(&work, &cancellation);
        let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
        let cut = view.snapshot(&mut accounting).await?;
        self.authorize_checkout_context(
            workspace,
            view.workspace(),
            &cut,
            &io,
            (&self.verifier, &self.signed),
        )
        .await?;
        let mut found = Vec::new();
        for (index, path) in paths.iter().enumerate() {
            let source_path = workspace
                .mounts
                .named(None)?
                .1
                .source_path(path, Some(&work))?;
            match view
                .read_at_snapshot(
                    &cut,
                    &format!("/{source_path}"),
                    None,
                    maximum,
                    &mut accounting,
                )
                .await
            {
                Ok((chunk, _)) => found.push((index, chunk.bytes.as_ref().to_vec())),
                Err(acyclic_fs::LazyWorkspaceError::NotFound) => {}
                Err(error) => return Err(error.into()),
            }
        }
        // The bytes disclose only what the authority still admits.
        self.authorize_checkout_context(
            workspace,
            view.workspace(),
            &cut,
            &io,
            (&self.verifier, &self.signed),
        )
        .await?;
        Ok(found)
    }
}

impl LocalHarness {
    /// Copies selected checkout files at one snapshot through the host input
    /// publisher. Native source authorization is checked before capture and
    /// after publication, under the original checkout admission and byte bound.
    pub(super) async fn capture_checkout_mentions(
        &self,
        workspace: &LocalWorkspace,
        text: &str,
        base: &str,
        publisher: &dyn ContentPublisher,
        operation: OperationId,
    ) -> Result<crate::conversation::ContentChanges> {
        let (selected, pending) = workspace.selected_view().await?;
        if let Some((operation, _)) = pending {
            return Err(Error::Indeterminate(OperationId::from_bytes(
                operation.into_bytes(),
            )));
        }
        let view = selected.as_ref().unwrap_or_else(|| workspace.view());
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs_work::performance::BOUNDARY_BUDGET,
            &cancellation,
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let mut accounting = acyclic_fs::kernel::KernelAccounting::native(&work, &cancellation);
        let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
        let cut = view.snapshot(&mut accounting).await?;
        self.authorize_checkout_context(
            workspace,
            view.workspace(),
            &cut,
            &io,
            (&self.verifier, &self.signed),
        )
        .await?;
        let mut remaining = self.limits.file_bytes;
        let mut copies = std::collections::BTreeMap::new();
        for (_, _, path) in crate::client::file_mention_paths(text)? {
            if copies.contains_key(path) {
                continue;
            }
            let source_path = workspace
                .mounts
                .named(None)?
                .1
                .source_path(path, Some(&work))?;
            let (chunk, _allocation) = view
                .read_at_snapshot(
                    &cut,
                    &format!("/{source_path}"),
                    None,
                    remaining,
                    &mut accounting,
                )
                .await?;
            if let Some(remaining) = &mut remaining {
                *remaining = remaining
                    .checked_sub(chunk.bytes.as_ref().len() as u64)
                    .ok_or_else(|| {
                        Error::Invalid("mentioned files exceed the byte bound".into())
                    })?;
            }
            // Native checkout bytes carry their own allocation meter, not a
            // preparation-ledger charge.
            copies.insert(
                path.to_owned(),
                crate::conversation::Custody::Unmetered(chunk.bytes),
            );
            #[cfg(test)]
            if let Some(observer) = self.context_read_observer {
                observer(self, path).await?;
            }
        }
        // The copies disclose only what the authority still admits, and are
        // saved only then: a replay of `operation` returns saved copies
        // without authorizing again, so nothing refused may be saved.
        self.authorize_checkout_context(
            workspace,
            view.workspace(),
            &cut,
            &io,
            (&self.verifier, &self.signed),
        )
        .await?;
        self.publish_mention_copies(base, copies, publisher, operation)
            .await
    }
}

struct CheckoutInstructions<'a> {
    harness: &'a LocalHarness,
    volume: &'a VolumeRef,
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
        Result<Option<crate::context::DiscoveryPage<Self::Revision, Self::Cursor>>>,
    > {
        self.directory.page(path, revision, cursor, maximum)
    }
}
impl crate::context::InstructionReader for CheckoutInstructions<'_> {
    fn instruction_directory<'a>(
        &'a mut self,
        path: &'a str,
        revision: Option<&'a Self::Revision>,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<Option<(Self::Revision, u32)>>> {
        Box::pin(async move {
            let cut = revision.unwrap_or(&self.directory.cut);
            // Each stat and read is one unit of native work.
            let work = &crate::context::native_discovery::native_work(self.directory.cancellation)?;
            let mut accounting =
                acyclic_fs::kernel::KernelAccounting::native(work, self.directory.cancellation);
            let Some(stat) = self
                .directory
                .view
                .stat_at_snapshot(cut, &format!("/{path}"), &mut accounting)
                .await?
            else {
                return Ok(None);
            };
            if stat.kind != acyclic_fs::demand::SourceNodeKind::Directory {
                return Err(Error::Invalid(
                    "instruction entry is not a directory".into(),
                ));
            }
            Ok(Some((cut.clone(), 0)))
        })
    }

    fn instruction<'a>(
        &'a mut self,
        path: &'a str,
        cut: &'a Self::Revision,
        maximum: u64,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<Option<crate::model::ProjectInstruction>>>
    {
        Box::pin(async move {
            let archive_length =
                ContextEpoch::INSTRUCTIONS_DIRECTORY.len() + 1 + 64 + 1 + path.len();
            if self
                .harness
                .limits
                .path_bytes
                .is_some_and(|maximum| archive_length > maximum.get())
            {
                return Err(Error::Invalid(
                    "instruction archive exceeds path bound".into(),
                ));
            }
            // Each stat and read is one unit of native work.
            let work = &crate::context::native_discovery::native_work(self.directory.cancellation)?;
            let mut accounting =
                acyclic_fs::kernel::KernelAccounting::native(work, self.directory.cancellation);
            let (bytes, _allocation) = match self
                .directory
                .view
                .read_at_snapshot(cut, &format!("/{path}"), None, maximum, &mut accounting)
                .await
            {
                Ok(file) => file,
                Err(acyclic_fs::LazyWorkspaceError::NotFound) => return Ok(None),
                Err(acyclic_fs::LazyWorkspaceError::NotRegularFile) => {
                    return Err(Error::Invalid(
                        "instruction entry is not a regular file".into(),
                    ));
                }
                Err(error) => return Err(error.into()),
            };
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
            Ok(Some(crate::model::ProjectInstruction::Captured {
                file,
                source: crate::model::InstructionIdentity::new(self.volume.clone(), path.into())?,
            }))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[tokio::test]
    async fn rooted_checkout_context_uses_only_the_selected_subtree() -> Result<()> {
        let directory = tempfile::tempdir().map_err(storage)?;
        let state = tempfile::tempdir().map_err(storage)?;
        std::fs::create_dir_all(directory.path().join("selected/src")).map_err(storage)?;
        for (path, bytes) in [
            ("AGENTS.md", "outside instructions"),
            ("selected/AGENTS.md", "selected instructions"),
            ("selected/src/AGENTS.md", "nested instructions"),
            ("sample.txt", "outside mention"),
            ("selected/sample.txt", "selected mention"),
        ] {
            std::fs::write(directory.path().join(path), bytes).map_err(storage)?;
        }
        for (root, name) in [("", "outside"), ("selected", "inside"), ("peer", "peer")] {
            let skill = directory
                .path()
                .join(root)
                .join(crate::context::PROJECT_SKILL_ROOTS[0])
                .join(name);
            std::fs::create_dir_all(&skill).map_err(storage)?;
            std::fs::write(
                skill.join("SKILL.md"),
                format!("---\nname: {name}\ndescription: scoped skill\n---\n{name} body"),
            )
            .map_err(storage)?;
        }
        let mut harness = LocalHarness::open(
            crate::LocalHarnessConfig::native_host(directory.path())
                .workspace_mounting(crate::local::WorkspaceMounting::Physical)
                .state_directory(state.path().join("runtime")),
        )
        .await?;
        harness.active_directory = "src".into();
        let workspace = harness.workspace.as_ref().expect("fixture workspace");
        let mut bindings = harness.session_bindings().to_vec();
        bindings[0].name = "repository".into();
        bindings[0].root = "selected".into();
        bindings[0].mode = crate::agent::WorkspaceMode::ReadOnly;
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs_work::performance::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let view = workspace
            .view()
            .open_related(
                workspace.view().workspace().clone(),
                &mut acyclic_fs::kernel::KernelAccounting::native(&work, &cancellation),
            )
            .await
            .map_err(storage)?;
        let selected = workspace
            .bound_task_view(
                view,
                &bindings,
                &crate::local::workspace::owner::AgentOwner,
                workspace
                    .mount_authority
                    .as_ref()
                    .expect("fixture mount authority"),
            )
            .await?;
        let issuer = harness.root_authority.as_ref().expect("fixture signer");
        let sources = crate::context::native_skills::NativeSkills::new(
            Arc::new(acyclic_stream::MemoryStream::default()),
            harness.journal.clone(),
            Some((
                selected.view(),
                selected.filesystem.clone(),
                selected.store.clone(),
            )),
            Vec::new(),
        )?;
        let cut = selected
            .view()
            .snapshot(&mut acyclic_fs::kernel::KernelAccounting::native(
                &work,
                &cancellation,
            ))
            .await?;
        for (root, expected) in [
            ("selected", "inside"),
            ("peer", "peer"),
            ("selected", "inside"),
        ] {
            let skills = sources
                .catalog(
                    workspace.source_root.workspace(),
                    selected.view(),
                    &cut,
                    root,
                    &mut Default::default(),
                )?
                .await?;
            assert_eq!(
                skills
                    .iter()
                    .map(|skill| skill.name.as_str())
                    .collect::<Vec<_>>(),
                [expected],
                "same-snapshot catalog cache crossed selected roots"
            );
        }
        assert!(
            sources
                .catalog(
                    workspace.source_root.workspace(),
                    selected.view(),
                    &cut,
                    "../peer",
                    &mut Default::default()
                )?
                .await
                .is_err()
        );
        let scope = issuer.bootstrap_scope(
            "selected-context",
            &Capabilities::new([harness
                .project
                .directory_read_capability("selected", &mut None)?]),
            Some(harness.root),
            &mut None,
        )?;
        let publisher = harness.input_publisher_with_limit(1024)?;
        let found = harness
            .checkout_instructions_with_work(
                &selected,
                &["AGENTS.md".into()],
                publisher.as_ref(),
                &mut Default::default(),
                (&harness.verifier, &scope),
            )
            .await?;
        assert_eq!(found.instructions.len(), 2);
        let reader = harness.context_reader()?;
        for (instruction, (path, bytes)) in found.instructions.iter().zip([
            ("selected/AGENTS.md", b"selected instructions".as_slice()),
            ("selected/src/AGENTS.md", b"nested instructions".as_slice()),
        ]) {
            let crate::model::ProjectInstruction::Captured { file, source } = instruction else {
                return Err(Error::Invalid("instruction was not captured".into()));
            };
            assert_eq!(
                source,
                &crate::model::InstructionIdentity::new(harness.project.clone(), path.into())?
            );
            assert_eq!(reader.read(file, 1024, None, None)?.await?.as_ref(), bytes);
        }
        let denied = issuer.bootstrap_scope(
            "peer-context",
            &Capabilities::new([harness
                .project
                .directory_read_capability("selected-peer", &mut None)?]),
            Some(harness.root),
            &mut None,
        )?;
        assert!(matches!(
            harness
                .checkout_instructions_with_work(
                    &selected,
                    &["AGENTS.md".into()],
                    publisher.as_ref(),
                    &mut Default::default(),
                    (&harness.verifier, &denied),
                )
                .await,
            Err(Error::FilesystemAccessConfiguration("checkout context"))
        ));
        {
            use acyclic_fs::access::{Action, Capability, Generations, Grant, ResourcePattern};
            for (paths, admitted) in [
                (["selected", "selected/**"], true),
                (["selected", "selected/src/**"], false),
                (["selected-peer", "selected-peer/**"], false),
            ] {
                let label = Capability {
                    layers: vec![
                        paths
                            .into_iter()
                            .map(|path| Grant {
                                actions: [Action::Read, Action::List].into(),
                                resource: ResourcePattern {
                                    lineage_root: None,
                                    volume: None,
                                    generations: Generations::All,
                                    path: path.into(),
                                },
                            })
                            .collect(),
                    ],
                    expires_at: None,
                }
                .to_scope_label()
                .map_err(storage)?;
                let scope = issuer.bootstrap_scope(
                    "scoped-context-policy",
                    &Capabilities::new([label]),
                    Some(harness.root),
                    &mut None,
                )?;
                let result = harness
                    .checkout_instructions_with_work(
                        &selected,
                        &["AGENTS.md".into()],
                        publisher.as_ref(),
                        &mut Default::default(),
                        (&harness.verifier, &scope),
                    )
                    .await;
                if admitted {
                    assert_eq!(result?.instructions.len(), 2);
                } else {
                    assert!(matches!(
                        result,
                        Err(Error::FilesystemAccessConfiguration("checkout context"))
                    ));
                }
            }
        }
        assert_eq!(
            harness
                .checkout_files(&selected, &["sample.txt".into()], Some(1024))
                .await?,
            vec![(0, b"selected mention".to_vec())]
        );
        assert!(
            harness
                .checkout_files(&selected, &["../sample.txt".into()], Some(1024))
                .await
                .is_err()
        );
        let copies = harness
            .capture_checkout_mentions(
                &selected,
                "@sample.txt",
                "inputs/rooted",
                publisher.as_ref(),
                OperationId::new(),
            )
            .await?;
        assert_eq!(copies.files().count(), 1);
        let copy = copies.files().next().expect("captured mention");
        assert_eq!(
            reader.read(copy, 1024, None, None)?.await?.as_ref(),
            b"selected mention"
        );
        let facts = crate::local::context::environment::gather(
            selected.native_cwd()?,
            0,
            &crate::native_environment::PlatformPolicy::Fixture {
                os: "fixture".into(),
                shell: "fixture".into(),
            },
        )?;
        assert_eq!(
            std::fs::canonicalize(selected.native_cwd()?.expect("selected native cwd"))
                .map_err(storage)?,
            std::fs::canonicalize(directory.path().join("selected")).map_err(storage)?
        );
        assert_eq!(facts.cwd.as_deref(), Some("."));
        assert_ne!(selected.native_cwd()?, Some(workspace.path.as_path()));
        Ok(())
    }

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
                harness.input_publisher_with_limit(1024)?.as_ref(),
            )
            .await?;
        assert_eq!(found.instructions.len(), 1);
        let issuer = AuthorityRoot::new(
            "context-test",
            [19; 32],
            harness.verifier.audience().clone(),
        );
        let verifier = issuer.verifier(&mut None)?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs_work::performance::BOUNDARY_BUDGET,
            &cancellation,
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
        let cut = workspace
            .view()
            .snapshot(&mut acyclic_fs::kernel::KernelAccounting::native(
                &work,
                &cancellation,
            ))
            .await?;
        let volume = &workspace.mounts.named(None)?.1.view.volume;
        let read = volume.capability(crate::conversation::VolumeOperation::Read, &mut None)?;
        let write = volume.capability(crate::conversation::VolumeOperation::Write, &mut None)?;
        let other = crate::conversation::VolumeRef::new(
            volume.provider().clone(),
            format!("{}-other", volume.id()),
            volume.class(),
            volume.owner().clone(),
            &mut None,
        )?
        .capability(crate::conversation::VolumeOperation::Read, &mut None)?;
        let journal = VolumeRef::new(
            volume.provider().clone(),
            "journal".to_owned(),
            crate::conversation::VolumeClass::AgentPrivate,
            crate::conversation::VolumeOwner::Agent(harness.root),
            &mut None,
        )?;
        let published =
            journal.directory_read_capability(ContextEpoch::INSTRUCTIONS_DIRECTORY, &mut None)?;
        for (grants, admitted) in [
            (Capabilities::new([read.as_str()]), true),
            (Capabilities::new(["model:generate"]), false),
            (Capabilities::new([write.as_str()]), false),
            (Capabilities::new([other.as_str()]), false),
        ] {
            let scope = issuer.bootstrap_scope(
                "context-test",
                &Capabilities::new(grants.iter().chain([published.as_str()])),
                harness.signed.agent(),
                &mut None,
            )?;
            let result = harness
                .authorize_checkout_context(
                    &workspace,
                    workspace.view().workspace(),
                    &cut,
                    &io,
                    (&verifier, &scope),
                )
                .await;
            if admitted {
                result?;
            } else {
                assert!(matches!(
                    result,
                    Err(Error::FilesystemAccessConfiguration("checkout context"))
                ));
            }
        }
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
        harness.signed = issuer.bootstrap_scope(
            "context-test",
            &Capabilities::new(
                harness
                    .signed
                    .capabilities()
                    .iter()
                    .chain([label.as_str(), published.as_str()]),
            ),
            harness.signed.agent(),
            &mut None,
        )?;
        harness.verifier = verifier;
        assert!(matches!(
            harness
                .checkout_instructions(
                    &workspace,
                    &names,
                    1024,
                    harness.input_publisher_with_limit(1024)?.as_ref(),
                )
                .await,
            Err(Error::FilesystemAccessConfiguration("checkout context"))
        ));
        Ok(())
    }

    /// Commits one more authored generation while a checkout context read is
    /// in flight, so the head moves between capture and the closing check.
    fn advance_head<'a>(
        harness: &'a LocalHarness,
        _: &'a str,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<()>> {
        Box::pin(async move {
            let cancellation = acyclic_fs::CancellationToken::new();
            let workspace = harness.workspace.as_ref().expect("fixture workspace");
            let mut checkout = workspace
                .view()
                .workspace()
                .checkout(
                    acyclic_fs::model::GenerationSelector::Head,
                    acyclic_fs::model::CheckoutMode::tracking_transaction(),
                    acyclic_fs_work::performance::WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await
                .map_err(storage)?
                .value;
            let path = acyclic_fs::kernel::NamespacePath::new(
                vec![
                    acyclic_fs::kernel::LogicalName::new(
                        acyclic_fs::kernel::NameEncoding::Utf8,
                        b"advanced".to_vec(),
                    )
                    .map_err(storage)?,
                ],
                checkout.volume_config().limits,
            )
            .map_err(storage)?;
            checkout
                .create(
                    acyclic_fs::AuthoredMutation::CreateFile {
                        file_id: None,
                        path,
                        bytes: bytes::Bytes::from_static(b"advanced").into(),
                        metadata: acyclic_fs_format::metadata::FileMetadata::default(),
                    },
                    acyclic_fs_work::performance::WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await
                .map_err(storage)?;
            checkout
                .commit(
                    acyclic_fs_format::identity::OperationId::new(),
                    acyclic_fs::PublicationIntent::guarded(
                        None,
                        acyclic_fs::PublicationPermit::Unrestricted,
                    ),
                    acyclic_fs_work::performance::WorkBudget::UNBOUNDED,
                    &cancellation,
                    |_| {
                        std::future::ready(Ok::<
                            _,
                            acyclic_fs_work::performance::OperationFailure<acyclic_fs::FsError>,
                        >(
                            acyclic_fs_work::performance::OperationReceipt {
                                value: (),
                                work: acyclic_fs_work::performance::WorkCounters::UNCHARGED,
                            },
                        ))
                    },
                )
                .await
                .map_err(storage)?;
            Ok(())
        })
    }

    /// Publication ordinal of `generation` in the fixture workspace.
    async fn ordinal(
        workspace: &LocalWorkspace,
        generation: acyclic_fs_format::object_reference::GenerationId,
    ) -> Result<u64> {
        let cancellation = acyclic_fs::CancellationToken::new();
        let head = workspace
            .view()
            .workspace()
            .checkout(
                acyclic_fs::model::GenerationSelector::Head,
                acyclic_fs::model::CheckoutMode::read_only_pinned(),
                acyclic_fs_work::performance::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(storage)?
            .value;
        Ok(head
            .generation_position(
                generation,
                acyclic_fs_work::performance::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(storage)?
            .value
            .0
            .get())
    }

    // A grant pinned to the captured generation admits that cut even after the
    // head moves past it mid-read; evaluated at the moved head it admits nothing.
    #[tokio::test]
    async fn checkout_context_reads_stay_fenced_to_the_captured_generation() -> Result<()> {
        use acyclic_fs::access::{
            Action, Capability, GenerationRange, Generations, Grant, ResourcePattern,
        };
        let directory = tempfile::tempdir().map_err(storage)?;
        let state = tempfile::tempdir().map_err(storage)?;
        std::fs::write(directory.path().join("AGENTS.md"), b"captured instructions")
            .map_err(storage)?;
        let mut harness = LocalHarness::open(
            crate::LocalHarnessConfig::native_host(directory.path())
                .workspace_mounting(crate::local::WorkspaceMounting::Physical)
                .state_directory(state.path().join("runtime")),
        )
        .await?;
        harness.context_read_observer = Some(advance_head);
        let workspace = harness
            .workspace
            .as_ref()
            .expect("fixture workspace")
            .clone();
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs_work::performance::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let captured = workspace
            .view()
            .snapshot(&mut acyclic_fs::kernel::KernelAccounting::native(
                &work,
                &cancellation,
            ))
            .await?
            .authored_generation;
        let pinned = ordinal(&workspace, captured).await?;
        let label = Capability {
            layers: vec![vec![Grant {
                actions: [Action::Read, Action::List, Action::ReadHistory].into(),
                resource: ResourcePattern {
                    lineage_root: None,
                    volume: None,
                    generations: Generations::Range(GenerationRange {
                        first: pinned,
                        last: pinned,
                    }),
                    path: "**".into(),
                },
            }]],
            expires_at: None,
        }
        .to_scope_label()
        .map_err(storage)?;
        let issuer = harness.root_authority.as_ref().expect("fixture signer");
        let scope = issuer.bootstrap_scope(
            "pinned-context-policy",
            &Capabilities::new([label]),
            Some(harness.root),
            &mut None,
        )?;
        let publisher = harness.input_publisher_with_limit(1024)?;
        let found = harness
            .checkout_instructions_with_work(
                &workspace,
                &["AGENTS.md".into()],
                publisher.as_ref(),
                &mut Default::default(),
                (&harness.verifier, &scope),
            )
            .await?;
        let head = workspace
            .view()
            .snapshot(&mut acyclic_fs::kernel::KernelAccounting::native(
                &work,
                &cancellation,
            ))
            .await?
            .authored_generation;
        assert!(
            ordinal(&workspace, head).await? > pinned,
            "the head did not move during the read"
        );
        let [crate::model::ProjectInstruction::Captured { file, .. }] =
            found.instructions.as_slice()
        else {
            return Err(Error::Invalid("instruction was not captured".into()));
        };
        assert_eq!(
            harness
                .context_reader()?
                .read(file, 1024, None, None)?
                .await?
                .as_ref(),
            b"captured instructions"
        );
        Ok(())
    }

    // A capture refused by its closing check saves no copies: a replay of its
    // operation returns saved copies without authorizing again.
    #[tokio::test]
    async fn a_refused_mention_capture_saves_nothing_a_replay_could_return() -> Result<()> {
        use acyclic_fs::access::{Action, Capability, Generations, Grant, ResourcePattern};
        let directory = tempfile::tempdir().map_err(storage)?;
        let state = tempfile::tempdir().map_err(storage)?;
        std::fs::write(directory.path().join("sample.txt"), b"captured mention")
            .map_err(storage)?;
        let mut harness = LocalHarness::open(
            crate::LocalHarnessConfig::native_host(directory.path())
                .workspace_mounting(crate::local::WorkspaceMounting::Physical)
                .state_directory(state.path().join("runtime")),
        )
        .await?;
        let workspace = harness
            .workspace
            .as_ref()
            .expect("fixture workspace")
            .clone();
        let publisher = harness.input_publisher_with_limit(1024)?;
        // Current reads only: the captured cut is refused once it is history.
        let label = Capability {
            layers: vec![vec![Grant {
                actions: [Action::Read, Action::List].into(),
                resource: ResourcePattern {
                    lineage_root: None,
                    volume: None,
                    generations: Generations::All,
                    path: "**".into(),
                },
            }]],
            expires_at: None,
        }
        .to_scope_label()
        .map_err(storage)?;
        let scope = harness
            .root_authority
            .as_ref()
            .expect("fixture signer")
            .bootstrap_scope(
                "current-reads-only",
                &Capabilities::new([label]),
                Some(harness.root),
                &mut None,
            )?;
        harness.signed = scope;
        harness.context_read_observer = Some(advance_head);
        let operation = OperationId::new();
        assert!(matches!(
            harness
                .capture_checkout_mentions(
                    &workspace,
                    "@sample.txt",
                    "inputs/refused",
                    publisher.as_ref(),
                    operation,
                )
                .await,
            Err(Error::FilesystemAccessConfiguration("checkout context"))
        ));
        let (saved, _) = publisher
            .observe_changes(
                publisher.volume(),
                operation,
                None,
                crate::conversation::ContentAccess::default(),
            )?
            .await?;
        assert!(saved.is_none(), "a refused capture saved its copies");
        Ok(())
    }

    proptest! {
        // Property: archives depend only on the selected name and exact bytes;
        // every generated name/body round-trips its path and content identity.
        #[test]
        fn archive_identity(name in prop::sample::select(vec!["AGENTS.md", "CLAUDE.md"]), bytes in prop::collection::vec(any::<u8>(), 0..128)) {
            let path = archive_path(name, &bytes);
            crate::conversation::validate_content_path(&path, &mut None)?;
            let suffix = path.strip_prefix("inputs/context/").unwrap();
            let (digest, retained_name) = suffix.split_once('/').unwrap();
            prop_assert_eq!(retained_name, name);
            let expected = blake3::hash(&bytes).to_hex();
            prop_assert_eq!(digest, expected.as_str());
            prop_assert_eq!(path, archive_path(name, &bytes));
        }
    }
}
