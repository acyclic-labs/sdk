use super::*;
use crate::context::{ContextRoot, DiscoveryWork, SkillMetadata};

pub(super) use crate::context::PROJECT_SKILL_ROOTS as PROJECT_ROOTS;

impl LocalHarness {
    /// Capture the host's pinned skill metadata without loading bodies.
    /// Storage paths are host references, not addresses for model file tools.
    ///
    /// # Errors
    /// Returns original discovery, authority or work-accounting failures.
    pub async fn skills_with_work(
        &self,
        generation: Option<&crate::resources::GenerationRef>,
        work: &mut DiscoveryWork,
    ) -> Result<Vec<SkillMetadata>> {
        let reader = self.context_reader()?;
        let binding = Binding {
            project: &self.project,
            project_root: "",
            scratch: &self.scratch,
            reader: reader.as_ref(),
            #[cfg(any(test, not(target_arch = "wasm32")))]
            journal: &self.journal,
            #[cfg(any(test, not(target_arch = "wasm32")))]
            verifier: self.verifier.clone(),
            #[cfg(any(test, not(target_arch = "wasm32")))]
            scope: &self.signed,
            #[cfg(any(test, not(target_arch = "wasm32")))]
            workspace: self.workspace.clone(),
        };
        self.bound_skills(&binding, generation, work)?.await
    }

    // Construct the large discovery state outside the caller's poll frame.
    #[inline(never)]
    pub(super) fn bound_skills<'a>(
        &'a self,
        binding: &'a Binding<'a>,
        generation: Option<&'a crate::resources::GenerationRef>,
        work: &'a mut DiscoveryWork,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Vec<SkillMetadata>>> {
        crate::conversation::BoundedContentFuture::new(None, || {
            self.discover_skills(binding, generation, work)
        })
    }

    async fn discover_skills(
        &self,
        binding: &Binding<'_>,
        generation: Option<&crate::resources::GenerationRef>,
        work: &mut DiscoveryWork,
    ) -> Result<Vec<SkillMetadata>> {
        let mut skills = Vec::new();
        #[cfg(any(test, not(target_arch = "wasm32")))]
        let sources = self.bound_skill_sources(binding)?;
        #[cfg(any(test, not(target_arch = "wasm32")))]
        let native_project = generation.is_none()
            && self.checkout_context
            && sources.is_some()
            && binding.workspace.is_some();
        #[cfg(not(any(test, not(target_arch = "wasm32"))))]
        let native_project = false;
        #[cfg(any(test, not(target_arch = "wasm32")))]
        if native_project
            && let Some(sources) = &sources
            && let Some(workspace) = &binding.workspace
        {
            let (selected, pending) = workspace.selected_view().await?;
            if let Some((operation, _)) = pending {
                return Err(Error::Indeterminate(OperationId::from_bytes(
                    operation.into_bytes(),
                )));
            }
            let view = selected.as_ref().unwrap_or_else(|| workspace.view());
            let cancellation = acyclic_fs::CancellationToken::new();
            let native_work = acyclic_fs::NativeMaterializationWork::new(
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &cancellation,
            )
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let io = crate::filesystem::PreparationIo::from_native_work(&native_work)?;
            self.authorize_checkout_context(
                workspace,
                view.workspace(),
                &io,
                (&binding.verifier, binding.scope),
            )
            .await?;
            skills = sources
                .catalog(
                    workspace.source_root.workspace(),
                    view,
                    binding.project_root,
                    work,
                )?
                .await?;
            self.authorize_checkout_context(
                workspace,
                view.workspace(),
                &io,
                (&binding.verifier, binding.scope),
            )
            .await?;
        }
        if !native_project {
            #[cfg(any(test, not(target_arch = "wasm32")))]
            if let Some(sources) = sources {
                skills = sources.user_catalog(work)?.await?;
            }
            skills.extend(self.volume_skills(binding, generation, work, false).await?);
        }
        skills.extend(self.volume_skills(binding, None, work, true).await?);
        crate::context::validate_skill_catalog(&skills, &mut None)?;
        Ok(skills)
    }

    #[cfg(any(test, not(target_arch = "wasm32")))]
    fn bound_skill_sources(
        &self,
        binding: &Binding<'_>,
    ) -> Result<Option<Arc<crate::context::native_skills::NativeSkills>>> {
        if let Some(sources) = self.files.skill_sources(binding.journal)? {
            return Ok(Some(sources));
        }
        let Some(sources) = self.files.skill_sources(&self.journal)? else {
            return Ok(None);
        };
        let files = self
            .files
            .clone()
            .with_native_skills(sources.for_volume(binding.journal.clone()))?;
        files.skill_sources(binding.journal)
    }

    /// The project's skills, or the agent's private ones: its home's
    /// `skills/` over the seed's. Immutable source storage can reuse its pinned
    /// metadata; discovery must still check this binding's original reader.
    async fn volume_skills(
        &self,
        binding: &Binding<'_>,
        generation: Option<&crate::resources::GenerationRef>,
        work: &mut DiscoveryWork,
        private: bool,
    ) -> Result<Vec<SkillMetadata>> {
        let volume = if private {
            binding.scratch
        } else {
            binding.project
        };
        let name = volume.storage_name(&mut None)?;
        let workspace = crate::filesystem::workspace_ref(volume.provider().clone(), &name)?;
        // Instructions already selected this generation. Reuse it both for
        // consistency without selecting another source revision.
        let observed;
        let generation = if let Some(generation) = generation {
            generation
        } else {
            observed = self.files.resolve(&workspace, None).await?.generation;
            &observed
        };
        let (seed, seeded) = &self.seed;
        let root = |volume: &VolumeRef, directory: &str| ContextRoot {
            volume: volume.clone(),
            directory: directory.into(),
        };
        let skills = if private {
            let (seed_root, home_root) = (
                root(seed, crate::seed::SKILLS),
                root(volume, crate::seed::SKILLS),
            );
            crate::context::discover_layered_skills(
                &mut [
                    (
                        crate::context::VolumeDiscovery {
                            reader: binding.reader,
                            root: &seed_root,
                            io: None,
                        },
                        seeded.clone(),
                    ),
                    (
                        crate::context::VolumeDiscovery {
                            reader: binding.reader,
                            root: &home_root,
                            io: None,
                        },
                        generation.clone(),
                    ),
                ],
                crate::seed::SKILLS,
                "SKILL.md",
                work,
            )
            .await?
        } else {
            let mut skills = Vec::new();
            for directory in PROJECT_ROOTS {
                let directory = if binding.project_root.is_empty() {
                    std::borrow::Cow::Borrowed(directory)
                } else {
                    std::borrow::Cow::Owned(format!("{}/{directory}", binding.project_root))
                };
                skills.extend(
                    crate::context::discover_skills_from(
                        &mut crate::context::VolumeDiscovery {
                            reader: binding.reader,
                            root: &root(volume, &directory),
                            io: None,
                        },
                        &directory,
                        "SKILL.md",
                        work,
                        Some(generation),
                        true,
                    )
                    .await?,
                );
            }
            skills
        };
        Ok(skills)
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn immutable_skill_catalogs_recheck_each_readers_source_authority() -> Result<()> {
        let checkout = tempfile::tempdir().map_err(storage)?;
        let state = tempfile::tempdir().map_err(storage)?;
        let harness = LocalHarness::open(
            crate::LocalHarnessConfig::native_host(checkout.path())
                .workspace_mounting(crate::local::WorkspaceMounting::Physical)
                .state_directory(state.path().join("runtime")),
        )
        .await?;
        let signer = harness
            .root_authority
            .as_ref()
            .ok_or_else(|| Error::NotFound("fixture signing custody".into()))?;
        let allowed_reader = harness.context_reader()?;
        let binding = Binding {
            project: &harness.project,
            project_root: "",
            scratch: &harness.scratch,
            journal: &harness.journal,
            reader: allowed_reader.as_ref(),
            verifier: harness.verifier.clone(),
            scope: &harness.signed,
            workspace: None,
        };
        for (private, volume, directory, name) in [
            (false, &harness.project, PROJECT_ROOTS[0], "project-secret"),
            (true, &harness.scratch, crate::seed::SKILLS, "home-secret"),
        ] {
            let workspace = crate::filesystem::workspace_ref(
                volume.provider().clone(),
                &volume.storage_name(&mut None)?,
            )?;
            let generation = harness
                .files
                .apply(
                    &workspace,
                    None,
                    &[
                        crate::filesystem::WorkspaceMutation::CreateDirectory {
                            path: format!("/{directory}/{name}"),
                        },
                        crate::filesystem::WorkspaceMutation::PutFile {
                            path: format!("/{directory}/{name}/SKILL.md"),
                            bytes: format!(
                                "---\nname: {name}\ndescription: metadata requires source authority\n---\nprivate body\n"
                            )
                            .into_bytes()
                            .into(),
                        },
                    ],
                    &crate::IdempotencyKey::new(format!("skill-authority-{name}"))?,
                )
                .await?;
            let ceiling = acyclic_fs::access::Capability::subtree(
                None,
                "unrelated",
                &[
                    acyclic_fs::access::Action::List,
                    acyclic_fs::access::Action::Read,
                ],
            )
            .map_err(storage)?
            .to_scope_label()
            .map_err(storage)?;
            for grants in [
                crate::Capabilities::default(),
                crate::Capabilities::new(
                    harness
                        .signed
                        .capabilities()
                        .iter()
                        .chain([ceiling.as_str()]),
                ),
            ] {
                let denied = signer.bootstrap_scope(
                    "skill-source-denied",
                    &grants,
                    Some(harness.root),
                    &mut None,
                )?;
                let denied_reader = FilesystemContentVerifier::new(
                    harness.files.clone(),
                    &harness.verifier,
                    &denied,
                    harness.limits.file_bytes,
                    None,
                )?;
                let forbidden = Binding {
                    reader: denied_reader.as_ref(),
                    scope: &denied,
                    verifier: harness.verifier.clone(),
                    workspace: None,
                    ..binding
                };
                // Cold refusal is the control: the original reader has no
                // authority even though the immutable source exists.
                assert_eq!(
                    harness
                        .volume_skills(
                            &forbidden,
                            Some(&generation),
                            &mut Default::default(),
                            private
                        )
                        .await
                        .unwrap_err()
                        .code(),
                    crate::wire::ErrorCode::Unauthorized
                );
                let skills = harness
                    .volume_skills(
                        &binding,
                        Some(&generation),
                        &mut Default::default(),
                        private,
                    )
                    .await?;
                assert!(skills.iter().any(|skill| skill.name == name));
                // Same generation and same source root, after an authorized
                // read: cached metadata must not disclose it to another scope.
                assert_eq!(
                    harness
                        .volume_skills(
                            &forbidden,
                            Some(&generation),
                            &mut Default::default(),
                            private
                        )
                        .await
                        .unwrap_err()
                        .code(),
                    crate::wire::ErrorCode::Unauthorized
                );
                assert_eq!(
                    harness
                        .volume_skills(
                            &binding,
                            Some(&generation),
                            &mut Default::default(),
                            private
                        )
                        .await?,
                    skills
                );
            }
        }
        Ok(())
    }
}
