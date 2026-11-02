use super::*;
use crate::context::{ContextRoot, DiscoveryWork, SkillCatalog, SkillMetadata};

/// A catalog and the identity of the layer stack it was discovered over.
type Catalog = Option<([u8; 32], SkillCatalog)>;

#[derive(Default)]
pub(in crate::local) struct Catalogs {
    project: Catalog,
    private: Catalog,
}

pub(in crate::local) type Cache = crate::sync::owned::Arc<crate::sync::owned::Mutex<Catalogs>>;

pub(in crate::local) fn cache() -> Cache {
    crate::sync::owned::Arc::new(crate::sync::owned::Mutex::new(Catalogs::default()))
}

pub(super) use crate::context::PROJECT_SKILL_ROOTS as PROJECT_ROOTS;

impl LocalHarness {
    pub(in crate::local) async fn skills_with_work(
        &self,
        generation: Option<&crate::resources::GenerationRef>,
        work: &mut DiscoveryWork,
    ) -> Result<Vec<SkillMetadata>> {
        let reader = self.context_reader()?;
        let binding = Binding {
            project: &self.project,
            scratch: &self.scratch,
            reader: reader.as_ref(),
            #[cfg(any(test, feature = "native-execution"))]
            journal: &self.journal,
            #[cfg(any(test, feature = "native-execution"))]
            verifier: self.verifier.clone(),
            #[cfg(any(test, feature = "native-execution"))]
            scope: &self.signed,
            #[cfg(any(test, feature = "native-execution"))]
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
        #[cfg(any(test, feature = "native-execution"))]
        let sources = self.bound_skill_sources(binding)?;
        #[cfg(any(test, feature = "native-execution"))]
        let native_project = generation.is_none()
            && self.checkout_context
            && sources.is_some()
            && binding.workspace.is_some();
        #[cfg(not(any(test, feature = "native-execution")))]
        let native_project = false;
        #[cfg(any(test, feature = "native-execution"))]
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
            skills = sources.catalog(&workspace.path, view, work)?.await?;
        }
        if !native_project {
            #[cfg(any(test, feature = "native-execution"))]
            if let Some(sources) = sources {
                skills = sources.user_catalog(work)?.await?;
            }
            skills.extend(self.volume_skills(binding, generation, work, false).await?);
        }
        skills.extend(self.volume_skills(binding, None, work, true).await?);
        crate::context::validate_skill_catalog(&skills, &mut None)?;
        Ok(skills)
    }

    #[cfg(any(test, feature = "native-execution"))]
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
    /// `skills/` over the seed's. Each catalog is reused while the identity
    /// of its pinned layer stack is unchanged.
    async fn volume_skills(
        &self,
        binding: &Binding<'_>,
        generation: Option<&crate::resources::GenerationRef>,
        work: &mut DiscoveryWork,
        private: bool,
    ) -> Result<Vec<SkillMetadata>> {
        use crate::layers::{SEED, stack_digest};
        let volume = if private {
            binding.scratch
        } else {
            binding.project
        };
        let name = volume.storage_name(&mut None)?;
        let workspace = crate::filesystem::workspace_ref(volume.provider().clone(), &name)?;
        // Instructions already selected this generation. Reuse it both for
        // consistency and to keep cache hits free of authority stream reads.
        let observed;
        let generation = if let Some(generation) = generation {
            generation
        } else {
            observed = self.files.resolve(&workspace, None).await?.generation;
            &observed
        };
        let (seed, seeded) = &self.seed;
        // The home layer is named by its volume, so agents never share it.
        let home = (name.as_str(), generation.as_resource().key());
        let stack = if private {
            stack_digest([(SEED, seeded.as_resource().key()), home])
        } else {
            stack_digest([home])
        };
        {
            let cache = crate::sync::owned::lock_owned(self.skill_cache.clone()).await;
            let catalog = if private {
                &cache.private
            } else {
                &cache.project
            };
            if let Some((cached, catalog)) = catalog
                && *cached == stack
                && let Some(skills) = catalog.reuse(work)?
            {
                return Ok(skills);
            }
        }
        let before = *work;
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
                skills.extend(
                    crate::context::discover_skills_from(
                        &mut crate::context::VolumeDiscovery {
                            reader: binding.reader,
                            root: &root(volume, directory),
                            io: None,
                        },
                        directory,
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
        let catalog = SkillCatalog::capture(skills.clone(), *work, before)?;
        let mut cache = crate::sync::owned::lock_owned(self.skill_cache.clone()).await;
        let slot = if private {
            &mut cache.private
        } else {
            &mut cache.project
        };
        *slot = Some((stack, catalog));
        Ok(skills)
    }
}
