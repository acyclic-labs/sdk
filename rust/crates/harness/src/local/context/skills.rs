use super::*;
use crate::context::{ContextRoot, DiscoveryWork, SkillCatalog, SkillMetadata};

pub(in crate::local) type Cache = crate::sync::owned::Arc<
    crate::sync::owned::Mutex<Option<(crate::resources::GenerationRef, SkillCatalog)>>,
>;

pub(in crate::local) fn cache() -> Cache {
    crate::sync::owned::Arc::new(crate::sync::owned::Mutex::new(None))
}

pub(super) use crate::context::PROJECT_SKILL_ROOTS as PROJECT_ROOTS;

impl LocalHarness {
    #[cfg(test)]
    pub(in crate::local) async fn skills(
        &self,
        generation: Option<&crate::resources::GenerationRef>,
    ) -> Result<Vec<SkillMetadata>> {
        self.skills_with_work(generation, &mut DiscoveryWork::default())
            .await
    }

    pub(in crate::local) async fn skills_with_work(
        &self,
        generation: Option<&crate::resources::GenerationRef>,
        work: &mut DiscoveryWork,
    ) -> Result<Vec<SkillMetadata>> {
        self.skill_discovery(generation, work)?.await
    }

    // Construct the large discovery state outside the caller's poll frame.
    #[inline(never)]
    fn skill_discovery<'a>(
        &'a self,
        generation: Option<&'a crate::resources::GenerationRef>,
        work: &'a mut DiscoveryWork,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Vec<SkillMetadata>>> {
        crate::conversation::BoundedContentFuture::new(None, || {
            self.discover_skills(generation, work)
        })
    }

    async fn discover_skills(
        &self,
        generation: Option<&crate::resources::GenerationRef>,
        work: &mut DiscoveryWork,
    ) -> Result<Vec<SkillMetadata>> {
        #[cfg(any(test, feature = "native-execution"))]
        if generation.is_none()
            && self.checkout_context
            && let Some(sources) = self.files.skill_sources(&self.journal)?
            && let Some(workspace) = &self.workspace
        {
            let (selected, pending) = workspace.selected_view().await?;
            if let Some((operation, _)) = pending {
                return Err(Error::Indeterminate(OperationId::from_bytes(
                    operation.into_bytes(),
                )));
            }
            let view = selected.as_ref().unwrap_or_else(|| workspace.view());
            return sources.catalog(&workspace.path, view, work)?.await;
        }
        let workspace = crate::filesystem::workspace_ref(
            self.project.provider().clone(),
            &self.project.storage_name(&mut None)?,
        )?;
        // Instructions already selected this generation. Reuse it both for
        // consistency and to keep cache hits free of authority stream reads.
        let observed;
        let generation = if let Some(generation) = generation {
            generation
        } else {
            observed = self.files.resolve(&workspace, None).await?.generation;
            &observed
        };
        let mut cache = crate::sync::owned::lock_owned(self.skill_cache.clone()).await;
        if let Some((cached, catalog)) = &*cache
            && cached == generation
            && let Some(skills) = catalog.reuse(work)?
        {
            return Ok(skills);
        }
        let reader = self.context_reader()?;
        let before = *work;
        let mut skills = Vec::new();
        #[cfg(any(test, feature = "native-execution"))]
        if let Some(sources) = self.files.skill_sources(&self.journal)? {
            skills = sources.user_catalog(work)?.await?;
        }
        for directory in PROJECT_ROOTS {
            let root = ContextRoot {
                volume: self.project.clone(),
                directory: directory.into(),
            };
            skills.extend(
                crate::context::discover_skills_from(
                    &mut crate::context::VolumeDiscovery {
                        reader: reader.as_ref(),
                        root: &root,
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
        let catalog = SkillCatalog::capture(skills.clone(), *work, before)?;
        *cache = Some((generation.clone(), catalog));
        Ok(skills)
    }
}
