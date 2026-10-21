use super::*;
use crate::context::{ContextRoot, DiscoveryWork, SkillMetadata, discover_skills_at};

pub(in crate::local) type Cache = crate::sync::owned::Arc<
    crate::sync::owned::Mutex<Option<(crate::resources::GenerationRef, Arc<Vec<SkillMetadata>>)>>,
>;

pub(in crate::local) fn cache() -> Cache {
    crate::sync::owned::Arc::new(crate::sync::owned::Mutex::new(None))
}

pub(super) const PROJECT_ROOTS: [&str; 2] = [".agents/skills", ".claude/skills"];

impl LocalHarness {
    pub(in crate::local) async fn skills(
        &self,
        generation: Option<&crate::resources::GenerationRef>,
    ) -> Result<Vec<SkillMetadata>> {
        self.skill_discovery(generation)?.await
    }

    // Construct the large discovery state outside the caller's poll frame.
    #[inline(never)]
    fn skill_discovery<'a>(
        &'a self,
        generation: Option<&'a crate::resources::GenerationRef>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Vec<SkillMetadata>>> {
        crate::conversation::BoundedContentFuture::new(None, || self.discover_skills(generation))
    }

    async fn discover_skills(
        &self,
        generation: Option<&crate::resources::GenerationRef>,
    ) -> Result<Vec<SkillMetadata>> {
        #[cfg(feature = "native-execution")]
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
            return sources.catalog(&workspace.path, view).await;
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
        if let Some((cached, skills)) = &*cache
            && cached == generation
        {
            return Ok(skills.as_ref().clone());
        }
        let reader = self.context_reader()?;
        let mut work = DiscoveryWork {
            entries: 0,
            directories: 0,
            limits: ContextDiscoveryLimits::default(),
        };
        let mut skills = Vec::new();
        #[cfg(feature = "native-execution")]
        if let Some(sources) = self.files.skill_sources(&self.journal)? {
            (skills, work) = sources.user_discovery().await?;
        }
        for directory in PROJECT_ROOTS {
            if let Err(error) = self
                .files
                .stat(&workspace, Some(generation), &format!("/{directory}"))
                .await
            {
                if matches!(error, Error::NotFound(_)) {
                    continue;
                }
                return Err(error);
            }
            let root = ContextRoot {
                volume: self.project.clone(),
                directory: directory.into(),
            };
            skills.extend(
                discover_skills_at(
                    reader.as_ref(),
                    &[root],
                    "SKILL.md",
                    &mut work,
                    Some(generation),
                )
                .await?,
            );
        }
        DiscoveredContext {
            instructions: Vec::new(),
            skills: skills.clone(),
        }
        .validate(&mut None)?;
        *cache = Some((generation.clone(), Arc::new(skills.clone())));
        Ok(skills)
    }
}
