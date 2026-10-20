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
    pub(super) async fn skills(&self) -> Result<Vec<SkillMetadata>> {
        #[cfg(feature = "native-execution")]
        if self.checkout_context
            && let Some(sources) = self.files.skill_sources(&self.journal)?
        {
            return sources.catalog().await;
        }
        let workspace = crate::filesystem::workspace_ref(
            self.project.provider().clone(),
            &self.project.storage_name(&mut None)?,
        )?;
        let generation = self.files.resolve(&workspace, None).await?.generation;
        let mut cache = crate::sync::owned::lock_owned(self.skill_cache.clone()).await;
        if let Some((cached, skills)) = &*cache
            && cached == &generation
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
            match self
                .files
                .stat(&workspace, Some(&generation), &format!("/{directory}"))
                .await
            {
                Ok(_) => {}
                Err(Error::NotFound(_)) => continue,
                Err(error) => return Err(error),
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
                    Some(&generation),
                )
                .await?,
            );
        }
        DiscoveredContext {
            instructions: Vec::new(),
            skills: skills.clone(),
        }
        .validate(&mut None)?;
        *cache = Some((generation, Arc::new(skills.clone())));
        Ok(skills)
    }
}
