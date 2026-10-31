//! Prompt commands and skills selected by a prompt's leading `/name`,
//! resolved when the prompt is admitted so its message records the text the
//! model reads: a project's `.claude/commands` or `.codex/prompts` Markdown,
//! or a discovered skill's body, with `$ARGUMENTS` substituted.
//!
//! Only the selected name's files are read; nothing is listed or scanned.
use super::*;
use crate::client::commands::{CommandCatalog, prompt_paths};

impl LocalHarness {
    /// The text `text` expands to when it selects a prompt command or skill;
    /// `None` when it selects neither a command nor a skill name.
    ///
    /// # Errors
    /// A name both a command and a skill define, or that multiple commands
    /// or skills define, is a conflict; an unknown name is not found.
    pub(in crate::local) async fn resolve_prompt_command(
        &self,
        text: &str,
    ) -> Result<Option<String>> {
        let catalog = CommandCatalog::builtins()?;
        let Some((name, arguments)) = catalog.selected(text, u64::MAX)? else {
            return Ok(None);
        };
        let maximum = self.limits.file_bytes;
        let paths = prompt_paths(&name).collect::<Vec<_>>();
        let mut commands = self.project_files(&paths, maximum).await?.into_iter();
        let command = commands.next();
        if commands.next().is_some() {
            return Err(Error::Conflict(format!("duplicate command: {name}")));
        }
        let mut skills = self
            .skills_with_work(None, &mut crate::context::DiscoveryWork::default())
            .await?
            .into_iter()
            .filter(|skill| skill.name.eq_ignore_ascii_case(&name));
        let skill = skills.next();
        if skills.next().is_some() {
            return Err(Error::Conflict(format!("ambiguous skill: {name}")));
        }
        let body = match (command, skill) {
            (Some(_), Some(_)) => {
                return Err(Error::Conflict(format!("skill/command collision: {name}")));
            }
            (Some((_, bytes)), None) => bytes,
            (None, Some(skill)) => {
                let message = skill
                    .invoke(self.context_reader()?.as_ref(), maximum)
                    .await?;
                let crate::model::ModelContent::Text(body) = message.content else {
                    return Err(Error::Invalid("skill body is not text".into()));
                };
                body.into_bytes()
            }
            (None, None) => return Err(Error::NotFound(format!("Unknown command: /{name}"))),
        };
        CommandCatalog::expand(&body, arguments, maximum).map(Some)
    }

    /// Each of `paths` present in the project, from the selected checkout
    /// view or the project volume's current generation.
    async fn project_files(&self, paths: &[String], maximum: u64) -> Result<Vec<(usize, Vec<u8>)>> {
        #[cfg(any(test, feature = "native-execution"))]
        if self.checkout_context
            && let Some(workspace) = &self.workspace
        {
            return self.checkout_files(workspace, paths, maximum).await;
        }
        let workspace = crate::filesystem::workspace_ref(
            self.project.provider().clone(),
            &self.project.storage_name(&mut None)?,
        )?;
        let generation = self.files.resolve(&workspace, None).await?.generation;
        let reader = self.context_reader()?;
        let mut found = Vec::new();
        for (index, path) in paths.iter().enumerate() {
            let read = reader
                .read_private_path(
                    &self.project,
                    "",
                    path,
                    Some(&generation),
                    maximum,
                    crate::conversation::ContentAccess::default(),
                )?
                .await;
            match read {
                Ok((_, bytes, _)) => found.push((index, bytes.as_ref().to_vec())),
                Err(Error::NotFound(_)) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(found)
    }
}
