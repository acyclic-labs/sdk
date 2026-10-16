//! A catalog is a projection of declared source files at snapshot/follow cuts.
use super::*;
use acyclic_stream::StreamProvider;
use futures::StreamExt as _;
use std::sync::Arc;

/// Disposable source subscription. The owner publishes filesystem arrivals to
/// its ordinary input-file query; catalog revisions use the same volume.
pub struct CommandSources {
    changes: crate::task_volume::ChangeStream,
    base: Vec<u8>,
    roots: Vec<CommandRoot>,
    skills: Vec<ContextRoot>,
    reader: Arc<dyn ContentResidencyVerifier>,
    limits: ContextDiscoveryLimits,
    maximum_bytes: u64,
    pending: bool,
    published: Option<[u8; 32]>,
    catalog_position: u64,
    maximum_files: usize,
    files: BTreeMap<crate::task_volume::FileKey, acyclic_stream::OwnedBytes>,
}

impl CommandSources {
    /// Always capture a fresh source cut, including after an earlier install.
    /// Subscribe before reading sources so edits during capture are not lost.
    pub async fn observe<P: StreamProvider + ?Sized>(
        client: &mut CommandSession<P>,
        base: Vec<u8>,
        roots: Vec<CommandRoot>,
        skills: Vec<ContextRoot>,
        reader: Arc<dyn ContentResidencyVerifier>,
        limits: ContextDiscoveryLimits,
        maximum_bytes: u64,
        maximum_files: usize,
    ) -> Result<Self> {
        CommandCatalog::from_bytes(&base, maximum_bytes)?;
        limits.validate()?;
        let volume = client.volume().clone();
        let observed = volume
            .observe(crate::task_volume::Query::Inputs, None, maximum_files)
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        let files = observed
            .snapshot
            .as_ref()
            .ok_or_else(|| Error::Conflict("source snapshot missing".into()))?
            .files
            .iter()
            .map(|file| (file.key.clone(), file.bytes.clone()))
            .collect();
        let mut sources = Self {
            changes: observed.changes,
            base,
            roots,
            skills,
            reader,
            limits,
            maximum_bytes,
            pending: true,
            published: None,
            catalog_position: observed
                .snapshot
                .as_ref()
                .map_or(0, |snapshot| snapshot.cursor.position),
            maximum_files,
            files,
        };
        let installed = client.state(CommandFileRoot::Config, "commands.json")?;
        if installed
            != Some(
                serde_json::from_slice(DEFAULT_COMMANDS_JSON)
                    .map_err(|error| Error::Invalid(error.to_string()))?,
            )
        {
            sources.rebase(client.catalog())?;
        }
        let selected: BTreeSet<_> = sources
            .files
            .keys()
            .filter(|key| sources.source(&key.path()).is_some())
            .cloned()
            .collect();
        sources.files.retain(|key, _| selected.contains(key));
        sources.refresh(client).await?;
        Ok(sources)
    }
    fn generated(&self, spec: &CommandSpec) -> bool {
        match &spec.action {
            CommandAction::Template { file, .. } => self.source(&file.path()).is_some(),
            CommandAction::Prompt { source, .. } => {
                self.roots.iter().any(|root| root.root == source.root)
            }
            CommandAction::Skill { skill } => self.skills.contains(&skill.source.root),
            _ => false,
        }
    }
    fn rebase(&mut self, catalog: &CommandCatalog) -> Result<()> {
        self.base = serde_json::to_vec(
            &catalog
                .commands()
                .filter(|spec| !self.generated(spec))
                .collect::<Vec<_>>(),
        )
        .map_err(|error| Error::Invalid(error.to_string()))?;
        Ok(())
    }
    fn source(&self, path: &str) -> Option<(String, bool)> {
        for root in &self.skills {
            let Some(relative) = relative(path, &root.directory) else {
                continue;
            };
            if relative.ends_with("/SKILL.md") {
                return Some((String::new(), true));
            }
        }
        if self.skills.is_empty() && path.starts_with("skills/") && path.ends_with("/SKILL.md") {
            return Some((String::new(), true));
        }
        if self.roots.is_empty() {
            let relative = path
                .strip_prefix(".claude/commands/")
                .or_else(|| path.strip_prefix(".codex/prompts/"))?;
            return relative
                .strip_suffix(".md")
                .map(|name| (name.replace('/', ":").to_ascii_lowercase(), false));
        }
        self.roots
            .iter()
            .filter_map(|root| {
                let relative = relative(path, &root.root.directory)?;
                let relative = relative
                    .strip_prefix(".claude/commands/")
                    .or_else(|| relative.strip_prefix(".codex/prompts/"))?;
                let name = relative
                    .strip_suffix(".md")?
                    .replace('/', ":")
                    .to_ascii_lowercase();
                Some((root.scope, name))
            })
            .max()
            .map(|(_, name)| (name, false))
    }

    /// Fold one source publication and publish the resulting catalog. Cancelling
    /// this future retains a received update until its catalog is installed.
    pub async fn changed(&mut self) -> Result<()> {
        if !self.pending {
            let change = self
                .changes
                .next()
                .await
                .ok_or_else(|| Error::Storage("command source query ended; reobserve".into()))?
                .map_err(|error| Error::Storage(error.to_string()))?;
            if let Some(file) = &change.file
                && file.key.path() == "commands/catalog.json"
                && change.cursor.position > self.catalog_position
                && self.published != Some(*blake3::hash(&file.bytes).as_bytes())
            {
                let catalog = CommandCatalog::from_bytes(&file.bytes, self.maximum_bytes)?;
                self.rebase(&catalog)?;
                self.catalog_position = change.cursor.position;
                self.pending = true;
            }
            if let Some(file) = change.file
                && self.source(&file.key.path()).is_some()
            {
                if !self.files.contains_key(&file.key) && self.files.len() == self.maximum_files {
                    return Err(Error::Invalid("source file count exceeds bound".into()));
                }
                self.files.insert(file.key, file.bytes);
                self.pending = true;
            }
        }
        Ok(())
    }

    /// Publish the complete next catalog after a selected source change.
    pub async fn refresh<P: StreamProvider + ?Sized>(
        &mut self,
        client: &mut CommandSession<P>,
    ) -> Result<()> {
        let result = self.publish(client).await;
        if matches!(result, Err(Error::Conflict(_))) {
            // install_catalog followed the winning revision before rejecting
            // our stale edit. Future captures must use that reviewed base;
            // older queued source checkpoints cannot roll it backwards.
            self.rebase(client.catalog())?;
            self.catalog_position = client.position();
            self.pending = true;
        }
        if matches!(result, Err(Error::Invalid(_))) {
            self.pending = false;
        }
        result
    }
    async fn publish<P: StreamProvider + ?Sized>(
        &mut self,
        client: &mut CommandSession<P>,
    ) -> Result<()> {
        if self.pending {
            let size = self
                .files
                .values()
                .try_fold(0u64, |size, bytes| size.checked_add(bytes.len() as u64))
                .ok_or_else(|| Error::Invalid("source byte count overflow".into()))?;
            if size > self.maximum_bytes {
                return Err(Error::Invalid(
                    "source files exceed catalog byte bound".into(),
                ));
            }
            let mut catalog = CommandCatalog::from_bytes(&self.base, self.maximum_bytes)?
                .with_sources(self.reader.as_ref(), &self.roots, &self.skills, self.limits)
                .await
                .map_err(|error| match error {
                    Error::Conflict(message) => Error::Invalid(message),
                    error => error,
                })?;
            let changed: BTreeSet<_> = self
                .files
                .keys()
                .filter(|key| self.source(&key.path()).is_some())
                .map(|key| key.path())
                .collect();
            catalog.commands.retain(|_, spec| match &spec.action {
                CommandAction::Prompt { source, .. } => !changed.contains(&source.path),
                CommandAction::Skill { skill } => !changed.contains(&skill.source.path),
                _ => true,
            });
            for (key, bytes) in &self.files {
                let path = key.path();
                let Some((name, skill)) = self.source(&path) else {
                    continue;
                };
                if bytes.len() as u64 > self.maximum_bytes {
                    return Err(Error::Invalid("source exceeds catalog byte bound".into()));
                }
                let (description, name, body) = if has_frontmatter(bytes) {
                    let metadata: PromptMetadata = parse_frontmatter(bytes)?;
                    let (_, _, body) = frontmatter_end(bytes)?;
                    if body > self.limits.header_bytes as usize {
                        return Err(Error::Invalid("source header exceeds bound".into()));
                    }
                    let name = if skill {
                        metadata
                            ._fields
                            .get("name")
                            .and_then(serde_json::Value::as_str)
                            .ok_or_else(|| Error::Invalid("skill name absent".into()))?
                            .to_owned()
                    } else {
                        name
                    };
                    let description = match metadata.description {
                        Some(description) => description,
                        None if skill => {
                            return Err(Error::Invalid("skill description absent".into()));
                        }
                        None => name.clone(),
                    };
                    (description, name, &bytes[body..])
                } else {
                    if skill {
                        return Err(Error::Invalid("skill frontmatter absent".into()));
                    }
                    (name.clone(), name, bytes.as_ref())
                };
                if skill
                    && (name.is_empty()
                        || name.starts_with('-')
                        || name.ends_with('-')
                        || name.contains("--")
                        || !name.bytes().all(|byte| {
                            byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-'
                        }))
                {
                    return Err(Error::Invalid("invalid skill name".into()));
                }
                if skill
                    && description
                        .chars()
                        .any(|ch| ch.is_control() && !ch.is_whitespace())
                {
                    return Err(Error::Invalid("invalid skill description".into()));
                }
                validate_name(&name)?;
                if catalog.commands.contains_key(&name) {
                    return Err(Error::Invalid(format!("reserved command: {name}")));
                }
                let body = std::str::from_utf8(body)
                    .map_err(|error| Error::Invalid(error.to_string()))?
                    .to_owned();
                catalog.commands.insert(
                    name.clone(),
                    CommandSpec {
                        name,
                        aliases: vec![],
                        description,
                        arguments: CommandArguments::Text,
                        action: CommandAction::Template {
                            file: key.clone(),
                            digest: *blake3::hash(bytes).as_bytes(),
                            body,
                        },
                    },
                );
            }
            // Apply catalog overrides after both declared-root imports and
            // mirrored files have rebuilt their current source actions.
            for (name, spec) in &mut catalog.commands {
                if let Some(previous) = client.catalog().commands.get(name)
                    && same_source(&previous.action, &spec.action)
                {
                    spec.aliases = previous.aliases.clone();
                }
            }
            if !client.catalog().commands().eq(catalog.commands()) {
                let specs: Vec<_> = catalog.commands().collect();
                let bytes = serde_json::to_vec(&specs)
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                let digest = *blake3::hash(&bytes).as_bytes();
                client.install_catalog(bytes).await?;
                self.published = Some(digest);
                self.catalog_position = client.position();
            }
            self.pending = false;
        }
        Ok(())
    }
}

fn same_source(left: &CommandAction, right: &CommandAction) -> bool {
    use CommandAction::{Prompt, Skill, Template};
    match (left, right) {
        (Template { file: left, .. }, Template { file: right, .. }) => left == right,
        (Prompt { source: left, .. }, Prompt { source: right, .. }) => {
            left.root == right.root && left.path == right.path
        }
        (Skill { skill: left }, Skill { skill: right }) => {
            left.source.root == right.source.root && left.source.path == right.source.path
        }
        (Template { file, .. }, Prompt { source, .. })
        | (Prompt { source, .. }, Template { file, .. }) => file.path() == source.path,
        (Template { file, .. }, Skill { skill }) | (Skill { skill }, Template { file, .. }) => {
            file.path() == skill.source.path
        }
        _ => false,
    }
}

fn relative<'a>(path: &'a str, root: &str) -> Option<&'a str> {
    if root.is_empty() {
        Some(path)
    } else {
        path.strip_prefix(root)?.strip_prefix('/')
    }
}

#[cfg(all(test, acyclic_loom))]
#[path = "source_models.rs"]
mod models;
