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
    maximum_files: usize,
    files: BTreeMap<crate::task_volume::FileKey, acyclic_stream::OwnedBytes>,
}

impl CommandSources {
    /// Always capture a fresh source cut, including after an earlier install.
    /// Subscribe before reading sources so edits during capture are not lost.
    pub async fn observe<P: StreamProvider + ?Sized>(
        client: &mut CommandSession<P>,
        volume: &crate::task_volume::TaskVolume<P>,
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
            maximum_files,
            files,
        };
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
    fn source(&self, path: &str) -> Option<(String, bool)> {
        for root in &self.skills {
            let Some(relative) = relative(path, &root.directory) else {
                continue;
            };
            if relative.ends_with("/SKILL.md") {
                return Some((String::new(), true));
            }
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
                .await?;
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
                if bytes.is_empty() {
                    continue;
                }
                let (description, name, body) = if has_frontmatter(bytes) {
                    let metadata: PromptMetadata = parse_frontmatter(bytes)?;
                    let (_, _, body) = frontmatter_end(bytes)?;
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
                    (
                        metadata.description.unwrap_or_else(|| name.clone()),
                        name,
                        &bytes[body..],
                    )
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
                validate_name(&name)?;
                if catalog.commands.contains_key(&name) {
                    return Err(Error::Conflict(format!("reserved command: {name}")));
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
            let specs: Vec<_> = catalog.commands().collect();
            let bytes =
                serde_json::to_vec(&specs).map_err(|error| Error::Invalid(error.to_string()))?;
            let current = client.state(CommandFileRoot::Config, "commands.json")?;
            if current.as_ref()
                != Some(
                    &serde_json::to_value(&specs)
                        .map_err(|error| Error::Invalid(error.to_string()))?,
                )
            {
                client.install_catalog(bytes).await?;
            }
            self.pending = false;
        }
        Ok(())
    }
}

fn relative<'a>(path: &'a str, root: &str) -> Option<&'a str> {
    if root.is_empty() {
        Some(path)
    } else {
        path.strip_prefix(root)?.strip_prefix('/')
    }
}
