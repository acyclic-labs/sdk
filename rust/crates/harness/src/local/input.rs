//! Native composer input becomes immutable content before admission.
use super::*;
use crate::{
    client::{CommandCatalog, CommandResolution},
    context::ContextRoot,
    conversation::ContentResidencyVerifier,
};

#[cfg(any(test, feature = "native-execution"))]
use crate::{
    client::{CommandRoot, CommandScope},
    context::ContextDiscoveryLimits,
};
#[cfg(any(test, feature = "native-execution"))]
mod native_sources;
#[cfg(any(test, feature = "native-execution"))]
mod native_storage;

// Storage must be destroyed before its accounting owner on every exit path.
#[cfg(any(test, feature = "native-execution"))]
struct Held<T, A> {
    value: T,
    _allocation: A,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PromptInput {
    text: String,
    content: ModelContent,
}

fn prompt_operation(operation: OperationId) -> OperationId {
    context::publication_operation(
        b"acyclic.local.composer-input/v1\0",
        &operation.into_bytes(),
    )
}

impl LocalHarness {
    pub(super) async fn retain_prompt_input(
        &self,
        operation: OperationId,
        resolution: CommandResolution,
    ) -> Result<String> {
        if let CommandResolution::Prompt { text, mentions, .. } = &resolution {
            if mentions.is_empty() && crate::client::file_mention_paths(text)?.is_empty() {
                return Ok(text.clone());
            }
            if mentions.is_empty() {
                let (saved, _) = self
                    .input_publisher()?
                    .observe_staged_once(
                        prompt_operation(operation),
                        self.limits.file_bytes,
                        crate::conversation::ContentAccess::default(),
                    )?
                    .await?;
                if let Some(saved) = saved {
                    let bytes = self
                        .client_reader()?
                        .read(&saved, self.limits.file_bytes, None, None)?
                        .await?;
                    let saved: PromptInput = serde_json::from_slice(&bytes).map_err(storage)?;
                    if saved.text != *text {
                        return Err(Error::Conflict("operation prompt changed".into()));
                    }
                    return Ok(saved.text);
                }
            }
        }
        let resolution = match resolution {
            CommandResolution::Prompt {
                text,
                source,
                mentions,
            } if mentions.is_empty() && !crate::client::file_mention_paths(&text)?.is_empty() => {
                let mut captured = self.resolve_prompt_mentions(operation, &text).await?;
                if let CommandResolution::Prompt {
                    source: captured_source,
                    ..
                } = &mut captured
                {
                    *captured_source = source;
                }
                captured
            }
            resolution => resolution,
        };
        let text = match &resolution {
            CommandResolution::Prompt { text, .. } => text.clone(),
            _ => return Err(Error::Invalid("expected prompt input".into())),
        };
        let input = PromptInput {
            text: text.clone(),
            content: resolution.into_model_input()?,
        };
        let bytes = serde_json::to_vec(&input).map_err(storage)?;
        self.input_publisher()?
            .stage_once(
                prompt_operation(operation),
                &format!("inputs/{operation}.composer.json"),
                &bytes,
                "application/json",
                "composer input",
                crate::conversation::ContentAccess::default(),
            )?
            .await?;
        Ok(text)
    }

    pub(super) async fn turn_input_content(
        &self,
        operation: OperationId,
        prompt: &str,
        publisher: &dyn ContentPublisher,
    ) -> Result<ModelContent> {
        let (retained, _) = publisher
            .observe_staged_once(
                prompt_operation(operation),
                self.limits.file_bytes,
                crate::conversation::ContentAccess::default(),
            )?
            .await?;
        let Some(retained) = retained else {
            return Ok(ModelContent::Text(prompt.into()));
        };
        if retained.path() != format!("inputs/{operation}.composer.json") {
            return Err(Error::Conflict(
                "composer input has another identity".into(),
            ));
        }
        let reader = self.client_reader()?;
        let bytes = reader
            .read(&retained, self.limits.file_bytes, None, None)?
            .await?;
        let mut input: PromptInput = serde_json::from_slice(&bytes).map_err(storage)?;
        match &mut input.content {
            ModelContent::Text(text) => *text = prompt.into(),
            ModelContent::Parts(parts) => match parts.first_mut() {
                Some(crate::model::ModelContentPart::Text { text }) if *text == input.text => {
                    *text = prompt.into()
                }
                _ => {
                    return Err(Error::Invalid(
                        "composer input lost its leading text".into(),
                    ));
                }
            },
            _ => return Err(Error::Invalid("composer input is not a user prompt".into())),
        }
        Ok(input.content)
    }
    async fn resolve_prompt_mentions(
        &self,
        operation: OperationId,
        text: &str,
    ) -> Result<CommandResolution> {
        self.composer_input()?
            .resolve_prompt_mentions(operation, text)
            .await
    }


    fn composer_input(&self) -> Result<ComposerInput> {
        Ok(ComposerInput {
            files: Arc::clone(&self.files),
            project: self.project.clone(),
            #[cfg(any(test, feature = "native-execution"))]
            journal: self.journal.clone(),
            maximum_bytes: self.limits.file_bytes,
            publisher: self.input_publisher()?,
            reader: self.client_reader()?,
            #[cfg(any(test, feature = "native-execution"))]
            workspace: self.workspace.clone(),
            #[cfg(any(test, feature = "native-execution"))]
            settings: Arc::clone(self.settings()?),
        })
    }
}

// The input binding retains authority and source handles, not an execution runtime.
struct ComposerInput {
    files: Arc<LocalFilesystem>,
    project: VolumeRef,
    #[cfg(any(test, feature = "native-execution"))]
    journal: VolumeRef,
    maximum_bytes: u64,
    publisher: Arc<dyn ContentPublisher>,
    reader: Arc<dyn ContentResidencyVerifier>,
    #[cfg(any(test, feature = "native-execution"))]
    workspace: Option<Arc<LocalWorkspace>>,
    #[cfg(any(test, feature = "native-execution"))]
    settings: Arc<crate::native_settings::SettingsConfig>,
}

impl ComposerInput {
    async fn resolve_prompt_mentions(
        &self,
        operation: OperationId,
        text: &str,
    ) -> Result<CommandResolution> {
        let reader = Arc::clone(&self.reader);
        #[cfg(any(test, feature = "native-execution"))]
        if let Some(workspace) = &self.workspace {
            return self
                .resolve_checkout_input(
                    operation,
                    text,
                    CommandCatalog::builtins()?,
                    workspace,
                    reader.as_ref(),
                    false,
                )
                .await;
        }
        let _ = operation;
        let workspace = crate::filesystem::workspace_ref(
            self.project.provider().clone(),
            &self.project.storage_name(&mut None)?,
        )?;
        let generation = self.files.resolve(&workspace, None).await?.generation;
        let root = ContextRoot {
            volume: self.project.clone(),
            directory: String::new(),
        };
        Ok(CommandResolution::Prompt {
            text: text.into(),
            source: None,
            mentions: crate::client::resolve_file_mentions(
                text,
                &root,
                &generation,
                reader.as_ref(),
                self.maximum_bytes,
            )
            .await?,
        })
    }

    #[cfg(any(test, feature = "native-execution"))]
    async fn checkout_command_catalog(
        &self,
        catalog: CommandCatalog,
        root: &ContextRoot,
        workspace: &LocalWorkspace,
        view: &workspace::View,
        cut: acyclic_fs::LazySnapshotRef,
        accounting: &mut acyclic_fs::kernel::KernelAccounting<'_>,
    ) -> Result<CommandCatalog> {
        let (work, cancellation) = original_work(accounting)?;
        let limits = ContextDiscoveryLimits::default();
        let mut directories = 0_u32;
        let mut entries = 0_u32;
        let mut remaining = self.maximum_bytes;
        let mut bounds = native_sources::Bounds {
            directories: &mut directories,
            entries: &mut entries,
            remaining: &mut remaining,
            limits,
        };
        let user_prefix = format!("{}/user", root.directory);
        native_sources::capture(
            self.settings.as_ref(),
            &user_prefix,
            self.publisher.as_ref(),
            work,
            cancellation,
            &mut bounds,
        )
        .await?;
        capture_checkout_commands(
            view,
            cut,
            &root.directory,
            self.publisher.as_ref(),
            accounting,
            &mut bounds,
        )
        .await?;
        let mut catalog = catalog
            .with_sources(
                self.reader.as_ref(),
                &[
                    CommandRoot {
                        scope: CommandScope::User,
                        root: ContextRoot {
                            volume: self.journal.clone(),
                            directory: user_prefix,
                        },
                    },
                    CommandRoot {
                        scope: CommandScope::Project,
                        root: root.clone(),
                    },
                ],
                &[],
                limits,
            )
            .await?;
        // Share the model context's retained catalog. Rediscovering host
        // paths here would lose selected-child and authored skill sources.
        if let Some(sources) = self.files.skill_sources(&self.journal)? {
            let mut discovery = crate::context::DiscoveryWork {
                directories,
                entries,
                limits,
            };
            catalog.add_skills(
                sources
                    .catalog(&workspace.path, view, &mut discovery)?
                    .await?,
            )?;
        }
        Ok(catalog)
    }
    #[cfg(any(test, feature = "native-execution"))]
    async fn resolve_checkout_input(
        &self,
        operation: OperationId,
        text: &str,
        catalog: CommandCatalog,
        workspace: &LocalWorkspace,
        reader: &dyn ContentResidencyVerifier,
        commands: bool,
    ) -> Result<CommandResolution> {
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
        let cut = view.snapshot(&mut accounting).await?;
        let publisher = Arc::clone(&self.publisher);
        let prefix = format!("inputs/composer/{operation}");
        let anchor = format!("{prefix}/.root");
        publisher
            .stage_once(
                context::publication_operation(
                    b"acyclic.local.composer-root/v1\0",
                    anchor.as_bytes(),
                ),
                &anchor,
                b"",
                "text/plain",
                "composer root",
                crate::conversation::ContentAccess::default(),
            )?
            .await?;
        let root = ContextRoot {
            volume: self.journal.clone(),
            directory: prefix.clone(),
        };
        let catalog = if commands
            && text.trim_start().starts_with('/')
            && !catalog.selects_sourceless(text, self.maximum_bytes)?
        {
            self.checkout_command_catalog(catalog, &root, workspace, view, cut, &mut accounting)
                .await?
        } else {
            catalog
        };
        let mut resolution = if commands {
            catalog.resolve(text, reader, self.maximum_bytes).await?
        } else {
            CommandResolution::Prompt {
                text: text.into(),
                source: None,
                mentions: Vec::new(),
            }
        };
        if let CommandResolution::Prompt { text, mentions, .. } = &mut resolution {
            let mut remaining = self.maximum_bytes;
            let mut copied = std::collections::BTreeSet::new();
            for (_, _, path) in crate::client::file_mention_paths(text)? {
                if copied.insert(path) {
                    let used = copy_checkout_file(
                        view,
                        cut,
                        &prefix,
                        path,
                        remaining,
                        publisher.as_ref(),
                        &mut accounting,
                    )
                    .await?;
                    remaining = remaining
                        .checked_sub(used)
                        .ok_or_else(|| Error::Invalid("mention byte bound exceeded".into()))?;
                }
            }
            if !copied.is_empty() {
                let workspace = crate::filesystem::workspace_ref(
                    self.journal.provider().clone(),
                    &self.journal.storage_name(&mut None)?,
                )?;
                let generation = self.files.resolve(&workspace, None).await?.generation;
                *mentions = crate::client::resolve_file_mentions(
                    text,
                    &root,
                    &generation,
                    reader,
                    self.maximum_bytes,
                )
                .await?;
            }
        }
        Ok(resolution)
    }
}

#[cfg(any(test, feature = "native-execution"))]
async fn capture_checkout_commands(
    view: &workspace::View,
    cut: acyclic_fs::LazySnapshotRef,
    prefix: &str,
    publisher: &dyn ContentPublisher,
    accounting: &mut acyclic_fs::kernel::KernelAccounting<'_>,
    bounds: &mut native_sources::Bounds<'_>,
) -> Result<()> {
    let (work, cancellation) = original_work(accounting)?;
    let mut pending = native_storage::PathFrontier::new(work)?;
    for path in [".claude/commands", ".codex/prompts"] {
        pending.push(native_storage::path(work, &[path])?)?;
    }
    while let Some(path) = pending.pop() {
        let path = &path.value;
        let absolute = native_storage::path(work, &["/", path])?;
        *bounds.directories += 1;
        if *bounds.directories > bounds.limits.directories {
            return Err(Error::Invalid("command directory bound exceeded".into()));
        }
        let mut cursor = None;
        loop {
            let listed = view
                .list_directory_at_snapshot(cut, &absolute.value, cursor, 128, accounting)
                .await;
            let (page, _, allocation) = match listed {
                Ok(page) => page,
                Err(
                    acyclic_fs::LazyWorkspaceError::NotFound
                    | acyclic_fs::LazyWorkspaceError::Filesystem(acyclic_fs::FsError::NotFound),
                ) if matches!(path.as_str(), ".claude/commands" | ".codex/prompts") => {
                    break;
                }
                Err(error) => return Err(error.into()),
            };
            let page = Held {
                value: page,
                _allocation: allocation,
            };
            for entry in &page.value.entries {
                *bounds.entries += 1;
                if *bounds.entries > bounds.limits.entries {
                    return Err(Error::Invalid("command entry bound exceeded".into()));
                }
                let (name, allocation) = entry
                    .name
                    .unicode_text(work, cancellation)
                    .map_err(storage)?;
                let name = Held {
                    value: name,
                    _allocation: allocation,
                };
                let name = &name.value;
                let child = native_storage::path(work, &[path, "/", name])?;
                match entry.kind {
                    acyclic_fs::demand::SourceNodeKind::Directory => pending.push(child)?,
                    acyclic_fs::demand::SourceNodeKind::RegularFile if name.ends_with(".md") => {
                        let used = copy_checkout_file(
                            view,
                            cut,
                            prefix,
                            &child.value,
                            *bounds.remaining,
                            publisher,
                            accounting,
                        )
                        .await?;
                        *bounds.remaining = bounds
                            .remaining
                            .checked_sub(used)
                            .ok_or_else(|| Error::Invalid("command byte bound exceeded".into()))?;
                    }
                    _ if name.ends_with(".md") => {
                        return Err(Error::Invalid("command entry is not a regular file".into()));
                    }
                    _ => {}
                }
            }
            cursor = page.value.next;
            if cursor.is_none() {
                break;
            }
        }
    }
    Ok(())
}

#[cfg(any(test, feature = "native-execution"))]
fn original_work<'a>(
    accounting: &acyclic_fs::kernel::KernelAccounting<'a>,
) -> Result<(
    &'a acyclic_fs::NativeMaterializationWork,
    &'a acyclic_fs::CancellationToken,
)> {
    match accounting {
        acyclic_fs::kernel::KernelAccounting::NativeOriginal { work, cancellation } => {
            Ok((*work, *cancellation))
        }
        _ => Err(Error::Invalid(
            "composer capture requires its original work".into(),
        )),
    }
}

#[cfg(any(test, feature = "native-execution"))]
fn source_display_name(path: &str) -> Result<&str> {
    crate::conversation::validate_content_path(path, &mut None)?;
    path.rsplit('/')
        .next()
        .ok_or_else(|| Error::Invalid("composer source has no filename".into()))
}

#[cfg(any(test, feature = "native-execution"))]
async fn copy_checkout_file(
    view: &workspace::View,
    cut: acyclic_fs::LazySnapshotRef,
    prefix: &str,
    path: &str,
    maximum: u64,
    publisher: &dyn ContentPublisher,
    accounting: &mut acyclic_fs::kernel::KernelAccounting<'_>,
) -> Result<u64> {
    crate::conversation::validate_content_path(path, &mut None)?;
    if crate::conversation::is_internal_path(path) {
        return Err(Error::Invalid(
            "composer path targets internal content".into(),
        ));
    }
    let (work, _) = original_work(accounting)?;
    let absolute = native_storage::path(work, &["/", path])?;
    let (chunk, _) = view
        .read_at_snapshot(cut, &absolute.value, None, maximum, accounting)
        .await?;
    let destination = native_storage::path(work, &[prefix, "/", path])?;
    publisher
        .stage_once(
            context::publication_operation(
                b"acyclic.local.composer-file/v1\0",
                destination.value.as_bytes(),
            ),
            &destination.value,
            chunk.bytes.as_ref(),
            crate::model::image_media_type(chunk.bytes.as_ref()).unwrap_or("text/plain"),
            source_display_name(path)?,
            crate::conversation::ContentAccess::default(),
        )?
        .await?;
    Ok(chunk.bytes.len() as u64)
}
