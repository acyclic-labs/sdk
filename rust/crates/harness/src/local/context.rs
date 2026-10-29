use super::*;
use crate::{
    context::{ContextDiscoveryLimits, ContextDiscoveryPolicy, ContextManifest, DiscoveredContext},
    conversation::{ContentResidencyVerifier, FileRef},
    filesystem::FilesystemContentVerifier,
};

#[cfg(any(test, feature = "native-execution"))]
mod checkout;
mod commands;
#[cfg(any(test, feature = "native-execution"))]
mod environment;
mod guidance;
pub(super) mod skills;

pub(super) fn publication_operation(domain: &[u8], key: &[u8]) -> OperationId {
    let mut hash = blake3::Hasher::new();
    hash.update(domain);
    hash.update(key);
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    OperationId::from_bytes(bytes)
}

fn manifest_operation(operation: OperationId) -> OperationId {
    publication_operation(
        b"acyclic.local.context-manifest/v1\0",
        &operation.into_bytes(),
    )
}

fn manifest_path(operation: OperationId) -> String {
    format!("inputs/{operation}.manifest.json")
}

impl LocalHarness {
    pub(super) fn context_reader(
        &self,
    ) -> Result<Arc<FilesystemContentVerifier<LocalAuthorityBackend, LocalObjectBackend>>> {
        FilesystemContentVerifier::new(
            self.files.clone(),
            &self.verifier,
            &self.signed,
            self.limits.file_bytes,
            None,
        )
    }

    async fn observe_context_manifest(
        &self,
        operation: OperationId,
        publisher: &dyn ContentPublisher,
    ) -> Result<Option<FileRef>> {
        let (reference, _) = publisher
            .observe_staged_once(
                manifest_operation(operation),
                // The manifest is a harness record bounded by what it
                // renders, not admitted content under the file bound.
                crate::conversation::MAX_EXACT_JS_INTEGER,
                crate::conversation::ContentAccess::default(),
            )?
            .await?;
        if let Some(reference) = &reference {
            self.limits.validate_record(reference, &mut None)?;
        }
        if reference
            .as_ref()
            .is_some_and(|file| file.path() != manifest_path(operation))
        {
            return Err(Error::Conflict(
                "retained context manifest has another path".into(),
            ));
        }
        Ok(reference)
    }

    /// Pins the live file conventions once per admission, alongside instructions.
    #[inline(never)]
    pub(super) fn stage_context_manifest_with<'a>(
        &'a self,
        operation: OperationId,
        prompt: Option<&'a str>,
        publisher: &'a dyn ContentPublisher,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, FileRef>> {
        crate::conversation::BoundedContentFuture::new(None, || async move {
            if let Some(reference) = self.observe_context_manifest(operation, publisher).await? {
                return Ok(reference);
            }
            let path = manifest_path(operation);
            if path.len() > self.limits.path_bytes {
                return Err(Error::Invalid(
                    "context manifest path exceeds path bound".into(),
                ));
            }
            #[cfg(any(test, feature = "hooks"))]
            let hooks =
                crate::native_settings::NativeSettings::load_hooks(Arc::clone(self.settings()?))
                    .await?;
            #[cfg(any(test, feature = "hooks"))]
            let hook_configuration = if hooks.is_empty() {
                None
            } else {
                Some(hooks.publish(publisher, self.limits).await?)
            };
            let policy = ContextDiscoveryPolicy::default();
            let limits = ContextDiscoveryLimits::default();
            let maximum = limits.instruction_bytes.min(self.limits.file_bytes);
            let mut work = crate::context::DiscoveryWork {
                limits: ContextDiscoveryLimits {
                    instruction_bytes: maximum,
                    ..limits
                },
                ..Default::default()
            };
            let discovery = self
                .instructions(&policy.instruction_names, publisher, &mut work)
                .await?;
            let (goal, plans) = self.file_guidance(prompt.unwrap_or_default())?.await?;
            let mut manifest =
                ContextManifest::for_files(discovery, Vec::new(), self.tools(), goal, plans)?;
            // The agent tools bind per turn, outside the registry; the model
            // sees them through the manifest like every other tool. A turn
            // binds each over any registered tool of its name, so the manifest
            // lists it in that tool's place.
            let agent_tools = crate::tools::agent_tool_definitions(&self.preset.tools)?;
            manifest
                .tools
                .retain(|tool| agent_tools.iter().all(|agent| agent.name != tool.name));
            manifest.tools.extend(agent_tools);
            #[cfg(any(test, feature = "native-execution"))]
            if let Some(workspace) = &self.workspace {
                manifest
                    .conventions
                    .push(workspace.context_line().to_owned());
                if self.checkout_context {
                    #[cfg(test)]
                    self.environment_reads
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    // A switched native mount is selected during tool
                    // execution, so it cannot truthfully advertise this path.
                    let cwd = if !(workspace.mounted.is_some()
                        && workspace.selected_view().await?.0.is_some())
                    {
                        Some(workspace.path.as_path())
                    } else {
                        None
                    };
                    let physical_git = cwd.is_some()
                        && matches!(
                            acyclic_fs::GitCompatStore::load(
                                &workspace.store,
                                workspace.view().workspace().id(),
                            )
                            .await,
                            Ok(None)
                        );
                    let (model, _) = self.agent()?;
                    manifest.environment = Some(
                        environment::gather(
                            cwd,
                            self.clock.now_unix_millis(),
                            &model.name,
                            &self.environment_platform,
                            physical_git,
                        )
                        .await,
                    );
                }
            }
            #[cfg(any(test, feature = "hooks"))]
            manifest.sources.extend(hook_configuration);
            let bytes = manifest.encode(self.limits)?;
            Ok(publisher
                .stage_once(
                    manifest_operation(operation),
                    &path,
                    &bytes,
                    "application/json",
                    "context-manifest.json",
                    crate::conversation::ContentAccess::default(),
                )?
                .await?
                .0)
        })
    }

    async fn instructions(
        &self,
        names: &[String],
        publisher: &dyn ContentPublisher,
        work: &mut crate::context::DiscoveryWork,
    ) -> Result<DiscoveredContext> {
        #[cfg(any(test, feature = "native-execution"))]
        if self.checkout_context
            && let Some(workspace) = &self.workspace
        {
            let mut discovery = self
                .checkout_instructions_with_work(workspace, names, publisher, work)
                .await?;
            discovery.skills = self.skills_with_work(None, work).await?;
            return Ok(discovery);
        }
        let _ = publisher;
        self.volume_instructions_with_work(names, self.context_reader()?.as_ref(), work)
            .await
    }

    async fn volume_instructions_with_work(
        &self,
        names: &[String],
        reader: &dyn ContentResidencyVerifier,
        work: &mut crate::context::DiscoveryWork,
    ) -> Result<DiscoveredContext> {
        let workspace = crate::filesystem::workspace_ref(
            self.project.provider().clone(),
            &self.project.storage_name(&mut None)?,
        )?;
        let generation = self.files.resolve(&workspace, None).await?.generation;
        let root = crate::context::ContextRoot {
            volume: self.project.clone(),
            directory: String::new(),
        };
        let mut source = VolumeInstructions {
            harness: self,
            source: crate::context::VolumeDiscovery {
                reader,
                root: &root,
            },
        };
        let instructions = crate::context::capture_instructions(
            &mut source,
            "",
            &self.active_directory,
            names,
            work,
            Some(&generation),
        )
        .await?;
        let mut discovery = DiscoveredContext {
            instructions,
            skills: Vec::new(),
        };
        discovery.skills = self.skills_with_work(Some(&generation), work).await?;
        Ok(discovery)
    }
}

impl LocalHarness {
    /// The attachment manifest of the files a prompt's `@` mentions name,
    /// pinned as it is sent: a checkout's mentioned files are captured at one
    /// snapshot onto the root's private volume, a volume project's are read
    /// at its current generation. Each resolves and projects through the one
    /// mention path. `key` is the message's own identity: the manifest is
    /// staged once for it, beside its copies, so a resend replays it exactly
    /// whatever has happened to the files since, and the message carries one
    /// reference however many files it names.
    pub(super) async fn prompt_attachments(
        &self,
        key: &[u8],
        text: Option<&str>,
    ) -> Result<Option<FileRef>> {
        // A new prompt without mentions stages nothing and observes nothing.
        if let Some(text) = text
            && crate::client::file_mention_paths(text)?.is_empty()
        {
            return Ok(None);
        }
        // The manifest's own encoding bounds it: it lists files already
        // admitted under the content bounds, so it takes no content bound.
        let publisher =
            self.input_publisher_with_limit(crate::conversation::MAX_EXACT_JS_INTEGER)?;
        let operation = publication_operation(b"acyclic.local.mention-manifest/v1\0", key);
        if let Some(manifest) = self.observe_manifest(operation, &*publisher).await? {
            return Ok(Some(manifest));
        }
        // A replay whose first delivery staged no manifest attaches none.
        let Some(text) = text else {
            return Ok(None);
        };
        let reader = self.client_reader()?;
        let base = format!("inputs/mentions/{}", blake3::hash(key));
        let items = self
            .resolve_attachments(&base, text, &*reader)
            .await?
            .into_iter()
            .map(|file| crate::conversation::Attachment { file, label: None })
            .collect::<Vec<_>>();
        let bytes = crate::conversation::encode_attachment_manifest(&items)?;
        let path = format!("{base}/{}.json", blake3::hash(&bytes));
        let staged = publisher
            .stage_once(
                operation,
                &path,
                &bytes,
                crate::conversation::ATTACHMENT_MANIFEST_MEDIA_TYPE,
                "attachments.json",
                crate::conversation::ContentAccess::default(),
            )?
            .await;
        match staged {
            Ok((manifest, _)) => Ok(Some(manifest)),
            // A concurrent first send pinned its manifest first: that one stands.
            Err(error) => self
                .observe_manifest(operation, &*publisher)
                .await?
                .map(Some)
                .ok_or(error),
        }
    }

    async fn observe_manifest(
        &self,
        operation: OperationId,
        publisher: &dyn ContentPublisher,
    ) -> Result<Option<FileRef>> {
        let (manifest, _) = publisher
            .observe_staged_once(
                operation,
                crate::conversation::MAX_EXACT_JS_INTEGER,
                crate::conversation::ContentAccess::default(),
            )?
            .await?;
        Ok(manifest)
    }

    async fn resolve_attachments(
        &self,
        base: &str,
        text: &str,
        reader: &dyn ContentResidencyVerifier,
    ) -> Result<Vec<FileRef>> {
        // Mentioned files are copied beside the manifest on the private
        // volume, which every fork of the agent inherits: a child forks a
        // volume project, so the original project would not carry them.
        let root = match self.checkout_mentions(base, text).await? {
            Some(root) => root,
            None => self.project_mentions(base, text, reader).await?,
        };
        self.resolve_at(&root, text, reader, |path| {
            format!("{}/{}", root.directory, copy_name(path))
        })
        .await
    }

    /// A volume project's mentioned files, read at its head and copied onto
    /// the root's private volume below `base`: their root.
    async fn project_mentions(
        &self,
        base: &str,
        text: &str,
        reader: &dyn ContentResidencyVerifier,
    ) -> Result<crate::context::ContextRoot> {
        let project = crate::context::ContextRoot {
            volume: self.project.clone(),
            directory: String::new(),
        };
        let mut copies = std::collections::BTreeMap::new();
        let locate = |path: &str| path.to_owned();
        for file in self.resolve_at(&project, text, reader, locate).await? {
            let bytes = reader
                .read(&file, file.descriptor().byte_length(), None, None)?
                .await?;
            copies.insert(file.path().trim_start_matches('/').to_owned(), bytes);
        }
        Ok(crate::context::ContextRoot {
            volume: self.journal.clone(),
            directory: self.publish_mention_copies(base, copies).await?,
        })
    }

    /// Publishes mentioned files' `copies` by path into one content-addressed
    /// directory below `base` on the private volume and names it. Each copy
    /// is named by [`copy_name`], so its path has the same length however
    /// long the mentioned path is.
    async fn publish_mention_copies(
        &self,
        base: &str,
        copies: std::collections::BTreeMap<String, acyclic_stream::OwnedBytes>,
    ) -> Result<String> {
        let mut address = blake3::Hasher::new();
        for (path, bytes) in &copies {
            address.update(&(path.len() as u64).to_le_bytes());
            address.update(path.as_bytes());
            address.update(blake3::hash(bytes.as_ref()).as_bytes());
        }
        let directory = format!("{base}/{}", address.finalize());
        // Every copy lands in one generation of the private volume, so a
        // prompt costs one commit however many files it mentions. A retry
        // of the same bytes replays that generation.
        let mut mutations = Vec::with_capacity(copies.len() + 1);
        mutations.push(crate::filesystem::WorkspaceMutation::CreateDirectory {
            path: format!("/{directory}"),
        });
        mutations.extend(copies.into_iter().map(|(path, bytes)| {
            crate::filesystem::WorkspaceMutation::PutFile {
                path: format!("/{directory}/{}", copy_name(&path)),
                bytes,
            }
        }));
        let journal = crate::filesystem::workspace_ref(
            self.journal.provider().clone(),
            &self.journal.storage_name(&mut None)?,
        )?;
        self.files
            .apply(
                &journal,
                None,
                &mutations,
                &crate::contract::IdempotencyKey::new(format!(
                    "mention-copies-{}",
                    blake3::hash(directory.as_bytes())
                ))?,
            )
            .await?;
        Ok(directory)
    }

    /// The files `text` mentions, resolved at the head of `root`'s volume,
    /// in mention order.
    async fn resolve_at(
        &self,
        root: &crate::context::ContextRoot,
        text: &str,
        reader: &dyn ContentResidencyVerifier,
        locate: impl Fn(&str) -> String,
    ) -> Result<Vec<FileRef>> {
        let workspace = crate::filesystem::workspace_ref(
            root.volume.provider().clone(),
            &root.volume.storage_name(&mut None)?,
        )?;
        let generation = self.files.resolve(&workspace, None).await?.generation;
        let mentions = crate::client::resolve_file_mentions_at(
            text,
            root,
            &generation,
            reader,
            self.limits.file_bytes,
            locate,
        )
        .await?;
        let content = crate::command_request::CommandResolution::Prompt {
            text: text.into(),
            source: None,
            mentions,
        }
        .into_model_input()?;
        // The leading part is the prompt text, which the message body carries.
        Ok(match content {
            crate::model::ModelContent::Parts(parts) => parts
                .into_iter()
                .filter_map(|part| match part {
                    crate::model::ModelContentPart::File { file, .. } => Some(file),
                    _ => None,
                })
                .collect(),
            _ => Vec::new(),
        })
    }
}

/// The name of a mentioned path's copy: 128 bits of its hash, so the copy's
/// path has a fixed length. A model never sees it; it sees the mentioned
/// path in the prompt and the copy's bytes.
pub(in crate::local) fn copy_name(path: &str) -> String {
    blake3::hash(path.as_bytes()).to_hex()[..32].to_owned()
}

impl LocalHarness {
    /// A checkout's mentioned files, copied onto the root's private volume
    /// below the message's `base` directory: their root.
    #[cfg(any(test, feature = "native-execution"))]
    async fn checkout_mentions(
        &self,
        base: &str,
        text: &str,
    ) -> Result<Option<crate::context::ContextRoot>> {
        let Some(workspace) = &self.workspace else {
            return Ok(None);
        };
        let directory = self
            .capture_checkout_mentions(workspace, text, base)
            .await?;
        Ok(Some(crate::context::ContextRoot {
            volume: self.journal.clone(),
            directory,
        }))
    }

    #[cfg(not(any(test, feature = "native-execution")))]
    async fn checkout_mentions(
        &self,
        _: &str,
        _: &str,
    ) -> Result<Option<crate::context::ContextRoot>> {
        Ok(None)
    }
}

struct VolumeInstructions<'a> {
    harness: &'a LocalHarness,
    source: crate::context::VolumeDiscovery<'a>,
}
impl crate::context::DiscoverySource for VolumeInstructions<'_> {
    type Revision = crate::resources::GenerationRef;
    type Cursor = String;
    fn page<'a>(
        &'a mut self,
        directory: &'a str,
        revision: Option<&'a Self::Revision>,
        cursor: Option<String>,
        maximum: u32,
    ) -> acyclic_stream::BoxProviderFuture<
        'a,
        Result<crate::context::DiscoveryPage<Self::Revision, String>>,
    > {
        self.source.page(directory, revision, cursor, maximum)
    }
}
impl crate::context::InstructionReader for VolumeInstructions<'_> {
    fn instruction<'a>(
        &'a mut self,
        path: &'a str,
        revision: &'a Self::Revision,
        maximum: u64,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<crate::model::ProjectInstruction>> {
        Box::pin(async move {
            if path.len() > self.harness.limits.path_bytes {
                return Err(Error::Invalid("instruction path exceeds path bound".into()));
            }
            let instruction = self.source.instruction(path, revision, maximum).await?;
            self.harness.limits.validate_file(&instruction, &mut None)?;
            #[cfg(test)]
            if let Some(observer) = self.harness.context_read_observer {
                observer(self.harness, path).await?;
            }
            Ok(instruction)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use proptest::prelude::*;

    fn retained_operation_bytes<Args, T>(_: impl FnOnce(Args) -> T) -> usize {
        std::mem::size_of::<T>()
    }

    #[test]
    fn manifest_children_retain_only_owned_polling_handles() {
        let observation = retained_operation_bytes(
            |(local, publisher): (&'static LocalHarness, &'static dyn ContentPublisher)| {
                local.observe_context_manifest(OperationId::from_bytes([1; 16]), publisher)
            },
        );
        let guidance =
            retained_operation_bytes(|local: &'static LocalHarness| local.file_guidance(""));
        eprintln!("manifest child retained bytes: [{observation}, {guidance}]");
        assert!(
            [observation, guidance]
                .into_iter()
                .all(|bytes| bytes <= 512),
            "manifest embeds concrete observation or guidance futures"
        );
    }

    #[test]
    fn context_boundaries_retain_only_owned_polling_handles() {
        let manifest = retained_operation_bytes(
            |(local, publisher): (&'static LocalHarness, &'static dyn ContentPublisher)| {
                local.stage_context_manifest_with(OperationId::from_bytes([1; 16]), None, publisher)
            },
        );
        #[cfg(feature = "native-execution")]
        let catalog = retained_operation_bytes(
            |(sources, root, view, work): (
                &'static crate::context::native_skills::NativeSkills,
                &'static std::path::Path,
                &'static super::super::workspace::View,
                &'static mut crate::context::DiscoveryWork,
            )| sources.catalog(root, view, work),
        );
        #[cfg(feature = "native-execution")]
        let users = retained_operation_bytes(
            |(sources, work): (
                &'static crate::context::native_skills::NativeSkills,
                &'static mut crate::context::DiscoveryWork,
            )| sources.user_catalog(work),
        );
        #[cfg(feature = "native-execution")]
        let sizes = [manifest, catalog, users];
        #[cfg(not(feature = "native-execution"))]
        let sizes = [manifest];
        eprintln!("context retained bytes: {sizes:?}");
        assert!(
            sizes.into_iter().all(|bytes| bytes <= 512),
            "context embeds a concrete child operation in its caller"
        );
    }

    proptest! {
        // Property: every accepted instruction round-trips without normalization;
        // exactly the positive byte bound, UTF-8 and NUL constraints determine acceptance.
        #[test]
        fn instruction_text_round_trip(bytes in prop::collection::vec(any::<u8>(), 0..128), maximum in 0u64..128) {
            let limits = ContextDiscoveryLimits { instruction_bytes: maximum, ..Default::default() };
            let accepted = limits.validate_instruction(&bytes).is_ok();
            prop_assert_eq!(accepted, maximum > 0 && bytes.len() as u64 <= maximum && std::str::from_utf8(&bytes).is_ok() && !bytes.contains(&0));
            if accepted {
                prop_assert_eq!(std::str::from_utf8(&bytes).unwrap().as_bytes(), bytes.as_slice());
                let mut invalid = bytes;
                invalid.push(0);
                prop_assert!(limits.validate_instruction(&invalid).is_err());
            }
        }

        // Property: generated request paths round-trip their exact operation,
        // remain in the private input subtree, and cannot alias another turn.
        #[test]
        fn manifest_paths_round_trip(operation in any::<[u8;16]>(), other in any::<[u8;16]>()) {
            let operation = OperationId::from_bytes(operation);
            let other = OperationId::from_bytes(other);
            let path = manifest_path(operation);
            crate::conversation::validate_content_path(&path, &mut None)?;
            let name = path.strip_prefix("inputs/").unwrap().strip_suffix(".manifest.json").unwrap();
            let decoded: OperationId = serde_json::from_value(serde_json::Value::String(name.into()))?;
            prop_assert_eq!(decoded, operation);
            prop_assert!(path.len() <= Limits::default().path_bytes);
            prop_assert_eq!(manifest_path(operation), path.clone());
            if operation != other {
                prop_assert_ne!(path, manifest_path(other));
            }
            prop_assert_eq!(manifest_operation(operation), manifest_operation(decoded));
        }
    }
}
