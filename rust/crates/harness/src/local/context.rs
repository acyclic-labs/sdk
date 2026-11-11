use super::*;
use crate::{
    context::{
        ContextDiscoveryLimits, ContextDiscoveryPolicy, ContextEpoch, ContextManifest,
        DiscoveredContext,
    },
    conversation::{ContentResidencyVerifier, FileRef},
    filesystem::FilesystemContentVerifier,
};

#[cfg(any(test, feature = "native-execution"))]
mod checkout;
mod commands;
#[cfg(any(test, feature = "native-execution"))]
mod environment;
pub(super) mod skills;

/// The executing agent's retained volumes and read authority. The workspace is
/// the same bound view used by its tools, including for a spawned agent.
pub(super) struct Binding<'a> {
    pub(super) project: &'a VolumeRef,
    pub(super) scratch: &'a VolumeRef,
    #[cfg(any(test, feature = "native-execution"))]
    pub(super) journal: &'a VolumeRef,
    pub(super) reader: &'a dyn ContentResidencyVerifier,
    #[cfg(any(test, feature = "native-execution"))]
    pub(super) verifier: AuthorityVerifier,
    #[cfg(any(test, feature = "native-execution"))]
    pub(super) scope: &'a Scope,
    #[cfg(any(test, feature = "native-execution"))]
    pub(super) workspace: Option<Arc<LocalWorkspace>>,
}

pub(super) fn publication_operation(domain: &[u8], key: &[u8]) -> OperationId {
    let mut hash = blake3::Hasher::new();
    hash.update(domain);
    hash.update(key);
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    OperationId::from_bytes(bytes)
}

/// What one agent was last told of its epoch's model-visible sources.
pub(super) struct Told {
    epoch: FileRef,
    file: Option<FileRef>,
    state: ToldState,
    /// Exclusive inbox cursor after the last committed notice. Every turn
    /// preparation fences its first request with it, so a retried
    /// preparation still admits a notice an earlier attempt sent; a reload
    /// recovers it by resending the retained outbox entry.
    through: Option<u64>,
}

impl Told {
    fn delivered(&mut self, delivery: crate::agent::Delivered) -> Result<()> {
        if let crate::agent::Delivered::Committed { inbox } = delivery {
            let cursor = inbox
                .checked_add(1)
                .ok_or_else(|| Error::Invalid("context delivery cursor overflow".into()))?;
            self.through = Some(self.through.map_or(cursor, |previous| previous.max(cursor)));
        }
        Ok(())
    }
}

/// The durable delivery frontier, including the outbox entry that must be
/// resent after a crash. Publishing it precedes the idempotent inbox send.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ToldState {
    version: u8,
    digests: std::collections::BTreeMap<String, [u8; 32]>,
    sent: u64,
    pending: Option<crate::agent::InboxRecord>,
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

    /// The complete tool array every request of an epoch declares. The agent
    /// tools bind per turn, outside the registry; a turn binds each over any
    /// registered tool of its name, so the array lists it in that tool's place.
    pub(super) fn declared_tools(&self) -> Result<Vec<crate::tool::ToolDefinition>> {
        let agent_tools = crate::tools::agent_tool_definitions(&self.agent_tools)?;
        let mut tools = self.tools().definitions(None)?;
        tools.retain(|tool| agent_tools.iter().all(|agent| agent.name != tool.name));
        tools.extend(agent_tools);
        Ok(tools)
    }

    /// `agent`'s frozen context epoch: rendered from the live sources and
    /// staged once at session start and after each compaction (its context
    /// checkpoint), and replayed unchanged by every other turn, so the cached
    /// prefix never changes between compactions.
    #[inline(never)]
    pub(super) fn context_epoch<'a>(
        &'a self,
        agent: &'a crate::agent::AgentState,
        binding: &'a Binding<'a>,
        publisher: &'a dyn ContentPublisher,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ContextEpoch>> {
        crate::conversation::BoundedContentFuture::new(None, || async move {
            let tools = self.declared_tools()?;
            let operation = ContextEpoch::operation(agent.path.as_str(), agent.checkpoint)?;
            ContextEpoch::open(publisher, operation, self.limits, || {
                self.live_manifest(binding, publisher, tools)
            })
            .await
        })
    }

    /// Tells `agent`, as appended quiet notices, each model-visible source
    /// change since its epoch froze that it was not told yet. Its requests
    /// keep the epoch's prefix and see each change after it. A notice's
    /// intent binds the epoch, the source, both digests and the durable
    /// count. The retained outbox recovers a send interrupted by a crash;
    /// unchanged requests use the in-memory frontier without storage reads.
    pub(super) async fn announce_changes(
        &self,
        agent: &crate::agent::AgentState,
        epoch: &ContextEpoch,
        binding: &Binding<'_>,
        publisher: &dyn ContentPublisher,
        reader: &dyn ContentResidencyVerifier,
    ) -> Result<Option<u64>> {
        let Some(hosted) = self.agents.get() else {
            return Ok(None);
        };
        let live = self.live_manifest(binding, publisher, Vec::new()).await?;
        let cached = self
            .told
            .lock()
            .map_err(|_| Error::Storage("context change lock poisoned".into()))?
            .remove(&agent.path)
            .filter(|told| told.epoch == *epoch.manifest());
        let path = format!("{}.told.v1.json", epoch.manifest().path());
        let mut told = match cached {
            Some(told) => told,
            None => {
                let retained = reader
                    .read_private_path(
                        publisher.volume(),
                        "inputs",
                        &path,
                        None,
                        crate::conversation::MAX_EXACT_JS_INTEGER,
                        crate::conversation::ContentAccess::default(),
                    )?
                    .await?;
                let (file, state) = match retained {
                    Some((file, bytes, _)) => {
                        let state: ToldState = serde_json::from_slice(&bytes).map_err(storage)?;
                        if state.version != 1 {
                            return Err(Error::Invalid(
                                "unsupported context delivery version".into(),
                            ));
                        }
                        (Some(file), state)
                    }
                    None => (
                        None,
                        ToldState {
                            version: 1,
                            digests: epoch.digests(reader, self.limits).await?,
                            sent: 0,
                            pending: None,
                        },
                    ),
                };
                Told {
                    epoch: epoch.manifest().clone(),
                    file,
                    state,
                    through: None,
                }
            }
        };
        let by = crate::agent::Sender::Agent {
            path: agent.path.clone(),
        };
        if let Some(pending) = told.state.pending.take() {
            told.delivered(
                hosted
                    .agents()
                    .send(&agent.path, &by, &pending)
                    .await
                    .map_err(storage)?,
            )?;
        }
        for change in ContextEpoch::changes(&told.state.digests, &live, reader).await? {
            let mut hash = blake3::Hasher::new_derive_key("acyclic.local.context-change.v1");
            hash.update(epoch.manifest().descriptor().sha256());
            hash.update(&(change.source.len() as u64).to_le_bytes());
            hash.update(change.source.as_bytes());
            hash.update(told.state.digests.get(&change.source).unwrap_or(&[0; 32]));
            hash.update(change.digest.as_ref().unwrap_or(&[0; 32]));
            hash.update(&told.state.sent.to_le_bytes());
            let id = crate::agent::IntentId::try_from(format!(
                "context:{}",
                &hash.finalize().to_hex()[..32]
            ))
            .map_err(storage)?;
            let operation =
                publication_operation(b"acyclic.local.context-told/v1\0", id.as_str().as_bytes());
            let record = crate::agent::InboxRecord::Notice {
                id,
                notice: crate::agent::NoticePayload::Quiet(change.notice),
            };
            match change.digest {
                Some(digest) => told.state.digests.insert(change.source, digest),
                None => told.state.digests.remove(&change.source),
            };
            told.state.sent += 1;
            told.state.pending = Some(record.clone());
            let bytes = serde_json::to_vec(&told.state).map_err(storage)?;
            // The generation check prevents two writers from advancing the
            // same frontier; the operation receipt recovers an uncertain write.
            let (file, _) = match &told.file {
                Some(file) => {
                    publisher
                        .stage_at(operation, file, &bytes, None, None)?
                        .await?
                }
                None => {
                    publisher
                        .stage_once(
                            operation,
                            &path,
                            &bytes,
                            "application/json",
                            "context-delivery.json",
                            crate::conversation::ContentAccess::default(),
                        )?
                        .await?
                }
            };
            told.file = Some(file);
            told.delivered(
                hosted
                    .agents()
                    .send(&agent.path, &by, &record)
                    .await
                    .map_err(storage)?,
            )?;
            told.state.pending = None;
        }
        let through = told.through;
        self.told
            .lock()
            .map_err(|_| Error::Storage("context change lock poisoned".into()))?
            .insert(agent.path.clone(), told);
        Ok(through)
    }

    /// This turn's hook configuration: the live one, pinned once per turn so
    /// a resumed turn binds the hooks it started with. Hooks are behaviour,
    /// not prefix, so they change at the next turn, never waiting for an
    /// epoch.
    #[cfg(any(test, feature = "hooks"))]
    pub(super) async fn turn_hooks(
        &self,
        turn: OperationId,
        publisher: &dyn ContentPublisher,
    ) -> Result<Option<FileRef>> {
        let operation = publication_operation(b"acyclic.local.turn-hooks/v1\0", &turn.into_bytes());
        if let Some(hooks) = self.observe_manifest(operation, publisher).await? {
            return Ok(Some(hooks));
        }
        // The user's imported host hooks rank above every plugin.
        let mut hooks = match &self.settings {
            Some(settings) => {
                crate::native_settings::NativeSettings::load_hooks(Arc::clone(settings)).await?
            }
            None => crate::hooks::Hooks::default(),
        };
        hooks.append(self.plugin_hooks.clone());
        let bytes = hooks.encode(self.limits.file_bytes)?;
        let path = format!("inputs/{turn}.hooks.json");
        let staged = publisher
            .stage_once(
                operation,
                &path,
                &bytes,
                crate::hooks::Hooks::MEDIA_TYPE,
                "hooks.json",
                crate::conversation::ContentAccess::default(),
            )?
            .await;
        match staged {
            Ok((hooks, _)) => Ok(Some(hooks)),
            Err(error) => self
                .observe_manifest(operation, publisher)
                .await?
                .map(Some)
                .ok_or(error),
        }
    }
    /// The manifest the live sources render now, as `binding` sees them.
    pub(super) async fn live_manifest(
        &self,
        binding: &Binding<'_>,
        publisher: &dyn ContentPublisher,
        tools: Vec<crate::tool::ToolDefinition>,
    ) -> Result<ContextManifest> {
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
            .instructions(binding, &policy.instruction_names, publisher, &mut work)
            .await?;
        let mut manifest = ContextManifest::new(discovery, Vec::new(), self.tools())?;
        manifest.tools = tools;
        let mode = match self.preset.approval_policy.approvers.as_slice() {
            [
                crate::approvals::Approver::Rules {
                    rules,
                    fallback: crate::approvals::RuleVerdict::Allow,
                    ..
                },
            ] if rules.is_empty() => "all admitted actions allowed",
            _ => "policy-controlled approvals",
        };
        // A physical workspace projects no home volume.
        #[cfg(any(test, feature = "native-execution"))]
        let home = binding
            .workspace
            .as_ref()
            .is_none_or(|workspace| workspace.mounting == super::WorkspaceMounting::Native);
        #[cfg(not(any(test, feature = "native-execution")))]
        let home = true;
        let home = if home {
            " .volumes/home is your private retained home volume and is never merged into the project."
        } else {
            ""
        };
        let sandbox = if cfg!(any(target_os = "linux", target_os = "macos")) {
            "Shell commands and lifecycle hooks run in a workspace-write OS sandbox."
        } else {
            "Shell commands retain the host boundary; native OS confinement is not yet available."
        };
        manifest.conventions.push(format!(
            "Approval mode: {mode}. {sandbox} A refusal returns a tool failure; adapt or report the block, never assume the action ran.{home} .volumes/shared belongs to this session tree and is shared with its descendants; condition edits on the exact version you read."
        ));
        #[cfg(any(test, feature = "native-execution"))]
        if let Some(workspace) = &binding.workspace {
            manifest
                .conventions
                .push(workspace.context_line().to_owned());
            if self.checkout_context {
                let (selected, _) = workspace.selected_view().await?;
                // A selected branch can use a separate physical mount. Reuse
                // that admitted handle; never report the base mount for it.
                let selected_mount = if let Some(view) = &selected
                    && workspace.mounted.is_some()
                {
                    workspace
                        .state
                        .lock()
                        .await
                        .workspace
                        .as_ref()
                        .filter(|(id, _)| *id == view.workspace().id())
                        .map(|(_, mounted)| Arc::clone(mounted))
                } else {
                    None
                };
                let cwd = if selected.is_none() || workspace.mounted.is_none() {
                    Some(workspace.path.as_path())
                } else {
                    selected_mount
                        .as_ref()
                        .map(|mounted| mounted.path.as_path())
                };
                let (model, _) = self.agent()?;
                manifest.environment = Some(environment::gather(
                    cwd,
                    self.clock.now_unix_millis(),
                    &model.name,
                    &self.environment_platform,
                )?);
            }
        }
        Ok(manifest)
    }

    async fn instructions(
        &self,
        binding: &Binding<'_>,
        names: &[String],
        publisher: &dyn ContentPublisher,
        work: &mut crate::context::DiscoveryWork,
    ) -> Result<DiscoveredContext> {
        #[cfg(any(test, feature = "native-execution"))]
        if self.checkout_context
            && let Some(workspace) = &binding.workspace
        {
            let mut discovery = self
                .checkout_instructions_with_work(
                    workspace,
                    names,
                    publisher,
                    work,
                    (&binding.verifier, binding.scope),
                )
                .await?;
            discovery.skills = self.bound_skills(binding, None, work)?.await?;
            return Ok(discovery);
        }
        let _ = publisher;
        self.volume_instructions_with_work(binding, names, work)
            .await
    }

    async fn volume_instructions_with_work(
        &self,
        binding: &Binding<'_>,
        names: &[String],
        work: &mut crate::context::DiscoveryWork,
    ) -> Result<DiscoveredContext> {
        let workspace = crate::filesystem::workspace_ref(
            binding.project.provider().clone(),
            &binding.project.storage_name(&mut None)?,
        )?;
        let generation = self.files.resolve(&workspace, None).await?.generation;
        let root = crate::context::ContextRoot {
            volume: binding.project.clone(),
            directory: String::new(),
        };
        let mut source = VolumeInstructions {
            harness: self,
            source: crate::context::VolumeDiscovery {
                reader: binding.reader,
                root: &root,
                io: None,
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
        discovery.skills = self.bound_skills(binding, Some(&generation), work)?.await?;
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
                None,
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
        Result<Option<crate::context::DiscoveryPage<Self::Revision, String>>>,
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
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<Option<crate::model::ProjectInstruction>>>
    {
        Box::pin(async move {
            if self
                .harness
                .limits
                .path_bytes
                .is_some_and(|maximum| path.len() > maximum.get())
            {
                return Err(Error::Invalid("instruction path exceeds path bound".into()));
            }
            let Some(instruction) = self.source.instruction(path, revision, maximum).await? else {
                return Ok(None);
            };
            self.harness.limits.validate_file(&instruction, &mut None)?;
            #[cfg(test)]
            if let Some(observer) = self.harness.context_read_observer {
                observer(self.harness, path).await?;
            }
            Ok(Some(instruction))
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
    fn context_boundaries_retain_only_owned_polling_handles() {
        let manifest = retained_operation_bytes(
            |(local, agent, binding, publisher): (
                &'static LocalHarness,
                &'static crate::agent::AgentState,
                &'static Binding<'static>,
                &'static dyn ContentPublisher,
            )| local.context_epoch(agent, binding, publisher),
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

        // Property: an epoch's path stays in the private input subtree within
        // the path bound, and agents or checkpoints never share an epoch.
        #[test]
        fn epochs_are_distinct_per_agent_and_checkpoint(agent in "[a-z]{1,16}", other in "[a-z]{1,16}", checkpoint in proptest::option::of(any::<u64>()), next in any::<u64>()) {
            let operation = ContextEpoch::operation(&agent, checkpoint)?;
            let path = ContextEpoch::path(operation);
            crate::conversation::validate_content_path(&path, &mut None)?;
            prop_assert_eq!(ContextEpoch::operation(&agent, checkpoint)?, operation);
            if agent != other {
                prop_assert_ne!(ContextEpoch::operation(&other, checkpoint)?, operation);
            }
            if checkpoint != Some(next) {
                prop_assert_ne!(ContextEpoch::operation(&agent, Some(next))?, operation);
            }
        }
    }
}
