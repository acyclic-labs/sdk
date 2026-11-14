use super::*;
use acyclic_native_runtime::signing_key::{MemorySigningStore, SigningStore};

/// Resolves ambient or injected roots before checkout capture can write metadata.
pub(super) enum StateLocation {
    Directory(PathBuf),
    ProjectRoot(PathBuf),
}

impl StateLocation {
    pub(super) fn resolve(
        &self,
        project: &LocalProject,
        checkout: Option<&owner::Checkout>,
    ) -> Result<PathBuf> {
        match self {
            Self::Directory(directory) => Ok(directory.clone()),
            Self::ProjectRoot(root) => owner::state_directory_in(root, project, checkout),
        }
    }
}

/// Storage selected for the agent's project view.
pub(super) enum LocalProject {
    /// A normal checkout, opened lazily without importing its files.
    Workspace(PathBuf),
    /// An existing project volume in a local filesystem directory.
    Volume {
        /// Existing local filesystem provider directory.
        directory: PathBuf,
        /// Existing project volume within that provider.
        volume: VolumeRef,
    },
}

/// How a checkout's task views reach the host filesystem.
///
/// `Native` projects every task view, the home volume and bound volumes
/// through the platform's native provider (Linux FUSE, Windows ProjFS, macOS
/// NFS) and refuses to open without it. `Physical` needs no device, capability
/// or mount helper: the checkout and every task view are plain directories,
/// and their changes are captured from userspace at each tool boundary. See
/// `NATIVE_MOUNT_QUALIFICATION.md` for the guarantees that differ.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum WorkspaceMounting {
    /// Native projection; opening refuses when the host lacks it.
    #[default]
    Native,
    /// Plain directories with userspace change capture.
    Physical,
}

impl WorkspaceMounting {
    /// `Native` when this host admits a native projection; otherwise
    /// `Physical`, with the probe's reason for the caller to report.
    #[cfg(not(target_arch = "wasm32"))]
    #[must_use]
    pub fn detect() -> (Self, Option<&'static str>) {
        match acyclic_fs::probe_native_mount().unavailable_reason {
            None => (Self::Native, None),
            Some(reason) => (Self::Physical, Some(reason)),
        }
    }
}

/// Durable local composition with a required model binding.
pub struct LocalHarnessConfig {
    pub(super) topology: Option<crate::topology::Topology>,
    pub(super) home_files: std::collections::BTreeMap<String, acyclic_stream::OwnedBytes>,
    pub(super) system_files: std::collections::BTreeMap<String, acyclic_stream::OwnedBytes>,
    pub(super) configuration: [Option<crate::layers::PinnedLayer>; 4],
    pub(super) active_directory: String,
    pub(super) goal_continuations: Option<crate::goal::GoalContinuationPolicy>,
    pub(super) project: LocalProject,
    pub(super) state_directory: Option<PathBuf>,
    /// Refuses ambient state and settings discovery; set only by `hermetic()`.
    pub(super) hermetic: bool,
    /// Explicit composition that never discovers user settings or their tools.
    pub(super) without_user_settings: bool,
    pub(super) signing_store: Option<Arc<dyn SigningStore>>,
    /// Test-owned state root, retained by every store opened in it.
    pub(super) test_state: Option<acyclic_native_runtime::OwnershipAnchor>,
    pub(super) model: Option<(Model, Arc<dyn ModelProvider>)>,
    /// Original fold input, retained when home settings select another model.
    pub(super) model_base: Option<String>,
    /// Resolves the model selection, approver models and search at open.
    #[cfg(any(test, not(target_arch = "wasm32")))]
    pub(super) models: Option<(Arc<ModelResolver>, Option<String>)>,
    pub(super) tools: Vec<Tool>,
    /// Standard tools enabled, by name.
    pub(super) default_tools: Vec<String>,
    pub(super) question: bool,
    pub(super) feedback: Option<bool>,
    /// Verifies the caller of every agent, including embedded sessions.
    pub(super) admission: Arc<dyn super::Verifier>,
    pub(super) approval_policy: crate::approvals::ApprovalPolicy,
    pub(super) approval_issuer: Option<AuthorityRoot>,
    pub(super) approval_models: std::collections::BTreeMap<String, (Model, Arc<dyn ModelProvider>)>,
    #[cfg(not(target_arch = "wasm32"))]
    pub(super) search_provider: Option<Arc<dyn crate::search::SearchProvider>>,
    #[cfg(not(target_arch = "wasm32"))]
    pub(super) web_tools: bool,
    pub(super) limits: Limits,
    pub(super) run_limits: TaskRunLimits,
    pub(super) compaction: Option<crate::context::CompactionPolicy>,
    #[cfg(any(test, not(target_arch = "wasm32")))]
    pub(crate) settings: Option<crate::native_settings::SettingsConfig>,
    #[cfg(not(target_arch = "wasm32"))]
    pub(super) mcp_limits: crate::mcp::McpClientLimits,
    pub(super) clock: Arc<dyn acyclic_actors::local::Clock>,
    #[cfg(any(test, not(target_arch = "wasm32")))]
    pub(crate) environment_platform: crate::native_environment::PlatformPolicy,
    #[cfg(any(test, not(target_arch = "wasm32")))]
    pub(super) workspace_mounting: WorkspaceMounting,
    #[cfg(any(test, not(target_arch = "wasm32")))]
    pub(super) native_mount_probe: acyclic_fs::NativeMountProbe,
}

#[cfg(any(test, not(target_arch = "wasm32")))]
impl LocalProject {
    pub(super) fn root(&self) -> &Path {
        match self {
            Self::Workspace(path) => path,
            Self::Volume { directory, .. } => directory,
        }
    }
}

impl LocalHarnessConfig {
    /// Binds original read-only user/org, scope, template and parent sources,
    /// in that order. The sources must belong to this composition's provider;
    /// their generation pins survive workspace and child bindings.
    #[must_use]
    pub fn configuration_layers(mut self, layers: [Option<crate::layers::PinnedLayer>; 4]) -> Self {
        self.configuration = layers;
        self
    }

    /// Initializes a non-secret home file before settings and roles are read.
    /// Files are published atomically through one stable initialization receipt.
    /// Reopening preserves subsequent edits and deletions, regardless of new input bytes.
    #[must_use]
    pub fn home_file(mut self, path: impl Into<String>, bytes: acyclic_stream::OwnedBytes) -> Self {
        self.home_files.insert(path.into(), bytes);
        self
    }

    /// Forces a run's delegation policy before its first model request.
    pub fn topology(mut self, topology: crate::topology::Topology) -> Result<Self> {
        topology.validate()?;
        self.topology = Some(topology);
        Ok(self)
    }
    /// Publishes public host metadata under `sys/models/` or `sys/secrets/`.
    /// The caller must project metadata before calling this method: credentials,
    /// secret values and private headers do not belong in this read-only layer.
    /// Files are shared by the run's agents and pinned when the composition opens.
    #[must_use]
    pub fn system_file(
        mut self,
        path: impl Into<String>,
        bytes: acyclic_stream::OwnedBytes,
    ) -> Self {
        self.system_files.insert(path.into(), bytes);
        self
    }
    /// Selects a checkout path and its stock model binding.
    #[must_use]
    pub fn new(workspace: impl Into<PathBuf>, model: (Model, Arc<dyn ModelProvider>)) -> Self {
        Self::configuration(workspace.into(), Some(model))
    }

    /// Selects a checkout whose model resolves at open through `models`: `selection`, else the
    /// native settings' configured model, else the resolver's default. Approver
    /// models resolve through `models` too; the settings' search provider backs the host's
    /// web tools.
    #[cfg(any(test, not(target_arch = "wasm32")))]
    #[must_use]
    pub fn resolving(
        workspace: impl Into<PathBuf>,
        models: Arc<ModelResolver>,
        selection: Option<String>,
    ) -> Self {
        let mut config = Self::configuration(workspace.into(), None);
        config.models = Some((models, selection));
        config
    }

    /// The one resolution of a [`Self::resolving`] composition under its native `settings`;
    /// `injected` settings keep search off the environment's native sources.
    #[cfg(any(test, not(target_arch = "wasm32")))]
    pub(super) async fn resolve(
        mut self,
        settings: Option<&crate::native_settings::SettingsConfig>,
        injected: bool,
    ) -> Result<Self> {
        let model_default = self
            .models
            .as_ref()
            .is_some_and(|(_, selection)| selection.is_none());
        let preferences = settings
            .map(|settings| {
                settings.configured_model_preferences(model_default, self.compaction.is_none())
            })
            .transpose()?
            .unwrap_or_default();
        if self.compaction.is_none() {
            self.compaction = preferences.compaction;
        }
        let Some((models, selection)) = self.models.take() else {
            if let Some((model, _)) = &self.model {
                let binding = format!("{}/{}", model.provider, model.name);
                crate::templates::root_model(&binding, None).map_err(|error| {
                    Error::Invalid(format!("{error}; use LocalHarnessConfig::resolving"))
                })?;
            }
            return Ok(self);
        };
        let selection = match selection {
            Some(selection) => selection,
            None => preferences
                .model
                .unwrap_or_else(|| models.default_model().to_owned()),
        };
        let role = crate::templates::root()?.model;
        let selection = crate::templates::resolve_model(role.as_deref(), Some(&selection))
            .ok_or_else(|| Error::Invalid("root model absent".into()))?;
        self.model_base = Some(selection.to_owned());
        for model in self.approval_policy.models() {
            self.approval_models
                .insert(model.into(), models.resolve(model)?);
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            if let Some(settings) = settings {
                let search =
                    crate::search::SearchConfig::from_file(&settings.search_config_path())?;
                let search = if injected {
                    search.without_native_sources()
                } else {
                    search
                };
                // Resolving credentials does not enable a tool.
                if let Some(provider) = search.provider()? {
                    self.search_provider = Some(provider);
                }
            }
        }
        #[cfg(target_arch = "wasm32")]
        let _ = injected;
        Ok(self)
    }

    pub(super) fn has_model(&self) -> bool {
        self.model.is_some() || self.model_base.is_some()
    }

    /// Opens workspace coordination without a model, tools, settings or agent execution.
    /// The trusted harness owner must start this outside the native host sandbox.
    #[cfg(any(test, not(target_arch = "wasm32")))]
    #[must_use]
    pub fn native_host(workspace: impl Into<PathBuf>) -> Self {
        let mut config = Self::configuration(workspace.into(), None);
        config.default_tools.clear();
        config.hermetic = true;
        config.goal_continuations = None;
        config.compaction = Some(crate::context::CompactionPolicy::Disabled);
        config
    }

    fn configuration(workspace: PathBuf, model: Option<(Model, Arc<dyn ModelProvider>)>) -> Self {
        Self {
            topology: None,
            home_files: Default::default(),
            system_files: Default::default(),
            configuration: Default::default(),
            active_directory: String::new(),
            goal_continuations: Some(Default::default()),
            project: LocalProject::Workspace(workspace),
            state_directory: None,
            hermetic: false,
            without_user_settings: false,
            signing_store: None,
            test_state: None,
            model,
            model_base: None,
            #[cfg(any(test, not(target_arch = "wasm32")))]
            models: None,
            tools: Vec::new(),
            default_tools: crate::tools::default_names(),
            question: false,
            feedback: None,
            admission: Arc::new(super::KeyFileVerifier),
            approval_policy: Default::default(),
            approval_issuer: None,
            approval_models: Default::default(),
            #[cfg(not(target_arch = "wasm32"))]
            search_provider: None,
            #[cfg(not(target_arch = "wasm32"))]
            web_tools: false,
            limits: Limits::default(),
            run_limits: TaskRunLimits::default(),
            compaction: None,
            #[cfg(any(test, not(target_arch = "wasm32")))]
            settings: None,
            #[cfg(not(target_arch = "wasm32"))]
            mcp_limits: crate::mcp::McpClientLimits::default(),
            clock: Arc::new(acyclic_actors::local::SystemClock),
            #[cfg(any(test, not(target_arch = "wasm32")))]
            environment_platform: crate::native_environment::PlatformPolicy::Native,
            #[cfg(any(test, not(target_arch = "wasm32")))]
            workspace_mounting: WorkspaceMounting::Native,
            #[cfg(any(test, not(target_arch = "wasm32")))]
            native_mount_probe: Default::default(),
        }
    }
    /// Selects how task views reach the host filesystem; `Native` by default.
    #[cfg(any(test, not(target_arch = "wasm32")))]
    #[must_use]
    pub fn workspace_mounting(mut self, mounting: WorkspaceMounting) -> Self {
        self.workspace_mounting = mounting;
        self
    }
    /// Selects the host filesystem root whose native mount prerequisites are
    /// admitted. Root coordination still uses the existing physical checkout;
    /// only projected views and native shell tools require these capabilities.
    #[cfg(any(test, not(target_arch = "wasm32")))]
    #[must_use]
    pub fn native_mount_probe(mut self, probe: acyclic_fs::NativeMountProbe) -> Self {
        self.native_mount_probe = probe;
        self
    }

    /// Selects instruction scope relative to the workspace boundary.
    /// Empty selects root instructions; traversal outside the workspace is rejected.
    #[must_use]
    pub fn active_directory(mut self, directory: impl Into<String>) -> Self {
        self.active_directory = directory.into();
        self
    }
    /// Selects the instruction scope containing an explicitly supplied host working directory.
    /// A working directory outside the bound project selects root instructions only.
    #[cfg(any(test, not(target_arch = "wasm32")))]
    pub fn working_directory(self, directory: &Path) -> Result<Self> {
        let root = std::fs::canonicalize(self.project.root()).map_err(storage)?;
        let directory = std::fs::canonicalize(directory).map_err(storage)?;
        let relative = directory.strip_prefix(root).ok();
        let active = relative
            .map(|path| {
                path.components()
                    .map(|component| {
                        component
                            .as_os_str()
                            .to_str()
                            .map(str::to_owned)
                            .ok_or_else(|| Error::Invalid("instruction scope is not UTF-8".into()))
                    })
                    .collect::<Result<Vec<_>>>()
                    .map(|parts| parts.join("/"))
            })
            .transpose()?
            .unwrap_or_default();
        Ok(self.active_directory(active))
    }
    /// Opens an existing volume in the local filesystem directory passed to `new`.
    #[must_use]
    pub fn volume(mut self, volume: VolumeRef) -> Self {
        let directory = match self.project {
            LocalProject::Workspace(path) => path,
            LocalProject::Volume { directory, .. } => directory,
        };
        self.project = LocalProject::Volume { directory, volume };
        self
    }
    /// Selects the caller-owned durable state directory, outside the checkout.
    #[must_use]
    pub fn state_directory(mut self, directory: impl Into<PathBuf>) -> Self {
        self.state_directory = Some(directory.into());
        self.test_state = None;
        self
    }

    /// Inject signing custody instead of consulting OS credentials or user settings.
    #[must_use]
    pub fn signing_store(mut self, store: Arc<dyn SigningStore>) -> Self {
        self.signing_store = Some(store);
        self
    }

    /// Delete an inactive store and its exact signing credential. All users must
    /// first disconnect. Deleting directories externally bypasses this operation.
    /// Credential refusal leaves the store intact; retry after restoring custody.
    pub fn delete_state(self) -> Result<()> {
        let path = self.resolved_state_directory()?;
        let directory = match std::fs::canonicalize(path) {
            Ok(directory) => directory,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(storage(error)),
        };
        let workspace = match &self.project {
            LocalProject::Workspace(path) => Some(path.as_path()),
            LocalProject::Volume { .. } => None,
        };
        owner::planned_separated_directory(&directory, workspace)?;
        let private = directory.join("signing-custody");
        let custody = match self.signing_store.as_deref() {
            Some(store) => acyclic_native_runtime::signing_key::SigningCustody::Store(store),
            None if self.without_user_settings => {
                acyclic_native_runtime::signing_key::SigningCustody::PrivateFile(&private)
            }
            None => acyclic_native_runtime::signing_key::SigningCustody::Configured,
        };
        owner::Owner::remove(&directory, custody)
    }
    /// Refuses ambient state/settings discovery and allocates state lazily unless explicitly rooted.
    #[must_use]
    pub fn hermetic(mut self) -> Self {
        self.hermetic = true;
        self
    }
    /// Uses explicit model/provider bindings and shipped tools without reading
    /// user model, MCP, search or hook configuration. Signing custody uses the
    /// original private-file protocol in the selected state directory; reopening
    /// state requires its original key in that same store.
    #[must_use]
    pub fn without_user_settings(mut self) -> Self {
        self.without_user_settings = true;
        self
    }
    /// Embedded bindings without a checkout never allocate an unused state root.
    pub(super) fn prepare_state(mut self) -> Result<Self> {
        if self.hermetic && self.state_directory.is_none() {
            // The default-selector tests inject a private fixture root. They
            // still cannot reach the process user's ambient state directory.
            #[cfg(test)]
            if owner::STATE_ROOT.with_borrow(Option::is_some) {
                return Ok(self);
            }
            #[cfg(test)]
            TEMPORARY_STATES.with(|count| count.set(count.get() + 1));
            let state = tempfile::tempdir().map_err(storage)?;
            self.state_directory = Some(state.path().to_path_buf());
            if self.signing_store.is_none() {
                self.signing_store = Some(Arc::new(MemorySigningStore::new().map_err(storage)?));
            }
            self.test_state = Some(acyclic_native_runtime::OwnershipAnchor::new(state));
        }
        Ok(self)
    }
    /// The one resolution of this composition's durable state directory: the injected directory, else the
    /// project's default under the per-user root.
    pub(super) fn resolved_state_directory(&self) -> Result<PathBuf> {
        let location = self.state_location()?;
        if let StateLocation::Directory(directory) = location {
            return Ok(directory);
        }
        if let (StateLocation::ProjectRoot(root), LocalProject::Workspace(path)) =
            (&location, &self.project)
        {
            owner::separated_directory(root, Some(path))?;
        }
        let checkout = match &self.project {
            LocalProject::Workspace(path) => Some(owner::Checkout::capture(path)?),
            LocalProject::Volume { .. } => None,
        };
        location.resolve(&self.project, checkout.as_ref())
    }
    pub(super) fn state_location(&self) -> Result<StateLocation> {
        match &self.state_directory {
            Some(directory) => crate::native_environment::state(Some(directory))
                .map(StateLocation::Directory)
                .ok_or_else(|| {
                    Error::Unsupported("injected state directory is unavailable".into())
                }),
            #[cfg(test)]
            None if owner::STATE_ROOT.with_borrow(Option::is_some) => {
                Ok(StateLocation::ProjectRoot(owner::user_state_root()?))
            }
            None if self.hermetic => Err(Error::Unsupported(
                "hermetic compositions must prepare or inject state".into(),
            )),
            None => owner::user_state_root().map(StateLocation::ProjectRoot),
        }
    }
    /// Binds ordered approval policy data to every enabled tool, including defaults.
    #[must_use]
    pub fn approval_policy(mut self, policy: crate::approvals::ApprovalPolicy) -> Self {
        self.approval_policy = policy;
        self
    }
    /// Supplies the existing runtime's authority for embedded automatic and human
    /// approvers. Its scopes are verified by that runtime's interaction owner.
    #[must_use]
    pub fn approval_authority(mut self, issuer: AuthorityRoot) -> Self {
        self.approval_issuer = Some(issuer);
        self
    }
    /// Binds an independent approver model; it receives its own prompt and no tools.
    #[must_use]
    pub fn approval_model(
        mut self,
        selection: impl Into<String>,
        model: (Model, Arc<dyn ModelProvider>),
    ) -> Self {
        self.approval_models.insert(selection.into(), model);
        self
    }
    /// Supplies the platform's caller verification. Embedded sessions use the
    /// same admission with the default key-file verifier.
    #[must_use]
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn admitted(mut self, verifier: Arc<dyn super::Verifier>) -> Self {
        self.admission = verifier;
        self
    }
    /// Applies the shared approval policy. Tool availability remains with the
    /// host's registry.
    #[must_use]
    pub fn preset(mut self, preset: &crate::preset::Preset) -> Self {
        self.approval_policy = preset.approval_policy.clone();
        self
    }
    pub(super) fn preset_value(&self) -> crate::preset::Preset {
        crate::preset::Preset {
            approval_policy: self.approval_policy.clone(),
        }
    }
    /// Fix the question plugin selection before this session starts. Later UI
    /// attachments do not change its tool catalog. Web and IDE hosts opt in here.
    #[must_use]
    pub fn question_plugin(mut self, enabled: bool) -> Self {
        self.question = enabled;
        self.default_tools.retain(|name| name != "ask_question");
        if enabled {
            self.default_tools.push("ask_question".into());
        }
        self
    }

    /// Selects feedback settlement notices before opening the session.
    /// Finite model scripts can disable notices without removing other hooks.
    #[must_use]
    pub fn feedback_plugin(mut self, enabled: bool) -> Self {
        self.feedback = Some(enabled);
        self
    }

    /// Disables the default set, retaining every custom tool.
    #[must_use]
    pub fn without_default_tools(mut self) -> Self {
        self.default_tools.clear();
        self
    }
    /// Adds one custom tool to the enabled default set.
    #[must_use]
    pub fn tool(mut self, tool: Tool) -> Self {
        self.tools.push(tool);
        self
    }
    /// Adds custom tools to the enabled default set.
    #[must_use]
    pub fn tools(mut self, tools: impl IntoIterator<Item = Tool>) -> Self {
        self.tools.extend(tools);
        self
    }
    /// Enables credential-free HTTP retrieval. Search requires a configured provider.
    #[cfg(not(target_arch = "wasm32"))]
    #[must_use]
    pub fn web_tools(mut self) -> Self {
        self.web_tools = true;
        self
    }

    /// Binds a host-owned search provider without serializing its credentials.
    #[cfg(not(target_arch = "wasm32"))]
    #[must_use]
    pub fn search_provider(mut self, provider: Arc<dyn crate::search::SearchProvider>) -> Self {
        self.search_provider = Some(provider);
        self.web_tools = true;
        self
    }

    /// Prebinds web tools before opening an embedded runtime and issuing its grants.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn bind_web_tools(&self, registry: &mut ToolRegistry) -> Result<()> {
        if self.web_tools {
            super::web::WebExecutor::bind(registry, self.search_provider.clone())?;
        }
        Ok(())
    }

    /// Selects content, projection and model-loop bounds.
    #[must_use]
    pub fn limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }
    /// Selects durable execution bounds.
    #[must_use]
    pub fn run_limits(mut self, limits: TaskRunLimits) -> Self {
        self.run_limits = limits;
        self
    }
    /// Selects the finite goal policy; `None` disables automatic continuations.
    #[must_use]
    pub fn goal_continuations(
        mut self,
        policy: Option<crate::goal::GoalContinuationPolicy>,
    ) -> Self {
        self.goal_continuations = policy;
        self
    }
    /// Selects an explicit compaction policy, validated without changing its budgets.
    #[must_use]
    pub fn compaction(mut self, policy: crate::context::CompactionPolicy) -> Self {
        self.compaction = Some(policy);
        self
    }
    /// Selects native settings locations and explicit project trust using the shared loader.
    #[cfg(any(test, not(target_arch = "wasm32")))]
    #[must_use]
    pub fn settings(mut self, settings: crate::native_settings::SettingsConfig) -> Self {
        self.settings = Some(settings);
        self
    }
    /// Selects protective MCP discovery-frame and tool-exchange bounds.
    #[cfg(not(target_arch = "wasm32"))]
    #[must_use]
    pub fn mcp_limits(mut self, limits: crate::mcp::McpClientLimits) -> Self {
        self.mcp_limits = limits;
        self
    }
    /// Selects the one clock used by actor alarms, durable admission and leases.
    #[must_use]
    pub fn clock(mut self, clock: Arc<dyn acyclic_actors::local::Clock>) -> Self {
        self.clock = clock;
        self
    }
}

#[cfg(test)]
std::thread_local! {
    pub(super) static TEMPORARY_STATES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(any(test, not(target_arch = "wasm32")))]
pub use crate::model::ModelResolver;

#[cfg(all(test, not(loom)))]
mod resolver_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn readiness_requires_selected_capacity_unless_compaction_is_disabled() -> Result<()> {
        let workspace = tempfile::tempdir().map_err(storage)?;
        let state_parent = tempfile::tempdir().map_err(storage)?;
        let state = state_parent.path().join("uncreated-state");
        let role = crate::templates::root()?.model;
        let selection =
            crate::templates::resolve_model(role.as_deref(), None).unwrap_or("fixture/base");
        let (provider, name) = selection
            .split_once('/')
            .ok_or_else(|| Error::Invalid("model requires provider/model".into()))?;
        let model = Model::new(provider.into(), name.into(), "1".into())?;
        let unsupported: Arc<dyn ModelProvider> = Arc::new(
            crate::test_support::ScriptedModelProvider::new(Vec::new()).without_accounting(),
        );
        for resolving in [false, true] {
            for disabled in [false, true] {
                let mut config = if resolving {
                    let unsupported = unsupported.clone();
                    let available = crate::local::tests::text();
                    let models = Arc::new(ModelResolver::new(selection, move |selection| {
                        let (provider, name) = selection.split_once('/').ok_or_else(|| {
                            Error::Invalid("model requires provider/model".into())
                        })?;
                        Ok((
                            Model::new(provider.into(), name.into(), "1".into())?,
                            if selection == "fixture/no-capacity" {
                                unsupported.clone()
                            } else {
                                available.clone()
                            },
                        ))
                    }));
                    LocalHarnessConfig::resolving(workspace.path(), models, None).home_file(
                        "agent.ts",
                        acyclic_stream::OwnedBytes::from_static(
                            b"export default base => ({...base, model: 'fixture/no-capacity'});",
                        ),
                    )
                } else {
                    LocalHarnessConfig::new(workspace.path(), (model.clone(), unsupported.clone()))
                }
                .state_directory(&state)
                .without_user_settings()
                .without_default_tools()
                .workspace_mounting(WorkspaceMounting::Physical);
                if disabled {
                    config = config.compaction(crate::context::CompactionPolicy::Disabled);
                }
                let refusals = LocalHarness::doctor(config).await;
                if disabled {
                    assert!(refusals.is_empty(), "{refusals:?}");
                } else {
                    assert!(
                        refusals
                            .iter()
                            .any(|error| error.to_string().contains("context capacity")),
                        "{refusals:?}"
                    );
                }
                assert!(!state.exists(), "readiness created durable state");
            }
        }
        Ok(())
    }
    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn readiness_checks_the_composed_model_without_creating_state() -> Result<()> {
        let workspace = tempfile::tempdir().map_err(storage)?;
        let state_parent = tempfile::tempdir().map_err(storage)?;
        let state = state_parent.path().join("uncreated-state");
        let provider = crate::local::tests::text();
        let models = Arc::new(ModelResolver::new("unknown/default", move |selection| {
            if selection != "available/ready" {
                return Err(Error::Unauthorized(format!(
                    "model unavailable: {selection}"
                )));
            }
            Ok((
                Model::new("available".into(), "ready".into(), "1".into())?,
                provider.clone(),
            ))
        }));
        let role = crate::templates::root()?.model;
        for (base, home, override_model) in [
            ("unknown/model", None, None),
            ("missing/credentials", None, None),
            (
                "missing/credentials",
                Some(b"export default base => ({...base, model: 'available/ready'});".as_slice()),
                Some("available/ready"),
            ),
            (
                "available/ready",
                Some(b"export default base => ({...base, model: 'missing/selected'});".as_slice()),
                Some("missing/selected"),
            ),
        ] {
            let selection = override_model.unwrap_or_else(|| {
                crate::templates::resolve_model(role.as_deref(), Some(base)).unwrap_or(base)
            });
            let expected = (selection != "available/ready").then_some(selection);
            let mut config =
                LocalHarnessConfig::resolving(workspace.path(), models.clone(), Some(base.into()))
                    .state_directory(&state)
                    .without_user_settings()
                    .without_default_tools()
                    .workspace_mounting(WorkspaceMounting::Physical);
            if let Some(home) = home {
                config =
                    config.home_file("agent.ts", acyclic_stream::OwnedBytes::from_static(home));
            }
            let refusals = LocalHarness::doctor(config).await;
            if let Some(selection) = expected {
                assert!(
                    refusals
                        .iter()
                        .any(|error| error.to_string().contains(selection)),
                    "{refusals:?}"
                );
            } else {
                assert!(refusals.is_empty(), "{refusals:?}");
            }
            assert!(!state.exists(), "readiness created durable state");
        }
        Ok(())
    }
    #[tokio::test]
    async fn saved_home_selects_credentials_and_ignores_replacement_initializers() -> Result<()> {
        let workspace = tempfile::tempdir().map_err(storage)?;
        let state = tempfile::tempdir().map_err(storage)?;
        let resolved = Arc::new(AtomicUsize::new(0));
        let calls = resolved.clone();
        let provider = crate::local::tests::text();
        let models = Arc::new(ModelResolver::new("unavailable/base", move |selection| {
            calls.fetch_add(1, Ordering::Relaxed);
            let name = selection.strip_prefix("available/").ok_or_else(|| {
                Error::Unauthorized("unselected provider has no credentials".into())
            })?;
            Ok((
                Model::new("available".into(), name.into(), "1".into())?,
                provider.clone(),
            ))
        }));
        let config = |source| {
            LocalHarnessConfig::resolving(workspace.path(), models.clone(), None)
                .state_directory(state.path())
                .home_file("agent.ts", acyclic_stream::OwnedBytes::from_static(source))
                .without_user_settings()
                .without_default_tools()
                .workspace_mounting(WorkspaceMounting::Physical)
                .compaction(crate::context::CompactionPolicy::Disabled)
        };
        let prepared = LocalHarness::prepare(config(
            b"export default base => ({...base, model: 'available/initial'});",
        ))
        .await?;
        assert_eq!(prepared.model_selection(None).await?, "available/initial");
        assert_eq!(
            resolved.load(Ordering::Relaxed),
            0,
            "preparation bound provider credentials"
        );
        let harness = prepared.open().await?;
        crate::local::tests::settings::write(
            &harness,
            "agent.ts",
            "import { model } from './choice.ts'; export default base => ({...base, model});",
        )
        .await?;
        crate::local::tests::settings::write(
            &harness,
            "choice.ts",
            "export const model = 'available/saved';",
        )
        .await?;
        drop(harness);
        for ignored in [
            b"invalid syntax".as_slice(),
            b"import x from './missing.ts'; export default x;".as_slice(),
        ] {
            let prepared = LocalHarness::prepare(config(ignored)).await?;
            assert_eq!(prepared.model_selection(None).await?, "available/saved");
            let harness = prepared.open().await?;
            assert_eq!(harness.agent()?.0.name, "saved");
        }
        assert_eq!(resolved.load(Ordering::Relaxed), 2);
        Ok(())
    }

    #[tokio::test]
    async fn home_model_resolves_before_provider_credentials_and_folds_only_once() -> Result<()> {
        let workspace = tempfile::tempdir().map_err(storage)?;
        let state = tempfile::tempdir().map_err(storage)?;
        let provider = crate::local::tests::text();
        let models = Arc::new(ModelResolver::new("unavailable/base", move |selection| {
            if selection != "available/ready" {
                return Err(Error::Unauthorized(
                    "unselected provider has no credentials".into(),
                ));
            }
            Ok((
                Model::new("available".into(), "ready".into(), "1".into())?,
                provider.clone(),
            ))
        }));
        let config = LocalHarnessConfig::resolving(workspace.path(), models, None)
            .state_directory(state.path())
            .home_file("agent.ts", acyclic_stream::OwnedBytes::from_static(
                b"export default base => ({...base, model: base.model === 'available/ready' ? 'incorrect/double-fold' : 'available/ready'});"
            ))
            .without_user_settings()
            .without_default_tools()
            .workspace_mounting(WorkspaceMounting::Physical)
            .compaction(crate::context::CompactionPolicy::Disabled);
        let harness = LocalHarness::open(config).await?;
        assert_eq!(harness.agent()?.0.name, "ready");
        assert_eq!(harness.agent()?.0.provider, "available");
        Ok(())
    }

    // 1k compositions over one resolver construct each selection's provider
    // once; a changed selection, here a changed settings model, resolves anew.
    #[tokio::test]
    async fn shared_model_resolution_honors_settings_and_explicit_selection() -> Result<()> {
        let constructed = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&constructed);
        let models = Arc::new(ModelResolver::new("test/first", move |selection| {
            counter.fetch_add(1, Ordering::Relaxed);
            let provider: Arc<dyn ModelProvider> =
                Arc::new(crate::test_support::ScriptedModelProvider::new(Vec::new()));
            let (provider_name, name) = selection
                .split_once('/')
                .ok_or_else(|| Error::Invalid("fixture model selection is unqualified".into()))?;
            Ok((
                Model::new(provider_name.into(), name.into(), "1".into())?,
                provider,
            ))
        }));
        let workspace = tempfile::tempdir().map_err(storage)?;
        let codex = tempfile::tempdir().map_err(storage)?;
        let settings = || {
            crate::native_settings::SettingsConfig::new(workspace.path(), workspace.path())
                .codex_home(codex.path())
        };
        let selected = |config: LocalHarnessConfig| {
            let selection = config
                .model_base
                .ok_or_else(|| Error::Invalid("model unresolved".into()))?;
            models.resolve(&selection).map(|(model, _)| model.name)
        };
        for _ in 0..1000 {
            let config = LocalHarnessConfig::resolving(workspace.path(), Arc::clone(&models), None);
            assert_eq!(
                selected(config.resolve(Some(&settings()), true).await?)?,
                "first"
            );
        }
        assert_eq!(constructed.load(Ordering::Relaxed), 1);
        std::fs::write(
            codex.path().join("config.toml"),
            "model = \"test/second\"\n",
        )
        .map_err(storage)?;
        let config = LocalHarnessConfig::resolving(workspace.path(), Arc::clone(&models), None);
        assert_eq!(
            selected(config.resolve(Some(&settings()), true).await?)?,
            "second"
        );
        assert_eq!(constructed.load(Ordering::Relaxed), 2);
        let explicit = LocalHarnessConfig::resolving(
            workspace.path(),
            Arc::clone(&models),
            Some("test/first".into()),
        );
        assert_eq!(
            selected(explicit.resolve(Some(&settings()), true).await?)?,
            "first"
        );
        let defaults = LocalHarnessConfig::resolving(workspace.path(), Arc::clone(&models), None)
            .without_user_settings();
        assert_eq!(selected(defaults.resolve(None, true).await?)?, "first");
        assert_eq!(constructed.load(Ordering::Relaxed), 2);
        Ok(())
    }
}

#[cfg(all(test, not(loom)))]
mod fixed_role_binding_tests {
    use super::*;

    #[tokio::test]
    async fn fixed_bindings_obey_the_configured_role_selection() -> Result<()> {
        let settings_root = tempfile::tempdir().map_err(storage)?;
        std::fs::write(
            settings_root.path().join("config.toml"),
            "model = ''\n[compaction]\nkind = 'disabled'\n",
        )
        .map_err(storage)?;
        let settings =
            crate::native_settings::SettingsConfig::new(settings_root.path(), settings_root.path())
                .codex_home(settings_root.path());
        let role = crate::templates::root()?.model;
        let selected =
            crate::templates::resolve_model(role.as_deref(), None).unwrap_or("test/fixed");
        for selection in [selected.to_owned(), format!("{selected}-different")] {
            let (provider, name) = selection
                .split_once('/')
                .ok_or_else(|| Error::Invalid("role model requires provider/model".into()))?;
            let model = Model::new(provider.into(), name.into(), "1".into())?;
            let provider: Arc<dyn ModelProvider> =
                Arc::new(crate::test_support::ScriptedModelProvider::new(Vec::new()));
            let allowed = crate::templates::resolve_model(role.as_deref(), Some(&selection))
                == Some(selection.as_str());
            let result = LocalHarnessConfig::new(".", (model.clone(), provider))
                .resolve(Some(&settings), true)
                .await;
            if allowed {
                assert_eq!(result?.model.map(|binding| binding.0), Some(model));
            } else {
                let error = result
                    .err()
                    .ok_or_else(|| Error::Invalid("incompatible binding admitted".into()))?;
                assert!(error.to_string().contains("LocalHarnessConfig::resolving"));
            }
        }
        Ok(())
    }
}
