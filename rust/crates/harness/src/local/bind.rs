use super::*;
use crate::conversation::ContentGrant;

impl LocalHarness {
    /// Binds command files to an existing runtime and its authenticated volume.
    pub async fn bind(
        mut config: LocalHarnessConfig,
        runtime: LocalTaskRuntime,
        files: Arc<LocalFilesystem>,
        storage: (acyclic_fs::LocalFs, Arc<LocalStream>),
        authority: (&AuthorityVerifier, &Scope),
        checkout: Option<&Path>,
    ) -> Result<Self> {
        validate_binding(&config, &runtime)?;
        #[cfg(any(test, feature = "native-execution"))]
        if checkout.is_some() {
            config = config.prepare_state()?;
        }
        #[cfg(any(test, feature = "native-execution"))]
        let state_owner = config.test_state.clone();
        #[cfg(any(test, feature = "native-execution"))]
        let checkout_state = checkout
            .map(|checkout| {
                owner::separated_directory(&config.resolved_state_directory()?, Some(checkout))
            })
            .transpose()?;
        let (verifier, signed) = authority;
        #[cfg(feature = "search-http")]
        config.validate_web_binding(runtime.tool_registry(), verifier, signed)?;
        let LocalProject::Volume { volume, .. } = &config.project else {
            return Err(Error::Invalid(
                "existing runtime requires its bound volume".into(),
            ));
        };
        #[cfg(any(test, feature = "native-execution"))]
        let skill_directory = checkout_state.as_ref().map_or_else(
            || config.project.root().join("skill-sources"),
            |directory| directory.join("skill-sources"),
        );
        verify_bound_volume(volume, verifier, signed, config.limits.file_bytes)?;
        #[cfg(any(test, feature = "hooks"))]
        let settings =
            settings::configuration(&config.project, config.settings.take(), config.hermetic)?;
        let (model, provider) = bound_model(&config)?;
        let compaction = compaction::select(
            config.compaction.take(),
            &model,
            provider.as_ref(),
            runtime.tool_registry(),
            config.limits,
        )
        .await?;
        let (filesystem, stream) = storage;
        let (runtime, task_files) = bind_task_files(
            runtime,
            stream,
            volume,
            verifier,
            signed,
            config.limits.file_bytes,
            config.approval_issuer.take(),
        )
        .await?;
        #[cfg(any(test, feature = "native-execution"))]
        let workspace =
            bound_workspace(checkout, checkout_state, &filesystem, volume, state_owner).await?;
        #[cfg(not(any(test, feature = "native-execution")))]
        {
            let _ = filesystem;
            if checkout.is_some() {
                return Err(Error::Unsupported(
                    "checkout commands require native-execution".into(),
                ));
            }
        }
        #[cfg(any(test, feature = "native-execution"))]
        let runtime = checkout_runtime(
            runtime,
            workspace.as_ref(),
            volume,
            config.clock.clone(),
            signed,
            verifier,
        )
        .await?;
        #[cfg(any(test, feature = "native-execution"))]
        let files = open::retain_skills(
            files,
            skill_directory,
            volume,
            workspace.as_ref(),
            settings.skill_roots().to_vec(),
        )?;
        let volume = volume.clone();
        Ok(BoundComposition {
            config,
            runtime,
            files,
            task_files,
            volume,
            verifier: verifier.clone(),
            signed: signed.clone(),
            compaction,
            #[cfg(any(test, feature = "hooks"))]
            settings,
            #[cfg(any(test, feature = "native-execution"))]
            workspace,
            #[cfg(any(test, feature = "native-execution"))]
            checkout_context: checkout.is_some(),
        }
        .finish())
    }

    /// The same task runtime used by request-file consumers and SDK queries.
    pub fn task_runtime(&self) -> &Arc<LocalTaskRuntime> {
        &self.runtime
    }
}

fn bound_model(config: &LocalHarnessConfig) -> Result<(Model, Arc<dyn ModelProvider>)> {
    let binding = config.model.as_ref().ok_or_else(|| {
        Error::Invalid(
            "bind requires an agent binding; use open for native host coordination".into(),
        )
    })?;
    binding.0.validate()?;
    Ok(binding.clone())
}

#[cfg(any(test, feature = "native-execution"))]
fn storage_error(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}

#[cfg(any(test, feature = "native-execution"))]
async fn bound_workspace(
    checkout: Option<&Path>,
    directory: Option<PathBuf>,
    filesystem: &acyclic_fs::LocalFs,
    volume: &VolumeRef,
    state_owner: Option<acyclic_native_runtime::OwnershipAnchor>,
) -> Result<Option<Arc<workspace::LocalWorkspace>>> {
    Ok(
        if let Some((checkout, directory)) = checkout.zip(directory) {
            let root =
                acyclic_fs::hold_root(&std::fs::canonicalize(checkout).map_err(storage_error)?)
                    .map_err(storage_error)?;
            let digest = blake3::derive_key(
                "acyclic.local.bound-checkout/v1",
                &serde_json::to_vec(volume).map_err(storage_error)?,
            );
            let operation = acyclic_fs::OperationId::from_bytes(
                digest[..16].try_into().map_err(storage_error)?,
            );
            let store = acyclic_fs::LocalCoreStateStore::new(directory.join("workspace"))
                .with_ownership_anchor(state_owner);
            let attach = workspace::Attach {
                authority: None,
                volume,
                scratch: volume,
                directory: &directory,
                operation,
                native_mounts: false,
            };
            Some(Arc::new(
                workspace::open(
                    filesystem,
                    &attach,
                    root,
                    true,
                    workspace::OwnerState {
                        store,
                        processes: None,
                    },
                )
                .await?,
            ))
        } else {
            None
        },
    )
}

struct BoundComposition {
    config: LocalHarnessConfig,
    runtime: LocalTaskRuntime,
    files: Arc<LocalFilesystem>,
    task_files: Arc<crate::task_volume::TaskFiles<LocalStream>>,
    volume: VolumeRef,
    verifier: AuthorityVerifier,
    signed: Scope,
    compaction: crate::context::CompactionPolicy,
    #[cfg(any(test, feature = "hooks"))]
    settings: Arc<crate::native_settings::SettingsConfig>,
    #[cfg(any(test, feature = "native-execution"))]
    workspace: Option<Arc<workspace::LocalWorkspace>>,
    #[cfg(any(test, feature = "native-execution"))]
    checkout_context: bool,
}
impl BoundComposition {
    fn finish(self) -> LocalHarness {
        let Self {
            config,
            runtime,
            files,
            task_files,
            volume,
            verifier,
            signed,
            compaction,
            #[cfg(any(test, feature = "hooks"))]
            settings,
            #[cfg(any(test, feature = "native-execution"))]
            workspace,
            #[cfg(any(test, feature = "native-execution"))]
            checkout_context,
        } = self;
        LocalHarness {
            files,
            runtime: Arc::new(runtime),
            root: AgentId::from_bytes([1; 16]),
            root_authority: None,
            agent_stream: None,
            agents: tokio::sync::OnceCell::new(),
            task_files,
            project: volume.clone(),
            scratch: volume.clone(),
            journal: volume,
            verifier,
            signed,
            approval_policy: config.approval_policy,
            approval_models: config.approval_models,
            agent: config.model,
            context_manifest: config.context_manifest,
            active_directory: config.active_directory,
            skill_cache: context::skills::cache(),
            #[cfg(any(test, feature = "hooks"))]
            settings: Some(settings),
            #[cfg(test)]
            context_read_observer: None,
            limits: config.limits,
            compaction,
            #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
            mcp_diagnostics: Vec::new(),
            goal_continuations: config.goal_continuations,
            admission: crate::sync::owned::Arc::new(crate::sync::owned::Mutex::new(())),
            publication: crate::sync::owned::Arc::new(crate::sync::owned::Mutex::new(())),
            drivers: super::orphans::Drivers::new(),
            #[cfg(any(test, feature = "native-execution"))]
            workspace,
            #[cfg(any(test, feature = "native-execution"))]
            checkout_context,
            #[cfg(test)]
            environment_reads: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(any(test, feature = "native-execution"))]
            clock: config.clock,
            #[cfg(any(test, feature = "native-execution"))]
            environment_platform: config.environment_platform,
        }
    }
}

fn verify_bound_volume(
    volume: &VolumeRef,
    verifier: &AuthorityVerifier,
    signed: &Scope,
    maximum: u64,
) -> Result<()> {
    for operation in [VolumeOperation::Read, VolumeOperation::Write] {
        ContentGrant::verify(verifier, signed, volume, operation, maximum, &mut None)?;
    }
    Ok(())
}

async fn bind_task_files(
    runtime: LocalTaskRuntime,
    stream: Arc<LocalStream>,
    volume: &VolumeRef,
    verifier: &AuthorityVerifier,
    signed: &Scope,
    maximum: u64,
    approval_issuer: Option<AuthorityRoot>,
) -> Result<(
    LocalTaskRuntime,
    Arc<crate::task_volume::TaskFiles<LocalStream>>,
)> {
    let task_files = Arc::new(
        crate::task_volume::TaskFiles::open(stream, volume, verifier, signed, maximum).await?,
    );
    let erased_files = Arc::new(task_files.reopen_erased().await?);
    let mut runtime = runtime.with_task_files(erased_files.clone())?;
    runtime.bind_task_files(Some(task_files.clone()));
    let runtime = match approval_issuer {
        Some(issuer) => runtime.with_approval_authority(&issuer)?,
        None => runtime,
    };
    Ok((runtime, task_files))
}

#[cfg(any(test, feature = "native-execution"))]
async fn checkout_runtime(
    runtime: LocalTaskRuntime,
    workspace: Option<&Arc<workspace::LocalWorkspace>>,
    volume: &VolumeRef,
    clock: Arc<dyn acyclic_stream::UnixMillisClock>,
    signed: &Scope,
    verifier: &AuthorityVerifier,
) -> Result<LocalTaskRuntime> {
    Ok(if let Some(workspace) = workspace {
        runtime.with_workspace_tools(Arc::new(
            checkout_tools::CheckoutTools::new(
                Arc::clone(workspace),
                volume,
                clock,
                signed,
                verifier,
            )
            .await?,
        ))?
    } else {
        runtime
    })
}

fn validate_binding(config: &LocalHarnessConfig, runtime: &LocalTaskRuntime) -> Result<()> {
    use crate::runtime::ToolPolicy as _;
    config.limits.validate(&mut None)?;
    if runtime.approval_policy_identity() != Some(config.approval_policy.bind()?.identity()) {
        return Err(Error::Conflict(
            "bound runtime and local approval policy differ".into(),
        ));
    }
    Ok(())
}
