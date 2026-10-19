use super::*;
use crate::conversation::ContentGrant;

impl LocalHarness {
    /// Registers request-file tool execution in an existing composition.
    pub fn register_commands(
        tasks: &mut TaskRegistry,
        machines: &mut MachineRegistry,
    ) -> Result<()> {
        let direct = Arc::new(command_machine::DirectTool::new());
        tasks.register(direct.definition()?)?;
        machines.register(direct, &mut None)
    }

    /// Binds command files to an existing runtime and its authenticated volume.
    pub async fn bind(
        mut config: LocalHarnessConfig,
        runtime: LocalTaskRuntime,
        files: Arc<LocalFilesystem>,
        storage: (acyclic_fs::LocalFs, Arc<LocalStream>),
        authority: (&AuthorityVerifier, &Scope),
        checkout: Option<&Path>,
    ) -> Result<Self> {
        config.limits.validate(&mut None)?;
        #[cfg(feature = "native-execution")]
        if checkout.is_some() {
            config = config.prepare_state()?;
        }
        #[cfg(feature = "native-execution")]
        let state_owner = config.test_state.clone();
        #[cfg(feature = "native-execution")]
        let checkout_state = checkout
            .map(|checkout| {
                owner::separated_directory(&config.resolved_state_directory()?, Some(checkout))
            })
            .transpose()?;
        let (verifier, signed) = authority;
        let LocalProject::Volume { volume, .. } = &config.project else {
            return Err(Error::Invalid(
                "existing runtime requires its bound volume".into(),
            ));
        };
        for operation in [VolumeOperation::Read, VolumeOperation::Write] {
            ContentGrant::verify(
                verifier,
                signed,
                volume,
                operation,
                config.limits.file_bytes,
                &mut None,
            )?;
        }
        #[cfg(feature = "hooks")]
        let settings =
            settings::configuration(&config.project, config.settings.take(), config.hermetic)?;
        config.model.0.validate()?;
        let compaction = compaction::select(
            config.compaction.take(),
            &config.model.0,
            config.model.1.as_ref(),
            runtime.tool_registry(),
            config.limits,
        )
        .await?;
        let (filesystem, stream) = storage;
        let task_files = Arc::new(
            crate::task_volume::TaskFiles::open(
                stream,
                volume,
                verifier,
                signed,
                config.limits.file_bytes,
            )
            .await?,
        );
        let erased_files = Arc::new(task_files.reopen_erased().await?);
        let mut runtime = runtime.with_task_files(erased_files.clone())?;
        runtime.bind_task_files(Some(task_files.clone()));
        #[cfg(feature = "native-execution")]
        let workspace = if let Some((checkout, directory)) = checkout.zip(checkout_state) {
            let identity = acyclic_fs::capture_root_identity(checkout).map_err(storage_error)?;
            let digest = blake3::derive_key(
                "acyclic.local.bound-checkout/v1",
                &serde_json::to_vec(volume).map_err(storage_error)?,
            );
            let operation = acyclic_fs::OperationId::from_bytes(
                digest[..16].try_into().map_err(storage_error)?,
            );
            Some(Arc::new(
                workspace::open(
                    &filesystem,
                    volume,
                    volume,
                    checkout,
                    &directory,
                    Some(identity.to_bytes()),
                    operation,
                    true,
                    false,
                    acyclic_fs::LocalCoreStateStore::new(directory.join("workspace"))
                        .with_ownership_anchor(state_owner),
                )
                .await?,
            ))
        } else {
            None
        };
        #[cfg(not(feature = "native-execution"))]
        {
            let _ = filesystem;
            if checkout.is_some() {
                return Err(Error::Unsupported(
                    "checkout commands require native-execution".into(),
                ));
            }
        }
        #[cfg(feature = "native-execution")]
        let runtime = if let Some(workspace) = &workspace {
            runtime.with_workspace_tools(Arc::new(
                checkout_tools::CheckoutTools::new(
                    Arc::clone(workspace),
                    volume,
                    config.clock,
                    Arc::clone(&task_files),
                )
                .await?,
            ))?
        } else {
            runtime
        };
        let (model, provider) = config.model;
        let local = Self {
            files,
            runtime: Arc::new(runtime),
            task_files,
            project: volume.clone(),
            scratch: volume.clone(),
            journal: volume.clone(),
            verifier: verifier.clone(),
            issuer: None,
            signed: signed.clone(),
            model,
            provider,
            context_manifest: config.context_manifest,
            #[cfg(feature = "hooks")]
            settings,
            #[cfg(test)]
            context_read_observer: None,
            limits: config.limits,
            compaction,
            #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
            mcp_diagnostics: Vec::new(),
            goal_continuations: config.goal_continuations,
            admission: crate::sync::owned::Arc::new(crate::sync::owned::Mutex::new(())),
            #[cfg(feature = "native-execution")]
            workspace,
            #[cfg(feature = "native-execution")]
            checkout_context: checkout.is_some(),
        };
        Ok(local)
    }

    /// The same task runtime used by request-file consumers and SDK queries.
    pub fn task_runtime(&self) -> &Arc<LocalTaskRuntime> {
        &self.runtime
    }
}

#[cfg(feature = "native-execution")]
fn storage_error(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}
