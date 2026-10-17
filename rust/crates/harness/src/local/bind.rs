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
        config: LocalHarnessConfig,
        runtime: LocalTaskRuntime,
        files: Arc<LocalFilesystem>,
        storage: (acyclic_fs::LocalFs, Arc<LocalStream>),
        authority: (&AuthorityVerifier, &Scope),
        checkout: Option<&Path>,
    ) -> Result<Self> {
        config.limits.validate(&mut None)?;
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
        let hooks = settings::load(&config.project, config.settings)
            .await?
            .hooks;
        let (model, provider) = config.model;
        model.validate()?;
        let compaction = compaction::select(
            config.compaction,
            &model,
            provider.as_ref(),
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
        #[cfg(feature = "native-execution")]
        let workspace = if let Some(checkout) = checkout {
            let directory = owner::directory(&owner::state_directory(&config.project)?)?;
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
                    acyclic_fs::LocalCoreStateStore::new(directory.join("workspace")),
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
        let mut local = Self {
            files,
            runtime: Arc::new(runtime),
            task_files,
            project: volume.clone(),
            scratch: volume.clone(),
            journal: volume.clone(),
            verifier: verifier.clone(),
            signed: signed.clone(),
            model,
            provider,
            context_manifest: config.context_manifest,
            #[cfg(feature = "hooks")]
            hooks,
            #[cfg(test)]
            context_read_observer: None,
            limits: config.limits,
            compaction,
            #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
            mcp_diagnostics: Vec::new(),
            goal_continuations: config.goal_continuations,
            admission: crate::sync::owned::Arc::new(crate::sync::owned::Mutex::new(())),
            drivers: super::orphans::Drivers::new(),
            #[cfg(feature = "native-execution")]
            workspace,
            #[cfg(feature = "native-execution")]
            checkout_context: checkout.is_some(),
        };
        if let Some(provider) = local
            .provider
            .bind_content(local.context_reader()?, local.limits.file_bytes)
        {
            local.provider = provider;
        }
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
