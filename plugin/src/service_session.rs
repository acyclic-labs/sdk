//! Session ownership behind the service's existing actor and catalog.

use super::*;
use acyclic_harness::{
    Error as RootError, Outcome, TaskId, conversation::FileRef, filesystem::FilesystemHost, runtime::DurableTaskHost,
};
use acyclic_stream::{LocalStream, LocalStreamLimits, StreamClient};

type PiRuntime = pi_runtime::PiTaskRuntime<LocalStream, LocalAuthorityBackend, LocalObjectBackend>;

#[path = "pi_spawn_session.rs"]
mod pi_spawn_session;
#[path = "pi_lifecycle_session.rs"]
mod pi_lifecycle_session;
#[path = "pi_selected_session.rs"]
mod pi_selected_session;
#[path = "pi_operator_session.rs"]
mod pi_operator_session;
#[path = "pi_builtin_session.rs"]
mod pi_builtin_session;

pub(crate) enum SessionController {
    HostManaged(Box<ControlPlane>),
    Pi(Box<PiSession>),
}

impl From<ControlPlane> for SessionController {
    fn from(control: ControlPlane) -> Self {
        Self::HostManaged(Box::new(control))
    }
}

impl SessionController {
    pub(crate) async fn open(
        directory: PathBuf,
        resources: &ServiceResources,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<Self> {
        let relative_directory = resources.private_session_data(&directory)?;
        let mut loaded = crate::adapter_state::load_state_with_native_work(
            &resources.private_state_root, relative_directory, work,
        )?;
        match &loaded.0.0.task_binding {
            PluginTaskBinding::HostManaged => {
                let host = resources.open_session_directory(directory).await.map_err(RootError::Storage)?;
                drop(loaded.0);
                drop(loaded.1);
                Ok(Self::from(host))
            }
            PluginTaskBinding::Pi(_) => {
                // Admit the existing actor box payload before Box::new. This
                // logical ownership claim is not a fabricated SDK allocator event.
                let mut custody = work.retain_owned(std::mem::size_of::<PiSession>() as u64)
                    .map_err(|cause| RootError::NativeWork { cause, custody: None })?;
                let session = PiSession::open(directory, loaded.0.0, loaded.0.1, resources, work).await?;
                // The original state was moved, not copied: transfer its
                // exclusive claim into the actor while every field is live.
                custody.absorb(&mut loaded.1)
                    .map_err(|cause| RootError::NativeWork { cause, custody: None })?;
                Self::Pi(Box::new(session)).retain_original_input(custody)
            }
        }
    }

    pub(crate) fn state(&self) -> &AdapterState {
        match self {
            Self::HostManaged(control) => &control.state,
            Self::Pi(session) => &session.state,
        }
    }

    pub(crate) fn directory(&self) -> &Path {
        match self {
            Self::HostManaged(control) => &control.data,
            Self::Pi(session) => &session.directory,
        }
    }

    pub(crate) fn unflushed(&self) -> bool {
        match self {
            Self::HostManaged(control) => control.unflushed,
            Self::Pi(_) => false,
        }
    }

    pub(crate) async fn make_durable(&mut self) -> acyclic_harness::Result<()> {
        match self {
            Self::HostManaged(control) => control.make_durable().await.map_err(RootError::Storage),
            Self::Pi(_) => Ok(()),
        }
    }

    pub(crate) async fn dispatch(&mut self, request: ControlRequest,
        original_native_work: &mut Option<acyclic_fs::NativeMaterializationWork>,
    ) -> acyclic_harness::Result<ControlDispatchResponse> {
        match self {
            Self::HostManaged(control) => dispatch_session_request(control, request).await
                .map(|value| (value, None)).map_err(RootError::Storage),
            Self::Pi(session) => session.dispatch(request, original_native_work).await,
        }
    }

    /// Transfer the real startup metadata admission into its actual actor owner.
    /// Failure destroys the controller's DTOs before either allocation guard.
    pub(crate) fn retain_original_input(self, custody: acyclic_fs::NativeAllocation)
        -> acyclic_harness::Result<Self>
    {
        let mut owned = (self, custody);
        match &mut owned.0 {
            Self::Pi(session) => {
                if let Some(existing) = &mut session.native_startup_custody {
                    existing.absorb(&mut owned.1)
                        .map_err(|cause| RootError::NativeWork { cause, custody: None })?;
                } else {
                    session.native_startup_custody = Some(owned.1);
                    return Ok(owned.0);
                }
            }
            Self::HostManaged(control) => {
                match control.retain_original_input(owned.1) {
                    Ok(()) => return Ok(owned.0),
                    Err((cause, custody)) => {
                        drop(owned.0);
                        return Err(RootError::NativeWork { cause, custody: Some(custody) });
                    }
                }
            }
        }
        Ok(owned.0)
    }

    pub(crate) async fn close(self, deactivate: bool, work: Option<&acyclic_fs::NativeMaterializationWork>)
        -> acyclic_harness::Result<StateSlots> {
        match self {
            Self::HostManaged(control) => control.close(deactivate).await.map_err(RootError::Storage),
            Self::Pi(session) => {
                let work = work.ok_or(RootError::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Invalid,
                    reason: "Pi close requires its original native query span",
                })?;
                session.close(deactivate, work).await
            }
        }
    }
}

pub(crate) struct PiSession {
    directory: PathBuf,
    relative_directory: PathBuf,
    state: AdapterState,
    slots: StateSlots,
    runtime: Arc<PiRuntime>,
    filesystem: Arc<FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>>,
    stream: StreamClient<LocalStream>,
    resources: ServiceResources,
    roots: BTreeMap<String, pi_root_provider::PiRootHandle>,
    native: Option<Arc<pi_native_host::PiNativeCommandHost>>,
    native_jobs: BTreeMap<TaskId, pi_lifecycle_session::PiNativeJob<(
        Option<acyclic_harness::filesystem::TaskWorkerAttempt>, acyclic_fs::NativeAllocation,
    )>>,
    // Declaration order keeps startup-owned data alive before its actual guard
    // is released, including cancellation/error destruction of the actor.
    native_startup_custody: Option<acyclic_fs::NativeAllocation>,
    native_pending_backing_custody: Option<acyclic_fs::NativeAllocation>,
    // Incremental DTOs belong to their original commands, not startup's ledger.
    native_catalog_custody: Vec<(TaskId, NativeCatalogComponent, acyclic_fs::NativeAllocation)>,
    native_catalog_backing_custody: Option<acyclic_fs::NativeAllocation>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum NativeCatalogComponent {
    Pending,
    Route,
}

/// Public observations contain task identities and outcomes, never execution fences, scheduler requirements or private journal credentials.
#[derive(Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
enum PiTaskStatus<'a> {
    Pending {
        task: TaskId,
    },
    Reserved {
        task: TaskId,
    },
    Unresolved {
        task: TaskId,
        #[serde(serialize_with = "crate::service::serialize_control_error")]
        error: &'a RootError,
    },
    Preparing {
        task: TaskId,
        preparation: TaskId,
        phase: &'a pi_recovery::PiPreparationPhase,
    },
    Outcome {
        task: TaskId,
        outcome: &'a Outcome<FileRef>,
    },
}

impl<'a> From<&'a pi_recovery::PiTaskObservation> for PiTaskStatus<'a> {
    fn from(observation: &'a pi_recovery::PiTaskObservation) -> Self {
        use pi_recovery::PiTaskObservation;
        match observation {
            PiTaskObservation::Pending { task_id } => Self::Pending { task: *task_id },
            PiTaskObservation::Reserved { lease } => Self::Reserved {
                task: TaskId::from_bytes(lease.operation.operation_id.into_bytes()),
            },
            PiTaskObservation::Unresolved { lease, error } => Self::Unresolved {
                task: TaskId::from_bytes(lease.operation.operation_id.into_bytes()),
                error,
            },
            PiTaskObservation::Preparation { task_id, preparation, phase } => Self::Preparing {
                task: *task_id, preparation: *preparation, phase,
            },
            PiTaskObservation::Outcome { task_id, outcome } => Self::Outcome {
                task: *task_id,
                outcome,
            },
        }
    }
}

struct PiTaskStatuses<'a>(&'a [pi_recovery::PiTaskObservation]);

impl Serialize for PiTaskStatuses<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut tasks = serializer.serialize_seq(Some(self.0.len()))?;
        for observation in self.0 {
            tasks.serialize_element(&PiTaskStatus::from(observation))?;
        }
        tasks.end()
    }
}

#[derive(Serialize)]
struct PiStatusResponse<'a> {
    session: &'a str,
    native_session: &'a str,
    tasks: PiTaskStatuses<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    caller_task: Option<TaskId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    caller: Option<&'a acyclic_harness::core::Scope>,
    #[serde(flatten)]
    operation: Option<&'a Value>,
}

#[derive(Serialize)]
#[serde(untagged)]
enum PiScopedBody<'a, T> {
    Fields(&'a T),
    Acknowledgement { acknowledgement: &'a T },
}

#[derive(Serialize)]
struct PiScopedResponse<'a, T> {
    session: &'a str,
    native_session: &'a str,
    caller_task: TaskId,
    #[serde(flatten)]
    body: PiScopedBody<'a, T>,
}

/// Finite native replies carry their actual caller, not a fabricated graph.
pub(crate) fn guarded_scoped_reply<T: Serialize>(
    session: &str, native_session: &str, caller: TaskId,
    mut reply: (T, acyclic_fs::NativeAllocation), acknowledgement: bool,
) -> acyclic_harness::Result<ControlDispatchResponse> {
    let body = if acknowledgement {
        PiScopedBody::Acknowledgement { acknowledgement: &reply.0 }
    } else {
        PiScopedBody::Fields(&reply.0)
    };
    let response = PiScopedResponse { session, native_session, caller_task: caller, body };
    let encoded = pi_root_provider::guarded_pi_value(
        &response, MAXIMUM_CONTROL_MESSAGE_BYTES as u64 * 8, reply.1.work(),
    );
    drop(reply.0);
    // Keeping each Value and guard in a tuple also preserves that destruction
    // order if the SDK encoder or same-ledger transfer rejects this response.
    let mut encoded = encoded?;
    encoded.1.absorb(&mut reply.1).map_err(|cause| RootError::NativeWork { cause, custody: None })?;
    Ok((encoded.0, Some(encoded.1)))
}

impl PiSession {
    async fn open(
        directory: PathBuf,
        state: AdapterState,
        slots: StateSlots,
        resources: &ServiceResources,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<Self> {
        let PluginTaskBinding::Pi(binding) = &state.task_binding else {
            return Err(RootError::Invalid("Pi session has no original task binding".to_owned()));
        };
        let relative_directory = resources.private_session_data(&directory)?;
        if !resources.original_session_directory_matches(relative_directory, &state.root_session_id, work)? {
            return Err(RootError::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid, reason: "Pi session directory differs from its original identity" });
        }
        let relative_directory = work.copy_path(relative_directory)
            .map_err(|cause| RootError::NativeWork { cause, custody: None })?;
        let owner = pi_owner::PiOwnerConfig::load_with_native_work(
            &resources.private_state_root, &relative_directory.0, work,
        )?;
        owner.validate_root(binding, work)?;
        // Recovery never bootstraps a missing task provider. Original admission
        // authentication below still rejects an empty or unrelated history.
        let stream_path = directory.join("stream");
        if !stream_path.is_dir() {
            return Err(RootError::Invalid("Pi original task provider is missing".to_owned()));
        }
        let filesystem = Arc::new(
            FilesystemHost::new(
                resources.fs.clone(),
                owner.policy().journal.provider().clone(),
            )?,
        );
        let stream = StreamClient::new(Arc::new(
            LocalStream::open(stream_path, LocalStreamLimits::default())
                .await
                .map_err(|error| RootError::Storage(error.to_string()))?,
        ));
        let (runtime, observations, custody) = PiRuntime::reopen(&owner, &state, stream.clone(), filesystem.clone(), work)
            .await?;
        drop(observations);
        drop(custody);
        let resources = pi_root_provider::service_resources_with_native_work(resources, work)?;
        let mut owned = (Self {
            directory,
            relative_directory: relative_directory.0,
            state,
            slots,
            runtime: Arc::new(runtime),
            filesystem,
            stream,
            resources: resources.0,
            roots: BTreeMap::new(),
            native: None,
            native_jobs: BTreeMap::new(),
            native_startup_custody: Some(relative_directory.1),
            native_pending_backing_custody: None,
            native_catalog_custody: Vec::new(),
            native_catalog_backing_custody: None,
        }, resources.1);
        owned.0.native_startup_custody.as_mut()
            .ok_or(RootError::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid, reason: "Pi actor lost its original relative source custody" })?
            .absorb(&mut owned.1)
            .map_err(|cause| RootError::NativeWork { cause, custody: None })?;
        Ok(owned.0)
    }

    pub(super) fn replace_native_pending_backing_custody(
        &mut self, custody: acyclic_fs::NativeAllocation,
    ) -> Option<acyclic_fs::NativeAllocation> {
        self.native_pending_backing_custody.replace(custody)
    }

    pub(super) fn prepare_native_catalog_slot(
        &mut self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<()> {
        if self.native_catalog_custody.len() < self.native_catalog_custody.capacity() {
            return Ok(());
        }
        let capacity = self.native_catalog_custody.capacity().checked_mul(2)
            .and_then(|capacity| capacity.checked_add(1))
            .ok_or(RootError::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Invalid,
                reason: "Pi native catalog capacity overflow",
            })?;
        let bytes = capacity.checked_mul(std::mem::size_of::<(
            TaskId, NativeCatalogComponent, acyclic_fs::NativeAllocation,
        )>()).ok_or(RootError::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Invalid,
            reason: "Pi native catalog backing allowance overflow",
        })?;
        // The real new backing is funded in full before its allocation attempt.
        // Its ledger can differ from the old backing and every resident DTO.
        let custody = work.claim(bytes as u64)
            .map_err(|cause| RootError::NativeWork { cause, custody: None })?;
        let replacement = Vec::with_capacity(capacity);
        let old = (
            std::mem::replace(&mut self.native_catalog_custody, replacement),
            self.native_catalog_backing_custody.replace(custody),
        );
        self.native_catalog_custody.extend(old.0);
        // extend consumes and physically frees the old Vec before this guard.
        drop(old.1);
        Ok(())
    }

    pub(super) fn install_native_catalog_custody(
        &mut self, key: TaskId, component: NativeCatalogComponent,
        custody: acyclic_fs::NativeAllocation,
    ) -> Option<acyclic_fs::NativeAllocation> {
        if let Some(entry) = self.native_catalog_custody.iter_mut()
            .find(|entry| entry.0 == key && entry.1 == component)
        {
            return Some(std::mem::replace(&mut entry.2, custody));
        }
        assert!(self.native_catalog_custody.len() < self.native_catalog_custody.capacity(),
            "Pi native catalog slot must be funded before inserting its DTO");
        self.native_catalog_custody.push((key, component, custody));
        None
    }

    pub(super) fn take_native_catalog_custody(
        &mut self, key: TaskId, component: NativeCatalogComponent,
    ) -> Option<acyclic_fs::NativeAllocation> {
        let index = self.native_catalog_custody.iter()
            .position(|entry| entry.0 == key && entry.1 == component)?;
        Some(self.native_catalog_custody.swap_remove(index).2)
    }

    /// Drives the original allocation command, then activates only its signed,
    /// immutable successful output and actual provider-created resources.
    async fn prepare_root(&mut self, work: &acyclic_fs::NativeMaterializationWork) -> acyclic_harness::Result<()> {
        self.prepare_root_with_allocation(true, work).await
    }

    async fn restore_root_sources(&mut self, work: &acyclic_fs::NativeMaterializationWork) -> acyclic_harness::Result<()> {
        self.prepare_root_with_allocation(false, work).await
    }

    async fn prepare_root_with_allocation(&mut self, allow_allocation: bool,
        original_native_work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<()> {
        let owner = pi_owner::PiOwnerConfig::load_with_native_work(
            &self.resources.private_state_root, &self.relative_directory, original_native_work,
        )?
            .into_native_shared(original_native_work)?;
        self.runtime.verify_owner_with_native_work(&owner, original_native_work)?;
        let PluginTaskBinding::Pi(binding) = &self.state.task_binding else {
            return Err(RootError::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid, reason: "Pi root has no original task binding" });
        };
        let preparation = pi_root_inputs::PiRootPreparation::load_native_from_state(
            &self.resources.private_state_root, &self.relative_directory, self.runtime.as_ref(), &self.filesystem, &self.state,
            None, original_native_work,
        ).await?;
        // Restore the original completed creator before running any Root
        // checkpoint: a cold Root may already be waiting on a child command.
        let observed = pi_root_provider::observe_root_allocation(
            Arc::clone(&owner), &self.resources, self.filesystem.clone(), self.stream.clone(),
            preparation, original_native_work,
        ).await?;
        let mut handles = BTreeMap::new();
        let mut allocation = match observed {
            Some(allocation) => allocation,
            None => {
                if !allow_allocation {
                    return Err(RootError::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid, reason: "Pi Root source has no original successful allocation to restore" });
                }
                let driver = pi_workflow::PiWorkflowHost::new(
                    &self.relative_directory, self.runtime.clone(), self.filesystem.clone(),
                    self.stream.clone(), &self.resources, &self.state,
                 original_native_work.clone());
                let progress = driver.drive_root().await?;
                let unresolved = match progress.0 {
                    Some(acyclic_harness::filesystem::TaskWorkerAttempt::Unresolved { lease, error }) => {
                        drop(lease);
                        Some(error)
                    }
                    attempt => {
                        drop(attempt);
                        None
                    }
                };
                // The producer's attempt custody covers lease/input metadata.
                // Native failures carry their own intrinsic cause custody.
                // Drop all non-error attempt DTOs before releasing that guard.
                drop(progress.1);
                if let Some(error) = unresolved {
                    return Err(error);
                }
                handles = driver.created_roots().await;
                if let Some(allocation) = driver.root_output().await? {
                    allocation
                } else {
                let preparation = pi_root_inputs::PiRootPreparation::load_native_from_state(
                    &self.resources.private_state_root, &self.relative_directory, self.runtime.as_ref(), &self.filesystem, &self.state,
                    None, original_native_work,
                ).await?;
                let Some(allocation) = pi_root_provider::observe_root_allocation(
                    owner, &self.resources, self.filesystem.clone(), self.stream.clone(),
                    preparation, original_native_work,
                ).await? else {
                    return Err(RootError::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid, reason: "Pi root has no authenticated successful allocation output" });
                };
                    allocation
                }
            }
        };
        allocation.0.1.validate(binding)?;
        if let PiScratchAllocation::Created(existing) = &binding.scratch.allocation
            && existing != &allocation.0.1.scratch
        {
            return Err(RootError::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid, reason: "Pi original scratch creation was substituted" });
        }
        let read_authority = self.runtime.runtime.readonly_task_context_with_native_work(
            binding.task_id, original_native_work,
        ).await?;
        let read_owner = &read_authority.0;
        let read_context = &read_authority.1;
        let proof_directory = &self.relative_directory;
        let proof_runtime = self.runtime.as_ref();
        let proof_filesystem = &self.filesystem;
        let proof_stream = &self.stream;
        let proof_resources = &self.resources;
        let proof_state = &self.state;
        let expected_allocation = &allocation.0.1;
        for (key, creation) in &allocation.0.1.roots {
            let root = self.state.roots.get(key).ok_or(RootError::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid, reason: "Pi allocation has an unselected root" })?;
            if let RootAllocation::Created(existing) = &root.allocation
                && (existing.repository_workspace_id != creation.repository_workspace_id
                    || existing.source_identity != creation.source_identity
                    || existing.native_root_identity != creation.native_root_identity
                    || existing.source_epoch < creation.source_epoch)
            {
                return Err(RootError::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid, reason: "Pi original root creation was substituted" });
            }
            if !handles.contains_key(key) && !self.roots.contains_key(key) {
                let expected = acyclic_fs::SourceReference {
                    identity: creation.source_identity, epoch: creation.source_epoch,
                };
                let physical = self.resources.shared_roots.acquire_with_native_work(
                    &root.path, SharedRootAdmission::Resume(expected), original_native_work,
                    || async move {
                        let preparation = pi_root_inputs::PiRootPreparation::load_native_from_state(
                            &proof_resources.private_state_root, proof_directory, proof_runtime, proof_filesystem, proof_state,
                            Some(Arc::clone(read_context)), original_native_work,
                        ).await?;
                        let owner = pi_owner::PiOwnerConfig::load_readonly_with_native_work(
                            &proof_resources.private_state_root, proof_directory, original_native_work,
                        )?.into_native_shared(original_native_work)?;
                        pi_root_provider::guard_created_root_source_with_native_work(
                            owner, proof_resources, Arc::clone(proof_filesystem),
                            proof_stream.clone(), preparation, read_owner, key,
                            expected_allocation, original_native_work,
                        ).await
                    },
                ).await?;
                if physical.source.inner().root_identity().to_bytes() != creation.native_root_identity {
                    return Err(RootError::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid, reason: "Pi native root identity changed after allocation" });
                }
                let volume = binding.project_volumes.get(key).ok_or(RootError::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid, reason: "Pi root has no original project volume" })?;
                let workspace_name = volume.storage_name_with_native_work(original_native_work)?;
                let workspace = self.resources.fs.open_workspace_with_native_work(
                    &workspace_name.0, original_native_work, original_native_work.cancellation(),
                ).await.map_err(|(source, custody)| RootError::NativeFilesystem { source, custody })?;
                drop(workspace_name.0);
                drop(workspace_name.1);
                if workspace.id().into_bytes() != creation.repository_workspace_id {
                    return Err(RootError::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid, reason: "Pi selected project workspace was substituted" });
                }
                let resumed = LazyWorkspace::resume_root_observation_with_native_work(
                    workspace, Arc::clone(&physical.source), self.resources.store.clone(),
                    expected, original_native_work,
                ).await.map_err(|(source, custody)| RootError::NativeSource { source, custody })?;
                let key = original_native_work.display(key)
                    .map_err(|cause| RootError::NativeWork { cause, custody: None })?;
                let (key, handle) = pi_root_provider::PiRootHandle::from_observed_with_native_work(
                    resumed, physical, key, original_native_work,
                )?;
                handles.insert(key, handle);
            }
        }
        self.roots.extend(handles);
        for (key, handle) in &self.roots {
            let rebound = handle.workspace.rebind_source_with_native_work(original_native_work)
                .await.map_err(|source| RootError::NativeSource { source, custody: None })?;
            let original = allocation.0.1.roots.get_mut(key).ok_or(RootError::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid, reason: "Pi retained root handle is not selected by allocation" })?;
            if rebound.0.source.identity != original.source_identity
                || rebound.0.source.epoch < original.source_epoch
            {
                return Err(RootError::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid, reason: "Pi root observation does not descend from original creation" });
            }
            // Only the genuine SDK rebind may advance the admitted creation epoch.
            original.source_epoch = rebound.0.source.epoch;
            drop(rebound.0);
            drop(rebound.1);
        }
        let PluginTaskBinding::Pi(binding) = &mut self.state.task_binding else {
            return Err(RootError::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid, reason: "Pi root binding changed during allocation" });
        };
        binding.scratch.allocation = PiScratchAllocation::Created(allocation.0.1.scratch);
        for (key, creation) in allocation.0.1.roots {
            self.state.roots.get_mut(&key).ok_or(RootError::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid, reason: "Pi allocated root disappeared" })?.allocation = RootAllocation::Created(creation);
        }
        self.slots.save_with_native_work(&self.resources.private_state_root, &self.relative_directory, &self.state, Survives::PowerLoss, original_native_work)?;
        let distributed = DistributedFs::new(self.resources.fs.clone(), self.resources.store.clone());
        let PluginTaskBinding::Pi(binding) = &self.state.task_binding else {
            return Err(RootError::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid, reason: "Pi root binding changed during allocation" });
        };
        // Admit the ordinary actor-to-SDK context DTO backing before copying.
        // This logical retention is not a fabricated SDK allocation operation.
        let context_bytes = self.roots.keys().try_fold(
            self.roots.len() as u64 * std::mem::size_of::<WorkspaceContextRoot>() as u64,
            |bytes, key| {
                let root = self.state.roots.get(key).ok_or(RootError::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid, reason: "Pi created root disappeared" })?;
                bytes.checked_add(root.path.as_os_str().as_encoded_bytes().len() as u64 * 2)
                    .ok_or(RootError::NativeBoundary {
                        code: acyclic_harness::wire::ErrorCode::Invalid,
                        reason: "Pi context metadata allowance overflow",
                    })
            },
        )?;
        let mut context_custody = original_native_work.retain_owned(context_bytes)
            .map_err(|cause| RootError::NativeWork { cause, custody: None })?;
        let mut context_roots = Vec::with_capacity(self.roots.len());
        for (key, handle) in &self.roots {
            let root = self.state.roots.get(key).ok_or(RootError::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid, reason: "Pi created root disappeared" })?;
            let lineage = distributed.lineage().register_root_with_native_work(
                handle.workspace.workspace(), original_native_work, original_native_work.cancellation(),
            ).await.map_err(|(source, custody)| RootError::NativeLineage { source, custody })?.into_parts();
            drop(lineage.0);
            drop(lineage.1);
            let mut workspace_name = binding.project_volumes.get(key)
                .ok_or(RootError::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid, reason: "Pi root lost its project volume" })?
                .storage_name_with_native_work(original_native_work)?;
            context_roots.push(WorkspaceContextRoot {
                root_id: WorkspaceRootId::from_bytes(root.root_id), source_path: root.path.clone(),
                workspace_id: handle.workspace.workspace().id(),
                workspace_name: workspace_name.0,
                parent_workspace_id: None, mount_path: None,
            });
            // Move the genuine funded name into the context vector, retaining
            // its same-ledger quota until that vector's final registration use.
            if let Err(cause) = context_custody.absorb(&mut workspace_name.1) {
                drop(context_roots);
                return Err(RootError::NativeWork { cause, custody: None });
            }
        }
        // Refine the conservative admission to this sole transferred DTO's
        // actual deep backing, never the source/owner handles held separately.
        let actual_context_bytes = context_roots.iter().try_fold(
            context_roots.capacity() as u64 * std::mem::size_of::<WorkspaceContextRoot>() as u64,
            |bytes, root| {
                let owned = root.source_path.capacity() as u64 + root.workspace_name.capacity() as u64
                    + root.mount_path.as_ref().map_or(0, |path| path.capacity() as u64);
                bytes.checked_add(owned).ok_or(RootError::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Invalid,
                    reason: "Pi context metadata allowance overflow",
                })
            },
        )?;
        context_custody.resize_retained(actual_context_bytes)
            .map_err(|cause| RootError::NativeWork { cause, custody: None })?;
        let context = distributed.contexts().register_root_with_native_work(
            WorkspaceContextId::from_bytes(self.state.root_context_id), (context_roots, context_custody),
            original_native_work, original_native_work.cancellation(),
        ).await.map_err(|(source, custody)| RootError::NativeContext { source, custody })?.into_parts();
        drop(context.0);
        drop(context.1);
        self.state.pending_root_registration = false;
        // All variable-size observed allocation DTOs are now consumed; created
        // state contains only fixed identities. The original source guards stay
        // resident in their real physical handles, not a Pi retention map.
        drop(allocation.0.0);
        drop(allocation.1);
        self.slots.save_with_native_work(&self.resources.private_state_root, &self.relative_directory, &self.state, Survives::PowerLoss, original_native_work)
    }

    fn binding(&self, task: TaskId) -> acyclic_harness::Result<&PiTaskBinding> {
        if let PluginTaskBinding::Pi(binding) = &self.state.task_binding
            && binding.task_id == task
        {
            return Ok(binding);
        }
        self.state.routes.values().find_map(|route| match &route.task_binding {
            PluginTaskBinding::Pi(binding) if binding.task_id == task => Some(binding.as_ref()),
            _ => None,
        }).ok_or_else(|| RootError::Invalid("Pi task has no retained original binding".to_owned()))
    }

    fn alias_task(&self, alias: &str) -> acyclic_harness::Result<TaskId> {
        if alias == self.state.root_agent_id || alias == self.state.root_session_id || alias == "root" {
            let PluginTaskBinding::Pi(binding) = &self.state.task_binding else {
                return Err(RootError::Invalid("Pi root has no original binding".to_owned()));
            };
            return Ok(binding.task_id);
        }
        let route = self.state.routes.get(alias).ok_or_else(|| RootError::Invalid("Pi selected agent is not registered".to_owned()))?;
        let PluginTaskBinding::Pi(binding) = &route.task_binding else {
            return Err(RootError::Invalid("Pi selected agent has no original task".to_owned()));
        };
        Ok(binding.task_id)
    }

    fn prepare_spawn(
        &self, caller: TaskId, arguments: &Value, owner: &pi_owner::PiOwnerConfig,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<(Value, acyclic_fs::NativeAllocation)> {
        let sdk_operation = string(arguments, "operation").map_err(RootError::Invalid)?;
        let alias = string(arguments, "id").map_err(RootError::Invalid)?;
        if sdk_operation.is_empty() || sdk_operation.len() > 512 || alias.is_empty()
            || alias.len() > 128 || alias == "root" || alias == self.state.root_agent_id
            || alias == self.state.root_session_id
            || !alias.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        {
            return Err(RootError::Invalid("Pi spawn requires bounded original tool and new agent identities".to_owned()));
        }
        if self.state.routes.contains_key(&alias)
            || self.state.pending.iter().any(|pending| pending.agent_id.as_deref() == Some(alias.as_str()))
        {
            return Err(RootError::Invalid("Pi new child agent identity is already registered".to_owned()));
        }
        let parent = self.binding(caller)?;
        if self.state.pending.iter().any(|pending| pending.tool_use_id == sdk_operation
            && matches!(&pending.task_binding, PluginTaskBinding::Pi(binding)
                if binding.agent_id != parent.agent_id))
        {
            return Err(RootError::Invalid("Pi original child preparation is uncertain; observe it without reseeding a session".to_owned()));
        }
        let context = arguments.get("context").and_then(Value::as_str).unwrap_or("scratch");
        if !matches!(context, "native_cut" | "scratch") {
            return Err(RootError::Invalid("Pi context must be native_cut or scratch".to_owned()));
        }
        let prompt = string(arguments, "prompt").map_err(RootError::Invalid)?;
        let provider = string(arguments, "provider").map_err(RootError::Invalid)?;
        let model = string(arguments, "model").map_err(RootError::Invalid)?;
        if prompt.len() as u64 > owner.policy().limits.file_bytes
            || provider.is_empty() || provider.len() > 256 || model.is_empty() || model.len() > 256
        {
            return Err(RootError::Invalid("Pi native instruction or model exceeds its admitted bounds".to_owned()));
        }
        owner.native_runtime()?.validate()?;
        let original = pi_fork::original_operation(owner.policy().root_task, caller, &sdk_operation);
        let operations = pi_fork::PiForkOperations::new(original);
        let task = TaskId::from_bytes(operations.child.into_bytes());
        let source_task = match arguments.get("source") {
            None | Some(Value::Null) => caller,
            Some(Value::String(source)) => self.alias_task(source)?,
            Some(_) => return Err(RootError::Invalid("Pi workspace source must be a registered agent identity".to_owned())),
        };
        if source_task != caller && !owner.policy().grants.contains("pi:source:select") {
            return Err(RootError::Invalid("Pi original root policy does not authorize explicit selected-source reads".to_owned()));
        }
        let source_binding = self.binding(source_task)?;
        let key = match arguments.get("root") {
            Some(Value::String(key)) => source_binding.project_volumes.get_key_value(key)
                .map(|(key, _)| key).ok_or_else(|| RootError::Invalid("Pi selected root is not an original source project".to_owned()))?,
            None | Some(Value::Null) if source_binding.project_volumes.len() == 1 =>
                source_binding.project_volumes.keys().next().ok_or_else(|| RootError::Invalid("Pi selected source has no original project".to_owned()))?,
            None | Some(Value::Null) =>
                return Err(RootError::Invalid("Pi multi-root source requires an explicit root identity".to_owned())),
            Some(_) => return Err(RootError::Invalid("Pi root selector must be its retained identity".to_owned())),
        };
        let paths = pi_native::planned_native_paths(
            &self.resources, &self.state.root_session_id, task, Some(key), work,
        )?;
        let paths = &paths.0;
        #[derive(Serialize)]
        struct Child<'a> {
            id: &'a str, task: TaskId, cwd: &'a Path,
            session_file: &'a Path, session_directory: &'a Path,
        }
        #[derive(Serialize)]
        struct Source { task: TaskId }
        #[derive(Serialize)]
        struct Preparation<'a> {
            operation: &'a str, state: &'a str, child: Child<'a>, source: Source,
        }
        // Planned paths are ordinary service transport metadata, not SDK source
        // bodies. The SDK admits the actual outgoing Value before any growth.
        pi_root_provider::guarded_pi_value(&Preparation {
            operation: &sdk_operation, state: "preparing",
            child: Child { id: &alias, task, cwd: &paths.cwd,
                session_file: &paths.session_file, session_directory: &paths.session_directory },
            source: Source { task: source_task },
        }, MAXIMUM_CONTROL_MESSAGE_BYTES as u64 * 8, work)
    }

    fn authorize_mail(&self, caller: TaskId, recipient: TaskId) -> acyclic_harness::Result<()> {
        if self.authorize_subtree(caller, recipient).is_ok() {
            return Ok(());
        }
        let route = self.state.routes.values().find(|route|
            matches!(&route.task_binding, PluginTaskBinding::Pi(binding) if binding.task_id == caller)).ok_or_else(|| RootError::Invalid("Pi sender has no original parent route".to_owned()))?;
        if self.alias_task(&route.parent_agent_id)? == recipient {
            Ok(())
        } else {
            Err(RootError::Invalid("Pi sender cannot impersonate another mailbox route".to_owned()))
        }
    }

    pub(crate) fn query_work(
        original_native_work: &mut Option<acyclic_fs::NativeMaterializationWork>,
    ) -> acyclic_harness::Result<acyclic_fs::NativeMaterializationWork> {
        if let Some(work) = original_native_work {
            return Ok(work.clone());
        }
        let work = acyclic_fs::NativeMaterializationWork::new(
            pi_root_provider::allocation_budget(), &CancellationToken::new(),
        ).map_err(|(cause, work)| RootError::NativeConstruction { cause, work })?;
        *original_native_work = Some(work.clone());
        Ok(work)
    }

    async fn mail_work(
        &mut self, caller: TaskId, owner: &pi_owner::PiOwnerConfig,
        query_work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<acyclic_fs::NativeMaterializationWork> {
        if caller == owner.policy().root_task {
            Ok(query_work.clone())
        } else {
            self.native_host_with_native_work(query_work)?.invocation_work(caller).await
        }
    }

    async fn send_control(
        &mut self, caller: TaskId, arguments: &Value, owner: &pi_owner::PiOwnerConfig,
        kind: pi_mail::PiMailKind,
        query_work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<(Value, acyclic_fs::NativeAllocation)> {
        let recipient = self.alias_task(&string(arguments, "id").map_err(RootError::Invalid)?)?;
        self.authorize_mail(caller, recipient)?;
        let sdk_operation = string(arguments, "operation").map_err(RootError::Invalid)?;
        if sdk_operation.is_empty() || sdk_operation.len() > 512 {
            return Err(RootError::Invalid("Pi mail requires its original bounded SDK operation".to_owned()));
        }
        let operation = pi_fork::original_operation(owner.policy().root_task, caller, &sdk_operation);
        let work = self.mail_work(caller, owner, query_work).await?;
        let message = arguments.get("message").and_then(Value::as_str).ok_or_else(|| RootError::Invalid("Pi mail message must be a string".to_owned()))?;
        let _envelope_custody = work.retain_owned(message.len() as u64)
            .map_err(|cause| RootError::NativeWork { cause, custody: None })?;
        let envelope = pi_mail::PiMailEnvelope { kind, message: message.to_owned() };
        if caller == owner.policy().root_task {
            let command = pi_mail::stage_send(
                owner, &self.filesystem, operation, caller, recipient, envelope, &work,
            ).await?;
            return pi_workflow::PiWorkflowHost::new(
                &self.relative_directory, self.runtime.clone(), self.filesystem.clone(),
                self.stream.clone(), &self.resources, &self.state,
             work).control_command_with_native_work(command.0).await;
        }
        let binding = self.binding(caller)?;
        let host = self.runtime.runtime.task_host();
        let lease = match host.recover_work(caller).await? {
            acyclic_harness::distributed::WorkPull::Claimed(lease) => lease,
            acyclic_harness::distributed::WorkPull::Unresolved { .. } =>
                return Err(RootError::Invalid("Pi sender's original native attempt is unresolved".to_owned())),
            acyclic_harness::distributed::WorkPull::Idle =>
                return Err(RootError::Invalid("Pi sender has no active original task".to_owned())),
        };
        let fence = acyclic_harness::scheduler::LeaseFence::from(&lease.reservation);
        let journal_owner = host.journal_owner(binding.task_id, fence.clone()).await?;
        let context = self.runtime.runtime.task_context(caller, fence.clone()).await?;
        let command = pi_mail::prepare_send(
            &context, &journal_owner, owner, &self.filesystem, operation, recipient, envelope,
            |sender, destination| self.authorize_mail(sender, destination),
            &work,
        ).await?;
        match pi_mail::execute_send(&self.runtime.runtime, context, &journal_owner, fence, command.0, &work)
            .await?
        {
            acyclic_harness::filesystem::TaskCommandProgress::ReadyWithNativeCustody { value, custody } =>
                Ok((value, custody)),
            _ => Err(RootError::Invalid("Pi original mail acknowledgement is uncertain; no replacement was sent".to_owned())),
        }
    }

    fn authorize_subtree(&self, caller: TaskId, target: TaskId) -> acyclic_harness::Result<()> {
        let PluginTaskBinding::Pi(root) = &self.state.task_binding else {
            return Err(RootError::Invalid("Pi task graph has no original root".to_owned()));
        };
        if target == root.task_id {
            return if caller == root.task_id { Ok(()) } else {
                Err(RootError::Invalid("Pi child cannot control its parent".to_owned()))
            };
        }
        let mut route = self.state.routes.values().find(|route| {
            matches!(&route.task_binding, PluginTaskBinding::Pi(binding) if binding.task_id == target)
        }).ok_or_else(|| RootError::Invalid("Pi target has no retained admitted route".to_owned()))?;
        for _ in 0..=self.state.routes.len() {
            if matches!(&route.task_binding, PluginTaskBinding::Pi(binding) if binding.task_id == caller) {
                return Ok(());
            }
            if route.parent_agent_id == self.state.root_agent_id {
                return if caller == root.task_id { Ok(()) } else {
                    Err(RootError::Invalid("Pi caller cannot control a sibling or unrelated subtree".to_owned()))
                };
            }
            route = self.state.routes.get(&route.parent_agent_id).ok_or_else(|| RootError::Invalid("Pi target lineage is incomplete".to_owned()))?;
        }
        Err(RootError::Invalid("Pi target lineage contains a cycle".to_owned()))
    }

    async fn dispatch(&mut self, request: ControlRequest,
        original_native_work: &mut Option<acyclic_fs::NativeMaterializationWork>,
    ) -> acyclic_harness::Result<ControlDispatchResponse> {
        let query_work = Self::query_work(original_native_work)?;
        let mut issued_caller = None;
        let mut authenticated_caller_task = None;
        let mut operation_result = None;
        if request.command == ControlCommand::Hook {
            let event = request.name.strip_prefix("pi:").ok_or_else(|| RootError::Invalid("Pi session refuses another host's control".to_owned()))?;
            if matches!(event, "OperatorInspect" | "OperatorApproval") {
                return self.operator_control(event, &request.arguments, original_native_work).await;
            }
            let session = request.arguments.get("session_id").and_then(Value::as_str)
                .ok_or(RootError::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid, reason: "Pi control requires its original session identity" })?;
            let owner = pi_owner::PiOwnerConfig::load_with_native_work(
                &self.resources.private_state_root, &self.relative_directory, &query_work,
            )?;
            self.runtime.verify_owner_with_native_work(&owner, &query_work)?;
            let caller = request.arguments.get("caller")
                .map(|value| acyclic_harness::core::Scope::from_value_with_native_work(value, &query_work))
                .transpose()?;
            let caller_task = if let Some((scope, _custody)) = &caller {
                let binding = pi_caller::verify(
                    &owner, &self.state, scope, session, &query_work,
                )?;
                binding.task_id
            } else {
                if event != "SessionStart" || session != self.state.root_session_id {
                    return Err(RootError::Invalid("Pi control requires its original caller proof".to_owned()));
                }
                let PluginTaskBinding::Pi(binding) = &self.state.task_binding else {
                    return Err(RootError::Invalid("Pi original root binding is missing".to_owned()));
                };
                issued_caller = Some(pi_caller::issue(
                    &owner, &self.state.root_session_id, binding, &query_work,
                )?);
                binding.task_id
            };
            authenticated_caller_task = Some(caller_task);
            match event {
                "SessionStart" => {
                    if session == self.state.root_session_id {
                        let selected = PathBuf::from(string(&request.arguments, "cwd").map_err(RootError::Invalid)?)
                            .canonicalize().map_err(|error| RootError::Storage(error.to_string()))?;
                        if !self.state.roots.values().any(|root| root.path == selected) {
                            return Err(RootError::Invalid("Pi root selection changed after original admission".to_owned()));
                        }
                        self.prepare_root(&query_work).await?;
                        self.recover_native_children(&query_work).await?;
                    } else {
                        if self.roots.is_empty() {
                            self.prepare_root(&query_work).await?;
                            self.recover_native_children(&query_work).await?;
                        }
                        let route = self.state.routes.values().find(|route| {
                            matches!(&route.task_binding, PluginTaskBinding::Pi(binding)
                                if binding.task_id == caller_task && binding.native_session_id == session)
                        }).ok_or_else(|| RootError::Invalid("Pi native child has no original created route".to_owned()))?;
                        let selected = PathBuf::from(string(&request.arguments, "cwd").map_err(RootError::Invalid)?)
                            .canonicalize().map_err(|error| RootError::Storage(error.to_string()))?;
                        if !selected.starts_with(&route.mount_path) || !route.lifecycle.needs_mount() {
                            return Err(RootError::Invalid("Pi native child selection differs from its created view".to_owned()));
                        }
                    }
                }
                "NativeBuiltinRead" | "NativeBuiltinWrite" | "NativeBuiltinEdit" => {
                    return self.native_builtin_control(caller_task, event, &request.arguments, &owner, &query_work).await;
                }
                "SpawnPrepare" => {
                    operation_result = Some(self.prepare_spawn(caller_task, &request.arguments, &owner, &query_work)?);
                }
                "SpawnCommit" => {
                    let committed = self.spawn_commit(caller_task, &request.arguments, &owner, &query_work).await?;
                    let binding = self.binding(caller_task)?;
                    return guarded_scoped_reply(&self.state.root_session_id, &binding.native_session_id,
                        caller_task, committed, false);
                }
                "TaskInspect" | "TaskDiff" | "TaskImport" | "TaskWriteback" => {
                    let selected = self.selected_control(caller_task, event, &request.arguments, &owner, &query_work).await?;
                    let binding = self.binding(caller_task)?;
                    return guarded_scoped_reply(&self.state.root_session_id, &binding.native_session_id,
                        caller_task, selected, false);
                }
                "TaskWritebackStatus" => {
                    return self.writeback_status(caller_task, &request.arguments, &query_work).await;
                }
                "TaskSend" | "TaskSteer" => {
                    let kind = if event == "TaskSteer" {
                        pi_mail::PiMailKind::Steer
                    } else {
                        pi_mail::PiMailKind::Message
                    };
                    let acknowledgement = self.send_control(
                        caller_task, &request.arguments, &owner, kind, &query_work,
                    ).await?;
                    let binding = self.binding(caller_task)?;
                    return guarded_scoped_reply(&self.state.root_session_id, &binding.native_session_id,
                        caller_task, acknowledgement, true);
                }
                "TaskReceive" => {
                    let after = request.arguments.get("after").and_then(Value::as_u64).ok_or_else(|| RootError::Invalid("Pi receive requires an exact mailbox cursor".to_owned()))?;
                    let work = self.mail_work(caller_task, &owner, &query_work).await?;
                    let binding = self.binding(caller_task)?;
                    let received = pi_mail::receive(
                        &self.runtime.runtime, binding, after, 1, &work,
                    ).await?;
                    return guarded_scoped_reply(&self.state.root_session_id, &binding.native_session_id,
                        caller_task, received, false);
                }
                "TaskWait" => {
                    let target = self.alias_task(&string(&request.arguments, "id").map_err(RootError::Invalid)?)?;
                    let work = self.mail_work(caller_task, &owner, &query_work).await?;
                    let binding = self.binding(caller_task)?;
                    let observed = pi_mail::wait(
                        &self.runtime.runtime, binding, target, 0,
                        |caller, target| self.authorize_subtree(caller, target),
                        &work,
                    ).await?;
                    return guarded_scoped_reply(&self.state.root_session_id, &binding.native_session_id,
                        caller_task, observed, false);
                }
                "TaskStatus" => {}
                "TaskCancel" => {
                    let task: TaskId = serde::Deserialize::deserialize(
                        request.arguments.get("task").ok_or_else(|| RootError::Invalid("Pi cancellation requires its original task".to_owned()))?,
                    ).map_err(|error| RootError::Invalid(error.to_string()))?;
                    self.cancel_native_subtree(caller_task, task, &query_work).await?;
                }
                _ => return Err(RootError::Invalid("unknown Pi task control operation".to_owned())),
            }
        } else if request.command == ControlCommand::Agents {
            parse_read_only_arguments(&request.argv).map_err(RootError::Invalid)?;
        } else {
            return Err(RootError::Invalid("Pi workspace operations require their owning task runtime".to_owned()));
        }
        // Observe the real complete SDK task graph under this request's original
        // query scope. Borrow its DTOs; no second status Vec or boxed outcome.
        let mut observations = pi_recovery::observe_pi_session(
            &self.runtime.runtime, &self.state, &query_work,
        ).await?;
        let PluginTaskBinding::Pi(root_binding) = &self.state.task_binding else {
            return Err(RootError::Invalid("Pi original root binding is missing".to_owned()));
        };
        let native_session = &self.binding(
            authenticated_caller_task.unwrap_or(root_binding.task_id),
        )?.native_session_id;
        let response = PiStatusResponse {
            session: &self.state.root_session_id, native_session,
            tasks: PiTaskStatuses(&observations.0), caller_task: authenticated_caller_task,
            caller: issued_caller.as_ref().map(|reply| &reply.0),
            operation: operation_result.as_ref().map(|reply| &reply.0),
        };
        let encoded = pi_root_provider::guarded_pi_value(
            &response, MAXIMUM_CONTROL_MESSAGE_BYTES as u64 * 8, observations.1.work(),
        );
        drop(observations.0);
        let mut encoded = encoded?;
        encoded.1.absorb(&mut observations.1).map_err(|cause| RootError::NativeWork { cause, custody: None })?;
        if let Some(mut reply) = operation_result {
            drop(reply.0);
            encoded.1.absorb(&mut reply.1).map_err(|cause| RootError::NativeWork { cause, custody: None })?;
        }
        if let Some(mut reply) = issued_caller {
            drop(reply.0);
            encoded.1.absorb(&mut reply.1).map_err(|cause| RootError::NativeWork { cause, custody: None })?;
        }
        Ok((encoded.0, Some(encoded.1)))
    }

    async fn close(mut self, deactivate: bool, work: &acyclic_fs::NativeMaterializationWork)
        -> acyclic_harness::Result<StateSlots> {
        self.drain_native(deactivate, work).await?;
        Ok(self.slots)
    }
}
