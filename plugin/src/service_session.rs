//! Session ownership behind the service's existing actor and catalog.

use super::*;
use crate::pi_selected_host::invalid;
use acyclic_harness::{
    Error as RootError, Outcome, TaskId,
    conversation::FileRef,
    filesystem::FilesystemHost,
    runtime::{DurableTaskHost, task_id_for_admission},
};
use acyclic_stream::{LocalStream, LocalStreamLimits};

use pi_runtime::PiTaskRuntime;

mod dispatch;
#[path = "pi_builtin_session.rs"]
mod pi_builtin_session;
#[path = "pi_lifecycle_session.rs"]
mod pi_lifecycle_session;
#[path = "pi_operator_session.rs"]
mod pi_operator_session;
#[path = "pi_selected_session.rs"]
mod pi_selected_session;
#[path = "pi_spawn_session/mod.rs"]
mod pi_spawn_session;
mod root;

pub(crate) enum SessionController {
    HostManaged(Box<ControlPlane>),
    Pi(Box<PiSession>),
}

impl SessionController {
    pub(crate) async fn open(
        directory: PathBuf,
        resources: &ServiceResources,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<Self> {
        let relative_directory = resources.private_session_data(&directory)?;
        let mut loaded = crate::adapter_state::load_state_with_native_work(
            &resources.private_state_root,
            relative_directory,
            work,
        )?;
        match &loaded.0.0.task_binding {
            PluginTaskBinding::HostManaged => {
                let host = resources
                    .open_session_directory(directory)
                    .await
                    .map_err(RootError::Storage)?;
                drop(loaded.0);
                drop(loaded.1);
                Ok(Self::HostManaged(Box::new(host)))
            }
            PluginTaskBinding::Pi(_) => {
                let mut custody = work.claim(
                    std::mem::size_of::<PiSession>() as u64,
                    0,
                    acyclic_fs::WorkCounters::UNCHARGED,
                )?;
                let session =
                    PiSession::open(directory, loaded.0.0, loaded.0.1, resources, work).await?;
                custody.absorb(&mut loaded.1)?;
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

    pub(crate) async fn dispatch(
        &mut self,
        request: ControlRequest,
        original_native_work: &mut Option<acyclic_fs::NativeMaterializationWork>,
    ) -> acyclic_harness::Result<ControlDispatchResponse> {
        match self {
            Self::HostManaged(control) => dispatch_session_request(control, request)
                .await
                .map(|value| (value, None))
                .map_err(RootError::Storage),
            Self::Pi(session) => session.dispatch(request, original_native_work).await,
        }
    }

    /// Retains startup metadata until the controller's data drops.
    pub(crate) fn retain_original_input(
        self,
        custody: acyclic_fs::NativeAllocation,
    ) -> acyclic_harness::Result<Self> {
        let mut owned = (self, custody);
        match &mut owned.0 {
            Self::Pi(session) => {
                session.native_startup_custody.absorb(&mut owned.1)?;
            }
            Self::HostManaged(control) => match control.retain_original_input(owned.1) {
                Ok(()) => return Ok(owned.0),
                Err((cause, custody)) => {
                    drop(owned.0);
                    return Err(RootError::NativeWork {
                        cause,
                        custody: Some(custody),
                    });
                }
            },
        }
        Ok(owned.0)
    }

    pub(crate) async fn close(
        self,
        deactivate: bool,
        work: Option<&acyclic_fs::NativeMaterializationWork>,
    ) -> acyclic_harness::Result<StateSlots> {
        match self {
            Self::HostManaged(control) => {
                control.close(deactivate).await.map_err(RootError::Storage)
            }
            Self::Pi(mut session) => {
                let work = work
                    .ok_or_else(|| invalid("Pi close requires its original native query span"))?;
                session.drain_native(deactivate, work).await?;
                Ok(session.slots)
            }
        }
    }
}

pub(crate) struct PiSession {
    directory: PathBuf,
    relative_directory: PathBuf,
    state: AdapterState,
    slots: StateSlots,
    runtime: Arc<PiTaskRuntime>,
    filesystem: Arc<FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>>,
    stream: std::sync::Arc<LocalStream>,
    resources: ServiceResources,
    roots: BTreeMap<String, pi_root_provider::PiRootHandle>,
    native: Option<Arc<pi_native_host::PiNativeCommandHost>>,
    native_jobs: BTreeMap<
        TaskId,
        (
            tokio::task::JoinHandle<
                acyclic_harness::Result<(
                    Option<acyclic_harness::filesystem::TaskWorkerAttempt>,
                    acyclic_fs::NativeAllocation,
                )>,
            >,
            acyclic_fs::NativeAllocation,
        ),
    >,
    // Drop startup data before its allocation guard.
    native_startup_custody: acyclic_fs::NativeAllocation,
    native_pending_backing_custody: Option<acyclic_fs::NativeAllocation>,
    // Incremental DTOs belong to their original commands, not startup's ledger.
    native_catalog_custody: Vec<(TaskId, NativeCatalogComponent, acyclic_fs::NativeAllocation)>,
    native_catalog_backing_custody: Option<acyclic_fs::NativeAllocation>,
}

impl PiSession {
    pub(super) fn command_host(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
        native: Option<Arc<pi_native_host::PiNativeCommandHost>>,
    ) -> acyclic_harness::Result<pi_host::PiControlHost<'_>> {
        let owner = match &native {
            Some(native) => std::sync::Arc::clone(&native.protected),
            None => pi_owner::PiOwnerConfig::load(
                &self.resources.private_state_root,
                &self.relative_directory,
                work,
            )?
            .into_native_shared(work)?,
        };
        pi_host::PiControlHost::new(
            &self.relative_directory,
            self.runtime.clone(),
            self.filesystem.clone(),
            self.stream.clone(),
            &self.resources,
            owner,
            &self.state,
            &self.roots,
            native,
            work,
        )
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum NativeCatalogComponent {
    Pending,
    Route,
}

#[derive(Serialize)]
struct PiStatusResponse<'a> {
    session: &'a str,
    native_session: &'a str,
    tasks: &'a [pi_recovery::PiTaskObservation],
    #[serde(skip_serializing_if = "Option::is_none")]
    caller_task: Option<TaskId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    caller: Option<&'a acyclic_harness::core::Scope>,
    #[serde(flatten)]
    operation: Option<&'a Value>,
}

/// Mailbox acknowledgement body.
#[derive(Serialize)]
pub(crate) struct PiAcknowledgement<T> {
    pub(crate) acknowledgement: T,
}

#[derive(Serialize)]
struct PiScopedResponse<'a, T> {
    session: &'a str,
    native_session: &'a str,
    caller_task: TaskId,
    #[serde(flatten)]
    body: &'a T,
}

/// Finite native replies carry their actual caller, not a fabricated graph.
pub(crate) fn guarded_scoped_reply<T: Serialize>(
    session: &str,
    native_session: &str,
    caller: TaskId,
    mut reply: (T, acyclic_fs::NativeAllocation),
) -> acyclic_harness::Result<ControlDispatchResponse> {
    let response = PiScopedResponse {
        session,
        native_session,
        caller_task: caller,
        body: &reply.0,
    };
    let encoded = pi_root_provider::guarded_pi_value(
        &response,
        MAXIMUM_CONTROL_MESSAGE_BYTES as u64 * 8,
        reply.1.work(),
    );
    drop(reply.0);
    // Drop each Value before its allocation guard.
    let mut encoded = encoded?;
    encoded.1.absorb(&mut reply.1)?;
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
            return Err(RootError::Invalid(
                "Pi session has no original task binding".to_owned(),
            ));
        };
        let relative_directory = resources.private_session_data(&directory)?;
        if !resources.original_session_directory_matches(
            relative_directory,
            &state.root_session_id,
            work,
        )? {
            return Err(invalid(
                "Pi session directory differs from its original identity",
            ));
        }
        let relative_directory = work.copy_path(relative_directory)?;
        let owner = pi_owner::PiOwnerConfig::load(
            &resources.private_state_root,
            &relative_directory.0,
            work,
        )?;
        owner.validate_root(binding, work)?;
        let stream_path = directory.join("stream");
        if !stream_path.is_dir() {
            return Err(RootError::Invalid(
                "Pi original task provider is missing".to_owned(),
            ));
        }
        let filesystem = FilesystemHost::new(
            resources.fs.clone(),
            owner.policy().journal.provider(),
            Some(&acyclic_harness::filesystem::PreparationIo::from_native_work(work)?),
        )?;
        let stream = Arc::new(
            LocalStream::open(stream_path, LocalStreamLimits::default())
                .await
                .map_err(|error| RootError::Storage(error.to_string()))?,
        );
        let (runtime, observations, custody) =
            PiTaskRuntime::reopen(&owner, &state, stream.clone(), filesystem.clone(), work).await?;
        drop(observations);
        drop(custody);
        let resources = pi_root_provider::service_resources_with_native_work(resources, work)?;
        let mut owned = (
            Self {
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
                native_startup_custody: relative_directory.1,
                native_pending_backing_custody: None,
                native_catalog_custody: Vec::new(),
                native_catalog_backing_custody: None,
            },
            resources.1,
        );
        owned.0.native_startup_custody.absorb(&mut owned.1)?;
        Ok(owned.0)
    }

    pub(super) fn prepare_native_catalog_slot(
        &mut self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<()> {
        if self.native_catalog_custody.len() < self.native_catalog_custody.capacity() {
            return Ok(());
        }
        let capacity = self
            .native_catalog_custody
            .capacity()
            .checked_mul(2)
            .and_then(|capacity| capacity.checked_add(1))
            .ok_or_else(|| invalid("Pi native catalog capacity overflow"))?;
        let replacement = pi_native::pi_native_vec(capacity, work)?;
        let old = (
            std::mem::replace(&mut self.native_catalog_custody, replacement.0),
            self.native_catalog_backing_custody.replace(replacement.1),
        );
        self.native_catalog_custody.extend(old.0);
        // extend consumes and physically frees the old Vec before this guard.
        drop(old.1);
        Ok(())
    }

    pub(super) fn install_native_catalog_custody(
        &mut self,
        key: TaskId,
        component: NativeCatalogComponent,
        custody: acyclic_fs::NativeAllocation,
    ) -> Option<acyclic_fs::NativeAllocation> {
        if let Some(entry) = self
            .native_catalog_custody
            .iter_mut()
            .find(|entry| entry.0 == key && entry.1 == component)
        {
            return Some(std::mem::replace(&mut entry.2, custody));
        }
        assert!(
            self.native_catalog_custody.len() < self.native_catalog_custody.capacity(),
            "Pi native catalog slot must be funded before inserting its DTO"
        );
        self.native_catalog_custody.push((key, component, custody));
        None
    }

    pub(super) fn take_native_catalog_custody(
        &mut self,
        key: TaskId,
        component: NativeCatalogComponent,
    ) -> Option<acyclic_fs::NativeAllocation> {
        let index = self
            .native_catalog_custody
            .iter()
            .position(|entry| entry.0 == key && entry.1 == component)?;
        Some(self.native_catalog_custody.swap_remove(index).2)
    }

    fn binding(&self, task: TaskId) -> acyclic_harness::Result<&PiTaskBinding> {
        if let PluginTaskBinding::Pi(binding) = &self.state.task_binding
            && binding.task_id == task
        {
            return Ok(binding);
        }
        self.state
            .routes
            .values()
            .find_map(|route| match &route.task_binding {
                PluginTaskBinding::Pi(binding) if binding.task_id == task => Some(binding.as_ref()),
                _ => None,
            })
            .ok_or_else(|| invalid("Pi task has no retained original binding"))
    }

    fn alias_task(&self, alias: &str) -> acyclic_harness::Result<TaskId> {
        if alias == self.state.root_agent_id
            || alias == self.state.root_session_id
            || alias == "root"
        {
            let PluginTaskBinding::Pi(binding) = &self.state.task_binding else {
                return Err(invalid("Pi root has no original binding"));
            };
            return Ok(binding.task_id);
        }
        let route =
            self.state.routes.get(alias).ok_or_else(|| {
                RootError::Invalid("Pi selected agent is not registered".to_owned())
            })?;
        let PluginTaskBinding::Pi(binding) = &route.task_binding else {
            return Err(invalid("Pi selected agent has no original task"));
        };
        Ok(binding.task_id)
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
            Err(RootError::Invalid(
                "Pi sender cannot impersonate another mailbox route".to_owned(),
            ))
        }
    }

    pub(crate) fn query_work(
        original_native_work: &mut Option<acyclic_fs::NativeMaterializationWork>,
    ) -> acyclic_harness::Result<acyclic_fs::NativeMaterializationWork> {
        if let Some(work) = original_native_work {
            return Ok(work.clone());
        }
        let work = acyclic_fs::NativeMaterializationWork::new(
            pi_root_provider::allocation_budget(),
            &CancellationToken::new(),
        )
        .map_err(|(cause, work)| RootError::NativeConstruction { cause, work })?;
        *original_native_work = Some(work.clone());
        Ok(work)
    }

    async fn mail_work(
        &mut self,
        caller: TaskId,
        owner: &pi_owner::PiOwnerConfig,
        query_work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<acyclic_fs::NativeMaterializationWork> {
        if caller == owner.policy().root_task {
            Ok(query_work.clone())
        } else {
            self.native_host_with_native_work(query_work)?
                .invocation_work(caller)
                .await
        }
    }

    async fn send_control(
        &mut self,
        caller: TaskId,
        arguments: &Value,
        owner: &pi_owner::PiOwnerConfig,
        kind: pi_mail::PiMailKind,
        query_work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<(Value, acyclic_fs::NativeAllocation)> {
        let recipient = self.alias_task(&string(arguments, "id").map_err(RootError::Invalid)?)?;
        self.authorize_mail(caller, recipient)?;
        let sdk_operation = string(arguments, "operation").map_err(RootError::Invalid)?;
        if sdk_operation.is_empty() || sdk_operation.len() > 512 {
            return Err(RootError::Invalid(
                "Pi mail requires its original bounded SDK operation".to_owned(),
            ));
        }
        let work = self.mail_work(caller, owner, query_work).await?;
        let operation =
            pi_fork::original_operation(owner.policy().root_task, caller, &sdk_operation, &work)?;
        let message = arguments
            .get("message")
            .and_then(Value::as_str)
            .ok_or_else(|| RootError::Invalid("Pi mail message must be a string".to_owned()))?;
        let _envelope_custody =
            work.claim(message.len() as u64, 0, acyclic_fs::WorkCounters::UNCHARGED)?;
        let envelope = pi_mail::PiMailEnvelope {
            kind,
            message: message.to_owned(),
        };
        if caller == owner.policy().root_task {
            let command = pi_mail::stage_send(
                owner,
                &self.filesystem,
                operation,
                caller,
                recipient,
                envelope,
                &work,
            )
            .await?;
            return self
                .command_host(&work, None)?
                .control_command_with_native_work(command.0)
                .await;
        }
        let binding = self.binding(caller)?;
        let host = self.runtime.runtime.task_host();
        let mut lease_io = acyclic_harness::store::HistoryWork::new(
            acyclic_harness::filesystem::PreparationIo::from_native_work(&work)?,
        );
        let reservation = host.recover_work(caller, &mut Some(&mut lease_io)).await?;
        let _reservation_custody = lease_io.into_custody()?;
        let lease = match reservation {
            acyclic_harness::distributed::WorkPull::Claimed(lease) => lease,
            acyclic_harness::distributed::WorkPull::Unresolved { .. } => {
                return Err(RootError::Invalid(
                    "Pi sender's original native attempt is unresolved".to_owned(),
                ));
            }
            acyclic_harness::distributed::WorkPull::Idle => {
                return Err(RootError::Invalid(
                    "Pi sender has no active original task".to_owned(),
                ));
            }
        };
        let fence = acyclic_harness::scheduler::LeaseFence::from(&lease.reservation);
        let journal_owner = host.journal_owner(binding.task_id, fence.clone()).await?;
        let context = self
            .runtime
            .runtime
            .task_context(caller, fence.clone())
            .await?;
        let command = pi_mail::prepare_send(
            &context,
            &journal_owner,
            owner,
            &self.filesystem,
            operation,
            recipient,
            envelope,
            |sender, destination| self.authorize_mail(sender, destination),
            &work,
        )
        .await?;
        match pi_mail::execute_send(
            &self.runtime.runtime,
            context,
            &journal_owner,
            fence,
            command.0,
            &work,
        )
        .await?
        {
            acyclic_harness::filesystem::TaskCommandProgress::ReadyWithNativeCustody {
                value,
                custody,
            } => Ok((value, custody)),
            _ => Err(RootError::Invalid(
                "Pi original mail acknowledgement is uncertain; no replacement was sent".to_owned(),
            )),
        }
    }

    fn authorize_subtree(&self, caller: TaskId, target: TaskId) -> acyclic_harness::Result<()> {
        let PluginTaskBinding::Pi(root) = &self.state.task_binding else {
            return Err(RootError::Invalid(
                "Pi task graph has no original root".to_owned(),
            ));
        };
        if target == root.task_id {
            return if caller == root.task_id {
                Ok(())
            } else {
                Err(RootError::Invalid(
                    "Pi child cannot control its parent".to_owned(),
                ))
            };
        }
        let mut route = self.state.routes.values().find(|route| {
            matches!(&route.task_binding, PluginTaskBinding::Pi(binding) if binding.task_id == target)
        }).ok_or_else(|| RootError::Invalid("Pi target has no retained admitted route".to_owned()))?;
        for _ in 0..=self.state.routes.len() {
            if matches!(&route.task_binding, PluginTaskBinding::Pi(binding) if binding.task_id == caller)
            {
                return Ok(());
            }
            if route.parent_agent_id == self.state.root_agent_id {
                return if caller == root.task_id {
                    Ok(())
                } else {
                    Err(RootError::Invalid(
                        "Pi caller cannot control a sibling or unrelated subtree".to_owned(),
                    ))
                };
            }
            route = self
                .state
                .routes
                .get(&route.parent_agent_id)
                .ok_or_else(|| RootError::Invalid("Pi target lineage is incomplete".to_owned()))?;
        }
        Err(RootError::Invalid(
            "Pi target lineage contains a cycle".to_owned(),
        ))
    }
}
