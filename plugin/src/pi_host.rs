//! Pi command orchestration over the SDK task and filesystem owners.
use crate::{
    AdapterState, PiTaskBinding, PluginTaskBinding, ServiceResources,
    pi_native_host::{PiNativeCommandHost, PiNativeFilesystem},
    pi_owner::PiOwnerConfig,
    pi_root_provider::PiRootHandle,
    pi_runtime::PiTaskRuntime,
    pi_selected_host::{SELECTED_DIAGNOSTIC_LABEL_BYTES, denied, invalid},
    pi_task::{PiTaskExecution, PiTaskInput},
};
use acyclic_harness::runtime::TaskStateProvider;
use acyclic_harness::{
    Error, Result, TaskId, durable_host::TaskJournalOwner, filesystem::PreparationIo,
    filesystem::TaskCommandProgress, runtime::TaskContext, scheduler::LeaseFence,
    store::HistoryWork, workflow::WorkflowCommand,
};
use acyclic_stream::LocalStream;
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};

pub(crate) struct PiControlHost<'a> {
    pub(crate) directory: &'a std::path::Path,
    pub(crate) runtime: Arc<PiTaskRuntime>,
    pub(crate) filesystem: Arc<PiNativeFilesystem>,
    pub(crate) stream: std::sync::Arc<LocalStream>,
    pub(crate) resources: &'a ServiceResources,
    pub(crate) protected: Arc<PiOwnerConfig>,
    pub(crate) retained: &'a AdapterState,
    pub(crate) roots: &'a BTreeMap<String, PiRootHandle>,
    pub(crate) native: Option<Arc<PiNativeCommandHost>>,
    pub(crate) root_provider:
        tokio::sync::Mutex<Option<Arc<crate::pi_root_provider::PiRootProvider>>>,
    pub(crate) root_source: Option<&'a crate::pi_fork_host::PiRootSource>,
    pub(crate) work: acyclic_fs::NativeMaterializationWork,
    pub(crate) pending:
        std::sync::Mutex<Option<(Vec<crate::PendingSpawn>, acyclic_fs::NativeAllocation)>>,
    pub(crate) prepared: std::sync::Mutex<
        Option<(
            Vec<crate::pi_fork::PreparedPiFork>,
            acyclic_fs::NativeAllocation,
        )>,
    >,
}

impl<'a> PiControlHost<'a> {
    #[allow(
        clippy::too_many_arguments,
        reason = "original providers and invocation work are independent"
    )]
    pub(crate) fn new(
        directory: &'a std::path::Path,
        runtime: Arc<PiTaskRuntime>,
        filesystem: Arc<PiNativeFilesystem>,
        stream: std::sync::Arc<LocalStream>,
        resources: &'a ServiceResources,
        protected: Arc<PiOwnerConfig>,
        retained: &'a AdapterState,
        roots: &'a BTreeMap<String, PiRootHandle>,
        native: Option<Arc<PiNativeCommandHost>>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        runtime.verify_owner_with_native_work(&protected, work)?;
        Ok(Self {
            directory,
            runtime,
            filesystem,
            stream,
            resources,
            protected,
            retained,
            roots,
            native,
            root_provider: tokio::sync::Mutex::new(None),
            root_source: None,
            work: work.clone(),
            pending: std::sync::Mutex::new(None),
            prepared: std::sync::Mutex::new(None),
        })
    }
    pub(crate) fn native(&self) -> Result<&Arc<PiNativeCommandHost>> {
        self.native
            .as_ref()
            .ok_or_else(|| denied("Pi control has no original native host"))
    }
    pub(crate) fn root_binding(&self) -> Result<&PiTaskBinding> {
        match &self.retained.task_binding {
            PluginTaskBinding::Pi(binding)
                if binding.task_id == self.protected.policy().root_task =>
            {
                Ok(binding)
            }
            _ => Err(denied(
                "selected Root binding differs from the original protected allocation",
            )),
        }
    }
    pub(crate) fn project_key<'b>(
        &self,
        binding: &'b PiTaskBinding,
        requested: Option<&str>,
    ) -> Result<&'b str> {
        match requested {
            Some(key) if binding.project_volumes.contains_key(key) => Ok(binding
                .project_volumes
                .get_key_value(key)
                .expect("known project")
                .0
                .as_str()),
            Some(_) => Err(denied(
                "selected project is not in the original source binding",
            )),
            None if binding.project_volumes.len() == 1 => Ok(binding
                .project_volumes
                .keys()
                .next()
                .expect("one project")
                .as_str()),
            None => Err(invalid(
                "select an explicit root when the source does not have exactly one project",
            )),
        }
    }
    pub(crate) async fn caller(
        &self,
        context: &TaskContext,
        fence: &LeaseFence,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(
        Arc<TaskJournalOwner<LocalStream>>,
        PiTaskInput,
        acyclic_fs::NativeAllocation,
    )> {
        self.runtime
            .verify_owner_with_native_work(&self.protected, work)?;
        let task = context
            .durable_task_id()
            .ok_or_else(|| denied("Pi control has no original task"))?;
        let mut admission = self
            .runtime
            .runtime
            .task_host()
            .observe_admission_with_native_work(task, work)
            .await?;
        if admission.0.task_id() != task
            || admission.0.task != *self.runtime.definition.identity()
            || context.scope().grants() != &admission.0.grants
            || context.scope().limits() != admission.0.limits
            || context.scope().run_limits() != admission.0.run_limits
            || context.owned_task_fence()? != *fence
        {
            return Err(denied(
                "Pi control differs from its original admission or fence",
            ));
        }
        let maximum = self
            .protected
            .policy()
            .maximum_payload_bytes
            .min(admission.0.limits.file_bytes);
        let bytes = crate::pi_root_provider::canonical_pi_value(&admission.0.input, maximum, work)?;
        let mut input = crate::pi_root_provider::guarded_pi_decode::<PiTaskInput>(
            &bytes.0,
            maximum,
            self.protected.policy().maximum_payload_bytes,
            acyclic_harness::wire::ErrorCode::Invalid,
            acyclic_harness::JsonValueSchema::None,
            crate::pi_task::PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES,
            work,
        )?;
        if input.0.binding.task_id != task {
            return Err(denied("Pi control substituted its admitted task binding"));
        }
        let owner = self
            .runtime
            .runtime
            .task_host()
            .journal_owner_with_native_work(task, fence, work)
            .await?
            .with_invocation_cancellation(work.cancellation())
            .into_native_shared(work)?;
        owner
            .verify(
                false,
                &mut Some(&mut HistoryWork::new(PreparationIo::from_native_work(
                    work,
                )?)),
            )
            .await?;
        if task == self.protected.policy().root_task
            && !matches!(&input.0.execution, PiTaskExecution::Host { .. })
        {
            return Err(denied("selected native Root became a child"));
        }
        admission.1.absorb(&mut input.1)?;
        Ok((owner, input.0, admission.1))
    }
    pub(crate) async fn admission_input(
        &self,
        task: TaskId,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(PiTaskInput, acyclic_fs::NativeAllocation)> {
        let admission = self
            .runtime
            .runtime
            .task_host()
            .observe_admission_with_native_work(task, work)
            .await?;
        if admission.0.task_id() != task {
            return Err(denied(
                "selected metadata substituted its actual admission task",
            ));
        }
        let maximum = self
            .protected
            .policy()
            .maximum_payload_bytes
            .min(crate::MAXIMUM_ADAPTER_STATE_BYTES) as u64;
        let encoded = crate::pi_root_provider::guarded_pi_bytes(&admission.0.input, maximum, work)?;
        drop(admission);
        let result = crate::pi_root_provider::guarded_pi_decode(
            &encoded.0,
            maximum,
            maximum
                .checked_mul(8)
                .ok_or_else(|| invalid("admission ownership bound overflow"))?,
            acyclic_harness::wire::ErrorCode::Invalid,
            acyclic_harness::JsonValueSchema::None,
            SELECTED_DIAGNOSTIC_LABEL_BYTES,
            work,
        );
        drop(encoded);
        result
    }

    pub(crate) async fn run(
        &self,
        context: Arc<TaskContext>,
        fence: LeaseFence,
        command: WorkflowCommand,
        payload: Value,
        reconcile: bool,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<TaskCommandProgress> {
        if matches!(
            command.kind.as_str(),
            crate::pi_builtin::PI_BUILTIN_COMMAND_KIND
                | crate::pi_operator_host::PI_OPERATOR_COMMAND_KIND
        ) && !self.work.shares_ledger(work)
        {
            return Err(denied("Pi control substituted its original native work"));
        }
        match command.kind.as_str() {
            crate::pi_fork_host::PI_FORK_COMMAND_KIND => {
                self.fork(context, fence, command, payload, reconcile, work)
                    .await
            }
            crate::pi_fork_host::PI_OBSERVE_SOURCE_COMMAND_KIND => {
                self.observe_source(context, fence, command, payload, work)
                    .await
            }
            crate::pi_operator_host::PI_OPERATOR_COMMAND_KIND => {
                self.run_operator(context, fence, command, payload, reconcile, work)
                    .await
            }
            crate::pi_builtin::PI_BUILTIN_COMMAND_KIND => {
                self.run_builtin(context, fence, command, payload).await
            }
            _ => {
                self.run_selected(context, fence, command, payload, reconcile, work)
                    .await
            }
        }
    }
}
