use super::*;
use crate::{pi_owner::PiOwnerPolicy, pi_root::PiRootIntent, pi_root_inputs::PiRootRequest};
use acyclic_harness::{
    conversation::{Limits, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    runtime::TaskRunLimits,
    scheduler::SessionLimits,
};
use std::path::PathBuf;

pub(crate) struct Fixture {
    pub(crate) root_handle: crate::pi_root_provider::PiRootHandle,
    pub(crate) context: acyclic_harness::runtime::TaskContext,
    pub(crate) journal: Arc<TaskJournalOwner<LocalStream>>,
    pub(crate) root_request: PiRootRequest,
    pub(crate) root_allocation: crate::pi_root_allocation::PiRootAllocation,
    _allocation: acyclic_fs::NativeAllocation,
    _service_lock: crate::service_process::ServiceLock,
    _directory: tempfile::TempDir,
}

impl Fixture {
    pub(crate) async fn new() -> Self {
        let root = crate::pi_root_provider::tests::Fixture::new(PiRootIntent::Project {
            context_id: [114; 16],
            root_id: [113; 16],
            paths: BTreeMap::new(),
        })
        .await;
        let acyclic_harness::effects::EffectStatus::Succeeded { result } =
            root.execute().await.unwrap().0
        else {
            panic!("original root allocation");
        };
        let allocation = root
            .provider
            .read_with_native_work::<crate::pi_root_allocation::PiRootAllocation>(
                &result,
                &root.startup,
            )
            .await
            .unwrap();
        let mut handles = root.provider.take_created_handles().await;
        let key = root.request.binding.project_volumes.keys().next().unwrap();
        let handle = handles.remove(key).unwrap();
        let task = root.request.binding.task_id;
        let fence = LeaseFence::from(&root.lease.reservation);
        let journal = Arc::new(
            root.runtime
                .runtime
                .task_host()
                .journal_owner(task, &(fence.clone()), None)
                .await
                .unwrap(),
        );
        let context = root
            .runtime
            .runtime
            .task_context(task, fence)
            .await
            .unwrap();
        Self {
            root_handle: handle,
            context,
            journal,
            root_request: root.request,
            root_allocation: allocation.0,
            _allocation: allocation.1,
            _service_lock: root._lock,
            _directory: root.directory,
        }
    }
}

pub(crate) fn policy() -> PiOwnerPolicy {
    let agent = AgentId::from_bytes([81; 16]);
    let provider = acyclic_harness::resources::ProviderRef::new(
        "pi-owner-test".into(),
        "filesystem".into(),
        "1".into(),
        &mut None,
    )
    .unwrap();
    let scratch = VolumeRef::new(
        provider.clone(),
        "scratch".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )
    .unwrap();
    PiOwnerPolicy {
        root_task: TaskId::from_bytes([82; 16]),
        agent,
        journal: VolumeRef::new(
            provider,
            "journal".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
            &mut None,
        )
        .unwrap(),
        grants: Capabilities::new([
            "operation:declare".to_owned(),
            "operation:observe".to_owned(),
            "operation:cancel".to_owned(),
            "operation:wake".to_owned(),
            "task:spawn:acyclic.pi@1".to_owned(),
            scratch
                .capability(VolumeOperation::Read, &mut None)
                .unwrap(),
            scratch
                .capability(VolumeOperation::Write, &mut None)
                .unwrap(),
        ]),
        limits: Limits {
            file_bytes: 65_536,
            path_bytes: 1024,
            attachments: 16,
            render_bytes: 65_536,
            model_steps: 16,
            model_events_per_step: 128,
            tool_calls_per_step: 16,
            context_messages: 128,
        },
        run_limits: TaskRunLimits {
            concurrency: Some(4),
            max_steps: Some(16),
            deadline_epoch_ms: None,
        },
        session_limits: SessionLimits {
            active_tasks: 4,
            total_tasks: 8,
            depth: 3,
            model_steps: 16,
        },
        concurrency: 4,
        maximum_payload_bytes: 65_536,
    }
}

pub(crate) fn work() -> acyclic_fs::NativeMaterializationWork {
    acyclic_fs::NativeMaterializationWork::new(
        crate::pi_root_provider::allocation_budget(),
        &CancellationToken::new(),
    )
    .unwrap()
}
