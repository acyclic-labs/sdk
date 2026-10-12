use super::*;
use acyclic_harness::runtime::TaskStateProvider;
use crate::pi_fork::tests::Fixture;

#[tokio::test]
async fn created_native_catalog_requires_original_alias_operation_uuid_and_creator() {
    let fixture = Fixture::new(None).await;
    let plan = fixture.plan(&crate::pi_fork::tests::work()).await;
    let prepared = fixture.advance_with(&fixture.coordinator(&crate::pi_fork::tests::work()).unwrap(), &fixture.context, &fixture.journal,
        &fixture.fence, &plan, fixture.root_handle.workspace.clone(),
        pi_root_provider::allocation_budget(), CancellationToken::new())
        .await.unwrap().unwrap();
    let key = prepared.binding.project_volumes.keys().next().unwrap();
    let root_id = crate::decode_fixed(key).unwrap();
    let route = Route {
        task_binding: PluginTaskBinding::Pi(Box::new(prepared.binding.clone())),
        agent_id: plan.commit.alias.clone(), turn_id: plan.commit.sdk_operation.clone(),
        context_id: pi_fork::PiForkOperations::new(plan.original, &crate::pi_fork::tests::work()).unwrap().fork.into_bytes(),
        root_id, parent_agent_id: fixture.root_request.binding.native_session_id.clone(),
        roots: BTreeMap::from([(key.clone(), RouteRoot {
            root_id, repository_workspace_id: prepared.workspace.workspace().id().into_bytes(),
            published_generation: prepared.native_lazy.authored_generation.digest().into_bytes(),
        })]),
        mount_path: pi_native::planned_native_paths(&fixture.resources,
            &fixture.root_request.binding.native_session_id, prepared.binding.task_id,
            Some(key), &crate::pi_fork::tests::work()).unwrap().0.view_root,
        lifecycle: RouteLifecycle::Active,
    };
    validate_created_native_route(&route, fixture.journal.task_id(), &plan).unwrap();
    validate_created_native_report(&route, &prepared).unwrap();

    let mut changed = route.clone();
    changed.agent_id = prepared.binding.native_session_id.clone();
    assert!(validate_created_native_route(&changed, fixture.journal.task_id(), &plan).is_err());
    let mut changed = route.clone();
    changed.turn_id.push_str("-replacement");
    assert!(validate_created_native_route(&changed, fixture.journal.task_id(), &plan).is_err());
    let mut changed = route.clone();
    let PluginTaskBinding::Pi(binding) = &mut changed.task_binding else { unreachable!(); };
    binding.native_session_id.push_str("-replacement");
    assert!(validate_created_native_route(&changed, fixture.journal.task_id(), &plan).is_err());
    let mut changed = route.clone();
    let PluginTaskBinding::Pi(binding) = &mut changed.task_binding else { unreachable!(); };
    binding.results.as_mut().unwrap().volume = binding.scratch.volume.clone();
    assert!(validate_created_native_route(&changed, fixture.journal.task_id(), &plan).is_err());
    let mut changed = route;
    changed.roots.values_mut().next().unwrap().repository_workspace_id = [99; 16];
    assert!(validate_created_native_report(&changed, &prepared).is_err());
    assert!(validate_created_native_route(&changed, prepared.binding.task_id, &plan).is_err());
}

#[tokio::test]
async fn cold_child_observation_preserves_original_cut_and_dispatch_is_only_initial_and_unreserved() {
    let fixture = Fixture::new(None).await;
    let plan = fixture.plan(&crate::pi_fork::tests::work()).await;
    let prepared = fixture.advance_with(&fixture.coordinator(&crate::pi_fork::tests::work()).unwrap(), &fixture.context, &fixture.journal,
        &fixture.fence, &plan, fixture.root_handle.workspace.clone(),
        pi_root_provider::allocation_budget(), CancellationToken::new())
        .await.unwrap().unwrap();
    let task = prepared.binding.task_id;
    assert!(matches!(fixture.runtime.runtime.task_host().recover_work(task).await.unwrap(),
        WorkPull::Idle));
    let observed = fixture.coordinator(&crate::pi_fork::tests::work()).unwrap().observe_prepared(&plan, fixture.root_handle.workspace.clone(),
        pi_root_provider::allocation_budget(), CancellationToken::new())
        .await.unwrap().unwrap();
    assert_eq!(observed.binding, prepared.binding);
    assert_eq!(observed.report, prepared.report);
    assert_eq!(observed.native_lazy, prepared.native_lazy);
    assert!(matches!(fixture.runtime.runtime.task_host().recover_work(task).await.unwrap(),
        WorkPull::Idle));
    assert!(fixture.runtime.runtime.task_host().outcome(task).await.unwrap().is_none());
    assert!(fixture.coordinator(&crate::pi_fork::tests::work()).unwrap().observe_prepared(&plan, prepared.workspace.clone(),
        pi_root_provider::allocation_budget(), CancellationToken::new()).await.is_err());
    let cancellation = CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        pi_root_provider::allocation_budget(), &cancellation).unwrap();
    assert!(original_native_first_dispatch_pending(&fixture.runtime, &fixture.stream,
        &fixture.owner, &plan, &work).await.unwrap());
    let worker = acyclic_harness::distributed::Worker {
        id: "lifecycle-original-child-owner".to_owned(),
        available: acyclic_harness::scheduler::ResourceSnapshot::default(),
        labels: BTreeMap::new(),
    };
    fixture.runtime.runtime.run_operation_with_native_work(&worker, prepared.binding.task_id,
        &fixture.runtime.runtime.commands(), 1, &work).await.unwrap();
    assert!(!original_native_first_dispatch_pending(&fixture.runtime, &fixture.stream,
        &fixture.owner, &plan, &work).await.unwrap());
    fixture.runtime.runtime.task_host().cancel(prepared.binding.task_id).await.unwrap();
    assert!(!original_native_first_dispatch_pending(&fixture.runtime, &fixture.stream,
        &fixture.owner, &plan, &work).await.unwrap());
}
