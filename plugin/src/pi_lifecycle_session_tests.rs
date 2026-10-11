use super::*;
use crate::pi_fork::tests::Fixture;

fn created_route(fixture: &Fixture, plan: &pi_fork::PiForkPlan,
    prepared: &pi_fork::PreparedPiFork) -> Route {
    let key = prepared.binding.project_volumes.keys().next().unwrap();
    let root_id = crate::decode_fixed(key).unwrap();
    Route {
        task_binding: PluginTaskBinding::Pi(Box::new(prepared.binding.clone())),
        agent_id: plan.commit.alias.clone(), turn_id: plan.commit.sdk_operation.clone(),
        context_id: pi_fork::PiForkOperations::new(plan.original).fork.into_bytes(),
        root_id, parent_agent_id: fixture.root_request.binding.native_session_id.clone(),
        roots: BTreeMap::from([(key.clone(), RouteRoot {
            root_id, repository_workspace_id: prepared.workspace.workspace().id().into_bytes(),
            published_generation: prepared.native_lazy.authored_generation.digest().into_bytes(),
        })]),
        mount_path: pi_native::planned_native_paths(&fixture.resources,
            &fixture.root_request.binding.native_session_id, prepared.binding.task_id,
            Some(key)).unwrap().view_root,
        lifecycle: RouteLifecycle::Active,
    }
}

#[tokio::test]
async fn created_native_catalog_requires_original_alias_operation_uuid_and_creator() {
    let fixture = Fixture::new().await;
    let plan = fixture.plan().await;
    let prepared = fixture.advance_with(&fixture.coordinator(), &fixture.context, &fixture.journal,
        fixture.fence.clone(), &plan, fixture.source.clone(),
        pi_root_provider::allocation_budget(), CancellationToken::new())
        .await.unwrap().unwrap();
    let route = created_route(&fixture, &plan, &prepared);
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
async fn cold_source_observation_retains_original_cut_without_claiming_child_work() {
    let fixture = Fixture::new().await;
    let plan = fixture.plan().await;
    let prepared = fixture.advance_with(&fixture.coordinator(), &fixture.context, &fixture.journal,
        fixture.fence.clone(), &plan, fixture.source.clone(),
        pi_root_provider::allocation_budget(), CancellationToken::new())
        .await.unwrap().unwrap();
    let task = prepared.binding.task_id;
    assert!(matches!(fixture.runtime.runtime.task_host().recover_work(task).await.unwrap(),
        WorkPull::Idle));
    let observed = fixture.coordinator().observe_prepared(&plan, fixture.source.clone(),
        pi_root_provider::allocation_budget(), CancellationToken::new())
        .await.unwrap().unwrap();
    assert_eq!(observed.binding, prepared.binding);
    assert_eq!(observed.report, prepared.report);
    assert_eq!(observed.native_lazy, prepared.native_lazy);
    assert!(matches!(fixture.runtime.runtime.task_host().recover_work(task).await.unwrap(),
        WorkPull::Idle));
    assert!(fixture.runtime.runtime.task_host().outcome(task).await.unwrap().is_none());
    assert!(fixture.coordinator().observe_prepared(&plan, Arc::new(prepared.workspace.clone()),
        pi_root_provider::allocation_budget(), CancellationToken::new()).await.is_err());
}

#[tokio::test]
async fn cold_created_child_dispatch_is_only_initial_and_unreserved() {
    let fixture = Fixture::new().await;
    let plan = fixture.plan().await;
    let prepared = fixture.advance_with(&fixture.coordinator(), &fixture.context, &fixture.journal,
        fixture.fence.clone(), &plan, fixture.source.clone(),
        pi_root_provider::allocation_budget(), CancellationToken::new())
        .await.unwrap().unwrap();
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
    fixture.runtime.runtime.run_operation(&worker, prepared.binding.task_id,
        &fixture.runtime.runtime.commands(), 1).await.unwrap().unwrap();
    assert!(!original_native_first_dispatch_pending(&fixture.runtime, &fixture.stream,
        &fixture.owner, &plan, &work).await.unwrap());
    fixture.runtime.runtime.task_host().cancel(prepared.binding.task_id).await.unwrap();
    assert!(!original_native_first_dispatch_pending(&fixture.runtime, &fixture.stream,
        &fixture.owner, &plan, &work).await.unwrap());
}

#[test]
fn independent_drain_failures_never_replace_the_original_failure() {
    let mut failure = None;
    retain_first_failure(&mut failure, Err(Error::Storage("original provider failure".to_owned())));
    retain_first_failure(&mut failure, Err(Error::Invalid("later scratch cleanup failure".to_owned())));
    retain_first_failure(&mut failure, Ok(()));
    assert!(matches!(finish_native_drain(failure),
        Err(Error::Storage(message)) if message == "original provider failure"));
}
