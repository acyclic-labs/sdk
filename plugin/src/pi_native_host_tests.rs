
use crate::{pi_fork::tests::Fixture, pi_native_host::PiNativeCommandHost, pi_owner::PiOwnerConfig};
use crate::pi_native::{PiNativeHostRuntime, PI_NATIVE_VERSION};
use acyclic_fs::CancellationToken;
use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

fn native_host(fixture: &Fixture, replacement: Option<PiOwnerConfig>) -> acyclic_harness::Result<Arc<PiNativeCommandHost>> {
    let work = acyclic_fs::NativeMaterializationWork::new(crate::pi_root_provider::allocation_budget(), &CancellationToken::new())
        .map_err(|(cause, work)| acyclic_harness::Error::NativeConstruction { cause, work })?;
    let owner = match replacement {
        Some(owner) => owner,
        None => PiOwnerConfig::load_with_native_work(&fixture.private_root, &fixture.relative_session, &work)?,
    };
    PiNativeCommandHost::new(fixture.runtime.clone(), fixture.filesystem.clone(), fixture.stream.clone(), &fixture.resources, owner, &work)
}

#[tokio::test]
async fn native_host_rejects_missing_bootstrap_and_replacement_owner() {
    let fixture = Fixture::new(None).await;
    assert!(native_host(&fixture, None).is_err());
    let replacement = PiOwnerConfig::new(fixture.owner.policy(), None, &crate::pi_fork::tests::work()).unwrap();
    assert!(fixture.runtime.verify_owner(&replacement).is_err());
    assert!(native_host(&fixture, Some(replacement)).is_err());
}

fn required_path(name: &str) -> PathBuf {
    let path = PathBuf::from(std::env::var_os(name).unwrap_or_else(|| panic!("installed Pi lane requires {name}")));
    assert!(path.is_absolute(), "{name} must be absolute");
    path
}

fn installed_runtime() -> PiNativeHostRuntime {
    let executable = required_path("ACYCLIC_PI_TEST_NODE");
    let cli = required_path("ACYCLIC_PI_TEST_CLI");
    let package = cli.parent().and_then(std::path::Path::parent).and_then(std::path::Path::parent)
        .expect("Pi CLI must be the installed dist/bundle/cli.js").join("package.json");
    let package: serde_json::Value = serde_json::from_slice(&std::fs::read(package).unwrap()).unwrap();
    assert_eq!(package["name"], "@earendil-works/pi-coding-agent");
    assert_eq!(package["version"], PI_NATIVE_VERSION);
    PiNativeHostRuntime { version: PI_NATIVE_VERSION.into(), executable,
        executable_prefix: vec![cli.to_str().unwrap().into()],
        extension: required_path("ACYCLIC_PI_TEST_EXTENSION"), provider_extension: None,
        environment: std::env::vars().collect::<BTreeMap<_, _>>() }
        .sanitize().unwrap()
}

#[tokio::test]
#[ignore = "requires the installed Pi 1.1.0 node, CLI and harness extension artifacts"]
async fn actual_prepared_binding_and_prelaunch_cancel_never_create_native_handles() {
    let fixture = Fixture::new(Some(installed_runtime())).await;
    let host = native_host(&fixture, None).unwrap();
    let plan = fixture.plan(&crate::pi_fork::tests::work()).await;
    let prepared = fixture.advance_with(&fixture.coordinator(&crate::pi_fork::tests::work()).unwrap(), &fixture.context, &fixture.journal, &fixture.fence,
        &plan, fixture.source.clone(), crate::pi_root_provider::allocation_budget(),
        CancellationToken::new()).await.unwrap().unwrap();
    let task = prepared.binding.task_id;
    let expected_binding = prepared.binding.clone();
    let expected_snapshot = prepared.native_lazy;
    let original_work = prepared.original_work().clone();
    host.register_prepared(prepared).await.unwrap();
    let entry = host.find_entry(task).unwrap();
    assert!(entry.native_custody.work().shares_ledger(&original_work));
    assert_eq!(host.binding(task, &original_work).unwrap().unwrap().0, expected_binding);
    assert_eq!(host.live_workspace(task).unwrap().workspace().id(), expected_snapshot.workspace_id);
    assert!(host.live_view(task).is_none());
    for volume in expected_binding.project_volumes.values() {
        assert!(host.project_mount(task, volume).is_none());
        assert!(host.project_work(task, volume).is_none());
    }
    assert_ne!(expected_binding.scratch.volume, expected_binding.results.unwrap().volume);
    let cleanup = CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        crate::pi_root_provider::allocation_budget(), &cleanup,
    ).unwrap();
    assert!(host.cancel_child_with_native_work(task, &work).await.unwrap().is_none());
    assert!(fixture.runtime.runtime.task_host().observe_task(task).await.unwrap().cancellation_requested);
    assert!(host.recover_child(task).await.unwrap().0.is_none());
    assert!(host.live_view(task).is_none());
    for volume in expected_binding.project_volumes.values() {
        assert!(host.project_mount(task, volume).is_none());
        assert!(host.project_work(task, volume).is_none());
    }
}

#[tokio::test]
#[ignore = "requires the installed Pi 1.1.0 node, CLI and harness extension artifacts"]
async fn actual_registration_denial_never_allocates_a_native_entry() {
    let fixture = Fixture::new(Some(installed_runtime())).await;
    let host = native_host(&fixture, None).unwrap();
    let plan = fixture.plan(&crate::pi_fork::tests::work()).await;
    let prepared = fixture.advance_with(&fixture.coordinator(&crate::pi_fork::tests::work()).unwrap(), &fixture.context, &fixture.journal, &fixture.fence,
        &plan, fixture.source.clone(), crate::pi_root_provider::allocation_budget(),
        CancellationToken::new()).await.unwrap().unwrap();
    let task = prepared.binding.task_id;
    let root_key = prepared.binding.project_volumes.keys().next().unwrap();
    let mut budget = crate::pi_root_provider::allocation_budget();
    budget.allocation_operations = 1;
    let cancellation = CancellationToken::new();
    let denied = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation).unwrap();
    let error = host.register_cold_locator_with_native_work(task, plan.commit.caller,
        &plan.commit.sdk_operation, root_key, fixture.source.clone(), &denied).await.err().unwrap();
    assert!(matches!(error, acyclic_harness::Error::NativeWork {
        cause: acyclic_fs::NativeTreeMaterializationError::Work(
            acyclic_fs::WorkError::BudgetExceeded { counter: "allocation_operations", .. }), ..
    }));
    assert!(host.find_entry(task).is_none());
    let original = prepared.original_work().clone();
    original.cancellation().cancel();
    let error = host.register_prepared(prepared).await.err().unwrap();
    assert!(matches!(error, acyclic_harness::Error::NativeWork {
        cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, ..
    }));
    assert!(host.find_entry(task).is_none());
}

#[tokio::test]
#[ignore = "requires the installed Pi 1.1.0 node, CLI and harness extension artifacts"]
async fn cold_locator_restores_original_sealed_child_without_native_dispatch() {
    use acyclic_harness::{distributed::Worker, filesystem::{TaskWorkerAttempt, TaskWorkerOutcome},
        scheduler::{LeaseFence, ResourceSnapshot}};
    let fixture = Fixture::new(Some(installed_runtime())).await;
    let plan = fixture.plan(&crate::pi_fork::tests::work()).await;
    let original = fixture.advance_with(&fixture.coordinator(&crate::pi_fork::tests::work()).unwrap(), &fixture.context, &fixture.journal, &fixture.fence,
        &plan, fixture.source.clone(), crate::pi_root_provider::allocation_budget(),
        CancellationToken::new()).await.unwrap().unwrap();
    let task = original.binding.task_id;
    let host = native_host(&fixture, None).unwrap();
    let root_key = original.binding.project_volumes.keys().next().unwrap().clone();
    let query_cancellation = CancellationToken::new();
    let query_work = acyclic_fs::NativeMaterializationWork::new(
        crate::pi_root_provider::allocation_budget(), &query_cancellation,
    ).unwrap();
    host.register_cold_locator_with_native_work(task, plan.commit.caller,
        &plan.commit.sdk_operation, &root_key, fixture.source.clone(), &query_work).await.unwrap();
    let replacement_operation = format!("{}-replacement", plan.commit.sdk_operation);
    assert!(host.register_cold_locator_with_native_work(task, plan.commit.caller,
        &replacement_operation, &root_key, fixture.source.clone(), &query_work).await.is_err());
    let worker = Worker { id: "pi-cold-native-owner".into(),
        available: ResourceSnapshot::default(), labels: BTreeMap::new() };
    let mut attempt = fixture.runtime.runtime.run_operation_with_native_work(&worker, task,
        &fixture.runtime.runtime.commands(), 1, &query_work).await.unwrap();
    let Some(TaskWorkerAttempt::Progress(TaskWorkerOutcome::Yielded { lease })) = attempt.0.take()
        else { panic!("original child workflow lease"); };
    let fence = LeaseFence::from(&lease.reservation);
    let owner = fixture.runtime.runtime.task_host()
        .journal_owner_with_native_work(task, &fence, &query_work).await.unwrap()
        .into_native_shared(&query_work).unwrap();
    let context = fixture.runtime.runtime.task_context_with_native_work(task, &fence, &query_work).await.unwrap();
    host.hydrate_child(&context, &owner, &fence, crate::pi_root_provider::allocation_budget(),
        &query_cancellation, &query_work).await.unwrap();
    assert_eq!(host.binding(task, &query_work).unwrap().unwrap().0, original.binding);
    assert_eq!(host.frozen_source(task), Some(original.frozen_source));
    assert_eq!(host.source_frozen(task), Some(original.native_lazy));
    assert_eq!(host.live_workspace(task).unwrap().workspace().id(), original.workspace.workspace().id());
    assert!(host.live_view(task).is_none());
}
