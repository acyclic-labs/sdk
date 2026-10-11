//! Native-host boundaries using the real filesystem, Stream and fork runtime.

use crate::{pi_fork::tests::Fixture, pi_native_host::PiNativeCommandHost, pi_owner::PiOwnerConfig};
use crate::pi_native::{PiNativeHostRuntime, PI_NATIVE_VERSION};
use acyclic_fs::CancellationToken;
use std::{collections::BTreeMap, path::PathBuf};

#[tokio::test]
async fn native_host_rejects_missing_original_pinned_bootstrap() {
    let fixture = Fixture::new().await;
    assert!(PiNativeCommandHost::new(fixture.runtime.clone(), fixture.filesystem.clone(),
        fixture.stream.clone(), fixture.resources.clone(), fixture.owner.clone()).is_err());
}

#[tokio::test]
async fn native_host_rejects_replacement_owner_before_opening_native_handles() {
    let fixture = Fixture::new().await;
    let replacement = PiOwnerConfig::new(fixture.owner.policy().clone()).unwrap();
    assert!(fixture.runtime.verify_owner(&replacement).is_err());
    assert!(PiNativeCommandHost::new(fixture.runtime.clone(), fixture.filesystem.clone(),
        fixture.stream.clone(), fixture.resources.clone(), replacement).is_err());
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
    let fixture = Fixture::new_with_native_runtime(Some(installed_runtime())).await;
    let host = PiNativeCommandHost::new(fixture.runtime.clone(), fixture.filesystem.clone(),
        fixture.stream.clone(), fixture.resources.clone(), fixture.owner.clone()).unwrap();
    let plan = fixture.plan().await;
    let prepared = fixture.coordinator().advance(&fixture.context, &fixture.journal, fixture.fence.clone(),
        &plan, fixture.source.clone(), crate::pi_root_provider::allocation_budget(),
        CancellationToken::new()).await.unwrap().unwrap();
    let task = prepared.binding.task_id;
    let expected_binding = prepared.binding.clone();
    let expected_snapshot = prepared.native_lazy;
    let expected_source = prepared.source.clone();
    host.register_prepared(prepared).await.unwrap();
    assert_eq!(host.binding(task), Some(expected_binding.clone()));
    assert_eq!(host.source(task), Some(expected_source));
    assert_eq!(host.live_workspace(task).unwrap().workspace().id(), expected_snapshot.workspace_id);
    assert!(host.live_view(task).is_none());
    assert_ne!(expected_binding.scratch.volume, expected_binding.results.unwrap().volume);
    assert!(host.cancel_child(task).await.unwrap().is_none());
    assert!(fixture.runtime.runtime.task_host().observe_task(task).await.unwrap().cancellation_requested);
    assert!(host.recover_child(task).await.unwrap().0.is_none());
    assert!(host.receipt(task).await.unwrap().is_none());
    assert!(host.output(task).await.unwrap().is_none());
    assert!(host.live_view(task).is_none());
}

#[tokio::test]
#[ignore = "requires the installed Pi 1.1.0 node, CLI and harness extension artifacts"]
async fn cold_locator_restores_original_sealed_child_without_native_dispatch() {
    use acyclic_harness::{distributed::Worker, filesystem::{TaskWorkerAttempt, TaskWorkerOutcome},
        scheduler::{LeaseFence, ResourceSnapshot}};
    use crate::pi_native_host::PiNativeColdLocator;
    use std::sync::Arc;
    let fixture = Fixture::new_with_native_runtime(Some(installed_runtime())).await;
    let plan = fixture.plan().await;
    let original = fixture.coordinator().advance(&fixture.context, &fixture.journal, fixture.fence.clone(),
        &plan, fixture.source.clone(), crate::pi_root_provider::allocation_budget(),
        CancellationToken::new()).await.unwrap().unwrap();
    let task = original.binding.task_id;
    let host = PiNativeCommandHost::new(fixture.runtime.clone(), fixture.filesystem.clone(),
        fixture.stream.clone(), fixture.resources.clone(), fixture.owner.clone()).unwrap();
    let root_key = original.binding.project_volumes.keys().next().unwrap().clone();
    host.register_cold_locator(task, PiNativeColdLocator { parent: plan.commit.caller,
        sdk_operation: plan.commit.sdk_operation.clone(), root_key: root_key.clone(),
        selected_source: fixture.source.clone() }).await.unwrap();
    assert!(host.register_cold_locator(task, PiNativeColdLocator { parent: plan.commit.caller,
        sdk_operation: format!("{}-replacement", plan.commit.sdk_operation), root_key,
        selected_source: fixture.source.clone() }).await.is_err());
    let worker = Worker { id: "pi-cold-native-owner".into(),
        available: ResourceSnapshot::default(), labels: BTreeMap::new() };
    let attempt = fixture.runtime.runtime.run_operation(&worker, task,
        &fixture.runtime.runtime.commands(), 1).await.unwrap().unwrap();
    let TaskWorkerAttempt::Progress(TaskWorkerOutcome::Yielded { lease }) = attempt
        else { panic!("original child workflow lease"); };
    let fence = LeaseFence::from(&lease.reservation);
    let owner = Arc::new(fixture.runtime.runtime.task_host().journal_owner(task, fence.clone()).await.unwrap());
    let context = fixture.runtime.runtime.task_context(task, fence.clone()).await.unwrap();
    host.hydrate_child(&context, &owner, fence, crate::pi_root_provider::allocation_budget(),
        &CancellationToken::new()).await.unwrap();
    assert_eq!(host.binding(task), Some(original.binding));
    assert_eq!(host.frozen_source(task), Some(original.frozen_source));
    assert_eq!(host.source_frozen(task), Some(original.native_lazy));
    assert_eq!(host.live_workspace(task).unwrap().workspace().id(), original.workspace.workspace().id());
    assert!(host.live_view(task).is_none());
    assert!(host.receipt(task).await.unwrap().is_none());
    assert!(host.output(task).await.unwrap().is_none());
}
