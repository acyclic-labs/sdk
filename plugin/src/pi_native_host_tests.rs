use crate::pi_native::{PI_NATIVE_VERSION, PiNativeHostRuntime};
use crate::{
    pi_fork::{
        PiForkPlan, PreparedPiFork,
        tests::{Fixture, work},
    },
    pi_native_host::PiNativeCommandHost,
    pi_owner::PiOwnerConfig,
};
use acyclic_fs::CancellationToken;
use acyclic_harness::runtime::TaskStateProvider;
use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

fn native_host(fixture: &Fixture) -> acyclic_harness::Result<Arc<PiNativeCommandHost>> {
    let work = work();
    let owner = PiOwnerConfig::load_with_native_work(
        &fixture.private_root,
        &fixture.relative_session,
        &work,
    )?;
    PiNativeCommandHost::new(
        fixture.runtime.clone(),
        fixture.filesystem.clone(),
        fixture.stream.clone(),
        &fixture.resources,
        owner,
        &work,
    )
}

fn required_path(name: &str) -> PathBuf {
    let path = PathBuf::from(
        std::env::var_os(name).unwrap_or_else(|| panic!("installed Pi lane requires {name}")),
    );
    assert!(path.is_absolute());
    path
}

fn installed_runtime() -> PiNativeHostRuntime {
    let cli = required_path("ACYCLIC_PI_TEST_CLI");
    let package = cli
        .parent()
        .and_then(std::path::Path::parent)
        .and_then(std::path::Path::parent)
        .unwrap()
        .join("package.json");
    let package: serde_json::Value =
        serde_json::from_slice(&std::fs::read(package).unwrap()).unwrap();
    assert_eq!(package["name"], "@earendil-works/pi-coding-agent");
    assert_eq!(package["version"], PI_NATIVE_VERSION);
    PiNativeHostRuntime {
        version: PI_NATIVE_VERSION.into(),
        executable: required_path("ACYCLIC_PI_TEST_NODE"),
        executable_prefix: vec![cli.to_str().unwrap().into()],
        extension: required_path("ACYCLIC_PI_TEST_EXTENSION"),
        provider_extension: None,
        environment: std::env::vars().collect::<BTreeMap<_, _>>(),
    }
    .sanitize(&work())
    .unwrap()
}

async fn prepared_child() -> (
    Fixture,
    Arc<PiNativeCommandHost>,
    PiForkPlan,
    PreparedPiFork,
) {
    let fixture = Fixture::new(Some(installed_runtime())).await;
    let host = native_host(&fixture).unwrap();
    let plan = fixture.plan(&work()).await;
    let prepared = fixture
        .advance_with(
            &fixture.coordinator(&work()).unwrap(),
            &fixture.context,
            &fixture.journal,
            &fixture.fence,
            &plan,
            fixture.root_handle.workspace.clone(),
            crate::pi_root_provider::allocation_budget(),
            CancellationToken::new(),
        )
        .await
        .unwrap()
        .unwrap();
    (fixture, host, plan, prepared)
}

#[tokio::test]
async fn native_host_rejects_missing_bootstrap_and_replacement_owner() {
    let fixture = Fixture::new(None).await;
    assert!(native_host(&fixture).is_err());
    let replacement = PiOwnerConfig::new(fixture.owner.policy(), None, &work()).unwrap();
    assert!(
        fixture
            .runtime
            .verify_owner_with_native_work(&replacement, &work())
            .is_err()
    );
    assert!(
        PiNativeCommandHost::new(
            fixture.runtime.clone(),
            fixture.filesystem.clone(),
            fixture.stream.clone(),
            &fixture.resources,
            replacement,
            &work()
        )
        .is_err()
    );
}

#[tokio::test]
#[ignore = "requires the installed Pi 1.1.0 node, CLI and harness extension artifacts"]
async fn actual_prepared_binding_and_prelaunch_cancel_never_create_native_handles() {
    let (fixture, host, _plan, prepared) = prepared_child().await;
    let task = prepared.binding.task_id;
    let binding = prepared.binding.clone();
    let snapshot = prepared.native_lazy;
    let query = prepared._metadata_custody.work().clone();
    host.register_prepared(prepared).await.unwrap();
    assert_eq!(host.binding(task, &query).unwrap().unwrap().0, binding);
    assert_eq!(
        host.live_workspace(task).unwrap().workspace().id(),
        snapshot.workspace_id
    );
    assert_ne!(
        binding.scratch.volume,
        binding.results.as_ref().unwrap().volume
    );
    assert!(host.live_view(task).is_none());
    for volume in binding.project_volumes.values() {
        assert!(host.project_mount(task, volume).is_none());
    }
    assert!(
        host.cancel_child_with_native_work(task, &work())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        fixture
            .runtime
            .runtime
            .task_host()
            .observe_task(task)
            .await
            .unwrap()
            .cancellation_requested
    );
    assert!(host.recover_child(task).await.unwrap().0.is_none());
    assert!(host.live_view(task).is_none());
    for volume in binding.project_volumes.values() {
        assert!(host.project_mount(task, volume).is_none());
    }
}

#[tokio::test]
#[ignore = "requires the installed Pi 1.1.0 node, CLI and harness extension artifacts"]
async fn actual_registration_denial_never_allocates_a_native_entry() {
    let (fixture, host, plan, prepared) = prepared_child().await;
    let task = prepared.binding.task_id;
    let root = prepared.binding.project_volumes.keys().next().unwrap();
    let mut budget = crate::pi_root_provider::allocation_budget();
    budget.allocation_operations = 1;
    let denied =
        acyclic_fs::NativeMaterializationWork::new(budget, &CancellationToken::new()).unwrap();
    assert!(matches!(
        host.register_cold_locator_with_native_work(
            task,
            plan.commit.caller,
            &plan.commit.sdk_operation,
            root,
            fixture.root_handle.workspace.clone(),
            &denied
        )
        .await,
        Err(acyclic_harness::Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Work(
                acyclic_fs::WorkError::BudgetExceeded { .. }
            ),
            ..
        })
    ));
    assert!(host.find_entry(task).is_none());
    prepared._metadata_custody.work().cancellation().cancel();
    assert!(matches!(
        host.register_prepared(prepared).await,
        Err(acyclic_harness::Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
            ..
        })
    ));
    assert!(host.find_entry(task).is_none());
}

#[tokio::test]
#[ignore = "requires the installed Pi 1.1.0 node, CLI and harness extension artifacts"]
async fn cold_locator_restores_original_sealed_child_without_native_dispatch() {
    use acyclic_harness::{
        distributed::Worker,
        filesystem::{TaskWorkerAttempt, TaskWorkerOutcome},
        scheduler::LeaseFence,
    };
    let (fixture, host, plan, original) = prepared_child().await;
    let task = original.binding.task_id;
    let root = original.binding.project_volumes.keys().next().unwrap();
    let query = work();
    host.register_cold_locator_with_native_work(
        task,
        plan.commit.caller,
        &plan.commit.sdk_operation,
        root,
        fixture.root_handle.workspace.clone(),
        &query,
    )
    .await
    .unwrap();
    assert!(
        host.register_cold_locator_with_native_work(
            task,
            plan.commit.caller,
            &format!("{}-replacement", plan.commit.sdk_operation),
            root,
            fixture.root_handle.workspace.clone(),
            &query
        )
        .await
        .is_err()
    );
    let worker = Worker {
        id: "pi-cold-native-owner".into(),
        available: Default::default(),
        labels: BTreeMap::new(),
    };
    let mut attempt = {
        let io = acyclic_harness::filesystem::PreparationIo::from_native_work(&query).unwrap();
        let value = fixture
            .runtime
            .runtime
            .run_operation(
                &worker,
                task,
                &fixture.runtime.runtime.commands(),
                1,
                Some(&io),
            )
            .await
            .unwrap();
        let acyclic_harness::filesystem::PreparationIo::Native { custody, .. } = io else {
            unreachable!()
        };
        (
            value,
            custody
                .into_inner()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    };
    let Some(TaskWorkerAttempt::Progress(TaskWorkerOutcome::Yielded { lease })) = attempt.0.take()
    else {
        panic!("original child workflow lease")
    };
    let fence = LeaseFence::from(&lease.reservation);
    let owner = fixture
        .runtime
        .runtime
        .task_host()
        .journal_owner_with_native_work(task, &fence, &query)
        .await
        .unwrap()
        .into_native_shared(&query)
        .unwrap();
    let context = fixture
        .runtime
        .runtime
        .task_context_with_native_work(task, &fence, &query)
        .await
        .unwrap();
    host.hydrate_child(&context, &owner, &fence, query.cancellation(), &query)
        .await
        .unwrap();
    assert_eq!(
        host.binding(task, &query).unwrap().unwrap().0,
        original.binding
    );
    assert_eq!(host.frozen_source(task), Some(original.frozen_source));
    assert_eq!(host.source_frozen(task), Some(original.native_lazy));
    assert_eq!(
        host.live_workspace(task).unwrap().workspace().id(),
        original.workspace.workspace().id()
    );
    assert!(host.live_view(task).is_none());
}
