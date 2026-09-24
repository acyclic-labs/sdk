//! Live checks against a real Daytona organization. All are ignored by default and need
//! `DAYTONA_API_KEY`; see the crate README for the commands.

#![allow(clippy::expect_used, clippy::panic, reason = "test assertions")]

use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use acyclic_machines::{
    Budgets, Capability, CompatibilityPolicy, ExpirationPolicy, ForkFidelity, IdempotencyKey,
    Image, MachineContract, MachineId, MachinesProvider, MutationOutcome, Performance,
    SuspensionPolicy,
};
use acyclic_machines_daytona::{
    DaytonaConfig, DaytonaProvider,
    api::{CreateSandboxRequest, DaytonaApi, ExecuteRequest, Sandbox},
    map,
};

/// Smoke test of the REST client with a *container* snapshot (VM classes may not be enabled
/// for the organization). Creates one short-lived sandbox, runs a command through the toolbox,
/// and deletes it whatever happens. `DAYTONA_SMOKE_SNAPSHOT` (default `daytona-small`) and
/// `DAYTONA_REGION` (default `eu`) select the snapshot and region.
#[tokio::test]
#[ignore = "needs DAYTONA_API_KEY and creates a billable sandbox"]
async fn smoke_container_create_exec_delete() {
    let mut config = DaytonaConfig::from_env().expect("DAYTONA_API_KEY must be set");
    config.region.get_or_insert_with(|| "eu".to_owned());
    let api = DaytonaApi::new(&config).expect("client builds");
    let snapshot =
        std::env::var("DAYTONA_SMOKE_SNAPSHOT").unwrap_or_else(|_| "daytona-small".to_owned());
    let class = api
        .get_snapshot(&snapshot)
        .await
        .expect("snapshot is readable")
        .sandbox_class;
    eprintln!("snapshot {snapshot} boots class {class:?}");

    let body = CreateSandboxRequest {
        snapshot,
        target: config.region.clone(),
        labels: BTreeMap::from([("acyclic".to_owned(), "probe".to_owned())]),
        auto_stop_interval: Some(15),
        auto_delete_interval: Some(0),
        ttl_minutes: Some(30),
        ..CreateSandboxRequest::default()
    };
    let started = Instant::now();
    let created = api.create(&body).await.expect("create succeeds");
    let create_ms = started.elapsed().as_millis();
    let id = created.id.clone();
    let outcome = exercise(&api, &id).await;
    let started = Instant::now();
    let deleted = api.delete(&id).await;
    eprintln!(
        "create request {create_ms} ms, delete request {} ms",
        started.elapsed().as_millis()
    );
    deleted.expect("delete succeeds");
    outcome.unwrap_or_else(|error| panic!("{error}"));
}

async fn exercise(api: &DaytonaApi, id: &str) -> Result<(), String> {
    let started = Instant::now();
    let sandbox = loop {
        let sandbox = api.get(id).await.map_err(|error| error.to_string())?;
        if map::is_settled(sandbox.state.as_deref()) {
            break sandbox;
        }
        if started.elapsed() > Duration::from_secs(120) {
            return Err(format!("sandbox {id} did not settle: {:?}", sandbox.state));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    eprintln!(
        "settled as {:?} ({:?}) after {} ms",
        sandbox.state,
        map::machine_state(sandbox.state.as_deref()),
        started.elapsed().as_millis()
    );
    if sandbox.state.as_deref() != Some("started") {
        return Err(format!("sandbox {id} settled in {:?}", sandbox.state));
    }
    // The listing is documented as eventually consistent; give the index a moment.
    let probe = BTreeMap::from([("acyclic".to_owned(), "probe".to_owned())]);
    let started = Instant::now();
    loop {
        let listed = api
            .list(Some(&probe))
            .await
            .map_err(|error| error.to_string())?;
        if listed.iter().any(|item| item.id == id) {
            eprintln!(
                "label listing caught up after {} ms",
                started.elapsed().as_millis()
            );
            break;
        }
        if started.elapsed() > Duration::from_secs(30) {
            return Err("label-filtered listing never returned the sandbox".into());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let mut labels = sandbox.labels.clone();
    labels.insert("acyclic.smoke".to_owned(), "relabelled".to_owned());
    api.replace_labels(id, labels)
        .await
        .map_err(|error| error.to_string())?;
    let started = Instant::now();
    let output = api
        .execute(
            &sandbox,
            &ExecuteRequest {
                command: "echo acyclic-$((6*7))".into(),
                cwd: None,
                timeout: Some(30),
            },
        )
        .await
        .map_err(|error| error.to_string())?;
    eprintln!("exec {} ms: {output:?}", started.elapsed().as_millis());
    if output.exit_code != Some(0)
        || !output
            .result
            .as_deref()
            .unwrap_or_default()
            .contains("acyclic-42")
    {
        return Err(format!("unexpected exec output {output:?}"));
    }
    Ok(())
}

/// Full Machines conformance suite. Needs `DAYTONA_SNAPSHOT` naming a Linux VM snapshot for
/// the suite's `Image::custom([7; 32])`, and bills the organization.
#[tokio::test]
#[ignore = "needs DAYTONA_API_KEY, a Linux VM snapshot, and creates billable sandboxes"]
async fn conformance_suite_against_linux_vms() {
    let config = DaytonaConfig::from_env().expect("DAYTONA_API_KEY must be set");
    assert!(
        config.default_snapshot.is_some(),
        "DAYTONA_SNAPSHOT must name a registered Linux VM snapshot"
    );
    let provider = DaytonaProvider::new(config).expect("provider builds from configuration");
    acyclic_conformance::machines(&provider)
        .await
        .expect("machines conformance suite passes");
}

/// Container disk fork through the trait: one `daytona-small` parent with a few MB in its
/// workspace, forked into two children whose workspaces must match byte for byte. Three
/// sandboxes in total, each with a 30-minute TTL, 15-minute auto-stop, delete-on-stop, and the
/// `acyclic=probe` label; all are deleted whatever happens.
#[tokio::test]
#[ignore = "needs DAYTONA_API_KEY and creates three billable container sandboxes"]
async fn container_disk_fork_copies_the_workspace() {
    let mut config = DaytonaConfig::from_env().expect("DAYTONA_API_KEY must be set");
    config.region.get_or_insert_with(|| "eu".to_owned());
    let snapshot =
        std::env::var("DAYTONA_SMOKE_SNAPSHOT").unwrap_or_else(|_| "daytona-small".to_owned());
    let workspace = config.workspace_dir.clone();
    let provider = DaytonaProvider::new(config.clone()).expect("provider builds");
    let api = provider.api().clone();
    let create_key = IdempotencyKey::new();
    let contract = MachineContract {
        image: Image::custom([7; 32]).expect("digest"),
        capabilities: [Capability::DiskFork].into(),
        compatibility: CompatibilityPolicy::BestEffort,
        compatibility_revision: DaytonaProvider::revision(),
        performance: Performance::Elastic,
        suspension: SuspensionPolicy::Manual,
        expiration: ExpirationPolicy::MaxAge(Duration::from_secs(30 * 60)),
        network_policy_digest: [8; 32],
        budgets: Budgets::default(),
    };
    let mut labels = map::labels(create_key, map::KIND_CREATE, None, None, &contract);
    labels.insert("acyclic".to_owned(), "probe".to_owned());
    let parent = api
        .create(&CreateSandboxRequest {
            name: Some(map::sandbox_name(create_key, None)),
            snapshot,
            target: config.region.clone(),
            labels,
            auto_stop_interval: Some(15),
            auto_delete_interval: Some(0),
            ttl_minutes: Some(30),
            ..CreateSandboxRequest::default()
        })
        .await
        .expect("parent create succeeds");
    let mut created = vec![parent.id.clone()];
    let outcome = disk_fork(&provider, &api, &parent.id, &workspace, &mut created).await;
    for id in created.iter().rev() {
        if let Err(error) = api.delete(id).await {
            eprintln!("delete {id} failed: {error}");
        }
    }
    outcome.unwrap_or_else(|error| panic!("{error}"));
}

async fn settle(api: &DaytonaApi, id: &str) -> Result<Sandbox, String> {
    let started = Instant::now();
    loop {
        let sandbox = api.get(id).await.map_err(|error| error.to_string())?;
        if map::is_settled(sandbox.state.as_deref()) {
            return Ok(sandbox);
        }
        if started.elapsed() > Duration::from_secs(180) {
            return Err(format!("sandbox {id} did not settle: {:?}", sandbox.state));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn shell(api: &DaytonaApi, sandbox: &Sandbox, command: &str) -> Result<String, String> {
    let output = api
        .execute(
            sandbox,
            &ExecuteRequest {
                command: format!("sh -c '{command}'"),
                cwd: None,
                timeout: Some(120),
            },
        )
        .await
        .map_err(|error| error.to_string())?;
    if output.exit_code == Some(0) {
        Ok(output.result.unwrap_or_default().trim().to_owned())
    } else {
        Err(format!("{command:?} failed: {output:?}"))
    }
}

async fn disk_fork(
    provider: &DaytonaProvider,
    api: &DaytonaApi,
    parent_id: &str,
    workspace: &str,
    created: &mut Vec<String>,
) -> Result<(), String> {
    let parent = settle(api, parent_id).await?;
    let digest_command = format!("cd {workspace} && sha256sum blob marker");
    shell(
        api,
        &parent,
        &format!(
            "mkdir -p {workspace} && head -c 4194304 /dev/urandom > {workspace}/blob && echo forked > {workspace}/marker"
        ),
    )
    .await?;
    let expected = shell(api, &parent, &digest_command).await?;
    let machine = MachineId::parse(parent_id).map_err(|error| error.to_string())?;
    let started = Instant::now();
    let forked = provider
        .fork_machine(
            machine,
            std::num::NonZeroU32::new(2).expect("two"),
            IdempotencyKey::new(),
        )
        .await;
    let elapsed = started.elapsed();
    if let Ok(MutationOutcome::MachineForked { children, .. }) = &forked {
        created.extend(children.iter().map(|child| child.id.to_string()));
    }
    let MutationOutcome::MachineForked {
        source,
        fidelity,
        children,
    } = forked.map_err(|error| error.to_string())?
    else {
        return Err("fork_machine returned the wrong outcome".into());
    };
    eprintln!(
        "disk fork of a 4 MiB workspace into {} children: {} ms",
        children.len(),
        elapsed.as_millis()
    );
    if source != machine || fidelity != ForkFidelity::DiskOnly || children.len() != 2 {
        return Err(format!("unexpected fork outcome {source} {fidelity:?}"));
    }
    for child in &children {
        let sandbox = api
            .get(&child.id.to_string())
            .await
            .map_err(|error| error.to_string())?;
        let observed = shell(api, &sandbox, &digest_command).await?;
        if observed != expected {
            return Err(format!(
                "child workspace differs: {observed:?} vs {expected:?}"
            ));
        }
        let labels = &sandbox.labels;
        if labels.get("acyclic").map(String::as_str) != Some("probe")
            || labels.get(map::LABEL_PARENT) != Some(&parent_id.to_owned())
            || labels.get(map::LABEL_KIND).map(String::as_str) != Some(map::KIND_LIVE_FORK)
            || sandbox.auto_stop_interval != Some(15)
            || sandbox.auto_delete_interval != Some(0)
        {
            return Err(format!(
                "child lineage or lifecycle not inherited: {sandbox:?}"
            ));
        }
    }
    Ok(())
}
