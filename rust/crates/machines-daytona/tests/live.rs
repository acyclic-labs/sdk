//! Live checks against a real Daytona organization. Both are ignored by default and need
//! `DAYTONA_API_KEY`; see the crate README for the commands.

#![allow(clippy::expect_used, clippy::panic, reason = "test assertions")]

use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use acyclic_machines_daytona::{
    DaytonaConfig, DaytonaProvider,
    api::{CreateSandboxRequest, DaytonaApi, ExecuteRequest},
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
            id,
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
    let mut config = DaytonaConfig::from_env().expect("DAYTONA_API_KEY must be set");
    // The suite commits every machine to network policy `[8; 32]`; run it with no egress.
    config.register_network_policy([8; 32], map::NetworkPolicy::BlockAll);
    assert!(
        config.default_snapshot.is_some(),
        "DAYTONA_SNAPSHOT must name a registered Linux VM snapshot"
    );
    let provider = DaytonaProvider::new(config).expect("provider builds from configuration");
    acyclic_conformance::machines(&provider)
        .await
        .expect("machines conformance suite passes");
}
