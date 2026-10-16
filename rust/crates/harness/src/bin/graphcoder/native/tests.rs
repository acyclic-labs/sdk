use super::*;
use acyclic_harness::{
    AgentId, Capabilities,
    conversation::{VolumeClass, VolumeOperation, VolumeOwner},
};
use std::{path::Path, time::Duration};

async fn configuration(root: &Path) -> Result<Config> {
    acyclic_native_runtime::initialize_native_io_services().map_err(storage)?;
    let provider = ProviderRef::new("restart".into(), "filesystem".into(), "1".into(), &mut None)?;
    let agent = AgentId::new();
    let private_volume = VolumeRef::new(
        provider.clone(),
        "private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    let authority = Authority {
        kind: acyclic_harness::core::AggregateKind::Conversation,
        id: "restart".into(),
    };
    let issuer = AuthorityIssuer::new("restart", [37; 32], authority.clone());
    #[derive(serde::Deserialize)]
    struct Scenario {
        grants: Vec<String>,
    }
    let mut grants = serde_json::from_str::<Scenario>(include_str!("../native_scenario/data.json"))
        .map_err(invalid)?
        .grants;
    grants.extend([
        private_volume.capability(VolumeOperation::Read, &mut None)?,
        private_volume.capability(VolumeOperation::Write, &mut None)?,
    ]);
    let scope = issuer.issue(
        "restart",
        &Capabilities::new(grants),
        Some(agent),
        None,
        &mut None,
    )?;
    let filesystem_root = root.join("filesystem");
    let files = FilesystemHost::new(
        acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(&filesystem_root))
            .await
            .map_err(storage)?,
        &provider,
        None,
    )?;
    files.create_volume(&private_volume).await?;
    Ok(Config {
        filesystem_root,
        stream_root: root.join("stream"),
        filesystem_provider: provider,
        stream_provider: ProviderRef::new(
            "restart".into(),
            "stream".into(),
            "1".into(),
            &mut None,
        )?,
        private_volume,
        owner: AuthorityBinding {
            authority,
            issuer: "restart".into(),
            key: [37; 32],
            scope,
        },
        generation: "initial".into(),
        model_invocation_ledger: root.join("invocations.jsonl"),
        limits: Limits::default(),
        run_limits: Default::default(),
        session_limits: SessionLimits {
            active_tasks: 1,
            total_tasks: 1,
            depth: 1,
            model_steps: 1,
        },
        concurrency: 1,
        deterministic_delay_ms: 5000,
        fork_sources: Vec::new(),
        tools: crate::native_tools::NativeToolsConfig {
            content_budget: acyclic_fs::performance::BOUNDARY_BUDGET,
            ..Default::default()
        },
        context_manifest: None,
        native_publications: Vec::new(),
        project_source_root: None,
    })
}

#[tokio::test]
async fn default_manifest_survives_restart_of_an_unfinished_native_turn() -> Result<()> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let config = configuration(directory.path()).await?;
    let first = NativeSession::open(config.clone()).await?;
    let contracts = first.context.contracts(None)?;
    let command = first
        .prompt_command("keep the plan in plans/release.md", OperationId::new())
        .await?;
    let task = match first.queue(OperationId::new(), command, None).await? {
        Admission::Accepted(RuntimeTask::Durable { task_id, .. }) => task_id,
        _ => panic!("expected an admitted durable task"),
    };
    let WorkPull::Claimed(lease) = first
        .claim(
            task,
            &Worker {
                id: "restart".into(),
                available: Default::default(),
                labels: Default::default(),
            },
        )
        .await?
    else {
        panic!("expected the original lease")
    };
    {
        let running = first.resume(lease);
        tokio::pin!(running);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                tokio::select! {
                    outcome = &mut running => panic!("turn completed before its delayed model: {outcome:?}"),
                    _ = tokio::time::sleep(Duration::from_millis(5)) => {},
                }
                if std::fs::read_to_string(&config.model_invocation_ledger).map_err(storage)?.contains("\"phase\":\"generate\"") { break; }
            }
            Ok::<_, Error>(())
        }).await.expect("model dispatch was never reached")?;
    }
    drop(first);
    let reopened = NativeSession::open(config.clone()).await?;
    assert_eq!(reopened.context.contracts(None)?, contracts);
    let WorkPull::Claimed(lease) = reopened
        .runtime
        .task_host()
        .recover_work(task, &mut None)
        .await?
    else {
        panic!("original lease was not retained")
    };
    let outcome = reopened.resume(lease).await?;
    assert!(outcome.starts_with("worker completed"), "{outcome}");
    let records: Vec<serde_json::Value> = std::fs::read_to_string(&config.model_invocation_ledger)
        .map_err(storage)?
        .lines()
        .map(serde_json::from_str)
        .collect::<std::result::Result<_, _>>()
        .map_err(invalid)?;
    assert_eq!(
        records
            .iter()
            .map(|record| record["phase"].as_str().expect("phase"))
            .collect::<Vec<_>>(),
        ["generate", "reconcile"]
    );
    assert_eq!(records[0]["request_digest"], records[1]["request_digest"]);
    Ok(())
}
