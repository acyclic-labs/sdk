//! The registered bash tool reaches the same scoped process boundary as effects.
use super::*;
use acyclic_harness::{
    model::{ModelContentPart, ModelEvent, ToolCall, ToolResultContent},
    test_support::ScriptedEvent,
};
use acyclic_native_runtime::{ProcessCaptureFailure, ProcessOutput};
use driver::{Driver, Outcome};
use std::{process::ExitStatus, time::Duration};

fn exited() -> ExitStatus {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        ExitStatus::from_raw(0)
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::ExitStatusExt;
        ExitStatus::from_raw(0)
    }
}

fn world<'a>(directory: &'a Path) -> EntryFuture<'a, Result<Trace>> {
    entry(async move {
        let mut driver = Driver::with_process(71, |request, check, observe| {
            assert_eq!(request.command.get_args().last().unwrap(), "printf native");
            assert!(request.command.get_current_dir().unwrap().is_absolute());
            assert_eq!(request.timeout, Duration::from_secs(300));
            assert!(request.maximum >= b"simulated bash".len());
            assert!(request.input.is_none());
            let fail = |error| ProcessCaptureFailure {
                error,
                status: None,
                stdout: Vec::new(),
                stderr: Vec::new(),
                cleanup_completed: true,
            };
            check().map_err(fail)?;
            acyclic_native_runtime::testing::blocking_on(tokio::time::sleep(
                Duration::from_millis(2),
            ));
            let action = observe(ProcessOutput::Stdout(b"simulated bash")).map_err(fail)?;
            assert!(action.bytes.is_empty());
            assert!(!action.complete);
            check().map_err(fail)?;
            Ok(std::process::Output {
                status: exited(),
                stdout: b"simulated bash".to_vec(),
                stderr: Vec::new(),
            })
        });
        let model = Model::new(
            "scripted".into(),
            "dst".into(),
            "1".into(),
            serde_json::Value::Null,
        )?;
        let provider = Arc::new(ScriptedModelProvider::new([
            ScriptedTurn::Events(vec![
                ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
                    "bash-0".into(),
                    "bash".into(),
                    serde_json::json!({"command":"printf native"}),
                ))),
                ScriptedEvent::Event(ModelEvent::Completed {
                    metadata: serde_json::json!({}),
                }),
            ]),
            ScriptedTurn::Text("done".into()),
        ]));
        let configuration =
            local_harness_config(directory.join("checkout"), (model, provider.clone()))?
                .state_directory(directory.join("state"));
        let operation = entry(async {
            let harness = Arc::new(entry(LocalHarness::open(configuration)).await?);
            let jsonl = prompt(&harness, "exercise bash", "done", true).await?;
            let files = harness
                .task_files()
                .observe(acyclic_harness::task_volume::Query::Root, None, 4096)
                .await
                .map_err(storage)?
                .snapshot
                .expect("missing task snapshot")
                .files;
            Ok::<_, Error>((jsonl, serde_json::to_vec(&files).map_err(storage)?))
        });
        let Outcome::Completed(result) = driver.run(operation).await else {
            panic!("uncut bash turn crashed");
        };
        let (jsonl, files) = result?;
        let requests = provider.requests();
        assert_eq!(
            requests.len(),
            2,
            "bash failed to settle into the next model step"
        );
        let results = requests[1]
            .messages
            .iter()
            .flat_map(|message| message.content.parts())
            .filter_map(|part| match part {
                ModelContentPart::ToolResult {
                    content: ToolResultContent::Json { value },
                    ..
                } => Some(value),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(results.len(), 1, "bash effect was lost or duplicated");
        assert_eq!(results[0]["stdout"], "simulated bash");
        assert_eq!(results[0]["success"], true);
        assert_eq!(results[0]["exit_code"], 0);
        assert_eq!(
            driver
                .schedule
                .trace()
                .iter()
                .filter(|event| event.starts_with("process:"))
                .count(),
            1
        );
        Ok(Trace {
            polls: driver.schedule.polls(),
            boundaries: driver.boundaries,
            writes: driver.schedule.durable_writes(),
            requests: serde_json::to_vec(&requests).map_err(storage)?,
            dispatches: serde_json::to_vec(&provider.dispatches()).map_err(storage)?,
            files,
            jsonl,
            durable: durable_bytes(&directory.join("state"))?,
        })
    })
}

#[test]
fn default_bash_uses_the_seeded_process_provider() -> Result<()> {
    digest::startup_value();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(storage)?;
    runtime.block_on(async {
        tokio::time::pause();
        let directory = tempfile::tempdir().map_err(storage)?;
        std::fs::create_dir(directory.path().join("checkout")).map_err(storage)?;
        let first = world(directory.path()).await?;
        // Reuse the external checkout's held-path identity. Both agent stores,
        // providers, schedules and task trees are independently initialized.
        std::fs::remove_dir_all(directory.path().join("state")).map_err(storage)?;
        let mounts = directory.path().join("checkout/.volumes");
        if mounts.exists() {
            std::fs::remove_dir_all(mounts).map_err(storage)?;
        }
        let second = world(directory.path()).await?;
        same_trace(&first, &second).expect("seeded bash replay diverged");
        // Cached process entropy passes independent in-process replays; the
        // qualification's second process must reject that same digest mutant.
        assert_eq!(digest::pair(&first), digest::pair(&second));
        Ok(())
    })
}
