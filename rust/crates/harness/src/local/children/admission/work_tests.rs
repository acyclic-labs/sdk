use super::*;
use crate::{
    client::{CommandResolution, CommandTool},
    model::Model,
    test_support::{ScriptedModelProvider, ScriptedTurn},
};
use acyclic_test_support::differential;

async fn fixture() -> (tempfile::TempDir, LocalHarness, FileValue) {
    let directory = tempfile::tempdir().unwrap();
    let provider = Arc::new(
        ScriptedModelProvider::new([ScriptedTurn::Text("delegated answer".into())])
            .with_context_capacity(crate::context::ModelContextCapacity {
                context_tokens: 32_768,
                output_tokens: 4096,
            }),
    );
    let model = Model::new(
        "scripted".into(),
        "admission-work".into(),
        "1".into(),
        serde_json::Value::Null,
    )
    .unwrap();
    let harness = LocalHarness::open(
        crate::test_support::local_harness_config(directory.path(), (model, provider)).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        harness
            .dispatch_command(
                OperationId::new(),
                None,
                CommandResolution::Tool {
                    tool: CommandTool::Spawn,
                    input: serde_json::json!({"task":"delegate this task","name":"delegate"})
                }
            )
            .await
            .unwrap(),
        serde_json::json!({"agent":"delegate"})
    );
    let request = harness
        .task_files
        .inner
        .child_request(&crate::task_volume::Component::new("delegate").unwrap())
        .await
        .unwrap();
    (directory, harness, request)
}

fn workspace_work(trace: &[(&str, u64)]) -> [u64; 2] {
    ["view-items", "view-allocations"].map(|kind| {
        trace
            .iter()
            .filter(|(operation, _)| *operation == kind)
            .map(|(_, amount)| amount)
            .sum()
    })
}

fn no_view_work(trace: &[(&str, u64)], assert_property: bool) {
    if assert_property {
        assert_eq!(
            workspace_work(trace),
            [0, 0],
            "controller reopened an unused child workspace: {trace:?}"
        );
    }
}

#[test]
fn replay_admission_preserves_results_and_errors_with_zero_workspace_work() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let pair = differential::pair_of(
        &["read", "verify", "retain", "view-items", "view-allocations"],
        || runtime.block_on(fixture()),
        |(_directory, harness, request), old| {
            runtime.block_on(async {
                let child = if old {
                    harness
                        .admit_child_request(&harness.task_files, &request)
                        .await
                        .unwrap()
                        .0
                } else {
                    harness
                        .admit_child_task(&harness.task_files, &request)
                        .await
                        .unwrap()
                };
                let record = harness
                    .runtime
                    .task_host()
                    .observe_admission(child)
                    .await
                    .unwrap();
                assert_eq!(record.parent, request.requester());
                let reference = crate::task_volume::Component::new("delegate").unwrap();
                let result = harness
                    .task_files
                    .inner
                    .snapshot(
                        &Query::File(FileKey::Child {
                            reference,
                            file: ChildFile::Result(1),
                        }),
                        1,
                    )
                    .await
                    .unwrap();
                let answer =
                    crate::task_volume::ChildTurnResult::parse(&result.files[0].bytes).unwrap();
                assert_eq!(answer.task, child);
                let mut forged = request;
                forged.bytes = acyclic_stream::OwnedBytes::from_static(b"forged");
                let error = match harness.admit_child_task(&harness.task_files, &forged).await {
                    Ok(_) => panic!("forged request admitted"),
                    Err(error) => error.to_string(),
                };
                let admission_bytes =
                    crate::contract::canonical_json_bytes(&record.canonical_value())
                        .unwrap()
                        .len() as u64;
                let output = (
                    serde_json::to_vec(&answer.outcome).unwrap(),
                    error,
                    admission_bytes,
                );
                harness.disconnect().await.unwrap();
                output
            })
        },
    );
    assert_eq!(
        (&pair[0].0.0, &pair[0].0.1),
        (&pair[1].0.0, &pair[1].0.1),
        "admission changed output bytes or its error"
    );
    for ((_, _, admission_bytes), trace) in &pair {
        let observations: Vec<_> = trace
            .iter()
            .copied()
            .filter(|(kind, _)| *kind != "view-items" && *kind != "view-allocations")
            .collect();
        // Independent stores have different digest bytes in their canonical
        // admissions. Check every operation against its own exact input size.
        let admission_read = [
            ("read", *admission_bytes),
            ("verify", *admission_bytes),
            ("retain", 0),
            ("verify", *admission_bytes),
        ];
        assert_eq!(observations, admission_read.repeat(2));
    }
    no_view_work(&pair[1].1, true);
    assert!(pair[0].1.iter().any(|(_, amount)| *amount > 0));
    std::panic::catch_unwind(|| no_view_work(&pair[0].1, true))
        .expect_err("redundant view work was not detected");
    no_view_work(&pair[0].1, false);
}

fn no_result_body(trace: &[(&str, u64)], check: bool) {
    if check {
        assert!(
            trace.is_empty(),
            "terminal status loaded its result body: {trace:?}"
        );
    }
}

#[test]
fn preparation_status_preserves_bytes_and_errors_without_loading_its_report() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let pair = differential::pair_of(
        &["read", "verify", "retain"],
        || runtime.block_on(fixture()),
        |(_directory, harness, _request), old| {
            runtime.block_on(async {
                let host = harness.runtime.task_host();
                let task = TaskId::from_bytes(
                    child_operation(harness.task_files.volume(), "delegate", b"preparation")
                        .unwrap()
                        .into_bytes(),
                );
                let mut report_bytes = 0;
                let status = if old {
                    let Some(crate::Outcome::Succeeded(value)) = host.outcome(task).await.unwrap()
                    else {
                        panic!("fixture preparation did not complete");
                    };
                    report_bytes =
                        crate::contract::canonical_json_bytes(&value).unwrap().len() as u64;
                    Some(crate::Outcome::Succeeded(()))
                } else {
                    host.terminal_status(task).await.unwrap()
                };
                assert_eq!(status, Some(crate::Outcome::Succeeded(())));
                let absent = TaskId::from_bytes([0xfd; 16]);
                let error = if old {
                    host.outcome(absent).await.unwrap_err().to_string()
                } else {
                    host.terminal_status(absent).await.unwrap_err().to_string()
                };
                let output = (serde_json::to_vec(&status).unwrap(), error, report_bytes);
                harness.disconnect().await.unwrap();
                output
            })
        },
    );
    assert_eq!((&pair[0].0.0, &pair[0].0.1), (&pair[1].0.0, &pair[1].0.1));
    let bytes = pair[0].0.2;
    assert!(bytes > 4096, "fixture omitted the large fork report");
    assert_eq!(
        pair[0].1,
        vec![
            ("read", bytes),
            ("verify", bytes),
            ("retain", 0),
            ("verify", bytes)
        ]
    );
    no_result_body(&pair[1].1, true);
    std::panic::catch_unwind(|| no_result_body(&pair[0].1, true))
        .expect_err("redundant result loading escaped the work assertion");
    no_result_body(&pair[0].1, false);
}
