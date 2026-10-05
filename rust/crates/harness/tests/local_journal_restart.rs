//! Cross-process proof for the public durable local Harness journal.

#![cfg(feature = "filesystem-local")]

use acyclic_harness::executor::{ExecutionEvent, ToolFailureKind};
use acyclic_harness::filesystem::LocalHarnessStorage;
use acyclic_harness::{AgentId, OperationId};
use std::{env, process::Command};
use tempfile::tempdir;

const CHILD_ENV: &str = "ACYCLIC_HARNESS_LOCAL_JOURNAL_CHILD";
const ROOT_ENV: &str = "ACYCLIC_HARNESS_LOCAL_JOURNAL_ROOT";
const OPERATION: OperationId = OperationId::from_bytes([209; 16]);
const AGENT: AgentId = AgentId::from_bytes([210; 16]);

fn retry_digest(key: &str) -> String {
    blake3::hash(format!("{OPERATION}:{key}").as_bytes())
        .to_hex()
        .to_string()
}

#[tokio::test]
async fn local_harness_journal_survives_process_restart()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let mode = env::var(CHILD_ENV).ok();
    if let (Some(mode), Ok(root)) = (mode.as_deref(), env::var(ROOT_ENV)) {
        let storage = LocalHarnessStorage::open(root, AGENT, 4_096).await?;
        match mode {
            "write" => {
                storage
                    .journal()
                    .append(
                        OPERATION,
                        "operation-start".into(),
                        ExecutionEvent::Started {
                            request_digest: [211; 32],
                        },
                    )
                    .await?;
            }
            "cancel" => {
                let records = storage.replay(OPERATION).await?;
                assert_eq!(records.len(), 1);
                storage
                    .journal()
                    .append(
                        OPERATION,
                        "operation-cancelled".into(),
                        ExecutionEvent::ToolFailed {
                            step: 0,
                            call_id: "cancelled".into(),
                            reason: ToolFailureKind::ExecutorRejected,
                        },
                    )
                    .await?;
            }
            "read" => {
                let records = storage.replay(OPERATION).await?;
                assert_eq!(records.len(), 2);
                assert_eq!(records[0].sequence, 1);
                assert_eq!(records[1].sequence, 2);
                assert_eq!(records[0].idempotency_key, retry_digest("operation-start"));
                assert_eq!(records[1].idempotency_key, retry_digest("operation-cancelled"));
                assert!(matches!(
                    records[1].event,
                    ExecutionEvent::ToolFailed { .. }
                ));
            }
            other => panic!("unknown child mode {other}"),
        }
        return Ok(());
    }

    let root = tempdir()?;
    for mode in ["write", "cancel", "read"] {
        let status = Command::new(env::current_exe()?)
            .arg("--exact")
            .arg("local_harness_journal_survives_process_restart")
            .arg("--nocapture")
            .env(CHILD_ENV, mode)
            .env(ROOT_ENV, root.path())
            .status()?;
        assert!(status.success(), "child mode {mode} exited with {status}");
    }
    Ok(())
}
