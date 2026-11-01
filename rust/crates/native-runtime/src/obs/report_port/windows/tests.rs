use super::*;
use crate::obs::{ReportAck, ReportOutcome, ReportRequest, WaitReport};
use std::process::Command;
use tokio::{
    io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader},
    net::windows::named_pipe::ServerOptions,
};
use tracing_subscriber::layer::SubscriberExt as _;

const CHILD: &str = "ACYCLIC_REPORT_PIPE_CHILD";
const TEST: &str = "obs::report_port::windows::tests::native_pipe_keeps_request_ack_and_producer_loss_on_the_selected_owner";

// The actual native pipe, selected executable and process tree are retained
// through acknowledgement or transport loss. No readiness deadline is used.
#[test]
fn native_pipe_keeps_request_ack_and_producer_loss_on_the_selected_owner() {
    let runtime = crate::coordination_runtime().unwrap();
    let report = WaitReport::default();
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(report.clone()));
    let _subscriber = tracing::dispatcher::set_default(&dispatch);
    runtime.block_on(async {
        if let Ok(mode) = std::env::var(CHILD) {
            let port = ReportPort::from_environment();
            if mode == "foreign" {
                assert!(matches!(port, Err(error) if error.kind() == io::ErrorKind::PermissionDenied));
                return;
            }
            let mut port = port.unwrap().expect("selected launch must expose its port");
            assert!(matches!(ReportPort::from_environment(), Err(error) if error.kind() == io::ErrorKind::AlreadyExists));
            let (_producer, completion) = tokio::sync::oneshot::channel::<()>();
            let _wait = (mode == "live")
                .then(|| crate::obs::waiting("channel-close", "report-control", completion));
            port.ready().await.unwrap();
            let request = port.request().await.unwrap();
            if mode == "withdrawn" {
                assert!(request.is_none(), "lost request producer must end the wait");
            } else {
                let ack = port.snapshot(request.unwrap(), &report);
                port.acknowledge(&ack).await.unwrap();
            }
            return;
        }
        for mode in ["ack", "live", "wrong-generation", "expired", "withdrawn", "foreign"] {
            let mut random = [0; 16];
            getrandom::fill(&mut random).unwrap();
            let generation = format!("{:032x}", u128::from_ne_bytes(random));
            let endpoint = format!(r"\\.\pipe\acyclic-report-{generation}");
            let pipe = ServerOptions::new()
                .first_pipe_instance(true)
                .create(&endpoint)
                .unwrap();
            let owner = std::process::id();
            let mut command = Command::new(std::env::current_exe().unwrap());
            let admitted_owner = if mode == "foreign" { owner ^ 0x8000_0000 } else { owner };
            command
                .args(["--exact", TEST, "--nocapture"])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .env(CHILD, mode)
                .env("ACYCLIC_REPORT_PIPE", endpoint)
                .env("ACYCLIC_REPORT_GENERATION", &generation)
                .env("ACYCLIC_REPORT_OWNER_PID", admitted_owner.to_string());
            let mut tree = crate::ProcessTree::spawn(&mut command).unwrap();
            let exited = tree.readiness().unwrap();
            tokio::select! {
                biased;
                result = pipe.connect() => result.unwrap(),
                result = exited => {
                    result.into_result().expect("original native producer event succeeded");
                    panic!("selected report producer exited before connecting");
                },
            }
            let mut pipe = BufReader::new(pipe);
            let mut line = String::new();
            if mode == "foreign" {
                assert_eq!(pipe.read_line(&mut line).await.unwrap(), 0);
            } else {
                assert_ne!(pipe.read_line(&mut line).await.unwrap(), 0);
                let ready: serde_json::Value = serde_json::from_str(&line).unwrap();
                assert_eq!(ready["ready"], true);
                assert_eq!(ready["generation"], generation);
                if !matches!(mode, "withdrawn" | "foreign") {
                    let request = ReportRequest {
                        request_id: format!("{:032x}", u128::from_ne_bytes(random) ^ 1),
                        generation: if mode == "wrong-generation" {
                            format!("{:032x}", u128::from_ne_bytes(random) ^ 2)
                        } else {
                            generation.clone()
                        },
                        deadline_unix_ms: if mode == "expired" { 0 } else { u64::MAX },
                    };
                    assert_ne!(request.request_id, request.generation);
                    let mut bytes = serde_json::to_vec(&request).unwrap();
                    bytes.push(b'\n');
                    pipe.get_mut().write_all(&bytes).await.unwrap();
                    line.clear();
                    assert_ne!(pipe.read_line(&mut line).await.unwrap(), 0);
                    let ack: ReportAck = serde_json::from_str(&line).unwrap();
                    assert_eq!(ack.request_id, request.request_id);
                    assert_eq!(ack.generation, request.generation);
                    let expected = match mode {
                        "live" => ReportOutcome::Report,
                        "wrong-generation" | "expired" => ReportOutcome::Unsupported,
                        _ => ReportOutcome::Empty,
                    };
                    assert_eq!(ack.outcome, expected);
                    if mode == "live" {
                        assert_eq!(ack.waits.len(), 1);
                        assert_eq!(ack.waits[0].primitive, "channel-close");
                        assert_eq!(ack.waits[0].key, "report-control");
                    } else {
                        assert!(ack.waits.is_empty());
                    }
                    line.clear();
                    assert_eq!(pipe.read_line(&mut line).await.unwrap(), 0);
                }
            }
            drop(pipe);
            // Dropping a pending asynchronous pipe can retain its handle until
            // this runtime drains cancellation. Keep its driver runnable while
            // the selected process observes EOF and exits.
            tree.readiness().unwrap().await;
            assert!(tree.try_wait(None).unwrap().unwrap().success());
            tree.terminate_descendants().unwrap();
            assert!(
                !tree.is_reaped(),
                "signalling containment retains direct-child retirement custody"
            );
            tree.terminate(None).unwrap();
            assert!(tree.is_reaped());
        }
    });
}
