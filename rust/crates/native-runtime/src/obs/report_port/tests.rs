use super::*;

const GENERATION: &str = "0123456789abcdef0123456789abcdef";

#[tokio::test]
async fn empty_and_rejected_requests_are_acknowledged_without_cancelling_work() {
    for (generation, deadline, expected) in [
        (GENERATION, u64::MAX, ReportOutcome::Empty),
        (
            "abcdef0123456789abcdef0123456789",
            u64::MAX,
            ReportOutcome::Unsupported,
        ),
        (GENERATION, 0, ReportOutcome::Unsupported),
    ] {
        let (owner, child) = tokio::io::duplex(4096);
        let mut port = ReportPort::new(GENERATION.into(), child).unwrap();
        let request = ReportRequest {
            request_id: GENERATION.into(),
            generation: generation.into(),
            deadline_unix_ms: deadline,
        };
        let producer = async {
            port.ready().await.unwrap();
            let request = port.request().await.unwrap().unwrap();
            let ack = port.snapshot(request, &WaitReport::default());
            port.acknowledge(&ack).await.unwrap();
            assert_eq!(
                port.acknowledge(&ack).await.unwrap_err().kind(),
                io::ErrorKind::InvalidInput
            );
        };
        let consumer = async {
            let mut owner = BufReader::new(owner);
            let mut line = String::new();
            owner.read_line(&mut line).await.unwrap();
            let mut bytes = serde_json::to_vec(&request).unwrap();
            bytes.push(b'\n');
            owner.get_mut().write_all(&bytes).await.unwrap();
            line.clear();
            owner.read_line(&mut line).await.unwrap();
            let ack: ReportAck = serde_json::from_str(&line).unwrap();
            assert_eq!(ack.request_id, request.request_id);
            assert_eq!(ack.generation, request.generation);
            assert_eq!(ack.outcome, expected);
            assert!(ack.waits.is_empty());
        };
        tokio::join!(producer, consumer);
    }
}

#[tokio::test]
async fn withdrawn_request_and_broken_sink_end_with_the_original_transport_cause() {
    let (owner, child) = tokio::io::duplex(64);
    let mut port = ReportPort::new(GENERATION.into(), child).unwrap();
    drop(owner);
    assert!(port.request().await.unwrap().is_none());
    let (mut owner, child) = tokio::io::duplex(1024);
    let mut port = ReportPort::new(GENERATION.into(), child).unwrap();
    let request = ReportRequest {
        request_id: GENERATION.into(),
        generation: GENERATION.into(),
        deadline_unix_ms: u64::MAX,
    };
    let mut bytes = serde_json::to_vec(&request).unwrap();
    bytes.push(b'\n');
    owner.write_all(&bytes).await.unwrap();
    let received = port.request().await.unwrap().unwrap();
    let mut ack = port.snapshot(received, &WaitReport::default());
    ack.request_id = "abcdef0123456789abcdef0123456789".into();
    assert_eq!(
        port.acknowledge(&ack).await.unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    ack.request_id = request.request_id;
    drop(owner);
    assert_eq!(
        port.acknowledge(&ack).await.unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
}

#[cfg(unix)]
#[test]
fn inherited_report_port_joins_the_selected_real_process() {
    use std::io::{BufRead as _, Write as _};
    use std::os::{fd::AsRawFd as _, unix::process::CommandExt as _};
    const CHILD: &str = "ACYCLIC_REPORT_PORT_CHILD";
    if let Ok(mode) = std::env::var(CHILD) {
        use tracing_subscriber::layer::SubscriberExt as _;
        let report = WaitReport::default();
        let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(report.clone()));
        let _subscriber = tracing::dispatcher::set_default(&dispatch);
        crate::coordination_runtime().unwrap().block_on(async {
            let mut port = ReportPort::from_environment().unwrap().unwrap();
            if mode == "no-ready" {
                drop(port);
                std::future::pending::<()>().await;
                return;
            }
            let (_producer, completion) = tokio::sync::oneshot::channel::<()>();
            let _wait = if mode == "live" {
                Some(crate::obs::waiting(
                    "channel-close",
                    "report-control",
                    completion,
                ))
            } else {
                None
            };
            port.ready().await.unwrap();
            let request = port.request().await.unwrap().unwrap();
            if mode == "crash" {
                std::process::exit(17);
            }
            if mode == "hold" {
                std::future::pending::<()>().await;
            }
            let ack = port.snapshot(request, &report);
            port.acknowledge(&ack).await.unwrap();
        });
        return;
    }
    let (mut owner, endpoint) = std::os::unix::net::UnixStream::pair().unwrap();
    let descriptor = endpoint.as_raw_fd();
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            "obs::report_port::tests::inherited_report_port_joins_the_selected_real_process",
            "--nocapture",
        ])
        .env(CHILD, "1")
        .env("ACYCLIC_REPORT_FD", "3")
        .env("ACYCLIC_REPORT_GENERATION", GENERATION)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped());
    #[expect(
        unsafe_code,
        reason = "transfers the original report socket into the selected child before exec"
    )]
    unsafe {
        command.pre_exec(move || {
            if libc::dup2(descriptor, 3) < 0 || libc::fcntl(3, libc::F_SETFD, 0) < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let child = command.spawn().unwrap();
    drop(endpoint);
    let mut lines = std::io::BufReader::new(owner.try_clone().unwrap());
    let mut line = String::new();
    assert_ne!(lines.read_line(&mut line).unwrap(), 0);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&line).unwrap()["generation"],
        GENERATION
    );
    let request = ReportRequest {
        request_id: GENERATION.into(),
        generation: GENERATION.into(),
        deadline_unix_ms: u64::MAX,
    };
    serde_json::to_writer(&mut owner, &request).unwrap();
    owner.write_all(b"\n").unwrap();
    line.clear();
    assert_ne!(lines.read_line(&mut line).unwrap(), 0);
    let ack: ReportAck = serde_json::from_str(&line).unwrap();
    assert_eq!(ack.request_id, GENERATION);
    assert_eq!(ack.outcome, ReportOutcome::Empty);
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    line.clear();
    assert_eq!(
        lines.read_line(&mut line).unwrap(),
        0,
        "selected owner exit must close the report producer"
    );
}
