use super::*;

const GENERATION: &str = "0123456789abcdef0123456789abcdef";

pub(super) fn native_read_refusal() -> io::Error {
    let file = tempfile::NamedTempFile::new().unwrap();
    let mut write_only = std::fs::OpenOptions::new()
        .write(true)
        .open(file.path())
        .unwrap();
    std::io::Read::read(&mut write_only, &mut [0]).unwrap_err()
}

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
fn kernel_backpressure_preserves_report_identity_and_unrelated_physical_completion() {
    use std::future::{Future as _, poll_fn};
    use std::io::Write as _;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::task::Poll;
    use tokio::io::AsyncReadExt as _;
    use tracing_subscriber::layer::SubscriberExt as _;

    let runtime = crate::coordination_runtime().unwrap();
    let report = WaitReport::default();
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(report.clone()));
    let _subscriber = tracing::dispatcher::set_default(&dispatch);
    runtime.block_on(async {
        for frame in ["ack", "setup", "execution"] {
            for terminal in ["drain", "close", "cancel"] {
                let reject_startup = frame == "setup";
                let close_sink = terminal == "close";
                let (owner, child) = std::os::unix::net::UnixStream::pair().unwrap();
                owner.set_nonblocking(true).unwrap();
                child.set_nonblocking(true).unwrap();
                let mut filler = child.try_clone().unwrap();
                let mut port = ReportPort::new(
                    GENERATION.into(),
                    tokio::net::UnixStream::from_std(child).unwrap(),
                )
                .unwrap();
                let mut owner = BufReader::new(tokio::net::UnixStream::from_std(owner).unwrap());
                let mut line = String::new();
                if !reject_startup {
                    port.ready().await.unwrap();
                    owner.read_line(&mut line).await.unwrap();
                    assert_eq!(
                        serde_json::from_str::<serde_json::Value>(&line).unwrap()["generation"],
                        GENERATION
                    );
                }
                let request = ReportRequest {
                    request_id: "fedcba9876543210fedcba9876543210".into(),
                    generation: GENERATION.into(),
                    deadline_unix_ms: u64::MAX,
                };
                let received = if !reject_startup {
                    let mut bytes = serde_json::to_vec(&request).unwrap();
                    bytes.push(b'\n');
                    owner.get_mut().write_all(&bytes).await.unwrap();
                    Some(port.request().await.unwrap().unwrap())
                } else {
                    None
                };
                let (_producer, completion) = tokio::sync::oneshot::channel::<()>();
                let wait = crate::obs::waiting("channel-close", "kernel-credit", completion);
                let ack = received.map(|received| port.snapshot(received, &report));
                if let Some(ack) = &ack {
                    assert_eq!(ack.waits.len(), 1);
                }
                let cause = native_read_refusal();
                let original = (cause.kind(), cause.raw_os_error(), cause.to_string());

                // Exhaust actual kernel credit before the original ACK is submitted.
                // Each successful iteration transfers bytes; WouldBlock is the cut,
                // never a readiness retry or an inferred quiet interval.
                let chunk = [b'x'; 8192];
                let mut filled = 0;
                loop {
                    match filler.write(&chunk) {
                        Ok(0) => panic!("the live report socket accepted no bytes"),
                        Ok(written) => filled += written,
                        Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                        Err(error) => panic!("fill report socket: {error}"),
                    }
                }
                assert!(filled > 0);
                let owner = {
                    let delivery = async {
                        if reject_startup {
                            port.reject_startup("setup", &cause).await
                        } else if frame == "execution" {
                            port.fail_execution(&cause).await
                        } else {
                            port.acknowledge(ack.as_ref().unwrap()).await
                        }
                    };
                    tokio::pin!(delivery);
                    assert!(
                        poll_fn(|cx| Poll::Ready(delivery.as_mut().poll(cx).is_pending())).await
                    );

                    let physical = Arc::new(AtomicUsize::new(0));
                    let completed = Arc::clone(&physical);
                    let position = u64::from(close_sink) + 2 * u64::from(reject_startup);
                    let result = crate::spawn_persistence(
                        "report",
                        "unrelated",
                        crate::PersistenceKey::new(Arc::as_ptr(&physical) as usize, position),
                        move || {
                            completed.fetch_add(1, Ordering::SeqCst);
                            position
                        },
                    )
                    .await
                    .unwrap();
                    assert_eq!(result, position);
                    assert_eq!(physical.load(Ordering::SeqCst), 1);
                    assert!(
                        poll_fn(|cx| Poll::Ready(delivery.as_mut().poll(cx).is_pending())).await
                    );

                    if terminal == "cancel" {
                        Some(owner)
                    } else if close_sink {
                        drop(owner);
                        assert_eq!(
                            delivery.await.unwrap_err().kind(),
                            io::ErrorKind::BrokenPipe
                        );
                        None
                    } else {
                        let mut drain = [0; 8192];
                        while filled > 0 {
                            let count = filled.min(drain.len());
                            owner.read_exact(&mut drain[..count]).await.unwrap();
                            assert!(drain[..count].iter().all(|byte| *byte == b'x'));
                            filled -= count;
                        }
                        line.clear();
                        let (delivered, read) = tokio::join!(delivery, owner.read_line(&mut line));
                        delivered.unwrap();
                        assert_ne!(read.unwrap(), 0);
                        if frame != "ack" {
                            let key = if reject_startup { "rejected" } else { "failed" };
                            let phase = if reject_startup { "setup" } else { "execution" };
                            let delivered: serde_json::Value = serde_json::from_str(&line).unwrap();
                            assert_eq!(delivered["generation"], GENERATION);
                            assert_eq!(delivered[key]["phase"], phase);
                            assert_eq!(delivered[key]["cause"], cause.to_string());
                            assert_eq!(
                                delivered[key]["raw_os_error"],
                                cause.raw_os_error().unwrap()
                            );
                        } else {
                            assert_eq!(
                                line,
                                format!(
                                    "{}\n",
                                    serde_json::to_string(ack.as_ref().unwrap()).unwrap()
                                )
                            );
                            let delivered: ReportAck = serde_json::from_str(&line).unwrap();
                            assert_eq!(delivered.request_id, request.request_id);
                            assert_eq!(delivered.generation, request.generation);
                            assert_eq!(delivered.outcome, ReportOutcome::Report);
                            assert_eq!(delivered.waits.len(), 1);
                            assert_eq!(delivered.waits[0].primitive, "channel-close");
                            assert_eq!(delivered.waits[0].key, "kernel-credit");
                        }
                        Some(owner)
                    }
                };
                assert_eq!(
                    (cause.kind(), cause.raw_os_error(), cause.to_string()),
                    original
                );
                if terminal == "cancel" {
                    assert_eq!(
                        port.fail_execution(&cause).await.unwrap_err().kind(),
                        io::ErrorKind::AlreadyExists
                    );
                }
                drop(port);
                drop(filler);
                if let Some(mut owner) = owner {
                    if terminal == "cancel" {
                        let mut drain = [0; 8192];
                        while filled > 0 {
                            let count = filled.min(drain.len());
                            owner.read_exact(&mut drain[..count]).await.unwrap();
                            assert!(drain[..count].iter().all(|byte| *byte == b'x'));
                            filled -= count;
                        }
                    }
                    line.clear();
                    assert_eq!(owner.read_line(&mut line).await.unwrap(), 0);
                }
                drop(wait);
                assert!(report.snapshot().is_empty());
            }
        }
    });
}

#[cfg(unix)]
#[test]
fn inherited_report_port_joins_the_selected_real_process() {
    use std::io::{BufRead as _, Write as _};
    use std::os::{fd::AsRawFd as _, unix::process::CommandExt as _};
    const CHILD: &str = "ACYCLIC_REPORT_PORT_CHILD";
    if let Ok(mode) = std::env::var(CHILD) {
        use tracing_subscriber::layer::SubscriberExt as _;
        let admission = ReportAdmission::from_environment().unwrap().unwrap();
        assert!(
            matches!(ReportAdmission::from_environment(), Err(error) if error.kind() == io::ErrorKind::AlreadyExists)
        );
        if mode == "no-runtime" {
            assert!(admission.into_port().is_err());
            return;
        }
        let report = WaitReport::default();
        let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(report.clone()));
        let _subscriber = tracing::dispatcher::set_default(&dispatch);
        crate::coordination_runtime().unwrap().block_on(async {
            let mut port = admission.into_port().unwrap();
            if matches!(mode.as_str(), "reject" | "reject-foreign" | "reject-held") {
                let cause = native_read_refusal();
                let original = (cause.kind(), cause.raw_os_error(), cause.to_string());
                if mode == "reject-foreign" {
                    port.generation = format!(
                        "{:032x}",
                        u128::from_str_radix(&port.generation, 16).unwrap() ^ 1
                    );
                }
                port.reject_startup("setup", &cause).await.unwrap();
                assert_eq!(
                    (cause.kind(), cause.raw_os_error(), cause.to_string()),
                    original
                );
                assert_eq!(
                    port.ready().await.unwrap_err().kind(),
                    io::ErrorKind::AlreadyExists
                );
                assert_eq!(
                    port.reject_startup("again", &cause)
                        .await
                        .unwrap_err()
                        .kind(),
                    io::ErrorKind::AlreadyExists
                );
                assert_eq!(
                    port.request().await.unwrap_err().kind(),
                    io::ErrorKind::AlreadyExists
                );
                if mode == "reject-held" {
                    // The direct launch's original stdin EOF releases this
                    // real producer only after its rejection was observed.
                    let mut byte = [0];
                    assert_eq!(
                        std::io::Read::read(&mut std::io::stdin(), &mut byte).unwrap(),
                        0
                    );
                }
                return;
            }
            if mode == "fail-before-ready" {
                port.fail_execution(&native_read_refusal()).await.unwrap();
                return;
            }
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
            if mode == "fail-ready" || mode == "fail-foreign" {
                if mode == "fail-foreign" {
                    port.generation = format!(
                        "{:032x}",
                        u128::from_str_radix(&port.generation, 16).unwrap() ^ 1
                    );
                }
                port.fail_execution(&native_read_refusal()).await.unwrap();
                return;
            }
            let request = port.request().await.unwrap().unwrap();
            if mode == "crash" {
                std::process::exit(17);
            }
            if mode == "eof-after-request" {
                drop(port);
                std::future::pending::<()>().await;
                return;
            }
            if mode == "hold" {
                std::future::pending::<()>().await;
            }
            let ack = port.snapshot(request, &report);
            port.acknowledge(&ack).await.unwrap();
            if mode == "fail-ack" {
                port.fail_execution(&native_read_refusal()).await.unwrap();
            }
        });
        return;
    }
    for mode in ["1", "reject", "no-runtime"] {
        let (mut owner, endpoint) = std::os::unix::net::UnixStream::pair().unwrap();
        let descriptor = endpoint.as_raw_fd();
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "obs::report_port::tests::inherited_report_port_joins_the_selected_real_process",
                "--nocapture",
            ])
            .env(CHILD, mode)
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
        let count = lines.read_line(&mut line).unwrap();
        if mode == "no-runtime" {
            assert_eq!(
                count, 0,
                "unavailable runtime cannot claim rejection delivery"
            );
            assert!(child.wait_with_output().unwrap().status.success());
            continue;
        }
        assert_ne!(count, 0);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&line).unwrap()["generation"],
            GENERATION
        );
        if mode == "reject" {
            let rejection: serde_json::Value = serde_json::from_str(&line).unwrap();
            let original = native_read_refusal();
            assert_eq!(rejection["rejected"]["phase"], "setup");
            assert_eq!(rejection["rejected"]["cause"], original.to_string());
            assert_eq!(
                rejection["rejected"]["raw_os_error"],
                original.raw_os_error().unwrap()
            );
        } else {
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
        }
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
}
