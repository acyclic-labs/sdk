use super::*;
use acyclic_native_runtime::obs::{ReportAck, ReportOutcome, ReportRequest};
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};

const GENERATION: &str = "0123456789abcdef0123456789abcdef";

fn request() -> ReportRequest {
    ReportRequest {
        request_id: "abcdef0123456789abcdef0123456789".into(),
        generation: GENERATION.into(),
        deadline_unix_ms: u64::MAX,
    }
}

#[test]
fn acknowledged_snapshot_precedes_cancellation_and_keeps_the_original_producer() {
    let runtime = acyclic_native_runtime::coordination_runtime().unwrap();
    let report = WaitReport::default();
    let dispatch = tracing::Dispatch::new(subscriber(&report, Some("off".into())));
    let _subscriber = tracing::dispatcher::set_default(&dispatch);
    runtime.block_on(async {
        let (owner, child) = tokio::io::duplex(4096);
        let port = ReportPort::new(GENERATION.into(), child).unwrap();
        let (producer, completion) = tokio::sync::oneshot::channel::<()>();
        let (terminate, signal) = tokio::sync::oneshot::channel();
        let shutdown = async {
            signal.await.unwrap();
            Ok(143)
        }
        .boxed()
        .shared();
        let operation = async {
            acyclic_native_runtime::obs::waiting("channel-close", "turn-17", completion).await?;
            Ok(0)
        };
        let host = async {
            let mut owner = BufReader::new(owner);
            let mut line = String::new();
            owner.read_line(&mut line).await.unwrap();
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&line).unwrap()["generation"],
                GENERATION
            );
            let mut bytes = serde_json::to_vec(&request()).unwrap();
            bytes.push(b'\n');
            owner.get_mut().write_all(&bytes).await.unwrap();
            line.clear();
            owner.read_line(&mut line).await.unwrap();
            let ack: ReportAck = serde_json::from_str(&line).unwrap();
            assert_eq!(ack.request_id, request().request_id);
            assert_eq!(ack.generation, GENERATION);
            assert_eq!(ack.outcome, ReportOutcome::Report);
            assert_eq!(ack.waits.len(), 1);
            assert_eq!(ack.waits[0].primitive, "channel-close");
            assert_eq!(ack.waits[0].key, "turn-17");
            assert!(
                !producer.is_closed(),
                "reporting must not cancel the observed operation"
            );
            terminate.send(()).unwrap();
            producer
        };
        let ((result, diagnostic), producer) = tokio::join!(
            observe(&report, Ok(Some(port)), shutdown, false, operation),
            host
        );
        assert_eq!(result.unwrap(), 143);
        diagnostic.unwrap();
        assert!(producer.is_closed());
        assert!(report.snapshot().is_empty());
    });
}

#[test]
fn dropped_producer_exits_with_its_typed_error_and_no_live_wait() {
    let runtime = acyclic_native_runtime::coordination_runtime().unwrap();
    let report = WaitReport::default();
    let dispatch = tracing::Dispatch::new(subscriber(&report, Some("off".into())));
    let _subscriber = tracing::dispatcher::set_default(&dispatch);
    runtime.block_on(async {
        let (producer, completion) = tokio::sync::oneshot::channel::<()>();
        drop(producer);
        let operation = async {
            acyclic_native_runtime::obs::waiting("channel-close", "turn-18", completion).await?;
            Ok(0)
        };
        let (result, _) = observe(
            &report,
            Ok(None),
            std::future::pending().boxed().shared(),
            false,
            operation,
        )
        .await;
        assert!(
            result
                .unwrap_err()
                .is::<tokio::sync::oneshot::error::RecvError>()
        );
        assert!(report.snapshot().is_empty());
    });
}

#[test]
fn blocked_or_closed_report_sink_cannot_hold_graceful_owner_cleanup() {
    for closed in [false, true] {
        let runtime = acyclic_native_runtime::coordination_runtime().unwrap();
        runtime.block_on(async {
            let report = WaitReport::default();
            let (owner, child) = tokio::io::duplex(1);
            let port = ReportPort::new(GENERATION.into(), child).unwrap();
            let owner = if closed {
                drop(owner);
                None
            } else {
                Some(owner)
            };
            let (terminate, signal) = tokio::sync::oneshot::channel();
            let shutdown = async {
                signal.await.unwrap();
                Ok(143)
            }
            .boxed()
            .shared();
            let stopped = shutdown.clone();
            let (release, retiring) = tokio::sync::oneshot::channel();
            let cleaned = std::cell::Cell::new(false);
            let operation = async {
                stopped.await.unwrap();
                retiring.await.unwrap();
                cleaned.set(true);
                Ok(0)
            };
            let controller = async {
                terminate.send(()).unwrap();
                release.send(()).unwrap();
            };
            let ((result, diagnostic), ()) = tokio::join!(
                observe(&report, Ok(Some(port)), shutdown, true, operation),
                controller
            );
            assert_eq!(result.unwrap(), 0);
            assert!(cleaned.get());
            assert!(diagnostic.is_err());
            drop(owner);
        });
    }
}
