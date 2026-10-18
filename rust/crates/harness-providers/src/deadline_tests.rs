//! Every exchange ends by its deadlines on a virtual clock: a slow trickle of bytes ends at the
//! total deadline, a silent body (before or after output) at the idle deadline, and a body that
//! keeps both completes untouched. The unguarded body is the negative control: it stays pending.
use super::*;
use acyclic_harness::ModelTransportFailure;
use futures::TryStreamExt as _;
use proptest::prelude::*;

/// Milliseconds of virtual time.
type Millis = u64;

/// A scripted exchange: the response after `head`, then each chunk after its gap, then the end
/// or, when `silent`, nothing forever.
#[derive(Clone, Debug)]
struct History {
    head: Millis,
    gaps: Vec<Millis>,
    silent: bool,
}

/// Plays a [`History`] on tokio's clock, which the test runtime pauses and auto-advances.
struct Script(History);

impl HttpTransport for Script {
    fn send(&self, _: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        let History { head, gaps, silent } = self.0.clone();
        Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(head)).await;
            let chunks = futures::stream::iter(gaps).then(|gap| async move {
                tokio::time::sleep(Duration::from_millis(gap)).await;
                Ok(Bytes::from_static(b"x"))
            });
            let body: BoxProviderStream<'static, Result<Bytes>> = if silent {
                Box::pin(chunks.chain(futures::stream::pending()))
            } else {
                Box::pin(chunks)
            };
            Ok(HttpResponse {
                status: 200,
                body,
                retry_after: None,
            })
        })
    }
    fn delay(&self, wait: Duration) -> BoxProviderFuture<'static, ()> {
        Box::pin(tokio::time::sleep(wait))
    }
    fn now(&self) -> Duration {
        crate::http::native_now()
    }
    fn delay_with_native_work(
        &self,
        wait: Duration,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<acyclic_harness::NativeFutureBundle<'static, ()>> {
        acyclic_harness::NativeFutureBundle::new(work, move || async move {
            tokio::time::sleep(wait).await;
            Ok(())
        })
    }
    fn send_with_native_work<'a>(
        &'a self,
        request: HttpRequest,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<acyclic_harness::NativeFutureBundle<'a, HttpResponse>> {
        acyclic_harness::NativeFutureBundle::new(work, || async move {
            let response = self.send(request).await?;
            HttpResponse::with_native_body(response.status, response.body, work)
        })
    }
}

fn request() -> HttpRequest {
    HttpRequest {
        url: "http://127.0.0.1:1/v1".into(),
        headers: std::collections::BTreeMap::new(),
        body: Bytes::new(),
    }
}

/// What the exchange delivered: its chunk count and the timeout that ended it, if any.
fn run(transport: Arc<dyn HttpTransport>) -> (usize, Option<String>) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("test runtime");
    runtime.block_on(async move {
        let mut chunks = 0;
        let response = match transport.send(request()).await {
            Ok(response) => response,
            Err(error) => return (chunks, Some(cause(&error))),
        };
        let mut body = response.body;
        loop {
            match body.try_next().await {
                Ok(Some(_)) => chunks += 1,
                Ok(None) => return (chunks, None),
                Err(error) => return (chunks, Some(cause(&error))),
            }
        }
    })
}

/// The excerpt of a typed timeout; any other failure fails the test.
fn cause(error: &Error) -> String {
    match error {
        Error::ModelUnavailable {
            failure: ModelTransportFailure::Timeout,
            excerpt,
        } => excerpt.clone(),
        other => panic!("untyped deadline failure: {other:?}"),
    }
}

fn bounded(history: History, idle_ms: u32, total_ms: u32) -> Arc<dyn HttpTransport> {
    Bounded::new(Arc::new(Script(history)), TimeoutSpec { idle_ms, total_ms })
}

const IDLE: u32 = 100;

/// A gap the idle deadline always tolerates, or always reports. Silence is measured on the
/// clock, so the bound is exact; a gap of exactly one deadline is a tie and is not generated.
fn gap() -> impl Strategy<Value = Millis> {
    prop_oneof![1..u64::from(IDLE), u64::from(IDLE) + 1..u64::from(5 * IDLE)]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Consumer work can let the total deadline pass while the producer is ready. Readiness
    // must never bypass an expired deadline, including an immediately ready EOF or headers.
    #[test]
    fn expired_total_precedes_ready_producers(total in 1..10_000u32, late in 1..10_000u64, eof in any::<bool>(), headers in any::<bool>()) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time().start_paused(true).build().unwrap();
        runtime.block_on(async {
            let transport = Bounded::new(Arc::new(Script(History {
                head: u64::from(total), gaps: vec![], silent: false,
            })), TimeoutSpec { idle_ms: u32::MAX, total_ms: total });
            let error = if headers {
                let mut send = pin!(transport.send(request()));
                assert!(send.as_mut().now_or_never().is_none());
                tokio::time::advance(Duration::from_millis(u64::from(total) + late)).await;
                send.await.err().expect("expired headers must fail")
            } else {
                let body: BoxProviderStream<'static, Result<Bytes>> = if eof {
                    Box::pin(futures::stream::empty())
                } else {
                    Box::pin(futures::stream::repeat_with(|| Ok(Bytes::from_static(b"x"))))
                };
                let deadlines = Deadlines::start(transport.inner.clone(), transport.timeouts, false);
                let mut guarded = Guarded::new(body, deadlines);
                tokio::time::advance(Duration::from_millis(u64::from(total) + late)).await;
                guarded.next().await.expect("timeout item").expect_err("expired body must fail")
            };
            assert_eq!(cause(&error), format!("exchange exceeded {total} ms"));
        });
    }

    // Idle: the first silence longer than the idle deadline ends the exchange right after the
    // chunks before it (a stall after partial output when there are any); shorter gaps never do.
    #[test]
    fn a_silent_body_ends_at_the_idle_deadline(
        head in 0..10_000u64,
        gaps in proptest::collection::vec(gap(), 0..12),
        silent in any::<bool>(),
    ) {
        let history = History { head, gaps: gaps.clone(), silent };
        let (chunks, ended) = run(bounded(history, IDLE, u32::MAX));
        let stall = gaps.iter().position(|gap| *gap > u64::from(IDLE));
        match (stall, silent) {
            (Some(index), _) => {
                prop_assert_eq!(chunks, index);
                prop_assert_eq!(ended, Some(format!("no response bytes for {IDLE} ms")));
            }
            (None, true) => {
                prop_assert_eq!(chunks, gaps.len());
                prop_assert_eq!(ended, Some(format!("no response bytes for {IDLE} ms")));
            }
            (None, false) => {
                prop_assert_eq!(chunks, gaps.len());
                prop_assert_eq!(ended, None);
            }
        }
    }

    // Total: bytes slow enough to keep the idle deadline still end at the total deadline,
    // counted from the send, and a response that ends first completes.
    #[test]
    fn a_slow_exchange_ends_at_the_total_deadline(
        head in (0..50u64).prop_map(|tens| tens * 10),
        gaps in proptest::collection::vec((1..50u64).prop_map(|tens| tens * 10), 0..40),
        total in (1..300u32).prop_map(|tens| tens * 10 + 5),
    ) {
        let history = History { head, gaps: gaps.clone(), silent: false };
        let (chunks, ended) = run(bounded(history, u32::MAX, total));
        // Every arrival is a multiple of 10 ms and the deadline is not, so there is no tie.
        let mut at = head;
        let arrived = gaps
            .iter()
            .take_while(|gap| {
                at += **gap;
                at < u64::from(total)
            })
            .count();
        if head >= u64::from(total) || arrived < gaps.len() {
            prop_assert_eq!(chunks, if head >= u64::from(total) { 0 } else { arrived });
            prop_assert_eq!(ended, Some(format!("exchange exceeded {total} ms")));
        } else {
            prop_assert_eq!(chunks, gaps.len());
            prop_assert_eq!(ended, None);
        }
    }
}

#[test]
fn the_unguarded_body_stays_pending_past_the_deadlines() {
    // Negative control: the live failure. A partially emitted stream, read without the guard,
    // is still pending after 150 virtual seconds; guarded, the same history times out.
    let history = History {
        head: 0,
        gaps: vec![10],
        silent: true,
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("test runtime");
    let pending = runtime.block_on(async {
        let response = Script(history.clone())
            .send(request())
            .await
            .expect("scripted response");
        let read = response.body.try_collect::<Vec<_>>();
        tokio::time::timeout(Duration::from_secs(150), read)
            .await
            .is_err()
    });
    assert!(pending);
    let (chunks, ended) = run(bounded(history, 60_000, u32::MAX));
    assert_eq!(
        (chunks, ended),
        (1, Some("no response bytes for 60000 ms".into()))
    );
}

#[test]
fn zero_deadlines_are_rejected() {
    for timeouts in [
        TimeoutSpec {
            idle_ms: 0,
            total_ms: 1,
        },
        TimeoutSpec {
            idle_ms: 1,
            total_ms: 0,
        },
    ] {
        assert!(timeouts.validate().is_err());
    }
    assert!(TimeoutSpec::DEFAULT.validate().is_ok());
}

#[test]
fn native_timeout_does_not_allocate_a_discarded_diagnostic() {
    let allocated = acyclic_test_support::measure(|| {
        let error = timeout(true, Expired::Total, TimeoutSpec::DEFAULT);
        assert!(matches!(error, Error::NativeBoundary { .. }));
    });
    assert_eq!(allocated.count_total, 0);
    // Negative control: the previous native path built this text and discarded it.
    let discarded = acyclic_test_support::measure(|| {
        std::hint::black_box(format!(
            "exchange exceeded {} ms",
            TimeoutSpec::DEFAULT.total_ms
        ));
    });
    assert!(discarded.count_total > 0);
}

/// A transport on the paused clock whose timer never fires: the timer driver is starved.
struct Starved(History);

impl HttpTransport for Starved {
    fn send(&self, request: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        let script = Script(self.0.clone());
        Box::pin(async move { script.send(request).await })
    }
    fn delay(&self, _: Duration) -> BoxProviderFuture<'static, ()> {
        Box::pin(futures::future::pending())
    }
    fn now(&self) -> Duration {
        crate::http::native_now()
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    // A starved timer cannot let ready bytes or EOF erase an already expired idle deadline.
    // Empty chunks never count as bytes of progress, even when polled frequently.
    #[test]
    fn idle_expiry_precedes_ready_body_and_empty_chunks(
        late in 1..1000u64, eof in any::<bool>(), partial in any::<bool>(),
        empty in any::<bool>(),
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time().start_paused(true).build().unwrap();
        runtime.block_on(async {
            let transport: Arc<dyn HttpTransport> = Arc::new(Starved(History {
                head: 0, gaps: vec![], silent: true,
            }));
            let deadlines = Deadlines::start(transport, TimeoutSpec {
                idle_ms: 100, total_ms: u32::MAX,
            }, false);
            let ready: BoxProviderStream<'static, Result<Bytes>> = if eof {
                Box::pin(futures::stream::empty())
            } else {
                Box::pin(futures::stream::repeat_with(move || Ok(if empty {
                    Bytes::new()
                } else { Bytes::from_static(b"x") })))
            };
            let prefix = futures::stream::iter(partial.then(|| Ok(Bytes::from_static(b"prefix"))));
            let mut guarded = Guarded::new(Box::pin(prefix.chain(ready)), deadlines);
            if partial { assert!(guarded.next().await.unwrap().is_ok()); }
            if empty && !eof {
                // No single gap exceeds idle, but empty chunks cannot postpone its expiry.
                for _ in 0..3 {
                    tokio::time::advance(Duration::from_millis(25)).await;
                    assert!(guarded.next().await.unwrap().unwrap().is_empty());
                }
            }
            tokio::time::advance(Duration::from_millis(100 + late)).await;
            // Negative control: polling the producer first, as the old guard did, reports
            // ready data/EOF and skips the clock check entirely.
            assert!(guarded.body.next().now_or_never().is_some());
            let error = guarded.next().await.unwrap().expect_err("expired idle before ready body");
            assert_eq!(cause(&error), "no response bytes for 100 ms");
            assert!(guarded.next().await.is_none());
        });
    }

    // The clock, not the timer, is the measure: time advances, no timer ever fires, and the next
    // poll still fails as a typed timeout, before or after partial output, for either deadline.
    #[test]
    fn a_starved_timer_cannot_extend_a_deadline(
        partial in any::<bool>(),
        idle in any::<bool>(),
        late in 1..10_000u64,
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .expect("test runtime");
        let (expected, observed, timer_only) = runtime.block_on(async {
            let gaps = if partial { vec![0] } else { vec![] };
            let history = History { head: 0, gaps, silent: true };
            let timeouts = if idle {
                TimeoutSpec { idle_ms: 100, total_ms: u32::MAX }
            } else {
                TimeoutSpec { idle_ms: u32::MAX, total_ms: 100 }
            };
            let bounded = Bounded::new(Arc::new(Starved(history.clone())), timeouts);
            let mut body = bounded.send(request()).await.expect("headers").body;
            if partial {
                assert!(body.next().await.expect("chunk").is_ok());
            }
            assert!(body.next().now_or_never().is_none());
            // Negative control: the timer-only measure, racing the body against a timer that
            // never fires, cannot tell that time passed.
            let mut unguarded = Script(history).send(request()).await.expect("headers").body;
            if partial {
                assert!(unguarded.next().await.expect("chunk").is_ok());
            }
            let starved = Starved(History { head: 0, gaps: vec![], silent: true });
            let mut timer = starved.delay(Duration::from_millis(100));
            tokio::time::advance(Duration::from_millis(100 + late)).await;
            let timer_only = futures::future::select(unguarded.next(), &mut timer).now_or_never();
            let observed = body
                .next()
                .now_or_never()
                .flatten()
                .map(|item| cause(&item.expect_err("timeout")));
            let expected = if idle {
                "no response bytes for 100 ms"
            } else {
                "exchange exceeded 100 ms"
            };
            (expected, observed, timer_only.is_some())
        });
        prop_assert_eq!(observed.as_deref(), Some(expected));
        prop_assert!(!timer_only);
    }
}

#[test]
fn native_deadline_timers_are_admitted_exactly_on_the_ledger() {
    use std::task::Context;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("test runtime");
    runtime.block_on(async {
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(65_536, 64 * 1024 * 1024),
            &acyclic_fs::CancellationToken::new(),
        )
        .unwrap_or_else(|_| panic!("ledger"));
        let before = work.live_allocation_bytes();
        let history = History {
            head: 0,
            gaps: vec![],
            silent: true,
        };
        let timeouts = TimeoutSpec {
            idle_ms: 100,
            total_ms: u32::MAX,
        };
        let bounded = Bounded::new(Arc::new(Script(history)), timeouts);
        let mut body = bounded
            .send_with_native_work(request(), &work)
            .expect("admitted send")
            .await
            .expect("headers")
            .body;
        let mut context = Context::from_waker(futures::task::noop_waker_ref());
        // The first silent poll arms the wake-up timer: one allocation, admitted before it
        // exists, byte for byte.
        let admitted = work.live_allocation_bytes();
        let allocated = acyclic_test_support::measure(|| {
            assert!(body.poll_next_unpin(&mut context).is_pending());
        });
        let claimed = work.live_allocation_bytes() - admitted;
        assert_eq!(allocated.count_total, 1);
        assert!(claimed > 0);
        assert_eq!(allocated.bytes_total, claimed);
        // Re-polling reuses the armed timer.
        let again = acyclic_test_support::measure(|| {
            assert!(body.poll_next_unpin(&mut context).is_pending());
        });
        assert_eq!(again.count_total, 0);
        drop(body);
        assert_eq!(work.live_allocation_bytes(), before);
        // Negative control: the same silent body on the portable path arms an unadmitted timer:
        // it allocates, and the ledger does not move.
        let mut portable = bounded.send(request()).await.expect("headers").body;
        let unadmitted = work.live_allocation_bytes();
        let allocated = acyclic_test_support::measure(|| {
            assert!(portable.poll_next_unpin(&mut context).is_pending());
        });
        assert!(allocated.bytes_total > 0);
        assert_eq!(work.live_allocation_bytes(), unadmitted);
    });
}
