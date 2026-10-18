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

/// A gap the idle deadline always tolerates, or always reports. A report needs a whole silent
/// window; a gap of exactly two windows can end on a window boundary, where the arriving chunk
/// counts as progress, so reported gaps are longer.
fn gap() -> impl Strategy<Value = Millis> {
    prop_oneof![
        1..u64::from(IDLE),
        u64::from(2 * IDLE) + 1..u64::from(5 * IDLE)
    ]
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
                let deadline = transport.inner.delay(transport.total());
                let mut guarded = transport.guard(body, deadline, false);
                tokio::time::advance(Duration::from_millis(u64::from(total) + late)).await;
                guarded.next().await.expect("timeout item").expect_err("expired body must fail")
            };
            assert_eq!(cause(&error), format!("exchange exceeded {total} ms"));
        });
    }

    // Idle: the first silence of two windows ends the exchange right after the chunks before it
    // (a stall after partial output when there are any); shorter gaps never do.
    #[test]
    fn a_silent_body_ends_at_the_idle_deadline(
        head in 0..10_000u64,
        gaps in proptest::collection::vec(gap(), 0..12),
        silent in any::<bool>(),
    ) {
        let history = History { head, gaps: gaps.clone(), silent };
        let (chunks, ended) = run(bounded(history, IDLE, u32::MAX));
        let stall = gaps.iter().position(|gap| *gap > u64::from(2 * IDLE));
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
