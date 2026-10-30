//! Generated request histories advance the caller's logical clock and release
//! headers/chunks through channels. Only the chosen request deadline owns a timer.
use super::*;
use futures::channel::{mpsc, oneshot};
use proptest::prelude::*;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Clone, Copy)]
enum Timer {
    Clock,
    Starved,
    Early,
}

struct Script {
    response: Mutex<Option<oneshot::Receiver<Result<HttpResponse>>>>,
    timer: Timer,
    arms: Arc<AtomicUsize>,
    sends: Arc<AtomicUsize>,
    clock_reads: AtomicUsize,
    origin: tokio::time::Instant,
}

impl HttpTransport for Script {
    fn send(&self, _: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        self.sends.fetch_add(1, Ordering::SeqCst);
        let response = self.response.lock().unwrap().take();
        Box::pin(async move {
            response
                .ok_or_else(|| crate::invalid("response was already consumed"))?
                .await
                .map_err(|_| {
                    crate::transport_failure(
                        false,
                        ModelTransportFailure::Interrupted,
                        "response producer disappeared",
                    )
                })?
        })
    }

    fn delay(&self, wait: Duration) -> BoxProviderFuture<'static, ()> {
        self.arms.fetch_add(1, Ordering::SeqCst);
        let timer = self.timer;
        Box::pin(async move {
            match timer {
                Timer::Clock => tokio::time::sleep(wait).await,
                Timer::Starved => futures::future::pending().await,
                Timer::Early => {}
            }
        })
    }

    fn now(&self) -> Duration {
        self.clock_reads.fetch_add(1, Ordering::SeqCst);
        tokio::time::Instant::now().saturating_duration_since(self.origin)
    }

    fn delay_with_native_work(
        &self,
        wait: Duration,
        work: &Work,
    ) -> Result<acyclic_harness_core::NativeFutureBundle<'static, ()>> {
        let timer = self.timer;
        let arms = Arc::clone(&self.arms);
        acyclic_harness_core::NativeFutureBundle::new(work, move || {
            arms.fetch_add(1, Ordering::SeqCst);
            async move {
                match timer {
                    Timer::Clock => tokio::time::sleep(wait).await,
                    Timer::Starved => futures::future::pending().await,
                    Timer::Early => {}
                }
                Ok(())
            }
        })
    }

    fn send_with_native_work<'a>(
        &'a self,
        request: HttpRequest,
        work: &'a Work,
    ) -> Result<acyclic_harness_core::NativeFutureBundle<'a, HttpResponse>> {
        acyclic_harness_core::NativeFutureBundle::new(work, || async move {
            let response = self.send(request).await?;
            HttpResponse::with_native_body(response.status, response.body, work)
        })
    }
}

fn script(timer: Timer) -> (Arc<Script>, oneshot::Sender<Result<HttpResponse>>) {
    let (headers, response) = oneshot::channel();
    (
        Arc::new(Script {
            response: Mutex::new(Some(response)),
            timer,
            arms: Arc::default(),
            sends: Arc::default(),
            clock_reads: AtomicUsize::new(0),
            origin: tokio::time::Instant::now(),
        }),
        headers,
    )
}

fn request() -> HttpRequest {
    HttpRequest {
        url: "http://127.0.0.1:1/v1".into(),
        headers: Default::default(),
        body: Bytes::new(),
    }
}

fn response(body: BoxProviderStream<'static, Result<Bytes>>) -> HttpResponse {
    HttpResponse {
        status: 200,
        body,
        retry_after: None,
    }
}

fn cause(error: &Error) -> &str {
    match error {
        Error::ModelUnavailable {
            failure: ModelTransportFailure::Timeout,
            excerpt,
        } => excerpt,
        other => panic!("untyped deadline failure: {other:?}"),
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    // Header and chunk publication cannot restart the one request deadline.
    // The timer registration count stays constant through every generated history.
    #[test]
    fn request_history_keeps_one_deadline_event(
        head in 0..1_000u64,
        gaps in proptest::collection::vec(0..1_000u64, 0..16),
        total in 0..10_000u64,
    ) {
        runtime().block_on(async {
            let (script, headers) = script(Timer::Clock);
            let transport = Bounded::new(script.clone(), TimeoutSpec { total_ms: Some(total) });
            let (chunks, body) = mpsc::unbounded();
            let mut send = pin!(transport.send(request()));
            if total == 0 {
                let error = send.await.err().expect("zero deadline refuses dispatch");
                assert_eq!(cause(&error), "exchange exceeded 0 ms");
                assert_eq!(script.sends.load(Ordering::SeqCst), 0);
                assert_eq!(script.arms.load(Ordering::SeqCst), 0);
                return;
            }
            assert!(send.as_mut().now_or_never().is_none());
            tokio::time::advance(Duration::from_millis(head)).await;
            assert!(headers.send(Ok(response(Box::pin(body)))).is_ok());
            let response = send.await;
            if head >= total {
                assert_eq!(cause(&response.err().expect("headers expired")), format!("exchange exceeded {total} ms"));
            } else {
                let mut body = response.expect("headers before expiry").body;
                let mut elapsed = head;
                for gap in gaps {
                    let item = {
                        let mut next = pin!(body.next());
                        assert!(next.as_mut().now_or_never().is_none());
                        tokio::time::advance(Duration::from_millis(gap)).await;
                        chunks.unbounded_send(Ok(Bytes::from_static(b"x"))).unwrap();
                        next.await.expect("chunk or deadline")
                    };
                    elapsed += gap;
                    if elapsed >= total {
                        assert_eq!(cause(&item.expect_err("fixed request deadline")), format!("exchange exceeded {total} ms"));
                        assert!(body.next().await.is_none());
                        assert!(chunks.unbounded_send(Ok(Bytes::new())).is_err());
                        assert_eq!(script.arms.load(Ordering::SeqCst), 1);
                        return;
                    }
                    assert_eq!(item.unwrap(), Bytes::from_static(b"x"));
                }
                drop(chunks);
                assert!(body.next().await.is_none());
            }
            assert_eq!(script.arms.load(Ordering::SeqCst), 1);
        });
    }

    // A starved timer cannot admit newly ready bytes or EOF after expiry.
    // The underlying ready producer is a negative control for the clock check.
    #[test]
    fn expiry_precedes_ready_bytes_and_eof(eof in any::<bool>(), partial in any::<bool>(), late in 0..1_000u64) {
        runtime().block_on(async {
            let (script, headers) = script(Timer::Starved);
            let transport = Bounded::new(script, TimeoutSpec { total_ms: Some(100) });
            let (chunks, body) = mpsc::unbounded();
            assert!(headers.send(Ok(response(Box::pin(body)))).is_ok());
            let mut body = transport.send(request()).await.unwrap().body;
            if partial {
                chunks.unbounded_send(Ok(Bytes::from_static(b"prefix"))).unwrap();
                assert!(body.next().await.unwrap().is_ok());
            }
            assert!(body.next().now_or_never().is_none());
            tokio::time::advance(Duration::from_millis(100 + late)).await;
            if !eof { chunks.unbounded_send(Ok(Bytes::from_static(b"tail"))).unwrap(); }
            drop(chunks);
            let error = body.next().await.unwrap().expect_err("expiry precedes ready producer");
            assert_eq!(cause(&error), "exchange exceeded 100 ms");
            assert!(body.next().await.is_none());
        });
    }
}

#[test]
fn unbounded_request_waits_only_on_its_producers() {
    runtime().block_on(async {
        let (script, headers) = script(Timer::Early);
        let transport = Bounded::new(script.clone(), TimeoutSpec::DEFAULT);
        let (chunks, body) = mpsc::unbounded();
        let mut send = pin!(transport.send(request()));
        assert!(send.as_mut().now_or_never().is_none());
        tokio::time::advance(Duration::from_secs(365 * 24 * 60 * 60)).await;
        assert!(send.as_mut().now_or_never().is_none());
        assert!(headers.send(Ok(response(Box::pin(body)))).is_ok());
        let mut body = send.await.unwrap().body;
        assert!(body.next().now_or_never().is_none());
        tokio::time::advance(Duration::from_secs(365 * 24 * 60 * 60)).await;
        chunks
            .unbounded_send(Ok(Bytes::from_static(b"released")))
            .unwrap();
        assert_eq!(
            body.next().await.unwrap().unwrap(),
            Bytes::from_static(b"released")
        );
        drop(chunks);
        assert!(body.next().await.is_none());
        assert_eq!(script.arms.load(Ordering::SeqCst), 0);
        assert_eq!(script.clock_reads.load(Ordering::SeqCst), 0);

        // Losing a producer terminates the unbounded wait with its typed error.
        let (script, headers) = self::script(Timer::Early);
        let transport = Bounded::new(script, TimeoutSpec::DEFAULT);
        drop(headers);
        assert!(matches!(
            transport.send(request()).await,
            Err(Error::ModelUnavailable {
                failure: ModelTransportFailure::Interrupted,
                ..
            })
        ));
    });
}

#[test]
fn an_early_timer_is_terminal_instead_of_rearmed() {
    runtime().block_on(async {
        let (script, headers) = script(Timer::Early);
        let transport = Bounded::new(
            script.clone(),
            TimeoutSpec {
                total_ms: Some(100),
            },
        );
        let (chunks, body) = mpsc::unbounded();
        assert!(headers.send(Ok(response(Box::pin(body)))).is_ok());
        let mut body = transport.send(request()).await.unwrap().body;
        assert!(matches!(body.next().await, Some(Err(Error::Invalid(_)))));
        assert!(body.next().await.is_none());
        assert_eq!(script.arms.load(Ordering::SeqCst), 1);
        assert!(chunks.unbounded_send(Ok(Bytes::new())).is_err());
    });
}

#[test]
fn native_timeout_does_not_allocate_a_discarded_diagnostic() {
    let allocated = acyclic_test_support::measure(|| {
        assert!(
            matches!(timeout(true, 100), Error::ModelUnavailable { excerpt, .. } if excerpt.is_empty())
        );
    });
    assert_eq!(allocated.count_total, 0);
    let discarded = acyclic_test_support::measure(|| {
        std::hint::black_box(format!("exchange exceeded {} ms", 100));
    });
    assert!(discarded.count_total > 0);
}

#[test]
fn native_deadline_timer_is_admitted_once_on_the_original_ledger() {
    runtime().block_on(async {
        let work = Work::new(
            acyclic_fs::performance::work_budget(65_536, 64 * 1024 * 1024),
            &acyclic_fs::CancellationToken::new(),
        )
        .unwrap_or_else(|_| panic!("ledger"));
        let before = work.live_allocation_bytes();
        let (script, headers) = script(Timer::Clock);
        let transport = Bounded::new(
            script.clone(),
            TimeoutSpec {
                total_ms: Some(100),
            },
        );
        let (chunks, body) = mpsc::unbounded();
        assert!(headers.send(Ok(response(Box::pin(body)))).is_ok());
        let mut body = transport
            .send_with_native_work(request(), &work)
            .unwrap()
            .await
            .unwrap()
            .body;
        let mut context = Context::from_waker(futures::task::noop_waker_ref());
        let admitted = work.live_allocation_bytes();
        let allocated = acyclic_test_support::measure(|| {
            assert!(body.poll_next_unpin(&mut context).is_pending());
        });
        let claimed = work.live_allocation_bytes() - admitted;
        assert_eq!(allocated.count_total, 1);
        assert!(claimed > 0);
        assert_eq!(allocated.bytes_total, claimed);
        let again = acyclic_test_support::measure(|| {
            assert!(body.poll_next_unpin(&mut context).is_pending());
        });
        assert_eq!(again.count_total, 0);
        assert_eq!(script.arms.load(Ordering::SeqCst), 1);
        drop(body);
        drop(chunks);
        assert_eq!(work.live_allocation_bytes(), before);
    });
}
