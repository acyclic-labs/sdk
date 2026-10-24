//! Reproducible real-device group-commit benchmark. Run an optimized build.
use acyclic_stream::{
    AppendRequest, LocalStream, LocalStreamLimits, OwnedBytes, StreamPath, StreamProvider,
};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Instant;
use tracing_subscriber::{Layer, layer::SubscriberExt as _};

#[derive(Clone, Default)]
struct Flushes(Arc<AtomicU64>);
impl<S: tracing::Subscriber> Layer<S> for Flushes {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        struct Visitor<'a>(&'a AtomicU64);
        impl tracing::field::Visit for Visitor<'_> {
            fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
                if field.name() == "fsyncs" {
                    self.0.fetch_add(value, Ordering::Relaxed);
                }
            }
            fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
        }
        event.record(&mut Visitor(&self.0));
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let iterations: usize = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "256".into())
        .parse()?;
    let rounds: usize = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "5".into())
        .parse()?;
    if iterations == 0 || rounds == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "iterations and rounds must be positive",
        )
        .into());
    }
    let flushes = Flushes::default();
    tracing::subscriber::set_global_default(
        tracing_subscriber::Registry::default().with(flushes.clone()),
    )?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?;
    println!("round,writers,bytes,operations,fsyncs,ops_per_second,p50_us,p95_us,journal_bytes");
    runtime.block_on(async {
        for round in 0..rounds {
            for (writers, bytes) in [(1, 256), (8, 256), (32, 256), (8, 64 * 1024)] {
                let directory = tempfile::tempdir()?;
                let stream =
                    LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
                let barrier = Arc::new(tokio::sync::Barrier::new(writers + 1));
                let mut handles = Vec::new();
                for writer in 0..writers {
                    let stream = stream.clone();
                    let barrier = barrier.clone();
                    handles.push(tokio::spawn(async move {
                        let path = StreamPath::new(format!("bench/{writer}"))?;
                        let payload = OwnedBytes::from(vec![42; bytes]);
                        let mut latencies = Vec::with_capacity(iterations);
                        barrier.wait().await;
                        for _ in 0..iterations {
                            let start = Instant::now();
                            stream
                                .append(AppendRequest {
                                    path: path.clone(),
                                    records: vec![payload.clone().into()],
                                    if_tail: None,
                                    idempotency_key: None,
                                })
                                .await?;
                            latencies.push(start.elapsed().as_micros());
                        }
                        Ok::<_, acyclic_stream::StreamError>(latencies)
                    }));
                }
                flushes.0.store(0, Ordering::Relaxed);
                let start = Instant::now();
                barrier.wait().await;
                let mut latencies = Vec::new();
                for handle in handles {
                    latencies.extend(handle.await??);
                }
                let elapsed = start.elapsed().as_secs_f64();
                latencies.sort_unstable();
                let operations = writers * iterations;
                let journal_bytes =
                    std::fs::metadata(directory.path().join("stream.journal"))?.len();
                println!(
                    "{round},{writers},{bytes},{operations},{},{:.0},{},{},{journal_bytes}",
                    flushes.0.load(Ordering::Relaxed),
                    operations as f64 / elapsed,
                    latencies[operations / 2],
                    latencies[operations * 95 / 100]
                );
                drop(stream);
                let reopened =
                    LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
                for writer in 0..writers {
                    assert_eq!(
                        reopened
                            .tail(StreamPath::new(format!("bench/{writer}"))?)
                            .await?,
                        iterations as u64
                    );
                }
            }
        }
        Ok::<_, Box<dyn std::error::Error>>(())
    })
}
