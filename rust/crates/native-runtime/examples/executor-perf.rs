//! Same-operation calibration for the shared coordination executor.
//! Run in release mode; the sample count is a measurement size, not a runtime limit.
use std::{future::Future, io, time::Instant};

async fn sample<F, Fut>(samples: usize, mut operation: F) -> io::Result<u128>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = io::Result<()>>,
{
    // Warm the exact path before retaining observations.
    operation().await?;
    let mut times = Vec::with_capacity(samples);
    for _ in 0..samples {
        let start = Instant::now();
        operation().await?;
        times.push(start.elapsed().as_nanos());
    }
    times.sort_unstable();
    Ok(times[samples / 2])
}

async fn operation(kind: &str) {
    match kind {
        "ready" => std::hint::black_box(()),
        "yield" => tokio::task::yield_now().await,
        "oneshot" => {
            let (send, receive) = tokio::sync::oneshot::channel();
            send.send(7).unwrap();
            assert_eq!(receive.await.unwrap(), 7);
        }
        "notify" => {
            let notify = tokio::sync::Notify::new();
            notify.notify_one();
            notify.notified().await;
        }
        _ => unreachable!(),
    }
}

fn main() -> io::Result<()> {
    let samples: usize = std::env::args()
        .nth(1)
        .map_or(Ok(10_001), |value| value.parse())
        .map_err(io::Error::other)?;
    if samples == 0 {
        return Err(io::Error::other("sample count must be positive"));
    }
    let start = Instant::now();
    // Retain the exact former construction only as a before/after experiment,
    // so both measurements use identical code, compiler and machine state.
    let runtime = if std::env::args().nth(2).as_deref() == Some("before") {
        tokio::runtime::Builder::new_current_thread()
            .max_blocking_threads(1)
            .enable_all()
            .build()?
    } else {
        acyclic_native_runtime::coordination_runtime()?
    };
    println!(
        "runtime workers={} construction_ns={} samples={samples}",
        runtime.metrics().num_workers(),
        start.elapsed().as_nanos(),
    );
    // Keep producer and children on the executor, just like a serving task.
    // Raw Tokio on this same executor calibrates the scheduler/allocation floor;
    // it is measured independently for each identical operation, not subtracted.
    runtime.block_on(runtime.spawn(async move {
        for kind in ["ready", "yield", "oneshot", "notify"] {
            let floor = sample(samples, || async move {
                tokio::spawn(operation(kind))
                    .await
                    .map_err(io::Error::other)
            })
            .await?;
            let local = sample(samples, || async move {
                acyclic_native_runtime::spawn(operation(kind)).await
            })
            .await?;
            println!(
                "op={kind} floor_p50_ns={floor} local_p50_ns={local} ratio={:.3}",
                local as f64 / floor as f64
            );
        }
        Ok::<_, io::Error>(())
    }))??;
    Ok(())
}
