//! Measures how one local Stream root behaves under concurrent writers on
//! distinct paths while another writer publishes large commits.
//!
//! ```text
//! cargo run --release -p acyclic-stream --features local --example local-contention -- \
//!     [--workers N] [--writers N] [--ops N] [--small-bytes N] [--large-bytes N] [--durability full|barrier]
//! ```
//!
//! Every small writer commits one small record per operation to its own
//! authority (`agents/<i>/memory`); one large writer commits a large record per
//! operation to `publish/blob` until the small writers finish; one reader polls
//! the tail of a small writer's path. Per-operation latencies are reported as
//! p50/p95/max together with throughput.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use acyclic_stream::{
    CommitCondition, CommitMutation, CommitOutcome, CommitRequest, IdempotencyKey, LocalDurability,
    LocalStream, LocalStreamLimits, MAX_RECORD_BYTES, MemoryLimits, StreamPath, StreamProvider,
};
use bytes::Bytes;

struct Options {
    workers: usize,
    writers: usize,
    ops: usize,
    small_bytes: usize,
    large_bytes: usize,
    durability: LocalDurability,
}

fn options() -> Result<Options, String> {
    let mut options = Options {
        workers: 1,
        writers: 8,
        ops: 100,
        small_bytes: 256,
        large_bytes: 960 * 1024,
        durability: LocalDurability::FullFlush,
    };
    let mut arguments = std::env::args().skip(1);
    while let Some(flag) = arguments.next() {
        let value = arguments
            .next()
            .ok_or_else(|| format!("{flag} needs a value"))?;
        let number = || {
            value
                .parse::<usize>()
                .map_err(|error| format!("{flag}: {error}"))
        };
        match flag.as_str() {
            "--workers" => options.workers = number()?,
            "--writers" => options.writers = number()?,
            "--ops" => options.ops = number()?,
            "--small-bytes" => options.small_bytes = number()?,
            "--large-bytes" => options.large_bytes = number()?,
            "--durability" => {
                options.durability = match value.as_str() {
                    "full" => LocalDurability::FullFlush,
                    "barrier" => LocalDurability::Barrier,
                    other => return Err(format!("unknown durability {other}")),
                }
            }
            other => return Err(format!("unknown flag {other}")),
        }
    }
    Ok(options)
}

fn summary(label: &str, mut samples: Vec<Duration>, elapsed: Duration) {
    if samples.is_empty() {
        println!("{label:<8} no samples");
        return;
    }
    samples.sort_unstable();
    let at = |percent: usize| {
        let index = (samples.len() - 1) * percent / 100;
        samples
            .get(index)
            .map_or(0.0, |sample| sample.as_secs_f64() * 1000.0)
    };
    let count = u32::try_from(samples.len()).map_or(f64::MAX, f64::from);
    println!(
        "{label:<8} n={:<6} p50={:>8.2}ms p95={:>8.2}ms max={:>8.2}ms throughput={:>8.1}/s",
        samples.len(),
        at(50),
        at(95),
        at(100),
        count / elapsed.as_secs_f64(),
    );
}

/// One append of `bytes` to `path`, split into records of at most
/// [`MAX_RECORD_BYTES`].
/// The path's tail advances by the record count.
fn commit(
    path: &StreamPath,
    tail: &mut u64,
    bytes: usize,
    key: String,
) -> Result<CommitRequest, String> {
    let records: Vec<Bytes> = (0..bytes.div_ceil(MAX_RECORD_BYTES))
        .map(|index| {
            let length = (bytes - index * MAX_RECORD_BYTES).min(MAX_RECORD_BYTES);
            Bytes::from(vec![b'r'; length])
        })
        .collect();
    let condition = if *tail == 0 {
        CommitCondition::Absent { path: path.clone() }
    } else {
        CommitCondition::Tail {
            path: path.clone(),
            expected: *tail,
        }
    };
    *tail += records.len() as u64;
    Ok(CommitRequest {
        conditions: vec![condition],
        mutations: vec![CommitMutation::Append {
            path: path.clone(),
            records,
        }],
        idempotency_key: IdempotencyKey::new(Bytes::from(key)).map_err(|e| e.to_string())?,
    })
}

async fn run(options: Options) -> Result<(), String> {
    let directory = std::env::temp_dir().join(format!("local-contention-{}", uuid_like()));
    let limits = LocalStreamLimits {
        memory: MemoryLimits {
            payload_bytes: 4 * 1024 * 1024 * 1024,
            ..MemoryLimits::default()
        },
        journal_bytes: 8 * 1024 * 1024 * 1024,
        durability: options.durability,
        ..LocalStreamLimits::default()
    };
    let provider = LocalStream::open(&directory, limits)
        .await
        .map_err(|e| e.to_string())?;
    let done = Arc::new(AtomicBool::new(false));
    let started = Instant::now();

    let mut small = Vec::new();
    for writer in 0..options.writers {
        let provider = provider.clone();
        let size = options.small_bytes;
        let ops = options.ops;
        small.push(tokio::spawn(async move {
            let path =
                StreamPath::new(format!("agents/{writer}/memory")).map_err(|e| e.to_string())?;
            let mut latencies = Vec::with_capacity(ops);
            let mut tail = 0;
            for op in 0..ops {
                let request = commit(&path, &mut tail, size, format!("s-{writer}-{op}"))?;
                let at = Instant::now();
                match provider.commit(request).await.map_err(|e| e.to_string())? {
                    CommitOutcome::Committed(_) => {}
                    CommitOutcome::Conflict(conflicts) => return Err(format!("{conflicts:?}")),
                }
                latencies.push(at.elapsed());
            }
            Ok::<_, String>(latencies)
        }));
    }
    let large = {
        let (provider, done) = (provider.clone(), Arc::clone(&done));
        let size = options.large_bytes;
        tokio::spawn(async move {
            let path = StreamPath::new("publish/blob").map_err(|e| e.to_string())?;
            let mut latencies = Vec::new();
            let mut tail = 0;
            let mut op = 0;
            while !done.load(Ordering::Acquire) && size > 0 {
                let request = commit(&path, &mut tail, size, format!("l-{op}"))?;
                let at = Instant::now();
                match provider.commit(request).await.map_err(|e| e.to_string())? {
                    CommitOutcome::Committed(_) => {}
                    CommitOutcome::Conflict(conflicts) => return Err(format!("{conflicts:?}")),
                }
                latencies.push(at.elapsed());
                op += 1;
            }
            Ok::<_, String>(latencies)
        })
    };
    let reader = {
        let (provider, done) = (provider.clone(), Arc::clone(&done));
        tokio::spawn(async move {
            let path = StreamPath::new("agents/0/memory").map_err(|e| e.to_string())?;
            let mut latencies = Vec::new();
            while !done.load(Ordering::Acquire) {
                let at = Instant::now();
                let _ = provider.tail(path.clone()).await;
                latencies.push(at.elapsed());
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Ok::<_, String>(latencies)
        })
    };

    let mut small_latencies = Vec::new();
    for writer in small {
        small_latencies.extend(writer.await.map_err(|e| e.to_string())??);
    }
    let small_elapsed = started.elapsed();
    done.store(true, Ordering::Release);
    let large_latencies = large.await.map_err(|e| e.to_string())??;
    let read_latencies = reader.await.map_err(|e| e.to_string())??;
    let elapsed = started.elapsed();
    drop(provider);
    let _ = std::fs::remove_dir_all(&directory);

    println!(
        "workers={} writers={} ops={} small={}B large={}B durability={:?}",
        options.workers,
        options.writers,
        options.ops,
        options.small_bytes,
        options.large_bytes,
        options.durability
    );
    summary("small", small_latencies, small_elapsed);
    summary("large", large_latencies, elapsed);
    summary("read", read_latencies, elapsed);
    Ok(())
}

fn uuid_like() -> String {
    format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos())
    )
}

fn main() {
    let result = options().and_then(|options| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(options.workers)
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?
            .block_on(run(options))
    });
    if let Err(error) = result {
        eprintln!("local-contention failed: {error}");
        std::process::exit(1);
    }
}
