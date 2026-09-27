//! Durable local put latency receipt.
//!
//! Run with `cargo test -p acyclic-objects --features local --release --test local_put_latency
//! -- --ignored --nocapture`. `ACYCLIC_OBJECTS_PUTS` sets the measured puts per body size and
//! `ACYCLIC_OBJECTS_PUT_SIZES` a comma-separated list of body sizes in bytes. Prints one JSON
//! line per body size.
#![cfg(feature = "local")]

use std::time::{Duration, Instant};

use acyclic_objects::{LocalObjects, LocalObjectsLimits, ObjectsProvider, PutRequest, wire};

const WARMUP_PUTS: usize = 20;

fn environment_count(name: &str, default: usize) -> Result<usize, Box<dyn std::error::Error>> {
    Ok(match std::env::var(name) {
        Ok(value) => value.parse()?,
        Err(_) => default,
    })
}

fn micros(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000_000.0
}

fn percentile(sorted: &[Duration], percent: usize) -> f64 {
    sorted
        .get(sorted.len().saturating_sub(1) * percent / 100)
        .copied()
        .map_or(f64::NAN, micros)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "local-only durable put latency receipt"]
async fn local_put_latency_receipt() -> Result<(), Box<dyn std::error::Error>> {
    let puts = environment_count("ACYCLIC_OBJECTS_PUTS", 200)?;
    let sizes = match std::env::var("ACYCLIC_OBJECTS_PUT_SIZES") {
        Ok(value) => value
            .split(',')
            .map(str::parse)
            .collect::<Result<Vec<usize>, _>>()?,
        Err(_) => vec![64, 1_024, 4_096, 16_384, 65_536, 262_144],
    };
    for size in sizes {
        let root = tempfile::tempdir()?;
        let limits = LocalObjectsLimits::default();
        let provider = LocalObjects::open(root.path(), limits).await?;
        let bucket = provider
            .create_bucket("latency".into(), None)
            .await?
            .bucket
            .ok_or("bucket reference")?;
        let mut samples = Vec::with_capacity(puts);
        for index in 0..WARMUP_PUTS + puts {
            let mut body = vec![0x5a_u8; size];
            for (byte, value) in body.iter_mut().zip(index.to_le_bytes()) {
                *byte = value;
            }
            let started = Instant::now();
            provider
                .put(PutRequest {
                    bucket: bucket.clone(),
                    object_key: format!("object-{index}"),
                    body: body.into(),
                    metadata: wire::ObjectMetadata::default(),
                    condition: None,
                    idempotency_key: None,
                })
                .await?;
            if index >= WARMUP_PUTS {
                samples.push(started.elapsed());
            }
        }
        drop(provider);
        let reopen_started = Instant::now();
        drop(LocalObjects::open(root.path(), limits).await?);
        let reopen = reopen_started.elapsed();
        samples.sort_unstable();
        let mean = micros(samples.iter().sum::<Duration>() / u32::try_from(samples.len())?);
        println!(
            "{{\"body_bytes\":{size},\"puts\":{puts},\"p50_us\":{:.1},\"p90_us\":{:.1},\"p99_us\":{:.1},\"mean_us\":{mean:.1},\"reopen_us\":{:.1}}}",
            percentile(&samples, 50),
            percentile(&samples, 90),
            percentile(&samples, 99),
            micros(reopen),
        );
    }
    Ok(())
}
