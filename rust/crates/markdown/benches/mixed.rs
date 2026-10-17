//! Mixed-document append latency and scaling gate.

use acyclic_markdown::Markdown;
use std::{
    hint::black_box,
    time::{Duration, Instant},
};

#[path = "../tests/fixtures/mixed.rs"]
mod mixed;

const MIB: usize = 1024 * 1024;
const SAMPLES: usize = 128;

struct Timing {
    total: Duration,
    p50: Duration,
    p99: Duration,
}

fn percentile(samples: &mut [Duration], percent: usize) -> Duration {
    samples.sort_unstable();
    samples
        .get(samples.len().saturating_sub(1) * percent / 100)
        .copied()
        .unwrap_or_default()
}

fn measure_pair(small: &str, large: &str, chunk: usize) -> Result<(Timing, Timing), &'static str> {
    let mut renderers = [Markdown::default(), Markdown::default()];
    let mut samples = [
        Vec::with_capacity(small.len().div_ceil(chunk)),
        Vec::with_capacity(large.len().div_ceil(chunk)),
    ];
    let mut chunks = [
        small.as_bytes().chunks(chunk),
        large.as_bytes().chunks(chunk),
    ];
    // Advance both documents proportionally in the same scheduling window.
    // Sequential whole-document runs confound doubling with changing VM load.
    for index in 0..small.len().div_ceil(chunk) {
        for which in [index % 2, 1 - index % 2] {
            for _ in 0..=which {
                let markdown = renderers.get_mut(which).ok_or("renderer index")?;
                let samples = samples.get_mut(which).ok_or("sample index")?;
                let Some(bytes) = chunks.get_mut(which).ok_or("chunk index")?.next() else {
                    break;
                };
                let text = std::str::from_utf8(bytes).map_err(|_| "ASCII benchmark fixture")?;
                let start = Instant::now();
                black_box(markdown.push(black_box(text)));
                samples.push(start.elapsed());
            }
        }
    }
    let [small, large] = samples.map(|mut samples| Timing {
        total: samples.iter().sum(),
        p50: percentile(&mut samples, 50),
        p99: percentile(&mut samples, 99),
    });
    Ok((small, large))
}

fn history(source: &str, chunk: usize) -> Result<(Duration, Duration), &'static str> {
    let mut samples = [Vec::new(), Vec::new()];
    // Sample complete short windows in ABBA order. Interleaving individual
    // pushes makes two provisional highlighters compete for the same allocator
    // cache; it measures that interaction as well as finalized history.
    for round in 0..4 {
        let mut renderers = [Markdown::default(), Markdown::default()];
        for (markdown, offset) in renderers.iter_mut().zip([1024, MIB]) {
            markdown.push(source.get(..offset).ok_or("benchmark history")?);
        }
        for which in [round % 2, 1 - round % 2] {
            for bytes in source
                .as_bytes()
                .get(MIB..MIB + SAMPLES * chunk)
                .ok_or("benchmark sample window")?
                .chunks(chunk)
            {
                let text = std::str::from_utf8(bytes).map_err(|_| "ASCII benchmark fixture")?;
                {
                    let markdown = renderers.get_mut(which).ok_or("history renderer")?;
                    let samples = samples.get_mut(which).ok_or("history samples")?;
                    let start = Instant::now();
                    black_box(markdown.push(black_box(text)));
                    samples.push(start.elapsed());
                }
            }
        }
    }
    let [early, late] = samples.map(|mut samples| percentile(&mut samples, 99));
    Ok((early, late))
}

fn main() -> Result<(), &'static str> {
    let small = mixed::document(MIB);
    let large = mixed::document(2 * MIB);
    // Warm the syntax/theme tables and the parser before collecting latencies.
    black_box(measure_pair(&small, &large, 4096)?);
    for chunk in [1, 7, 4096] {
        let (early, late) = history(&large, chunk)?;
        let (small, large) = measure_pair(&small, &large, chunk)?;
        let total_ratio = large.total.as_secs_f64() / small.total.as_secs_f64();
        let history_ratio = late.as_secs_f64() / early.as_secs_f64();
        println!(
            "chunk={chunk}: 1MiB total={:.3}ms p50={:.3}us p99={:.3}us; 2MiB total={:.3}ms p50={:.3}us p99={:.3}us; total ratio={total_ratio:.3}; p99 at 1KiB/1MiB={:.3}/{:.3}us ratio={history_ratio:.3}",
            small.total.as_secs_f64() * 1000.0,
            small.p50.as_secs_f64() * 1e6,
            small.p99.as_secs_f64() * 1e6,
            large.total.as_secs_f64() * 1000.0,
            large.p50.as_secs_f64() * 1e6,
            large.p99.as_secs_f64() * 1e6,
            early.as_secs_f64() * 1e6,
            late.as_secs_f64() * 1e6,
        );
        assert!(
            total_ratio <= 2.2,
            "total append time grew faster than document size"
        );
        assert!(
            history_ratio <= 1.5,
            "per-chunk p99 grew with finalized history"
        );
    }
    Ok(())
}
