//! Native write-range validation time and temporary allocations.

use acyclic_native_runtime::OwnedWrite;
use bytes::Bytes;
use divan::{AllocProfiler, Bencher, black_box};

#[path = "../../native-runtime/src/file/write_validation.rs"]
mod write_validation;

#[global_allocator]
static ALLOC: AllocProfiler = AllocProfiler::system();

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var("NATIVE_BENCH_TRACING").as_deref() == Ok("1") {
        tracing::subscriber::set_global_default(
            tracing_subscriber::fmt()
                .with_max_level(tracing::Level::TRACE)
                .with_writer(std::io::sink)
                .finish(),
        )?;
    }
    divan::main();
    Ok(())
}

#[divan::bench(args = [0, 1, 16, 256, 4096])]
fn ordered(bencher: Bencher, count: usize) {
    let writes: Vec<_> = (0..count)
        .map(|index| OwnedWrite {
            offset: index as u64 * 8,
            bytes: Bytes::from_static(b"abcd"),
        })
        .collect();
    bencher.bench_local(|| write_validation::validate_write_batch(black_box(&writes)));
}

#[divan::bench(args = [1, 16, 256, 4096])]
fn reversed(bencher: Bencher, count: usize) {
    let writes: Vec<_> = (0..count)
        .rev()
        .map(|index| OwnedWrite {
            offset: index as u64 * 8,
            bytes: Bytes::from_static(b"abcd"),
        })
        .collect();
    bencher.bench_local(|| write_validation::validate_write_batch(black_box(&writes)));
}
