//! Windows callback decoder CPU, allocation and peak-memory costs.

#[path = "../src/native_mount/projfs/callback_path.rs"]
mod callback_path;
#[cfg(windows)]
#[path = "../tests/support/projfs_legacy_path.rs"]
mod legacy;

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    if std::env::var_os("ACYCLIC_BENCH_TRACE").is_some() {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_writer(std::io::sink)
            .init();
    }
    divan::main();
}

fn path(depth: usize) -> Vec<u16> {
    std::iter::repeat_n("entry000", depth)
        .collect::<Vec<_>>()
        .join("\\")
        .encode_utf16()
        .collect()
}

#[divan::bench(args = [0, 1, 4, 16, 32], sample_count = 20, sample_size = 32)]
fn direct(bencher: divan::Bencher, depth: usize) {
    let wide = path(depth);
    bencher.bench_local(|| callback_path::components(divan::black_box(&wide)));
}

#[cfg(windows)]
#[divan::bench(args = [0, 1, 4, 16, 32], sample_count = 20, sample_size = 32)]
fn previous(bencher: divan::Bencher, depth: usize) {
    let wide = path(depth);
    bencher.bench_local(|| legacy::components(divan::black_box(&wide)));
}
