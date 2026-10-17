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

#[divan::bench(args = [0, 1, 4, 16, 32], sample_count = 20, sample_size = 32)]
fn metadata_direct(bencher: divan::Bencher, depth: usize) {
    let parts = callback_path::components(&path(depth));
    assert!(parts.is_some());
    let parts = parts.unwrap_or_default();
    bencher.bench_local(|| {
        let Some(units) = callback_path::wide_components(divan::black_box(&parts)) else {
            unreachable!("canonical benchmark path");
        };
        let mut name = Vec::with_capacity(units.clone().count());
        name.extend(units);
        name
    });
}

#[cfg(windows)]
#[divan::bench(args = [0, 1, 4, 16, 32], sample_count = 20, sample_size = 32)]
fn metadata_previous(bencher: divan::Bencher, depth: usize) {
    use std::os::windows::ffi::{OsStrExt as _, OsStringExt as _};
    let parts = callback_path::components(&path(depth));
    assert!(parts.is_some());
    let parts = parts.unwrap_or_default();
    bencher.bench_local(|| {
        let mut host = std::path::PathBuf::new();
        for part in divan::black_box(&parts) {
            let wide: Vec<_> = part
                .chunks_exact(2)
                .map(|p| u16::from_le_bytes([p[0], p[1]]))
                .collect();
            host.push(std::ffi::OsString::from_wide(&wide));
        }
        let mut name = Vec::with_capacity(host.as_os_str().encode_wide().count());
        name.extend(host.as_os_str().encode_wide());
        name
    });
}
