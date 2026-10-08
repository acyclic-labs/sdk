//! Positional file I/O and sync cost (see `docs/observability.md`).
#![allow(clippy::unwrap_used, reason = "benchmarks abort on setup failure")]

use acyclic_native_runtime::{Durability, read_at, sync_data, write_all_at};
use divan::Bencher;
use divan::counter::BytesCount;
use std::fs::File;

fn main() {
    divan::main();
}

fn file(bytes: usize) -> (tempfile::TempDir, File) {
    let root = tempfile::tempdir().unwrap();
    let file = File::options()
        .read(true)
        .write(true)
        .create_new(true)
        .open(root.path().join("data"))
        .unwrap();
    write_all_at(&file, 0, &vec![0x5a; bytes]).unwrap();
    (root, file)
}

#[divan::bench(args = [4096, 1 << 20])]
fn read_at_bytes(bencher: Bencher, bytes: usize) {
    let (_root, file) = file(bytes);
    let mut buffer = vec![0; bytes];
    bencher
        .counter(BytesCount::new(bytes))
        .bench_local(|| read_at(&file, 0, &mut buffer).unwrap());
}

#[divan::bench(args = [4096, 1 << 20])]
fn write_all_at_bytes(bencher: Bencher, bytes: usize) {
    let (_root, file) = file(bytes);
    let buffer = vec![0xa5; bytes];
    bencher
        .counter(BytesCount::new(bytes))
        .bench_local(|| write_all_at(&file, 0, &buffer).unwrap());
}

/// A 4 KiB overwrite followed by a full data sync.
#[divan::bench]
fn write_then_sync_data(bencher: Bencher) {
    let (_root, file) = file(4096);
    let buffer = vec![0xa5; 4096];
    bencher.bench_local(|| {
        write_all_at(&file, 0, &buffer).unwrap();
        sync_data(&file, Durability::Full).unwrap();
    });
}
