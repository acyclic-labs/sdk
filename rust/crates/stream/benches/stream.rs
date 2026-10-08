//! Local Stream hot paths (see `docs/observability.md`).
#![allow(clippy::unwrap_used, reason = "benchmarks abort on setup failure")]

use acyclic_stream::{
    AppendRequest, LocalStream, LocalStreamLimits, ReadRequest, StreamPath, StreamProvider,
    deferring_durability,
};
use bytes::Bytes;
use divan::Bencher;
use divan::counter::ItemsCount;
use futures::TryStreamExt;
use tokio::runtime::Runtime;

fn main() {
    divan::main();
}

fn open(runtime: &Runtime) -> (tempfile::TempDir, LocalStream, StreamPath) {
    let root = tempfile::tempdir().unwrap();
    let stream = runtime
        .block_on(LocalStream::open(root.path(), LocalStreamLimits::default()))
        .unwrap();
    (root, stream, StreamPath::new("bench/log").unwrap())
}

fn append(path: &StreamPath, records: usize) -> AppendRequest {
    AppendRequest {
        path: path.clone(),
        records: vec![Bytes::from_static(&[0x5a; 64]); records],
        if_tail: None,
        idempotency_key: None,
    }
}

/// One durable append of `records` 64-byte records.
#[divan::bench(args = [1, 16])]
fn append_durable(bencher: Bencher, records: usize) {
    let runtime = Runtime::new().unwrap();
    let (_root, stream, path) = open(&runtime);
    bencher.counter(ItemsCount::new(records)).bench_local(|| {
        runtime
            .block_on(stream.append(append(&path, records)))
            .unwrap()
    });
}

/// Sixteen single-record appends that defer durability to one flush.
#[divan::bench]
fn append_deferred_then_flush(bencher: Bencher) {
    let runtime = Runtime::new().unwrap();
    let (_root, stream, path) = open(&runtime);
    bencher.counter(ItemsCount::new(16_usize)).bench_local(|| {
        runtime.block_on(deferring_durability(async {
            for _ in 0..16 {
                stream.append(append(&path, 1)).await.unwrap();
            }
        }));
        stream.flush().unwrap();
    });
}

/// Reads `records` records of committed history.
#[divan::bench(args = [100, 1000])]
fn read_history(bencher: Bencher, records: u32) {
    let runtime = Runtime::new().unwrap();
    let (_root, stream, path) = open(&runtime);
    runtime
        .block_on(stream.append(append(&path, records as usize)))
        .unwrap();
    bencher.counter(ItemsCount::new(records)).bench_local(|| {
        runtime.block_on(async {
            let request = ReadRequest {
                path: path.clone(),
                from: 0,
                limit: records,
            };
            let read: Vec<_> = stream
                .read(request)
                .await
                .unwrap()
                .try_collect()
                .await
                .unwrap();
            read
        })
    });
}
