//! Local blob hot paths.
#![allow(clippy::unwrap_used, reason = "benchmarks abort on setup failure")]

use acyclic_objects::{Blobs, ByteRange, Digest, LocalMedium, LocalObjects, Store};
use acyclic_stream::OwnedBytes;
use divan::Bencher;
use divan::counter::ItemsCount;
use tokio::runtime::Runtime;

fn main() {
    divan::main();
}

fn runtime() -> Runtime {
    tokio::runtime::Builder::new_current_thread().build().unwrap()
}

/// Opens a fresh local account.
fn store(runtime: &Runtime) -> (tempfile::TempDir, LocalObjects) {
    let root = tempfile::tempdir().unwrap();
    let medium = runtime
        .block_on(LocalMedium::open(root.path(), Default::default(), None))
        .unwrap();
    (root, Store::new(medium).account(b"bench").unwrap())
}

/// A distinct 200-byte body per index.
fn body(index: u64) -> OwnedBytes {
    let mut bytes = vec![0x5a; 200];
    bytes[..8].copy_from_slice(&index.to_le_bytes());
    bytes.into()
}

fn put(runtime: &Runtime, blobs: &LocalObjects, index: u64) -> Digest {
    let put = runtime.block_on(blobs.put(body(index), Vec::new())).unwrap();
    put.verified().unwrap().digest
}

#[divan::bench]
fn put_small(bencher: Bencher) {
    let runtime = runtime();
    let (_root, blobs) = store(&runtime);
    let mut index = 0;
    bencher.bench_local(|| {
        index += 1;
        put(&runtime, &blobs, index)
    });
}

#[divan::bench]
fn put_batch_10(bencher: Bencher) {
    let runtime = runtime();
    let (_root, blobs) = store(&runtime);
    let mut index = 0;
    bencher.counter(ItemsCount::new(10usize)).bench_local(|| {
        let requests = (0..10)
            .map(|_| {
                index += 1;
                (body(index), Vec::new())
            })
            .collect();
        runtime.block_on(blobs.put_batch(requests))
    });
}

#[divan::bench]
fn get_after_put(bencher: Bencher) {
    let runtime = runtime();
    let (_root, blobs) = store(&runtime);
    let digest = put(&runtime, &blobs, 0);
    bencher.bench_local(|| {
        runtime
            .block_on(blobs.get(digest, ByteRange::FULL, 200))
            .unwrap()
    });
}

#[divan::bench(args = [10, 100, 1000])]
fn list_pages(bencher: Bencher, page_size: u32) {
    let runtime = runtime();
    let (_root, blobs) = store(&runtime);
    for index in 0..1000 {
        put(&runtime, &blobs, index);
    }
    bencher.counter(ItemsCount::new(1000usize)).bench_local(|| {
        let mut after = None;
        let mut listed = 0;
        loop {
            let page = runtime.block_on(blobs.list(after, page_size)).unwrap();
            listed += page.digests.len();
            match page.next {
                Some(next) => after = Some(next),
                None => return listed,
            }
        }
    });
}
