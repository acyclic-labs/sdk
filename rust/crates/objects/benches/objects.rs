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
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
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
    let put = runtime
        .block_on(blobs.put(body(index), Vec::new()))
        .unwrap();
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

const SIZES: [usize; 3] = [4 << 10, 1 << 20, 64 << 20];

/// A distinct body of `length` bytes per index.
fn sized(index: u64, length: usize) -> OwnedBytes {
    let mut bytes = vec![0x5a; length];
    bytes[..8].copy_from_slice(&index.to_le_bytes());
    bytes.into()
}

/// A put and the flush that makes it durable: one hash, one copy, one
/// device flush is the floor.
#[divan::bench(args = SIZES, sample_count = 20)]
fn put_durable(bencher: Bencher, length: usize) {
    let runtime = runtime();
    let (_root, blobs) = store(&runtime);
    let mut index = 0;
    bencher
        .counter(divan::counter::BytesCount::new(length))
        .with_inputs(|| {
            index += 1;
            sized(index, length)
        })
        .bench_local_values(|body| {
            runtime.block_on(async {
                let digest = blobs.put(body, Vec::new()).await.unwrap();
                blobs.flush(None).await.unwrap();
                digest
            })
        });
}

/// Floor term `B_hash`: one content hash of the body.
#[divan::bench(args = SIZES, sample_count = 20)]
fn floor_hash(bencher: Bencher, length: usize) {
    let body = sized(0, length);
    bencher
        .counter(divan::counter::BytesCount::new(length))
        .bench_local(|| acyclic_stream::content_digest(&body));
}

/// Floor term `B_copy`: one copy of the body.
#[divan::bench(args = SIZES, sample_count = 20)]
fn floor_copy(bencher: Bencher, length: usize) {
    let body = sized(0, length);
    bencher
        .counter(divan::counter::BytesCount::new(length))
        .bench_local(|| body.to_vec());
}

/// Floor term `F_flush`: one data flush of a just-written file.
#[divan::bench(sample_count = 20)]
fn floor_flush(bencher: Bencher) {
    use std::io::Write;
    let root = tempfile::tempdir().unwrap();
    let mut file = std::fs::File::create(root.path().join("flush")).unwrap();
    bencher.bench_local(|| {
        file.write_all(&[1]).unwrap();
        file.sync_data().unwrap();
    });
}
