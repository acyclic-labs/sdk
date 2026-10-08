//! Local Objects hot paths (see `docs/observability.md`).
#![allow(clippy::unwrap_used, reason = "benchmarks abort on setup failure")]

use acyclic_objects::v1::{MemoryObjects, NativeBatchObjects, ObjectsProvider, wire};
use acyclic_objects::{LocalObjects, LocalObjectsLimits};
use bytes::Bytes;
use divan::Bencher;
use divan::counter::ItemsCount;
use std::path::Path;
use tokio::runtime::Runtime;

const BODY: Bytes = Bytes::from_static(&[0x5a; 200]);

fn main() {
    divan::main();
}

fn bucket() -> wire::BucketRef {
    wire::BucketRef {
        name: "bench".into(),
    }
}

fn header(key: String) -> wire::PutObjectHeader {
    wire::PutObjectHeader {
        bucket: Some(bucket()),
        object_key: key,
        ..Default::default()
    }
}

fn get(key: String) -> wire::GetObjectRequest {
    wire::GetObjectRequest {
        bucket: Some(bucket()),
        object_key: key,
        ..Default::default()
    }
}

fn open(runtime: &Runtime, root: &Path) -> LocalObjects {
    runtime
        .block_on(LocalObjects::open(root, LocalObjectsLimits::default()))
        .unwrap()
}

/// Opens a fresh store holding the `bench` bucket.
fn store(runtime: &Runtime) -> (tempfile::TempDir, LocalObjects) {
    let root = tempfile::tempdir().unwrap();
    let objects = open(runtime, root.path());
    runtime
        .block_on(objects.create_bucket(wire::CreateBucketRequest {
            name: "bench".into(),
            mutation: None,
        }))
        .unwrap();
    (root, objects)
}

#[divan::bench]
fn put_small(bencher: Bencher) {
    let runtime = Runtime::new().unwrap();
    let (_root, objects) = store(&runtime);
    let mut next = 0_u64;
    bencher.bench_local(|| {
        next += 1;
        runtime
            .block_on(objects.put(header(format!("put/{next}")), BODY))
            .unwrap()
    });
}

#[divan::bench]
fn put_batch_10(bencher: Bencher) {
    let runtime = Runtime::new().unwrap();
    let (_root, objects) = store(&runtime);
    let mut next = 0_u64;
    bencher
        .counter(ItemsCount::new(10_usize))
        .with_inputs(|| {
            next += 1;
            (0..10)
                .map(|item| (header(format!("batch/{next}/{item}")), BODY))
                .collect::<Vec<_>>()
        })
        .bench_local_values(|requests| {
            for result in runtime.block_on(objects.put_batch(requests)) {
                result.unwrap();
            }
        });
}

/// Reads a key immediately after it was appended to the journal.
#[divan::bench]
fn get_after_put(bencher: Bencher) {
    let runtime = Runtime::new().unwrap();
    let (_root, objects) = store(&runtime);
    let mut next = 0_u64;
    bencher
        .with_inputs(|| {
            next += 1;
            let key = format!("mixed/{next}");
            runtime
                .block_on(objects.put(header(key.clone()), BODY))
                .unwrap();
            key
        })
        .bench_local_values(|key| runtime.block_on(objects.get(get(key), 1024)).unwrap());
}

/// Reads one small object from a freshly reopened store.
#[divan::bench(sample_size = 1)]
fn get_cold(bencher: Bencher) {
    let runtime = Runtime::new().unwrap();
    let (root, objects) = store(&runtime);
    for key in 0..100 {
        runtime
            .block_on(objects.put(header(format!("cold/{key}")), BODY))
            .unwrap();
    }
    drop(objects);
    let mut next = 0_u64;
    bencher
        .with_inputs(|| open(&runtime, root.path()))
        .bench_local_values(|objects| {
            next = (next + 7) % 100;
            runtime
                .block_on(objects.get(get(format!("cold/{next}")), 1024))
                .unwrap();
            // Returned so the store closes outside the timed region.
            objects
        });
}

/// Lists 2000 current keys page by page.
#[divan::bench(args = [10, 100, 1000])]
fn list_pages(bencher: Bencher, page_size: u32) {
    let runtime = Runtime::new().unwrap();
    let (objects, bucket) = MemoryObjects::with_default_bucket();
    for key in 0..2000 {
        let header = wire::PutObjectHeader {
            bucket: Some(bucket.clone()),
            object_key: format!("group/{key:08}"),
            ..Default::default()
        };
        runtime
            .block_on(objects.put(header, Bytes::from_static(b"x")))
            .unwrap();
    }
    let query = wire::ListObjectsRequest {
        bucket: Some(bucket),
        prefix: "group/".into(),
        page_size,
        ..Default::default()
    };
    bencher
        .counter(ItemsCount::new(2000_usize))
        .bench_local(|| {
            let mut request = query.clone();
            loop {
                let page = runtime.block_on(objects.list(request.clone())).unwrap();
                if page.continuation_token.is_empty() {
                    break;
                }
                request.continuation_token = page.continuation_token;
            }
        });
}
