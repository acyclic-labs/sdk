use acyclic_fs::{
    AsyncObjectStore, AuthorityWork, CancellationToken, HashedObject, LogicalObjectStore,
    NativeMaterializationWork, ObjectKind, WorkCounters, storage::ObjectWrite,
};
use acyclic_objects::v1::MemoryObjects;
use acyclic_stream::OwnedBytes;
use divan::Bencher;
use std::sync::Arc;
use tokio::runtime::Runtime;

pub fn main() {
    if std::env::var_os("ACYCLIC_BENCH_TRACE").is_some() {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_writer(std::io::sink)
            .init();
    }
    divan::main();
}

fn publish(
    runtime: &Runtime,
    store: &LogicalObjectStore<MemoryObjects>,
    objects: &[HashedObject],
    writes: &[ObjectWrite],
    cancellation: &CancellationToken,
    original: Option<&NativeMaterializationWork>,
) -> WorkCounters {
    let mut work = match original {
        Some(original) => AuthorityWork::native(original, cancellation),
        None => AuthorityWork::new(acyclic_fs::performance::BOUNDARY_BUDGET, cancellation),
    };
    // These are the two actual BlobBatchStore::flush dispatch paths.
    if original.is_some() {
        runtime.block_on(store.put_many(writes, &mut work)).unwrap();
    } else {
        runtime
            .block_on(store.put_many_hashed(objects, &mut work))
            .unwrap();
    }
    work.spent()
}

#[divan::bench(args = [(false, 1024), (false, 65_536), (true, 65_536)])]
fn authenticated_batch(bencher: Bencher, (native, size): (bool, usize)) {
    acyclic_native_runtime::initialize_native_io_services().unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let objects: Vec<_> = (0..4)
        .map(|byte| HashedObject::new(ObjectKind::BlobChunk, OwnedBytes::from(vec![byte; size])))
        .collect();
    let writes: Vec<_> = objects
        .iter()
        .map(|object| ObjectWrite {
            object_id: object.object_id(),
            bytes: object.bytes().clone(),
        })
        .collect();
    let input = || {
        let cancellation = CancellationToken::new();
        let original = native.then(|| {
            NativeMaterializationWork::new(acyclic_fs::performance::BOUNDARY_BUDGET, &cancellation)
                .unwrap()
        });
        let (provider, bucket) = MemoryObjects::with_default_bucket();
        let store = LogicalObjectStore::new(Arc::new(provider), bucket);
        (store, cancellation, original)
    };
    if std::env::var_os("ACYCLIC_BENCH_WORK").is_some() {
        let (store, cancellation, original) = input();
        let receipt = publish(
            &runtime,
            &store,
            &objects,
            &writes,
            &cancellation,
            original.as_ref(),
        );
        eprintln!("batch native={native} size={size} work={receipt:?}");
    }
    let markers = std::env::var_os("ACYCLIC_BENCH_MARKERS").is_some();
    bencher.with_inputs(input).bench_local_refs(|input| {
        if markers {
            eprintln!("ACYCLIC_BATCH_BOUNDARY native={native} size={size} begin=true");
        }
        let receipt = publish(
            &runtime,
            &input.0,
            &objects,
            &writes,
            &input.1,
            input.2.as_ref(),
        );
        if markers {
            eprintln!("ACYCLIC_BATCH_BOUNDARY native={native} size={size} begin=false");
        }
        receipt
    });
}
