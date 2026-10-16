use acyclic_fs::kernel::{
    DecodeLimits, Extent, ExtentKind, ExtentMutation, ExtentPage, apply_extent_mutations,
    encode_extent_page,
};
use acyclic_fs::storage::{HashedObject, ObjectKind};
use acyclic_fs::{AsyncObjectStore, AuthorityWork, CancellationToken, LogicalObjectStore};
use acyclic_objects::v1::MemoryObjects;
use acyclic_stream::OwnedBytes;
use divan::Bencher;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

fn fail<T>(error: impl std::fmt::Debug) -> T {
    eprintln!("extent benchmark failed: {error:?}");
    std::process::exit(1);
}

fn complete<T>(future: impl Future<Output = T>) -> T {
    let mut future = std::pin::pin!(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(result) => result,
        Poll::Pending => fail("memory extent operation unexpectedly suspended"),
    }
}

#[divan::bench(args = [1, 2], threads = [1, 4])]
fn extent_replace(bencher: Bencher, patches: usize) {
    let (provider, bucket) = MemoryObjects::with_default_bucket();
    let store = LogicalObjectStore::new(Arc::new(provider), bucket);
    let cancellation = CancellationToken::new();
    let budget = acyclic_fs::performance::BOUNDARY_BUDGET;
    let page = ExtentPage::Leaf(vec![Extent {
        offset: 0,
        length: 64,
        kind: ExtentKind::Hole,
    }]);
    let encoded = encode_extent_page(
        &page,
        8,
        |_| Ok::<_, acyclic_fs::kernel::CanonicalDecodeError>(()),
        |_| acyclic_fs::kernel::CanonicalDecodeError::AllocationFailed,
    )
    .unwrap_or_else(fail);
    let object = HashedObject::new(ObjectKind::ExtentPage, OwnedBytes::from(encoded));
    let root = object.object_id();
    complete(store.put_hashed(object, &mut AuthorityWork::new(budget, &cancellation)))
        .unwrap_or_else(fail);
    let changes: Vec<_> = (0..patches)
        .map(|i| ExtentMutation::Replace {
            offset: 8 + i as u64 * 16,
            length: 8,
            kind: ExtentKind::AllocatedZero,
            extend: false,
        })
        .collect();
    let apply = || {
        complete(apply_extent_mutations(
            &store,
            (root, 64),
            &changes,
            8,
            DecodeLimits::default(),
            budget,
            &cancellation,
        ))
        .unwrap_or_else(fail)
    };
    // Identical mutations address the same immutable pages on every iteration;
    // retained store size stays bounded even with long sample runs.
    if std::env::var_os("ACYCLIC_BENCH_WORK").is_some() {
        eprintln!("extent patches={patches} work={:?}", apply().work);
    }
    let markers = std::env::var_os("ACYCLIC_BENCH_MARKERS").is_some();
    if markers {
        eprintln!("ACYCLIC_EXTENT_BOUNDARY patches={patches} begin=true");
    }
    bencher.bench(apply);
    if markers {
        eprintln!("ACYCLIC_EXTENT_BOUNDARY patches={patches} begin=false");
    }
}
