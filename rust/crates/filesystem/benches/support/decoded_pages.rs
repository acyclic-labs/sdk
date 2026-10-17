use acyclic_fs::kernel::{
    CanonicalDecodeError, DecodeLimits, FileKind, FilePayload, FileRecord, FileTablePage,
    encode_file_table_page, file_table_page_id, lookup_file_record,
};
use acyclic_fs::{
    AsyncObjectStore, AuthorityWork, CancellationToken, FileId, LogicalObjectStore, ObjectId,
    ObjectKind,
};
use acyclic_stream::OwnedBytes;
use divan::Bencher;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

fn fail<T>(error: impl std::fmt::Debug) -> T {
    eprintln!("file-table benchmark failed: {error:?}");
    std::process::exit(1);
}

fn complete<T>(future: impl Future<Output = T>) -> T {
    let mut future = std::pin::pin!(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(value) => value,
        Poll::Pending => fail("in-process file-table operation unexpectedly suspended"),
    }
}

fn page(count: u8) -> FileTablePage {
    FileTablePage::Leaf(
        (1..=count)
            .map(|key| FileRecord {
                file_id: FileId::from_bytes([key; 16]),
                kind: FileKind::Fifo,
                link_count: 1,
                metadata: ObjectId {
                    kind: ObjectKind::Metadata,
                    digest: acyclic_fs::Digest::from_bytes([key; 32]),
                },
                payload: FilePayload::Empty,
            })
            .collect(),
    )
}

fn encoded(page: &FileTablePage) -> Vec<u8> {
    encode_file_table_page(
        page,
        64,
        |_| Ok::<_, CanonicalDecodeError>(()),
        |_| CanonicalDecodeError::AllocationFailed,
    )
    .unwrap_or_else(fail)
}

fn measured(bencher: Bencher, name: &str, count: u8, operation: impl Fn() + Sync) {
    let markers = std::env::var_os("ACYCLIC_BENCH_MARKERS").is_some();
    if markers {
        eprintln!("ACYCLIC_DECODED_PAGE_BOUNDARY name={name} count={count} begin=true");
    }
    bencher.bench(operation);
    if markers {
        eprintln!("ACYCLIC_DECODED_PAGE_BOUNDARY name={name} count={count} begin=false");
    }
}

#[divan::bench(args = [1, 8, 64], threads = [1, 4], sample_count = 32, sample_size = 16)]
fn lookup(bencher: Bencher, count: u8) {
    let (objects, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
    let store = LogicalObjectStore::new(Arc::new(objects), bucket);
    let page = page(count);
    let id = file_table_page_id(&page, 64).unwrap_or_else(fail);
    let cancellation = CancellationToken::new();
    let budget = acyclic_fs::performance::BOUNDARY_BUDGET;
    complete(store.put(
        id,
        OwnedBytes::from(encoded(&page)),
        &mut AuthorityWork::new(budget, &cancellation),
    ))
    .unwrap_or_else(fail);
    let operation = || {
        complete(lookup_file_record(
            &store,
            id,
            FileId::from_bytes([count; 16]),
            DecodeLimits::default(),
            budget,
            &cancellation,
        ))
        .unwrap_or_else(fail)
    };
    let receipt = operation();
    assert!(receipt.value.is_some());
    if std::env::var_os("ACYCLIC_BENCH_WORK").is_some() {
        eprintln!("decoded_page_lookup count={count} work={:?}", receipt.work);
    }
    measured(bencher, "lookup", count, || {
        std::hint::black_box(operation());
    });
}
