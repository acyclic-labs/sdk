use acyclic_fs::kernel::{
    CanonicalDecodeError, DecodeLimits, FileKind, FilePayload, FileRecord, FileTablePage,
    decode_file_table_page, encode_file_table_page, file_table_page_id, lookup_file_record,
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

fn measured(bencher: Bencher, name: &str, count: u8, operation: impl FnOnce(Bencher)) {
    let markers = std::env::var_os("ACYCLIC_BENCH_MARKERS").is_some();
    if markers {
        eprintln!("ACYCLIC_DECODED_PAGE_BOUNDARY name={name} count={count} begin=true");
    }
    operation(bencher);
    if markers {
        eprintln!("ACYCLIC_DECODED_PAGE_BOUNDARY name={name} count={count} begin=false");
    }
}

#[divan::bench(args = [1, 8, 64], threads = [1, 4], sample_count = 32, sample_size = 16)]
fn lookup(bencher: Bencher, count: u8) {
    lookup_store(bencher, count, false);
}

#[divan::bench(args = [1, 8, 64], threads = [1, 4], sample_count = 32, sample_size = 16)]
fn lookup_cold(bencher: Bencher, count: u8) {
    lookup_store(bencher, count, true);
}

fn lookup_store(bencher: Bencher, count: u8, cold: bool) {
    let (objects, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
    let store = LogicalObjectStore::new(Arc::new(objects), bucket);
    let name = if cold { "lookup_cold" } else { "lookup" };
    let page = page(count);
    let id = file_table_page_id(&page, 64).unwrap_or_else(fail);
    let encoded = encoded(&page);
    let maximum = encoded.len() as u64;
    let cancellation = CancellationToken::new();
    let budget = acyclic_fs::performance::BOUNDARY_BUDGET;
    complete(store.put(
        id,
        OwnedBytes::from(encoded),
        &mut AuthorityWork::new(budget, &cancellation),
    ))
    .unwrap_or_else(fail);
    let operation = |store: &LogicalObjectStore<_>| {
        complete(lookup_file_record(
            store,
            id,
            FileId::from_bytes([count; 16]),
            DecodeLimits::default(),
            budget,
            &cancellation,
        ))
        .unwrap_or_else(fail)
    };
    let receipt_store = store
        .clone()
        .with_object_limit(maximum)
        .unwrap_or_else(fail);
    let receipt = operation(if cold { &receipt_store } else { &store });
    assert!(receipt.value.is_some());
    let expected_items = match count {
        1 => 3,
        8 => 12,
        64 => 71,
        _ => fail("unqualified file-table fixture"),
    };
    assert_eq!(receipt.work.items_examined, expected_items);
    assert_eq!(receipt.work.allocation_operations, 7);
    assert_eq!(receipt.work.bytes_copied, 79);
    assert_eq!(receipt.work.object_probes, 1);
    assert_eq!(receipt.work.backend_read_operations, 1);
    assert_eq!(receipt.work.page_reads, 1);
    assert_eq!(receipt.work.object_bytes_read, maximum);
    assert_eq!(receipt.work.bytes_hashed, maximum);
    if std::env::var_os("ACYCLIC_BENCH_WORK").is_some() {
        eprintln!("decoded_page_{name} count={count} work={:?}", receipt.work);
    }
    measured(bencher, name, count, |bencher| {
        if cold {
            bencher
                .with_inputs(|| {
                    store
                        .clone()
                        .with_object_limit(maximum)
                        .unwrap_or_else(fail)
                })
                .bench_refs(|store| std::hint::black_box(operation(store)));
        } else {
            bencher.bench(|| std::hint::black_box(operation(&store)));
        }
    });
}

#[divan::bench(args = [1, 8, 64], threads = [1, 4], sample_count = 32, sample_size = 16)]
fn decode(bencher: Bencher, count: u8) {
    let bytes = encoded(&page(count));
    measured(bencher, "decode", count, |bencher| {
        bencher.bench(|| {
            std::hint::black_box(
                decode_file_table_page(&bytes, DecodeLimits::default(), &mut |_| {
                    Ok::<_, CanonicalDecodeError>(())
                })
                .unwrap_or_else(fail),
            );
        });
    });
}
