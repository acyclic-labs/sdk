use super::*;
use crate::async_storage::AsyncObjectStore;
use crate::foundation::{Digest, FileId};
use crate::kernel::{
    BlobBuildOptions, Extent, ExtentPage, InlineFileData, build_blob, encode_extent_page,
    extent_page_id,
};
use crate::memory::MemoryObjectStore;
use crate::storage::{ObjectId, ObjectKind};
use acyclic_stream::OwnedBytes;
use std::io::Cursor;

#[path = "file_read/accounting.rs"]
mod accounting;

fn inline_record(bytes: &[u8]) -> Result<FileRecord, Box<dyn std::error::Error>> {
    Ok(FileRecord {
        file_id: FileId::from_bytes([7; 16]),
        kind: FileKind::Regular,
        link_count: 1,
        metadata: ObjectId {
            kind: ObjectKind::Metadata,
            digest: Digest::from_bytes([9; 32]),
        },
        payload: FilePayload::InlineRegular(InlineFileData::new(bytes)?),
    })
}

fn limits() -> DecodeLimits {
    DecodeLimits {
        maximum_object_bytes: 1024,
        maximum_name_bytes: 255,
        maximum_page_items: 16,
        maximum_page_bytes: 1024,
        maximum_page_height: 8,
        maximum_visited_pages: 64,
    }
}

#[tokio::test]
async fn inline_ranges_are_exact_and_need_no_backend_work() -> Result<(), Box<dyn std::error::Error>>
{
    let request = FileRangeRequest {
        record: inline_record(b"abcdef")?,
        range: ByteRange {
            offset: 2,
            length: 3,
        },
        maximum_spans: 1,
        limits: limits(),
        budget: WorkBudget::UNBOUNDED,
    };
    let read = (read_file_range_async(
        &MemoryObjectStore::default(),
        request,
        &CancellationToken::new(),
    ))
    .await?;
    assert_eq!(&read.bytes[..], b"cde");
    assert_eq!(read.logical_owned_bytes(), 3);
    let shared = read.bytes.slice(1..2);
    assert_eq!(shared.as_ref(), b"d");
    assert_eq!(
        read.logical_owned_bytes(),
        3,
        "a slice does not shrink producer custody"
    );
    assert_eq!(read.work.backend_read_operations, 0);
    assert_eq!(read.work.output_bytes, 3);
    assert_eq!(read.work.peak_allocation_bytes, 3);
    Ok(())
}

#[tokio::test]
async fn invalid_inline_range_fails_before_work() -> Result<(), Box<dyn std::error::Error>> {
    let request = FileRangeRequest {
        record: inline_record(b"abc")?,
        range: ByteRange {
            offset: 2,
            length: 2,
        },
        maximum_spans: 1,
        limits: limits(),
        budget: WorkBudget::UNBOUNDED,
    };
    let result = (read_file_range_async(
        &MemoryObjectStore::default(),
        request,
        &CancellationToken::new(),
    ))
    .await;
    let Err(failure) = result else {
        return Err("invalid range succeeded".into());
    };
    assert!(matches!(failure.error, FileRangeReadError::InvalidRange));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn cancellation_kind_and_payload_fail_at_the_first_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let cancelled = (read_file_range_async(
        &store,
        FileRangeRequest {
            record: inline_record(b"abc")?,
            range: ByteRange {
                offset: 0,
                length: 1,
            },
            maximum_spans: 1,
            limits: limits(),
            budget: WorkBudget::UNBOUNDED,
        },
        &cancellation,
    ))
    .await
    .err()
    .ok_or("cancelled read succeeded")?;
    assert!(matches!(cancelled.error, FileRangeReadError::Cancelled));
    assert_eq!(*cancelled.work, WorkCounters::default());

    let metadata = inline_record(b"")?.metadata;
    let not_regular = FileRecord {
        file_id: FileId::from_bytes([8; 16]),
        kind: FileKind::Directory,
        link_count: 1,
        metadata,
        payload: FilePayload::Directory {
            entries: ObjectId {
                kind: ObjectKind::TreePage,
                digest: Digest::from_bytes([10; 32]),
            },
        },
    };
    let rejected = (read_file_range_async(
        &store,
        FileRangeRequest {
            record: not_regular,
            range: ByteRange {
                offset: 0,
                length: 0,
            },
            maximum_spans: 1,
            limits: limits(),
            budget: WorkBudget::UNBOUNDED,
        },
        &CancellationToken::new(),
    ))
    .await
    .err()
    .ok_or("non-regular read succeeded")?;
    assert!(matches!(rejected.error, FileRangeReadError::NotRegular));
    assert_eq!(*rejected.work, WorkCounters::default());

    let mut mismatched = inline_record(b"abc")?;
    mismatched.payload = FilePayload::SymbolicLink {
        target_bytes: 0,
        target: ObjectId {
            kind: ObjectKind::Blob,
            digest: Digest::from_bytes([11; 32]),
        },
    };
    let rejected = (read_file_range_async(
        &store,
        FileRangeRequest {
            record: mismatched,
            range: ByteRange {
                offset: 0,
                length: 0,
            },
            maximum_spans: 1,
            limits: limits(),
            budget: WorkBudget::UNBOUNDED,
        },
        &CancellationToken::new(),
    ))
    .await
    .err()
    .ok_or("payload-mismatched read succeeded")?;
    assert!(matches!(rejected.error, FileRangeReadError::NotRegular));
    assert_eq!(*rejected.work, WorkCounters::default());

    Ok(())
}

#[tokio::test]
async fn inline_budget_rejects_before_copy_or_allocation() -> Result<(), Box<dyn std::error::Error>>
{
    let store = MemoryObjectStore::default();
    let mut budget = WorkBudget::UNBOUNDED;
    budget.output_bytes = 2;
    let rejected = (read_file_range_async(
        &store,
        FileRangeRequest {
            record: inline_record(b"abc")?,
            range: ByteRange {
                offset: 0,
                length: 3,
            },
            maximum_spans: 1,
            limits: limits(),
            budget,
        },
        &CancellationToken::new(),
    ))
    .await
    .err()
    .ok_or("inline read exceeded output budget")?;
    assert!(matches!(
        rejected.error,
        FileRangeReadError::Work(WorkError::BudgetExceeded {
            counter: "output_bytes",
            ..
        })
    ));
    assert_eq!(*rejected.work, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn zero_length_inline_read_at_eof_is_allocation_free()
-> Result<(), Box<dyn std::error::Error>> {
    let read = (read_file_range_async(
        &MemoryObjectStore::default(),
        FileRangeRequest {
            record: inline_record(b"abc")?,
            range: ByteRange {
                offset: 3,
                length: 0,
            },
            maximum_spans: 1,
            limits: limits(),
            budget: WorkBudget::UNBOUNDED,
        },
        &CancellationToken::new(),
    ))
    .await?;
    assert!(read.bytes.is_empty());
    assert_eq!(read.logical_owned_bytes(), 0);
    assert_eq!(read.work.output_bytes, 0);
    assert_eq!(read.work.bytes_copied, 0);
    assert_eq!(read.work.allocation_operations, 0);
    assert_eq!(read.work.items_returned, 1);
    Ok(())
}

#[tokio::test]
async fn sparse_file_read_composes_multichunk_content_holes_and_allocated_zero()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let blob = build_blob(
        &store,
        &mut Cursor::new(b"ABCDEFGHIJ"),
        BlobBuildOptions {
            chunk_bytes: 2,
            page_items: 4,
            page_bytes: 1024,
            maximum_blob_bytes: 10,
        },
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    let extents = ExtentPage::Leaf(vec![
        Extent {
            offset: 0,
            length: 2,
            kind: ExtentKind::Content {
                object: blob.root,
                object_offset: 0,
            },
        },
        Extent {
            offset: 2,
            length: 2,
            kind: ExtentKind::Hole,
        },
        Extent {
            offset: 4,
            length: 2,
            kind: ExtentKind::AllocatedZero,
        },
        Extent {
            offset: 6,
            length: 4,
            kind: ExtentKind::Content {
                object: blob.root,
                object_offset: 4,
            },
        },
        Extent {
            offset: 10,
            length: 2,
            kind: ExtentKind::Hole,
        },
    ]);
    let extent_root = extent_page_id(&extents, 16)?;
    {
        let __object_args = (
            extent_root,
            OwnedBytes::from(encode_extent_page(
                &extents,
                16,
                |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
                |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
            )?),
            WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        );
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result =
            AsyncObjectStore::put(&store, __object_args.0, __object_args.1, &mut __object_work)
                .await;
        __object_work.receipt(__object_result)
    }?;
    let record = FileRecord {
        file_id: FileId::from_bytes([12; 16]),
        kind: FileKind::Regular,
        link_count: 1,
        metadata: ObjectId {
            kind: ObjectKind::Metadata,
            digest: Digest::from_bytes([13; 32]),
        },
        payload: FilePayload::Regular {
            logical_bytes: 12,
            extents: extent_root,
        },
    };
    let denied = (read_file_range_async(
        &store,
        FileRangeRequest {
            record,
            range: ByteRange {
                offset: 1,
                length: 8,
            },
            maximum_spans: 5,
            limits: DecodeLimits::default(),
            budget: WorkBudget {
                allocation_operations: 0,
                ..WorkBudget::UNBOUNDED
            },
        },
        &CancellationToken::new(),
    ))
    .await
    .expect_err("the canonical sparse future is admitted before immutable I/O");
    assert_eq!(denied.work.allocation_operations, 0);
    assert_eq!(denied.work.backend_read_operations, 0);
    assert_eq!(denied.work.bytes_copied, 0);
    let read = (read_file_range_async(
        &store,
        FileRangeRequest {
            record,
            range: ByteRange {
                offset: 1,
                length: 8,
            },
            maximum_spans: 5,
            limits: DecodeLimits::default(),
            budget: WorkBudget::UNBOUNDED,
        },
        &CancellationToken::new(),
    ))
    .await?;
    assert_eq!(&read.bytes[..], &[b'B', 0, 0, 0, 0, b'E', b'F', b'G']);
    assert_eq!(read.logical_owned_bytes(), 8);
    assert_eq!(read.work.output_bytes, 8);
    assert!(read.work.bytes_copied >= 6);
    assert!(read.work.page_reads >= 3);
    assert!(read.work.backend_read_operations >= 3);
    assert!(read.work.peak_allocation_bytes >= 8);
    #[cfg(not(target_arch = "wasm32"))]
    {
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let mut accounting = KernelAccounting::native(&original, &cancellation);
        let native = (read_file_range_with_accounting(
            &store,
            FileRangeRequest {
                record,
                range: ByteRange {
                    offset: 1,
                    length: 8,
                },
                maximum_spans: 5,
                limits: DecodeLimits::default(),
                budget: WorkBudget::UNBOUNDED,
            },
            &mut accounting,
            &cancellation,
        ))
        .await?;
        assert_eq!(native.bytes.as_ref(), read.bytes.as_ref());
        assert_eq!(original.receipt().output_bytes, 8);
        assert!(original.receipt().backend_read_operations >= 3);
        let alias = native.bytes.slice(1..4);
        drop(native);
        assert!(
            original.live_allocation_bytes() > baseline,
            "a physical OwnedBytes alias retains original custody"
        );
        drop(alias);
        assert_eq!(original.live_allocation_bytes(), baseline);
    }
    assert_sparse_initialized_once(&store, record, extent_root).await?;
    Ok(())
}

async fn assert_sparse_initialized_once(
    store: &MemoryObjectStore,
    record: FileRecord,
    root: ObjectId,
) -> Result<(), Box<dyn std::error::Error>> {
    let model = [b'A', b'B', 0, 0, 0, 0, b'E', b'F', b'G', b'H', 0, 0];
    for (offset, length) in [(0, 2), (1, 8), (2, 4), (10, 2), (12, 0), (5, 6)] {
        let cancellation = CancellationToken::new();
        let range = ByteRange { offset, length };
        // Independent composition oracle: extent/blob reads plus exactly one
        // initialization of each returned byte, whether content or zero.
        let plan = crate::kernel::plan_extent_range(
            store,
            ExtentRangeRequest {
                root,
                file_size: 12,
                range,
                maximum_spans: 5,
                limits: DecodeLimits::default(),
                budget: WorkBudget::UNBOUNDED,
            },
            &cancellation,
        )
        .await?;
        let mut copies = plan.work.bytes_copied + length;
        for span in plan.spans {
            if let ExtentKind::Content {
                object,
                object_offset,
            } = span.kind
            {
                let blob = crate::kernel::read_blob_range(
                    store,
                    object,
                    ByteRange {
                        offset: object_offset,
                        length: span.length,
                    },
                    DecodeLimits::default(),
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await?;
                copies += blob.work.bytes_copied;
            }
        }
        let request = FileRangeRequest {
            record,
            range,
            maximum_spans: 5,
            limits: DecodeLimits::default(),
            budget: WorkBudget {
                bytes_copied: copies,
                ..WorkBudget::UNBOUNDED
            },
        };
        let expected = &model[offset as usize..(offset + length) as usize];
        let read = read_file_range_async(store, request, &cancellation).await?;
        assert_eq!(read.bytes.as_ref(), expected);
        assert_eq!(read.work.bytes_copied, copies);
        if let Some(allowance) = copies.checked_sub(1) {
            let denied = read_file_range_async(
                store,
                FileRangeRequest {
                    budget: WorkBudget {
                        bytes_copied: allowance,
                        ..request.budget
                    },
                    ..request
                },
                &cancellation,
            )
            .await
            .expect_err("one fewer copy byte cannot initialize this range");
            assert!(denied.error.to_string().contains("bytes_copied"));
            assert!(denied.work.bytes_copied < copies);
        }
        #[cfg(not(target_arch = "wasm32"))]
        assert_native_sparse_copy_budget(store, request, expected).await?;
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
async fn assert_native_sparse_copy_budget(
    store: &MemoryObjectStore,
    request: FileRangeRequest,
    expected: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(request.budget, &cancellation)
        .map_err(|(error, _)| error)?;
    let baseline = original.live_allocation_bytes();
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let read =
        read_file_range_with_accounting(store, request, &mut accounting, &cancellation).await?;
    assert_eq!(read.bytes.as_ref(), expected);
    assert_eq!(original.receipt().bytes_copied, request.budget.bytes_copied);
    drop(read);
    assert_eq!(original.live_allocation_bytes(), baseline);
    cancellation.cancel();
    assert!(
        read_file_range_with_accounting(store, request, &mut accounting, &cancellation)
            .await
            .is_err()
    );
    assert_eq!(original.live_allocation_bytes(), baseline);
    Ok(())
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn original_decode_completion_survives_cancellation_after_the_copy()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::kernel::codec::ReadAllocationStep;
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let baseline = original.live_allocation_bytes();
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let mut observed = OriginalReadAccounting::new(&mut accounting)?;
    let mut bytes = Vec::<u8>::new();
    let result = (|| -> Result<(), FsError> {
        reserve_decoded_items(&mut bytes, 4, &mut |step| observed.observe(step))?;
        observed.observe(ReadAllocationStep::BeforeCopy(4))?;
        bytes.extend_from_slice(b"data");
        cancellation.cancel();
        observed.observe(ReadAllocationStep::Copied(4))?;
        observed.observe(ReadAllocationStep::BeforeItem)
    })();
    drop(bytes);
    observed.release_failed()?;
    assert!(matches!(
        result,
        Err(FsError::NativeKernel(
            crate::NativeTreeMaterializationError::Cancelled
        ))
    ));
    assert_eq!(original.receipt().bytes_copied, 4);
    assert_eq!(original.receipt().items_examined, 0);
    assert_eq!(original.live_allocation_bytes(), baseline);
    Ok(())
}

#[test]
fn late_backend_retention_refusal_releases_only_returned_backing()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let budget = WorkBudget {
        peak_allocation_bytes: 4,
        ..WorkBudget::UNBOUNDED
    };
    let mut accounting = KernelAccounting::standalone(budget, &cancellation);
    let existing = allocate_read_buffer(4, &mut accounting)?;
    let receipt = crate::OperationReceipt {
        value: crate::storage::ObjectRead {
            bytes: OwnedBytes::from(vec![1, 2, 3]),
            retention: crate::storage::ObjectReadRetention::Owned { logical_bytes: 3 },
        },
        work: WorkCounters {
            allocation_operations: 1,
            peak_allocation_bytes: 3,
            ..WorkCounters::UNCHARGED
        },
    };
    let error = accept_standalone_object_read(Ok(receipt), &mut accounting)
        .err()
        .ok_or("over-budget returned backing was accepted")?;
    assert!(matches!(
        error,
        FsError::Work(WorkError::BudgetExceeded { .. })
    ));
    assert_eq!(
        accounting
            .standalone_parts()
            .ok_or("standalone parts unavailable")?
            .0
            .live_bytes(),
        4
    );
    assert_eq!(accounting.spent().allocation_operations, 2);
    assert_eq!(accounting.spent().peak_allocation_bytes, 7);
    drop(existing.value);
    release_read_allocation(4, existing.custody, &mut accounting)?;
    assert_eq!(
        accounting
            .standalone_parts()
            .ok_or("standalone parts unavailable")?
            .0
            .live_bytes(),
        0
    );
    Ok(())
}

#[test]
fn backend_merge_failure_does_not_release_unregistered_returned_backing()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let existing = allocate_read_buffer(4, &mut accounting)?;
    accounting
        .standalone_parts()
        .ok_or("standalone parts unavailable")?
        .1
        .object_probes = u64::MAX;
    let receipt = crate::OperationReceipt {
        value: crate::storage::ObjectRead {
            bytes: OwnedBytes::from(vec![1, 2, 3]),
            retention: crate::storage::ObjectReadRetention::Owned { logical_bytes: 3 },
        },
        work: WorkCounters {
            object_probes: 1,
            ..WorkCounters::UNCHARGED
        },
    };
    let error = accept_standalone_object_read(Ok(receipt), &mut accounting)
        .err()
        .ok_or("overflowing backend work was accepted")?;
    assert!(matches!(error, FsError::Work(WorkError::Overflow)));
    assert_eq!(
        accounting
            .standalone_parts()
            .ok_or("standalone parts unavailable")?
            .0
            .live_bytes(),
        4
    );
    drop(existing.value);
    release_read_allocation(4, existing.custody, &mut accounting)?;
    Ok(())
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn owned_read_error_scope_drops_payload_before_original_guard()
-> Result<(), Box<dyn std::error::Error>> {
    struct Payload<'a> {
        bytes: Vec<u8>,
        original: &'a crate::NativeMaterializationWork,
        baseline: u64,
        dropped: &'a std::cell::Cell<bool>,
    }
    impl Drop for Payload<'_> {
        fn drop(&mut self) {
            assert_eq!(
                self.original.live_allocation_bytes(),
                self.baseline + usize_to_u64(self.bytes.capacity())
            );
            self.dropped.set(true);
        }
    }
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let baseline = original.live_allocation_bytes();
    let dropped = std::cell::Cell::new(false);
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let buffer = allocate_read_buffer(4, &mut accounting)?;
    let owner = ReadOwned {
        value: Payload {
            bytes: buffer.value,
            original: &original,
            baseline,
            dropped: &dropped,
        },
        custody: buffer.custody,
    };
    let fail = |owner: ReadOwned<Payload<'_>>| -> Result<(), FsError> {
        let _owner = owner;
        Err(FileRangeReadError::InvalidRange.into())
    };
    assert!(fail(owner).is_err());
    assert!(dropped.get());
    assert_eq!(original.live_allocation_bytes(), baseline);
    Ok(())
}
