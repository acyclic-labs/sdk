use super::*;
use crate::async_storage::poll_ready;
use crate::foundation::{Digest, FileId};
use crate::kernel::{
    BlobBuildOptions, Extent, ExtentPage, InlineFileData, build_blob, encode_extent_page,
    extent_page_id,
};
use crate::memory::MemoryObjectStore;
use crate::storage::{ObjectId, ObjectKind, ObjectStore};
use std::io::Cursor;

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

#[test]
fn inline_ranges_are_exact_and_need_no_backend_work() -> Result<(), Box<dyn std::error::Error>> {
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
    let read = poll_ready(read_file_range_async(
        &MemoryObjectStore::default(),
        request,
        &CancellationToken::new(),
    ))
    .ok_or("inline read blocked")??;
    assert_eq!(&read.bytes[..], b"cde");
    assert_eq!(read.work.backend_read_operations, 0);
    assert_eq!(read.work.output_bytes, 3);
    assert_eq!(read.work.peak_allocation_bytes, 3);
    Ok(())
}

#[test]
fn invalid_inline_range_fails_before_work() -> Result<(), Box<dyn std::error::Error>> {
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
    let result = poll_ready(read_file_range_async(
        &MemoryObjectStore::default(),
        request,
        &CancellationToken::new(),
    ))
    .ok_or("inline read blocked")?;
    let Err(failure) = result else {
        return Err("invalid range succeeded".into());
    };
    assert!(matches!(failure.error, FileRangeReadError::InvalidRange));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[test]
fn cancellation_kind_and_payload_fail_at_the_first_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let cancelled = poll_ready(read_file_range_async(
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
    .ok_or("cancelled read blocked")?
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
    let rejected = poll_ready(read_file_range_async(
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
    .ok_or("non-regular read blocked")?
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
    let rejected = poll_ready(read_file_range_async(
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
    .ok_or("payload-mismatched read blocked")?
    .err()
    .ok_or("payload-mismatched read succeeded")?;
    assert!(matches!(rejected.error, FileRangeReadError::NotRegular));
    assert_eq!(*rejected.work, WorkCounters::default());

    Ok(())
}

#[test]
fn inline_budget_rejects_before_copy_or_allocation() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let mut budget = WorkBudget::UNBOUNDED;
    budget.output_bytes = 2;
    let rejected = poll_ready(read_file_range_async(
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
    .ok_or("budgeted inline read blocked")?
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

#[test]
fn zero_length_inline_read_at_eof_is_allocation_free() -> Result<(), Box<dyn std::error::Error>> {
    let read = poll_ready(read_file_range_async(
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
    .ok_or("zero-length inline read blocked")??;
    assert!(read.bytes.is_empty());
    assert_eq!(read.work.output_bytes, 0);
    assert_eq!(read.work.bytes_copied, 0);
    assert_eq!(read.work.allocation_operations, 0);
    assert_eq!(read.work.items_returned, 1);
    Ok(())
}

#[test]
fn sparse_file_read_composes_clipped_content_holes_and_allocated_zero()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let blob = build_blob(
        &store,
        &mut Cursor::new(b"ABCDEFGHIJ"),
        BlobBuildOptions {
            chunk_bytes: 4,
            page_items: 4,
            page_bytes: 1024,
            maximum_blob_bytes: 10,
        },
        WorkBudget::UNBOUNDED,
    )?;
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
    ObjectStore::put(
        &store,
        extent_root,
        Bytes::from(encode_extent_page(&extents, 16)?),
        WorkBudget::UNBOUNDED,
    )?;
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
    let read = poll_ready(read_file_range_async(
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
    .ok_or("sparse file read blocked")??;
    assert_eq!(&read.bytes[..], &[b'B', 0, 0, 0, 0, b'E', b'F', b'G']);
    assert_eq!(read.work.output_bytes, 8);
    assert_eq!(read.work.items_returned, 7);
    assert!(read.work.bytes_copied >= 6);
    assert!(read.work.page_reads >= 3);
    assert!(read.work.backend_read_operations >= 3);
    assert!(read.work.peak_allocation_bytes >= 8);
    Ok(())
}
