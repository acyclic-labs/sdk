use acyclic_fs::MemoryObjectStore;
use acyclic_fs::kernel::{
    DecodeLimits, FileKind, FilePayload, FileRangeReadError, FileRangeRequest, FileRecord,
    InlineFileData, read_file_range_async,
};
use acyclic_fs::{ByteRange, CancellationToken};
use acyclic_fs_format::{
    identity::{Digest, FileId},
    object_reference::{ObjectId, ObjectKind},
};
use acyclic_fs_work::performance::{WorkBudget, WorkCounters, WorkError};

fn inline_record(bytes: &[u8]) -> Result<FileRecord, Box<dyn std::error::Error>> {
    Ok(FileRecord {
        bindings: None,
        file_id: FileId::from_bytes([7; 16]),
        kind: FileKind::Regular,
        link_count: 1,
        metadata: ObjectId {
            storage: None,
            kind: ObjectKind::Metadata,
            digest: Digest::from_bytes([9; 32]),
        },
        payload: FilePayload::InlineRegular(InlineFileData::new(bytes)?),
    })
}

fn limits() -> DecodeLimits {
    DecodeLimits {
        maximum_object_bytes: 1024,
        maximum_page_items: 16,
        maximum_page_bytes: 1024,
        maximum_page_height: 8,
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
        maximum_spans: Some(1),
        limits: limits(),
        budget: WorkBudget::UNBOUNDED,
    };
    let read = (read_file_range_async(
        &MemoryObjectStore::default(),
        request,
        &CancellationToken::new(),
    ))
    .await?;
    assert_eq!(read.bytes.as_ref(), b"cde");
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
        maximum_spans: Some(1),
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
    assert_eq!(*failure.work()?, WorkCounters::default());
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
            maximum_spans: Some(1),
            limits: limits(),
            budget: WorkBudget::UNBOUNDED,
        },
        &cancellation,
    ))
    .await
    .err()
    .ok_or("cancelled read succeeded")?;
    assert!(matches!(cancelled.error, FileRangeReadError::Cancelled));
    assert_eq!(*cancelled.work()?, WorkCounters::default());

    let metadata = inline_record(b"")?.metadata;
    let not_regular = FileRecord {
        bindings: None,
        file_id: FileId::from_bytes([8; 16]),
        kind: FileKind::Directory,
        link_count: 1,
        metadata,
        payload: FilePayload::Directory {
            entries: ObjectId {
                storage: None,
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
            maximum_spans: Some(1),
            limits: limits(),
            budget: WorkBudget::UNBOUNDED,
        },
        &CancellationToken::new(),
    ))
    .await
    .err()
    .ok_or("non-regular read succeeded")?;
    assert!(matches!(rejected.error, FileRangeReadError::NotRegular));
    assert_eq!(*rejected.work()?, WorkCounters::default());

    let mut mismatched = inline_record(b"abc")?;
    mismatched.payload = FilePayload::SymbolicLink {
        target_bytes: 0,
        target: ObjectId {
            storage: None,
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
            maximum_spans: Some(1),
            limits: limits(),
            budget: WorkBudget::UNBOUNDED,
        },
        &CancellationToken::new(),
    ))
    .await
    .err()
    .ok_or("payload-mismatched read succeeded")?;
    assert!(matches!(rejected.error, FileRangeReadError::NotRegular));
    assert_eq!(*rejected.work()?, WorkCounters::default());

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
            maximum_spans: Some(1),
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
    assert_eq!(*rejected.work()?, WorkCounters::default());
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
            maximum_spans: Some(1),
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
