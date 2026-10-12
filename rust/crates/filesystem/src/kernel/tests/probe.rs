use super::*;
use crate::foundation::VolumeId;
use crate::kernel::{
    BlobChunkRef, BlobNode, BlobPage, CheckoutDependencies, Dependency, Extent, ExtentPage,
    FileKind, FileMetadata, FileTablePage, InlineFileData, LogicalName, MetadataField,
    NameEncoding, RebaseDecision, TreeEntry, TreePage, classify_rebase_async, encode_blob_page,
    encode_extent_page, encode_file_metadata, encode_file_table_page, encode_generation_root, encode_tree_page,
};
use crate::memory::MemoryObjectStore;
use crate::storage::{ObjectStore, object_digest};
use acyclic_stream::OwnedBytes;
use std::future::Future;
use std::task::{Context, Poll, Waker};

fn put(
    store: &MemoryObjectStore,
    kind: ObjectKind,
    bytes: Vec<u8>,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let id = ObjectId {
        kind,
        digest: object_digest(kind, &bytes),
    };
    ObjectStore::put(store, id, OwnedBytes::from(bytes), WorkBudget::UNBOUNDED)?;
    Ok(id)
}

fn fixture(
    store: &MemoryObjectStore,
    inserted_name: Option<&str>,
) -> Result<(GenerationId, FileId, ObjectId), Box<dyn std::error::Error>> {
    let metadata = put(
        store,
        ObjectKind::Metadata,
        encode_file_metadata(FileMetadata {
            posix_mode: MetadataField::Unavailable,
            posix_uid: MetadataField::Unavailable,
            posix_gid: MetadataField::Unavailable,
            posix_flags: MetadataField::Unavailable,
            windows_attributes: MetadataField::Unavailable,
            created_ns: MetadataField::Unavailable,
            modified_ns: MetadataField::Unavailable,
            accessed_ns: MetadataField::Unavailable,
            changed_ns: MetadataField::Unavailable,
            named_attributes: MetadataField::Unavailable,
            acl: MetadataField::Unavailable,
            security_descriptor: MetadataField::Unavailable,
        }, |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()), |_| crate::kernel::CanonicalDecodeError::AllocationFailed)?,
    )?;
    let file_id = FileId::from_bytes([1; 16]);
    let child_id = FileId::from_bytes([3; 16]);
    let tree_entries = inserted_name
        .map(|value| {
            Ok(TreeEntry {
                name: LogicalName::new(NameEncoding::Utf8, value.as_bytes().to_vec(), 255)?,
                file_id: child_id,
                kind: FileKind::Fifo,
            })
        })
        .into_iter()
        .collect::<Result<Vec<_>, super::super::TreePageError>>()?;
    let entries = put(
        store,
        ObjectKind::TreePage,
        encode_tree_page(&TreePage::Leaf(tree_entries), 8, |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()), |_| crate::kernel::CanonicalDecodeError::AllocationFailed)?,
    )?;
    let mut records = vec![FileRecord {
        file_id,
        kind: FileKind::Directory,
        link_count: 1,
        metadata,
        payload: FilePayload::Directory { entries },
    }];
    if inserted_name.is_some() {
        records.push(FileRecord {
            file_id: child_id,
            kind: FileKind::Fifo,
            link_count: 1,
            metadata,
            payload: FilePayload::Empty,
        });
    }
    let table = put(
        store,
        ObjectKind::FileTablePage,
        encode_file_table_page(&FileTablePage::Leaf(records), 8, |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()), |_| crate::kernel::CanonicalDecodeError::AllocationFailed)?,
    )?;
    let root = put(
        store,
        ObjectKind::GenerationRoot,
        encode_generation_root(&GenerationRoot {
            volume_id: VolumeId::from_bytes([2; 16]),
            root_file_id: file_id,
            file_table: table,
            parents: Vec::new(),
            required_features: 0,
        }, |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()), |_| crate::kernel::CanonicalDecodeError::AllocationFailed)?,
    )?;
    Ok((GenerationId::new(root.digest), file_id, metadata))
}

fn non_regular_fixture(
    store: &MemoryObjectStore,
) -> Result<(GenerationId, Vec<FileId>), Box<dyn std::error::Error>> {
    let metadata = put(
        store,
        ObjectKind::Metadata,
        encode_file_metadata(FileMetadata {
            posix_mode: MetadataField::Unavailable,
            posix_uid: MetadataField::Unavailable,
            posix_gid: MetadataField::Unavailable,
            posix_flags: MetadataField::Unavailable,
            windows_attributes: MetadataField::Unavailable,
            created_ns: MetadataField::Unavailable,
            modified_ns: MetadataField::Unavailable,
            accessed_ns: MetadataField::Unavailable,
            changed_ns: MetadataField::Unavailable,
            named_attributes: MetadataField::Unavailable,
            acl: MetadataField::Unavailable,
            security_descriptor: MetadataField::Unavailable,
        }, |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()), |_| crate::kernel::CanonicalDecodeError::AllocationFailed)?,
    )?;
    let root_id = FileId::from_bytes([11; 16]);
    let symbolic_link_id = FileId::from_bytes([12; 16]);
    let empty_id = FileId::from_bytes([13; 16]);
    let device_id = FileId::from_bytes([14; 16]);
    let reparse_id = FileId::from_bytes([15; 16]);
    let empty_tree = put(
        store,
        ObjectKind::TreePage,
        encode_tree_page(&TreePage::Leaf(Vec::new()), 8, |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()), |_| crate::kernel::CanonicalDecodeError::AllocationFailed)?,
    )?;
    let absent_blob = ObjectId {
        kind: ObjectKind::Blob,
        digest: Digest::from_bytes([16; 32]),
    };
    let records = vec![
        FileRecord {
            file_id: root_id,
            kind: FileKind::Directory,
            link_count: 1,
            metadata,
            payload: FilePayload::Directory {
                entries: empty_tree,
            },
        },
        FileRecord {
            file_id: symbolic_link_id,
            kind: FileKind::SymbolicLink,
            link_count: 1,
            metadata,
            payload: FilePayload::SymbolicLink {
                target_bytes: 3,
                target: absent_blob,
            },
        },
        FileRecord {
            file_id: empty_id,
            kind: FileKind::Fifo,
            link_count: 1,
            metadata,
            payload: FilePayload::Empty,
        },
        FileRecord {
            file_id: device_id,
            kind: FileKind::CharacterDevice,
            link_count: 1,
            metadata,
            payload: FilePayload::Device { major: 1, minor: 3 },
        },
        FileRecord {
            file_id: reparse_id,
            kind: FileKind::ReparsePoint,
            link_count: 1,
            metadata,
            payload: FilePayload::ReparsePoint {
                payload_bytes: 3,
                payload: absent_blob,
            },
        },
    ];
    let table = put(
        store,
        ObjectKind::FileTablePage,
        encode_file_table_page(&FileTablePage::Leaf(records), 8, |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()), |_| crate::kernel::CanonicalDecodeError::AllocationFailed)?,
    )?;
    let root = put(
        store,
        ObjectKind::GenerationRoot,
        encode_generation_root(&GenerationRoot {
            volume_id: VolumeId::from_bytes([17; 16]),
            root_file_id: root_id,
            file_table: table,
            parents: Vec::new(),
            required_features: 0,
        }, |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()), |_| crate::kernel::CanonicalDecodeError::AllocationFailed)?,
    )?;
    Ok((
        GenerationId::new(root.digest),
        vec![symbolic_link_id, empty_id, device_id, reparse_id],
    ))
}

struct SparseProbeFixture {
    generation: GenerationId,
    root_id: FileId,
    inline_id: FileId,
    regular_id: FileId,
    regular_name: LogicalName,
}

fn sparse_probe_fixture(
    store: &MemoryObjectStore,
) -> Result<SparseProbeFixture, Box<dyn std::error::Error>> {
    let metadata = put(
        store,
        ObjectKind::Metadata,
        encode_file_metadata(FileMetadata {
            posix_mode: MetadataField::Unavailable,
            posix_uid: MetadataField::Unavailable,
            posix_gid: MetadataField::Unavailable,
            posix_flags: MetadataField::Unavailable,
            windows_attributes: MetadataField::Unavailable,
            created_ns: MetadataField::Unavailable,
            modified_ns: MetadataField::Unavailable,
            accessed_ns: MetadataField::Unavailable,
            changed_ns: MetadataField::Unavailable,
            named_attributes: MetadataField::Unavailable,
            acl: MetadataField::Unavailable,
            security_descriptor: MetadataField::Unavailable,
        }, |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()), |_| crate::kernel::CanonicalDecodeError::AllocationFailed)?,
    )?;
    let chunk = put(store, ObjectKind::BlobChunk, b"DATA".to_vec())?;
    let blob = put(
        store,
        ObjectKind::Blob,
        encode_blob_page(&BlobPage {
                first_offset: 0,
                end_offset: 4,
                node: BlobNode::Leaf(vec![BlobChunkRef {
                    first_offset: 0,
                    end_offset: 4,
                    chunk,
                }]),
            }, 8, |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()), |_| crate::kernel::CanonicalDecodeError::AllocationFailed)?,
    )?;
    let extents = put(
        store,
        ObjectKind::ExtentPage,
        encode_extent_page(&ExtentPage::Leaf(vec![
                Extent {
                    offset: 0,
                    length: 4,
                    kind: ExtentKind::Hole,
                },
                Extent {
                    offset: 4,
                    length: 4,
                    kind: ExtentKind::AllocatedZero,
                },
                Extent {
                    offset: 8,
                    length: 4,
                    kind: ExtentKind::Content {
                        object: blob,
                        object_offset: 0,
                    },
                },
                Extent {
                    offset: 12,
                    length: 4,
                    kind: ExtentKind::Hole,
                },
            ]), 8, |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()), |_| crate::kernel::CanonicalDecodeError::AllocationFailed)?,
    )?;
    let root_id = FileId::from_bytes([21; 16]);
    let inline_id = FileId::from_bytes([22; 16]);
    let regular_id = FileId::from_bytes([23; 16]);
    let inline_name = LogicalName::new(NameEncoding::Utf8, b"inline".to_vec(), 255)?;
    let regular_name = LogicalName::new(NameEncoding::Utf8, b"regular".to_vec(), 255)?;
    let tree = put(
        store,
        ObjectKind::TreePage,
        encode_tree_page(&TreePage::Leaf(vec![
                TreeEntry {
                    name: inline_name,
                    file_id: inline_id,
                    kind: FileKind::Regular,
                },
                TreeEntry {
                    name: regular_name.clone(),
                    file_id: regular_id,
                    kind: FileKind::Regular,
                },
            ]), 8, |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()), |_| crate::kernel::CanonicalDecodeError::AllocationFailed)?,
    )?;
    let table = put(
        store,
        ObjectKind::FileTablePage,
        encode_file_table_page(&FileTablePage::Leaf(vec![
                FileRecord {
                    file_id: root_id,
                    kind: FileKind::Directory,
                    link_count: 1,
                    metadata,
                    payload: FilePayload::Directory { entries: tree },
                },
                FileRecord {
                    file_id: inline_id,
                    kind: FileKind::Regular,
                    link_count: 1,
                    metadata,
                    payload: FilePayload::InlineRegular(InlineFileData::new(b"abcdefgh")?),
                },
                FileRecord {
                    file_id: regular_id,
                    kind: FileKind::Regular,
                    link_count: 1,
                    metadata,
                    payload: FilePayload::Regular {
                        logical_bytes: 16,
                        extents,
                    },
                },
            ]), 8, |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()), |_| crate::kernel::CanonicalDecodeError::AllocationFailed)?,
    )?;
    let root = put(
        store,
        ObjectKind::GenerationRoot,
        encode_generation_root(&GenerationRoot {
            volume_id: VolumeId::from_bytes([24; 16]),
            root_file_id: root_id,
            file_table: table,
            parents: Vec::new(),
            required_features: 0,
        }, |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()), |_| crate::kernel::CanonicalDecodeError::AllocationFailed)?,
    )?;
    Ok(SparseProbeFixture {
        generation: GenerationId::new(root.digest),
        root_id,
        inline_id,
        regular_id,
        regular_name,
    })
}

#[tokio::test]
async fn immutable_summaries_are_cached_without_becoming_authority()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, file_id, metadata) = fixture(&store, None)?;
    let probe = AuthenticatedGenerationProbe::new(&store, ProbeLimits::default())?;
    let first = probe.probe_async(
        generation,
        &DependencyRegion::Metadata(file_id),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ).await?;
    assert_eq!(first.value, DependencyState::Present(metadata.digest));
    assert_eq!(first.work.backend_read_operations, 2);
    let cached = probe.probe_async(
        generation,
        &DependencyRegion::Metadata(file_id),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ).await?;
    assert_eq!(cached.value, first.value);
    assert_eq!(cached.work.backend_read_operations, 0);
    let absent = probe.probe_async(
        generation,
        &DependencyRegion::DirectoryName {
            directory_id: file_id,
            name: LogicalName::new(NameEncoding::Utf8, b"absent".to_vec(), 255)?,
        },
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ).await?;
    assert_eq!(absent.value, DependencyState::Absent);
    assert_eq!(absent.work.page_reads, 1);
    assert_eq!(absent.work.backend_read_operations, 1);

    let async_probe = AuthenticatedGenerationProbe::new(&store, ProbeLimits::default())?;
    let cancellation = CancellationToken::new();
    let metadata_region = DependencyRegion::Metadata(file_id);
    let mut future = std::pin::pin!(async_probe.probe_async(
        generation,
        &metadata_region,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let Poll::Ready(asynchronous) = future.as_mut().poll(&mut context) else {
        return Err("memory-backed asynchronous dependency probe remained pending".into());
    };
    assert_eq!(asynchronous?.value, first.value);

    let cancelled_probe = AuthenticatedGenerationProbe::new(&store, ProbeLimits::default())?;
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let mut future = std::pin::pin!(cancelled_probe.probe_async(
        generation,
        &metadata_region,
        WorkBudget::UNBOUNDED,
        &cancelled,
    ));
    let Poll::Ready(cancelled_result) = future.as_mut().poll(&mut context) else {
        return Err("pre-cancelled asynchronous dependency probe remained pending".into());
    };
    let failure = cancelled_result
        .err()
        .ok_or("pre-cancelled asynchronous dependency probe unexpectedly succeeded")?;
    assert!(matches!(failure.error, AuthenticatedProbeError::Cancelled));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[test]
fn poisoned_summary_caches_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, file_id, _) = fixture(&store, None)?;
    let region = DependencyRegion::Metadata(file_id);

    let generation_probe = AuthenticatedGenerationProbe::new(&store, ProbeLimits::default())?;
    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _async_guard = generation_probe.async_generations.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        std::panic::resume_unwind(Box::new("poison generation summary cache"));
    }));
    assert!(poisoned.is_err());
    let cancellation = CancellationToken::new();
    let failure = crate::async_storage::poll_ready(generation_probe.probe_async(
        generation,
        &region,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("poisoned asynchronous generation cache probe blocked")?
    .err()
    .ok_or("poisoned asynchronous generation cache probe succeeded")?;
    assert!(matches!(
        failure.error,
        AuthenticatedProbeError::CachePoisoned
    ));
    assert_eq!(*failure.work, WorkCounters::UNCHARGED);

    let record_probe = AuthenticatedGenerationProbe::new(&store, ProbeLimits::default())?;
    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _async_guard = record_probe.async_records.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        std::panic::resume_unwind(Box::new("poison record summary cache"));
    }));
    assert!(poisoned.is_err());
    let failure = crate::async_storage::poll_ready(record_probe.probe_async(
        generation,
        &region,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("poisoned asynchronous record cache probe blocked")?
    .err()
    .ok_or("poisoned asynchronous record cache probe succeeded")?;
    assert!(matches!(
        failure.error,
        AuthenticatedProbeError::CachePoisoned
    ));
    assert_eq!(*failure.work, WorkCounters::UNCHARGED);
    Ok(())
}

#[tokio::test]
async fn inserted_directory_entry_conflicts_with_an_observed_empty_page()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (base, directory_id, _) = fixture(&store, None)?;
    let (candidate, _, _) = fixture(&store, Some("phantom"))?;
    let probe = AuthenticatedGenerationProbe::new(&store, ProbeLimits::default())?;
    let region = DependencyRegion::DirectoryRange {
        directory_id,
        after: None,
        maximum_entries: 8,
    };
    let captured = probe.probe_async(base, &region, WorkBudget::UNBOUNDED, &CancellationToken::new()).await?;
    let dependencies = CheckoutDependencies::new(
        [Dependency {
            region: region.clone(),
            expected: captured.value,
        }],
        [],
        1,
    )?;
    let decision = classify_rebase_async(
        &probe,
        base,
        candidate,
        &dependencies,
        1,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ).await?;
    assert!(matches!(
        decision.decision,
        RebaseDecision::Conflicted {
            ref conflicts,
            truncated: false,
        } if conflicts.len() == 1 && conflicts[0].region == region
    ));

    Ok(())
}

#[tokio::test]
async fn content_digest_is_layout_independent_and_hash_budget_is_exact()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let probe = AuthenticatedGenerationProbe::new(&store, ProbeLimits::default())?;
    let (_, file_id, metadata) = fixture(&store, None)?;
    let domain = b"acyclic-fs-dependency-content-range-v1\0";
    let hash_bytes = u64::try_from(domain.len())?.saturating_add(16);
    let exact_budget = WorkBudget {
        bytes_hashed: hash_bytes,
        ..WorkBudget::UNBOUNDED
    };
    let inline = capture_content_range_bytes(&[0; 8], 8, exact_budget)?;
    let extents = put(&store, ObjectKind::ExtentPage, encode_extent_page(
        &ExtentPage::Leaf(vec![Extent {
            offset: 0,
            length: 8,
            kind: ExtentKind::Hole,
        }]), 8, |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()), |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
    )?)?;
    let sparse = probe.capture_record_async(
        Some(FileRecord { file_id, kind: FileKind::Regular, link_count: 1, metadata,
            payload: FilePayload::Regular { logical_bytes: 8, extents } }),
        &DependencyRegion::ContentRange { file_id, offset: 0, length: 8 },
        WorkBudget::UNBOUNDED, &CancellationToken::new(),
    ).await?;
    assert_eq!(inline.value, sparse.value);
    assert_eq!(inline.work.bytes_hashed, hash_bytes);
    let failure = capture_content_range_bytes(
        &[0; 8], 8,
        WorkBudget {
            bytes_hashed: hash_bytes - 1,
            ..WorkBudget::UNBOUNDED
        },
    )
    .err()
    .ok_or("underbudgeted inline hash unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        AuthenticatedProbeError::Work(WorkError::BudgetExceeded {
            counter: "bytes_hashed",
            observed,
            maximum,
        }) if observed == hash_bytes && maximum == hash_bytes - 1
    ));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[test]
fn canonical_content_range_surface_matches_golden_vector_and_fails_before_work()
-> Result<(), Box<dyn std::error::Error>> {
    let expected = hex::decode("e06aee566a0811dd9212ffd2d478c71bcc6fab338bfe63af1bc0ddc513366f64")?;
    let receipt = capture_content_range_bytes(b"hello", 5, WorkBudget::UNBOUNDED)?;
    assert_eq!(
        receipt.value,
        DependencyState::Present(Digest::from_bytes(
            expected
                .try_into()
                .map_err(|_: Vec<u8>| "golden content-range digest has the wrong length",)?
        ))
    );
    assert_eq!(receipt.work.bytes_hashed, 52);

    let cases = [
        capture_content_range_bytes(b"hello", 0, WorkBudget::UNBOUNDED)
            .err()
            .ok_or("zero maximum unexpectedly succeeded")?,
        capture_content_range_bytes(b"", 1, WorkBudget::UNBOUNDED)
            .err()
            .ok_or("empty range unexpectedly succeeded")?,
        capture_content_range_bytes(b"hello", 4, WorkBudget::UNBOUNDED)
            .err()
            .ok_or("oversized range unexpectedly succeeded")?,
        capture_content_range_bytes(
            b"hello",
            5,
            WorkBudget {
                bytes_hashed: 51,
                ..WorkBudget::UNBOUNDED
            },
        )
        .err()
        .ok_or("underbudgeted range unexpectedly succeeded")?,
    ];
    assert!(matches!(
        cases[0].error,
        AuthenticatedProbeError::InvalidLimits
    ));
    assert!(matches!(
        cases[1].error,
        AuthenticatedProbeError::InvalidDependency(DependencyError::EmptyContentRange)
    ));
    assert!(matches!(
        cases[2].error,
        AuthenticatedProbeError::ContentRangeTooLarge
    ));
    assert!(matches!(
        cases[3].error,
        AuthenticatedProbeError::Work(WorkError::BudgetExceeded {
            counter: "bytes_hashed",
            observed: 52,
            maximum: 51,
        })
    ));
    assert!(
        cases
            .iter()
            .all(|failure| *failure.work == WorkCounters::default())
    );
    Ok(())
}

#[tokio::test]
async fn probe_limits_reject_oversized_dependencies_before_backend_work()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let probe = AuthenticatedGenerationProbe::new(
        &store,
        ProbeLimits {
            maximum_content_payload_bytes: 8,
            maximum_directory_entries: 2,
            ..ProbeLimits::default()
        },
    )?;
    let generation = GenerationId::new(Digest::from_bytes([1; 32]));
    let file_id = FileId::from_bytes([2; 16]);
    let content = probe
        .probe_async(
            generation,
            &DependencyRegion::ContentRange {
                file_id,
                offset: 0,
                length: 9,
            },
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .err()
        .ok_or("oversized content dependency unexpectedly succeeded")?;
    assert!(matches!(
        content.error,
        AuthenticatedProbeError::ContentRangeTooLarge
    ));
    assert_eq!(*content.work, WorkCounters::default());

    let directory = probe
        .probe_async(
            generation,
            &DependencyRegion::DirectoryRange {
                directory_id: file_id,
                after: None,
                maximum_entries: 3,
            },
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .err()
        .ok_or("oversized directory dependency unexpectedly succeeded")?;
    assert!(matches!(
        directory.error,
        AuthenticatedProbeError::DirectoryPageTooLarge
    ));
    assert_eq!(*directory.work, WorkCounters::default());

    let empty = probe
        .probe_async(
            generation,
            &DependencyRegion::ContentRange {
                file_id,
                offset: 0,
                length: 0,
            },
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .err()
        .ok_or("empty content dependency unexpectedly succeeded")?;
    assert!(matches!(
        empty.error,
        AuthenticatedProbeError::InvalidDependency(DependencyError::EmptyContentRange)
    ));
    assert_eq!(*empty.work, WorkCounters::default());

    let overflow = probe
        .probe_async(
            generation,
            &DependencyRegion::ContentRange {
                file_id,
                offset: u64::MAX,
                length: 1,
            },
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .err()
        .ok_or("overflowing content dependency unexpectedly succeeded")?;
    assert!(matches!(
        overflow.error,
        AuthenticatedProbeError::InvalidDependency(DependencyError::RangeOverflow)
    ));
    assert_eq!(*overflow.work, WorkCounters::default());

    let zero_page = probe
        .probe_async(
            generation,
            &DependencyRegion::DirectoryRange {
                directory_id: file_id,
                after: None,
                maximum_entries: 0,
            },
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .err()
        .ok_or("zero-entry directory dependency unexpectedly succeeded")?;
    assert!(matches!(
        zero_page.error,
        AuthenticatedProbeError::InvalidDependency(DependencyError::ZeroDirectoryPageLimit)
    ));
    assert_eq!(*zero_page.work, WorkCounters::default());
    Ok(())
}

#[test]
fn every_zero_probe_bound_is_rejected_before_cache_or_backend_access() {
    let store = MemoryObjectStore::default();
    let defaults = ProbeLimits::default();
    let invalid = [
        ProbeLimits {
            maximum_cached_generations: 0,
            ..defaults
        },
        ProbeLimits {
            maximum_cached_records: 0,
            ..defaults
        },
        ProbeLimits {
            maximum_extent_spans: 0,
            ..defaults
        },
        ProbeLimits {
            maximum_content_payload_bytes: 0,
            ..defaults
        },
        ProbeLimits {
            maximum_directory_entries: 0,
            ..defaults
        },
        ProbeLimits {
            decode: DecodeLimits {
                maximum_page_items: 0,
                ..defaults.decode
            },
            ..defaults
        },
    ];
    for limits in invalid {
        assert!(matches!(
            AuthenticatedGenerationProbe::new(&store, limits),
            Err(AuthenticatedProbeError::InvalidLimits)
        ));
    }
}

#[tokio::test]
async fn bounded_summary_cache_eviction_reauthenticates_without_changing_state()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation_a, file_id, metadata) = fixture(&store, None)?;
    let (generation_b, _, _) = fixture(&store, Some("other"))?;
    let probe = AuthenticatedGenerationProbe::new(
        &store,
        ProbeLimits {
            maximum_cached_generations: 1,
            maximum_cached_records: 1,
            ..ProbeLimits::default()
        },
    )?;
    let region = DependencyRegion::Metadata(file_id);
    let first = probe.probe_async(generation_a, &region, WorkBudget::UNBOUNDED, &CancellationToken::new()).await?;
    assert_eq!(first.value, DependencyState::Present(metadata.digest));
    assert!(first.work.backend_read_operations > 0);
    let cached = probe.probe_async(generation_a, &region, WorkBudget::UNBOUNDED, &CancellationToken::new()).await?;
    assert_eq!(cached.value, first.value);
    assert_eq!(cached.work.backend_read_operations, 0);
    let other = probe.probe_async(generation_b, &region, WorkBudget::UNBOUNDED, &CancellationToken::new()).await?;
    assert_eq!(other.value, first.value);
    assert!(other.work.backend_read_operations > 0);
    let reauthenticated = probe.probe_async(generation_a, &region, WorkBudget::UNBOUNDED, &CancellationToken::new()).await?;
    assert_eq!(reauthenticated.value, first.value);
    assert!(reauthenticated.work.backend_read_operations > 0);
    Ok(())
}

#[tokio::test]
async fn non_regular_dependency_regions_use_authenticated_record_state()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, file_ids) = non_regular_fixture(&store)?;
    let name = LogicalName::new(NameEncoding::Utf8, b"not-a-directory-entry".to_vec(), 255)?;
    for file_id in file_ids {
        let baseline_probe = AuthenticatedGenerationProbe::new(&store, ProbeLimits::default())?;
        let baseline = baseline_probe.probe_async(
            generation,
            &DependencyRegion::FileRecord(file_id),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        ).await?;
        let regions = [
            DependencyRegion::FileLength(file_id),
            DependencyRegion::ContentRange {
                file_id,
                offset: 0,
                length: 1,
            },
            DependencyRegion::SparseSeek {
                file_id,
                offset: 0,
                target: ExtentSeekTarget::Data,
            },
            DependencyRegion::DirectoryName {
                directory_id: file_id,
                name: name.clone(),
            },
            DependencyRegion::DirectoryRange {
                directory_id: file_id,
                after: None,
                maximum_entries: 1,
            },
        ];
        for region in regions {
            let asynchronous_probe =
                AuthenticatedGenerationProbe::new(&store, ProbeLimits::default())?;
            let asynchronous = crate::async_storage::poll_ready(asynchronous_probe.probe_async(
                generation,
                &region,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            ))
            .ok_or("memory-backed dependency probe remained pending")??;
            assert_eq!(asynchronous.value, baseline.value);
        }
    }
    Ok(())
}

#[tokio::test]
async fn inline_content_probe_rejects_overflow_and_eof_before_hashing()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let probe = AuthenticatedGenerationProbe::new(&store, ProbeLimits::default())?;
    let file_id = FileId::from_bytes([1; 16]);
    let record = Some(FileRecord {
        file_id, kind: FileKind::Regular, link_count: 1,
        metadata: ObjectId { kind: ObjectKind::Metadata, digest: Digest::from_bytes([0; 32]) },
        payload: FilePayload::InlineRegular(InlineFileData::new(b"abcdefgh")?),
    });
    for (offset, length) in [(u64::MAX, 1), (7, 2)] {
        let failure = probe.capture_record_async(
            record, &DependencyRegion::ContentRange { file_id, offset, length },
            WorkBudget::UNBOUNDED, &CancellationToken::new(),
        ).await
        .err()
        .ok_or("invalid inline content range unexpectedly succeeded")?;
        if offset == u64::MAX {
            assert!(matches!(failure.error, AuthenticatedProbeError::InvalidDependency(DependencyError::RangeOverflow)));
        } else {
            assert!(matches!(failure.error, AuthenticatedProbeError::Extent(ExtentReadError::InvalidRange)));
        }
        assert_eq!(*failure.work, WorkCounters::default());
    }
    let exact = probe.capture_record_async(
        record, &DependencyRegion::ContentRange { file_id, offset: 4, length: 4 },
        WorkBudget::UNBOUNDED, &CancellationToken::new(),
    ).await?;
    assert!(matches!(exact.value, DependencyState::Present(_)));
    assert_eq!(exact.work.backend_read_operations, 0);
    assert!(exact.work.bytes_hashed > 4);
    Ok(())
}

#[test]
fn every_sparse_dependency_region_preserves_ordinary_and_native_evidence()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let SparseProbeFixture {
        generation,
        root_id,
        inline_id,
        regular_id,
        regular_name,
    } = sparse_probe_fixture(&store)?;
    let missing_name = LogicalName::new(NameEncoding::Utf8, b"missing".to_vec(), 255)?;
    let regions = vec![
        DependencyRegion::FileRecord(regular_id),
        DependencyRegion::Metadata(regular_id),
        DependencyRegion::FileLength(inline_id),
        DependencyRegion::FileLength(regular_id),
        DependencyRegion::ContentRange {
            file_id: inline_id,
            offset: 2,
            length: 4,
        },
        DependencyRegion::ContentRange {
            file_id: regular_id,
            offset: 0,
            length: 16,
        },
        DependencyRegion::SparseSeek {
            file_id: inline_id,
            offset: 0,
            target: ExtentSeekTarget::Data,
        },
        DependencyRegion::SparseSeek {
            file_id: regular_id,
            offset: 0,
            target: ExtentSeekTarget::Data,
        },
        DependencyRegion::SparseSeek {
            file_id: regular_id,
            offset: 0,
            target: ExtentSeekTarget::Hole,
        },
        DependencyRegion::SparseSeek {
            file_id: regular_id,
            offset: 17,
            target: ExtentSeekTarget::Data,
        },
        DependencyRegion::SparseSeek {
            file_id: regular_id,
            offset: 17,
            target: ExtentSeekTarget::Hole,
        },
        DependencyRegion::FileRecord(FileId::from_bytes([99; 16])),
        DependencyRegion::DirectoryName {
            directory_id: root_id,
            name: regular_name.clone(),
        },
        DependencyRegion::DirectoryName {
            directory_id: root_id,
            name: missing_name,
        },
        DependencyRegion::DirectoryRange {
            directory_id: root_id,
            after: None,
            maximum_entries: 1,
        },
        DependencyRegion::DirectoryRange {
            directory_id: root_id,
            after: Some(regular_name),
            maximum_entries: 1,
        },
    ];
    let mut page_states = Vec::new();
    for region in regions {
        let asynchronous_probe = AuthenticatedGenerationProbe::new(&store, ProbeLimits::default())?;
        let asynchronous = crate::async_storage::poll_ready(asynchronous_probe.probe_async(
            generation,
            &region,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        ))
        .ok_or("memory-backed sparse probe remained pending")??;
        #[cfg(not(target_arch = "wasm32"))]
        {
            let cancellation = CancellationToken::new();
            let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
                .map_err(|(error, _)| error)?;
            let baseline = original.live_allocation_bytes();
            let mut accounting = KernelAccounting::native(&original, &cancellation);
            let (native_probe, constructor) = AuthenticatedGenerationProbe::new_with_accounting(
                &store, ProbeLimits::default(), &mut accounting,
            )?;
            let native = crate::async_storage::poll_ready(native_probe.probe_async_with_accounting(
                generation, &region, &mut accounting, &cancellation,
            )).ok_or("original sparse probe remained pending")??;
            assert_eq!(native, asynchronous.value);
            native_probe.retire_with_accounting(&mut accounting)?;
            drop(constructor);
            assert_eq!(original.live_allocation_bytes(), baseline);
        }
        if matches!(region, DependencyRegion::DirectoryRange { .. }) {
            page_states.push(asynchronous.value);
        }
    }
    assert_eq!(page_states.len(), 2);
    assert_ne!(page_states[0], page_states[1]);
    Ok(())
}

#[test]
fn bounded_batch_capture_reuses_generation_and_record_frontiers()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, file_id, metadata) = fixture(&store, None)?;
    let regions = vec![
        DependencyRegion::Metadata(file_id),
        DependencyRegion::FileRecord(file_id),
        DependencyRegion::Metadata(file_id),
    ];
    let probe = AuthenticatedGenerationProbe::new(&store, ProbeLimits::default())?;
    let captured = crate::async_storage::poll_ready(probe.capture_many_async(
        generation,
        regions.clone(),
        3,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .ok_or("memory-backed batch probe remained pending")??;

    assert_eq!(captured.value.len(), 3);
    assert_eq!(captured.value[0].region, regions[0]);
    assert_eq!(captured.value[1].region, regions[1]);
    assert_eq!(captured.value[2], captured.value[0]);
    assert_eq!(
        captured.value[0].expected,
        DependencyState::Present(metadata.digest)
    );
    assert_eq!(captured.work.backend_read_operations, 2);
    assert!(captured.work.items_returned >= 3);
    assert!(captured.work.bytes_copied >= 3 * usize_to_u64(size_of::<Dependency>()));

    let rejected_probe = AuthenticatedGenerationProbe::new(&store, ProbeLimits::default())?;
    let rejected = crate::async_storage::poll_ready(rejected_probe.capture_many_async(
        generation,
        regions,
        2,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .ok_or("memory-backed rejected batch remained pending")?
    .err()
    .ok_or("excessive batch unexpectedly succeeded")?;
    assert!(matches!(
        rejected.error,
        AuthenticatedProbeError::TooManyRegions { maximum: 2 }
    ));
    assert_eq!(*rejected.work, WorkCounters::UNCHARGED);
    Ok(())
}

#[tokio::test]
async fn authenticated_batch_admits_dependency_copy_before_output_push()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, file_id, metadata) = fixture(&store, None)?;
    let probe = AuthenticatedGenerationProbe::new(&store, ProbeLimits::default())?;
    let cancellation = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let record = probe.record_with_accounting(generation, file_id, &mut accounting, &cancellation).await?;
    for budget in [
        WorkBudget { bytes_copied: 0, ..WorkBudget::UNBOUNDED },
        WorkBudget { items_returned: 0, ..WorkBudget::UNBOUNDED },
    ] {
        let failure = crate::async_storage::poll_ready(probe.capture_records_many_async(
            vec![(record, DependencyRegion::Metadata(file_id))], 1, budget, &cancellation,
        )).ok_or("dependency copy denial remained pending")?.err().ok_or("output push exceeded its budget")?;
        assert!(matches!(failure.error, AuthenticatedProbeError::Work(WorkError::BudgetExceeded { .. })));
        assert_eq!(failure.work.bytes_copied, 0);
        assert_eq!(failure.work.items_returned, 0);
        assert_eq!(failure.work.backend_read_operations, 0);
        assert_eq!(failure.work.allocation_operations, 1);
    }
    let copied = usize_to_u64(size_of::<Dependency>());
    let receipt = crate::async_storage::poll_ready(probe.capture_records_many_async(
        vec![(record, DependencyRegion::Metadata(file_id))], 1,
        WorkBudget { bytes_copied: copied, items_returned: 1, ..WorkBudget::UNBOUNDED }, &cancellation,
    )).ok_or("dependency output push remained pending")??;
    assert_eq!(receipt.value.len(), 1);
    assert_eq!(receipt.value[0].expected, DependencyState::Present(metadata.digest));
    assert_eq!(receipt.work.bytes_copied, copied);
    assert_eq!(receipt.work.items_returned, 1);
    assert_eq!(receipt.work.backend_read_operations, 0);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
struct PanicProbeStore<'a> {
    store: MemoryObjectStore,
    original: &'a crate::NativeMaterializationWork,
    reads: std::sync::atomic::AtomicU64,
    reached: Mutex<Option<(u64, WorkCounters)>>,
}

#[cfg(not(target_arch = "wasm32"))]
impl AsyncObjectStore for PanicProbeStore<'_> {
    async fn put(
        &self, object: ObjectId, bytes: OwnedBytes, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> crate::storage::ObjectResult<()> {
        AsyncObjectStore::put(&self.store, object, bytes, budget, cancellation).await
    }

    async fn read(
        &self, object: ObjectId, maximum_bytes: u64, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> crate::storage::ObjectResult<crate::storage::ObjectRead> {
        AsyncObjectStore::read(&self.store, object, maximum_bytes, budget, cancellation).await
    }

    async fn read_many(
        &self, requests: &[crate::storage::ObjectReadRequest], budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::storage::ObjectResult<Vec<crate::storage::ObjectRead>> {
        crate::async_storage::read_many_sequential_async(self, requests, budget, cancellation).await
    }

    async fn contains(
        &self, object: ObjectId, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> crate::storage::ObjectResult<bool> {
        AsyncObjectStore::contains(&self.store, object, budget, cancellation).await
    }

    async fn read_with_native_work(
        &self, object: ObjectId, maximum_bytes: u64, work: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<crate::async_storage::NativeObjectRead, ObjectStoreError> {
        cancellation.check().map_err(|_| ObjectStoreError::Cancelled)?;
        work.admit(WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED })
            .map_err(ObjectStoreError::Native)?;
        self.reads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if object.kind == ObjectKind::FileTablePage {
            *self.reached.lock().unwrap_or_else(std::sync::PoisonError::into_inner) =
                Some((self.original.live_allocation_bytes(), self.original.receipt()));
            std::panic::resume_unwind(Box::new("cold original authenticated probe read"));
        }
        AsyncObjectStore::read_with_native_work(&self.store, object, maximum_bytes, work, cancellation).await
    }

    async fn read_many_with_native_work(
        &self, requests: &[crate::storage::ObjectReadRequest], work: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<crate::async_storage::NativeObjectReadBatch, ObjectStoreError> {
        crate::async_storage::read_many_sequential_with_native_work(self, requests, work, cancellation).await
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn original_probe_denies_before_backend_and_retires_panic_temporaries()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, file_id, _) = fixture(&store, None)?;
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(
        WorkBudget { backend_read_operations: 0, ..WorkBudget::UNBOUNDED }, &cancellation,
    ).map_err(|(error, _)| error)?;
    let store = PanicProbeStore {
        store, original: &original, reads: std::sync::atomic::AtomicU64::new(0), reached: Mutex::new(None),
    };
    let baseline = original.live_allocation_bytes();
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let (probe, constructor) = AuthenticatedGenerationProbe::new_with_accounting(
        &store, ProbeLimits::default(), &mut accounting,
    )?;
    let region = DependencyRegion::Metadata(file_id);
    let denied = crate::async_storage::poll_ready(super::super::rebase::AccountingRebaseProbe::probe_with_accounting(
        &probe, generation, &region, &mut accounting, &cancellation,
    )).ok_or("original probe denial remained pending")?.err().ok_or("zero read budget accepted")?;
    assert!(matches!(denied, super::super::rebase::ClassifyRebaseFailure::Accounting(
        crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::Work(_))
    )));
    assert_eq!(store.reads.load(std::sync::atomic::Ordering::Relaxed), 0);
    assert_eq!(original.receipt().backend_read_operations, 0);
    probe.retire_with_accounting(&mut accounting)?;
    drop(constructor);
    assert_eq!(original.live_allocation_bytes(), baseline);

    let store = store.store;
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let store = PanicProbeStore {
        store, original: &original, reads: std::sync::atomic::AtomicU64::new(0), reached: Mutex::new(None),
    };
    let baseline = original.live_allocation_bytes();
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let (probe, constructor) = AuthenticatedGenerationProbe::new_with_accounting(
        &store, ProbeLimits::default(), &mut accounting,
    )?;
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        crate::async_storage::poll_ready(probe.probe_async_with_accounting(
            generation, &region, &mut accounting, &cancellation,
        ))
    }));
    assert!(panic.is_err());
    let (reached_live, reached_work) = store.reached.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
        .ok_or("original file-table read was not reached")?;
    let cached = probe.async_generations.lock().unwrap_or_else(std::sync::PoisonError::into_inner).entries.bytes
        + probe.async_records.lock().unwrap_or_else(std::sync::PoisonError::into_inner).entries.bytes;
    assert_eq!(original.live_allocation_bytes(), baseline + cached);
    assert!(reached_live > original.live_allocation_bytes());
    assert_eq!(original.receipt(), reached_work);
    assert_eq!(original.receipt().backend_read_operations, 1);
    cancellation.cancel();
    probe.retire_with_accounting(&mut accounting)?;
    drop(constructor);
    assert_eq!(original.live_allocation_bytes(), baseline);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_probe_generation_caches_preserve_dependency_conflicts()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (base, directory_id, _) = fixture(&store, None)?;
    let (candidate, _, _) = fixture(&store, Some("phantom"))?;
    let region = DependencyRegion::DirectoryRange { directory_id, after: None, maximum_entries: 8 };
    let ordinary = AuthenticatedGenerationProbe::new(&store, ProbeLimits::default())?;
    let expected = ordinary.probe_async(base, &region, WorkBudget::UNBOUNDED, &CancellationToken::new()).await?.value;
    let dependencies = CheckoutDependencies::new([Dependency { region: region.clone(), expected }], [], 1)?;
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let baseline = original.live_allocation_bytes();
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let (probe, constructor) = AuthenticatedGenerationProbe::new_with_accounting(
        &store, ProbeLimits { maximum_cached_generations: 1, maximum_cached_records: 1, ..ProbeLimits::default() },
        &mut accounting,
    )?;
    let source = crate::async_storage::poll_ready(probe.probe_async_with_accounting(
        base, &region, &mut accounting, &cancellation,
    )).ok_or("original source probe remained pending")??;
    let destination = crate::async_storage::poll_ready(probe.probe_async_with_accounting(
        candidate, &region, &mut accounting, &cancellation,
    )).ok_or("original destination probe remained pending")??;
    assert_eq!(source, expected);
    assert_ne!(destination, source);
    let source_again = crate::async_storage::poll_ready(probe.probe_async_with_accounting(
        base, &region, &mut accounting, &cancellation,
    )).ok_or("original source re-probe remained pending")??;
    assert_eq!(source_again, source);
    let (decision, allocation) = crate::async_storage::poll_ready(super::super::rebase::classify_rebase_with_accounting(
        &probe, base, candidate, &dependencies, 1, &mut accounting, &cancellation,
    )).ok_or("original classification remained pending")?
        .map_err(|failure| -> Box<dyn std::error::Error> {
            match failure {
                super::super::rebase::ClassifyRebaseFailure::Accounting(error)
                | super::super::rebase::ClassifyRebaseFailure::Probe(error) => Box::new(error),
                super::super::rebase::ClassifyRebaseFailure::ZeroConflictLimit => "zero conflict limit".into(),
            }
        })?;
    assert!(matches!(&decision, RebaseDecision::Conflicted { conflicts, truncated: false }
        if conflicts.len() == 1 && conflicts[0].region == region
            && conflicts[0].expected == source && conflicts[0].actual == destination));
    drop(decision);
    drop(allocation);
    probe.retire_with_accounting(&mut accounting)?;
    drop(constructor);
    assert_eq!(original.live_allocation_bytes(), baseline);
    Ok(())
}

#[test]
fn stack_dependency_denial_precedes_array_encoding()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    for budget in [
        WorkBudget { bytes_encoded: 0, ..WorkBudget::UNBOUNDED },
        WorkBudget { bytes_hashed: 0, ..WorkBudget::UNBOUNDED },
    ] {
        let reached = std::cell::Cell::new(false);
        let mut accounting = KernelAccounting::standalone(budget, &cancellation);
        let denied = capture_stack_state_with_accounting(
            || { reached.set(true); [0_u8; 10] },
            b"acyclic-fs-dependency-sparse-seek-v1\0", &mut accounting, &cancellation, None,
        ).err().ok_or("stack dependency exceeded its admission")?;
        assert!(matches!(denied, crate::FsError::Work(WorkError::BudgetExceeded { .. })));
        assert!(!reached.get());
        assert_eq!(accounting.spent(), WorkCounters::UNCHARGED);
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn original_stack_encoding_retains_completed_prefix_on_late_cancellation()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let before = original.receipt();
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let result = capture_stack_state_with_accounting(
        || { cancellation.cancel(); [0_u8; 10] },
        b"acyclic-fs-dependency-sparse-seek-v1\0", &mut accounting, &cancellation, None,
    );
    assert!(result.is_err());
    assert_eq!(original.receipt().bytes_encoded, before.bytes_encoded + 10);
    assert_eq!(original.receipt().bytes_hashed, before.bytes_hashed);
    assert_eq!(original.receipt().allocation_operations, before.allocation_operations);
    Ok(())
}
