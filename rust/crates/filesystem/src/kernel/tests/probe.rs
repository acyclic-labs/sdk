use super::*;
use crate::foundation::VolumeId;
use crate::kernel::{
    BlobChunkRef, BlobNode, BlobPage, CheckoutDependencies, Dependency, Extent, ExtentPage,
    FileKind, FileMetadata, FileTablePage, InlineFileData, LogicalName, MetadataField,
    NameEncoding, RebaseDecision, TreeEntry, TreePage, classify_rebase, encode_blob_page,
    encode_extent_page, encode_file_metadata, encode_file_table_page, encode_generation_root,
};
use crate::memory::MemoryObjectStore;
use crate::storage::{ObjectStore, object_digest};
use bytes::Bytes;
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
    ObjectStore::put(store, id, Bytes::from(bytes), WorkBudget::UNBOUNDED)?;
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
        })?,
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
        encode_tree_page(&TreePage::Leaf(tree_entries), 8)?,
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
        encode_file_table_page(&FileTablePage::Leaf(records), 8)?,
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
        })?,
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
        })?,
    )?;
    let root_id = FileId::from_bytes([11; 16]);
    let symbolic_link_id = FileId::from_bytes([12; 16]);
    let empty_id = FileId::from_bytes([13; 16]);
    let device_id = FileId::from_bytes([14; 16]);
    let reparse_id = FileId::from_bytes([15; 16]);
    let empty_tree = put(
        store,
        ObjectKind::TreePage,
        encode_tree_page(&TreePage::Leaf(Vec::new()), 8)?,
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
        encode_file_table_page(&FileTablePage::Leaf(records), 8)?,
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
        })?,
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

#[allow(clippy::too_many_lines)]
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
        })?,
    )?;
    let chunk = put(store, ObjectKind::BlobChunk, b"DATA".to_vec())?;
    let blob = put(
        store,
        ObjectKind::Blob,
        encode_blob_page(
            &BlobPage {
                first_offset: 0,
                end_offset: 4,
                node: BlobNode::Leaf(vec![BlobChunkRef {
                    first_offset: 0,
                    end_offset: 4,
                    chunk,
                }]),
            },
            8,
        )?,
    )?;
    let extents = put(
        store,
        ObjectKind::ExtentPage,
        encode_extent_page(
            &ExtentPage::Leaf(vec![
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
            ]),
            8,
        )?,
    )?;
    let root_id = FileId::from_bytes([21; 16]);
    let inline_id = FileId::from_bytes([22; 16]);
    let regular_id = FileId::from_bytes([23; 16]);
    let inline_name = LogicalName::new(NameEncoding::Utf8, b"inline".to_vec(), 255)?;
    let regular_name = LogicalName::new(NameEncoding::Utf8, b"regular".to_vec(), 255)?;
    let tree = put(
        store,
        ObjectKind::TreePage,
        encode_tree_page(
            &TreePage::Leaf(vec![
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
            ]),
            8,
        )?,
    )?;
    let table = put(
        store,
        ObjectKind::FileTablePage,
        encode_file_table_page(
            &FileTablePage::Leaf(vec![
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
            ]),
            8,
        )?,
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
        })?,
    )?;
    Ok(SparseProbeFixture {
        generation: GenerationId::new(root.digest),
        root_id,
        inline_id,
        regular_id,
        regular_name,
    })
}

#[test]
fn immutable_summaries_are_cached_without_becoming_authority()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, file_id, metadata) = fixture(&store, None)?;
    let probe = AuthenticatedGenerationProbe::new(&store, ProbeLimits::default())?;
    let first = probe.probe(
        generation,
        &DependencyRegion::Metadata(file_id),
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(first.value, DependencyState::Present(metadata.digest));
    assert_eq!(first.work.backend_read_operations, 2);
    let cached = probe.probe(
        generation,
        &DependencyRegion::Metadata(file_id),
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(cached.value, first.value);
    assert_eq!(cached.work, WorkCounters::default());
    let absent = probe.probe(
        generation,
        &DependencyRegion::DirectoryName {
            directory_id: file_id,
            name: LogicalName::new(NameEncoding::Utf8, b"absent".to_vec(), 255)?,
        },
        WorkBudget::UNBOUNDED,
    )?;
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
    assert_eq!(asynchronous?, first);

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
fn poisoned_summary_caches_fail_closed_for_sync_and_async_probes()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, file_id, _) = fixture(&store, None)?;
    let region = DependencyRegion::Metadata(file_id);

    let generation_probe = AuthenticatedGenerationProbe::new(&store, ProbeLimits::default())?;
    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = generation_probe
            .generations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        std::panic::resume_unwind(Box::new("poison generation summary cache"));
    }));
    assert!(poisoned.is_err());
    let failure = generation_probe
        .probe(generation, &region, WorkBudget::UNBOUNDED)
        .err()
        .ok_or("poisoned synchronous generation cache probe succeeded")?;
    assert!(matches!(
        failure.error,
        AuthenticatedProbeError::CachePoisoned
    ));
    assert_eq!(*failure.work, WorkCounters::default());
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
    assert_eq!(*failure.work, WorkCounters::default());

    let record_probe = AuthenticatedGenerationProbe::new(&store, ProbeLimits::default())?;
    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = record_probe
            .records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        std::panic::resume_unwind(Box::new("poison record summary cache"));
    }));
    assert!(poisoned.is_err());
    let failure = record_probe
        .probe(generation, &region, WorkBudget::UNBOUNDED)
        .err()
        .ok_or("poisoned synchronous record cache probe succeeded")?;
    assert!(matches!(
        failure.error,
        AuthenticatedProbeError::CachePoisoned
    ));
    assert_eq!(*failure.work, WorkCounters::default());
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
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[test]
fn inserted_directory_entry_conflicts_with_an_observed_empty_page()
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
    let captured = probe.probe(base, &region, WorkBudget::UNBOUNDED)?;
    let dependencies = CheckoutDependencies::new(
        [Dependency {
            region: region.clone(),
            expected: captured.value,
        }],
        [],
        1,
    )?;
    let decision = classify_rebase(
        &probe,
        base,
        candidate,
        &dependencies,
        1,
        WorkBudget::UNBOUNDED,
    )?;
    assert!(matches!(
        decision.decision,
        RebaseDecision::Conflicted {
            ref conflicts,
            truncated: false,
        } if conflicts.len() == 1 && conflicts[0].region == region
    ));

    let async_probe = AuthenticatedGenerationProbe::new(&store, ProbeLimits::default())?;
    let cancellation = CancellationToken::new();
    let mut future = std::pin::pin!(super::super::classify_rebase_async(
        &async_probe,
        base,
        candidate,
        &dependencies,
        1,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let Poll::Ready(asynchronous) = future.as_mut().poll(&mut context) else {
        return Err("memory-backed asynchronous rebase remained pending".into());
    };
    assert_eq!(asynchronous?, decision);
    Ok(())
}

#[test]
fn content_digest_is_layout_independent_and_hash_budget_is_exact()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let probe = AuthenticatedGenerationProbe::new(&store, ProbeLimits::default())?;
    let data = InlineFileData::new(&[0; 8])?;
    let domain = b"acyclic-fs-dependency-content-range-v1\0";
    let hash_bytes = u64::try_from(domain.len())?.saturating_add(16);
    let exact_budget = WorkBudget {
        bytes_hashed: hash_bytes,
        ..WorkBudget::UNBOUNDED
    };
    let inline = AuthenticatedGenerationProbe::<MemoryObjectStore>::inline_content_state(
        data,
        0,
        8,
        exact_budget,
        WorkCounters::default(),
    )?;
    let sparse = probe.hash_content_spans(
        vec![super::super::ExtentSlice {
            offset: 0,
            length: 8,
            source_end: 8,
            kind: ExtentKind::Hole,
        }],
        8,
        WorkCounters {
            bytes_hashed: hash_bytes,
            ..WorkCounters::default()
        },
        exact_budget,
        WorkCounters::default(),
    )?;
    assert_eq!(inline.value, sparse.value);
    assert_eq!(inline.work.bytes_hashed, hash_bytes);
    assert_eq!(sparse.work.bytes_hashed, hash_bytes);

    let failure = AuthenticatedGenerationProbe::<MemoryObjectStore>::inline_content_state(
        data,
        0,
        8,
        WorkBudget {
            bytes_hashed: hash_bytes - 1,
            ..WorkBudget::UNBOUNDED
        },
        WorkCounters::default(),
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

#[test]
fn probe_limits_reject_oversized_dependencies_before_backend_work()
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
        .probe(
            generation,
            &DependencyRegion::ContentRange {
                file_id,
                offset: 0,
                length: 9,
            },
            WorkBudget::UNBOUNDED,
        )
        .err()
        .ok_or("oversized content dependency unexpectedly succeeded")?;
    assert!(matches!(
        content.error,
        AuthenticatedProbeError::ContentRangeTooLarge
    ));
    assert_eq!(*content.work, WorkCounters::default());

    let directory = probe
        .probe(
            generation,
            &DependencyRegion::DirectoryRange {
                directory_id: file_id,
                after: None,
                maximum_entries: 3,
            },
            WorkBudget::UNBOUNDED,
        )
        .err()
        .ok_or("oversized directory dependency unexpectedly succeeded")?;
    assert!(matches!(
        directory.error,
        AuthenticatedProbeError::DirectoryPageTooLarge
    ));
    assert_eq!(*directory.work, WorkCounters::default());

    let empty = probe
        .probe(
            generation,
            &DependencyRegion::ContentRange {
                file_id,
                offset: 0,
                length: 0,
            },
            WorkBudget::UNBOUNDED,
        )
        .err()
        .ok_or("empty content dependency unexpectedly succeeded")?;
    assert!(matches!(
        empty.error,
        AuthenticatedProbeError::InvalidDependency(DependencyError::EmptyContentRange)
    ));
    assert_eq!(*empty.work, WorkCounters::default());

    let overflow = probe
        .probe(
            generation,
            &DependencyRegion::ContentRange {
                file_id,
                offset: u64::MAX,
                length: 1,
            },
            WorkBudget::UNBOUNDED,
        )
        .err()
        .ok_or("overflowing content dependency unexpectedly succeeded")?;
    assert!(matches!(
        overflow.error,
        AuthenticatedProbeError::InvalidDependency(DependencyError::RangeOverflow)
    ));
    assert_eq!(*overflow.work, WorkCounters::default());

    let zero_page = probe
        .probe(
            generation,
            &DependencyRegion::DirectoryRange {
                directory_id: file_id,
                after: None,
                maximum_entries: 0,
            },
            WorkBudget::UNBOUNDED,
        )
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

#[test]
fn bounded_summary_cache_eviction_reauthenticates_without_changing_state()
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
    let first = probe.probe(generation_a, &region, WorkBudget::UNBOUNDED)?;
    assert_eq!(first.value, DependencyState::Present(metadata.digest));
    assert!(first.work.backend_read_operations > 0);
    let cached = probe.probe(generation_a, &region, WorkBudget::UNBOUNDED)?;
    assert_eq!(cached.value, first.value);
    assert_eq!(cached.work, WorkCounters::default());
    let other = probe.probe(generation_b, &region, WorkBudget::UNBOUNDED)?;
    assert_eq!(other.value, first.value);
    assert!(other.work.backend_read_operations > 0);
    let reauthenticated = probe.probe(generation_a, &region, WorkBudget::UNBOUNDED)?;
    assert_eq!(reauthenticated.value, first.value);
    assert!(reauthenticated.work.backend_read_operations > 0);
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn non_regular_dependency_regions_fall_back_to_authenticated_record_state()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, file_ids) = non_regular_fixture(&store)?;
    let name = LogicalName::new(NameEncoding::Utf8, b"not-a-directory-entry".to_vec(), 255)?;
    for file_id in file_ids {
        let baseline_probe = AuthenticatedGenerationProbe::new(&store, ProbeLimits::default())?;
        let baseline = baseline_probe.probe(
            generation,
            &DependencyRegion::FileRecord(file_id),
            WorkBudget::UNBOUNDED,
        )?;
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
            let synchronous_probe =
                AuthenticatedGenerationProbe::new(&store, ProbeLimits::default())?;
            let synchronous =
                synchronous_probe.probe(generation, &region, WorkBudget::UNBOUNDED)?;
            assert_eq!(synchronous.value, baseline.value);

            let asynchronous_probe =
                AuthenticatedGenerationProbe::new(&store, ProbeLimits::default())?;
            let asynchronous = crate::async_storage::poll_ready(asynchronous_probe.probe_async(
                generation,
                &region,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            ))
            .ok_or("memory-backed dependency probe remained pending")??;
            assert_eq!(asynchronous, synchronous);
        }
    }
    Ok(())
}

#[test]
fn inline_content_probe_rejects_overflow_and_eof_before_hashing()
-> Result<(), Box<dyn std::error::Error>> {
    let data = InlineFileData::new(b"abcdefgh")?;
    for (offset, length) in [(u64::MAX, 1), (7, 2)] {
        let failure = AuthenticatedGenerationProbe::<MemoryObjectStore>::inline_content_state(
            data,
            offset,
            length,
            WorkBudget::UNBOUNDED,
            WorkCounters::default(),
        )
        .err()
        .ok_or("invalid inline content range unexpectedly succeeded")?;
        assert!(matches!(
            failure.error,
            AuthenticatedProbeError::Extent(ExtentReadError::InvalidRange)
        ));
        assert_eq!(*failure.work, WorkCounters::default());
    }
    let exact = AuthenticatedGenerationProbe::<MemoryObjectStore>::inline_content_state(
        data,
        4,
        4,
        WorkBudget::UNBOUNDED,
        WorkCounters::default(),
    )?;
    assert!(matches!(exact.value, DependencyState::Present(_)));
    assert_eq!(exact.work.backend_read_operations, 0);
    assert!(exact.work.bytes_hashed > 4);
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn every_sparse_dependency_region_has_identical_sync_and_async_evidence()
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
        let synchronous_probe = AuthenticatedGenerationProbe::new(&store, ProbeLimits::default())?;
        let synchronous = synchronous_probe.probe(generation, &region, WorkBudget::UNBOUNDED)?;
        let asynchronous_probe = AuthenticatedGenerationProbe::new(&store, ProbeLimits::default())?;
        let asynchronous = crate::async_storage::poll_ready(asynchronous_probe.probe_async(
            generation,
            &region,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        ))
        .ok_or("memory-backed sparse probe remained pending")??;
        assert_eq!(asynchronous, synchronous);
        if matches!(region, DependencyRegion::DirectoryRange { .. }) {
            page_states.push(synchronous.value);
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
    assert_eq!(*rejected.work, WorkCounters::default());
    Ok(())
}

#[test]
fn authenticated_record_capture_avoids_generation_rereads_and_fails_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, file_id, metadata) = fixture(&store, None)?;
    let probe = AuthenticatedGenerationProbe::new(&store, ProbeLimits::default())?;
    let (record, authenticated_work) = probe.record(generation, file_id, WorkBudget::UNBOUNDED)?;
    assert_eq!(authenticated_work.backend_read_operations, 2);
    let region = DependencyRegion::Metadata(file_id);
    let captured = crate::async_storage::poll_ready(probe.capture_record_async(
        record,
        &region,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .ok_or("direct record capture remained pending")??;
    assert_eq!(captured.value, DependencyState::Present(metadata.digest));
    assert_eq!(captured.work.backend_read_operations, 0);

    let absent = crate::async_storage::poll_ready(probe.capture_record_async(
        None,
        &region,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .ok_or("absent record capture remained pending")??;
    assert_eq!(absent.value, DependencyState::Absent);
    assert_eq!(absent.work, WorkCounters::default());

    let wrong_region = DependencyRegion::Metadata(FileId::from_bytes([99; 16]));
    let mismatch = crate::async_storage::poll_ready(probe.capture_record_async(
        record,
        &wrong_region,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .ok_or("mismatched record capture remained pending")?
    .err()
    .ok_or("mismatched record identity was accepted")?;
    assert!(matches!(
        mismatch.error,
        AuthenticatedProbeError::RecordIdentityMismatch
    ));
    assert_eq!(*mismatch.work, WorkCounters::default());

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let cancellation = crate::async_storage::poll_ready(probe.capture_record_async(
        None,
        &region,
        WorkBudget::UNBOUNDED,
        &cancelled,
    ))
    .ok_or("cancelled record capture remained pending")?
    .err()
    .ok_or("cancelled record capture succeeded")?;
    assert!(matches!(
        cancellation.error,
        AuthenticatedProbeError::Cancelled
    ));
    assert_eq!(*cancellation.work, WorkCounters::default());
    Ok(())
}

#[test]
fn probe_cache_seek_and_batch_admission_helpers_are_total() -> Result<(), Box<dyn std::error::Error>>
{
    let store = MemoryObjectStore::default();
    let (generation, file_id, _) = fixture(&store, None)?;
    let probe = AuthenticatedGenerationProbe::new(&store, ProbeLimits::default())?;
    let (first, first_work) = probe.generation(generation, WorkBudget::UNBOUNDED)?;
    assert_eq!(first.volume_id, VolumeId::from_bytes([2; 16]));
    assert_eq!(first_work.backend_read_operations, 1);
    let (cached, cached_work) = probe.generation(generation, WorkBudget::UNBOUNDED)?;
    assert_eq!(cached, first);
    assert_eq!(cached_work, WorkCounters::default());

    let inline = InlineFileData::new(b"abc")?;
    assert_eq!(
        inline_seek_result(inline, 0, ExtentSeekTarget::Data),
        Some(0)
    );
    assert_eq!(inline_seek_result(inline, 3, ExtentSeekTarget::Data), None);
    assert_eq!(
        inline_seek_result(inline, 3, ExtentSeekTarget::Hole),
        Some(3)
    );
    assert_eq!(inline_seek_result(inline, 4, ExtentSeekTarget::Hole), None);

    let zero = validate_capture_batch(0, 0)
        .err()
        .ok_or("zero batch limit was accepted")?;
    assert!(matches!(
        zero.error,
        AuthenticatedProbeError::ZeroBatchLimit
    ));
    let excessive = validate_capture_batch(2, 1)
        .err()
        .ok_or("excessive capture batch was accepted")?;
    assert!(matches!(
        excessive.error,
        AuthenticatedProbeError::TooManyRegions { maximum: 1 }
    ));
    validate_capture_batch(1, 1)?;

    let regions = vec![DependencyRegion::FileRecord(file_id)];
    let allocation = allocate_capture_output(&regions, WorkBudget::UNBOUNDED)?;
    assert_eq!(allocation.0.capacity(), 1);
    assert!(allocation.1.peak_allocation_bytes > 0);
    let rejected = allocate_capture_output(&regions, WorkBudget::default())
        .err()
        .ok_or("nonempty capture allocation exceeded a zero budget")?;
    assert!(matches!(
        rejected.error,
        AuthenticatedProbeError::Work(WorkError::BudgetExceeded { .. })
    ));
    let empty = allocate_capture_output(&Vec::<DependencyRegion>::new(), WorkBudget::default())?;
    assert!(empty.0.is_empty());
    assert_eq!(empty.1, WorkCounters::default());
    Ok(())
}
