use super::*;
use crate::async_storage::poll_ready;
use crate::foundation::{Digest, FileId};
use crate::kernel::{
    FileKind, FileRangeRequest, FileRecord, build_blob, decode_extent_page, read_file_range_async,
};
use crate::memory::MemoryObjectStore;
use crate::model::{
    CaseSensitivity, ConcurrencyMode, FilesystemProfile, Lifecycle, UnicodePolicy, VolumeLimits,
};
use crate::storage::{ByteRange, ObjectStore};

fn config() -> VolumeConfig {
    VolumeConfig {
        profile: FilesystemProfile::Portable,
        concurrency: ConcurrencyMode::Optimistic,
        lifecycle: Lifecycle::Ephemeral,
        case_sensitivity: CaseSensitivity::Sensitive,
        unicode: UnicodePolicy::Preserve,
        symbolic_links: true,
        hard_links: true,
        sparse_files: true,
        limits: VolumeLimits::default(),
    }
}

fn blob(store: &MemoryObjectStore, bytes: &[u8]) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let mut source = Cursor::new(bytes);
    Ok(build_blob(
        store,
        &mut source,
        BlobBuildOptions {
            chunk_bytes: 64,
            page_items: 8,
            page_bytes: 4096,
            maximum_blob_bytes: u64::try_from(bytes.len())?,
        },
        WorkBudget::UNBOUNDED,
    )?
    .root)
}

fn apply(
    store: &MemoryObjectStore,
    payload: FilePayload,
    mutation: RegularMutation,
) -> Result<RegularMutationReceipt, RegularMutationFailure> {
    poll_ready(apply_regular_mutation_async(
        store,
        payload,
        mutation,
        config(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .unwrap_or_else(|| {
        Err(OperationFailure::before_work(
            RegularMutationError::Storage(ObjectStoreError::Cancelled),
        ))
    })
}

fn clone_range(
    store: &MemoryObjectStore,
    source: FilePayload,
    source_offset: u64,
    destination: FilePayload,
    destination_offset: u64,
    length: u64,
) -> Result<RegularCloneReceipt, RegularMutationFailure> {
    poll_ready(apply_regular_clone_async(
        store,
        source,
        source_offset,
        destination,
        destination_offset,
        length,
        config(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .unwrap_or_else(|| {
        Err(OperationFailure::before_work(
            RegularMutationError::Storage(ObjectStoreError::Cancelled),
        ))
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PhysicalByteKind {
    Hole,
    AllocatedZero,
    Content,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ModeledByte {
    kind: PhysicalByteKind,
    value: u8,
}

impl ModeledByte {
    const HOLE: Self = Self {
        kind: PhysicalByteKind::Hole,
        value: 0,
    };
    const ALLOCATED_ZERO: Self = Self {
        kind: PhysicalByteKind::AllocatedZero,
        value: 0,
    };

    const fn content(value: u8) -> Self {
        Self {
            kind: PhysicalByteKind::Content,
            value,
        }
    }
}

fn flatten_extents(
    store: &MemoryObjectStore,
    root: ObjectId,
) -> Result<Vec<Extent>, Box<dyn std::error::Error>> {
    let encoded = ObjectStore::read(store, root, u64::MAX, WorkBudget::UNBOUNDED)?.value;
    Ok(
        match decode_extent_page(&encoded, decode_limits(config()))? {
            ExtentPage::Leaf(extents) => extents,
            ExtentPage::Internal(children) => {
                let mut extents = Vec::new();
                for child in children {
                    extents.extend(flatten_extents(store, child.page)?);
                }
                extents
            }
        },
    )
}

fn payload_size(payload: FilePayload) -> Result<u64, Box<dyn std::error::Error>> {
    match payload {
        FilePayload::InlineRegular(data) => Ok(u64::try_from(data.as_bytes().len())?),
        FilePayload::Regular { logical_bytes, .. } => Ok(logical_bytes),
        _ => Err("modeled payload stopped being regular".into()),
    }
}

fn payload_kinds(
    store: &MemoryObjectStore,
    payload: FilePayload,
) -> Result<Vec<PhysicalByteKind>, Box<dyn std::error::Error>> {
    match payload {
        FilePayload::InlineRegular(data) => {
            Ok(vec![PhysicalByteKind::Content; data.as_bytes().len()])
        }
        FilePayload::Regular {
            logical_bytes,
            extents,
        } => {
            let mut kinds = Vec::new();
            for extent in flatten_extents(store, extents)? {
                assert_eq!(extent.offset, u64::try_from(kinds.len())?);
                let kind = match extent.kind {
                    ExtentKind::Hole => PhysicalByteKind::Hole,
                    ExtentKind::AllocatedZero => PhysicalByteKind::AllocatedZero,
                    ExtentKind::Content { .. } => PhysicalByteKind::Content,
                };
                kinds.extend(std::iter::repeat_n(kind, usize::try_from(extent.length)?));
            }
            assert_eq!(u64::try_from(kinds.len())?, logical_bytes);
            Ok(kinds)
        }
        _ => Err("modeled payload stopped being regular".into()),
    }
}

fn payload_bytes(
    store: &MemoryObjectStore,
    payload: FilePayload,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let logical_bytes = payload_size(payload)?;
    let read = poll_ready(read_file_range_async(
        store,
        FileRangeRequest {
            record: FileRecord {
                file_id: FileId::from_bytes([247; 16]),
                kind: FileKind::Regular,
                link_count: 1,
                metadata: ObjectId {
                    kind: ObjectKind::Metadata,
                    digest: Digest::from_bytes([248; 32]),
                },
                payload,
            },
            range: ByteRange {
                offset: 0,
                length: logical_bytes,
            },
            maximum_spans: 1_024,
            limits: decode_limits(config()),
            budget: WorkBudget::UNBOUNDED,
        },
        &CancellationToken::new(),
    ))
    .ok_or("modeled read blocked")??;
    Ok(read.bytes.to_vec())
}

fn assert_payload_matches_model(
    store: &MemoryObjectStore,
    payload: FilePayload,
    model: &[ModeledByte],
) -> Result<(), Box<dyn std::error::Error>> {
    let expected_kinds: Vec<_> = model.iter().map(|byte| byte.kind).collect();
    let actual_kinds = payload_kinds(store, payload)?;
    if actual_kinds != expected_kinds {
        return Err(format!(
            "physical byte kinds diverged: actual={actual_kinds:?}, expected={expected_kinds:?}"
        )
        .into());
    }
    let actual_bytes = payload_bytes(store, payload)?;
    let expected_bytes = model.iter().map(|byte| byte.value).collect::<Vec<_>>();
    if actual_bytes != expected_bytes {
        return Err(format!(
            "logical bytes diverged: actual={actual_bytes:?}, expected={expected_bytes:?}"
        )
        .into());
    }
    Ok(())
}

fn model_replace(model: &mut Vec<ModeledByte>, offset: usize, bytes: &[u8]) {
    model.resize(model.len().max(offset), ModeledByte::HOLE);
    model.resize(model.len().max(offset + bytes.len()), ModeledByte::HOLE);
    for (target, value) in model[offset..offset + bytes.len()].iter_mut().zip(bytes) {
        *target = ModeledByte::content(*value);
    }
}

fn model_zero(
    model: &mut Vec<ModeledByte>,
    offset: usize,
    length: usize,
    allocated: bool,
    extend: bool,
    allocated_is_inline_content: bool,
) {
    if !extend && offset >= model.len() {
        return;
    }
    let requested_end = offset + length;
    if extend {
        model.resize(model.len().max(requested_end), ModeledByte::HOLE);
    }
    let end = requested_end.min(model.len());
    let replacement = if allocated_is_inline_content {
        ModeledByte::content(0)
    } else if allocated {
        ModeledByte::ALLOCATED_ZERO
    } else {
        ModeledByte::HOLE
    };
    model[offset..end].fill(replacement);
}

fn model_preallocate(
    model: &mut Vec<ModeledByte>,
    offset: usize,
    length: usize,
    keep_size: bool,
) -> bool {
    let end = offset + length;
    if keep_size && end > model.len() {
        return false;
    }
    if !keep_size {
        model.resize(model.len().max(end), ModeledByte::HOLE);
    }
    let bounded_end = end.min(model.len());
    for byte in &mut model[offset..bounded_end] {
        if byte.kind == PhysicalByteKind::Hole {
            *byte = ModeledByte::ALLOCATED_ZERO;
        }
    }
    true
}

fn next_generated(seed: &mut u64) -> u64 {
    *seed = seed
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    *seed
}

#[test]
fn writes_at_sixty_three_and_sixty_four_bytes_remain_inline()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let content = blob(&store, b"xy")?;
    for (initial, offset, expected_last) in [(63_usize, 62_u64, b'x'), (63, 63, b'y')] {
        let receipt = apply(
            &store,
            FilePayload::InlineRegular(InlineFileData::new(&vec![b'a'; initial])?),
            RegularMutation::Write {
                offset,
                length: 1,
                content,
                content_offset: u64::from(expected_last == b'y'),
            },
        )?;
        let FilePayload::InlineRegular(data) = receipt.payload else {
            return Err("small write unexpectedly promoted inline data".into());
        };
        assert_eq!(
            data.as_bytes().len(),
            usize::try_from(offset + 1)?.max(initial)
        );
        assert_eq!(data.as_bytes()[usize::try_from(offset)?], expected_last);
    }
    Ok(())
}

#[test]
fn sixty_fifth_byte_promotes_to_sparse_without_materializing_a_hole()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let content = blob(&store, b"z")?;
    let receipt = apply(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(&[b'a'; 64])?),
        RegularMutation::Write {
            offset: 64,
            length: 1,
            content,
            content_offset: 0,
        },
    )?;
    let FilePayload::Regular {
        logical_bytes,
        extents,
    } = receipt.payload
    else {
        return Err("sixty-fifth byte did not promote inline data".into());
    };
    assert_eq!(logical_bytes, 65);
    let encoded = ObjectStore::read(
        &store,
        extents,
        config().limits.maximum_object_bytes,
        WorkBudget::UNBOUNDED,
    )?
    .value;
    let ExtentPage::Leaf(values) = decode_extent_page(&encoded, decode_limits(config()))? else {
        return Err("promoted extent root was not a leaf".into());
    };
    assert_eq!(values.len(), 2);
    assert_eq!((values[0].offset, values[0].length), (0, 64));
    assert_eq!((values[1].offset, values[1].length), (64, 1));
    assert!(matches!(values[0].kind, ExtentKind::Content { .. }));
    assert!(matches!(values[1].kind, ExtentKind::Content { .. }));

    let demoted = apply(
        &store,
        receipt.payload,
        RegularMutation::Resize { logical_bytes: 64 },
    )?;
    let FilePayload::InlineRegular(data) = demoted.payload else {
        return Err("dense sixty-four-byte shrink did not demote inline".into());
    };
    assert_eq!(data.as_bytes(), &[b'a'; 64]);

    let sparse_gap = apply(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(&[b'a'; 63])?),
        RegularMutation::Write {
            offset: 64,
            length: 1,
            content,
            content_offset: 0,
        },
    )?;
    let still_sparse = apply(
        &store,
        sparse_gap.payload,
        RegularMutation::Resize { logical_bytes: 64 },
    )?;
    assert!(matches!(still_sparse.payload, FilePayload::Regular { .. }));

    let promoted = poll_ready(promote_inline(
        &store,
        InlineFileData::new(&[b'q'; 64])?,
        config(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .ok_or("memory-backed inline promotion unexpectedly blocked")??;
    let promoted_payload = FilePayload::Regular {
        logical_bytes: promoted.logical_bytes,
        extents: promoted.extents,
    };
    let clone_demoted = clone_range(&store, promoted_payload, 0, promoted_payload, 4, 4)?;
    let FilePayload::InlineRegular(clone_data) = clone_demoted.destination else {
        return Err("dense small sparse clone did not demote inline".into());
    };
    assert_eq!(clone_data.as_bytes(), &[b'q'; 64]);
    Ok(())
}

#[test]
fn empty_inline_growth_promotes_directly_to_one_sparse_hole()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let receipt = apply(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(&[])?),
        RegularMutation::Resize { logical_bytes: 128 },
    )?;
    let FilePayload::Regular {
        logical_bytes,
        extents,
    } = receipt.payload
    else {
        return Err("empty inline growth unexpectedly remained inline".into());
    };
    assert_eq!(logical_bytes, 128);
    assert_eq!(receipt.work.source_bytes_read, 0);
    let encoded = ObjectStore::read(
        &store,
        extents,
        config().limits.maximum_object_bytes,
        WorkBudget::UNBOUNDED,
    )?
    .value;
    let ExtentPage::Leaf(values) = decode_extent_page(&encoded, decode_limits(config()))? else {
        return Err("empty growth extent root was not a leaf".into());
    };
    assert_eq!(values.len(), 1);
    assert_eq!((values[0].offset, values[0].length), (0, 128));
    assert!(matches!(values[0].kind, ExtentKind::Hole));
    Ok(())
}

#[test]
fn inline_growth_within_threshold_preserves_trailing_hole() -> Result<(), Box<dyn std::error::Error>>
{
    let store = MemoryObjectStore::default();
    let receipt = apply(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(b"a")?),
        RegularMutation::Resize { logical_bytes: 64 },
    )?;
    let FilePayload::Regular {
        logical_bytes,
        extents,
    } = receipt.payload
    else {
        return Err("inline growth with a trailing hole demoted incorrectly".into());
    };
    assert_eq!(logical_bytes, 64);
    let encoded = ObjectStore::read(
        &store,
        extents,
        config().limits.maximum_object_bytes,
        WorkBudget::UNBOUNDED,
    )?
    .value;
    let ExtentPage::Leaf(values) = decode_extent_page(&encoded, decode_limits(config()))? else {
        return Err("grown extent root was not a leaf".into());
    };
    assert_eq!(values.len(), 2);
    assert!(matches!(values[0].kind, ExtentKind::Content { .. }));
    assert!(matches!(values[1].kind, ExtentKind::Hole));
    assert_eq!((values[1].offset, values[1].length), (1, 63));
    Ok(())
}

#[test]
fn inline_shrink_and_allocated_zero_are_allocation_free() -> Result<(), Box<dyn std::error::Error>>
{
    let store = MemoryObjectStore::default();
    let shrunk = apply(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(&[7; 64])?),
        RegularMutation::Resize { logical_bytes: 63 },
    )?;
    assert_eq!(shrunk.work, WorkCounters::default());
    let zeroed = apply(
        &store,
        shrunk.payload,
        RegularMutation::ZeroRange {
            offset: 2,
            length: 4,
            allocated: true,
            extend: false,
        },
    )?;
    assert_eq!(zeroed.work.allocation_operations, 0);
    assert_eq!(zeroed.work.bytes_copied, 4);
    let FilePayload::InlineRegular(data) = zeroed.payload else {
        return Err("allocated zero unexpectedly promoted inline data".into());
    };
    assert_eq!(&data.as_bytes()[2..6], &[0; 4]);
    let cloned = clone_range(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(b"abcdef")?),
        0,
        FilePayload::InlineRegular(InlineFileData::new(b"000000")?),
        1,
        4,
    )?;
    assert_eq!(cloned.work.bytes_copied, 4);
    assert_eq!(cloned.work.allocation_operations, 0);
    let FilePayload::InlineRegular(cloned_data) = cloned.destination else {
        return Err("small clone unexpectedly promoted inline data".into());
    };
    assert_eq!(cloned_data.as_bytes(), b"0abcd0");
    Ok(())
}

#[test]
fn clone_promotion_matrix_preserves_dense_and_sparse_semantics()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();

    let extended_inline = clone_range(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(b"abcd")?),
        0,
        FilePayload::InlineRegular(InlineFileData::new(b"z")?),
        1,
        4,
    )?;
    let FilePayload::InlineRegular(extended_inline) = extended_inline.destination else {
        return Err("small extending clone unexpectedly promoted".into());
    };
    assert_eq!(extended_inline.as_bytes(), b"zabcd");

    let sparse_source = apply(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(&[b's'; 64])?),
        RegularMutation::Resize { logical_bytes: 128 },
    )?;
    let promoted_destination = clone_range(
        &store,
        sparse_source.payload,
        0,
        FilePayload::InlineRegular(InlineFileData::new(b"zzzz")?),
        4,
        4,
    )?;
    let FilePayload::InlineRegular(promoted_destination) = promoted_destination.destination else {
        return Err("dense small clone did not demote after sparse execution".into());
    };
    assert_eq!(promoted_destination.as_bytes(), b"zzzzssss");

    let sparse_destination = apply(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(&[b'd'; 64])?),
        RegularMutation::Resize { logical_bytes: 128 },
    )?;
    let promoted_source = clone_range(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(b"abcd")?),
        0,
        sparse_destination.payload,
        80,
        4,
    )?;
    let FilePayload::Regular {
        logical_bytes,
        extents,
    } = promoted_source.destination
    else {
        return Err("clone into sparse destination unexpectedly demoted".into());
    };
    assert_eq!(logical_bytes, 128);
    let encoded = ObjectStore::read(
        &store,
        extents,
        config().limits.maximum_object_bytes,
        WorkBudget::UNBOUNDED,
    )?
    .value;
    let ExtentPage::Leaf(extents) = decode_extent_page(&encoded, decode_limits(config()))? else {
        return Err("clone destination root was not a leaf".into());
    };
    assert!(extents.iter().any(|extent| {
        extent.offset == 80
            && extent.length == 4
            && matches!(extent.kind, ExtentKind::Content { .. })
    }));

    let same = FilePayload::InlineRegular(InlineFileData::new(b"same")?);
    let overlapping_identity = clone_range(&store, same, 0, same, 8, 4)?;
    let FilePayload::Regular {
        logical_bytes,
        extents,
    } = overlapping_identity.destination
    else {
        return Err("hole-bearing identity clone unexpectedly remained inline".into());
    };
    assert_eq!(logical_bytes, 12);
    let encoded = ObjectStore::read(
        &store,
        extents,
        config().limits.maximum_object_bytes,
        WorkBudget::UNBOUNDED,
    )?
    .value;
    let ExtentPage::Leaf(extents) = decode_extent_page(&encoded, decode_limits(config()))? else {
        return Err("identity clone destination root was not a leaf".into());
    };
    assert!(extents.iter().any(|extent| {
        extent.offset == 4 && extent.length == 4 && matches!(extent.kind, ExtentKind::Hole)
    }));
    Ok(())
}

#[test]
fn clone_budget_and_precancellation_fail_before_visible_work()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let source = FilePayload::InlineRegular(InlineFileData::new(b"abcd")?);
    let destination = FilePayload::InlineRegular(InlineFileData::new(b"0000")?);
    let budget = WorkBudget {
        bytes_copied: 3,
        ..WorkBudget::UNBOUNDED
    };
    let failure = poll_ready(apply_regular_clone_async(
        &store,
        source,
        0,
        destination,
        0,
        4,
        config(),
        budget,
        &CancellationToken::new(),
    ))
    .ok_or("budgeted clone blocked")?
    .err()
    .ok_or("over-budget clone unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        RegularMutationError::Work(WorkError::BudgetExceeded {
            counter: "bytes_copied",
            observed: 4,
            maximum: 3,
        })
    ));
    assert_eq!(*failure.work, WorkCounters::default());

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let failure = poll_ready(apply_regular_clone_async(
        &store,
        source,
        0,
        destination,
        0,
        1,
        config(),
        WorkBudget::UNBOUNDED,
        &cancelled,
    ))
    .ok_or("cancelled clone blocked")?
    .err()
    .ok_or("cancelled clone unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        RegularMutationError::Storage(ObjectStoreError::Cancelled)
    ));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn regular_mutation_cancellation_nonregular_clone_and_inline_budget_matrix_is_total()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let content = blob(&store, b"xy")?;
    let inline = FilePayload::InlineRegular(InlineFileData::new(b"abcd")?);
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    for mutation in [
        RegularMutation::Write {
            offset: 0,
            length: 1,
            content,
            content_offset: 0,
        },
        RegularMutation::Resize { logical_bytes: 2 },
        RegularMutation::ZeroRange {
            offset: 0,
            length: 1,
            allocated: true,
            extend: false,
        },
        RegularMutation::Preallocate {
            offset: 0,
            length: 1,
            keep_size: true,
        },
    ] {
        let failure = poll_ready(apply_regular_mutation_async(
            &store,
            inline,
            mutation,
            config(),
            WorkBudget::UNBOUNDED,
            &cancelled,
        ))
        .ok_or("pre-cancelled regular mutation blocked")?
        .err()
        .ok_or("pre-cancelled regular mutation succeeded")?;
        assert!(matches!(
            failure.error,
            RegularMutationError::Storage(ObjectStoreError::Cancelled)
        ));
        assert_eq!(*failure.work, WorkCounters::default());
    }

    let typed = |kind| ObjectId {
        kind,
        digest: Digest::from_bytes([91; 32]),
    };
    for payload in [
        FilePayload::Empty,
        FilePayload::Directory {
            entries: typed(ObjectKind::TreePage),
        },
        FilePayload::SymbolicLink {
            target_bytes: 1,
            target: typed(ObjectKind::Blob),
        },
        FilePayload::Device { major: 1, minor: 3 },
        FilePayload::ReparsePoint {
            payload_bytes: 1,
            payload: typed(ObjectKind::Blob),
        },
    ] {
        let failure = apply(
            &store,
            payload,
            RegularMutation::Resize { logical_bytes: 0 },
        )
        .err()
        .ok_or("non-regular payload mutation succeeded")?;
        assert!(matches!(failure.error, RegularMutationError::NotRegular));
        assert_eq!(*failure.work, WorkCounters::default());
    }

    let source_failure = clone_range(&store, FilePayload::Empty, 0, inline, 0, 1)
        .err()
        .ok_or("non-regular clone source succeeded")?;
    assert!(matches!(
        source_failure.error,
        RegularMutationError::NotRegular
    ));
    assert_eq!(*source_failure.work, WorkCounters::default());
    let destination_failure = clone_range(&store, inline, 0, FilePayload::Empty, 0, 1)
        .err()
        .ok_or("non-regular clone destination succeeded")?;
    assert!(matches!(
        destination_failure.error,
        RegularMutationError::NotRegular
    ));
    assert_eq!(*destination_failure.work, WorkCounters::default());

    let range_failure = apply(
        &store,
        inline,
        RegularMutation::Write {
            offset: 0,
            length: 2,
            content,
            content_offset: 1,
        },
    )
    .err()
    .ok_or("out-of-range blob write succeeded")?;
    assert!(matches!(
        range_failure.error,
        RegularMutationError::BlobRead(BlobReadError::InvalidRange)
    ));
    assert!(range_failure.work.backend_read_operations > 0);

    let baseline = apply(
        &store,
        inline,
        RegularMutation::Write {
            offset: 1,
            length: 1,
            content,
            content_offset: 0,
        },
    )?;
    for (counter, maximum) in [
        ("bytes_copied", baseline.work.bytes_copied - 1),
        ("object_bytes_read", baseline.work.object_bytes_read - 1),
    ] {
        let mut budget = WorkBudget::UNBOUNDED;
        match counter {
            "bytes_copied" => budget.bytes_copied = maximum,
            "object_bytes_read" => budget.object_bytes_read = maximum,
            _ => return Err("unknown budget counter".into()),
        }
        let failure = poll_ready(apply_regular_mutation_async(
            &store,
            inline,
            RegularMutation::Write {
                offset: 1,
                length: 1,
                content,
                content_offset: 0,
            },
            config(),
            budget,
            &CancellationToken::new(),
        ))
        .ok_or("budgeted inline write blocked")?
        .err()
        .ok_or("under-budget inline write succeeded")?;
        match counter {
            "bytes_copied" => assert!(matches!(
                failure.error,
                RegularMutationError::Work(WorkError::BudgetExceeded {
                    counter: "bytes_copied",
                    maximum: actual_maximum,
                    ..
                }) if actual_maximum == maximum
            )),
            "object_bytes_read" => match failure.error {
                RegularMutationError::BlobRead(BlobReadError::Storage(ObjectStoreError::Work(
                    WorkError::BudgetExceeded {
                        counter,
                        observed,
                        maximum: actual_maximum,
                    },
                ))) => {
                    assert_eq!(counter, "object_bytes_read");
                    assert!(observed > actual_maximum);
                    assert!(actual_maximum < baseline.work.object_bytes_read);
                }
                error => {
                    return Err(format!("unexpected object-read budget failure: {error:#?}").into());
                }
            },
            _ => return Err("unknown budget counter".into()),
        }
    }
    Ok(())
}

#[test]
fn inline_zero_extension_preserves_exact_sparse_semantics() -> Result<(), Box<dyn std::error::Error>>
{
    let store = MemoryObjectStore::default();
    let inline = FilePayload::InlineRegular(InlineFileData::new(b"a")?);
    let allocated = apply(
        &store,
        inline,
        RegularMutation::ZeroRange {
            offset: 1,
            length: 3,
            allocated: true,
            extend: true,
        },
    )?;
    let FilePayload::InlineRegular(data) = allocated.payload else {
        return Err("contiguous allocated-zero extension did not remain inline".into());
    };
    assert_eq!(data.as_bytes(), b"a\0\0\0");
    assert_eq!(allocated.work.bytes_copied, 3);

    for (offset, allocated) in [(1_u64, false), (3, true)] {
        let promoted = apply(
            &store,
            inline,
            RegularMutation::ZeroRange {
                offset,
                length: 2,
                allocated,
                extend: true,
            },
        )?;
        let FilePayload::Regular {
            logical_bytes,
            extents,
        } = promoted.payload
        else {
            return Err("hole-bearing zero extension remained inline".into());
        };
        assert_eq!(logical_bytes, offset + 2);
        let encoded = ObjectStore::read(
            &store,
            extents,
            config().limits.maximum_object_bytes,
            WorkBudget::UNBOUNDED,
        )?
        .value;
        let ExtentPage::Leaf(values) = decode_extent_page(&encoded, decode_limits(config()))?
        else {
            return Err("promoted extent root was not a leaf".into());
        };
        assert!(
            values
                .iter()
                .any(|extent| matches!(extent.kind, ExtentKind::Hole))
        );
        if allocated {
            assert!(
                values
                    .iter()
                    .any(|extent| matches!(extent.kind, ExtentKind::AllocatedZero))
            );
        }
    }

    let oversized = apply(
        &store,
        inline,
        RegularMutation::ZeroRange {
            offset: 1,
            length: 64,
            allocated: true,
            extend: true,
        },
    )?;
    assert!(matches!(oversized.payload, FilePayload::Regular { .. }));
    Ok(())
}

#[test]
fn sparse_nonextending_zero_clips_at_eof_and_is_inert_beyond_eof()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let sparse = apply(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(&[b'a'; 64])?),
        RegularMutation::Resize { logical_bytes: 128 },
    )?;
    let clipped = apply(
        &store,
        sparse.payload,
        RegularMutation::ZeroRange {
            offset: 120,
            length: 16,
            allocated: true,
            extend: false,
        },
    )?;
    assert_eq!(payload_size(clipped.payload)?, 128);
    assert_eq!(
        &payload_kinds(&store, clipped.payload)?[120..],
        &[PhysicalByteKind::AllocatedZero; 8]
    );

    let outside = apply(
        &store,
        clipped.payload,
        RegularMutation::ZeroRange {
            offset: 128,
            length: 8,
            allocated: true,
            extend: false,
        },
    )?;
    assert_eq!(outside.payload, clipped.payload);
    assert_eq!(outside.work, WorkCounters::default());
    Ok(())
}

#[test]
fn preallocation_preserves_content_allocates_only_holes_and_handles_eof_explicitly()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let sparse = apply(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(&[b'a'; 64])?),
        RegularMutation::Resize { logical_bytes: 128 },
    )?;
    let allocated = apply(
        &store,
        sparse.payload,
        RegularMutation::Preallocate {
            offset: 32,
            length: 64,
            keep_size: true,
        },
    )?;
    let FilePayload::Regular {
        logical_bytes,
        extents,
    } = allocated.payload
    else {
        return Err("preallocation unexpectedly produced inline data".into());
    };
    assert_eq!(logical_bytes, 128);
    let encoded = ObjectStore::read(
        &store,
        extents,
        config().limits.maximum_object_bytes,
        WorkBudget::UNBOUNDED,
    )?
    .value;
    let ExtentPage::Leaf(values) = decode_extent_page(&encoded, decode_limits(config()))? else {
        return Err("preallocated extent root was not a leaf".into());
    };
    assert_eq!(values.len(), 3);
    assert_eq!((values[0].offset, values[0].length), (0, 64));
    assert!(matches!(values[0].kind, ExtentKind::Content { .. }));
    assert_eq!((values[1].offset, values[1].length), (64, 32));
    assert!(matches!(values[1].kind, ExtentKind::AllocatedZero));
    assert_eq!((values[2].offset, values[2].length), (96, 32));
    assert!(matches!(values[2].kind, ExtentKind::Hole));

    let extended = apply(
        &store,
        allocated.payload,
        RegularMutation::Preallocate {
            offset: 120,
            length: 40,
            keep_size: false,
        },
    )?;
    let FilePayload::Regular {
        logical_bytes,
        extents,
    } = extended.payload
    else {
        return Err("extending preallocation unexpectedly produced inline data".into());
    };
    assert_eq!(logical_bytes, 160);
    let encoded = ObjectStore::read(
        &store,
        extents,
        config().limits.maximum_object_bytes,
        WorkBudget::UNBOUNDED,
    )?
    .value;
    let ExtentPage::Leaf(values) = decode_extent_page(&encoded, decode_limits(config()))? else {
        return Err("extended extent root was not a leaf".into());
    };
    assert!(values.iter().any(|extent| {
        extent.offset == 120
            && extent.length == 40
            && matches!(extent.kind, ExtentKind::AllocatedZero)
    }));

    let failure = apply(
        &store,
        sparse.payload,
        RegularMutation::Preallocate {
            offset: 120,
            length: 40,
            keep_size: true,
        },
    )
    .err()
    .ok_or("unrepresentable keep-size allocation unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        RegularMutationError::KeepSizeBeyondEofUnsupported
    ));
    Ok(())
}

#[test]
fn extending_preallocation_merges_the_eof_hole_and_extension_into_one_mutation()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let sparse = apply(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(&[b'a'; 64])?),
        RegularMutation::Resize { logical_bytes: 128 },
    )?;
    let mut constrained = config();
    constrained.limits.maximum_mutations_per_batch = 1;
    let allocated = poll_ready(apply_regular_mutation_async(
        &store,
        sparse.payload,
        RegularMutation::Preallocate {
            offset: 64,
            length: 128,
            keep_size: false,
        },
        constrained,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .ok_or("bounded preallocation blocked")??;
    let FilePayload::Regular {
        logical_bytes,
        extents,
    } = allocated.payload
    else {
        return Err("extending preallocation unexpectedly produced inline data".into());
    };
    assert_eq!(logical_bytes, 192);
    let encoded = ObjectStore::read(
        &store,
        extents,
        config().limits.maximum_object_bytes,
        WorkBudget::UNBOUNDED,
    )?
    .value;
    let ExtentPage::Leaf(values) = decode_extent_page(&encoded, decode_limits(config()))? else {
        return Err("allocated extent root was not a leaf".into());
    };
    assert_eq!(values.len(), 2);
    assert_eq!((values[1].offset, values[1].length), (64, 128));
    assert!(matches!(values[1].kind, ExtentKind::AllocatedZero));
    Ok(())
}

#[test]
fn preallocation_budget_rejections_report_only_completed_work()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let sparse = apply(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(&[b'a'; 64])?),
        RegularMutation::Resize { logical_bytes: 128 },
    )?;
    let mut allocation_budget = WorkBudget::UNBOUNDED;
    allocation_budget.allocation_operations = 0;
    let failure = poll_ready(apply_regular_mutation_async(
        &store,
        sparse.payload,
        RegularMutation::Preallocate {
            offset: 128,
            length: 64,
            keep_size: false,
        },
        config(),
        allocation_budget,
        &CancellationToken::new(),
    ))
    .ok_or("allocation-limited preallocation blocked")?
    .err()
    .ok_or("allocation-limited preallocation unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        RegularMutationError::Work(WorkError::BudgetExceeded {
            counter: "allocation_operations",
            observed: 1,
            maximum: 0,
        })
    ));
    assert_eq!(*failure.work, WorkCounters::default());

    let sparse = apply(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(&[b'b'; 64])?),
        RegularMutation::Resize { logical_bytes: 128 },
    )?;
    let mut scan_budget = WorkBudget::UNBOUNDED;
    scan_budget.items_examined = 5;
    let failure = poll_ready(apply_regular_mutation_async(
        &store,
        sparse.payload,
        RegularMutation::Preallocate {
            offset: 64,
            length: 32,
            keep_size: true,
        },
        config(),
        scan_budget,
        &CancellationToken::new(),
    ))
    .ok_or("scan-limited preallocation blocked")?
    .err()
    .ok_or("scan-limited preallocation unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        RegularMutationError::Work(WorkError::BudgetExceeded {
            counter: "items_examined",
            observed: 6,
            maximum: 5,
        })
    ));
    assert_eq!(failure.work.items_examined, 5);
    assert_eq!(failure.work.backend_write_operations, 0);
    Ok(())
}

#[test]
fn hole_bearing_sparse_shrink_attempts_inline_demotion_once()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let sparse = apply(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(&[b'a'; 64])?),
        RegularMutation::Resize { logical_bytes: 128 },
    )?;
    let with_hole = apply(
        &store,
        sparse.payload,
        RegularMutation::ZeroRange {
            offset: 16,
            length: 16,
            allocated: false,
            extend: false,
        },
    )?;
    let shrunk = apply(
        &store,
        with_hole.payload,
        RegularMutation::Resize { logical_bytes: 64 },
    )?;
    let FilePayload::Regular { logical_bytes, .. } = shrunk.payload else {
        return Err("hole-bearing sparse file unexpectedly demoted".into());
    };
    assert_eq!(logical_bytes, 64);
    assert_eq!(shrunk.work.page_reads, 2);
    assert_eq!(shrunk.work.backend_read_operations, 2);
    Ok(())
}

#[test]
fn rejection_precedes_storage_work() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let failure = apply(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(b"a")?),
        RegularMutation::Write {
            offset: 0,
            length: 1,
            content: ObjectId {
                kind: ObjectKind::Metadata,
                digest: object_digest(ObjectKind::Metadata, b"wrong"),
            },
            content_offset: 0,
        },
    )
    .err()
    .ok_or("wrong content kind unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        RegularMutationError::WrongContentKind
    ));
    assert_eq!(*failure.work, WorkCounters::default());
    let clone_failure = clone_range(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(b"a")?),
        0,
        FilePayload::InlineRegular(InlineFileData::new(b"b")?),
        0,
        2,
    )
    .err()
    .ok_or("out-of-range clone unexpectedly succeeded")?;
    assert!(matches!(
        clone_failure.error,
        RegularMutationError::SourceRangeOutOfBounds
    ));
    assert_eq!(*clone_failure.work, WorkCounters::default());
    Ok(())
}

#[test]
fn dense_volume_rejects_sparse_semantics_before_work() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let content = blob(&store, b"XY")?;
    let mut dense = config();
    dense.sparse_files = false;
    let cancellation = CancellationToken::new();
    let inline = FilePayload::InlineRegular(InlineFileData::new(b"abcd")?);

    let replaced = poll_ready(apply_regular_mutation_async(
        &store,
        inline,
        RegularMutation::Write {
            offset: 1,
            length: 2,
            content,
            content_offset: 0,
        },
        dense,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("dense replacement blocked")??;
    let FilePayload::InlineRegular(replaced) = replaced.payload else {
        return Err("in-range dense write did not remain inline".into());
    };
    assert_eq!(replaced.as_bytes(), b"aXYd");

    for mutation in [
        RegularMutation::Write {
            offset: 5,
            length: 1,
            content,
            content_offset: 0,
        },
        RegularMutation::Resize { logical_bytes: 5 },
        RegularMutation::ZeroRange {
            offset: 0,
            length: 1,
            allocated: false,
            extend: false,
        },
        RegularMutation::Preallocate {
            offset: 0,
            length: 1,
            keep_size: true,
        },
    ] {
        let failure = poll_ready(apply_regular_mutation_async(
            &store,
            inline,
            mutation,
            dense,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("dense rejection blocked")?
        .err()
        .ok_or("sparse semantics unexpectedly succeeded in a dense volume")?;
        assert!(matches!(
            failure.error,
            RegularMutationError::SparseSemanticsDisabled
        ));
        assert_eq!(*failure.work, WorkCounters::default());
    }

    let clone_failure = poll_ready(apply_regular_clone_async(
        &store,
        inline,
        0,
        inline,
        0,
        1,
        dense,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("dense clone rejection blocked")?
    .err()
    .ok_or("clone unexpectedly succeeded in a dense volume")?;
    assert!(matches!(
        clone_failure.error,
        RegularMutationError::SparseSemanticsDisabled
    ));
    assert_eq!(*clone_failure.work, WorkCounters::default());
    Ok(())
}

#[test]
fn sparse_write_rewrites_only_the_target_and_preserves_the_trailing_hole()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let sparse = apply(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(&[b'a'; 64])?),
        RegularMutation::Resize { logical_bytes: 128 },
    )?;
    let replacement = blob(&store, b"WXYZWXYZ")?;
    let written = apply(
        &store,
        sparse.payload,
        RegularMutation::Write {
            offset: 60,
            length: 8,
            content: replacement,
            content_offset: 0,
        },
    )?;
    let FilePayload::Regular {
        logical_bytes,
        extents,
    } = written.payload
    else {
        return Err("sparse write unexpectedly demoted".into());
    };
    assert_eq!(logical_bytes, 128);
    assert!(written.work.backend_write_operations > 0);
    let encoded = ObjectStore::read(
        &store,
        extents,
        config().limits.maximum_object_bytes,
        WorkBudget::UNBOUNDED,
    )?
    .value;
    let ExtentPage::Leaf(values) = decode_extent_page(&encoded, decode_limits(config()))? else {
        return Err("sparse write root was not a leaf".into());
    };
    assert_eq!(values.len(), 3);
    assert_eq!((values[0].offset, values[0].length), (0, 60));
    assert!(matches!(values[0].kind, ExtentKind::Content { .. }));
    assert_eq!((values[1].offset, values[1].length), (60, 8));
    assert!(matches!(
        values[1].kind,
        ExtentKind::Content { object, object_offset: 0 } if object == replacement
    ));
    assert_eq!((values[2].offset, values[2].length), (68, 60));
    assert!(matches!(values[2].kind, ExtentKind::Hole));
    Ok(())
}

#[test]
fn sparse_clone_preserves_holes_and_destination_size_with_bounded_promotion_copies()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let source = apply(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(&[b's'; 64])?),
        RegularMutation::Resize { logical_bytes: 128 },
    )?;
    let destination = apply(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(&[b'd'; 64])?),
        RegularMutation::Resize { logical_bytes: 160 },
    )?;
    let cloned = clone_range(&store, source.payload, 0, destination.payload, 16, 128)?;
    let FilePayload::Regular {
        logical_bytes,
        extents,
    } = cloned.destination
    else {
        return Err("large sparse clone unexpectedly demoted".into());
    };
    assert_eq!(logical_bytes, 160);
    assert_eq!(cloned.work.bytes_copied, 256);
    assert!(cloned.work.backend_write_operations > 0);
    let encoded = ObjectStore::read(
        &store,
        extents,
        config().limits.maximum_object_bytes,
        WorkBudget::UNBOUNDED,
    )?
    .value;
    let ExtentPage::Leaf(values) = decode_extent_page(&encoded, decode_limits(config()))? else {
        return Err("sparse clone root was not a leaf".into());
    };
    assert_eq!(values.len(), 3);
    assert_eq!((values[0].offset, values[0].length), (0, 16));
    assert!(matches!(values[0].kind, ExtentKind::Content { .. }));
    assert_eq!((values[1].offset, values[1].length), (16, 64));
    assert!(matches!(values[1].kind, ExtentKind::Content { .. }));
    assert_eq!((values[2].offset, values[2].length), (80, 80));
    assert!(matches!(values[2].kind, ExtentKind::Hole));
    Ok(())
}

#[allow(clippy::too_many_lines)]
#[test]
fn malformed_ranges_nonregular_payloads_and_semantic_noops_are_exact()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let content = blob(&store, b"x")?;
    let inline = FilePayload::InlineRegular(InlineFileData::new(b"abcd")?);
    for mutation in [
        RegularMutation::Write {
            offset: 0,
            length: 0,
            content,
            content_offset: 0,
        },
        RegularMutation::Write {
            offset: u64::MAX,
            length: 1,
            content,
            content_offset: 0,
        },
        RegularMutation::Write {
            offset: 0,
            length: 1,
            content,
            content_offset: u64::MAX,
        },
        RegularMutation::ZeroRange {
            offset: 0,
            length: 0,
            allocated: false,
            extend: false,
        },
        RegularMutation::ZeroRange {
            offset: u64::MAX,
            length: 1,
            allocated: false,
            extend: false,
        },
        RegularMutation::Preallocate {
            offset: 0,
            length: 0,
            keep_size: true,
        },
        RegularMutation::Preallocate {
            offset: u64::MAX,
            length: 1,
            keep_size: false,
        },
    ] {
        let failure = poll_ready(apply_regular_mutation_async(
            &store,
            inline,
            mutation,
            config(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("malformed range blocked")?
        .err()
        .ok_or("malformed range unexpectedly succeeded")?;
        assert!(matches!(failure.error, RegularMutationError::RangeOverflow));
        assert_eq!(*failure.work, WorkCounters::default());
    }

    let nonregular = poll_ready(apply_regular_mutation_async(
        &store,
        FilePayload::Empty,
        RegularMutation::Resize { logical_bytes: 0 },
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("nonregular mutation blocked")?
    .err()
    .ok_or("nonregular mutation unexpectedly succeeded")?;
    assert!(matches!(nonregular.error, RegularMutationError::NotRegular));
    assert_eq!(*nonregular.work, WorkCounters::default());
    for (source, destination) in [(FilePayload::Empty, inline), (inline, FilePayload::Empty)] {
        let failure = poll_ready(apply_regular_clone_async(
            &store,
            source,
            0,
            destination,
            0,
            1,
            config(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("nonregular clone blocked")?
        .err()
        .ok_or("nonregular clone unexpectedly succeeded")?;
        assert!(matches!(failure.error, RegularMutationError::NotRegular));
        assert_eq!(*failure.work, WorkCounters::default());
    }
    for (source_offset, destination_offset, length) in
        [(u64::MAX, 0, 1), (0, u64::MAX, 1), (0, 0, 0)]
    {
        let failure = poll_ready(apply_regular_clone_async(
            &store,
            inline,
            source_offset,
            inline,
            destination_offset,
            length,
            config(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("invalid clone blocked")?
        .err()
        .ok_or("invalid clone unexpectedly succeeded")?;
        assert!(matches!(
            failure.error,
            RegularMutationError::RangeOverflow | RegularMutationError::SourceRangeOutOfBounds
        ));
        assert_eq!(*failure.work, WorkCounters::default());
    }

    let zero_noop = apply(
        &store,
        inline,
        RegularMutation::ZeroRange {
            offset: 4,
            length: 2,
            allocated: true,
            extend: false,
        },
    )?;
    assert_eq!(zero_noop.payload, inline);
    assert_eq!(zero_noop.work, WorkCounters::default());
    let preallocate_noop = apply(
        &store,
        inline,
        RegularMutation::Preallocate {
            offset: 1,
            length: 2,
            keep_size: true,
        },
    )?;
    assert_eq!(preallocate_noop.payload, inline);
    assert_eq!(preallocate_noop.work, WorkCounters::default());

    let promoted = apply(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(&[b'p'; 64])?),
        RegularMutation::Resize { logical_bytes: 128 },
    )?;
    let contained = apply(
        &store,
        promoted.payload,
        RegularMutation::Preallocate {
            offset: 0,
            length: 32,
            keep_size: true,
        },
    )?;
    assert_eq!(contained.payload, promoted.payload);
    assert_eq!(contained.work.backend_write_operations, 0);
    let empty = apply(
        &store,
        promoted.payload,
        RegularMutation::Resize { logical_bytes: 0 },
    )?;
    let FilePayload::InlineRegular(data) = empty.payload else {
        return Err("zero-length sparse resize did not demote".into());
    };
    assert!(data.as_bytes().is_empty());
    Ok(())
}

#[test]
fn inline_preallocation_handles_keep_size_and_growth() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let inline = FilePayload::InlineRegular(InlineFileData::new(b"abcd")?);
    let failure = apply(
        &store,
        inline,
        RegularMutation::Preallocate {
            offset: 4,
            length: 1,
            keep_size: true,
        },
    )
    .err()
    .ok_or("keep-size preallocation beyond EOF unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        RegularMutationError::KeepSizeBeyondEofUnsupported
    ));
    assert_eq!(*failure.work, WorkCounters::default());

    let grown = apply(
        &store,
        inline,
        RegularMutation::Preallocate {
            offset: 4,
            length: 2,
            keep_size: false,
        },
    )?;
    let FilePayload::Regular {
        logical_bytes,
        extents,
    } = grown.payload
    else {
        return Err("inline extending preallocation did not promote".into());
    };
    assert_eq!(logical_bytes, 6);
    let encoded = ObjectStore::read(
        &store,
        extents,
        config().limits.maximum_object_bytes,
        WorkBudget::UNBOUNDED,
    )?
    .value;
    let ExtentPage::Leaf(values) = decode_extent_page(&encoded, decode_limits(config()))? else {
        return Err("preallocation root was not a leaf".into());
    };
    assert!(matches!(
        values.last().map(|extent| extent.kind.clone()),
        Some(ExtentKind::AllocatedZero)
    ));
    assert_eq!(
        values.last().map(|extent| (extent.offset, extent.length)),
        Some((4, 2))
    );
    Ok(())
}

#[test]
fn preallocation_rejects_fragmented_ranges_at_the_configured_limit()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let sparse = apply(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(&[b'a'; 64])?),
        RegularMutation::Resize { logical_bytes: 256 },
    )?;
    let content = blob(&store, b"x")?;
    let fragmented = apply(
        &store,
        sparse.payload,
        RegularMutation::Write {
            offset: 128,
            length: 1,
            content,
            content_offset: 0,
        },
    )?;
    let mut constrained = config();
    constrained.limits.maximum_mutations_per_batch = 1;
    let failure = poll_ready(apply_regular_mutation_async(
        &store,
        fragmented.payload,
        RegularMutation::Preallocate {
            offset: 0,
            length: 256,
            keep_size: false,
        },
        constrained,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .ok_or("bounded preallocation blocked")?
    .err()
    .ok_or("over-limit preallocation unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        RegularMutationError::Range(ExtentReadError::TooManySpans)
    ));
    assert_eq!(failure.work.backend_write_operations, 0);
    Ok(())
}

#[test]
fn preallocation_allocates_multiple_holes_and_a_nonadjacent_extension_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let sparse = apply(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(&[b'a'; 64])?),
        RegularMutation::Resize { logical_bytes: 129 },
    )?;
    let tail = blob(&store, b"z")?;
    let fragmented = apply(
        &store,
        sparse.payload,
        RegularMutation::Write {
            offset: 128,
            length: 1,
            content: tail,
            content_offset: 0,
        },
    )?;
    let allocated = apply(
        &store,
        fragmented.payload,
        RegularMutation::Preallocate {
            offset: 64,
            length: 128,
            keep_size: false,
        },
    )?;
    let FilePayload::Regular {
        logical_bytes,
        extents,
    } = allocated.payload
    else {
        return Err("multi-region preallocation unexpectedly demoted".into());
    };
    assert_eq!(logical_bytes, 192);
    let encoded = ObjectStore::read(
        &store,
        extents,
        config().limits.maximum_object_bytes,
        WorkBudget::UNBOUNDED,
    )?
    .value;
    let ExtentPage::Leaf(values) = decode_extent_page(&encoded, decode_limits(config()))? else {
        return Err("multi-region preallocation root was not a leaf".into());
    };
    assert_eq!(values.len(), 4);
    assert_eq!(
        values
            .iter()
            .map(|extent| (extent.offset, extent.length))
            .collect::<Vec<_>>(),
        vec![(0, 64), (64, 64), (128, 1), (129, 63)]
    );
    assert!(matches!(values[0].kind, ExtentKind::Content { .. }));
    assert!(matches!(values[1].kind, ExtentKind::AllocatedZero));
    assert!(matches!(values[2].kind, ExtentKind::Content { .. }));
    assert!(matches!(values[3].kind, ExtentKind::AllocatedZero));
    assert!(allocated.work.backend_write_operations > 0);
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn generated_regular_file_histories_match_independent_logical_and_physical_model()
-> Result<(), Box<dyn std::error::Error>> {
    for history in 0..192_u64 {
        let store = MemoryObjectStore::default();
        let mut seed = history + 1;
        let initial_len = usize::try_from(next_generated(&mut seed) % 65)?;
        let initial = (0..initial_len)
            .map(|_| u8::try_from(next_generated(&mut seed) & 0xff).unwrap_or(0))
            .collect::<Vec<_>>();
        let mut model = initial
            .iter()
            .copied()
            .map(ModeledByte::content)
            .collect::<Vec<_>>();
        let mut payload = FilePayload::InlineRegular(InlineFileData::new(&initial)?);
        assert_payload_matches_model(&store, payload, &model)?;

        for step in 0..32_u64 {
            match next_generated(&mut seed) % 7 {
                0 => {
                    let logical_bytes = usize::try_from(next_generated(&mut seed) % 129)?;
                    payload = apply(
                        &store,
                        payload,
                        RegularMutation::Resize {
                            logical_bytes: u64::try_from(logical_bytes)?,
                        },
                    )
                    .map_err(|error| format!("history {history}, step {step}, resize: {error:?}"))?
                    .payload;
                    model.resize(logical_bytes, ModeledByte::HOLE);
                }
                1 => {
                    let offset = usize::try_from(next_generated(&mut seed) % 129)?;
                    let length = usize::try_from(next_generated(&mut seed) % 12 + 1)?;
                    let bytes = (0..length)
                        .map(|_| u8::try_from(next_generated(&mut seed) & 0xff).unwrap_or(0))
                        .collect::<Vec<_>>();
                    let content = blob(&store, &bytes)?;
                    payload = apply(
                        &store,
                        payload,
                        RegularMutation::Write {
                            offset: u64::try_from(offset)?,
                            length: u64::try_from(length)?,
                            content,
                            content_offset: 0,
                        },
                    )
                    .map_err(|error| format!("history {history}, step {step}, write: {error:?}"))?
                    .payload;
                    model_replace(&mut model, offset, &bytes);
                }
                2 | 3 => {
                    let offset = usize::try_from(next_generated(&mut seed) % 129)?;
                    let length = usize::try_from(next_generated(&mut seed) % 16 + 1)?;
                    let allocated = step.is_multiple_of(2);
                    let extend = next_generated(&mut seed).is_multiple_of(2);
                    let old_length = model.len();
                    let requested_end = offset + length;
                    let allocated_is_inline_content = allocated
                        && matches!(payload, FilePayload::InlineRegular(_))
                        && (!extend || offset <= old_length)
                        && (!extend || requested_end.max(old_length) <= MAXIMUM_INLINE_FILE_BYTES);
                    payload = apply(
                        &store,
                        payload,
                        RegularMutation::ZeroRange {
                            offset: u64::try_from(offset)?,
                            length: u64::try_from(length)?,
                            allocated,
                            extend,
                        },
                    )
                    .map_err(|error| format!("history {history}, step {step}, zero: {error:?}"))?
                    .payload;
                    model_zero(
                        &mut model,
                        offset,
                        length,
                        allocated,
                        extend,
                        allocated_is_inline_content,
                    );
                }
                4 | 5 => {
                    let offset = usize::try_from(next_generated(&mut seed) % 129)?;
                    let length = usize::try_from(next_generated(&mut seed) % 16 + 1)?;
                    let keep_size = step.is_multiple_of(2);
                    let before = payload;
                    let outcome = apply(
                        &store,
                        payload,
                        RegularMutation::Preallocate {
                            offset: u64::try_from(offset)?,
                            length: u64::try_from(length)?,
                            keep_size,
                        },
                    );
                    if model_preallocate(&mut model, offset, length, keep_size) {
                        payload = outcome?.payload;
                    } else {
                        let failure = outcome
                            .err()
                            .ok_or("keep-size preallocation beyond EOF succeeded")?;
                        assert!(matches!(
                            failure.error,
                            RegularMutationError::KeepSizeBeyondEofUnsupported
                        ));
                        assert_eq!(failure.work.backend_write_operations, 0);
                        assert_eq!(payload, before);
                    }
                }
                _ if !model.is_empty() => {
                    let source_offset =
                        usize::try_from(next_generated(&mut seed) % u64::try_from(model.len())?)?;
                    let maximum_length = model.len() - source_offset;
                    let length = usize::try_from(
                        next_generated(&mut seed) % u64::try_from(maximum_length)? + 1,
                    )?;
                    let destination_offset = usize::try_from(next_generated(&mut seed) % 129)?;
                    let source = model[source_offset..source_offset + length].to_vec();
                    payload = clone_range(
                        &store,
                        payload,
                        u64::try_from(source_offset)?,
                        payload,
                        u64::try_from(destination_offset)?,
                        u64::try_from(length)?,
                    )
                    .map_err(|error| format!("history {history}, step {step}, clone: {error:?}"))?
                    .destination;
                    model.resize(
                        model.len().max(destination_offset + length),
                        ModeledByte::HOLE,
                    );
                    model[destination_offset..destination_offset + length].copy_from_slice(&source);
                }
                _ => {}
            }
            assert_payload_matches_model(&store, payload, &model)
                .map_err(|error| format!("history {history}, step {step}: {error}"))?;
        }
    }
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn regular_lower_boundaries_preserve_errors_work_and_allocation_classes()
-> Result<(), Box<dyn std::error::Error>> {
    let prior = WorkCounters {
        bytes_copied: 2,
        peak_allocation_bytes: 3,
        ..WorkCounters::default()
    };
    let nested = WorkCounters {
        bytes_hashed: 4,
        peak_allocation_bytes: 5,
        ..WorkCounters::default()
    };
    let merged = simultaneous(prior, nested, 7, WorkBudget::UNBOUNDED)?;
    assert_eq!(merged.bytes_copied, 2);
    assert_eq!(merged.bytes_hashed, 4);
    assert_eq!(merged.peak_allocation_bytes, 12);
    let normal = simultaneous_failure(prior, nested, 7, RegularMutationError::NotRegular);
    assert!(matches!(normal.error, RegularMutationError::NotRegular));
    assert_eq!(normal.work.peak_allocation_bytes, 12);
    let peak_overflow = simultaneous_failure(
        prior,
        WorkCounters {
            peak_allocation_bytes: u64::MAX,
            ..WorkCounters::default()
        },
        1,
        RegularMutationError::NotRegular,
    );
    assert!(matches!(
        peak_overflow.error,
        RegularMutationError::RangeOverflow
    ));
    let work_overflow = simultaneous_failure(
        WorkCounters {
            bytes_hashed: u64::MAX,
            ..WorkCounters::default()
        },
        WorkCounters {
            bytes_hashed: 1,
            ..WorkCounters::default()
        },
        0,
        RegularMutationError::NotRegular,
    );
    assert!(matches!(
        work_overflow.error,
        RegularMutationError::Work(WorkError::Overflow)
    ));

    let work = WorkCounters {
        object_probes: 1,
        ..WorkCounters::default()
    };
    assert!(matches!(
        allocation_error(AllocationError::Work(WorkError::Overflow), work).error,
        RegularMutationError::Work(WorkError::Overflow)
    ));
    for allocation in [AllocationError::Overflow, AllocationError::ReleaseInvariant] {
        assert!(matches!(
            allocation_error(allocation, work).error,
            RegularMutationError::RangeOverflow
        ));
    }
    assert_eq!(
        regular_logical_bytes(FilePayload::Regular {
            logical_bytes: 17,
            extents: ObjectId {
                kind: ObjectKind::ExtentPage,
                digest: Digest::ZERO,
            },
        })?,
        17
    );
    for allocation in [
        AllocationError::InvalidCapacity,
        AllocationError::CapacityExceeded,
        AllocationError::AllocationFailed,
    ] {
        assert!(matches!(
            allocation_error(allocation, work).error,
            RegularMutationError::AllocationFailed
        ));
    }
    assert!(matches!(
        cancelled(),
        RegularMutationError::Storage(ObjectStoreError::Cancelled)
    ));

    let store = MemoryObjectStore::default();
    let page = ExtentPage::Leaf(Vec::new());
    let too_small = DecodeLimits {
        maximum_object_bytes: 1,
        ..DecodeLimits::default()
    };
    let rejected = poll_ready(put_extent_page_async(
        &store,
        &page,
        too_small,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .ok_or("tiny-limit extent page write blocked")?
    .err()
    .ok_or("tiny-limit extent page write succeeded")?;
    assert!(matches!(
        rejected.error,
        RegularMutationError::Codec(CanonicalDecodeError::ObjectTooLarge { maximum: 1, .. })
    ));
    assert_eq!(*rejected.work, WorkCounters::default());

    let source = FilePayload::InlineRegular(InlineFileData::new(&[b's'; 64])?);
    let destination_failure = poll_ready(apply_regular_clone_async(
        &store,
        source,
        0,
        FilePayload::Empty,
        0,
        64,
        config(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .ok_or("post-promotion destination rejection blocked")?
    .err()
    .ok_or("non-regular destination was accepted after source promotion")?;
    assert!(matches!(
        destination_failure.error,
        RegularMutationError::NotRegular
    ));
    assert_eq!(*destination_failure.work, WorkCounters::default());
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn inline_and_validation_helpers_reject_every_malformed_shape_before_storage()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let data = InlineFileData::new(b"abcd")?;
    for payload in [
        FilePayload::Directory {
            entries: ObjectId {
                kind: ObjectKind::TreePage,
                digest: Digest::ZERO,
            },
        },
        FilePayload::SymbolicLink {
            target: ObjectId {
                kind: ObjectKind::Blob,
                digest: Digest::ZERO,
            },
            target_bytes: 0,
        },
        FilePayload::Empty,
        FilePayload::Device { major: 1, minor: 2 },
        FilePayload::ReparsePoint {
            payload_bytes: 0,
            payload: ObjectId {
                kind: ObjectKind::Blob,
                digest: Digest::ZERO,
            },
        },
    ] {
        assert!(matches!(
            regular_logical_bytes(payload).map_err(|failure| failure.error),
            Err(RegularMutationError::NotRegular)
        ));
    }

    assert!(matches!(
        validate_mutation(RegularMutation::Write {
            offset: 0,
            length: 1,
            content: ObjectId {
                kind: ObjectKind::ExtentPage,
                digest: Digest::ZERO,
            },
            content_offset: 0,
        })
        .map_err(|failure| failure.error),
        Err(RegularMutationError::WrongContentKind)
    ));
    for invalid in [
        RegularMutation::Write {
            offset: u64::MAX,
            length: 1,
            content: ObjectId {
                kind: ObjectKind::Blob,
                digest: Digest::ZERO,
            },
            content_offset: 0,
        },
        RegularMutation::ZeroRange {
            offset: u64::MAX,
            length: 1,
            allocated: false,
            extend: true,
        },
        RegularMutation::Preallocate {
            offset: 0,
            length: 0,
            keep_size: false,
        },
    ] {
        assert!(matches!(
            validate_mutation(invalid).map_err(|failure| failure.error),
            Err(RegularMutationError::RangeOverflow)
        ));
    }

    let overflow = poll_ready(try_inline(
        &store,
        data,
        RegularMutation::ZeroRange {
            offset: u64::MAX,
            length: 1,
            allocated: true,
            extend: true,
        },
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("inline overflow guard unexpectedly suspended")?
    .err()
    .ok_or("inline overflowing range unexpectedly succeeded")?;
    assert!(matches!(
        overflow.error,
        RegularMutationError::RangeOverflow
    ));

    let keep_size = poll_ready(try_inline(
        &store,
        data,
        RegularMutation::Preallocate {
            offset: 4,
            length: 1,
            keep_size: true,
        },
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("inline keep-size guard unexpectedly suspended")?
    .err()
    .ok_or("inline keep-size growth unexpectedly succeeded")?;
    assert!(matches!(
        keep_size.error,
        RegularMutationError::KeepSizeBeyondEofUnsupported
    ));

    for mutation in [
        RegularMutation::ZeroRange {
            offset: 8,
            length: 1,
            allocated: true,
            extend: false,
        },
        RegularMutation::ZeroRange {
            offset: 0,
            length: 1,
            allocated: false,
            extend: true,
        },
        RegularMutation::ZeroRange {
            offset: 8,
            length: 1,
            allocated: true,
            extend: true,
        },
        RegularMutation::Preallocate {
            offset: 0,
            length: 1,
            keep_size: false,
        },
        RegularMutation::Write {
            offset: 8,
            length: 1,
            content: ObjectId {
                kind: ObjectKind::Blob,
                digest: Digest::ZERO,
            },
            content_offset: 0,
        },
    ] {
        let result = poll_ready(try_inline(
            &store,
            data,
            mutation,
            config(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("inline decision unexpectedly suspended")??;
        if let Some(receipt) = result {
            assert_eq!(receipt.work, WorkCounters::default());
        }
    }
    Ok(())
}
