use super::*;
use crate::async_storage::AsyncObjectStore;
use crate::foundation::{Digest, FileId};
use crate::kernel::{
    BlobBuildOptions, FileKind, FileRangeRequest, FileRecord, build_blob, decode_extent_page,
    read_file_range_async,
};
use crate::memory::MemoryObjectStore;
use crate::model::{
    CaseSensitivity, ConcurrencyMode, FilesystemProfile, Lifecycle, UnicodePolicy, VolumeLimits,
};
use crate::storage::ByteRange;
use crate::storage::object_digest;
use std::io::Cursor;

fn config() -> VolumeConfig {
    VolumeConfig::portable(Lifecycle::Ephemeral)
}

async fn blob(
    store: &MemoryObjectStore,
    bytes: &[u8],
) -> Result<ObjectId, Box<dyn std::error::Error>> {
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
        &CancellationToken::new(),
    )
    .await?
    .root)
}

async fn apply(
    store: &MemoryObjectStore,
    payload: FilePayload,
    mutation: RegularMutation,
) -> Result<RegularMutationReceipt, RegularMutationFailure> {
    (apply_regular_mutation_async(
        store,
        payload,
        mutation,
        config(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .await
}

async fn clone_range(
    store: &MemoryObjectStore,
    source: FilePayload,
    source_offset: u64,
    destination: FilePayload,
    destination_offset: u64,
    length: u64,
) -> Result<RegularCloneReceipt, RegularMutationFailure> {
    let cancellation = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    apply_regular_clone_with_accounting(
        store,
        source,
        source_offset,
        destination,
        destination_offset,
        length,
        config(),
        &mut accounting,
        &cancellation,
    )
    .await
    .map_err(|error| OperationFailure::new(project_regular_error(error), accounting.spent()))
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

async fn flatten_extents(
    store: &MemoryObjectStore,
    root: ObjectId,
) -> Result<Vec<Extent>, Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let mut work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let encoded = store.read(root, u64::MAX, &mut work).await?;
    Ok(
        match decode_extent_page(&encoded, DecodeLimits::for_volume(config()), &mut |_| {
            Ok::<_, crate::kernel::CanonicalDecodeError>(())
        })? {
            ExtentPage::Leaf(extents) => extents,
            ExtentPage::Internal(children) => {
                let mut extents = Vec::new();
                for child in children {
                    extents.extend(Box::pin(flatten_extents(store, child.page)).await?);
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

async fn payload_kinds(
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
            for extent in flatten_extents(store, extents).await? {
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

async fn payload_bytes(
    store: &MemoryObjectStore,
    payload: FilePayload,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let logical_bytes = payload_size(payload)?;
    let read = (read_file_range_async(
        store,
        FileRangeRequest {
            record: FileRecord {
                file_id: FileId::from_bytes([247; 16]),
                kind: FileKind::Regular,
                link_count: 1,
                metadata: ObjectId {
                    storage: None,
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
            limits: DecodeLimits::for_volume(config()),
            budget: WorkBudget::UNBOUNDED,
        },
        &CancellationToken::new(),
    ))
    .await?;
    Ok(read.bytes.to_vec())
}

async fn assert_payload_matches_model(
    store: &MemoryObjectStore,
    payload: FilePayload,
    model: &[ModeledByte],
) -> Result<(), Box<dyn std::error::Error>> {
    let expected_kinds: Vec<_> = model.iter().map(|byte| byte.kind).collect();
    let actual_kinds = payload_kinds(store, payload).await?;
    if actual_kinds != expected_kinds {
        return Err(format!(
            "physical byte kinds diverged: actual={actual_kinds:?}, expected={expected_kinds:?}"
        )
        .into());
    }
    let actual_bytes = payload_bytes(store, payload).await?;
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
