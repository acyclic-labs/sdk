//! Byte-input oracle shared by canonical encoder properties and cargo-fuzz.
use super::allocation::KernelAccounting;
use super::codec::{EncodeAccounting, EncodeAllocationStep, EncodeWorkStep};
use super::*;
use crate::{CancellationToken, FsError, WorkBudget, WorkCounters};

// Independent wire-schema oracle; do not call the writer's length helpers.
fn reference_copies(object: crate::ObjectId) -> u64 {
    1 + 32
        + if object.storage.is_some() {
            32 + 16 + 32
        } else {
            0
        }
}

fn record_copies(record: &FileRecord) -> u64 {
    16 + reference_copies(record.metadata)
        + record.bindings.map_or(0, reference_copies)
        + match record.payload {
            FilePayload::Empty | FilePayload::Device { .. } => 0,
            FilePayload::InlineRegular(bytes) => bytes.as_bytes().len() as u64,
            FilePayload::Regular {
                extents: object, ..
            }
            | FilePayload::Directory { entries: object }
            | FilePayload::SymbolicLink { target: object, .. }
            | FilePayload::ReparsePoint {
                payload: object, ..
            } => reference_copies(object),
        }
}

fn check_file_table_write(
    expected: Option<FileTableError>,
    examined: u64,
    encode: impl FnOnce(
        &mut dyn FnMut(EncodeWorkStep) -> Result<(), FsError>,
    ) -> Result<Vec<u8>, FsError>,
) -> Option<Vec<u8>> {
    let mut completed = WorkCounters::UNCHARGED;
    let mut allocated = false;
    let result = encode(&mut |step| {
        match step {
            EncodeWorkStep::Completed(delta) => {
                completed.items_examined += delta.items_examined;
                completed.bytes_encoded += delta.bytes_encoded;
                completed.bytes_copied += delta.bytes_copied;
            }
            EncodeWorkStep::Allocation(_) => allocated = true,
            _ => {}
        }
        Ok(())
    });
    let Some(expected) = expected else {
        assert!(
            result.is_ok(),
            "file-table writer refused an admitted record"
        );
        return result.ok();
    };
    let actual = match result {
        Err(FsError::Decode(CanonicalDecodeError::Invariant(
            super::codec::CanonicalInvariant::FileTable(error),
        ))) => Some(error),
        _ => None,
    };
    assert_eq!(actual, Some(expected), "file-table writer refusal differs");
    assert_eq!(
        completed,
        WorkCounters {
            items_examined: examined,
            ..WorkCounters::UNCHARGED
        },
        "file-table writer refusal completed effects"
    );
    assert!(!allocated, "file-table writer refusal allocated output");
    None
}

// Mutate admitted records, retaining their captured identities and references.
// Both production writer entry points must agree on admission and preserve values.
fn check_file_record_writer(record: FileRecord) {
    let check = |record: FileRecord, valid: bool| {
        let expected = (!valid).then_some(FileTableError::InvalidRecord);
        let bytes = check_file_table_write(expected, 1, |observe| {
            super::file_table::encode_file_record(record, observe, |_| {
                FsError::from(CanonicalDecodeError::AllocationFailed)
            })
        });
        if let Some(bytes) = bytes {
            let decoded = super::file_table::decode_file_record(&bytes);
            assert_eq!(decoded.as_ref().ok(), Some(&record));
        }
        let page = FileTablePage::Leaf(vec![record]);
        let bytes = check_file_table_write(expected, 1, |observe| {
            encode_file_table_page(&page, u32::MAX, observe, |_| {
                FsError::from(CanonicalDecodeError::AllocationFailed)
            })
        });
        if let Some(bytes) = bytes {
            let decoded = decode_file_table_page(&bytes, DecodeLimits::default(), &mut |_| {
                Ok::<_, FsError>(())
            });
            assert_eq!(decoded.as_ref().ok(), Some(&page));
        }
    };
    let wrong_reference = crate::ObjectId {
        kind: crate::ObjectKind::Blob,
        ..record.metadata
    };
    for invalid in [
        FileRecord {
            link_count: 0,
            ..record
        },
        FileRecord {
            metadata: wrong_reference,
            ..record
        },
        FileRecord {
            bindings: Some(wrong_reference),
            ..record
        },
    ] {
        check(invalid, false);
    }
    if let Some(payload) = wrong_payload(record.payload) {
        check(FileRecord { payload, ..record }, false);
    }
    // Enumerate the complete persisted kind byte, rather than a second kind list.
    for kind in (0..=u8::MAX).filter_map(|tag| FileKind::from_tag(tag).ok()) {
        let valid = match record.kind {
            FileKind::Regular
            | FileKind::Directory
            | FileKind::SymbolicLink
            | FileKind::ReparsePoint => kind == record.kind,
            FileKind::CharacterDevice | FileKind::BlockDevice => {
                matches!(kind, FileKind::CharacterDevice | FileKind::BlockDevice)
            }
            FileKind::Fifo | FileKind::Socket | FileKind::MountBoundary => matches!(
                kind,
                FileKind::Fifo | FileKind::Socket | FileKind::MountBoundary
            ),
        };
        check(FileRecord { kind, ..record }, valid);
    }
}

fn wrong_payload(payload: FilePayload) -> Option<FilePayload> {
    match payload {
        FilePayload::Regular {
            logical_bytes,
            mut extents,
        } => {
            extents.kind = crate::ObjectKind::Blob;
            Some(FilePayload::Regular {
                logical_bytes,
                extents,
            })
        }
        FilePayload::Directory { mut entries } => {
            entries.kind = crate::ObjectKind::Blob;
            Some(FilePayload::Directory { entries })
        }
        FilePayload::SymbolicLink {
            target_bytes,
            mut target,
        } => {
            target.kind = crate::ObjectKind::TreePage;
            Some(FilePayload::SymbolicLink {
                target_bytes,
                target,
            })
        }
        FilePayload::ReparsePoint {
            payload_bytes,
            mut payload,
        } => {
            payload.kind = crate::ObjectKind::TreePage;
            Some(FilePayload::ReparsePoint {
                payload_bytes,
                payload,
            })
        }
        FilePayload::InlineRegular(_) | FilePayload::Device { .. } | FilePayload::Empty => None,
    }
}

// Derive invalid typed inputs from admitted writer bytes. Strict decoding alone
// cannot exercise the writer's own admission guards.
fn check_file_table_writer(page: &FileTablePage) {
    let descending = |id: crate::FileId| {
        let mut other = id.into_bytes();
        other[0] ^= 1;
        let other = crate::FileId::from_bytes(other);
        [id.max(other), id.min(other)]
    };
    let refuse = |page: &FileTablePage, maximum, expected, examined| {
        check_file_table_write(Some(expected), examined, |observe| {
            encode_file_table_page(page, maximum, observe, |_| {
                FsError::from(CanonicalDecodeError::AllocationFailed)
            })
        });
    };
    match page {
        FileTablePage::Leaf(records) => {
            if let Some(first) = records.first() {
                refuse(
                    page,
                    u32::try_from(records.len() - 1).unwrap_or(u32::MAX),
                    FileTableError::TooManyItems,
                    0,
                );
                let mut duplicate = vec![*first; 2];
                refuse(
                    &FileTablePage::Leaf(duplicate.clone()),
                    u32::MAX,
                    FileTableError::NotStrictlyOrdered,
                    2,
                );
                [duplicate[0].file_id, duplicate[1].file_id] = descending(first.file_id);
                refuse(
                    &FileTablePage::Leaf(duplicate),
                    u32::MAX,
                    FileTableError::NotStrictlyOrdered,
                    2,
                );
                check_file_record_writer(*first);
            }
        }
        FileTablePage::Internal(children) => {
            refuse(
                &FileTablePage::Internal(Vec::new()),
                u32::MAX,
                FileTableError::InvalidInternalPage,
                0,
            );
            if let Some(first) = children.first() {
                refuse(
                    page,
                    u32::try_from(children.len() - 1).unwrap_or(u32::MAX),
                    FileTableError::TooManyItems,
                    0,
                );
                let mut duplicate = vec![first.clone(); 2];
                refuse(
                    &FileTablePage::Internal(duplicate.clone()),
                    u32::MAX,
                    FileTableError::InvalidInternalPage,
                    2,
                );
                [duplicate[0].first, duplicate[1].first] = descending(first.first);
                refuse(
                    &FileTablePage::Internal(duplicate),
                    u32::MAX,
                    FileTableError::InvalidInternalPage,
                    2,
                );
                let mut invalid = first.clone();
                invalid.page.kind = crate::ObjectKind::Blob;
                refuse(
                    &FileTablePage::Internal(vec![invalid]),
                    u32::MAX,
                    FileTableError::InvalidInternalPage,
                    1,
                );
            }
        }
    }
}

fn decode_limits() -> DecodeLimits {
    DecodeLimits {
        maximum_object_bytes: 4096,
        maximum_page_bytes: 4096,
        maximum_page_items: 32,
        maximum_name_bytes: 256,
        ..DecodeLimits::default()
    }
}

fn produce(
    kind: u8,
    wire: &[u8],
    observe: &mut dyn FnMut(EncodeWorkStep) -> Result<(), FsError>,
    copies: &mut u64,
) -> Result<Vec<u8>, FsError> {
    let limits = decode_limits();
    let mut nothing = |_| Ok::<_, FsError>(());
    let allocation = |_| CanonicalDecodeError::AllocationFailed.into();
    match kind {
        0 => {
            let value = decode_file_metadata(wire, limits, &mut nothing)?;
            *copies = [value.named_attributes, value.acl, value.security_descriptor]
                .into_iter()
                .map(|field| match field {
                    MetadataField::Value(object) => reference_copies(object),
                    MetadataField::Unavailable => 0,
                })
                .sum();
            encode_file_metadata(value, observe, allocation)
        }
        1 => {
            let value = decode_generation_root(wire, limits, &mut nothing)?;
            *copies = 32
                + reference_copies(value.file_table)
                + value
                    .parents
                    .iter()
                    .map(|parent| reference_copies(parent.object_id()))
                    .sum::<u64>();
            encode_generation_root(&value, observe, allocation)
        }
        2 => {
            let value = decode_file_table_page(wire, limits, &mut nothing)?;
            *copies = match &value {
                FileTablePage::Leaf(records) => records.iter().map(record_copies).sum(),
                FileTablePage::Internal(children) => children
                    .iter()
                    .map(|child| 16 + reference_copies(child.page))
                    .sum(),
            };
            encode_file_table_page(&value, 32, observe, allocation)
        }
        3 => {
            let value = decode_tree_page(wire, limits, &mut nothing)?;
            *copies = match &value {
                TreePage::Leaf(entries) => entries
                    .iter()
                    .map(|entry| entry.name.as_bytes().len() as u64 + 16)
                    .sum(),
                TreePage::Internal(children) => children
                    .iter()
                    .map(|child| child.first.as_bytes().len() as u64 + reference_copies(child.page))
                    .sum(),
            };
            encode_tree_page(&value, 32, observe, allocation)
        }
        4 => {
            let value = decode_extent_page(wire, limits, &mut nothing)?;
            encode_extent_page(&value, 32, observe, allocation)
        }
        5 => {
            let value = decode_attribute_page(wire, limits, &mut nothing)?;
            *copies = match &value {
                AttributePage::Leaf(entries) => entries
                    .iter()
                    .map(|entry| entry.name.as_bytes().len() as u64 + reference_copies(entry.value))
                    .sum(),
                AttributePage::Internal(children) => children
                    .iter()
                    .map(|child| child.first.as_bytes().len() as u64 + reference_copies(child.page))
                    .sum(),
            };
            encode_attribute_page(&value, 32, observe, allocation)
        }
        6 => {
            let value = decode_blob_page(wire, limits, &mut nothing)?;
            encode_blob_page(
                &value,
                32,
                |step| observe(super::blob::page_write_step(step)),
                allocation,
            )
        }
        7 => {
            let value = super::volume::decode_volume_created(wire, 4096, &mut nothing)?;
            *copies = 16 + reference_copies(value.initial_generation_root);
            super::volume::encode_volume_created(value, observe, allocation)
        }
        8 => {
            let value = decode_retention_created(wire, 4096)?;
            *copies = 16 + reference_copies(value.generation_root) + value.label.len() as u64;
            super::retention::encode_retention_created(&value, observe, allocation)
        }
        9 => {
            let value = super::retention::decode_workspace_deleted(wire, 4096)?;
            *copies = 16;
            super::retention::encode_workspace_deleted(value, observe, allocation)
        }
        _ => {
            let value = super::file_table::decode_file_record(wire)?;
            *copies = record_copies(&value);
            super::file_table::encode_file_record(value, observe, allocation)
        }
    }
}

fn completed(step: &EncodeWorkStep) -> Option<WorkCounters> {
    match step {
        EncodeWorkStep::Completed(delta) => Some(*delta),
        EncodeWorkStep::Allocation(EncodeAllocationStep::HeaderWritten { bytes }) => {
            Some(WorkCounters {
                bytes_encoded: *bytes,
                ..WorkCounters::UNCHARGED
            })
        }
        _ => None,
    }
}

// Independent scalar oracle: admits read the completed prefix; completion
// records actual effects before cancellation. No production ledger arithmetic.
fn reference(
    kind: u8,
    wire: &[u8],
    budget: WorkBudget,
    recheck: bool,
    fault: u8,
    at: usize,
) -> (
    Result<Vec<u8>, FsError>,
    WorkCounters,
    Option<(usize, bool)>,
    u64,
) {
    let mut work = WorkCounters::UNCHARGED;
    let (mut index, mut cancelled, mut failure, mut copies) = (0, false, None, 0);
    let result = produce(
        kind,
        wire,
        &mut |step| {
            let current = index;
            index += 1;
            let actual = completed(&step);
            if let Some(delta) = actual {
                assert_eq!(
                    delta,
                    WorkCounters {
                        bytes_encoded: delta.bytes_encoded,
                        bytes_copied: delta.bytes_copied,
                        items_examined: delta.items_examined,
                        page_writes: delta.page_writes,
                        ..WorkCounters::UNCHARGED
                    },
                    "unmodeled completed counter"
                );
                work.bytes_encoded += delta.bytes_encoded;
                work.bytes_copied += delta.bytes_copied;
                work.items_examined += if recheck { 0 } else { delta.items_examined };
                work.page_writes += delta.page_writes;
            }
            if current == at && fault & 1 != 0 && actual.is_none() {
                failure = Some((current, actual.is_some()));
                return Err(CanonicalDecodeError::Truncated.into());
            }
            if current == at && fault & 2 != 0 {
                cancelled = true;
            }
            if cancelled {
                failure = Some((current, actual.is_some()));
                return Err(crate::CancellationError.into());
            }
            let proposed = match step {
                EncodeWorkStep::Admit(delta) => delta,
                EncodeWorkStep::Allocation(EncodeAllocationStep::BeforeHeader { bytes }) => {
                    WorkCounters {
                        bytes_encoded: bytes,
                        ..WorkCounters::UNCHARGED
                    }
                }
                _ => WorkCounters::UNCHARGED,
            };
            for (spent, extra, limit) in [
                (
                    work.bytes_encoded,
                    proposed.bytes_encoded,
                    budget.bytes_encoded,
                ),
                (
                    work.bytes_copied,
                    proposed.bytes_copied,
                    budget.bytes_copied,
                ),
                (
                    work.items_examined,
                    if recheck { 0 } else { proposed.items_examined },
                    budget.items_examined,
                ),
                (work.page_writes, proposed.page_writes, budget.page_writes),
            ] {
                if spent + extra > limit {
                    failure = Some((current, actual.is_some()));
                    return Err(CanonicalDecodeError::Truncated.into());
                }
            }
            match step {
                EncodeWorkStep::Allocation(EncodeAllocationStep::Reserve { capacity }) => {
                    work.allocation_operations += u64::from(capacity != 0);
                    work.peak_allocation_bytes = work.peak_allocation_bytes.max(capacity as u64);
                }
                EncodeWorkStep::Allocation(EncodeAllocationStep::Capacity {
                    actual_bytes, ..
                }) => {
                    work.peak_allocation_bytes = work.peak_allocation_bytes.max(actual_bytes);
                }
                _ => {}
            }
            Ok(())
        },
        &mut copies,
    );
    (result, work, failure, copies)
}

/// Fuzz canonical bytes, four independent budgets, callback rejection and cancellation.
pub fn check(input: &[u8]) {
    check_inner::<0, true>(input);
}
fn check_inner<const MUTANT: u8, const ASSERT_PREFIX: bool>(input: &[u8]) {
    let Some(header) = input.get(..10) else {
        return;
    };
    let wire = &input[10..];
    if wire.len() > 4096 {
        return;
    }
    let (kind, mode, fault, at) = (
        header[0] % 11,
        header[6] % 5,
        header[7],
        usize::from(header[8]),
    );
    // Admission is independent of the accounting mode and fault prefix. Check
    // typed writer mutations once, outside the reference/observed replay pair.
    match kind {
        2 => {
            if let Ok(page) =
                decode_file_table_page(wire, decode_limits(), &mut |_| Ok::<_, FsError>(()))
            {
                check_file_table_writer(&page);
            }
        }
        10 => {
            if let Ok(record) = super::file_table::decode_file_record(wire) {
                check_file_record_writer(record);
            }
        }
        _ => {}
    }
    let recheck = cfg!(not(target_arch = "wasm32")) && mode >= 3;
    let budget = WorkBudget {
        bytes_encoded: u64::from(u16::from_le_bytes([header[1], header[2]])),
        bytes_copied: u64::from(u16::from_le_bytes([header[3], header[4]])),
        items_examined: u64::from(header[5]),
        page_writes: u64::from(header[9]),
        ..WorkBudget::UNBOUNDED
    };
    let (expected_result, mut expected, expected_failure, expected_copies) =
        reference(kind, wire, budget, recheck, fault, at);
    if expected_result.is_ok() {
        assert_eq!(
            expected.bytes_encoded,
            wire.len() as u64,
            "every encoded byte needs a completion"
        );
        assert_eq!(
            expected.bytes_copied, expected_copies,
            "canonical schema copy count"
        );
        assert_eq!(expected.page_writes, u64::from(kind == 6));
    }
    let cancellation = CancellationToken::new();
    #[cfg(not(target_arch = "wasm32"))]
    let root = (mode != 0).then(|| {
        crate::NativeMaterializationWork::new(
            if mode == 2 || mode == 4 {
                WorkBudget::UNBOUNDED
            } else {
                budget
            },
            &cancellation,
        )
        .unwrap()
    });
    #[cfg(not(target_arch = "wasm32"))]
    let context = root.as_ref().map(|root| {
        let work = if mode == 2 || mode == 4 {
            root.native_volume_work(budget).unwrap()
        } else {
            root.clone()
        };
        if recheck {
            work.recheck_admitted()
        } else {
            work
        }
    });
    let mut accounting = match mode {
        #[cfg(not(target_arch = "wasm32"))]
        1..=4 => KernelAccounting::native(context.as_ref().unwrap(), &cancellation),
        _ => KernelAccounting::standalone(budget, &cancellation),
    };
    let baseline = accounting.spent();
    #[cfg(not(target_arch = "wasm32"))]
    let live = context
        .as_ref()
        .map_or(0, |work| work.live_allocation_bytes());
    #[cfg(target_arch = "wasm32")]
    let live = 0;
    let mut observer = EncodeAccounting::new(&mut accounting);
    let (mut index, mut failure, mut copies) = (0, None, 0);
    let produced = produce(
        kind,
        wire,
        &mut |step| {
            let current = index;
            index += 1;
            let actual = completed(&step);
            if current == at && fault & 1 != 0 && actual.is_none() {
                failure = Some((current, actual.is_some()));
                return Err(CanonicalDecodeError::Truncated.into());
            }
            if current == at && fault & 2 != 0 {
                cancellation.cancel();
            }
            let result = observer.observe(&step);
            if result.is_err() {
                failure = Some((current, actual.is_some()));
            }
            result
        },
        &mut copies,
    );
    let result = observer.finish(produced);
    assert_eq!(
        result.is_ok(),
        expected_result.is_ok(),
        "independent encoding outcome kind={kind} mode={mode}"
    );
    if let Some((index, after)) = failure.as_mut() {
        if MUTANT == 2 {
            *index += 1;
        }
        if MUTANT == 3 {
            *after = !*after;
        }
    }
    if ASSERT_PREFIX {
        assert_eq!(
            failure, expected_failure,
            "independent encoding failure prefix"
        );
    }
    if let Ok((bytes, allocation)) = result {
        assert_eq!(bytes, wire, "canonical encoding round trip");
        drop(bytes);
        drop(allocation);
    }
    assert_eq!(
        baseline,
        WorkCounters {
            allocation_operations: baseline.allocation_operations,
            peak_allocation_bytes: baseline.peak_allocation_bytes,
            ..WorkCounters::UNCHARGED
        }
    );
    expected.allocation_operations += baseline.allocation_operations;
    expected.peak_allocation_bytes = baseline
        .peak_allocation_bytes
        .max(live + expected.peak_allocation_bytes);
    assert_eq!(
        accounting.spent(),
        expected,
        "exact independent encoding receipt"
    );
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(work) = context.as_ref() {
        let (remaining, restored, initial) =
            work.state(|state| (state.remaining(), state.budget, state.original_budget()));
        assert_eq!(
            restored, initial,
            "no encoding reservation may survive an exit"
        );
        let remaining = remaining.unwrap();
        assert_eq!(
            remaining.bytes_encoded,
            budget.bytes_encoded - expected.bytes_encoded
        );
        assert_eq!(
            remaining.bytes_copied,
            budget.bytes_copied - expected.bytes_copied
        );
        assert_eq!(
            remaining.items_examined,
            budget.items_examined - expected.items_examined
        );
        assert_eq!(
            remaining.page_writes,
            budget.page_writes - expected.page_writes
        );
        assert_eq!(work.live_allocation_bytes(), live);
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests;
