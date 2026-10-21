//! Byte-input oracle shared by canonical encoder properties and cargo-fuzz.
use super::allocation::KernelAccounting;
use super::codec::{EncodeAccounting, EncodeAllocationStep, EncodeWorkStep};
use super::*;
use crate::{CancellationToken, FsError, WorkBudget, WorkCounters};

fn record_copies(record: &FileRecord) -> u64 {
    48 + match record.payload {
        FilePayload::Empty | FilePayload::Device { .. } => 0,
        FilePayload::InlineRegular(bytes) => bytes.as_bytes().len() as u64,
        FilePayload::Regular { .. } | FilePayload::Directory { .. } => 32,
        FilePayload::SymbolicLink { .. } | FilePayload::ReparsePoint { .. } => 32,
    }
}

fn produce(
    kind: u8,
    wire: &[u8],
    observe: &mut dyn FnMut(EncodeWorkStep) -> Result<(), FsError>,
    copies: &mut u64,
) -> Result<Vec<u8>, FsError> {
    let limits = DecodeLimits {
        maximum_object_bytes: 4096,
        maximum_page_bytes: 4096,
        maximum_page_items: 32,
        maximum_name_bytes: 256,
        ..DecodeLimits::default()
    };
    let mut nothing = |_| Ok::<_, FsError>(());
    let allocation = |_| CanonicalDecodeError::AllocationFailed.into();
    match kind {
        0 => {
            let value = decode_file_metadata(wire, limits, &mut nothing)?;
            *copies = [value.named_attributes, value.acl, value.security_descriptor]
                .into_iter()
                .filter(|field| matches!(field, MetadataField::Value(_)))
                .count() as u64
                * 32;
            encode_file_metadata(value, observe, allocation)
        }
        1 => {
            let value = decode_generation_root(wire, limits, &mut nothing)?;
            *copies = 64 + value.parents.len() as u64 * 32;
            encode_generation_root(&value, observe, allocation)
        }
        2 => {
            let value = decode_file_table_page(wire, limits, &mut nothing)?;
            *copies = match &value {
                FileTablePage::Leaf(records) => records.iter().map(record_copies).sum(),
                FileTablePage::Internal(children) => children.len() as u64 * 48,
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
                    .map(|child| child.first.as_bytes().len() as u64 + 32)
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
                    .map(|entry| entry.name.as_bytes().len() as u64 + 32)
                    .sum(),
                AttributePage::Internal(children) => children
                    .iter()
                    .map(|child| child.first.as_bytes().len() as u64 + 32)
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
            *copies = 48;
            super::volume::encode_volume_created(value, observe, allocation)
        }
        8 => {
            let value = decode_retention_created(wire, 4096)?;
            *copies = 48 + value.label.len() as u64;
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
            let result = if MUTANT == 1 && actual.is_some() {
                Ok(())
            } else {
                observer.observe(&step)
            };
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
#[cfg(not(acyclic_fs_loom))]
mod tests;
