use super::*;
use crate::kernel::allocation::KernelAccounting;
use std::cell::Cell;

std::thread_local! {
    static PROBE: Cell<(bool, usize, u8)> = const { Cell::new((false, 0, 0)) };
}

pub(super) fn validation() {
    PROBE.with(|probe| {
        let (active, validations, fault) = probe.get();
        if active {
            probe.set((active, validations + 1, fault));
        }
    });
}

pub(super) fn after_decode<E>(
    records: &[FileRecord],
    observe: &mut impl FnMut(ReadAllocationStep) -> Result<(), E>,
) -> Result<(), E> {
    match PROBE.with(|probe| probe.get().2) {
        1 => {
            for record in records {
                record.validate().unwrap();
            }
        }
        2 => {
            std::hint::black_box(records.to_vec());
        }
        3 => {
            observe(ReadAllocationStep::BeforeItem)?;
            observe(ReadAllocationStep::Item)?;
        }
        4 => {
            observe(ReadAllocationStep::BeforeCopy(1))?;
            observe(ReadAllocationStep::Copied(1))?;
        }
        _ => (),
    }
    Ok(())
}

fn check(count: u8, bindings: bool, sealed: bool, fault: u8) -> Result<(), &'static str> {
    let object = |kind, key| ObjectId {
        kind,
        digest: crate::foundation::Digest::from_bytes([key; 32]),
        storage: sealed.then_some(crate::ObjectStorage {
            locator: crate::foundation::Digest::from_bytes([key; 32]),
            root_file_id: FileId::from_bytes([key; 16]),
            origin_generation: crate::foundation::Digest::from_bytes([key; 32]),
        }),
    };
    let records = (1..=count)
        .map(|key| FileRecord {
            bindings: bindings.then(|| object(ObjectKind::BindingPage, key)),
            file_id: FileId::from_bytes([key; 16]),
            kind: FileKind::Fifo,
            link_count: 1,
            metadata: object(ObjectKind::Metadata, key),
            payload: FilePayload::Empty,
        })
        .collect();
    let bytes = encode_file_table_page(
        &FileTablePage::Leaf(records),
        64,
        |_| Ok::<_, CanonicalDecodeError>(()),
        |_| CanonicalDecodeError::AllocationFailed,
    )
    .unwrap();
    // The parser compares the domain in place and copies each remaining wire
    // byte once, including optional binding and sealed-reference fields.
    let copied_bytes = u64::try_from(bytes.len() - DOMAIN.len()).unwrap();
    let cancellation = crate::CancellationToken::new();
    let original =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let decode = |accounting: &mut KernelAccounting<'_>| {
        crate::kernel::decode_owned(accounting, |observe| {
            decode_file_table_page::<crate::FsError>(&bytes, DecodeLimits::default(), &mut |step| {
                observe(step)
            })
        })
        .unwrap()
    };
    // Warm thread-local rank checking before measuring the actual parser,
    // original accounting observer, output Vec and funded owner together.
    drop(decode(&mut accounting));
    let before = original.receipt();
    let live = original.live_allocation_bytes();
    struct Restore((bool, usize, u8));
    impl Drop for Restore {
        fn drop(&mut self) {
            PROBE.with(|probe| probe.set(self.0));
        }
    }
    let _restore = Restore(PROBE.with(|probe| probe.replace((true, 0, fault))));
    let mut decoded = None;
    let heap = acyclic_test_support::measure(|| decoded = Some(decode(&mut accounting)));
    let validations = PROBE.with(|probe| probe.get().1);
    let bytes = (usize::from(count) * std::mem::size_of::<FileRecord>()) as u64;
    if validations != usize::from(count) {
        return Err("validation count");
    }
    let operations = u64::from(count != 0);
    let expected_heap = acyclic_test_support::AllocationInfo {
        count_total: operations,
        count_current: operations as i64,
        count_max: operations,
        bytes_total: bytes,
        bytes_current: bytes as i64,
        bytes_max: bytes,
    };
    if heap != expected_heap {
        return Err("allocation bound");
    }
    let expected = before
        .checked_add(WorkCounters {
            items_examined: u64::from(count),
            bytes_copied: copied_bytes,
            allocation_operations: operations,
            peak_allocation_bytes: live + bytes,
            ..WorkCounters::UNCHARGED
        })
        .unwrap();
    if original.receipt() != expected {
        return Err("work bound");
    }
    if original.live_allocation_bytes() != live + bytes {
        return Err("live allocation");
    }
    drop(decoded);
    if original.live_allocation_bytes() != live {
        return Err("retirement");
    }
    Ok(())
}

#[test]
fn production_decode_reaches_exact_validation_allocation_and_work_bounds() {
    for bindings in [false, true] {
        for sealed in [false, true] {
            for count in [0, 1, 8, 64] {
                check(count, bindings, sealed, 0).unwrap();
            }
        }
    }
}

#[test]
fn redundant_production_validation_and_allocation_break_the_same_bounds() {
    for bindings in [false, true] {
        for sealed in [false, true] {
            for count in [1, 8, 64] {
                assert_eq!(check(count, bindings, sealed, 1), Err("validation count"));
                assert_eq!(check(count, bindings, sealed, 2), Err("allocation bound"));
                assert_eq!(check(count, bindings, sealed, 3), Err("work bound"));
                assert_eq!(check(count, bindings, sealed, 4), Err("work bound"));
            }
        }
    }
}
