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
        _ => (),
    }
    Ok(())
}

fn check(count: u8, fault: u8) -> Result<(), &'static str> {
    let records = (1..=count)
        .map(|key| FileRecord {
            file_id: FileId::from_bytes([key; 16]),
            kind: FileKind::Fifo,
            link_count: 1,
            metadata: digest_object(ObjectKind::Metadata, [key; 32]),
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
    let cancellation = crate::CancellationToken::new();
    let original =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let decode = |accounting: &mut KernelAccounting<'_>| {
        crate::kernel::decode_owned(accounting, |observe| {
            decode_file_table_page::<crate::FsError>(&bytes, DecodeLimits::default(), observe)
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
            bytes_copied: 7 + 58 * u64::from(count),
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
    for count in [0, 1, 8, 64] {
        check(count, 0).unwrap();
    }
}

#[test]
fn redundant_production_validation_and_allocation_break_the_same_bounds() {
    for count in [1, 8, 64] {
        assert_eq!(check(count, 1), Err("validation count"));
        assert_eq!(check(count, 2), Err("allocation bound"));
        assert_eq!(check(count, 3), Err("work bound"));
    }
}
