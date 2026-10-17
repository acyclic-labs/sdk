use super::*;
use crate::foundation::{Digest, FileId, GenerationId, VolumeId};
use crate::kernel::{GenerationRoot, decode_generation_root, encode_generation_root};
use crate::storage::{ObjectId, ObjectKind};
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    // Reserved but unperformed work is returned, including partial completion,
    // cancellation and all observer exit methods. Completed work stays charged.
    #[test]
    fn unused_credit_is_returned_without_losing_completed_work(
        reserved in 0_u64..64,
        completed in 0_u64..64,
        mode in 0_u8..3,
        exit in 0_u8..3,
        cancel in any::<bool>(),
    ) {
        let completed = completed.min(reserved);
        let cancellation = CancellationToken::new();
        let limit = WorkBudget { bytes_copied: reserved, ..WorkBudget::UNBOUNDED };
        let original = crate::NativeMaterializationWork::new(
            if mode == 0 { limit } else { WorkBudget::UNBOUNDED }, &cancellation,
        ).unwrap();
        let work = if mode == 0 { original.clone() } else {
            original.native_volume_work(limit).unwrap()
        };
        let recheck = work.recheck_admitted();
        let work = if mode == 2 { &recheck } else { &work };
        let mut accounting = KernelAccounting::native(work, &cancellation);
        let mut observed = OriginalReadAccounting::new(&mut accounting).unwrap();
        observed.observe(super::super::super::codec::ReadAllocationStep::BeforeCopy(reserved)).unwrap();
        if cancel { cancellation.cancel(); }
        let result = observed.observe(super::super::super::codec::ReadAllocationStep::Copied(completed));
        prop_assert_eq!(result.is_err(), cancel);
        match exit {
            0 => drop(observed),
            1 => drop(observed.into_allocation()),
            _ => drop(observed.into_parts()),
        }
        prop_assert_eq!(original.receipt().bytes_copied, completed);
        // Read remaining through the original work to include outstanding
        // root and resource reservations without creating another owner.
        let remaining = work.state(|state| state.remaining()).unwrap().bytes_copied;
        prop_assert_eq!(remaining, reserved - completed);
        if !cancel {
            work.charge(WorkCounters { bytes_copied: remaining, ..WorkCounters::UNCHARGED }).unwrap();
            prop_assert_eq!(original.receipt().bytes_copied, reserved);
        }
    }

    // Cloning admits the exact name once, before allocation; a denied clone
    // leaves both copy work and allocation operations unchanged.
    #[test]
    fn name_clone_uses_exactly_one_copy_allowance(
        length in 1_usize..65,
        mode in 0_u8..4,
        short in any::<bool>(),
    ) {
        for length in [8, length] {
            let cancellation = CancellationToken::new();
            let maximum = length as u64 - u64::from(short);
            let budget = WorkBudget { bytes_copied: maximum, ..WorkBudget::UNBOUNDED };
            let name = super::super::super::LogicalName::new(
                super::super::super::NameEncoding::Utf8, vec![b'x'; length], 64,
            ).unwrap();
            let original = crate::NativeMaterializationWork::new(budget, &cancellation).unwrap();
            let child = original.native_volume_work(budget).unwrap();
            let recheck = child.recheck_admitted();
            let mut accounting = match mode {
                0 => KernelAccounting::standalone(budget, &cancellation),
                1 => KernelAccounting::native(&original, &cancellation),
                2 => KernelAccounting::native(&child, &cancellation),
                _ => KernelAccounting::native(&recheck, &cancellation),
            };
            let before = accounting.spent();
            let result = name.clone_with_accounting(&mut accounting, &cancellation);
            prop_assert_eq!(result.is_err(), short);
            let receipt = accounting.spent();
            prop_assert_eq!(receipt.bytes_copied - before.bytes_copied, if short { 0 } else { length as u64 });
            if short {
                prop_assert_eq!(receipt.allocation_operations, before.allocation_operations);
            } else {
                let (cloned, allocation) = result.unwrap();
                prop_assert_eq!(cloned, name);
                drop(allocation);
            }
            prop_assert_eq!(accounting.remaining().unwrap().bytes_copied, if short { maximum } else { 0 });
        }
    }

    // The consolidated reservation preserves the old error priority: existing
    // original/resource failures, then caller cancellation, then new work.
    #[test]
    fn reservation_keeps_preflight_failure_priority(
        caller_cancelled in any::<bool>(),
        original_cancelled in any::<bool>(),
        poisoned in any::<bool>(),
        overrun in any::<bool>(),
        resource in any::<bool>(),
        amount in 0_u64..17,
    ) {
        let cancellation = CancellationToken::new();
        let caller = CancellationToken::new();
        let budget = WorkBudget { bytes_copied: 8, ..WorkBudget::UNBOUNDED };
        let original = crate::NativeMaterializationWork::new(budget, &cancellation).unwrap();
        let child = original.native_volume_work(budget).unwrap();
        let work = if resource { &child } else { &original };
        let mut accounting = KernelAccounting::native(work, &caller);
        let mut observed = OriginalReadAccounting::new(&mut accounting).unwrap();
        if overrun { let _ = work.record_admitted(WorkCounters { bytes_copied: 9, ..WorkCounters::UNCHARGED }); }
        if poisoned { work.state(|state| state.poison_native_original_work()); }
        if original_cancelled { cancellation.cancel(); }
        if caller_cancelled { caller.cancel(); }
        let delta = WorkCounters { bytes_copied: amount, ..WorkCounters::UNCHARGED };
        let expected = work.admit(WorkCounters::UNCHARGED).map_err(FsError::from)
            .and_then(|()| caller.check().map_err(FsError::from))
            .and_then(|()| work.admit(delta).map_err(FsError::from));
        let expected = expected.map_err(|error| format!("{error:?}"));
        let before = original.receipt();
        let before_budget = work.state(|state| state.budget);
        let preflight = observed.observe(super::super::super::codec::ReadAllocationStep::CheckCopy(amount));
        prop_assert_eq!(preflight.map_err(|error| format!("{error:?}")), expected.clone());
        prop_assert_eq!(original.receipt(), before);
        prop_assert_eq!(work.state(|state| state.budget), before_budget);
        let actual = observed.admit(delta);
        prop_assert_eq!(actual.map_err(|error| format!("{error:?}")), expected);
    }

    // The aggregate parent check is a preflight, followed by individually
    // reserved copies. An exact budget must not reserve the same bytes twice.
    #[test]
    fn generation_preflight_does_not_double_reserve(
        first in any::<[u8; 32]>(),
        parent_count in 0_usize..3,
        resource in any::<bool>(),
    ) {
        let mut second = first;
        second[0] ^= 1;
        let root = GenerationRoot {
            volume_id: VolumeId::from_bytes([1; 16]),
            root_file_id: FileId::from_bytes([2; 16]),
            file_table: ObjectId { kind: ObjectKind::FileTablePage, digest: Digest::from_bytes([3; 32]) },
            parents: [first, second].into_iter().take(parent_count)
                .map(|bytes| GenerationId::new(Digest::from_bytes(bytes))).collect(),
            required_features: 0,
        };
        let bytes = encode_generation_root(&root,
            |_| Ok::<_, super::super::super::CanonicalDecodeError>(()),
            |_| super::super::super::CanonicalDecodeError::AllocationFailed,
        ).unwrap();
        let maximum = (bytes.len() - 8) as u64;
        let limit = WorkBudget { bytes_copied: maximum, ..WorkBudget::UNBOUNDED };
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(
            if resource { WorkBudget::UNBOUNDED } else { limit }, &cancellation,
        ).unwrap();
        let work = if resource { original.native_volume_work(limit).unwrap() } else { original.clone() };
        let mut accounting = KernelAccounting::native(&work, &cancellation);
        let mut observed = OriginalReadAccounting::new(&mut accounting).unwrap();
        let decoded = decode_generation_root(&bytes, DecodeLimits::default(), |step| observed.observe(step)).unwrap();
        prop_assert_eq!(decoded, root);
        drop(observed.into_allocation());
        prop_assert_eq!(original.receipt().bytes_copied, maximum);
        prop_assert_eq!(work.remaining().unwrap().bytes_copied, 0);
    }
}
