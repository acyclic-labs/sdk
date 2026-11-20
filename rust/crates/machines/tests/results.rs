use acyclic_machines::results::{NativeResults, ResultCursor, ResultCut, Settlement};
use acyclic_machines::{
    CreateMachine, IdempotencyKey, Image, MachineId, MachinesProvider as _, MutationOutcome,
    OperationId, SimulatedMachines,
};
use acyclic_stream::CommitId;
use proptest::prelude::*;
use std::num::{NonZeroU32, NonZeroU64};

proptest! {
    #[test]
    fn result_cursor_preserves_owner_and_bounded_progress(
        count in any::<u64>(),
        from in any::<u64>(),
        allowance in 1..=u32::MAX,
        revision in 1..=u64::MAX,
    ) {
        let cut = ResultCut {
            operation: OperationId::from_bytes([7; 16]).unwrap(),
            settlement: Settlement::NativeController(NonZeroU64::new(revision).unwrap()),
            count,
        };
        let cursor = ResultCursor { cut, from };
        let credit = NonZeroU32::new(allowance).unwrap();
        let interval = cursor.interval(cut, credit);
        if from > count {
            prop_assert!(interval.is_err());
        } else {
            let (range, next) = interval.unwrap();
            prop_assert_eq!(range.start, from);
            prop_assert_eq!(range.end - from, (count - from).min(u64::from(allowance)));
            prop_assert!(range.end <= count);
            prop_assert_eq!(next.is_some(), range.end < count);
            if let Some(next) = next {
                prop_assert_eq!(next.cut, cut);
                prop_assert_eq!(next.from, range.end);
                prop_assert!(next.from > from);
            }
        }
        // A revision cast into commit bytes would erase the actual owner. The
        // same operation/index/extent must still refuse that substitution.
        let mut cast = [0; 32];
        cast[..8].copy_from_slice(&revision.to_le_bytes());
        let substituted = ResultCursor {
            cut: ResultCut { settlement: Settlement::Streams(CommitId::from_bytes(cast)), ..cut },
            from: from.min(count),
        };
        prop_assert!(substituted.interval(cut, credit).is_err());
        // Owner-erasing comparison is the failing negative control.
        prop_assert_eq!(substituted.cut.operation, cut.operation);
        prop_assert_eq!(u64::from_le_bytes(cast[..8].try_into().unwrap()), revision);
    }
}

#[tokio::test]
async fn native_pages_borrow_the_original_observations_and_validate_their_cut()
-> Result<(), Box<dyn std::error::Error>> {
    let MutationOutcome::Created(machine) = SimulatedMachines::default()
        .create(CreateMachine::new(
            IdempotencyKey::new(),
            Some(Image::custom([7; 32])?),
            [8; 32],
        ))
        .await?
    else {
        panic!("original creation must settle");
    };
    let operation = OperationId::new();
    let mut machines = Vec::new();
    for index in 0..1_025_u64 {
        let mut value = machine.clone();
        let mut identity = [7; 32];
        identity[..8].copy_from_slice(&index.to_le_bytes());
        value.id = MachineId::from_digest(identity);
        machines.push(value);
    }
    let native_revision = NonZeroU64::new(17).unwrap();
    let native = NativeResults::open(operation, native_revision, &machines)?;
    let original = native.cursor();
    let borrowed = native.page(original, NonZeroU32::new(1).unwrap())?;
    assert_eq!(borrowed.machines.as_ptr(), machines.as_ptr());
    assert_eq!(borrowed.machines.len(), 1);
    assert_eq!(borrowed.next.unwrap().from, 1);
    // Copying even this one-item page fails the original-allocation oracle.
    let copied = borrowed.machines.to_vec();
    assert_ne!(copied.as_ptr(), borrowed.machines.as_ptr());
    let wrong_owner = ResultCursor {
        cut: ResultCut {
            settlement: Settlement::Streams(CommitId::from_bytes([9; 32])),
            ..original.cut
        },
        ..original
    };
    assert!(
        native
            .page(wrong_owner, NonZeroU32::new(1).unwrap())
            .is_err()
    );
    for cut in [
        ResultCut {
            operation: OperationId::new(),
            ..original.cut
        },
        ResultCut {
            settlement: Settlement::NativeController(NonZeroU64::new(18).unwrap()),
            ..original.cut
        },
        ResultCut {
            count: original.cut.count - 1,
            ..original.cut
        },
    ] {
        assert!(
            native
                .page(
                    ResultCursor { cut, ..original },
                    NonZeroU32::new(1).unwrap()
                )
                .is_err()
        );
    }
    // Dropping page interest changes no result. Reopening the same original
    // revision resumes exactly; no producer wait, cancellation or redispatch.
    let resumed = borrowed.next.unwrap();
    let reopened = NativeResults::open(operation, native_revision, &machines)?;
    let remaining = reopened.page(resumed, NonZeroU32::new(u32::MAX).unwrap())?;
    assert_eq!(remaining.machines, &machines[1..]);
    assert_eq!(remaining.machines.as_ptr(), machines[1..].as_ptr());
    assert!(remaining.next.is_none());
    Ok(())
}
