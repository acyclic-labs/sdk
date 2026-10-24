use super::*;
use crate::foundation::Digest;
use crate::kernel::tree_mutation::{TreeFormat, TreeMutation};
use crate::kernel::{LogicalName, NameEncoding};
use crate::{MemoryObjectStore, NativeMaterializationWork, WorkBudget};
use acyclic_test_support::{AllocationInfo, measure};

fn children(count: u8, accounting: &mut KernelAccounting<'_>) -> Owned<Vec<Child<LogicalName>>> {
    let mut values = Owned::allocate(usize::from(count), accounting).unwrap();
    for key in 0..count {
        let name = LogicalName::new(NameEncoding::PosixBytes, vec![key + 1; 64], 255).unwrap();
        let (first, mut allocation) =
            persistent_io::clone_key::<TreeFormat>(&name, 255, accounting, None, 0).unwrap();
        values.bytes += TreeFormat::key_owned_bytes(&first);
        values.allocation.absorb(&mut allocation).unwrap();
        values.value.push(Child {
            first,
            page: ObjectId {
                kind: TreeFormat::kind(),
                digest: Digest::from_bytes([key; 32]),
            },
        });
    }
    values
}

fn preserve_counts(count: u8, historical: bool, native: bool) -> AllocationInfo {
    let token = CancellationToken::new();
    let original = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &token).unwrap();
    let baseline = original.live_allocation_bytes();
    let mut accounting = if native {
        KernelAccounting::native(&original, &token)
    } else {
        KernelAccounting::standalone(WorkBudget::UNBOUNDED, &token)
    };
    let values = children(count, &mut accounting);
    let store = MemoryObjectStore::default();
    let visited = VisitedObjectSet::new_with_accounting(1, &mut accounting).unwrap();
    let mut context = Context::<_, TreeFormat, TreeMutation> {
        store: &store,
        limits: DecodeLimits::default(),
        accounting: &mut accounting,
        visited,
        maximum_seen_height: 0,
        cancellation: &token,
        marker: PhantomData,
    };
    let mut shared_source = None;
    let mut entries = if historical {
        InternalEntries::Owned {
            children: values,
            rewritten: Owned::allocate(usize::from(count), context.accounting).unwrap(),
        }
    } else {
        let (shared, _) = share_values(
            values,
            |child| TreeFormat::key_owned_bytes(&child.first),
            context.accounting,
        )
        .unwrap();
        let entries = context.shared_internal_entries(&shared.value).unwrap();
        shared_source = Some(shared);
        entries
    };
    let start = std::time::Instant::now();
    let measured = measure(|| {
        for index in 0..usize::from(count) {
            context.preserve_child(&mut entries, index).unwrap();
        }
    });
    eprintln!(
        "unchanged internal keys K={count} old={historical} native={native}: {measured:?} {:?}",
        start.elapsed()
    );
    match entries {
        InternalEntries::Owned {
            children,
            rewritten,
        } => {
            assert_eq!(children.value, rewritten.value);
            rewritten.retire(context.accounting).unwrap();
            children.retire(context.accounting).unwrap();
        }
        InternalEntries::Shared {
            children,
            rewritten,
        } => {
            assert_eq!(children.value.len(), rewritten.value.len());
            for (before, after) in children.value.iter().zip(&rewritten.value) {
                assert!(
                    Arc::ptr_eq(before, after),
                    "unchanged routing key was copied"
                );
            }
            rewritten.retire(context.accounting).unwrap();
            children.retire(context.accounting).unwrap();
        }
    }
    context.visited.retire(context.accounting).unwrap();
    if let Some(shared) = shared_source {
        shared.retire(&mut accounting).unwrap();
    }
    if native {
        assert_eq!(original.live_allocation_bytes(), baseline);
    } else {
        assert_eq!(accounting.standalone_parts().unwrap().0.live_bytes(), 0);
    }
    measured
}

fn pin(historical: bool) {
    for native in [false, true] {
        let counts = [8, 24].map(|count| preserve_counts(count, historical, native));
        for count in counts {
            assert_eq!(
                count,
                AllocationInfo::default(),
                "unchanged internal routing keys allocate"
            );
        }
    }
}

#[test]
fn unchanged_internal_routing_entries_retain_their_owner_without_allocating() {
    pin(false);
}

#[test]
#[should_panic(expected = "unchanged internal routing keys allocate")]
fn restored_owned_children_fail_the_same_zero_allocation_pin() {
    pin(true);
}
