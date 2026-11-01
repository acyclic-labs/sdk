//! Exact namespace/object singleflight identity on the shared funded radix.
use super::*;
use crate::kernel::allocation::{
    fixed_index::PreparedIndexWork,
    retained::{RetainedAllocation, RetainedArc},
};

const KEY_BYTES: usize = 8 + ObjectId::ENCODED_LEN;
type Index = acyclic_collections::FixedIndex<RetainedArc<Flight>, RetainedAllocation, KEY_BYTES>;
type Retired =
    acyclic_collections::ExtractedPrefix<RetainedArc<Flight>, RetainedAllocation, KEY_BYTES>;

#[derive(Default)]
pub(super) struct Flights {
    index: Index,
    len: usize,
}

fn key((namespace, object): (u64, ObjectId)) -> [u8; KEY_BYTES] {
    let mut bytes = [0; KEY_BYTES];
    bytes[..8].copy_from_slice(&namespace.to_be_bytes());
    bytes[8..].copy_from_slice(&object.index_key());
    bytes
}

impl Flights {
    pub(super) fn len(&self) -> usize {
        self.len
    }

    pub(super) fn get(
        &self,
        object: (u64, ObjectId),
        work: &mut impl acyclic_collections::IndexWork<
            Error = crate::FsError,
            Allocation = RetainedAllocation,
        >,
    ) -> Result<Option<RetainedArc<Flight>>, ObjectStoreError> {
        self.index
            .get(key(object), work)
            .map(|value| value.cloned())
            .map_err(crate::async_storage::object_error)
    }

    pub(super) fn install(
        &mut self,
        object: (u64, ObjectId),
        proposed: &RetainedArc<Flight>,
        work: &mut impl acyclic_collections::IndexWork<
            Error = crate::FsError,
            Allocation = RetainedAllocation,
        >,
    ) -> Result<ReadPlan, ObjectStoreError> {
        let next = self.len.checked_add(1).ok_or(crate::WorkError::Overflow)?;
        let mut inserted = false;
        let flight = self
            .index
            .get_or_try_insert_with(key(object), work, |_| {
                inserted = true;
                Ok(proposed.clone())
            })
            .map_err(crate::async_storage::object_error)?
            .clone();
        if inserted {
            self.len = next;
            Ok(ReadPlan::Leader(flight))
        } else {
            Ok(ReadPlan::Follower(flight))
        }
    }

    // Serialized retained cleanup cannot admit work, refund backing, or lose a
    // later owner. Return the physical nodes and tokens for unlocked retirement.
    pub(super) fn detach(
        &mut self,
        object: (u64, ObjectId),
        owner: &RetainedArc<Flight>,
    ) -> Option<Retired> {
        let key = key(object);
        self.index
            .get_retained_mut(key, |active| same_flight(active, owner), |_| {})?;
        let retired = self
            .index
            .extract_prefix(&key, |_| {})
            .unwrap_or_else(|_| unreachable!("a complete key fits its original width"));
        self.len -= 1;
        Some(retired)
    }
}

pub(super) fn same_flight(a: &RetainedArc<Flight>, b: &RetainedArc<Flight>) -> bool {
    std::ptr::eq(&**a, &**b)
}

pub(super) fn remove(
    namespace: &Namespace,
    object: ObjectId,
    owner: &RetainedArc<Flight>,
) -> Result<(), ObjectStoreError> {
    let retired = namespace
        .pool
        .state
        .lock()
        .map_err(|_| ObjectStoreError::Corrupt)?
        .flights
        .detach(namespace.key(object), owner);
    drop(retired);
    Ok(())
}

pub(super) fn read_work(
    accounting: &mut crate::kernel::KernelAccounting<'_>,
) -> Result<PreparedIndexWork, ObjectStoreError> {
    PreparedIndexWork::read::<KEY_BYTES>(accounting).map_err(crate::async_storage::object_error)
}

pub(super) fn insertion_work(
    accounting: &mut crate::kernel::KernelAccounting<'_>,
) -> Result<PreparedIndexWork, ObjectStoreError> {
    PreparedIndexWork::insert::<RetainedArc<Flight>, KEY_BYTES>(accounting)
        .map_err(crate::async_storage::object_error)
}

#[cfg(all(test, not(target_arch = "wasm32")))]
pub(crate) fn verify_original_flight_admission_and_retirement()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::kernel::KernelAccounting;
    use acyclic_collections::IndexWork;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct Counted<'a> {
        original: &'a mut PreparedIndexWork,
        visits: u64,
    }
    impl IndexWork for Counted<'_> {
        type Error = crate::FsError;
        type Allocation = RetainedAllocation;
        fn check(&mut self) -> Result<(), Self::Error> {
            self.original.check()
        }
        fn charge_items(&mut self, items: u64) -> Result<(), Self::Error> {
            self.visits += items;
            self.original.charge_items(items)
        }
        fn claim(&mut self, bytes: u64) -> Result<Self::Allocation, Self::Error> {
            self.original.claim(bytes)
        }
    }

    let object = ObjectId {
        kind: crate::ObjectKind::FileTablePage,
        digest: crate::Digest::from_bytes([0; 32]),
        storage: None,
    };
    let mut counts = Vec::new();
    let mut ordered_counts = Vec::new();
    // This is the original ordered-map mechanism, with comparisons counted by
    // its actual key comparison rather than an elapsed-time surrogate.
    #[derive(Eq, PartialEq)]
    struct Ordered(u64);
    static COMPARISONS: AtomicU64 = AtomicU64::new(0);
    impl Ord for Ordered {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            COMPARISONS.fetch_add(1, Ordering::Relaxed);
            self.0.cmp(&other.0)
        }
    }
    impl PartialOrd for Ordered {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(other))
        }
    }
    for population in [1_u64, 100_000] {
        let cancel = CancellationToken::new();
        let bound = Index::insertion_bound(population + 2).ok_or(crate::WorkError::Overflow)?;
        let setup_budget = WorkBudget {
            items_examined: bound.examined_items + 4 * KEY_BYTES as u64,
            allocation_operations: 2 * population + 16,
            peak_allocation_bytes: bound.allocation_bytes + 65_536,
            ..crate::performance::BOUNDARY_BUDGET
        };
        let original = crate::NativeMaterializationWork::new(setup_budget, &cancel)
            .map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let mut accounting = KernelAccounting::native(&original, &cancel);
        let owner = RetainedArc::new(Flight::new(0), &mut accounting)?;
        let mut index = Flights::default();
        // Actual input construction is separate from the fixed-output probe.
        for namespace in (0..population).rev().chain(std::iter::once(u64::MAX)) {
            let mut work = insertion_work(&mut accounting)?;
            assert!(matches!(
                index.install((namespace, object), &owner, &mut work)?,
                ReadPlan::Leader(_)
            ));
        }
        let mut lookup = read_work(&mut accounting)?;
        let mut measured = Counted {
            original: &mut lookup,
            visits: 0,
        };
        assert!(same_flight(
            &index
                .get((u64::MAX, object), &mut measured)?
                .ok_or("missing own flight")?,
            &owner
        ));
        counts.push(measured.visits);
        let mut old = BTreeMap::new();
        for namespace in 0..population {
            old.insert(Ordered(namespace), ());
        }
        old.insert(Ordered(u64::MAX), ());
        COMPARISONS.store(0, Ordering::Relaxed);
        assert!(old.get(&Ordered(u64::MAX)).is_some());
        ordered_counts.push(COMPARISONS.load(Ordering::Relaxed));
        drop(index);
        drop(owner);
        assert_eq!(original.live_allocation_bytes(), baseline);
        original.receipt().verify(setup_budget)?;
    }
    assert_eq!(counts[0], counts[1]);
    assert!(counts[0] <= KEY_BYTES as u64 + 2);
    assert!(ordered_counts[1] > ordered_counts[0]);
    println!("flight visits={counts:?}; original ordered comparisons={ordered_counts:?}");

    // Full sealed association participates in identity. Detaching the original
    // owner cannot retire its replacement, even after cancellation.
    let cancel = CancellationToken::new();
    let original =
        crate::NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &cancel)
            .map_err(|(error, _)| error)?;
    let baseline = original.live_allocation_bytes();
    let mut accounting = KernelAccounting::native(&original, &cancel);
    let owner = RetainedArc::new(Flight::new(0), &mut accounting)?;
    let replacement = RetainedArc::new(Flight::new(1), &mut accounting)?;
    let mut index = Flights::default();
    let sealed = ObjectId {
        storage: Some(crate::ObjectStorage {
            locator: object.digest,
            root_file_id: crate::FileId::from_bytes([0; 16]),
            origin_generation: object.digest,
        }),
        ..object
    };
    for id in [object, sealed] {
        let mut work = insertion_work(&mut accounting)?;
        assert!(matches!(
            index.install((7, id), &owner, &mut work)?,
            ReadPlan::Leader(_)
        ));
    }
    let retired = index
        .detach((7, object), &owner)
        .ok_or("missing original retirement")?;
    let mut work = insertion_work(&mut accounting)?;
    assert!(matches!(
        index.install((7, object), &replacement, &mut work)?,
        ReadPlan::Leader(_)
    ));
    drop(work);
    cancel.cancel();
    assert!(read_work(&mut accounting).is_err());
    assert!(index.detach((7, object), &owner).is_none());
    assert!(index.detach((7, sealed), &replacement).is_none());
    assert_eq!(index.len(), 2);
    let before_retirement = original.live_allocation_bytes();
    drop(retired);
    assert!(original.live_allocation_bytes() < before_retirement);
    drop(index);
    drop(owner);
    drop(replacement);
    assert_eq!(original.live_allocation_bytes(), baseline);

    // Ordinary retention splits the same original ledger. Failed splits leave
    // both claims intact; no new ledger or copied allowance can fund the node.
    let cancel = CancellationToken::new();
    let mut ordinary = KernelAccounting::standalone(crate::performance::BOUNDARY_BUDGET, &cancel);
    let mut paid = RetainedAllocation::claim(17, 1, &mut ordinary)?;
    let before = ordinary
        .standalone_parts()
        .ok_or("missing original ledger")?
        .0
        .live_bytes();
    assert!(paid.split_off(18).is_err());
    let split = paid.split_off(7)?;
    assert_eq!(
        ordinary
            .standalone_parts()
            .ok_or("missing original ledger")?
            .0
            .live_bytes(),
        before
    );
    drop(paid);
    assert_eq!(
        ordinary
            .standalone_parts()
            .ok_or("missing original ledger")?
            .0
            .live_bytes(),
        before - 10
    );
    drop(split);
    assert_eq!(
        ordinary
            .standalone_parts()
            .ok_or("missing original ledger")?
            .0
            .live_bytes(),
        before - 17
    );
    let baseline = ordinary
        .standalone_parts()
        .ok_or("missing original ledger")?
        .0
        .live_bytes();
    let owner = RetainedArc::new(Flight::new(0), &mut ordinary)?;
    let mut index = Flights::default();
    let mut admission = insertion_work(&mut ordinary)?;
    assert!(matches!(
        index.install((0, object), &owner, &mut admission)?,
        ReadPlan::Leader(_)
    ));
    drop(admission);
    drop(index);
    drop(owner);
    assert_eq!(
        ordinary
            .standalone_parts()
            .ok_or("missing original ledger")?
            .0
            .live_bytes(),
        baseline
    );

    let bound = Index::insertion_bound(1).ok_or("missing insertion bound")?;
    let denied = WorkBudget {
        peak_allocation_bytes: crate::NativeMaterializationWork::accounting_state_bytes()
            + bound.allocation_bytes
            - 1,
        ..crate::performance::BOUNDARY_BUDGET
    };
    let original =
        crate::NativeMaterializationWork::new(denied, &cancel).map_err(|(error, _)| error)?;
    let baseline = original.live_allocation_bytes();
    assert!(insertion_work(&mut KernelAccounting::native(&original, &cancel)).is_err());
    assert_eq!(original.live_allocation_bytes(), baseline);
    Ok(())
}
