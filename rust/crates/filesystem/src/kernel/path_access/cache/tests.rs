use super::*;

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
#[tokio::test]
async fn owned_cache_batches_preserve_original_funding_at_every_capacity()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::kernel::allocation::retained::Retained;
    crate::initialize_test_native_io();
    let directory = tempfile::tempdir()?;
    let provider =
        acyclic_objects::local::LocalObjects::open(directory.path(), Default::default(), None)
            .await?;
    let provider = std::sync::Arc::new(provider);
    let store = crate::LogicalObjectStore::new(std::sync::Arc::clone(&provider));
    let mut requests = Vec::new();
    for bytes in [b"first".as_slice(), b"second".as_slice()] {
        requests.push(ObjectReadRequest {
            object_id: crate::test_support::put_object(&store, crate::ObjectKind::BlobChunk, bytes)
                .await?,
            maximum_bytes: bytes.len() as u64,
        });
    }
    requests.push(requests[0]);
    let page =
        crate::test_support::put_tree_page(&store, &crate::kernel::TreePage::Leaf(Vec::new()), 8)
            .await?;
    let cancellation = CancellationToken::new();
    for (capacity, expected_owned, expected_page_reads) in
        [(4096, 3, 2), (0, 3, 2), (1, 1, 2), (2, 0, 2), (3, 0, 1)]
    {
        let mut measured_peak = u64::MAX;
        // The observed operation must fit the exact peak of the same physical
        // reads without an observer. Residency is already on the source ledger.
        for observed in [false, true] {
            let custody = crate::kernel::ProofDataCustody::default();
            let owner = observed.then_some(&custody);
            let store = crate::LogicalObjectStore::new(std::sync::Arc::clone(&provider));
            let mut work = crate::AuthorityWork::new(
                WorkBudget {
                    peak_allocation_bytes: measured_peak,
                    ..WorkBudget::UNBOUNDED
                },
                &cancellation,
            );
            let mut independent = Retained::<Vec<u8>>::allocate(73, &mut work.accounting())?;
            independent.value.resize(73, 0xa5);
            let independent_bytes = independent.value.capacity() as u64;
            let baseline = work.allocations.live_bytes();
            let cache = OperationReadCache::new(&store, capacity, &mut work.accounting(), owner)?;
            if capacity == 4096 {
                // Persist distinct canonical chunks through the real provider, then
                // fill the operation cache through its ordinary read entry point.
                let objects: Vec<_> = (0..capacity as u64)
                    .map(|value| {
                        crate::storage::HashedObject::new(
                            crate::ObjectKind::BlobChunk,
                            OwnedBytes::from(value.to_le_bytes().to_vec()),
                        )
                    })
                    .collect();
                store
                    .put_many_hashed(
                        &objects,
                        &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation),
                    )
                    .await?;
                let warm: Vec<_> = objects
                    .iter()
                    .map(|object| ObjectReadRequest {
                        object_id: object.object_id(),
                        maximum_bytes: object.bytes().len() as u64,
                    })
                    .collect();
                let reads = crate::kernel::read_objects_with_accounting(
                    &cache,
                    &warm,
                    &mut work.accounting(),
                    &cancellation,
                )
                .await
                .map_err(|error| format!("warming {capacity} operation cache entries: {error}"))?;
                assert_eq!(cache.lock_state().entry_count, capacity);
                let retained = crate::kernel::file_read::retained_object_reads_bytes(
                    &reads.value,
                    reads.value.capacity(),
                )?;
                drop(reads.value);
                crate::kernel::file_read::release_read_allocation(
                    retained,
                    reads.custody,
                    &mut work.accounting(),
                )?;
            }
            let reads = crate::kernel::read_objects_with_accounting(
                &cache,
                &requests,
                &mut work.accounting(),
                &cancellation,
            )
            .await
            .map_err(|error| format!("owned miss batch at cache capacity {capacity}: {error}"))?;
            assert_eq!(reads.value.len(), requests.len());
            assert_eq!(reads.value[0].bytes.as_ref(), b"first");
            assert_eq!(reads.value[1].bytes.as_ref(), b"second");
            assert_eq!(reads.value[2].bytes.as_ref(), b"first");
            let owned = reads
                .value
                .iter()
                .filter(|read| matches!(read.retention, ObjectReadRetention::Owned { .. }))
                .count();
            assert_eq!(owned, expected_owned);
            let retained = crate::kernel::file_read::retained_object_reads_bytes(
                &reads.value,
                reads.value.capacity(),
            )?;
            let state = cache.lock_state();
            let resident =
                OperationReadCache::<()>::slot_bytes(&state)? + state.retained_owned_bytes;
            drop(state);
            assert_eq!(
                work.allocations.live_bytes(),
                baseline + resident + retained
            );
            drop(reads.value);
            crate::kernel::file_read::release_read_allocation(
                retained,
                reads.custody,
                &mut work.accounting(),
            )?;
            assert_eq!(work.allocations.live_bytes(), baseline + resident);
            // Keep a physical buffer larger than the earlier batch high-water
            // mark alive, so that history cannot hide a duplicate cache charge.
            let mut independent_peak = Retained::<Vec<u8>>::allocate(
                usize::try_from(work.spent().peak_allocation_bytes)?,
                &mut work.accounting(),
            )?;
            let peak_bytes = independent_peak.value.capacity() as u64;
            independent_peak
                .value
                .resize(usize::try_from(peak_bytes)?, 0x5a);
            let baseline = baseline + peak_bytes;
            let before = work.spent().backend_read_operations;
            // Distinct decode bounds miss the decoded cache while sharing the
            // operation cache's original encoded object.
            for maximum_page_items in [1, 2] {
                let (decoded, allocation) =
                    crate::kernel::persistent_io::read_page_with_accounting::<
                        _,
                        crate::kernel::tree_mutation::TreeFormat,
                    >(
                        &cache,
                        page,
                        DecodeLimits {
                            maximum_page_items,
                            ..DecodeLimits::default()
                        },
                        &mut work.accounting(),
                        &cancellation,
                        owner,
                    )
                    .await?;
                let retained = decoded.logical_bytes;
                drop(decoded);
                crate::kernel::file_read::release_read_allocation(
                    retained,
                    allocation,
                    &mut work.accounting(),
                )?;
                let state = cache.lock_state();
                let resident =
                    OperationReadCache::<()>::slot_bytes(&state)? + state.retained_owned_bytes;
                assert_eq!(work.allocations.live_bytes(), baseline + resident);
                assert_eq!(work.allocations.scratch_bytes(), 0);
            }
            assert_eq!(
                work.spent().backend_read_operations - before,
                expected_page_reads
            );
            if capacity == 3 {
                // Decode the real cached page after all asynchronous frames
                // retire. A live independent buffer makes this exact admission
                // the operation's peak, without frame-size headroom.
                let mut boundary = Retained::<Vec<u8>>::allocate(
                    usize::try_from(work.spent().peak_allocation_bytes)?,
                    &mut work.accounting(),
                )?;
                boundary.value.resize(boundary.value.capacity(), 0x6b);
                let read = cache
                    .probe(page, &mut work.accounting())?
                    .ok_or("encoded page was not cached")?;
                let (decoded, allocation) =
                    crate::kernel::persistent_io::decode_read_with_accounting::<
                        crate::kernel::tree_mutation::TreeFormat,
                    >(
                        &read,
                        DecodeLimits::default(),
                        &mut work.accounting(),
                        owner,
                    )?;
                let retained = decoded.logical_bytes;
                drop(decoded);
                crate::kernel::file_read::release_read_allocation(
                    retained,
                    allocation,
                    &mut work.accounting(),
                )?;
                assert!(boundary.value.iter().all(|byte| *byte == 0x6b));
                drop(boundary);
                let state = cache.lock_state();
                let resident =
                    OperationReadCache::<()>::slot_bytes(&state)? + state.retained_owned_bytes;
                assert_eq!(work.allocations.live_bytes(), baseline + resident);
            }
            if observed && capacity == 3 {
                // A different ledger still owes the simultaneously resident
                // cache. Excluding every cache byte would make this succeed.
                let resident = custody.operation_cache_bytes();
                let mut foreign = KernelAccounting::standalone(
                    WorkBudget {
                        peak_allocation_bytes: resident - 1,
                        ..WorkBudget::UNBOUNDED
                    },
                    &cancellation,
                );
                let read = cache
                    .probe(page, &mut foreign)?
                    .ok_or("encoded page was not cached")?;
                let failure = crate::kernel::persistent_io::decode_read_with_accounting::<
                    crate::kernel::tree_mutation::TreeFormat,
                >(&read, DecodeLimits::default(), &mut foreign, owner)
                .err()
                .ok_or("foreign cache residency was not admitted")?;
                assert!(
                    matches!(failure, crate::FsError::Work(WorkError::BudgetExceeded {
                    counter: "peak_allocation_bytes", observed, maximum,
                }) if observed == resident && maximum == resident - 1)
                );
            }
            drop(cache);
            assert_eq!(custody.operation_cache_bytes(), 0);
            assert_eq!(work.allocations.live_bytes(), baseline);
            assert!(independent.value.iter().all(|byte| *byte == 0xa5));
            assert!(independent_peak.value.iter().all(|byte| *byte == 0x5a));
            drop(independent_peak);
            drop(independent);
            assert_eq!(
                work.allocations.live_bytes(),
                baseline - peak_bytes - independent_bytes
            );
            if observed {
                assert_eq!(work.spent().peak_allocation_bytes, measured_peak);
            } else {
                measured_peak = work.spent().peak_allocation_bytes;
            }
        }
    }
    Ok(())
}
use proptest::prelude::*;

std::thread_local! {
    static SLOT_ACCESSES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

pub(super) fn record_slot_access() {
    SLOT_ACCESSES.with(|count| count.set(count.get() + 1));
}

fn take_slot_accesses() -> u64 {
    SLOT_ACCESSES.with(|count| count.replace(0))
}

fn id(value: u8) -> ObjectId {
    let mut bytes = [0; 32];
    bytes[0] = value;
    ObjectId {
        storage: None,
        kind: crate::ObjectKind::Blob,
        digest: crate::Digest::from_bytes(bytes),
    }
}

// Independent open-addressed table: no production probing or cache state is
// consulted. Count every occupied collision slot and the terminating slot,
// including old-table lookup, each rehash insertion, and the post-growth lookup.
struct ProbeModel {
    slots: Vec<Option<ObjectId>>,
    entries: usize,
}

impl ProbeModel {
    fn new() -> Self {
        Self {
            slots: vec![None; 16],
            entries: 0,
        }
    }

    // FNV over the canonical reference, as the cache binds the full
    // reference: kind, digest, and any sealed association.
    fn hash(key: ObjectId) -> usize {
        key.to_bytes()
            .iter()
            .fold(usize::from(key.kind.canonical_tag()), |hash, byte| {
                (hash ^ usize::from(*byte)).wrapping_mul(16_777_619)
            })
    }

    fn find(slots: &[Option<ObjectId>], key: ObjectId) -> (usize, bool, u64) {
        let start = Self::hash(key) % slots.len();
        for visits in 1..=slots.len() {
            let index = (start + visits - 1) % slots.len();
            match slots[index] {
                Some(found) if found != key => continue,
                found => return (index, found.is_some(), visits as u64),
            }
        }
        panic!("model must retain a vacant slot")
    }

    fn admit(&mut self, key: ObjectId) -> (bool, u64) {
        if self.entries == 32 {
            return (false, 0);
        }
        let (mut vacant, duplicate, mut visits) = Self::find(&self.slots, key);
        if duplicate {
            return (false, visits);
        }
        if self.entries * 2 >= self.slots.len() && self.slots.len() < 64 {
            let mut grown = vec![None; self.slots.len() * 2];
            for old in self.slots.iter().flatten().copied() {
                let (index, duplicate, cost) = Self::find(&grown, old);
                assert!(!duplicate);
                visits += cost;
                grown[index] = Some(old);
            }
            self.slots = grown;
            let (index, duplicate, cost) = Self::find(&self.slots, key);
            assert!(!duplicate);
            vacant = index;
            visits += cost;
        }
        self.slots[vacant] = Some(key);
        self.entries += 1;
        (true, visits)
    }
}

fn compare<const CHARGE_REHASH: bool, const EXTRA_UNCHARGED_PROBE: bool>(
    keys: &[u8],
) -> proptest::test_runner::TestCaseResult {
    let cancellation = CancellationToken::new();
    let mut setup = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let cache = OperationReadCache::new(&(), 32, &mut setup, None).unwrap();
    let bytes = OwnedBytes::from_static(b"payload");
    let mut model = ProbeModel::new();
    for &key in keys {
        let object = id(key);
        let expected = model.admit(object);
        let mut work = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
        take_slot_accesses();
        if EXTRA_UNCHARGED_PROBE {
            let state = cache.lock_state();
            let _ = probe_slots_with_charge::<false>(&state.slots.value, object, &mut work, None)
                .unwrap();
        }
        let inserted = cache
            .admit_with_rehash::<true, CHARGE_REHASH>(object, &bytes, 0, &mut work)
            .unwrap();
        let observed = take_slot_accesses();
        prop_assert_eq!(inserted, expected.0);
        prop_assert_eq!(work.spent().items_examined, expected.1);
        prop_assert_eq!(
            observed,
            work.spent().items_examined,
            "slot visits must match charged items"
        );
        prop_assert_eq!(observed, expected.1);
        for previous in model.slots.iter().flatten().copied() {
            let (_, found, visits) = ProbeModel::find(&model.slots, previous);
            let mut read = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
            prop_assert!(found);
            take_slot_accesses();
            prop_assert_eq!(
                cache.probe(previous, &mut read).unwrap().unwrap().bytes,
                bytes.clone()
            );
            let observed = take_slot_accesses();
            prop_assert_eq!(observed, visits);
            prop_assert_eq!(read.spent().items_examined, observed);
        }
    }
    Ok(())
}

proptest! {
    #[test]
    fn vacant_slot_reuse_keeps_exact_probes_and_all_prior_entries(
        keys in prop::collection::vec(any::<u8>(), 1..97),
    ) {
        compare::<true, false>(&keys)?;
    }
}

#[test]
fn collision_growth_duplicates_and_capacity_match_independent_counts() {
    let colliding: Vec<_> = (0..=255)
        .filter(|&key| ProbeModel::hash(id(key)) % 16 == 0)
        .collect();
    assert_eq!(colliding.len(), 16);
    let mut keys = colliding.clone();
    keys.extend(colliding.iter().copied());
    keys.extend(0..40);
    keys.extend(0..40);
    compare::<true, false>(&keys).unwrap();
}

#[test]
fn independent_counts_detect_uncharged_rehash_probes() {
    let keys: Vec<_> = (0..17).collect();
    compare::<true, false>(&keys).unwrap();
    assert!(compare::<false, false>(&keys).is_err());
}

#[test]
fn actual_slot_observer_detects_an_extra_discarded_uncharged_probe() {
    compare::<true, false>(&[1]).unwrap();
    let error = compare::<true, true>(&[1]).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("slot visits must match charged items")
    );
}

#[test]
fn one_required_probe_is_sufficient_at_the_exact_item_limit() {
    for native in [
        false,
        #[cfg(not(target_arch = "wasm32"))]
        true,
    ] {
        let cancellation = CancellationToken::new();
        #[cfg(not(target_arch = "wasm32"))]
        let original = crate::NativeMaterializationWork::new(
            WorkBudget {
                items_examined: 1,
                ..crate::performance::BOUNDARY_BUDGET
            },
            &cancellation,
        )
        .unwrap();
        let mut setup = match native {
            #[cfg(not(target_arch = "wasm32"))]
            true => KernelAccounting::native(&original, &cancellation),
            _ => KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation),
        };
        let cache = OperationReadCache::new(&(), 2, &mut setup, None).unwrap();
        let mut exact = match native {
            #[cfg(not(target_arch = "wasm32"))]
            true => KernelAccounting::native(&original, &cancellation),
            _ => KernelAccounting::standalone(
                WorkBudget {
                    items_examined: 1,
                    ..WorkBudget::UNBOUNDED
                },
                &cancellation,
            ),
        };
        assert!(
            cache
                .admit(id(1), &OwnedBytes::from_static(b"one"), 0, &mut exact)
                .unwrap()
        );
        assert_eq!(exact.spent().items_examined, 1);
        let mut verify = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
        assert!(cache.probe(id(1), &mut verify).unwrap().is_some());
    }
}
