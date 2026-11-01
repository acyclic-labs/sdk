use super::*;
use proptest::prelude::*;

fn live(meter: &ProofMeter<'_>) -> u64 {
    match meter {
        ProofMeter::Standalone { state, .. } => state.lock().unwrap().allocations.live_bytes(),
        #[cfg(not(target_arch = "wasm32"))]
        ProofMeter::Native { original, .. } => original.live_allocation_bytes(),
    }
}

fn object(ordinal: u64) -> ObjectId {
    let mut digest = [0; 32];
    digest[..8].copy_from_slice(&ordinal.to_be_bytes());
    ObjectId {
        kind: ObjectKind::BlobChunk,
        digest: crate::Digest::from_bytes(digest),
        storage: None,
    }
}

#[test]
fn indexed_proof_membership_preserves_order_and_original_custody()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let meter = ProofMeter::standalone(WorkBudget::UNBOUNDED, &cancellation)?;
    // Promote the same original ledger before taking its retained baseline.
    let original = meter.with_accounting(|accounting| {
        crate::kernel::allocation::retained::RetainedAllocation::claim(0, 0, accounting)
    })?;
    let baseline = live(&meter);
    for population in [1_u64, 100_000] {
        let mut set = ProofSet::new(&meter);
        let mut map = ProofMap::new(&meter);
        let before = meter.work();
        for ordinal in (0..population).rev() {
            assert!(set_insert(&mut set, object(ordinal))?);
            assert_eq!(
                map_insert(
                    &mut map,
                    FileId::from_bytes(u128::from(ordinal).to_be_bytes()),
                    ordinal
                )?,
                None
            );
        }
        // One object move per distinct insertion. The old descending sorted
        // vector moves N(N+1)/2 objects and cannot satisfy this linear bound.
        let row_bytes = usize_to_u64(size_of::<(FileId, u64)>());
        let copied = meter.work().bytes_copied - before.bytes_copied;
        assert!(copied <= population * (usize_to_u64(size_of::<ObjectId>()) + 3 * row_bytes));
        if population > 1 {
            let old_moves = population * (population + 1) / 2;
            assert!(old_moves * usize_to_u64(size_of::<ObjectId>()) > copied);
        }
        for ordinal in [0, population / 2, population - 1, population] {
            let before = meter.work();
            assert_eq!(set.contains(object(ordinal))?, ordinal < population);
            assert_eq!(
                map_get(&map, &FileId::from_bytes(u128::from(ordinal).to_be_bytes()))?.copied(),
                (ordinal < population).then_some(ordinal)
            );
            let after = meter.work();
            assert!(
                after.items_examined - before.items_examined
                    <= (9 * ORDERED_OBJECT_KEY_BYTES + 9 * 16 + 2) as u64
            );
            assert_eq!(after.allocation_operations, before.allocation_operations);
            assert_eq!(after.bytes_copied, before.bytes_copied);
        }
        assert!(!set.insert(object(0))?);
        assert_eq!(set.len(), population as usize);
        assert!(!set.contains(object(population))?);
        assert_eq!(
            map.values.first().map(|entry| entry.1),
            Some(population - 1)
        );
        assert_eq!(
            map_insert(&mut map, FileId::from_bytes([0; 16]), 99)?,
            Some(0)
        );
        assert_eq!(
            map.position(FileId::from_bytes([0; 16]))?,
            Some(population as usize - 1)
        );
        let (objects, guard) = set.into_parts()?;
        assert_eq!(objects.len(), population as usize);
        assert!(objects.iter().copied().eq((0..population).map(object)));
        drop(map);
        assert!(live(&meter) > baseline);
        drop(objects);
        drop(guard);
        assert_eq!(live(&meter), baseline);
    }

    // All full-identity fields participate, including seal presence and the
    // creating generation. Wire-tag order differs from ObjectId::Ord here.
    let mut identities = std::collections::BTreeSet::new();
    for bit in 0..(8 * ObjectId::ENCODED_LEN) {
        let mut id = object(0);
        let mut storage = crate::ObjectStorage {
            locator: crate::Digest::from_bytes([0; 32]),
            root_file_id: FileId::from_bytes([0; 16]),
            origin_generation: crate::Digest::from_bytes([0; 32]),
        };
        let byte = bit / 8;
        let mask = 1 << (bit % 8);
        if byte < 33 {
            let mut digest = [0; 32];
            digest[byte % 32] = mask;
            id.digest = crate::Digest::from_bytes(digest);
        } else if byte < 65 {
            let mut locator = [0; 32];
            locator[byte - 33] = mask;
            storage.locator = crate::Digest::from_bytes(locator);
        } else if byte < 81 {
            let mut root = [0; 16];
            root[byte - 65] = mask;
            storage.root_file_id = FileId::from_bytes(root);
        } else {
            let mut generation = [0; 32];
            generation[byte - 81] = mask;
            storage.origin_generation = crate::Digest::from_bytes(generation);
        }
        identities.insert(id);
        id.storage = Some(storage);
        identities.insert(id);
        id.kind = ObjectKind::BindingPage;
        identities.insert(id);
    }
    let mut set = ProofSet::new(&meter);
    for id in identities.iter().rev() {
        assert!(set_insert(&mut set, *id)?);
    }
    let (objects, guard) = set.into_parts()?;
    assert!(objects.iter().eq(identities.iter()));
    drop(objects);
    drop(guard);
    assert_eq!(live(&meter), baseline);

    let mut map = ProofMap::new(&meter);
    map_insert(&mut map, FileId::from_bytes([0; 16]), 1)?;
    for bit in 0..128 {
        map_insert(
            &mut map,
            FileId::from_bytes((1_u128 << bit).to_be_bytes()),
            bit,
        )?;
    }
    let before = meter.work();
    assert_eq!(map_get(&map, &FileId::from_bytes([0; 16]))?, Some(&1));
    assert!(meter.work().items_examined - before.items_examined <= 145);
    cancellation.cancel();
    assert!(map_get(&map, &FileId::from_bytes([0; 16])).is_err());
    drop(map);
    assert_eq!(live(&meter), baseline);
    drop(original);
    Ok(())
}

#[test]
fn refused_proof_map_admission_retires_nested_guards_outside_the_meter()
-> Result<(), Box<dyn std::error::Error>> {
    fn attempt(
        meter: &ProofMeter<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), crate::FsError> {
        let original = meter.with_accounting(|accounting| {
            crate::kernel::allocation::retained::RetainedAllocation::claim(0, 0, accounting)
        })?;
        let baseline = live(meter);
        let (_, guard) = meter.owned(|accounting| Ok(((), accounting.claim_owned(8, 1)?, 8)))?;
        let mut map = ProofMap::new(meter);
        let inserted = map_insert(&mut map, FileId::from_bytes([0; 16]), guard);
        match inserted {
            Ok(previous) => assert!(previous.is_none()),
            Err(_) => {
                assert!(map.values.is_empty());
                assert_eq!(map.position(FileId::from_bytes([0; 16]))?, None);
            }
        }
        cancellation.cancel();
        drop(map);
        assert_eq!(live(meter), baseline);
        drop(original);
        Ok(())
    }

    for allowance in 0..=5 {
        let cancellation = CancellationToken::new();
        let budget = WorkBudget {
            allocation_operations: allowance + 3,
            ..WorkBudget::UNBOUNDED
        };
        let meter = ProofMeter::standalone(budget, &cancellation)?;
        attempt(&meter, &cancellation)?;
        #[cfg(not(target_arch = "wasm32"))]
        {
            let cancellation = CancellationToken::new();
            let original = crate::NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(error, _)| error)?;
            let meter = ProofMeter::Native {
                original: &original,
                cancellation: &cancellation,
            };
            attempt(&meter, &cancellation)?;
        }
    }
    Ok(())
}

fn denied(meter: &ProofMeter<'_>) {
    let mut cache = CachedProofs::default();
    let mut result = None;
    let allocation = acyclic_test_support::measure(|| {
        result = Some(cache.slot(crate::VolumeId::from_bytes([1; 16]), meter));
    });
    assert!(matches!(result, Some(Err(_))));
    assert_eq!(allocation.count_total, 0, "denied slot admission allocated");
    assert_eq!(allocation.bytes_total, 0);
    assert!(
        meter
            .with_accounting(|accounting| cache.entries.get([1; 16], accounting))
            .unwrap()
            .is_none()
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    #[test]
    fn denied_first_slot_admission_never_allocates(shortfall in 1u64..=64) {
        let cancellation = CancellationToken::new();
        let slot_bytes = usize_to_u64(size_of::<ProofSlot<RetainedArc<CachedProof>>>() + 2 * size_of::<usize>());
        let available = slot_bytes.saturating_sub(shortfall);
        let mut budget = WorkBudget::UNBOUNDED;
        budget.peak_allocation_bytes = proof_standalone_control_bytes() + available;
        let meter = ProofMeter::standalone(budget, &cancellation)
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
        denied(&meter);
    }
}

#[test]
fn denied_first_slot_allocation_operation_never_allocates() -> Result<(), Box<dyn std::error::Error>>
{
    let cancellation = CancellationToken::new();
    let mut budget = WorkBudget::UNBOUNDED;
    budget.allocation_operations = 1; // The standalone meter consumes this one.
    denied(&ProofMeter::standalone(budget, &cancellation)?);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn denied_native_first_slot_never_allocates() -> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let mut budget = WorkBudget::UNBOUNDED;
    budget.allocation_operations = 1; // The native ledger consumes this one.
    let original =
        crate::NativeMaterializationWork::new(budget, &cancellation).map_err(|(cause, _)| cause)?;
    // Read the existing ledger before probing a new volume. This also initializes
    // the debug lock-order observer's per-thread storage outside measurement.
    let before = original.receipt();
    assert_eq!(before.allocation_operations, 1);
    denied(&ProofMeter::Native {
        original: &original,
        cancellation: &cancellation,
    });
    assert_eq!(original.receipt(), before);
    Ok(())
}

#[test]
fn identity_tables_preserve_values_without_insertion_shifts()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let meter = ProofMeter::standalone(WorkBudget::UNBOUNDED, &cancellation)?;
    // Initialize shared standalone custody before taking the live baseline.
    meter.with_accounting(|accounting| {
        drop(crate::kernel::allocation::retained::RetainedAllocation::claim(0, 0, accounting)?);
        Ok(())
    })?;
    let live_before = meter.with_accounting(|accounting| {
        let (ledger, _, _, _) = accounting.standalone_parts().ok_or(WorkError::Overflow)?;
        Ok(ledger.live_bytes())
    })?;
    let baseline = meter.work();
    let count = 2048u64;
    let mut expected = std::collections::BTreeMap::new();
    let mut entries = ProofVec::new(&meter);
    let mut objects = ProofVec::new(&meter);
    // Multiplication permutes this power-of-two range; arrivals are neither
    // sorted nor reversed. The independent map supplies exact expected values.
    for arrival in 0..count {
        let number = (arrival * 109) % count;
        let mut bytes = [0; 16];
        bytes[..8].copy_from_slice(&number.to_le_bytes());
        let key = FileId::from_bytes(bytes);
        assert_eq!(
            map_insert(&mut entries, key, arrival)?,
            expected.insert(key, arrival)
        );
        let mut digest = [0; 32];
        digest[..8].copy_from_slice(&number.to_be_bytes());
        let object = ObjectId {
            storage: None,
            kind: ObjectKind::Blob,
            digest: crate::Digest::from_bytes(digest),
        };
        assert!(set_insert(&mut objects, object)?);
        assert!(!set_insert(&mut objects, object)?);
    }
    for (key, value) in &expected {
        assert_eq!(map_get(&entries, key)?, Some(value));
    }
    for (key, value) in &mut expected {
        assert_eq!(map_insert(&mut entries, *key, *value + 1)?, Some(*value));
        *value += 1;
        assert_eq!(map_get(&entries, key)?, Some(&*value));
    }
    assert_eq!(entries.values.len() as u64, count);
    assert_eq!(objects.values.len() as u64, count);
    // Vector storage grows geometrically. Its copy work does not include a
    // quadratic sorted-insertion suffix for each arriving identity.
    assert!(meter.work().bytes_copied < baseline.bytes_copied + count * 256);
    drop(entries);
    drop(objects);
    meter.with_accounting(|accounting| {
        let (ledger, _, _, _) = accounting.standalone_parts().ok_or(WorkError::Overflow)?;
        assert_eq!(ledger.live_bytes(), live_before);
        Ok(())
    })?;
    Ok(())
}
