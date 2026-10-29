use super::*;
use proptest::prelude::*;

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
