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
    assert!(cache.entries.is_empty());
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    #[test]
    fn denied_first_slot_admission_never_allocates(shortfall in 1u64..=64) {
        let cancellation = CancellationToken::new();
        let slot_bytes = usize_to_u64(size_of::<ProofSlot<CachedProof>>() + 2 * size_of::<usize>());
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
