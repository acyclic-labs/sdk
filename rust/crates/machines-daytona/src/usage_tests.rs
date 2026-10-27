use acyclic_machines::MachineId;
use acyclic_machines::provider_core::{Allocation, PROVISIONAL_MARKER, allocation_receipt};

const ALLOCATION: Allocation = Allocation {
    cpu: 2,
    memory_gib: 4,
    disk_gib: 10,
};

#[test]
fn allocation_integrates_over_the_overlap_only() -> Result<(), Box<dyn std::error::Error>> {
    let machine = MachineId::new();
    let start = 1_788_775_200_000;
    let end = start + 3_600_000;
    // Created half-way through the hour, still alive afterwards.
    let value = allocation_receipt(
        "daytona",
        machine,
        "sb",
        (start, end),
        (start + 1_800_000, end + 1),
        false,
        ALLOCATION,
    )?;
    assert_eq!(value.machine, machine);
    assert_eq!(value.elastic_cpu_ns, 2 * 1_800 * 1_000_000_000);
    assert_eq!(value.dedicated_cpu_ns, 0);
    assert_eq!(value.private_resident_byte_seconds, 4 * 1_800 * (1 << 30));
    assert_eq!(value.durable_private_bytes, 10 * (1 << 30));
    assert_eq!(value.egress_bytes, 0);
    let payload: serde_json::Value = serde_json::from_slice(&value.receipt)?;
    assert_eq!(payload[PROVISIONAL_MARKER], true);
    assert_eq!(payload["basis"], "allocation");
    assert_ne!(value.lineage_receipt_sha256, [0; 32]);
    let dedicated = allocation_receipt(
        "daytona",
        machine,
        "sb",
        (start, end),
        (start, end),
        true,
        ALLOCATION,
    )?;
    assert_eq!(dedicated.dedicated_cpu_ns, 2 * 3_600 * 1_000_000_000);
    assert_eq!(dedicated.elastic_cpu_ns, 0);
    Ok(())
}

#[test]
fn intervals_outside_the_lifetime_accrue_nothing() -> Result<(), Box<dyn std::error::Error>> {
    let machine = MachineId::new();
    let value = allocation_receipt(
        "daytona",
        machine,
        "sb",
        (1, 2),
        (1_000, 2_000),
        false,
        ALLOCATION,
    )?;
    assert_eq!(value.elastic_cpu_ns, 0);
    assert_eq!(value.durable_private_bytes, 0);
    assert!(!value.receipt.is_empty());
    assert!(
        allocation_receipt("daytona", machine, "sb", (2, 1), (0, 3), false, ALLOCATION).is_err()
    );
    Ok(())
}
