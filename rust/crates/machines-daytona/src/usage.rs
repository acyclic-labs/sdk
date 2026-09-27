//! Allocation-based usage estimate mapped to [`UsageReceipt`].
//!
//! Daytona exposes no per-sandbox consumption endpoint (only an organization-wide usage
//! overview), so this provider cannot read back what a sandbox was billed. Instead it reports
//! the sandbox's *allocation* integrated over the part of the interval the sandbox existed:
//! allocated vCPUs as CPU time, allocated memory as resident byte-seconds, and allocated disk as
//! durable bytes. That is an upper bound on compute (a paused or stopped sandbox is not
//! charged for CPU) and it carries no egress. Every receipt says so in its bytes
//! (`"basis": "allocation"`, `"provisional": true`) and must not be used as billing evidence.

use acyclic_machines::{MachineId, ProviderError, UsageReceipt};
use serde::Serialize;
use sha2::{Digest as _, Sha256};

/// Marker embedded in every receipt produced by this module.
pub const PROVISIONAL_MARKER: &str = "provisional";

const GIB: u128 = 1 << 30;

/// Allocation of one sandbox, as Daytona reports it.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub struct Allocation {
    /// Allocated vCPUs.
    pub cpu: u64,
    /// Allocated memory in GiB.
    pub memory_gib: u64,
    /// Allocated disk in GiB.
    pub disk_gib: u64,
}

/// Receipt payload serialized into [`UsageReceipt::receipt`].
#[derive(Debug, Serialize)]
struct ReceiptPayload<'a> {
    provider: &'static str,
    basis: &'static str,
    provisional: bool,
    sandbox_id: &'a str,
    start_unix_ms: u64,
    end_unix_ms: u64,
    overlap_ms: u64,
    allocation: Allocation,
}

fn saturate(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

/// Builds a provisional allocation-based receipt.
///
/// The sandbox is taken to exist from `created_at_unix_ms` until `now_unix_ms`; only the part
/// of `[start_unix_ms, end_unix_ms)` inside that span accrues.
///
/// # Errors
/// Returns [`ProviderError::Invalid`] for an empty interval or when the payload cannot be
/// encoded.
pub fn receipt(
    machine: MachineId,
    sandbox_id: &str,
    interval: (u64, u64),
    lifetime: (u64, u64),
    dedicated: bool,
    allocation: Allocation,
) -> Result<UsageReceipt, ProviderError> {
    let (start_unix_ms, end_unix_ms) = interval;
    if start_unix_ms >= end_unix_ms {
        return Err(ProviderError::Invalid(
            "usage interval must be non-empty".into(),
        ));
    }
    let (created_at_unix_ms, now_unix_ms) = lifetime;
    let overlap_ms = end_unix_ms
        .min(now_unix_ms)
        .saturating_sub(start_unix_ms.max(created_at_unix_ms));
    let overlap = u128::from(overlap_ms);
    let cpu_ns = saturate(u128::from(allocation.cpu) * overlap * 1_000_000);
    let memory_byte_seconds = saturate(u128::from(allocation.memory_gib) * GIB * overlap / 1_000);
    let disk_bytes = if overlap_ms == 0 {
        0
    } else {
        saturate(u128::from(allocation.disk_gib) * GIB)
    };
    let payload = ReceiptPayload {
        provider: "daytona",
        basis: "allocation",
        provisional: true,
        sandbox_id,
        start_unix_ms,
        end_unix_ms,
        overlap_ms,
        allocation,
    };
    let receipt = serde_json::to_vec(&payload).map_err(|error| {
        ProviderError::Invalid(format!("usage receipt cannot be encoded: {error}"))
    })?;
    // Daytona issues no signed lineage receipt; this commits to the estimate itself.
    let lineage_receipt_sha256: [u8; 32] = Sha256::digest(&receipt).into();
    Ok(UsageReceipt {
        machine,
        start_unix_ms,
        end_unix_ms,
        elastic_cpu_ns: if dedicated { 0 } else { cpu_ns },
        dedicated_cpu_ns: if dedicated { cpu_ns } else { 0 },
        private_resident_byte_seconds: memory_byte_seconds,
        durable_private_bytes: disk_bytes,
        lineage_receipt_sha256,
        egress_bytes: 0,
        receipt,
    })
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests {
    use super::*;

    const ALLOCATION: Allocation = Allocation {
        cpu: 2,
        memory_gib: 4,
        disk_gib: 10,
    };

    #[test]
    fn allocation_integrates_over_the_overlap_only() {
        let machine = MachineId::new();
        let start = 1_788_775_200_000;
        let end = start + 3_600_000;
        // Created half-way through the hour, still alive afterwards.
        let value = receipt(
            machine,
            "sb",
            (start, end),
            (start + 1_800_000, end + 1),
            false,
            ALLOCATION,
        )
        .unwrap();
        assert_eq!(value.machine, machine);
        assert_eq!(value.elastic_cpu_ns, 2 * 1_800 * 1_000_000_000);
        assert_eq!(value.dedicated_cpu_ns, 0);
        assert_eq!(value.private_resident_byte_seconds, 4 * 1_800 * (1 << 30));
        assert_eq!(value.durable_private_bytes, 10 * (1 << 30));
        assert_eq!(value.egress_bytes, 0);
        let payload: serde_json::Value = serde_json::from_slice(&value.receipt).unwrap();
        assert_eq!(payload[PROVISIONAL_MARKER], true);
        assert_eq!(payload["basis"], "allocation");
        assert_ne!(value.lineage_receipt_sha256, [0; 32]);
        let dedicated =
            receipt(machine, "sb", (start, end), (start, end), true, ALLOCATION).unwrap();
        assert_eq!(dedicated.dedicated_cpu_ns, 2 * 3_600 * 1_000_000_000);
        assert_eq!(dedicated.elastic_cpu_ns, 0);
    }

    #[test]
    fn intervals_outside_the_lifetime_accrue_nothing() {
        let machine = MachineId::new();
        let value = receipt(machine, "sb", (1, 2), (1_000, 2_000), false, ALLOCATION).unwrap();
        assert_eq!(value.elastic_cpu_ns, 0);
        assert_eq!(value.durable_private_bytes, 0);
        assert!(!value.receipt.is_empty());
        assert!(receipt(machine, "sb", (2, 1), (0, 3), false, ALLOCATION).is_err());
    }
}
