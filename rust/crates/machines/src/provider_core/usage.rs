//! Shared provisional allocation estimates. These receipts are not billing evidence.

use crate::{MachineId, ProviderError, UsageReceipt};
use serde::Serialize;
use sha2::{Digest as _, Sha256};

/// Marker embedded in every receipt produced by this module.
pub const PROVISIONAL_MARKER: &str = "provisional";

const GIB: u128 = 1 << 30;

/// Allocation of one sandbox, as the adapter reports it.
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
    provider: &'a str,
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
pub fn allocation_receipt(
    provider: &str,
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
        provider,
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
    // This commits to the explicitly provisional estimate, not a signed meter reading.
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
