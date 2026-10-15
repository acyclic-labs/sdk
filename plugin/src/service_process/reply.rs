//! Service replies charged to the original query context.

use super::*;

/// Encodes the bounded reply on the caller's existing native ledger.
pub(crate) fn encode_native_control_value<T: serde::Serialize>(
    value: &T,
    work: &acyclic_fs::NativeMaterializationWork,
) -> acyclic_harness::Result<(Vec<u8>, acyclic_fs::NativeAllocation)> {
    pi_root_provider::guarded_pi_bytes(value, (MAXIMUM_CONTROL_MESSAGE_BYTES - 1) as u64, work)
}
