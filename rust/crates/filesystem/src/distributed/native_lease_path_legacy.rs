// Pre-PR encoder from bf727ef8959153f3ef81186f1d6928367fd7bda1. Keep independent.
use super::{OperationLeaseId, WorkspaceId};

/// Encodes a lease path, admitting and charging writes on the caller's ledger.
pub(crate) fn encode(
    workspace_id: WorkspaceId,
    lease_id: OperationLeaseId,
    expires_at_millis: u64,
    text: &mut [u8; acyclic_stream::MAX_PATH_BYTES],
    mut work: Option<&mut crate::AuthorityWork<'_>>,
) -> Result<usize, crate::AuthorityStoreError> {
    const PREFIX: &[u8] = b"fs/operation-windows-v1/";
    const SEPARATOR: &[u8] = b"/leases/";
    let workspace = workspace_id.into_bytes();
    let lease = lease_id.into_bytes();
    let mut digits = 1_usize;
    let mut number = expires_at_millis;
    while number >= 10 {
        digits += 1;
        number /= 10;
    }
    let length =
        PREFIX.len() + workspace.len() * 2 + SEPARATOR.len() + lease.len() * 2 + 1 + digits;
    let writes = crate::WorkCounters {
        bytes_encoded: length as u64,
        bytes_copied: (PREFIX.len() + SEPARATOR.len()) as u64,
        ..crate::WorkCounters::UNCHARGED
    };
    if let Some(work) = &mut work {
        work.admit(writes)?;
    }
    text[..PREFIX.len()].copy_from_slice(PREFIX);
    let mut cursor = PREFIX.len();
    hex::encode_to_slice(workspace, &mut text[cursor..cursor + workspace.len() * 2])
        .expect("fixed-width workspace lease path has its exact hex capacity");
    cursor += workspace.len() * 2;
    text[cursor..cursor + SEPARATOR.len()].copy_from_slice(SEPARATOR);
    cursor += SEPARATOR.len();
    hex::encode_to_slice(lease, &mut text[cursor..cursor + lease.len() * 2])
        .expect("fixed-width lease identity has its exact hex capacity");
    cursor += lease.len() * 2;
    text[cursor] = b'/';
    number = expires_at_millis;
    for position in (cursor + 1..length).rev() {
        text[position] = b'0' + (number % 10) as u8;
        number /= 10;
    }
    if let Some(work) = work {
        work.charge(writes)?;
    }
    Ok(length)
}
