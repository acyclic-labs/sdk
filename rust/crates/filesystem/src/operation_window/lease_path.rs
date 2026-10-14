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
    let invalid = || crate::AuthorityStoreError::RejectedStatic("invalid lease path capacity");
    let text = text.get_mut(..length).ok_or_else(invalid)?;
    let (prefix, rest) = text
        .split_at_mut_checked(PREFIX.len())
        .ok_or_else(invalid)?;
    prefix.copy_from_slice(PREFIX);
    let (workspace_text, rest) = rest
        .split_at_mut_checked(workspace.len() * 2)
        .ok_or_else(invalid)?;
    hex::encode_to_slice(workspace, workspace_text).map_err(|_| invalid())?;
    let (separator, rest) = rest
        .split_at_mut_checked(SEPARATOR.len())
        .ok_or_else(invalid)?;
    separator.copy_from_slice(SEPARATOR);
    let (lease_text, rest) = rest
        .split_at_mut_checked(lease.len() * 2)
        .ok_or_else(invalid)?;
    hex::encode_to_slice(lease, lease_text).map_err(|_| invalid())?;
    let (slash, digits) = rest.split_first_mut().ok_or_else(invalid)?;
    *slash = b'/';
    number = expires_at_millis;
    for digit in digits.iter_mut().rev() {
        *digit = b'0' + (number % 10) as u8;
        number /= 10;
    }
    if let Some(work) = work {
        work.charge(writes)?;
    }
    Ok(length)
}
