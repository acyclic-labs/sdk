use super::{OperationLeaseId, WorkspaceId};

mod encoding;
pub(crate) use encoding::MAX_ENCODED_BYTES;
const _: () = assert!(MAX_ENCODED_BYTES <= acyclic_stream::MAX_PATH_BYTES);

/// Encodes a lease path, admitting and charging writes on the caller's ledger.
pub(crate) fn encode<const N: usize>(
    workspace_id: WorkspaceId,
    lease_id: OperationLeaseId,
    expires_at_millis: u64,
    text: &mut [u8; N],
    mut work: Option<&mut crate::AuthorityWork<'_>>,
) -> Result<usize, crate::AuthorityStoreError> {
    const { assert!(N >= MAX_ENCODED_BYTES) };
    let length = encoding::length(expires_at_millis);
    let writes = crate::WorkCounters {
        bytes_encoded: length as u64,
        bytes_copied: (encoding::PREFIX.len() + encoding::SEPARATOR.len()) as u64,
        ..crate::WorkCounters::UNCHARGED
    };
    if let Some(work) = &mut work {
        work.admit(writes)?;
    }
    encoding::write(
        workspace_id.into_bytes(),
        lease_id.into_bytes(),
        expires_at_millis,
        length,
        text,
    );
    if let Some(work) = work {
        work.charge(writes)?;
    }
    Ok(length)
}
