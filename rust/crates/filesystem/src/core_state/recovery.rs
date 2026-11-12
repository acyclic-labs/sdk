//! Native preparation paths live in the existing private control-plane namespace.
use super::*;
use crate::{NativeMaterializationWork, NativeRootIdentity, NativeTreeMaterializationError};

const FAMILY: &str = "native-preparation-v1";

impl LocalCoreStateStore {
    /// Selects the original operation's external staging path on its caller's
    /// allocation ledger. Materialization validates separation and the same
    /// filesystem before any checkout effects; the existing journal retains
    /// directory identities and recovery progress.
    pub fn native_preparation_directory(
        &self,
        identity: NativeRootIdentity,
        operation: OperationId,
        work: &NativeMaterializationWork,
    ) -> Result<(PathBuf, crate::NativeAllocation), NativeTreeMaterializationError> {
        let credit = crate::WorkCounters {
            bytes_encoded: 64,
            bytes_copied: (FAMILY.len() + 66) as u64,
            ..crate::WorkCounters::UNCHARGED
        };
        work.admit(credit)?;
        let mut name = [b'/'; FAMILY.len() + 66];
        name[..FAMILY.len()].copy_from_slice(FAMILY.as_bytes());
        encode_hex(
            identity.to_bytes(),
            &mut name[FAMILY.len() + 1..FAMILY.len() + 33],
        );
        encode_hex(operation.into_bytes(), &mut name[FAMILY.len() + 34..]);
        work.spent(credit)?;
        let name = std::str::from_utf8(&name)
            .map_err(|_| NativeTreeMaterializationError::InvalidLayout)?;
        work.path(self.root(), Some(Path::new(name)))
    }
}

fn encode_hex(bytes: [u8; 16], output: &mut [u8]) {
    for (byte, pair) in bytes.into_iter().zip(output.chunks_exact_mut(2)) {
        pair[0] = b"0123456789abcdef"[usize::from(byte >> 4)];
        pair[1] = b"0123456789abcdef"[usize::from(byte & 15)];
    }
}
