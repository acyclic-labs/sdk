use super::*;
use crate::kernel::KernelAccounting;

pub(crate) fn hash_lazy_value<T: Serialize>(
    value: &T,
    accounting: &mut KernelAccounting<'_>,
) -> Result<[u8; 32], LazyWorkspaceError> {
    struct Sink<'a, 'b> {
        hash: blake3::Hasher,
        accounting: &'a mut KernelAccounting<'b>,
        failed: Option<crate::FsError>,
    }
    impl std::io::Write for Sink<'_, '_> {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if let Err(error) = self.accounting.charge(WorkCounters {
                bytes_hashed: bytes.len() as u64,
                ..WorkCounters::UNCHARGED
            }) {
                self.failed = Some(error);
                return Err(std::io::Error::other("lazy hash admission failed"));
            }
            self.hash.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut sink = Sink {
        hash: blake3::Hasher::new(),
        accounting,
        failed: None,
    };
    let encoded = serde_json::to_writer(&mut sink, value);
    if let Some(error) = sink.failed {
        return Err(error.into());
    }
    encoded.map_err(|e| LazyWorkspaceError::Store(e.to_string()))?;
    Ok(*sink.hash.finalize().as_bytes())
}
