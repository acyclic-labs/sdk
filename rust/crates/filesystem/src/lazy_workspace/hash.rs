use super::*;
use crate::kernel::KernelAccounting;

pub(crate) fn hash_lazy_value<T: Serialize>(
    value: &T,
    accounting: &mut KernelAccounting<'_>,
) -> Result<[u8; 32], LazyWorkspaceError> {
    struct Writer<'a, 'b> {
        hash: blake3::Hasher,
        accounting: &'a mut KernelAccounting<'b>,
        error: Option<crate::FsError>,
    }
    impl std::io::Write for Writer<'_, '_> {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let work = WorkCounters {
                bytes_hashed: bytes.len() as u64,
                ..WorkCounters::UNCHARGED
            };
            let result = (|| {
                self.accounting.admit(work)?;
                self.hash.update(bytes);
                self.accounting.record_completed(work)
            })();
            if let Err(error) = result {
                self.error = Some(error);
                return Err(std::io::Error::other("lazy hash admission failed"));
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Writer {
        hash: blake3::Hasher::new(),
        accounting,
        error: None,
    };
    let encoded = serde_json::to_writer(&mut writer, value);
    if let Some(error) = writer.error {
        return Err(error.into());
    }
    encoded.map_err(|error| LazyWorkspaceError::Store(error.to_string()))?;
    Ok(*writer.hash.finalize().as_bytes())
}
