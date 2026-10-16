use super::*;

pub(crate) fn hash_lazy_value<T: MemoryLazyNode>(
    value: &T,
    accounting: &mut KernelAccounting<'_>,
) -> Result<[u8; 32], LazyWorkspaceError> {
    #[cfg(not(target_arch = "wasm32"))]
    if let KernelAccounting::NativeOriginal { work, cancellation } = accounting {
        // These two SDK node types derive Serialize from scalar/string/byte
        // fields. No I/O, owner destruction or demand callback enters the leaf.
        return work.state(|state| {
            hash_with_charge(value, |delta| {
                state
                    .admit(WorkCounters::UNCHARGED)
                    .map_err(crate::materializer::native_materialization_work_error)?;
                cancellation.check()?;
                state
                    .charge(delta)
                    .map_err(crate::materializer::native_materialization_work_error)?;
                Ok(())
            })
        });
    }
    hash_with_charge(value, |delta| accounting.charge(delta))
}

fn hash_with_charge<T: Serialize>(
    value: &T,
    charge: impl FnMut(WorkCounters) -> Result<(), crate::FsError>,
) -> Result<[u8; 32], LazyWorkspaceError> {
    struct Sink<F> {
        hash: blake3::Hasher,
        charge: F,
        failed: Option<crate::FsError>,
    }
    impl<F: FnMut(WorkCounters) -> Result<(), crate::FsError>> std::io::Write for Sink<F> {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if let Err(error) = (self.charge)(WorkCounters {
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
        charge,
        failed: None,
    };
    let encoded = serde_json::to_writer(&mut sink, value);
    if let Some(error) = sink.failed {
        return Err(error.into());
    }
    encoded.map_err(|e| LazyWorkspaceError::Store(e.to_string()))?;
    Ok(*sink.hash.finalize().as_bytes())
}

#[cfg(all(test, not(acyclic_fs_loom), not(target_arch = "wasm32")))]
#[path = "hash_tests.rs"]
mod tests;

#[cfg(all(test, acyclic_fs_loom, not(target_arch = "wasm32")))]
#[path = "hash_loom_tests.rs"]
mod models;
