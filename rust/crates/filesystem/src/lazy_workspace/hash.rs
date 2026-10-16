use super::*;

pub(crate) fn hash_lazy_value<T: MemoryLazyNode>(
    value: &T,
    accounting: &mut KernelAccounting<'_>,
) -> Result<[u8; 32], LazyWorkspaceError> {
    #[cfg(not(target_arch = "wasm32"))]
    if let KernelAccounting::NativeOriginal { work, cancellation } = accounting {
        // These two SDK node types derive Serialize from scalar/string/byte
        // fields. No I/O, owner destruction or demand callback enters the leaf.
        return work.state(|state| hash_with_charge(value, Charge::Native(state, cancellation)));
    }
    hash_with_charge(value, Charge::Accounting(accounting))
}

// Keep one serializer instantiation per node type for both charging paths.
enum Charge<'a, 'b> {
    Accounting(&'a mut KernelAccounting<'b>),
    #[cfg(not(target_arch = "wasm32"))]
    Native(&'a mut crate::demand::DemandWork, &'a CancellationToken),
}
impl Charge<'_, '_> {
    fn charge(&mut self, delta: WorkCounters) -> Result<(), crate::FsError> {
        match self {
            Self::Accounting(accounting) => accounting.charge(delta),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native(state, cancellation) => {
                state
                    .admit(WorkCounters::UNCHARGED)
                    .map_err(crate::materializer::native_materialization_work_error)?;
                cancellation.check()?;
                state
                    .charge(delta)
                    .map_err(crate::materializer::native_materialization_work_error)?;
                Ok(())
            }
        }
    }
}

fn hash_with_charge<T: Serialize>(
    value: &T,
    charge: Charge<'_, '_>,
) -> Result<[u8; 32], LazyWorkspaceError> {
    struct Sink<'a, 'b> {
        hash: blake3::Hasher,
        charge: Charge<'a, 'b>,
        failed: Option<crate::FsError>,
    }
    impl std::io::Write for Sink<'_, '_> {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if let Err(error) = self.charge.charge(WorkCounters {
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
