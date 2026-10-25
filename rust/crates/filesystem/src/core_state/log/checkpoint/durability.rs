//! A checkpoint may discard its WAL only after all staged records are durable.
use super::*;

pub(super) struct CheckpointDurability<'a> {
    #[cfg(target_os = "macos")]
    anchor: &'a File,
    #[cfg(target_os = "macos")]
    device: u64,
    #[cfg(not(target_os = "macos"))]
    _lifetime: PhantomData<&'a File>,
}

// Only finish can construct the permission consumed by WAL reset.
pub(super) struct CheckpointDurable(());

impl<'a> CheckpointDurability<'a> {
    pub(super) fn new(
        anchor: &'a File,
        work: Option<&crate::NativeMaterializationWork>,
    ) -> Result<Self, LocalCoreStateStoreError> {
        #[cfg(target_os = "macos")]
        return Ok(Self {
            anchor,
            device: device(anchor, work)?,
        });
        #[cfg(not(target_os = "macos"))]
        {
            let _ = (anchor, work);
            Ok(Self {
                _lifetime: PhantomData,
            })
        }
    }

    pub(super) fn file(
        &self,
        file: &File,
        work: Option<&crate::NativeMaterializationWork>,
    ) -> Result<(), LocalCoreStateStoreError> {
        #[cfg(target_os = "macos")]
        return self.stage(file, work);
        #[cfg(not(target_os = "macos"))]
        {
            if let Some(work) = work {
                work.charge(crate::WorkCounters {
                    durability_operations: 1,
                    ..Default::default()
                })?;
            }
            acyclic_native_runtime::flush_file_all(file)?;
            Ok(())
        }
    }

    pub(super) fn directory(
        &self,
        directory: &cap_std::fs::Dir,
        work: Option<&crate::NativeMaterializationWork>,
    ) -> Result<(), LocalCoreStateStoreError> {
        #[cfg(target_os = "macos")]
        return self.stage(directory, work);
        #[cfg(not(target_os = "macos"))]
        {
            match work {
                Some(work) => crate::native_host::sync_directory_held(directory, &mut &*work)?,
                None => ordinary_checkpoint(|work| {
                    crate::native_host::sync_directory_held(directory, work)
                })?,
            }
            Ok(())
        }
    }

    pub(super) fn finish(
        self,
        work: Option<&crate::NativeMaterializationWork>,
    ) -> Result<CheckpointDurable, LocalCoreStateStoreError> {
        #[cfg(target_os = "macos")]
        attempt(work, DURABILITY, || {
            operation("file:sync_checkpoint_batch", || {
                acyclic_native_runtime::sync_descriptor_once(std::os::fd::AsFd::as_fd(self.anchor))
            })
        })?;
        #[cfg(not(target_os = "macos"))]
        let _ = work;
        Ok(CheckpointDurable(()))
    }

    #[cfg(target_os = "macos")]
    fn stage(
        &self,
        descriptor: &impl std::os::fd::AsFd,
        work: Option<&crate::NativeMaterializationWork>,
    ) -> Result<(), LocalCoreStateStoreError> {
        // Held descriptors, rather than paths or the namespace's nominal
        // volume, decide which device drain covers this actual write.
        if device(descriptor, work)? != self.device {
            return attempt(work, DURABILITY, || {
                operation("file:sync_all", || {
                    acyclic_native_runtime::sync_descriptor_once(descriptor.as_fd())
                })
            });
        }
        attempt(work, DURABILITY, || {
            operation("file:stage_checkpoint", || {
                rustix::fs::fsync(descriptor).map_err(std::io::Error::from)
            })
        })
    }
}

#[cfg(target_os = "macos")]
const DURABILITY: crate::WorkCounters = crate::WorkCounters {
    durability_operations: 1,
    ..crate::WorkCounters::UNCHARGED
};

#[cfg(target_os = "macos")]
fn device(
    descriptor: &impl std::os::fd::AsFd,
    work: Option<&crate::NativeMaterializationWork>,
) -> Result<u64, LocalCoreStateStoreError> {
    attempt(
        work,
        crate::WorkCounters {
            backend_read_operations: 1,
            ..crate::WorkCounters::UNCHARGED
        },
        || {
            rustix::fs::fstat(descriptor)
                .map(|stat| i64::from(stat.st_dev).cast_unsigned())
                .map_err(std::io::Error::from)
        },
    )
}

#[cfg(target_os = "macos")]
fn attempt<T>(
    work: Option<&crate::NativeMaterializationWork>,
    cost: crate::WorkCounters,
    mut operation: impl FnMut() -> std::io::Result<T>,
) -> Result<T, LocalCoreStateStoreError> {
    loop {
        if let Some(work) = work {
            native_records::check(work, work.cancellation())?;
            work.charge(cost)?;
        }
        match operation() {
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            result => return result.map_err(Into::into),
        }
    }
}

#[cfg(target_os = "macos")]
fn operation<T>(
    name: &'static str,
    operation: impl FnOnce() -> std::io::Result<T>,
) -> std::io::Result<T> {
    #[cfg(any(test, feature = "test-support"))]
    return acyclic_native_runtime::testing::durable_operation(name, operation);
    #[cfg(not(any(test, feature = "test-support")))]
    {
        let _ = name;
        operation()
    }
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    use super::*;

    fn finite_budget() -> crate::WorkBudget {
        let mut budget = serde_json::to_value(crate::WorkBudget::UNBOUNDED).unwrap();
        for value in budget.as_object_mut().unwrap().values_mut() {
            *value = serde_json::Value::from(100_000_000_u64);
        }
        serde_json::from_value(budget).unwrap()
    }

    #[test]
    fn every_interrupted_attempt_is_prepaid_and_refusal_precedes_io() {
        for interrupted in 0..8_u64 {
            for allowed in 0..=interrupted + 1 {
                let cancellation = crate::CancellationToken::new();
                let budget = crate::WorkBudget {
                    durability_operations: allowed,
                    ..finite_budget()
                };
                let work = crate::NativeMaterializationWork::new(budget, &cancellation).unwrap();
                let mut calls = 0;
                let result = attempt(Some(&work), DURABILITY, || {
                    calls += 1;
                    if calls <= interrupted {
                        Err(std::io::ErrorKind::Interrupted.into())
                    } else {
                        Ok(())
                    }
                });
                assert_eq!(calls, allowed.min(interrupted + 1));
                assert_eq!(result.is_ok(), allowed > interrupted);
                assert_eq!(work.receipt().durability_operations, calls);
                work.receipt().verify(budget).unwrap();
            }
        }
    }

    #[test]
    fn cancellation_after_an_interruption_prevents_the_next_native_attempt() {
        let cancellation = crate::CancellationToken::new();
        let work = crate::NativeMaterializationWork::new(finite_budget(), &cancellation).unwrap();
        let mut calls = 0;
        let result: Result<(), _> = attempt(Some(&work), DURABILITY, || {
            calls += 1;
            cancellation.cancel();
            Err(std::io::ErrorKind::Interrupted.into())
        });
        assert!(result.is_err());
        assert_eq!(calls, 1);
        assert_eq!(work.receipt().durability_operations, 1);
    }

    #[test]
    fn another_device_requires_its_own_full_flush_and_preserves_its_error() {
        let directory = tempfile::tempdir().unwrap();
        let anchor = File::create(directory.path().join("wal")).unwrap();
        let other = File::open("/dev/null").unwrap();
        let durability = CheckpointDurability::new(&anchor, None).unwrap();
        assert_ne!(device(&other, None).unwrap(), durability.device);
        let LocalCoreStateStoreError::Io(cause) = durability.stage(&other, None).unwrap_err()
        else {
            panic!("a foreign-device flush lost its native cause");
        };
        assert_eq!(cause.raw_os_error(), Some(libc::ENODEV));
        assert_ne!(
            rustix::fs::fsync(&other)
                .err()
                .map(|error| error.raw_os_error()),
            Some(libc::ENODEV),
            "bare fsync must fail the full-flush error-preservation detector"
        );
    }
}
