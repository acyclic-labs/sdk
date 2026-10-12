use super::*;

pub(crate) type FileSequencer = Arc<Mutex<Option<Arc<OperationFence>>>>;

#[cfg(windows)]
pub(crate) type FileIdentity = (u64, [u8; 16]);

#[cfg(any(target_os = "linux", target_vendor = "apple"))]
pub(crate) type FileIdentity = (u64, u64);

pub(crate) struct FileHealth {
    pub(crate) tail: FileSequencer,
    pub(crate) uncertain: Arc<AtomicBool>,
    pub(crate) poison_anchor: Mutex<Option<Arc<NativeFilePayload>>>,
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
pub(crate) static FILE_HEALTH: OnceLock<Mutex<FileHealthRegistry>> = OnceLock::new();

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
pub(crate) struct FileHealthRegistry {
    pub(crate) entries: Vec<(FileIdentity, Weak<FileHealth>, Option<Arc<FileHealth>>)>,
    pub(crate) capacity_limit: usize,
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
impl FileHealthRegistry {
    pub(crate) fn new() -> Self {
        #[cfg(target_os = "linux")]
        let capacity_limit = {
            let mut limit = libc::rlimit {
                rlim_cur: 0,
                rlim_max: 0,
            };
            // SAFETY: limit is a writable rlimit structure.
            #[allow(unsafe_code)]
            if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0 {
                128
            } else {
                usize::try_from(limit.rlim_cur)
                    .unwrap_or(usize::MAX)
                    .saturating_div(4)
                    .clamp(1, 4096)
            }
        };
        #[cfg(target_vendor = "apple")]
        let capacity_limit = usize::try_from(
            rustix::process::getrlimit(rustix::process::Resource::Nofile)
                .current
                .unwrap_or(u64::MAX),
        )
        .unwrap_or(usize::MAX)
        .saturating_div(4)
        .clamp(1, 4096);
        #[cfg(windows)]
        let capacity_limit = 4096;
        Self {
            entries: Vec::new(),
            capacity_limit,
        }
    }

    pub(crate) fn find(
        &self,
        identity: FileIdentity,
        meter: Option<&dyn NativeReadMeter>,
    ) -> io::Result<Result<usize, usize>> {
        let mut low = 0;
        let mut high = self.entries.len();
        while low < high {
            if let Some(meter) = meter {
                meter.examine(1, 0)?;
            }
            let middle = low + (high - low) / 2;
            match self.entries[middle].0.cmp(&identity) {
                std::cmp::Ordering::Less => low = middle + 1,
                std::cmp::Ordering::Greater => high = middle,
                std::cmp::Ordering::Equal => return Ok(Ok(middle)),
            }
        }
        Ok(Err(low))
    }

    pub(crate) fn sweep_idle(&mut self, meter: Option<&dyn NativeReadMeter>) -> io::Result<()> {
        let mut removed = 0;
        let mut shifted = 0;
        // Snapshot liveness once so concurrent drops cannot change admitted compaction.
        let mut live = [0_u64; 64];
        for (index, (_, health, _)) in self.entries.iter().enumerate() {
            if let Some(meter) = meter {
                meter.examine(1, 0)?;
            }
            if health.strong_count() == 0 {
                removed += 1;
            } else {
                live[index / 64] |= 1 << (index % 64);
                if removed != 0 {
                    shifted += 1;
                }
            }
        }
        if let Some(meter) = meter {
            meter.examine(
                self.entries.len() as u64,
                (shifted * std::mem::size_of::<(FileIdentity, Weak<FileHealth>, Option<Arc<FileHealth>>)>()) as u64,
            )?;
        }
        let mut index = 0;
        self.entries.retain(|_| {
            let keep = live[index / 64] & (1 << (index % 64)) != 0;
            index += 1;
            keep
        });
        Ok(())
    }
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
pub(crate) fn file_identity(file: &File) -> io::Result<FileIdentity> {
    #[cfg(windows)]
    return windows::file_identity(file);
    #[cfg(any(target_os = "linux", target_vendor = "apple"))]
    {
        use std::os::unix::fs::MetadataExt as _;
        let metadata = file.metadata()?;
        Ok((metadata.dev(), metadata.ino()))
    }
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
pub(crate) fn shared_file_health(
    file: &File,
    meter: Option<&dyn NativeReadMeter>,
) -> io::Result<Arc<FileHealth>> {
    let registry_lock = match FILE_HEALTH.get() {
        Some(registry) => registry,
        None if meter.is_some() => return Err(io::ErrorKind::NotConnected.into()),
        None => FILE_HEALTH.get_or_init(|| Mutex::new(FileHealthRegistry::new())),
    };
    if let Some(meter) = meter {
        meter.admit_read(0)?;
    }
    let identity = file_identity(file)?;
    let mut registry = registry_lock
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let old_bytes = registry.entries.capacity()
        * std::mem::size_of::<(FileIdentity, Weak<FileHealth>, Option<Arc<FileHealth>>)>();
    let mut retained_registry_bytes = 0;
    let mut pending_registry_bytes = 0;
    let mut pending_health_bytes = 0usize;
    let result = (|| {
        if let Ok(index) = registry.find(identity, meter)? {
            if let Some(health) = registry.entries[index].1.upgrade() {
                return Ok(health);
            }
            if let Some(meter) = meter {
                meter.examine(
                    0,
                    ((registry.entries.len() - index - 1)
                        * std::mem::size_of::<(
                            FileIdentity,
                            Weak<FileHealth>,
                            Option<Arc<FileHealth>>,
                        )>()) as u64,
                )?;
            }
            registry.entries.remove(index);
        }
        let capacity = registry.capacity_limit;
        if registry.entries.len() >= capacity {
            registry.sweep_idle(meter)?;
        }
        if registry.entries.len() >= capacity {
            return Err(io::Error::other("native file admission capacity exhausted"));
        }
        if registry.entries.len() == registry.entries.capacity() {
            let next = registry
                .entries
                .capacity()
                .saturating_mul(2)
                .max(1)
                .min(capacity);
            let next_bytes = next
                * std::mem::size_of::<(FileIdentity, Weak<FileHealth>, Option<Arc<FileHealth>>)>();
            if let Some(meter) = meter {
                meter.admit_examination(
                    0,
                    (registry.entries.len()
                        * std::mem::size_of::<(
                            FileIdentity,
                            Weak<FileHealth>,
                            Option<Arc<FileHealth>>,
                        )>()) as u64,
                )?;
                meter.adopt(old_bytes as u64)?;
                retained_registry_bytes = old_bytes;
                meter.allocate(next_bytes as u64)?;
                pending_registry_bytes = next_bytes;
            }
            let mut replacement = Vec::new();
            replacement
                .try_reserve_exact(next)
                .map_err(io::Error::other)?;
            replacement.append(&mut registry.entries);
            registry.entries = replacement;
            if let Some(meter) = meter {
                meter.release(old_bytes as u64);
            }
            if let Some(meter) = meter {
                retained_registry_bytes = next_bytes;
                pending_registry_bytes = 0;
                meter.record_examination(
                    0,
                    (registry.entries.len()
                        * std::mem::size_of::<(
                            FileIdentity,
                            Weak<FileHealth>,
                            Option<Arc<FileHealth>>,
                        )>()) as u64,
                )?;
                let actual_bytes = registry.entries.capacity()
                    * std::mem::size_of::<(FileIdentity, Weak<FileHealth>, Option<Arc<FileHealth>>)>(
                    );
                if actual_bytes > next_bytes {
                    meter.adopt((actual_bytes - next_bytes) as u64)?;
                    retained_registry_bytes = actual_bytes;
                }
            }
        }
        let index = registry
            .find(identity, meter)?
            .unwrap_or_else(|index| index);
        if let Some(meter) = meter {
            meter.allocate(std::mem::size_of::<Mutex<Option<Arc<OperationFence>>>>() as u64)?;
            pending_health_bytes += std::mem::size_of::<Mutex<Option<Arc<OperationFence>>>>();
        }
        let tail = Arc::new(Mutex::new(None));
        if let Some(meter) = meter {
            meter.allocate(std::mem::size_of::<AtomicBool>() as u64)?;
            pending_health_bytes += std::mem::size_of::<AtomicBool>();
        }
        let uncertain = Arc::new(AtomicBool::new(false));
        if let Some(meter) = meter {
            meter.allocate(std::mem::size_of::<FileHealth>() as u64)?;
            pending_health_bytes += std::mem::size_of::<FileHealth>();
        }
        let health = Arc::new(FileHealth {
            tail,
            uncertain,
            poison_anchor: Mutex::new(None),
        });
        if let Some(meter) = meter {
            meter.examine(
                0,
                ((registry.entries.len() - index)
                    * std::mem::size_of::<(FileIdentity, Weak<FileHealth>, Option<Arc<FileHealth>>)>())
                    as u64,
            )?;
        }
        registry
            .entries
            .insert(index, (identity, Arc::downgrade(&health), None));
        Ok(health)
    })();
    if let Some(meter) = meter {
        meter.release((retained_registry_bytes + pending_registry_bytes) as u64);
        meter.release(pending_health_bytes as u64);
    }
    result
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
pub(crate) fn poison_file_health(file: &Arc<NativeFilePayload>, uncertain: &Arc<AtomicBool>) {
    let identity = file_identity(file).unwrap_or_else(|_| std::process::abort());
    let registry_lock = FILE_HEALTH.get().unwrap_or_else(|| std::process::abort());
    let mut registry = registry_lock
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let index = registry
        .find(identity, None)
        .unwrap_or_else(|_| std::process::abort())
        .unwrap_or_else(|_| std::process::abort());
    let health = registry.entries[index]
        .1
        .upgrade()
        .unwrap_or_else(|| std::process::abort());
    if !Arc::ptr_eq(&health.uncertain, uncertain) {
        std::process::abort();
    }
    *health
        .poison_anchor
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(file));
    registry.entries[index].2 = Some(health);
    uncertain.store(true, Ordering::Release);
    tracing::debug!(name: "acyclic.runtime.file_uncertain", "file health poisoned");
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
pub(crate) fn poison_borrowed_file_health(file: &File, uncertain: &Arc<AtomicBool>) {
    let anchor = Arc::new(NativeFilePayload {
        file: file.try_clone().unwrap_or_else(|_| std::process::abort()),
        _allocation: None,
    });
    poison_file_health(&anchor, uncertain);
}

#[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
pub(crate) fn with_file_admission<T>(
    _: &File,
    operation: impl FnOnce() -> io::Result<T>,
) -> io::Result<T> {
    operation()
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
pub(crate) fn with_file_admission<T>(
    file: &File,
    operation: impl FnOnce() -> io::Result<T>,
) -> io::Result<T> {
    let health = shared_file_health(file, None)?;
    let completion = if in_file_scope(&health.tail) {
        None
    } else {
        let (predecessor, fence) = {
            let mut tail = health
                .tail
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let predecessor = tail.clone();
            let fence = Arc::new(OperationFence::default());
            *tail = Some(Arc::clone(&fence));
            (predecessor, fence)
        };
        if let Some(predecessor) = predecessor {
            predecessor.wait_until_complete();
        }
        Some(fence)
    };
    let result = if health.uncertain.load(Ordering::Acquire) {
        Err(io::Error::other(
            "prior native I/O completion on this file is uncertain",
        ))
    } else {
        with_file_scope(Some(&health.tail), || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(operation))
                .unwrap_or_else(|_| std::process::abort());
            if result.as_ref().err().is_some_and(is_uncertain_io_error) {
                poison_borrowed_file_health(file, &health.uncertain);
            }
            result
        })
    };
    if let Some(completion) = completion {
        completion.complete();
    }
    result
}
