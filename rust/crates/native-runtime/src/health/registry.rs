use super::FileHealth;
use crate::{FileIdentity, NativeFilePayload, NativeReadMeter, file_identity};
use std::{
    fs::File,
    io,
    sync::{Arc, Mutex, OnceLock, Weak, atomic::Ordering},
};

pub(crate) static FILE_HEALTH: OnceLock<Mutex<FileHealthRegistry>> = OnceLock::new();
enum Entry {
    Healthy(Weak<FileHealth>),
    Poisoned(Arc<FileHealth>),
}
impl Entry {
    fn owner(&self) -> Option<Arc<FileHealth>> {
        match self {
            Self::Healthy(health) => health.upgrade(),
            Self::Poisoned(health) => Some(Arc::clone(health)),
        }
    }
    fn live(&self) -> bool {
        match self {
            Self::Healthy(health) => health.strong_count() != 0,
            Self::Poisoned(_) => true,
        }
    }
}
pub(crate) struct FileHealthRegistry {
    entries: Vec<(FileIdentity, Entry)>,
    capacity_limit: usize,
}
impl FileHealthRegistry {
    #[allow(
        unsafe_code,
        reason = "the process resource-limit query initializes an rlimit output"
    )]
    pub(crate) fn new() -> Self {
        #[cfg(windows)]
        let capacity_limit = 4096;
        #[cfg(target_os = "linux")]
        let capacity_limit = {
            let mut limit = libc::rlimit {
                rlim_cur: 0,
                rlim_max: 0,
            };
            // SAFETY: limit is an initialized writable rlimit.
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
        Self {
            entries: Vec::new(),
            capacity_limit,
        }
    }
    fn find(
        &self,
        identity: FileIdentity,
        meter: Option<&dyn NativeReadMeter>,
    ) -> io::Result<Result<usize, usize>> {
        let (mut low, mut high) = (0, self.entries.len());
        while low < high {
            if let Some(meter) = meter {
                meter.examine(1, 0)?;
            }
            let middle = low + (high - low) / 2;
            match self
                .entries
                .get(middle)
                .ok_or(io::ErrorKind::InvalidData)?
                .0
                .cmp(&identity)
            {
                std::cmp::Ordering::Less => low = middle + 1,
                std::cmp::Ordering::Greater => high = middle,
                std::cmp::Ordering::Equal => return Ok(Ok(middle)),
            }
        }
        Ok(Err(low))
    }
    fn sweep_idle(&mut self, meter: Option<&dyn NativeReadMeter>) -> io::Result<()> {
        let (mut removed, mut shifted) = (0, 0);
        // Snapshot liveness once so concurrent owner drops cannot change admitted compaction.
        let mut live = [0_u64; 64];
        for (index, (_, entry)) in self.entries.iter().enumerate() {
            if let Some(meter) = meter {
                meter.examine(1, 0)?;
            }
            if entry.live() {
                *live.get_mut(index / 64).ok_or(io::ErrorKind::InvalidData)? |= 1 << (index % 64);
                if removed != 0 {
                    shifted += 1;
                }
            } else {
                removed += 1;
            }
        }
        if let Some(meter) = meter {
            meter.examine(
                self.entries.len() as u64,
                (shifted * std::mem::size_of::<(FileIdentity, Entry)>()) as u64,
            )?;
        }
        let mut index = 0;
        self.entries.retain(|_| {
            let keep = live
                .get(index / 64)
                .unwrap_or_else(|| std::process::abort())
                & (1 << (index % 64))
                != 0;
            index += 1;
            keep
        });
        Ok(())
    }
}

pub(crate) fn shared_file_health(
    file: &File,
    meter: Option<&dyn NativeReadMeter>,
) -> io::Result<Arc<FileHealth>> {
    let lock = match FILE_HEALTH.get() {
        Some(lock) => lock,
        None if meter.is_some() => return Err(io::ErrorKind::NotConnected.into()),
        None => FILE_HEALTH.get_or_init(|| Mutex::new(FileHealthRegistry::new())),
    };
    if let Some(meter) = meter {
        meter.admit_read(0)?;
    }
    let identity = file_identity(file)?;
    lock.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .admit(identity, meter)
}

impl FileHealthRegistry {
    /// Returns the identity's live owner, or registers a fresh one at its single search position.
    fn admit(
        &mut self,
        identity: FileIdentity,
        meter: Option<&dyn NativeReadMeter>,
    ) -> io::Result<Arc<FileHealth>> {
        let mut charged = 0;
        let result = self.admit_charged(identity, meter, &mut charged);
        if let Some(meter) = meter {
            meter.release(charged as u64);
        }
        result
    }
    fn admit_charged(
        &mut self,
        identity: FileIdentity,
        meter: Option<&dyn NativeReadMeter>,
        charged: &mut usize,
    ) -> io::Result<Arc<FileHealth>> {
        let row = std::mem::size_of::<(FileIdentity, Entry)>();
        let mut index = match self.find(identity, meter)? {
            Ok(index) => {
                let entry = self
                    .entries
                    .get_mut(index)
                    .ok_or(io::ErrorKind::InvalidData)?;
                if let Some(owner) = entry.1.owner() {
                    return Ok(owner);
                }
                // Only a dropped healthy owner expires: reuse its sorted slot without moving rows,
                // freeing the expired allocation before admitting its replacement.
                entry.1 = Entry::Healthy(Weak::new());
                let health = new_health(meter, charged)?;
                entry.1 = Entry::Healthy(Arc::downgrade(&health));
                return Ok(health);
            }
            Err(index) => index,
        };
        if self.entries.len() >= self.capacity_limit {
            let before = self.entries.len();
            self.sweep_idle(meter)?;
            if self.entries.len() != before {
                index = self.find(identity, meter)?.unwrap_or_else(|index| index);
            }
        }
        if self.entries.len() >= self.capacity_limit {
            return Err(io::Error::other("native file admission capacity exhausted"));
        }
        if self.entries.len() == self.entries.capacity() {
            let old = self.entries.capacity() * row;
            let next = self
                .entries
                .capacity()
                .saturating_mul(2)
                .max(1)
                .min(self.capacity_limit);
            let bytes = next * row;
            if let Some(meter) = meter {
                meter.admit_examination(0, (self.entries.len() * row) as u64)?;
                meter.adopt(old as u64)?;
                *charged += old;
                meter.allocate(bytes as u64)?;
                *charged += bytes;
            }
            let mut replacement = Vec::new();
            replacement
                .try_reserve_exact(next)
                .map_err(io::Error::other)?;
            // Appending preserves order, so the search position stays valid.
            replacement.append(&mut self.entries);
            self.entries = replacement;
            if let Some(meter) = meter {
                meter.release(old as u64);
                *charged -= old;
                meter.record_examination(0, (self.entries.len() * row) as u64)?;
                let actual = self.entries.capacity() * row;
                if actual > bytes {
                    meter.adopt((actual - bytes) as u64)?;
                    *charged += actual - bytes;
                }
            }
        }
        let health = new_health(meter, charged)?;
        if let Some(meter) = meter {
            meter.examine(0, ((self.entries.len() - index) * row) as u64)?;
        }
        self.entries
            .insert(index, (identity, Entry::Healthy(Arc::downgrade(&health))));
        Ok(health)
    }
}

fn new_health(
    meter: Option<&dyn NativeReadMeter>,
    charged: &mut usize,
) -> io::Result<Arc<FileHealth>> {
    if let Some(meter) = meter {
        meter.allocate(std::mem::size_of::<FileHealth>() as u64)?;
        *charged += std::mem::size_of::<FileHealth>();
    }
    Ok(Arc::new(FileHealth::default()))
}

pub(crate) fn poison_file_health(file: &Arc<NativeFilePayload>, health: &Arc<FileHealth>) {
    let identity = file_identity(file).unwrap_or_else(|_| std::process::abort());
    let lock = FILE_HEALTH.get().unwrap_or_else(|| std::process::abort());
    let mut registry = lock
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let index = registry
        .find(identity, None)
        .unwrap_or_else(|_| std::process::abort())
        .unwrap_or_else(|_| std::process::abort());
    let entry = registry
        .entries
        .get_mut(index)
        .unwrap_or_else(|| std::process::abort());
    let registered = entry.1.owner().unwrap_or_else(|| std::process::abort());
    if !Arc::ptr_eq(&registered, health) {
        std::process::abort();
    }
    *health
        .poison_anchor
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(file));
    entry.1 = Entry::Poisoned(registered);
    health.uncertain.store(true, Ordering::Release);
    tracing::debug!(name: "acyclic.runtime.file_uncertain", "file health poisoned");
}

#[cfg(test)]
#[path = "registry_tests.rs"]
mod tests;
