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
    /// Whether this is the empty owner a sweep leaves on an expired row.
    fn swept(&self) -> bool {
        matches!(self, Self::Healthy(health) if Weak::ptr_eq(health, &Weak::new()))
    }
    fn live(&self) -> bool {
        match self {
            Self::Healthy(health) => health.strong_count() != 0,
            Self::Poisoned(_) => true,
        }
    }
}
/// Rows sorted by identity. A row's owner holds the file open (a poisoned
/// row anchors it), so live rows are bounded by the process's descriptors
/// and the registry needs no limit of its own. Expired rows are swept once
/// the table reaches twice its size after the previous sweep, so sweeping
/// stays amortized constant work per admission.
pub(crate) struct FileHealthRegistry {
    entries: Vec<(FileIdentity, Entry)>,
    sweep_at: usize,
}
/// Rows admitted before the first sweep.
const FIRST_SWEEP: usize = 64;
impl FileHealthRegistry {
    pub(crate) const fn new() -> Self {
        Self {
            entries: Vec::new(),
            sweep_at: FIRST_SWEEP,
        }
    }
    pub(crate) fn find(
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
        // Liveness is decided once per row: an expired row becomes the empty
        // owner, which no concurrent drop can change, so the compaction is
        // exactly the admitted one.
        for (_, entry) in &mut self.entries {
            if let Some(meter) = meter {
                meter.examine(1, 0)?;
            }
            if entry.live() {
                if removed != 0 {
                    shifted += 1;
                }
            } else {
                *entry = Entry::Healthy(Weak::new());
                removed += 1;
            }
        }
        if let Some(meter) = meter {
            meter.examine(
                self.entries.len() as u64,
                (shifted * std::mem::size_of::<(FileIdentity, Entry)>()) as u64,
            )?;
        }
        self.entries.retain(|(_, entry)| !entry.swept());
        Ok(())
    }
}

pub(crate) fn shared_file_health(
    file: &File,
    meter: Option<&dyn NativeReadMeter>,
) -> io::Result<(FileIdentity, Arc<FileHealth>)> {
    let lock = match FILE_HEALTH.get() {
        Some(lock) => lock,
        None if meter.is_some() => return Err(io::ErrorKind::NotConnected.into()),
        None => FILE_HEALTH.get_or_init(|| Mutex::new(FileHealthRegistry::new())),
    };
    if let Some(meter) = meter {
        meter.admit_read(0)?;
    }
    let identity = file_identity(file)?;
    let health = lock
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .admit(identity, meter)?;
    Ok((identity, health))
}

/// Unregisters `health` when the caller holds its last owner, so dropping
/// that owner frees it on the caller's thread. A healthy row's `Weak` would
/// otherwise keep the allocation until a later admission or sweep, on any
/// thread, replaced the row. Owners are created and upgraded from rows only
/// under the registry lock, so the last owner stays last while it is held; a
/// retired job's fence may still upgrade its `Weak` to read `uncertain`,
/// which a healthy row never sets. Poisoned rows own their health and stay.
pub(crate) fn release_file_health(identity: FileIdentity, health: &Arc<FileHealth>) {
    if Arc::strong_count(health) != 1 {
        return;
    }
    let Some(lock) = FILE_HEALTH.get() else {
        return;
    };
    let mut registry = lock
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if Arc::strong_count(health) != 1 {
        return;
    }
    if let Ok(Ok(index)) = registry.find(identity, None)
        && matches!(
            registry.entries.get(index),
            Some((_, Entry::Healthy(row))) if std::ptr::eq(row.as_ptr(), Arc::as_ptr(health))
        )
    {
        registry.entries.remove(index);
    }
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
        if self.entries.len() >= self.sweep_at {
            let before = self.entries.len();
            self.sweep_idle(meter)?;
            self.sweep_at = self.entries.len().saturating_mul(2).max(FIRST_SWEEP);
            if self.entries.len() != before {
                index = self.find(identity, meter)?.unwrap_or_else(|index| index);
            }
        }
        if self.entries.len() == self.entries.capacity() {
            let old = self.entries.capacity() * row;
            let next = self.entries.capacity().saturating_mul(2).max(1);
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
