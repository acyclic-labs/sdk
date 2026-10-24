use super::*;

struct RestoreName<'a, B: Copy> {
    slot: &'a RwLock<(MountPath, Option<B>)>,
    path: &'a mut MountPath,
}

impl<B: Copy> Drop for RestoreName<'_, B> {
    fn drop(&mut self) {
        let mut slot = self
            .slot
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        std::mem::swap(&mut slot.0, self.path);
        slot.1 = None;
    }
}

pub(super) fn with_creating_name<B: Copy, T>(
    slot: &RwLock<(MountPath, Option<B>)>,
    path: &mut MountPath,
    basis: B,
    write: impl FnOnce(&MountPath) -> T,
) -> T {
    // Never queue a writer behind the creator's read lock: a synchronous file
    // creation waits for a callback that also needs to read this slot.
    let mut available = match slot.try_write() {
        Ok(available) => available,
        Err(TryLockError::Poisoned(error)) => error.into_inner(),
        Err(TryLockError::WouldBlock) => return write(path),
    };
    if available.1.is_some() {
        drop(available);
        return write(path);
    }
    std::mem::swap(&mut available.0, path);
    available.1 = Some(basis);
    let restore = RestoreName { slot, path };
    drop(available);
    let current = slot
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let result = write(&current.0);
    drop(current);
    drop(restore);
    result
}

pub(super) fn callback_name_matches(path: &MountPath, name: &[u16]) -> bool {
    let mut name = name.iter().copied();
    for (index, component) in path.components().iter().enumerate() {
        if index != 0 && name.next() != Some(92) {
            return false;
        }
        if component.is_empty()
            || component.len() % 2 != 0
            || matches!(component.as_slice(), [46, 0] | [46, 0, 46, 0])
        {
            return false;
        }
        for &[low, high] in component.as_chunks::<2>().0 {
            let unit = u16::from_le_bytes([low, high]);
            if matches!(unit, 0 | 47 | 58 | 92) || name.next() != Some(unit) {
                return false;
            }
        }
    }
    name.next().is_none()
}
