use super::*;

impl<K> Default for KeyedLocks<K> {
    fn default() -> Self {
        Self(Mutex::new(HashMap::new()))
    }
}

pub(super) type FileLocks = KeyedLocks<crate::NativeRootIdentity>;

impl<K: Eq + std::hash::Hash + Clone> KeyedLocks<K> {
    pub(super) fn with<T>(&self, key: &K, edit: impl FnOnce() -> T) -> T {
        let lock = Arc::clone(lock_recover(&self.0).entry(key.clone()).or_default());
        let result = {
            let _held = lock_recover(&lock);
            edit()
        };
        let mut locks = lock_recover(&self.0);
        if Arc::strong_count(&lock) == 2 {
            locks.remove(key);
        }
        result
    }
}

pub(super) fn lock_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

pub(super) unsafe fn runtime<'a>(
    data: *const PRJ_CALLBACK_DATA,
) -> Option<(&'a PRJ_CALLBACK_DATA, &'a Runtime)> {
    let data = data.as_ref()?;
    let pointer = data.InstanceContext.cast::<Runtime>();
    pointer.as_ref().map(|value| (data, value))
}
