use crate::sync::Mutex;

/// Outermost lock: snapshot -> credential sources. Sources never call the
/// registry. Explicit catalog publication requires exclusive `&mut Registry`.
pub(super) struct Snapshot<T> {
    value: Mutex<T>,
    #[cfg(test)]
    pub(super) live: bool,
}

impl<T: Default> Default for Snapshot<T> {
    fn default() -> Self {
        Self {
            value: Mutex::new(T::default()),
            #[cfg(test)]
            live: false,
        }
    }
}

impl<T: Default> Snapshot<T> {
    /// Observations `select` records stay memoized for this generation.
    pub(super) fn read<R>(&self, select: impl FnOnce(&mut T) -> R) -> Result<R, ()> {
        let mut value = self.value.lock().map_err(|_| ())?;
        #[cfg(test)]
        if self.live {
            *value = T::default();
        }
        Ok(select(&mut value))
    }

    pub(super) fn publish(&mut self, update: impl FnOnce(&mut T)) -> Result<(), ()> {
        update(self.value.get_mut().map_err(|_| ())?);
        Ok(())
    }
}
