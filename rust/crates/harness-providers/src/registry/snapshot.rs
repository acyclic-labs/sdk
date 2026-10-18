use crate::sync::Mutex;

/// Outermost lock: snapshot -> credential sources. Sources never call the
/// registry. Explicit catalog publication requires exclusive `&mut Registry`.
pub(super) struct Snapshot<T> {
    value: Mutex<Option<T>>,
    #[cfg(test)]
    pub(super) live: bool,
}

impl<T> Default for Snapshot<T> {
    fn default() -> Self {
        Self {
            value: Mutex::new(None),
            #[cfg(test)]
            live: false,
        }
    }
}

impl<T> Snapshot<T> {
    pub(super) fn read<R>(
        &self,
        observe: impl FnOnce() -> T,
        select: impl FnOnce(&T) -> R,
    ) -> Result<R, ()> {
        let mut value = self.value.lock().map_err(|_| ())?;
        #[cfg(test)]
        if self.live {
            *value = None;
        }
        Ok(select(value.get_or_insert_with(observe)))
    }

    pub(super) fn publish(&mut self, observed: T, accepted: bool) -> Result<(), ()> {
        let value = self.value.get_mut().map_err(|_| ())?;
        // A rejected refresh keeps the previously admitted generation. The
        // first observation, including a failure, is sticky until an update.
        if accepted || value.is_none() {
            *value = Some(observed);
        }
        Ok(())
    }
}
