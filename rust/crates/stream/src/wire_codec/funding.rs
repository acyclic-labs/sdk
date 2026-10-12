use super::*;

impl<'a> MemoryOutput<WireMeter<'a>> {
    pub(crate) fn fund(meter: WireMeter<'a>) -> Self {
        Self {
            value: meter,
            #[cfg(not(target_arch = "wasm32"))]
            allocation: meter.map(ReadAllocation::empty),
            #[cfg(target_arch = "wasm32")]
            allocation: (),
        }
    }

    pub(crate) fn records(&mut self, values: &[crate::OwnedBytes]) -> std::io::Result<Vec<crate::OwnedBytes>> {
        let bytes = values.len().checked_mul(std::mem::size_of::<crate::OwnedBytes>())
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
        self.copy_admission(bytes)?;
        let mut owned = self.vector(values.len())?;
        owned.extend(values.iter().cloned());
        self.copy_completed(bytes)?;
        Ok(owned)
    }

    pub(crate) fn vector<T>(&mut self, length: usize) -> std::io::Result<Vec<T>> {
        let bytes = length.checked_mul(std::mem::size_of::<T>())
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
        let before = self.capacity();
        self.allocate(bytes)?;
        let mut owned = Vec::new();
        self.reserve_vector(&mut owned, length)?;
        self.reconcile(before, owned.capacity() * std::mem::size_of::<T>())?;
        Ok(owned)
    }

    pub(crate) fn ensure_vector<T>(&mut self, values: &mut Vec<T>, additional: usize) -> std::io::Result<()> {
        if additional <= values.capacity() - values.len() { return Ok(()); }
        let required = values.len().checked_add(additional)
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
        let capacity = required.max(values.capacity().saturating_mul(2)).max(4);
        let item = std::mem::size_of::<T>();
        let old = values.capacity().checked_mul(item)
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
        let planned = capacity.checked_mul(item)
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
        let before = if self.value.is_some() {
            self.capacity().checked_sub(old)
                .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidInput))?
        } else { 0 };
        self.allocate(planned - old)?;
        self.reserve_vector(values, capacity - values.len())?;
        self.reconcile(before, values.capacity() * item)
    }

    fn reserve_vector<T>(&self, values: &mut Vec<T>, additional: usize) -> std::io::Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(meter) = self.value {
            return values.try_reserve_exact(additional)
                .map_err(|error| meter.allocation_failed(error));
        }
        values.reserve_exact(additional);
        Ok(())
    }

    pub(crate) fn retire(&mut self, bytes: usize) -> std::io::Result<()> {
        #[cfg(target_arch = "wasm32")]
        let _ = bytes;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(allocation) = &mut self.allocation { allocation.release(bytes)?; }
        Ok(())
    }

    pub(crate) fn examine(&self, items: u64) -> std::io::Result<()> {
        #[cfg(target_arch = "wasm32")]
        let _ = items;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(meter) = self.value {
            meter.examine(items, 0)?;
        }
        Ok(())
    }

    pub(crate) fn capacity(&self) -> usize {
        #[cfg(not(target_arch = "wasm32"))]
        { self.allocation.as_ref().map_or(0, |allocation| allocation.bytes() as usize) }
        #[cfg(target_arch = "wasm32")]
        { 0 }
    }

    pub(crate) fn allocate(&mut self, bytes: usize) -> std::io::Result<()> {
        #[cfg(target_arch = "wasm32")]
        let _ = bytes;
        #[cfg(not(target_arch = "wasm32"))]
        if bytes != 0
            && let Some(allocation) = &mut self.allocation
        {
            allocation.grow(bytes)?;
        }
        Ok(())
    }

    pub(crate) fn reconcile(&mut self, previous: usize, actual: usize) -> std::io::Result<()> {
        #[cfg(target_arch = "wasm32")]
        let _ = (previous, actual);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(allocation) = &mut self.allocation {
            let total = previous.checked_add(actual)
                .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
            allocation.reconcile_capacity(total)?;
        }
        Ok(())
    }

    pub(crate) fn copy_admission(&self, bytes: usize) -> std::io::Result<()> {
        #[cfg(target_arch = "wasm32")]
        let _ = bytes;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(meter) = self.value {
            meter.admit_codec(0, bytes as u64, 0)?;
        }
        Ok(())
    }

    pub(crate) fn copy_completed(&self, bytes: usize) -> std::io::Result<()> {
        #[cfg(target_arch = "wasm32")]
        let _ = bytes;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(meter) = self.value {
            meter.record_codec(0, bytes as u64, 0)?;
            meter.check()?;
        }
        Ok(())
    }

}
