use super::*;

pub(super) const READ_WORK: crate::WorkCounters = crate::WorkCounters {
    backend_read_operations: 1,
    ..crate::WorkCounters::UNCHARGED
};
pub(super) const WRITE_WORK: crate::WorkCounters = crate::WorkCounters {
    backend_write_operations: 1,
    ..crate::WorkCounters::UNCHARGED
};

pub(super) struct DarwinOwned<T> {
    pub(super) value: T,
    // Physical backing drops before its original allocation owner.
    pub(super) allocation: Option<crate::NativeAllocation>,
}

impl<T> std::ops::Deref for DarwinOwned<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T> std::ops::DerefMut for DarwinOwned<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.value
    }
}

impl<T> DarwinOwned<T> {
    pub(super) fn empty(value: T) -> Self {
        Self {
            value,
            allocation: None,
        }
    }

    pub(super) fn claim_more(
        &mut self,
        bytes: u64,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        if bytes == 0 {
            return Ok(());
        }
        let mut next = work.claim(bytes, 1, crate::WorkCounters::UNCHARGED)?;
        if let Some(allocation) = &mut self.allocation {
            allocation.absorb(&mut next)?;
        } else {
            self.allocation = Some(next);
        }
        Ok(())
    }

    pub(super) fn map<U>(self, transform: impl FnOnce(T) -> U) -> DarwinOwned<U> {
        let Self { value, allocation } = self;
        DarwinOwned {
            value: transform(value),
            allocation,
        }
    }
}

pub(super) fn owned<T>(
    work: &crate::NativeMaterializationWork,
    bytes: u64,
    build: impl FnOnce() -> Result<T, NativeMountError>,
) -> Result<DarwinOwned<T>, NativeMountError> {
    let allocation = if bytes == 0 {
        None
    } else {
        Some(work.claim(bytes, 1, crate::WorkCounters::UNCHARGED)?)
    };
    Ok(DarwinOwned {
        value: build()?,
        allocation,
    })
}

pub(super) fn vector<T>(
    work: &crate::NativeMaterializationWork,
    count: usize,
) -> Result<DarwinOwned<Vec<T>>, NativeMountError> {
    let bytes = count
        .checked_mul(std::mem::size_of::<T>())
        .ok_or(crate::WorkError::Overflow)? as u64;
    let mut value = owned(work, bytes, || {
        let mut value = Vec::new();
        value.try_reserve_exact(count)?;
        Ok(value)
    })?;
    if let Some(allocation) = &mut value.allocation {
        allocation.reconcile_capacity(
            value
                .value
                .capacity()
                .checked_mul(std::mem::size_of::<T>())
                .ok_or(crate::WorkError::Overflow)? as u64,
        )?;
    }
    Ok(value)
}

pub(super) fn shared<T>(
    work: &crate::NativeMaterializationWork,
    mut value: DarwinOwned<T>,
) -> Result<Arc<DarwinOwned<T>>, NativeMountError> {
    let bytes = std::mem::size_of::<DarwinOwned<T>>()
        .checked_add(2 * std::mem::size_of::<usize>())
        .ok_or(crate::WorkError::Overflow)? as u64;
    value.claim_more(bytes, work)?;
    Ok(Arc::new(value))
}

pub(super) fn grow_map<K: std::hash::Hash + Eq, V>(
    map: &mut DarwinOwned<HashMap<K, V>>,
    work: &crate::NativeMaterializationWork,
) -> Result<(), NativeMountError> {
    work.admit(crate::WorkCounters::UNCHARGED)?;
    if map.len() < map.capacity() {
        return Ok(());
    }
    let capacity = map
        .capacity()
        .max(4)
        .checked_mul(2)
        .ok_or(crate::WorkError::Overflow)?;
    let bytes = |capacity: usize| -> Result<u64, NativeMountError> {
        let bucket = std::mem::size_of::<(K, V)>()
            .checked_add(1)
            .and_then(|n| n.checked_mul(2))
            .ok_or(crate::WorkError::Overflow)?;
        Ok(capacity
            .checked_mul(bucket)
            .and_then(|n| n.checked_add(32))
            .ok_or(crate::WorkError::Overflow)? as u64)
    };
    let copied = crate::WorkCounters {
        items_examined: map.len() as u64,
        bytes_copied: map
            .len()
            .checked_mul(std::mem::size_of::<(K, V)>())
            .ok_or(crate::WorkError::Overflow)? as u64,
        ..crate::WorkCounters::UNCHARGED
    };
    work.admit(copied)?;
    let mut replacement = owned(work, bytes(capacity)?, || {
        let mut value = HashMap::new();
        value.try_reserve(capacity)?;
        Ok(value)
    })?;
    let actual = bytes(replacement.capacity())?;
    replacement
        .allocation
        .as_mut()
        .expect("funded map")
        .reconcile_capacity(actual)?;
    work.charge(copied)?;
    replacement.extend(map.drain());
    drop(std::mem::replace(map, replacement));
    Ok(())
}

pub(super) fn retained_file(
    work: &crate::NativeMaterializationWork,
    (file, bytes): (Arc<dyn MountOpenFile>, u64),
) -> Result<
    (
        Arc<dyn MountOpenFile>,
        Option<std::sync::Arc<crate::demand::NativeMountAllocationCustody>>,
    ),
    NativeMountError,
> {
    if bytes == 0 {
        return Ok((file, None));
    }
    let retained = (|| {
        let custody = crate::demand::NativeMountAllocationCustody::new(
            &mut crate::demand::NativeSourceWork::from_native_materialization_work(work),
        )?;
        work.with_original_leaf(|original| custody.retain_admitted(bytes, original))?;
        Ok::<_, crate::native_mount::NativeMountStartupAdmissionError>(custody)
    })();
    match retained {
        Ok(custody) => Ok((file, Some(custody))),
        Err(cause) => {
            drop(file);
            let retired = work.state.lock().release(bytes);
            retired?;
            Err(cause.into())
        }
    }
}

pub(super) fn grow_bytes(
    value: &mut DarwinOwned<Vec<u8>>,
    required: usize,
    work: &crate::NativeMaterializationWork,
) -> Result<(), NativeMountError> {
    if required > MAXIMUM_NATIVE_ATTRIBUTE_LIST_BYTES {
        return Err(driver_errno(libc::E2BIG));
    }
    if required <= value.capacity() {
        return Ok(());
    }
    let capacity = value
        .capacity()
        .checked_mul(2)
        .unwrap_or(required)
        .max(required)
        .min(MAXIMUM_NATIVE_ATTRIBUTE_LIST_BYTES);
    let copied = copy_work(value.len() as u64);
    work.admit(copied)?;
    let mut next = vector(work, capacity)?;
    work.charge(copied)?;
    next.extend_from_slice(value);
    drop(std::mem::replace(value, next));
    Ok(())
}
