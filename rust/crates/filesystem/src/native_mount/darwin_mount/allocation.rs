use super::*;
use crate::demand::NativeMountAllocationCustody;

pub(super) type RetainedFile = (Arc<dyn MountOpenFile>, Option<NativeMountAllocationCustody>);

pub(super) const READ_WORK: WorkCounters = WorkCounters {
    backend_read_operations: 1,
    ..WorkCounters::UNCHARGED
};
pub(super) const WRITE_WORK: WorkCounters = WorkCounters {
    backend_write_operations: 1,
    ..WorkCounters::UNCHARGED
};

pub(super) struct DarwinOwned<T> {
    pub(super) value: T,
    // Physical backing drops before its original allocation owner.
    pub(super) allocation: Option<NativeAllocation>,
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

    pub(super) fn claim_more(&mut self, bytes: u64, work: &Work) -> Result<(), NativeMountError> {
        if bytes == 0 {
            return Ok(());
        }
        let mut next = work.claim(bytes, 1, WorkCounters::UNCHARGED)?;
        if let Some(allocation) = &mut self.allocation {
            allocation.absorb(&mut next)?;
        } else {
            self.allocation = Some(next);
        }
        Ok(())
    }
}

pub(super) fn vector<T>(
    work: &Work,
    count: usize,
) -> Result<DarwinOwned<Vec<T>>, NativeMountError> {
    let (value, allocation) = crate::native_host::buffer(count, &mut &*work)?;
    Ok(DarwinOwned {
        value,
        allocation: Some(allocation),
    })
}

pub(super) fn shared<T>(
    work: &Work,
    mut value: DarwinOwned<T>,
) -> Result<Arc<DarwinOwned<T>>, NativeMountError> {
    let bytes = crate::arc_layout::allocation_bytes(std::alloc::Layout::new::<DarwinOwned<T>>())
        .map_err(|_| WorkError::Overflow)? as u64;
    value.claim_more(bytes, work)?;
    Ok(Arc::new(value))
}

pub(super) fn grow_map<K: std::hash::Hash + Eq, V>(
    map: &mut DarwinOwned<HashMap<K, V>>,
    work: &Work,
) -> Result<(), NativeMountError> {
    work.admit(WorkCounters::UNCHARGED)?;
    if map.len() < map.capacity() {
        return Ok(());
    }
    let capacity = map
        .capacity()
        .max(4)
        .checked_mul(2)
        .ok_or(WorkError::Overflow)?;
    let bytes = |capacity: usize| -> Result<u64, NativeMountError> {
        let bucket = std::mem::size_of::<(K, V)>()
            .checked_add(1)
            .and_then(|n| n.checked_mul(2))
            .ok_or(WorkError::Overflow)?;
        Ok(capacity
            .checked_mul(bucket)
            .and_then(|n| n.checked_add(32))
            .ok_or(WorkError::Overflow)? as u64)
    };
    let mut replacement = DarwinOwned::empty(HashMap::new());
    replacement.claim_more(bytes(capacity)?, work)?;
    replacement.try_reserve(capacity)?;
    let actual = bytes(replacement.capacity())?;
    replacement
        .allocation
        .as_mut()
        .ok_or(NativeMountError::ControlPending)?
        .reconcile_capacity(actual)?;
    replacement.extend(map.drain());
    drop(std::mem::replace(map, replacement));
    Ok(())
}

pub(super) fn retained_file(
    work: &Work,
    (file, bytes): (Arc<dyn MountOpenFile>, u64),
) -> Result<RetainedFile, NativeMountError> {
    NativeMountAllocationCustody::adopt(file, bytes, work).map_err(Into::into)
}

pub(super) fn source_bytes(
    chunk: crate::kernel::OwnedSourceChunk,
    work: &Work,
    use_bytes: impl FnOnce(&[u8]) -> Result<c_int, NativeMountError>,
) -> Result<c_int, NativeMountError> {
    let crate::kernel::OwnedSourceChunk {
        bytes,
        logical_owned_bytes: capacity,
        intrinsic,
    } = chunk;
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| use_bytes(&bytes)));
    drop(bytes);
    if !intrinsic {
        work.state.lock().release(capacity)?;
    }
    match result {
        Ok(result) => result,
        Err(panic) => Err(NativeMountError::DriverPanicked {
            operation: "Darwin byte reply",
            panic: Mutex::new(panic),
        }),
    }
}
