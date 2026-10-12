use super::*;

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
    let bytes = std::mem::size_of::<DarwinOwned<T>>()
        .checked_add(2 * std::mem::size_of::<usize>())
        .ok_or(WorkError::Overflow)? as u64;
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
        .expect("funded map")
        .reconcile_capacity(actual)?;
    replacement.extend(map.drain());
    drop(std::mem::replace(map, replacement));
    Ok(())
}

pub(super) fn retained_file(
    work: &Work,
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
            &mut work.clone(),
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
    work: &Work,
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

use acyclic_stream::OwnedBytes;

pub(super) fn source_bytes(
    chunk: crate::kernel::OwnedSourceChunk,
    work: &Work,
    use_bytes: impl FnOnce(&[u8]) -> Result<c_int, NativeMountError>,
) -> Result<c_int, NativeMountError> {
    let (bytes, capacity, intrinsic) = chunk.into_owned_parts();
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

pub(super) fn copy_bytes(
    work: &Work,
    bytes: &[u8],
) -> Result<crate::kernel::OwnedSourceChunk, NativeMountError> {
    work.admit(copy_work(bytes.len() as u64))?;
    if bytes.is_empty() {
        return Ok(crate::kernel::OwnedSourceChunk::intrinsic(
            OwnedBytes::new(),
            0,
        ));
    }
    let (value, allocation) = work.copy_bytes(bytes)?;
    let retained = allocation.bytes();
    let bytes = OwnedBytes::from_vec_with_guard(value, allocation, work.runtime_meter()).map_err(
        |failure| match failure.into_error() {
            acyclic_stream::PreparedOwnedBytesError::Admission(error) => {
                NativeMountError::from(work.runtime_error(error))
            }
            acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized => {
                unreachable!("initialized vector")
            }
        },
    )?;
    Ok(crate::kernel::OwnedSourceChunk::intrinsic(bytes, retained))
}
