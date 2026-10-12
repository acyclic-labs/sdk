use super::super::adapter::NativeDemandCarrier;
use super::super::lazy::mount_demand_error;
use super::*;
use crate::demand::NativeMountAllocationCustody;

pub(super) fn vector<T>(
    count: usize,
    carrier: &mut NativeDemandCarrier<'_>,
) -> Result<(Vec<T>, Option<NativeMountAllocationCustody>), MountSourceError> {
    carrier
        .with_work(|work| work.check())
        .map_err(mount_demand_error)?;
    if count == 0 {
        return Ok((Vec::new(), None));
    }
    let custody = carrier.new_custody()?;
    let (value, bytes) = carrier
        .with_work(|work| crate::native_host::buffer::<T, _>(count, work))
        .map_err(mount_demand_error)?;
    if let Err(error) = carrier.with_work(|work| custody.retain_admitted(bytes, work)) {
        drop(value);
        carrier
            .with_work(|work| work.release(bytes))
            .map_err(mount_demand_error)?;
        return Err(mount_demand_error(error));
    }
    Ok((value, Some(custody)))
}

pub(super) fn charge(
    carrier: &mut NativeDemandCarrier<'_>,
    delta: crate::WorkCounters,
) -> Result<(), MountSourceError> {
    carrier
        .with_work(|work| work.charge(delta))
        .map_err(mount_demand_error)
}

pub(super) fn map_bytes(capacity: usize) -> Result<u64, MountSourceError> {
    if capacity == 0 {
        return Ok(0);
    }
    // Fund payload and control storage before asking HashMap to allocate.
    (capacity as u64)
        .checked_mul(2 * std::mem::size_of::<(u64, u64)>() as u64 + 2)
        .and_then(|n| n.checked_add(32))
        .ok_or(crate::WorkError::Overflow.into())
}

#[derive(Default)]
pub(super) struct ChangePositions {
    floor: u64,
    latest: HashMap<u64, u64>,
    // Physical backing drops before its original allocation owner.
    custody: Option<NativeMountAllocationCustody>,
}

pub(super) fn record(
    map: &RwLock<ChangePositions>,
    key: u64,
    position: ViewStamp,
    carrier: &mut NativeDemandCarrier<'_>,
) -> Result<(), MountSourceError> {
    let mut positions = map.write().unwrap_or_else(PoisonError::into_inner);
    if !positions.latest.is_empty() {
        charge(carrier, scan_work(1))?;
    }
    let fold =
        positions.latest.len() >= MAXIMUM_TRACKED_KEYS && !positions.latest.contains_key(&key);
    if !fold
        && positions.latest.len() == positions.latest.capacity()
        && !positions.latest.contains_key(&key)
    {
        let count = positions
            .latest
            .capacity()
            .max(4)
            .checked_mul(2)
            .ok_or(crate::WorkError::Overflow)?
            .min(MAXIMUM_TRACKED_KEYS);
        let old_bytes = map_bytes(positions.latest.capacity())?;
        let ChangePositions {
            latest, custody, ..
        } = &mut *positions;
        let borrowed = adopt_backing(custody.as_ref(), old_bytes, carrier)?;
        let result = (|| {
            let next_custody = carrier.new_custody()?;
            let bytes = map_bytes(count)?;
            carrier
                .with_work(|work| next_custody.claim(bytes, work))
                .map_err(mount_demand_error)?;
            let mut next = HashMap::new();
            next.try_reserve(count)
                .map_err(MountSourceError::Allocation)?;
            let actual = map_bytes(next.capacity())?;
            if actual > bytes {
                carrier
                    .with_work(|work| next_custody.claim(actual - bytes, work))
                    .map_err(mount_demand_error)?;
            } else {
                next_custody.retire(bytes - actual)?;
            }
            next.extend(latest.drain());
            drop(std::mem::replace(latest, next));
            drop(std::mem::replace(custody, Some(next_custody)));
            Ok(())
        })();
        let released = release_backing(borrowed, carrier);
        result.and(released)?;
    }
    charge(
        carrier,
        crate::WorkCounters {
            items_examined: if fold {
                positions.latest.len() as u64
            } else {
                0
            },
            ..crate::WorkCounters::UNCHARGED
        },
    )?;
    if fold {
        let floor = positions.floor;
        positions.floor = positions
            .latest
            .drain()
            .fold(floor, |floor, (_, at)| floor.max(at));
    }
    let at = positions.latest.entry(key).or_insert(0);
    *at = (*at).max(position.0);
    Ok(())
}

pub(super) fn unchanged_since(
    map: &RwLock<ChangePositions>,
    stamp: ViewStamp,
    keys: impl IntoIterator<Item = u64>,
) -> bool {
    let positions = map.read().unwrap_or_else(PoisonError::into_inner);
    keys.into_iter().all(|key| {
        positions
            .latest
            .get(&key)
            .map_or(positions.floor, |at| (*at).max(positions.floor))
            <= stamp.0
    })
}
pub(super) fn grow_vector<T>(
    entries: &mut Vec<T>,
    owner: &mut Option<NativeMountAllocationCustody>,
    capacity: usize,
    carrier: &mut NativeDemandCarrier<'_>,
) -> Result<(), MountSourceError> {
    let bytes = entries
        .capacity()
        .checked_mul(std::mem::size_of::<T>())
        .ok_or(crate::WorkError::Overflow)? as u64;
    let borrowed = adopt_backing(owner.as_ref(), bytes, carrier)?;
    let result = (|| {
        let (mut next, custody) = vector(capacity, carrier)?;
        charge(
            carrier,
            copy_work(
                (entries.len() as u64)
                    .checked_mul(std::mem::size_of::<T>() as u64)
                    .ok_or(crate::WorkError::Overflow)?,
            ),
        )?;
        next.append(entries);
        drop(std::mem::replace(entries, next));
        drop(std::mem::replace(owner, custody));
        Ok(())
    })();
    result.and(release_backing(borrowed, carrier))
}

fn adopt_backing(
    owner: Option<&NativeMountAllocationCustody>,
    bytes: u64,
    carrier: &mut NativeDemandCarrier<'_>,
) -> Result<u64, MountSourceError> {
    if owner
        .map(|owner| carrier.with_work(|work| owner.is_admitted(work)))
        .transpose()
        .map_err(mount_demand_error)?
        .is_some_and(|admitted| !admitted)
    {
        carrier
            .with_work(|work| work.adopt(bytes))
            .map_err(mount_demand_error)?;
        Ok(bytes)
    } else {
        Ok(0)
    }
}

fn release_backing(
    bytes: u64,
    carrier: &mut NativeDemandCarrier<'_>,
) -> Result<(), MountSourceError> {
    if bytes == 0 {
        Ok(())
    } else {
        carrier
            .with_work(|work| work.release(bytes))
            .map_err(mount_demand_error)
    }
}

pub(in crate::native_mount) fn same_work(
    left: &crate::NativeMaterializationWork,
    right: &crate::NativeMaterializationWork,
) -> bool {
    left.shares_allocation_context(right)
        && left.capture_origin.as_ref().map(Weak::as_ptr)
            == right.capture_origin.as_ref().map(Weak::as_ptr)
        && left.invocation.as_ref().map(Arc::as_ptr) == right.invocation.as_ref().map(Arc::as_ptr)
        && left.execution.as_ref().map(Arc::as_ptr) == right.execution.as_ref().map(Arc::as_ptr)
}

pub(in crate::native_mount) fn copy_work(bytes: u64) -> crate::WorkCounters {
    crate::WorkCounters {
        bytes_copied: bytes,
        ..crate::WorkCounters::UNCHARGED
    }
}

pub(in crate::native_mount) fn scan_work(items: u64) -> crate::WorkCounters {
    crate::WorkCounters {
        items_examined: items,
        ..crate::WorkCounters::UNCHARGED
    }
}
