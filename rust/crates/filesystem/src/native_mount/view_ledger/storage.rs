use super::super::adapter::NativeDemandCarrier;
use super::super::lazy::mount_demand_error;
use super::*;
use crate::demand::NativeMountAllocationCustody;
use std::sync::RwLockWriteGuard;

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
    let bytes = count
        .checked_mul(std::mem::size_of::<T>())
        .ok_or(crate::WorkError::Overflow)? as u64;
    let custody = carrier.new_custody()?;
    carrier
        .with_work(|work| custody.claim(bytes, work))
        .map_err(mount_demand_error)?;
    let mut value = Vec::new();
    value
        .try_reserve_exact(count)
        .map_err(MountSourceError::Allocation)?;
    let extra = value
        .capacity()
        .checked_sub(count)
        .and_then(|n| n.checked_mul(std::mem::size_of::<T>()))
        .ok_or(crate::WorkError::Overflow)? as u64;
    if extra != 0 {
        carrier
            .with_work(|work| custody.claim(extra, work))
            .map_err(mount_demand_error)?;
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
pub(super) struct ChangeMap(pub(super) RwLock<ChangePositions>);

#[derive(Default)]
pub(super) struct ChangePositions {
    floor: u64,
    latest: HashMap<u64, u64>,
    // Physical backing drops before its original allocation owner.
    custody: Option<NativeMountAllocationCustody>,
}

impl ChangePositions {
    fn folds(&self, key: u64) -> bool {
        self.latest.len() >= MAXIMUM_TRACKED_KEYS && !self.latest.contains_key(&key)
    }

    pub(super) fn work(&self, key: u64) -> Result<crate::WorkCounters, MountSourceError> {
        let fold = self.folds(key);
        let controls = if fold {
            (self.latest.capacity() as u64)
                .checked_mul(2)
                .and_then(|n| n.checked_add(32))
                .ok_or(crate::WorkError::Overflow)?
        } else {
            0
        };
        Ok(crate::WorkCounters {
            items_examined: if fold { self.latest.len() as u64 } else { 0 },
            bytes_copied: controls
                .checked_add(std::mem::size_of::<(u64, u64)>() as u64)
                .ok_or(crate::WorkError::Overflow)?,
            ..crate::WorkCounters::UNCHARGED
        })
    }

    pub(super) fn record(&mut self, key: u64, position: ViewStamp) {
        if self.folds(key) {
            self.floor = self
                .latest
                .drain()
                .fold(self.floor, |floor, (_, at)| floor.max(at));
        }
        let at = self.latest.entry(key).or_insert(0);
        *at = (*at).max(position.0);
    }
}

impl ChangeMap {
    pub(super) fn prepare(
        &self,
        key: u64,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<RwLockWriteGuard<'_, ChangePositions>, MountSourceError> {
        let mut positions = self.0.write().unwrap_or_else(PoisonError::into_inner);
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
            replace(latest, custody, old_bytes, carrier, |old, carrier| {
                let custody = carrier.new_custody()?;
                let bytes = map_bytes(count)?;
                carrier
                    .with_work(|work| custody.claim(bytes, work))
                    .map_err(mount_demand_error)?;
                let mut next = HashMap::new();
                next.try_reserve(count)
                    .map_err(MountSourceError::Allocation)?;
                let actual = map_bytes(next.capacity())?;
                if actual > bytes {
                    carrier
                        .with_work(|work| custody.claim(actual - bytes, work))
                        .map_err(mount_demand_error)?;
                } else {
                    custody.retire(bytes - actual)?;
                }
                charge(
                    carrier,
                    crate::WorkCounters {
                        items_examined: old.len() as u64,
                        bytes_copied: (old.len() as u64)
                            .checked_mul(std::mem::size_of::<(u64, u64)>() as u64)
                            .ok_or(crate::WorkError::Overflow)?,
                        ..crate::WorkCounters::UNCHARGED
                    },
                )?;
                next.extend(old.drain());
                Ok((next, Some(custody)))
            })?;
        }
        Ok(positions)
    }

    pub(super) fn record(
        &self,
        key: u64,
        position: ViewStamp,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError> {
        let mut positions = self.prepare(key, carrier)?;
        charge(carrier, positions.work(key)?)?;
        positions.record(key, position);
        Ok(())
    }

    pub(super) fn unchanged_since(
        &self,
        stamp: ViewStamp,
        check: impl FnOnce(&dyn Fn(u64) -> bool) -> bool,
    ) -> bool {
        let positions = self.0.read().unwrap_or_else(PoisonError::into_inner);
        check(&|key| {
            positions
                .latest
                .get(&key)
                .map_or(positions.floor, |at| (*at).max(positions.floor))
                <= stamp.0
        })
    }
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
    replace(entries, owner, bytes, carrier, |old, carrier| {
        let mut next = vector(capacity, carrier)?;
        charge(
            carrier,
            copy_work(
                (old.len() as u64)
                    .checked_mul(std::mem::size_of::<T>() as u64)
                    .ok_or(crate::WorkError::Overflow)?,
            ),
        )?;
        next.0.append(old);
        Ok(next)
    })
}

fn replace<T>(
    value: &mut T,
    owner: &mut Option<NativeMountAllocationCustody>,
    bytes: u64,
    carrier: &mut NativeDemandCarrier<'_>,
    build: impl FnOnce(
        &mut T,
        &mut NativeDemandCarrier<'_>,
    ) -> Result<(T, Option<NativeMountAllocationCustody>), MountSourceError>,
) -> Result<(), MountSourceError> {
    let borrowed = owner
        .as_ref()
        .map(|owner| carrier.with_work(|work| owner.is_admitted(work)))
        .transpose()
        .map_err(mount_demand_error)?
        .is_some_and(|admitted| !admitted);
    if borrowed {
        carrier
            .with_work(|work| work.adopt(bytes))
            .map_err(mount_demand_error)?;
    }
    let result = build(value, carrier).map(|(next, custody)| {
        drop(std::mem::replace(value, next));
        drop(std::mem::replace(owner, custody));
    });
    let released = if borrowed {
        carrier
            .with_work(|work| work.release(bytes))
            .map_err(mount_demand_error)
    } else {
        Ok(())
    };
    result.and(released)
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
