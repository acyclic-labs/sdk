use super::super::adapter::NativeDemandCarrier;
use super::super::lazy::mount_demand_error;
use super::*;
use crate::demand::NativeMountAllocationCustody;
use std::sync::RwLockWriteGuard;

pub(super) fn vector<T>(
    count: usize,
    carrier: &mut NativeDemandCarrier<'_>,
) -> Result<(Vec<T>, Option<std::sync::Arc<NativeMountAllocationCustody>>), MountSourceError> {
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
    custody: Option<std::sync::Arc<NativeMountAllocationCustody>>,
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
            charge(
                carrier,
                crate::WorkCounters {
                    items_examined: 1,
                    ..crate::WorkCounters::UNCHARGED
                },
            )?;
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
            let borrowed = if let Some(custody) = &positions.custody {
                !carrier
                    .with_work(|work| custody.is_admitted(work))
                    .map_err(mount_demand_error)?
            } else {
                false
            };
            if borrowed {
                carrier
                    .with_work(|work| work.claim_allocation(old_bytes, 0, crate::WorkCounters::UNCHARGED))
                    .map_err(mount_demand_error)?;
            }
            let result = (|| {
                let custody = carrier.new_custody()?;
                let bytes = map_bytes(count)?;
                carrier
                    .with_work(|work| custody.claim(bytes, work))
                    .map_err(mount_demand_error)?;
                let mut replacement = HashMap::new();
                replacement
                    .try_reserve(count)
                    .map_err(MountSourceError::Allocation)?;
                let actual = map_bytes(replacement.capacity())?;
                if actual > bytes {
                    carrier
                        .with_work(|work| custody.claim(actual - bytes, work))
                        .map_err(mount_demand_error)?;
                } else if bytes > actual {
                    custody.retire(bytes - actual)?;
                }
                charge(
                    carrier,
                    crate::WorkCounters {
                        items_examined: positions.latest.len() as u64,
                        bytes_copied: positions
                            .latest
                            .len()
                            .checked_mul(std::mem::size_of::<(u64, u64)>())
                            .ok_or(crate::WorkError::Overflow)?
                            as u64,
                        ..crate::WorkCounters::UNCHARGED
                    },
                )?;
                replacement.extend(positions.latest.drain());
                drop(std::mem::replace(&mut positions.latest, replacement));
                drop(positions.custody.replace(custody));
                Ok::<_, MountSourceError>(())
            })();
            let released = if borrowed {
                carrier
                    .with_work(|work| work.release(old_bytes))
                    .map_err(mount_demand_error)
            } else {
                Ok(())
            };
            result.and(released)?;
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
    owner: &mut Option<std::sync::Arc<NativeMountAllocationCustody>>,
    capacity: usize,
    carrier: &mut NativeDemandCarrier<'_>,
) -> Result<(), MountSourceError> {
    let bytes = entries
        .capacity()
        .checked_mul(std::mem::size_of::<T>())
        .ok_or(crate::WorkError::Overflow)? as u64;
    let borrowed = if let Some(owner) = owner {
        !carrier
            .with_work(|work| owner.is_admitted(work))
            .map_err(mount_demand_error)?
    } else {
        false
    };
    if borrowed {
        carrier
            .with_work(|work| work.claim_allocation(bytes, 0, crate::WorkCounters::UNCHARGED))
            .map_err(mount_demand_error)?;
    }
    let replaced = (|| {
        let mut next = vector(capacity, carrier)?;
        charge(
            carrier,
            crate::WorkCounters {
                bytes_copied: entries
                    .len()
                    .checked_mul(std::mem::size_of::<T>())
                    .ok_or(crate::WorkError::Overflow)? as u64,
                ..crate::WorkCounters::UNCHARGED
            },
        )?;
        next.0.append(entries);
        drop(std::mem::replace(entries, next.0));
        drop(std::mem::replace(owner, next.1));
        Ok::<_, MountSourceError>(())
    })();
    let released = if borrowed {
        carrier
            .with_work(|work| work.release(bytes))
            .map_err(mount_demand_error)
    } else {
        Ok(())
    };
    replaced.and(released)
}
