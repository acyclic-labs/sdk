use super::*;

#[derive(Default)]
pub(in crate::native_mount::view_ledger) struct ChangePositions {
    floor: u64,
    latest: HashMap<u64, u64>,
    // Physical backing drops before its original allocation owner.
    custody: Option<Arc<NativeMountAllocationCustody>>,
}
/// The shape of a map that one record was funded for.
struct Plan {
    tracked: usize,
    capacity: usize,
    known: bool,
    custody: Option<Arc<NativeMountAllocationCustody>>,
}

impl Plan {
    fn of(positions: &ChangePositions, key: u64) -> Self {
        Self {
            tracked: positions.latest.len(),
            capacity: positions.latest.capacity(),
            known: positions.latest.contains_key(&key),
            custody: positions.custody.clone(),
        }
    }

    fn holds_for(&self, positions: &ChangePositions, key: u64) -> bool {
        positions.latest.len() == self.tracked
            && positions.latest.capacity() == self.capacity
            && positions.latest.contains_key(&key) == self.known
            && match (&positions.custody, &self.custody) {
                (Some(held), Some(planned)) => Arc::ptr_eq(held, planned),
                (None, None) => true,
                _ => false,
            }
    }
}

/// Records that `key` changed at `position`.
///
/// Lock order is meter, then ledger map: a reader consults the ledger while
/// it holds its meter. So every charge, admission and allocation here runs
/// with no map lock held, and the write lock only applies a plan that is
/// already paid for. If another writer changed the map in between, the plan
/// is paid for again.
pub(in crate::native_mount::view_ledger) fn record(
    map: &RwLock<ChangePositions>,
    key: u64,
    position: ViewStamp,
    carrier: &mut NativeDemandCarrier<'_>,
) -> Result<(), MountSourceError> {
    loop {
        let plan = Plan::of(&map.read().unwrap_or_else(PoisonError::into_inner), key);
        if plan.tracked != 0 {
            charge(carrier, scan_work(1))?;
        }
        if plan.known || (plan.tracked < plan.capacity && plan.tracked < MAXIMUM_TRACKED_KEYS) {
            let mut positions = map.write().unwrap_or_else(PoisonError::into_inner);
            if plan.holds_for(&positions, key) {
                let at = positions.latest.entry(key).or_insert(position.0);
                *at = (*at).max(position.0);
                return Ok(());
            }
        } else if plan.tracked >= MAXIMUM_TRACKED_KEYS {
            charge(carrier, scan_work(plan.tracked as u64))?;
            let mut positions = map.write().unwrap_or_else(PoisonError::into_inner);
            if plan.holds_for(&positions, key) {
                let floor = positions.floor;
                positions.floor = positions
                    .latest
                    .drain()
                    .fold(floor, |floor, (_, at)| floor.max(at));
                positions.latest.insert(key, position.0);
                return Ok(());
            }
        } else if grow(map, key, position, &plan, carrier)? {
            return Ok(());
        }
    }
}

/// Moves a full map into a larger one that holds `key`, if the map still has
/// the planned shape.
fn grow(
    map: &RwLock<ChangePositions>,
    key: u64,
    position: ViewStamp,
    plan: &Plan,
    carrier: &mut NativeDemandCarrier<'_>,
) -> Result<bool, MountSourceError> {
    let count = (plan.capacity.max(4) * 2).min(MAXIMUM_TRACKED_KEYS);
    let old_bytes = map_bytes(plan.capacity)?;
    let borrowed = adopt_backing(plan.custody.as_deref(), old_bytes, carrier)?;
    let prepared = (|| {
        let next_custody = carrier.new_custody()?;
        let bytes = map_bytes(count)?;
        carrier
            .with_work(|work| next_custody.claim(bytes, work))
            .map_err(MountSourceError::from)?;
        let mut next = HashMap::new();
        next.try_reserve(count)
            .map_err(MountSourceError::Allocation)?;
        let actual = map_bytes(next.capacity())?;
        if actual > bytes {
            carrier
                .with_work(|work| next_custody.claim(actual - bytes, work))
                .map_err(MountSourceError::from)?;
        } else {
            next_custody.retire(bytes - actual)?;
        }
        Ok((next, next_custody))
    })();
    let applied = prepared.map(|(mut next, next_custody)| {
        let mut positions = map.write().unwrap_or_else(PoisonError::into_inner);
        if !plan.holds_for(&positions, key) {
            drop(positions);
            // Physical backing drops before its allocation owner.
            drop(next);
            drop(next_custody);
            return false;
        }
        next.extend(positions.latest.drain());
        next.insert(key, position.0);
        let replaced = std::mem::replace(&mut positions.latest, next);
        let retired = positions.custody.replace(next_custody);
        // Retiring custody reaches the meter, so it waits for the lock to go.
        drop(positions);
        drop(replaced);
        drop(retired);
        true
    });
    let released = release_backing(borrowed, carrier);
    applied.and_then(|applied| released.map(|()| applied))
}

pub(in crate::native_mount::view_ledger) fn unchanged_since(
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
