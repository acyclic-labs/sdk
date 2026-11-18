use super::*;
use std::cell::Cell;
use std::mem::ManuallyDrop;

loom::thread_local! {
    static MAP_BEFORE_METER: Cell<bool> = Cell::new(false);
    static OVERWRITE_STAMP: Cell<bool> = Cell::new(false);
}

pub(super) fn overwrite_stamp() -> bool {
    OVERWRITE_STAMP.with(Cell::get)
}

pub(super) fn hold_map_while_charging(
    map: &RwLock<ChangePositions>,
) -> Option<ManuallyDrop<crate::sync::RwLockWriteGuard<'_, ChangePositions>>> {
    // Loom 0.7's RwLock unlock accesses a missing active thread after a
    // deadlock. Release explicitly on progress; retain it on deadlock unwind.
    MAP_BEFORE_METER
        .with(Cell::get)
        .then(|| ManuallyDrop::new(map.write().unwrap()))
}
