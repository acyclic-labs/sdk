//! Bounded ordered seed inventories.

use super::*;

fn search<T>(
    values: &[T],
    value: &T,
    compare: impl Fn(&T, &T) -> std::cmp::Ordering,
    io: &PreparationIo,
) -> Result<std::result::Result<usize, usize>> {
    let (mut low, mut high) = (0, values.len());
    while low < high {
        let middle = low + (high - low) / 2;
        crate::model::charge_items(Some(io), 1)?;
        match compare(&values[middle], value) {
            std::cmp::Ordering::Less => low = middle + 1,
            std::cmp::Ordering::Greater => high = middle,
            std::cmp::Ordering::Equal => return Ok(Ok(middle)),
        }
    }
    Ok(Err(low))
}

pub(in crate::filesystem) fn fork_verify_insert<T>(
    values: &mut Vec<T>,
    value: T,
    compare: impl Fn(&T, &T) -> std::cmp::Ordering,
    io: &PreparationIo,
) -> Result<bool> {
    let index = match search(values, &value, compare, io)? {
        Ok(index) => {
            values[index] = value;
            return Ok(false);
        }
        Err(index) => index,
    };
    let capacity = values
        .len()
        .checked_add(1)
        .ok_or_else(|| io.invalid("fork verification vector overflow"))?;
    PreparationIo::reserve(values, capacity, Some(io))?;
    let shifted = (values.len() - index)
        .checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| io.invalid("fork verification copy overflow"))? as u64;
    let copies = WorkCounters {
        bytes_copied: shifted,
        ..Default::default()
    };
    io.preflight(copies)?;
    io.charge(copies)?;
    values.insert(index, value);
    Ok(true)
}

pub(in crate::filesystem) fn fork_verify_equal<T: Eq>(
    left: &[T],
    right: &[T],
    io: &PreparationIo,
) -> Result<bool> {
    if left.len() != right.len() {
        return Ok(false);
    }
    for (left, right) in left.iter().zip(right) {
        crate::model::charge_items(Some(io), 1)?;
        if left != right {
            return Ok(false);
        }
    }
    Ok(true)
}
