use super::*;

pub(super) fn capture_work(
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<crate::NativeMaterializationWork, OperationFailure<CaptureError>> {
    crate::NativeMaterializationWork::new(budget, cancellation)
        .map_err(|(error, completed)| OperationFailure::new(CaptureError::from(error), *completed))
}

pub(super) struct CaptureWorkScope {
    pub(super) native: crate::NativeMaterializationWork,
    pub(super) custody: Option<std::sync::Arc<crate::demand::CaptureTransientCustody>>,
}

impl CaptureWorkScope {
    pub(super) fn new(
        native: &crate::NativeMaterializationWork,
    ) -> Result<Self, OperationFailure<CaptureError>> {
        let custody = crate::demand::CaptureTransientCustody::new_native(native)
            .map_err(|error| OperationFailure::new(CaptureError::from(error), native.receipt()))?;
        let view = native
            .with_capture_custody(&custody)
            .map_err(|error| OperationFailure::new(CaptureError::from(error), native.receipt()))?;
        Ok(Self {
            native: view,
            custody: Some(custody),
        })
    }

    pub(super) fn release(&mut self) -> Result<(), CaptureError> {
        // Accepted workers and source packets keep the token until their payloads retire.
        let Some(custody) = self.custody.take() else {
            return Ok(());
        };
        while std::sync::Arc::strong_count(&custody) != 1 {
            std::thread::yield_now();
        }
        let bytes = custody.take_retained_bytes();
        drop(custody);
        self.native
            .with_original_leaf(|work| {
                work.scope(
                    |work| &mut work.capture_custody,
                    None,
                    |work| work.release(bytes),
                )
            })
            .map_err(capture_demand_error)
    }
}

impl Drop for CaptureWorkScope {
    fn drop(&mut self) {
        let _ = self.release();
    }
}

pub(super) fn capture_scope_result<T>(
    mut scope: CaptureWorkScope,
    result: Result<T, CaptureError>,
) -> Result<OperationReceipt<T>, OperationFailure<CaptureError>> {
    let released = scope.release();
    let checked = scope
        .native
        .admit(WorkCounters::UNCHARGED)
        .map_err(CaptureError::from);
    let completed = scope.native.receipt();
    result
        .and_then(|value| released.and(checked).map(|()| value))
        .map(|value| OperationReceipt {
            value,
            work: completed,
        })
        .map_err(|error| OperationFailure::new(error, completed))
}

pub(super) fn capture_demand_error(error: DemandError) -> CaptureError {
    match error {
        DemandError::Work(error) => CaptureError::from(error),
        DemandError::Io(error) => CaptureError::from(error),
        DemandError::Allocation(error) => CaptureError::from(error),
        DemandError::StaleVersion => CaptureError::from(crate::FsError::SourceSnapshotStale),
        DemandError::Cancelled => crate::CancellationError.into(),
        DemandError::OriginalWorkPoisoned => {
            crate::NativeTreeMaterializationError::OriginalWorkPoisoned.into()
        }
        _ => CaptureError::InvalidOptions,
    }
}

pub(super) fn capture_sort_by<T>(
    values: &mut [T],
    mut compare: impl FnMut(&T, &T) -> std::cmp::Ordering,
    meter: &NativeMaterializationWork,
) -> Result<(), CaptureError> {
    fn sift<T>(
        values: &mut [T],
        mut root: usize,
        end: usize,
        compare: &mut impl FnMut(&T, &T) -> std::cmp::Ordering,
        meter: &NativeMaterializationWork,
    ) -> Result<(), CaptureError> {
        while root < end / 2 {
            let mut child = root * 2 + 1;
            if child + 1 < end {
                capture_charge(
                    meter,
                    crate::materializer::NativeResourceCredit::Items.delta(2),
                )?;
                if compare(
                    values.get(child).ok_or(WorkError::Overflow)?,
                    values.get(child + 1).ok_or(WorkError::Overflow)?,
                )
                .is_lt()
                {
                    child += 1;
                }
            }
            capture_charge(
                meter,
                crate::materializer::NativeResourceCredit::Items.delta(2),
            )?;
            if !compare(
                values.get(root).ok_or(WorkError::Overflow)?,
                values.get(child).ok_or(WorkError::Overflow)?,
            )
            .is_lt()
            {
                break;
            }
            values.swap(root, child);
            root = child;
        }
        Ok(())
    }
    meter
        .with_original_leaf(|work| work.check())
        .map_err(capture_demand_error)?;
    let length = values.len();
    for root in (0..length / 2).rev() {
        sift(values, root, length, &mut compare, meter)?;
    }
    for end in (1..length).rev() {
        meter
            .with_original_leaf(|work| work.check())
            .map_err(capture_demand_error)?;
        values.swap(0, end);
        sift(values, 0, end, &mut compare, meter)?;
    }
    Ok(())
}

pub(super) fn capture_charge(
    meter: &NativeMaterializationWork,
    work: WorkCounters,
) -> Result<(), CaptureError> {
    meter
        .with_original_leaf(|meter| meter.charge(work))
        .map_err(capture_demand_error)
}

pub(super) fn capture_vec<T>(
    count: usize,
    meter: &NativeMaterializationWork,
) -> Result<Vec<T>, CaptureError> {
    crate::native_host::buffer(count, &mut meter.clone())
        .map(|(values, _allocation)| values)
        .map_err(capture_demand_error)
}

pub(super) fn capture_reserve<T>(
    values: &mut Vec<T>,
    additional: usize,
    meter: &NativeMaterializationWork,
) -> Result<(), CaptureError> {
    meter
        .with_original_leaf(|work| work.check())
        .map_err(capture_demand_error)?;
    let wanted = values
        .len()
        .checked_add(additional)
        .ok_or(CaptureError::from(WorkError::Overflow))?;
    if wanted <= values.capacity() {
        return Ok(());
    }
    let size = std::mem::size_of::<T>();
    let old = values
        .capacity()
        .checked_mul(size)
        .ok_or(CaptureError::from(WorkError::Overflow))?;
    let copied = values
        .len()
        .checked_mul(size)
        .ok_or(CaptureError::from(WorkError::Overflow))?;
    let grown = wanted.max(values.capacity().saturating_mul(2).max(4));
    let capacity = match grown.checked_mul(size) {
        Some(bytes)
            if bytes as u64
                <= meter
                    .with_original_leaf(|work| work.remaining())
                    .map_err(capture_demand_error)?
                    .peak_allocation_bytes =>
        {
            grown
        }
        _ => wanted,
    };
    let mut replacement = capture_vec(capacity, meter)?;
    capture_charge(
        meter,
        crate::materializer::NativeResourceCredit::Copy.delta(copied as u64),
    )?;
    replacement.append(values);
    let old_values = std::mem::replace(values, replacement);
    drop(old_values);
    meter
        .with_original_leaf(|work| work.release(old as u64))
        .map_err(capture_demand_error)
}

pub(super) fn vector_bytes<T>(values: &Vec<T>) -> u64 {
    (values.capacity() * std::mem::size_of::<T>()) as u64
}
pub(super) fn capture_position<V>(
    entries: &[(NamespacePath, V)],
    key: &NamespacePath,
    meter: &NativeMaterializationWork,
) -> Result<Result<usize, usize>, CaptureError> {
    let (mut low, mut high) = (0, entries.len());
    while low < high {
        capture_charge(
            meter,
            crate::materializer::NativeResourceCredit::Items.delta(2),
        )?;
        let middle = low + (high - low) / 2;
        match entries.get(middle).ok_or(WorkError::Overflow)?.0.cmp(key) {
            std::cmp::Ordering::Less => low = middle + 1,
            std::cmp::Ordering::Greater => high = middle,
            std::cmp::Ordering::Equal => return Ok(Ok(middle)),
        }
    }
    Ok(Err(low))
}
pub(super) fn capture_insert_at<V>(
    entries: &mut Vec<(NamespacePath, V)>,
    index: usize,
    key: NamespacePath,
    value: V,
    meter: &NativeMaterializationWork,
) -> Result<(), CaptureError> {
    let copied = entries
        .len()
        .checked_sub(index)
        .ok_or(WorkError::Overflow)?
        .checked_mul(std::mem::size_of::<(NamespacePath, V)>())
        .ok_or(WorkError::Overflow)? as u64;
    capture_reserve(entries, 1, meter)?;
    capture_charge(
        meter,
        crate::materializer::NativeResourceCredit::Copy.delta(copied),
    )?;
    entries.insert(index, (key, value));
    Ok(())
}
pub(super) fn capture_replace<V>(
    entries: &mut Vec<(NamespacePath, V)>,
    key: NamespacePath,
    value: V,
    meter: &NativeMaterializationWork,
) -> Result<(), CaptureError> {
    match capture_position(entries, &key, meter)? {
        Ok(index) => {
            capture_drop_path(key, meter)?;
            entries.get_mut(index).ok_or(WorkError::Overflow)?.1 = value;
            Ok(())
        }
        Err(index) => capture_insert_at(entries, index, key, value, meter),
    }
}
pub(super) fn capture_entry<'a, V>(
    entries: &'a mut Vec<(NamespacePath, V)>,
    key: NamespacePath,
    value: V,
    meter: &NativeMaterializationWork,
) -> Result<&'a mut V, CaptureError> {
    let index = match capture_position(entries, &key, meter)? {
        Ok(index) => {
            capture_drop_path(key, meter)?;
            index
        }
        Err(index) => {
            capture_insert_at(entries, index, key, value, meter)?;
            index
        }
    };
    Ok(&mut entries.get_mut(index).ok_or(WorkError::Overflow)?.1)
}
pub(super) fn capture_remove<V>(
    entries: &mut Vec<(NamespacePath, V)>,
    key: &NamespacePath,
    meter: &NativeMaterializationWork,
) -> Result<Option<(NamespacePath, V)>, CaptureError> {
    let Ok(index) = capture_position(entries, key, meter)? else {
        return Ok(None);
    };
    let copied = (entries.len() - index - 1)
        .checked_mul(std::mem::size_of::<(NamespacePath, V)>())
        .ok_or(WorkError::Overflow)? as u64;
    capture_charge(
        meter,
        crate::materializer::NativeResourceCredit::Copy.delta(copied),
    )?;
    Ok(Some(entries.remove(index)))
}
