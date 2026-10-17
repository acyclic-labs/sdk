use super::*;

/// Capture order: absent paths deepest first, then directories, recorded
/// files and new files, each shallowest first; paths break every tie.
fn rank(observation: &Option<HostStat>, current: &Option<FileRecord>) -> usize {
    match observation {
        None => 0,
        Some(observation) if observation.is_dir() => 1,
        Some(_) if current.is_some() => 2,
        Some(_) => 3,
    }
}

pub(super) fn capture_state_order(
    (left, left_path, left_current): &CapturePathState,
    (right, right_path, right_current): &CapturePathState,
) -> std::cmp::Ordering {
    let left_rank = rank(left, left_current);
    let right_rank = rank(right, right_current);
    left_rank.cmp(&right_rank).then_with(|| {
        if left_rank == 0 {
            right_path
                .depth()
                .cmp(&left_path.depth())
                .then_with(|| left_path.cmp(right_path))
        } else {
            left_path
                .depth()
                .cmp(&right_path.depth())
                .then_with(|| left_path.cmp(right_path))
        }
    })
}

/// Lookups yield states in strictly ascending path order. Then every
/// `(rank, depth)` class already holds its paths in order, and one stable
/// counting pass over those classes is the capture order. Any other input
/// takes the comparison sort.
pub(super) fn sort_capture_states(
    states: &mut [CapturePathState],
    meter: &NativeMaterializationWork,
) -> Result<(), CaptureError> {
    #[cfg(test)]
    if super::state_order_tests::use_previous_sort() {
        return super::state_order_tests::comparison_sort(states, meter);
    }
    // Even the cheapest heapsort costs less than the counting pass for two
    // or three states. Select it before inspecting paths or charging work.
    if states.len() < 4 {
        return capture_sort_by(states, capture_state_order, meter);
    }
    let remaining = meter
        .with_original_leaf(|work| work.remaining())
        .map_err(capture_demand_error)?;
    let slots = if states.len() > INLINE_SLOTS {
        states.len()
    } else {
        0
    };
    if slots != 0 && remaining.allocation_operations == 0 {
        return capture_sort_by(states, capture_state_order, meter);
    }
    let pairs = states.len() - 1;
    if !states.windows(2).all(|pair| pair[0].1 < pair[1].1) {
        return capture_sort_by(states, capture_state_order, meter);
    }
    let deepest = states
        .iter()
        .map(|state| state.1.depth())
        .max()
        .unwrap_or(0);
    let classes = deepest.checked_add(1).and_then(|span| span.checked_mul(4));
    let Some(classes) = classes else {
        return capture_sort_by(states, capture_state_order, meter);
    };
    let starts = if classes > INLINE_CLASSES { classes } else { 0 };
    let bytes = slots
        .checked_add(starts)
        .and_then(|slots| slots.checked_mul(std::mem::size_of::<usize>()));
    let operations = u64::from(slots != 0) + u64::from(starts != 0);
    let items = 2 * pairs as u64 + states.len() as u64;
    if bytes.is_none_or(|bytes| bytes as u64 > remaining.peak_allocation_bytes)
        || operations > remaining.allocation_operations
        || items > remaining.items_examined
    {
        return capture_sort_by(states, capture_state_order, meter);
    }
    // Strategy selection does not spend a rejected strategy's allowance.
    // Once selected, charge every path comparison and the keying pass.
    capture_charge(
        meter,
        crate::materializer::NativeResourceCredit::Items.delta(2 * pairs as u64),
    )?;
    bucket_path_ordered_states(states, meter)
}

/// Scratch kept on the stack; larger orders take one measured buffer each.
const INLINE_SLOTS: usize = 128;
const INLINE_CLASSES: usize = 64;

fn scratch<'a>(
    inline: &'a mut [usize],
    measured: &'a mut Vec<usize>,
    length: usize,
    meter: &NativeMaterializationWork,
) -> Result<&'a mut [usize], CaptureError> {
    if length <= inline.len() {
        return Ok(inline.get_mut(..length).ok_or(WorkError::Overflow)?);
    }
    *measured = capture_vec(length, meter)?;
    measured.resize(length, 0);
    Ok(measured.as_mut_slice())
}

fn release_scratch(
    measured: [Vec<usize>; 2],
    meter: &NativeMaterializationWork,
) -> Result<(), CaptureError> {
    let bytes = measured.iter().map(vector_bytes).sum::<u64>();
    drop(measured);
    if bytes == 0 {
        return Ok(());
    }
    meter.clone().release(bytes).map_err(capture_demand_error)
}

/// Stable counting sort by `(rank, depth)` of strictly path-ordered states:
/// one keying pass over the states, then one placement per state.
fn bucket_path_ordered_states(
    states: &mut [CapturePathState],
    meter: &NativeMaterializationWork,
) -> Result<(), CaptureError> {
    let count = states.len();
    if count < 2 {
        return Ok(());
    }
    capture_charge(
        meter,
        crate::materializer::NativeResourceCredit::Items.delta(count as u64),
    )?;
    let mut measured = [Vec::new(), Vec::new()];
    let [measured_slots, measured_starts] = &mut measured;
    let mut inline_slots = [0_usize; INLINE_SLOTS];
    let slots = scratch(&mut inline_slots, measured_slots, count, meter)?;
    let mut deepest = 0;
    for ((observation, path, current), slot) in states.iter().zip(slots.iter_mut()) {
        let depth = path.depth();
        deepest = deepest.max(depth);
        *slot = depth
            .checked_mul(4)
            .and_then(|key| key.checked_add(rank(observation, current)))
            .ok_or(WorkError::Overflow)?;
    }
    let span = deepest.checked_add(1).ok_or(WorkError::Overflow)?;
    let classes = span.checked_mul(4).ok_or(WorkError::Overflow)?;
    let mut inline_starts = [0_usize; INLINE_CLASSES];
    let starts = scratch(&mut inline_starts, measured_starts, classes, meter)?;
    for slot in slots.iter_mut() {
        let (depth, rank) = (*slot / 4, *slot % 4);
        *slot = rank * span + if rank == 0 { deepest - depth } else { depth };
        *starts.get_mut(*slot).ok_or(WorkError::Overflow)? += 1;
    }
    let mut next = 0;
    for start in starts.iter_mut() {
        next += std::mem::replace(start, next);
    }
    for slot in slots.iter_mut() {
        let start = starts.get_mut(*slot).ok_or(WorkError::Overflow)?;
        *slot = *start;
        *start += 1;
    }
    // Follow each permutation cycle, placing one state per swap.
    for index in 0..count {
        loop {
            let target = *slots.get(index).ok_or(WorkError::Overflow)?;
            if target == index {
                break;
            }
            states.swap(index, target);
            slots.swap(index, target);
        }
    }
    release_scratch(measured, meter)
}

#[cfg(test)]
pub(super) fn unchecked_bucket_path_ordered_states(
    states: &mut [CapturePathState],
    meter: &NativeMaterializationWork,
) -> Result<(), CaptureError> {
    bucket_path_ordered_states(states, meter)
}
