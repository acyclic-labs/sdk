//! In-place heap traversal with caller-owned work and cancellation boundaries.

use std::cmp::Ordering;

pub(crate) fn heap_sort<T, C, E>(
    values: &mut [T],
    context: &mut C,
    mut compare: impl FnMut(&T, &T, &mut C) -> Result<Ordering, E>,
    mut swap: impl FnMut(&mut [T], usize, usize, &mut C) -> Result<(), E>,
    mut before_extract: impl FnMut(&mut C) -> Result<(), E>,
) -> Result<(), E> {
    fn sift<T, C, E>(
        values: &mut [T],
        mut root: usize,
        end: usize,
        context: &mut C,
        compare: &mut impl FnMut(&T, &T, &mut C) -> Result<Ordering, E>,
        swap: &mut impl FnMut(&mut [T], usize, usize, &mut C) -> Result<(), E>,
    ) -> Result<(), E> {
        // The heap is `values[..end]`; a child the slice does not hold is no child.
        while root < end / 2 {
            let mut child = root * 2 + 1;
            let (Some(parent), Some(mut larger)) = (values.get(root), values.get(child)) else {
                break;
            };
            if child + 1 < end
                && let Some(right) = values.get(child + 1)
                && compare(larger, right, context)?.is_lt()
            {
                child += 1;
                larger = right;
            }
            if !compare(parent, larger, context)?.is_lt() {
                break;
            }
            swap(values, root, child, context)?;
            root = child;
        }
        Ok(())
    }

    let length = values.len();
    for root in (0..length / 2).rev() {
        sift(values, root, length, context, &mut compare, &mut swap)?;
    }
    for end in (1..length).rev() {
        before_extract(context)?;
        swap(values, 0, end, context)?;
        sift(values, 0, end, context, &mut compare, &mut swap)?;
    }
    Ok(())
}
