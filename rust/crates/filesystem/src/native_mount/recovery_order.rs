//! Namespace ordering over cached kernel facts. No destination is traversed.

use std::path::Path;

pub(super) struct ObservedMount<'a, T> {
    pub(super) destination: &'a Path,
    pub(super) native: &'a T,
}

/// A successful detach retires exactly one observed mount. A live, foreign,
/// unauthenticated or failed child blocks every ancestor, while independent
/// branches still run. Errors retain their original recovery custody.
pub(super) fn recover_mounts<T, E>(
    mounts: &[ObservedMount<'_, T>],
    mut detach: impl FnMut(&T) -> Result<bool, E>,
) -> Result<(), Vec<E>> {
    let mut order: Vec<_> = mounts.iter().collect();
    // Component ordering keeps each subtree contiguous, with children before
    // parents. Only the latest retained branch can block a remaining ancestor;
    // any ancestor of an earlier branch is also an ancestor of this branch.
    order.sort_unstable_by(|left, right| right.destination.cmp(left.destination));
    let mut blocked: Option<&Path> = None;
    let mut failures = Vec::new();
    for stacked in order.chunk_by(|left, right| left.destination == right.destination) {
        let Some((mount, _)) = stacked.split_first() else {
            unreachable!("mount groups are nonempty")
        };
        let destination = mount.destination;
        // Equal destinations describe a stack whose individual kernel identity
        // must be refreshed before any member can be detached.
        if stacked.len() > 1 || blocked.is_some_and(|path| path.starts_with(destination)) {
            blocked = Some(destination);
            continue;
        }
        match detach(mount.native) {
            Ok(true) => {}
            Ok(false) => blocked = Some(destination),
            Err(error) => {
                blocked = Some(destination);
                failures.push(error);
            }
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures)
    }
}
