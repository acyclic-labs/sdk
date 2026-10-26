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
    let mut order: Vec<_> = (0..mounts.len()).collect();
    order.sort_unstable_by_key(|&index| {
        std::cmp::Reverse(mounts[index].destination.components().count())
    });
    let mut retained = vec![true; mounts.len()];
    let mut failures = Vec::new();
    for index in order {
        let destination = mounts[index].destination;
        let has_descendant = mounts.iter().enumerate().any(|(other, mount)| {
            retained[other] && other != index && mount.destination.starts_with(destination)
        });
        if has_descendant {
            continue;
        }
        match detach(mounts[index].native) {
            Ok(detached) => retained[index] = !detached,
            Err(error) => failures.push(error),
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    fn observed<'a>(paths: &'a [&'a str]) -> Vec<ObservedMount<'a, &'a str>> {
        paths
            .iter()
            .map(|path| ObservedMount {
                destination: Path::new(path),
                native: path,
            })
            .collect()
    }

    #[test]
    fn every_kernel_order_retires_children_before_their_parent() {
        // All permutations, including the parent-first mount-table order.
        let paths = ["/owned", "/owned/child", "/owned/child/grandchild"];
        for first in 0..3 {
            for second in 0..3 {
                if first == second {
                    continue;
                }
                let third = 3 - first - second;
                let input = [paths[first], paths[second], paths[third]];
                let called = RefCell::new(Vec::new());
                recover_mounts(&observed(&input), |path| {
                    called.borrow_mut().push(*path);
                    Ok::<_, ()>(true)
                })
                .unwrap();
                assert_eq!(*called.borrow(), [paths[2], paths[1], paths[0]]);
            }
        }
    }

    #[test]
    fn a_retained_child_blocks_ancestors_but_not_adjacent_names_or_other_roots() {
        let input = [
            "/owned",
            "/owned/live-or-foreign",
            "/owned-adjacent",
            "/independent",
        ];
        let mut called = Vec::new();
        recover_mounts(&observed(&input), |path| {
            called.push(*path);
            Ok::<_, ()>(*path != input[1])
        })
        .unwrap();
        assert!(!called.contains(&input[0]));
        for path in &input[1..] {
            assert!(called.contains(path));
        }
    }

    #[test]
    fn every_failed_owner_is_retained_and_independent_recovery_continues() {
        let input = [
            "/failed",
            "/failed/child",
            "/another-failure",
            "/recoverable",
        ];
        let mut called = Vec::new();
        let failures = recover_mounts(&observed(&input), |path| {
            called.push(*path);
            if *path == input[3] {
                Ok(true)
            } else {
                Err(*path)
            }
        })
        .unwrap_err();
        assert!(!called.contains(&input[0]));
        assert!(called.contains(&input[3]));
        assert_eq!(failures.len(), 2);
        assert!(failures.contains(&input[1]));
        assert!(failures.contains(&input[2]));
    }

    #[test]
    fn stacked_mounts_require_a_fresh_kernel_identity_before_either_can_retire() {
        let input = ["/stacked", "/stacked", "/independent"];
        let mut called = Vec::new();
        recover_mounts(&observed(&input), |path| {
            called.push(*path);
            Ok::<_, ()>(true)
        })
        .unwrap();
        assert_eq!(called, [input[2]]);
    }
}
