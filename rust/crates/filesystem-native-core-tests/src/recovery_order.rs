//! Kernel mount recovery against independent retained-subtree ownership facts.
#![cfg(not(loom))]

#[path = "../../filesystem/src/native_mount/recovery_order.rs"]
mod recovery_order;

use proptest::prelude::*;
use recovery_order::{ObservedMount, recover_mounts};
use std::{collections::BTreeSet, path::PathBuf};

#[test]
fn every_kernel_order_retires_children_before_their_parent() {
    let paths = ["/owned", "/owned/child", "/owned/child/grandchild"];
    for first in paths {
        for second in paths {
            for third in paths {
                if first == second || second == third || first == third {
                    continue;
                }
                let input = [first, second, third];
                let observed: Vec<_> = input
                    .iter()
                    .map(|path| ObservedMount {
                        destination: std::path::Path::new(path),
                        native: path,
                    })
                    .collect();
                let mut called = Vec::new();
                assert!(
                    recover_mounts(&observed, |path| {
                        called.push(*path);
                        Ok::<_, ()>(true)
                    })
                    .is_ok()
                );
                assert_eq!(called, paths.into_iter().rev().collect::<Vec<_>>());
            }
        }
    }
}

struct Mount {
    id: usize,
    destination: PathBuf,
    fate: u8,
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 128,
        rng_seed: proptest::test_runner::RngSeed::Fixed(0x7265_636f_7665_7231),
        ..ProptestConfig::default()
    })]

    #[test]
    fn recovery_matches_retained_subtrees(
        entries in proptest::collection::vec((proptest::collection::vec(0u8..4, 0..4), 0u8..3), 0..24)
    ) {
        let mounts: Vec<_> = entries.into_iter().enumerate().map(|(id, (segments, fate))| {
            let mut destination = PathBuf::from("/mounts");
            for segment in segments {
                destination.push(match segment { 0 => "a", 1 => "aa", 2 => "a-b", _ => "b" });
            }
            Mount { id, destination, fate }
        }).collect();
        let copies = |mount: &Mount| mounts.iter().filter(|other| other.destination == mount.destination).count();
        // Refused/failed owners and ambiguous stacks remain present regardless
        // of scheduling. Their proper ancestors cannot be attempted.
        let retained: Vec<_> = mounts.iter().filter(|mount| mount.fate != 0 || copies(mount) > 1).collect();
        let expected: BTreeSet<_> = mounts.iter().filter(|mount| copies(mount) == 1 && !retained.iter().any(|child| {
            child.destination != mount.destination && child.destination.starts_with(&mount.destination)
        })).map(|mount| mount.id).collect();
        let expected_failures: BTreeSet<_> = mounts.iter().filter(|mount| expected.contains(&mount.id) && mount.fate == 2).map(|mount| mount.id).collect();
        let observed: Vec<_> = mounts.iter().map(|mount| ObservedMount {
            destination: &mount.destination,
            native: mount,
        }).collect();
        let mut called = Vec::new();
        let mut retired = BTreeSet::new();
        let failures = recover_mounts(&observed, |mount| {
            assert!(mounts.iter().all(|child| child.id == mount.id || !child.destination.starts_with(&mount.destination) || retired.contains(&child.id)), "attempted a parent before its descendants retired");
            called.push(mount.id);
            match mount.fate {
                0 => { retired.insert(mount.id); Ok(true) }
                1 => Ok(false),
                _ => Err(mount.id),
            }
        }).err().unwrap_or_default();
        prop_assert_eq!(called.len(), expected.len());
        prop_assert_eq!(called.into_iter().collect::<BTreeSet<_>>(), expected);
        prop_assert_eq!(failures.len(), expected_failures.len());
        prop_assert_eq!(failures.into_iter().collect::<BTreeSet<_>>(), expected_failures);
    }
}
