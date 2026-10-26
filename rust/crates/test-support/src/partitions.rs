//! Exhaustive independent fixture registration, with one checked assignment.
use std::collections::BTreeSet;
use std::io;

/// Assigns every index to exactly one registered partition. Mandatory witnesses
/// may additionally appear in every partition. Noncontiguous or duplicate
/// declarations fail before executing any fixture.
pub fn indices(
    total: usize,
    registered: &[usize],
    selected: usize,
    witness: impl Fn(usize) -> bool,
) -> io::Result<Vec<usize>> {
    let declared: BTreeSet<_> = registered.iter().copied().collect();
    if registered.is_empty()
        || declared.len() != registered.len()
        || declared != (0..registered.len()).collect()
        || !declared.contains(&selected)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "missing, duplicate or unregistered exhaustive fixture partition",
        ));
    }
    Ok((0..total)
        .filter(|index| index % registered.len() == selected || witness(*index))
        .collect())
}

/// Declares independent tests and their exhaustive registration together. The
/// runner receives its assigned indices; no call site can maintain a separate
/// count that silently includes a test which was never declared.
#[macro_export]
macro_rules! partitioned_tests {
    ($(#[$attribute:meta])* async fn -> $result:ty;
     $registry:ident, $total:expr, $runner:path;
     $($index:literal => $name:ident),+ $(,)?) => {
        $crate::partitioned_tests!(@emit [$(#[$attribute])*] [async] [.await]
            $result; $registry, $total, $runner; $($index => $name),+);
    };
    ($(#[$attribute:meta])* fn -> $result:ty;
     $registry:ident, $total:expr, $runner:path;
     $($index:literal => $name:ident),+ $(,)?) => {
        $crate::partitioned_tests!(@emit [$(#[$attribute])*] [] []
            $result; $registry, $total, $runner; $($index => $name),+);
    };
    (@emit $attributes:tt $asynchronous:tt $wait:tt
     $result:ty; $registry:ident, $total:expr, $runner:path;
     $($index:literal => $name:ident),+ $(,)?) => {
        const $registry: &[usize] = &[$($index),+];
        $(
            $crate::partitioned_tests!(@one $attributes $asynchronous $wait
                $result; $registry, $total, $runner; $index => $name);
        )+
    };
    (@one [$($attribute:tt)*] [$($asynchronous:tt)*] [$($wait:tt)*]
     $result:ty; $registry:ident, $total:expr, $runner:path;
     $index:literal => $name:ident) => {
        $($attribute)*
        $($asynchronous)* fn $name() -> $result {
            let assigned = $crate::partitions::indices($total, $registry, $index, |_| false)
                .expect("every independent fixture partition must be registered exactly once");
            $runner(assigned)$($wait)*
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn every_fixture_is_assigned_and_broken_registration_is_rejected(
            total in 1usize..4096, count in 2usize..17, mandatory in 0usize..4096,
        ) {
            let registered: Vec<_> = (0..count).collect();
            let mut all = Vec::new();
            for selected in 0..count {
                let assigned = indices(total, &registered, selected, |_| false).unwrap();
                all.extend(assigned);
                let witness = mandatory % total;
                prop_assert!(indices(total, &registered, selected, |index| index == witness)
                    .unwrap().contains(&witness));
            }
            all.sort_unstable();
            prop_assert_eq!(all, (0..total).collect::<Vec<_>>());
            prop_assert!(indices(total, &registered[1..], 1, |_| false).is_err());
            let mut duplicated = registered.clone();
            duplicated[count-1] = 0;
            prop_assert!(indices(total, &duplicated, 0, |_| false).is_err());
            prop_assert!(indices(total, &registered, count, |_| false).is_err());
        }
    }

    fn selected(assigned: Vec<usize>) -> io::Result<()> {
        assert_eq!(assigned.len(), 1);
        Ok(())
    }

    crate::partitioned_tests! {
        #[test] fn -> io::Result<()>;
        MACRO_PARTITIONS, 2, selected;
        0 => declaration_registers_the_first_fixture,
        1 => declaration_registers_the_last_fixture,
    }
}
