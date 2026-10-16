use super::*;
use proptest::prelude::*;
use std::collections::BTreeMap;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // The environment is the host/mount union, with mounts winning collisions.
    #[test]
    fn mounts_override_host_values_without_losing_other_variables(
        host in prop::collection::btree_map("[A-Z_]{1,12}", ".{0,32}", 0..8),
        mounts in prop::collection::btree_map("[A-Z_]{1,12}", ".{0,32}", 0..8),
        lowercase in any::<bool>(),
    ) {
        let host: BTreeMap<_, _> = host.into_iter().map(|(key, value)| {
            (if lowercase { key.to_lowercase() } else { key }, value)
        }).collect();
        let actual = environment(host.iter().map(|(key, value)| (key.as_str().into(), value.as_str().into())), mounts.clone()).unwrap();
        for (key, value) in &host {
            let key = if cfg!(windows) { key.to_uppercase() } else { key.clone() };
            prop_assert_eq!(actual.get(&key), mounts.get(&key).or(Some(value)));
        }
        for (key, value) in &mounts {
            prop_assert_eq!(actual.get(key), Some(value));
        }
        let keys: std::collections::BTreeSet<_> = host.keys()
            .map(|key| if cfg!(windows) { key.to_uppercase() } else { key.clone() })
            .chain(mounts.keys().cloned()).collect();
        prop_assert_eq!(actual.len(), keys.len());
    }
}
