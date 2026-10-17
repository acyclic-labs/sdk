use super::*;
use proptest::prelude::*;
use std::cell::Cell;

proptest! {
    #[test]
    fn versions_are_reused_exactly_while_the_concrete_identity_is_unchanged(
        operations in prop::collection::vec((0usize..4, any::<bool>()), 1..32),
    ) {
        let versions = Mutex::new(BTreeMap::new());
        let mut stamps = [0u64; 4];
        let mut previous = BTreeMap::new();
        let calls = Cell::new(0usize);
        for (key, changed) in operations {
            if changed { stamps[key] += 1; }
            let stamp = stamps[key];
            let expected = format!("{key}:{stamp}");
            let before = calls.get();
            let value = query(&versions, key, || Ok(stamp), || {
                calls.set(calls.get()+1);
                Ok::<_, ()>(expected.clone())
            }, || ()).unwrap();
            prop_assert_eq!(value, expected);
            prop_assert_eq!(calls.get()-before, usize::from(previous.get(&key) != Some(&stamp)));
            previous.insert(key, stamp);
        }
    }
}

#[test]
fn changed_identity_cannot_publish_a_version_under_the_old_identity() {
    let versions: Versions<i32, i32> = Mutex::new(BTreeMap::new());
    let stamp = Cell::new(0);
    assert_eq!(
        query(
            &versions,
            0,
            || Ok(stamp.get()),
            || {
                stamp.set(1);
                Ok("new executable".into())
            },
            || "changed"
        ),
        Err("changed")
    );
    stamp.set(0);
    assert_eq!(
        query(
            &versions,
            0,
            || Ok(stamp.get()),
            || Ok::<_, &str>("original executable".into()),
            || "changed"
        )
        .unwrap(),
        "original executable"
    );
}

#[test]
fn a_cache_hit_is_fenced_on_the_observation_after_lookup() {
    let versions: Versions<i32, i32> = Mutex::new(BTreeMap::from([(0, (0, "original".into()))]));
    let calls = Cell::new(0);
    let result = query(
        &versions,
        0,
        || {
            let generation = calls.get();
            calls.set(generation + 1);
            Ok(generation)
        },
        || Ok("unexpected probe".into()),
        || "changed",
    );
    assert_eq!(result, Err("changed"));
    assert_eq!(versions.lock().unwrap()[&0], (0, "original".into()));
}
