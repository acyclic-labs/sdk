use super::*;
use proptest::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Copied {
    value: i32,
    copies: Arc<AtomicUsize>,
}
impl Clone for Copied {
    fn clone(&self) -> Self {
        self.copies.fetch_add(1, Ordering::Relaxed);
        Self {
            value: self.value,
            copies: Arc::clone(&self.copies),
        }
    }
}

/// Exercise the actual immutable frontier, including retaining and retiring
/// the old exact cut. No reconstructed map or estimated copy count is measured.
#[test]
fn one_field_edit_copies_one_value_at_n_and_16n() {
    for population in [1_024, 16_384] {
        let copies = Arc::new(AtomicUsize::new(0));
        let mut fields: StateMap<_> = (0..population)
            .map(|index| {
                (
                    format!("field-{index:08x}"),
                    Copied {
                        value: index,
                        copies: Arc::clone(&copies),
                    },
                )
            })
            .collect();
        let before = copies.load(Ordering::Relaxed);
        let original = fields.clone();
        assert_eq!(copies.load(Ordering::Relaxed), before);
        fields
            .get_mut("field-00000000")
            .expect("original field")
            .value = -1;
        assert_eq!(copies.load(Ordering::Relaxed) - before, 1);
        assert_eq!(
            original.get("field-00000000").map(|value| value.value),
            Some(0)
        );
        assert_eq!(
            fields.get("field-00000000").map(|value| value.value),
            Some(-1)
        );
        assert_eq!(fields.len(), population as usize);
        drop(original);
        assert_eq!(
            fields.get("field-00000000").map(|value| value.value),
            Some(-1)
        );
    }
}

proptest! {
    #[test]
    fn immutable_edits_and_ordered_seeks_match_complete_string_keys(
        edits in prop::collection::vec((0_u8..3, any::<String>(), any::<i32>()), 0..150)
    ) {
        let mut fields = StateMap::default();
        let mut expected = BTreeMap::new();
        for (action, key, value) in edits {
            let retained = fields.clone();
            let original = expected.clone();
            match action {
                0 => prop_assert_eq!(fields.insert(key.clone(), value), expected.insert(key.clone(), value)),
                1 => prop_assert_eq!(fields.remove(&key), expected.remove(&key)),
                _ => {
                    if let Some(field) = fields.get_mut(&key) {*field = value;}
                    if let Some(field) = expected.get_mut(&key) {*field = value;}
                }
            }
            prop_assert_eq!(fields.len(), expected.len());
            prop_assert_eq!(fields.iter().collect::<Vec<_>>(), expected.iter().collect::<Vec<_>>());
            prop_assert_eq!(retained.iter().collect::<Vec<_>>(), original.iter().collect::<Vec<_>>());
            for lower in [std::ops::Bound::Included(key.as_str()), std::ops::Bound::Excluded(key.as_str())] {
                prop_assert_eq!(fields.range((lower, std::ops::Bound::Unbounded)).collect::<Vec<_>>(), expected.range::<str,_>((lower, std::ops::Bound::Unbounded)).collect::<Vec<_>>());
            }
        }
    }
}
