use super::*;
use acyclic_harness::task_volume::PlanName;
use proptest::prelude::*;

#[test]
fn empty_plan_stems_are_rejected_at_every_input_boundary() {
    assert!(matches!(PlanName::new(""), Err(Error::InvalidPath)));
    assert!(serde_json::from_str::<PlanName>(r#""""#).is_err());
    assert!(serde_json::from_str::<FileKey>(r#"{"Plan":""}"#).is_err());
    assert!(matches!(
        FileKey::parse("plans/.md"),
        Err(Error::InvalidPath)
    ));
    assert!(Query::folder("plans/.md").is_err());
}

proptest! {
    // Every accepted name is nonempty and survives both name and path codecs
    // exactly: no trimming, Unicode normalization or hidden fallback stem.
    #[test]
    fn accepted_plan_names_are_nonempty_and_round_trip(
        value in prop::collection::vec(any::<char>(), 0..64)
            .prop_map(|chars| chars.into_iter().collect::<String>()),
    ) {
        if let Ok(name) = PlanName::new(&value) {
            prop_assert!(!name.as_str().is_empty());
            prop_assert_eq!(name.as_str(), value.as_str());
            let decoded: PlanName = serde_json::from_slice(&serde_json::to_vec(&name)?)?;
            prop_assert_eq!(&decoded, &name);
            let key = FileKey::Plan(name);
            prop_assert_eq!(FileKey::parse(&key.path())?, key);
        }
    }

    #[test]
    fn ordinary_unicode_stems_remain_valid(stem in "[a-zA-Z0-9 λ雪_-]{1,64}") {
        let name = PlanName::new(&stem)?;
        prop_assert_eq!(name.as_str(), stem.as_str());
    }
}
