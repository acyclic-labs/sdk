use super::*;
use proptest::prelude::*;
proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    // Archive prefixes are copied once, independent of live chunk count.
    // Resetting the projection every frame is the broken negative control.
    #[test]
    fn live_updates_copy_only_the_new_suffix(prefix in "[a-z]{32,256}", count in 8usize..32) {
        let task = crate::TaskId::new();
        let key = format!("AGGREGATE_KIND_TASK:{task}");
        let mut views = super::super::ClientViews::default();
        let mut projection = Projection { owner: "owner".into(), after: 1,
            revision: 1, segments: vec![std::sync::Arc::from(prefix.as_str())], ..Default::default() };
        let mut actual = 0;
        let mut broken = 0;
        for index in 0..count {
            views.transcripts.entry(key.clone()).or_default().streaming_text.push_str("abcdefgh");
            actual += projection.apply(&mut views, Some(task));
            let mut reset = Projection { owner: "broken".into(), after: 1,
                revision: 1, segments: vec![std::sync::Arc::from(prefix.as_str())], ..Default::default() };
            broken += reset.apply(&mut views, Some(task));
            prop_assert_eq!(actual, prefix.len() + (index + 1) * 8);
        }
        prop_assert!(broken > actual * 2, "negative control must expose archive and live prefix copying");
        prop_assert_eq!(&views.transcripts["owner"].streaming_text, &format!("{prefix}\n\n{}", "abcdefgh".repeat(count)));
    }
}
