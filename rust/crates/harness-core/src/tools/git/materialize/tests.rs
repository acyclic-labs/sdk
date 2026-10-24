use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // For every ownership-loss point during preparation, no prepared value
    // reaches publication unless the owner still holds its fence afterward.
    #[test]
    fn merge_preparation_rechecks_ownership_with_negative_control(
        lose_owner in any::<bool>(), phase in 0usize..4,
    ) {
        let run = |broken| {
            let owned = std::cell::Cell::new(true);
            let mut published = false;
            let result = futures::executor::block_on(prepared(async {
                for step in 0..4 {
                    if lose_owner && phase == step { owned.set(false); }
                    std::future::ready(()).await;
                }
                Ok(())
            }, &|| std::future::ready(if broken || owned.get() { Ok(()) }
                else { Err(Error::Unauthorized("execution owner lost during merge preparation".into())) })));
            if result.is_ok() { published = true; }
            (result.is_ok(), published && !owned.get())
        };
        let (accepted, stale_publication) = run(false);
        prop_assert_eq!(accepted, !lose_owner);
        prop_assert!(!stale_publication);
        // Removing the post-preparation check exposes precisely the stale
        // publication that this property forbids.
        prop_assert_eq!(run(true).1, lose_owner);
    }
}
