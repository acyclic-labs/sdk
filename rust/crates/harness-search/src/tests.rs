use super::*;
use proptest::strategy::Strategy as _;

// The advertised schema must never be stricter than the runtime validator, and
// the validator accepts every non-blank query at any length with a positive
// limit: there is no hidden length or result cap.
proptest::proptest! {
    #[test]
    fn validation_accepts_exactly_non_blank_queries_with_a_positive_limit(
        query in proptest::collection::vec(proptest::char::any(), 0..8192)
            .prop_map(|characters| characters.into_iter().collect::<String>()),
        max_results in proptest::arbitrary::any::<u32>(),
    ) {
        let accepted = query.chars().any(|character| !character.is_whitespace()) && max_results >= 1;
        let request = SearchRequest { query: query.clone(), max_results };
        proptest::prop_assert_eq!(request.validate().is_ok(), accepted);
        if accepted {
            let schema = crate::tool::schema::input::<SearchRequest>().expect("search request schema");
            let validator = jsonschema::validator_for(&schema).expect("search request validator");
            let input = serde_json::json!({"query": query, "max_results": max_results});
            proptest::prop_assert!(validator.is_valid(&input));
        }
    }
}
