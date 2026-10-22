#[cfg(test)]
#[path = "matchers_properties.rs"]
mod properties;

pub use super::{ordered_matcher::ordered, scenario_matchers::subset};

#[test]
fn recursive_subsets_keep_types_null_and_array_order() {
    use serde_json::json;
    assert!(subset(
        &json!({"a":{"b":1}}),
        &json!({"a":{"b":1,"c":2},"d":3})
    ));
    assert!(subset(
        &json!([{"id":1},{"id":3}]),
        &json!([{"id":1},2,{"id":3}])
    ));
    for (expected, actual) in [
        (json!({"a":null}), json!({})),
        (json!({"a":1}), json!({"a":"1"})),
        (json!([2, 1]), json!([1, 2])),
        (json!([1, 1]), json!([1])),
        (json!([]), json!([1])),
        (json!({}), json!([])),
    ] {
        assert!(
            !subset(&expected, &actual),
            "{expected} unexpectedly matched {actual}"
        );
    }
}

#[test]
fn ordered_matchers_do_not_reuse_observations() {
    use serde_json::json;
    assert!(ordered(&[], &[], "frames").is_ok());
    assert!(ordered(&[], &[json!(1)], "frames").is_err());
    assert!(
        ordered(
            &[json!({"kind":"a"}), json!({"kind":"a"})],
            &[json!({"kind":"a"})],
            "frames"
        )
        .is_err()
    );
    assert!(ordered(&[json!(2)], &[json!(1), json!(2)], "frames").is_ok());
    assert!(ordered(&[json!(2), json!(1)], &[json!(1), json!(2)], "frames").is_err());
}

#[test]
fn json_text_matchers_keep_subset_order_and_reject_invalid_encodings() {
    use serde_json::json;
    assert!(subset(
        &json!({"$json":[{"name":"first"},{"name":"last"}]}),
        &json!(r#"[{"name":"first","extra":1},{"name":"middle"},{"name":"last"}]"#)
    ));
    assert!(subset(&json!({"$json":[]}), &json!("[]")));
    for (expected, actual) in [
        (json!({"$json":[]}), json!("[1]")),
        (json!({"$json":{"name":"a"}}), json!("not JSON")),
        (json!({"$json":[2,1]}), json!("[1,2]")),
        (json!({"$json":[1,1]}), json!("[1]")),
        (json!({"$json":{"name":"a"}}), json!({"name":"a"})),
        (json!({"$json":[],"extra":true}), json!("[]")),
        (json!({"name":"a"}), json!(r#"{"name":"a"}"#)),
    ] {
        assert!(!subset(&expected, &actual), "{expected} matched {actual}");
    }
}

#[test]
fn text_matchers_find_fragments_in_nested_content_without_vacuous_matches() {
    use serde_json::json;
    for actual in [
        json!("SECRET BODY"),
        json!("before SECRET BODY after"),
        json!([{"kind":"text","text":"before SECRET BODY after"}]),
        json!({"content":{"nested":["before SECRET BODY after"]}}),
    ] {
        assert!(subset(&json!({"$contains":"SECRET BODY"}), &actual));
    }
    for (expected, actual) in [
        (json!({"$contains":"SECRET BODY"}), json!("public metadata")),
        (json!({"$contains":"SECRET BODY"}), json!(null)),
        (json!({"$contains":""}), json!("anything")),
        (json!({"$contains":7}), json!("7")),
    ] {
        assert!(!subset(&expected, &actual), "{expected} matched {actual}");
    }
}
