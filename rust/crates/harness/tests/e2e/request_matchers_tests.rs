use super::matchers::{model_requests, ordered};
use proptest::prelude::*;
use serde_json::{Value, json};

fn request(parts: Vec<Value>) -> Value {
    json!({"messages":[{"role":"user","content":parts}]})
}

#[test]
fn image_only_requests_reject_inserted_empty_text_and_reasoning() {
    let expected = request(vec![json!({"kind":"file","file":{"path":"image.png"}})]);
    let mut actual = expected.clone();
    actual["messages"][0]["content"][0]["file"]["digest"] = "pinned".into();
    assert!(model_requests(&[expected.clone()], &[actual.clone()]).is_ok());
    for position in 0..=1 {
        for extra in [
            json!({"kind":"text","text":""}),
            json!({"kind":"text","text":"UNEXPECTED TEXT IN IMAGE-ONLY INPUT"}),
            json!({"kind":"reasoning","block":{"text":"injected"}}),
        ] {
            let mut broken = actual.clone();
            broken["messages"][0]["content"]
                .as_array_mut()
                .unwrap()
                .insert(position, extra);
            // Negative control: the former comparison accepts this same payload,
            // retaining the original image expectation and all its metadata.
            assert!(ordered(&[expected.clone()], &[broken.clone()], "legacy").is_ok());
            assert!(model_requests(&[expected.clone()], &[broken]).is_err());
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // Every declared message has a complete ordered content shape. Metadata
    // within a part may be a subset, but inserting/removing parts never matches.
    #[test]
    fn complete_content_shapes_reject_every_insertion_and_removal(
        parts in proptest::collection::vec((any::<bool>(), ".{0,16}"), 0..6),
        text in ".{0,16}",
    ) {
        let parts: Vec<_> = parts.into_iter().map(|(file, text)| if file {
            json!({"kind":"file","file":{"path":text}})
        } else { json!({"kind":"text","text":text}) }).collect();
        let expected = request(parts.clone());
        let decoded: Value = serde_json::from_slice(&serde_json::to_vec(&expected)?)?;
        prop_assert!(model_requests(&[expected.clone()], &[decoded]).is_ok());
        for position in 0..=parts.len() {
            for extra in [json!({"kind":"text","text":""}), json!({"kind":"text","text":text})] {
                let mut actual = parts.clone();
                actual.insert(position, extra);
                prop_assert!(model_requests(&[expected.clone()], &[request(actual)]).is_err());
            }
        }
        for position in 0..parts.len() {
            let mut actual = parts.clone(); actual.remove(position);
            prop_assert!(model_requests(&[expected.clone()], &[request(actual)]).is_err());
        }
        let mut reversed = parts.clone(); reversed.reverse();
        if reversed != parts {
            prop_assert!(model_requests(&[expected], &[request(reversed)]).is_err());
        }
    }
}

#[tokio::test]
async fn scripted_context_insertions_fail_the_real_request_oracle() -> crate::TestResult {
    let bytes = include_bytes!("../scenarios/core/content-shape.json");
    crate::run(crate::Scenario::parse(bytes)?).await?;
    for position in 0..=2 {
        for text in ["", "UNEXPECTED TEXT"] {
            let mut value: Value = serde_json::from_slice(bytes)?;
            value["context"][0]["content"]
                .as_array_mut()
                .unwrap()
                .insert(position, json!({"kind":"text","text":text}));
            let case = crate::Scenario::from_value(value)?;
            let error = crate::run(case)
                .await
                .expect_err("inserted context part passed");
            assert!(error.to_string().contains("requests[0]"), "{error}");
        }
    }
    Ok(())
}
