//! Independent identity oracles for the actual generation and counting codecs.
use super::*;
use proptest::prelude::*;

thread_local! {
    pub(crate) static SUBSTITUTE_WIRE_MODEL: std::cell::Cell<bool> = const {std::cell::Cell::new(false)};
}

fn projected_identity(adapter: Adapter, name: &str, counting: bool) {
    let (mut provider, _) = provider(adapter, 200);
    provider.model.name = name.into();
    let (request, _) = request(&provider).unwrap();
    let body = projection::body_named(
        &provider,
        request.request(),
        &crate::tool_names::Names::new(request.request()),
        &provider.reasoning_origin(None, None),
        &mut Default::default(),
    )
    .unwrap();
    if counting {
        let count =
            serde_json::to_value(crate::counting::wire::body(&body, name, adapter)).unwrap();
        let actual = if adapter == Adapter::Gemini {
            count["generateContentRequest"]["model"]
                .as_str()
                .unwrap()
                .to_owned()
        } else {
            count["model"].as_str().unwrap().to_owned()
        };
        let expected = if adapter == Adapter::Gemini {
            format!("models/{name}")
        } else {
            name.into()
        };
        assert_eq!(
            actual, expected,
            "counting codec preserves the resolved model"
        );
    } else if adapter == Adapter::Gemini {
        assert_eq!(
            projection::endpoint(&provider).unwrap(),
            format!("http://127.0.0.1:1234/v1/models/{name}:streamGenerateContent?alt=sse"),
            "generation codec preserves the resolved model"
        );
    } else {
        let generation = serde_json::to_value(body).unwrap();
        assert_eq!(
            generation["model"], name,
            "generation codec preserves the resolved model"
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn every_count_and_generation_codec_preserves_its_opaque_selection(
        segments in proptest::collection::vec("[a-z]{1,12}", 1..5),
    ) {
        for adapter in [Adapter::Responses, Adapter::Chat, Adapter::Anthropic, Adapter::Gemini] {
            // Gemini's protocol uses one path segment rather than an opaque body name.
            let name = if adapter == Adapter::Gemini { segments[0].clone() } else { segments.join("/") };
            for counting in [false, true] { projected_identity(adapter, &name, counting); }
        }
    }
}

#[test]
fn negative_controls_detect_production_model_substitution_in_both_codecs() {
    for adapter in [
        Adapter::Responses,
        Adapter::Chat,
        Adapter::Anthropic,
        Adapter::Gemini,
    ] {
        for counting in [false, true] {
            SUBSTITUTE_WIRE_MODEL.with(|flag| flag.set(true));
            let detected = std::panic::catch_unwind(|| {
                projected_identity(adapter, "selected-model", counting)
            });
            SUBSTITUTE_WIRE_MODEL.with(|flag| flag.set(false));
            let panic =
                detected.expect_err("substituted wire identity must fail the same codec oracle");
            let text = panic
                .downcast_ref::<String>()
                .map(String::as_str)
                .unwrap_or("");
            assert!(
                text.contains("codec preserves the resolved model"),
                "{text}"
            );
        }
    }
}
