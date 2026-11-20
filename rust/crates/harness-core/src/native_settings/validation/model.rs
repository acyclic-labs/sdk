use super::{CheckResult, selection};

fn check(bytes: &[u8]) -> CheckResult {
    if let Ok(selected) = selection::parse(bytes) {
        let text = std::str::from_utf8(bytes)?;
        let original: toml::Value = toml::from_str(text)?;
        let model = original
            .get("model")
            .map(|value| value.as_str().ok_or("admitted model is not a string"))
            .transpose()?;
        if let Some(selected) = selected {
            let model = model.ok_or("selection has no model")?;
            assert!(!model.trim().is_empty());
            if let Some(provider) = original.get("model_provider") {
                let provider = provider
                    .as_str()
                    .ok_or("admitted provider is not a string")?;
                assert!(!provider.trim().is_empty() && !provider.contains('/'));
                assert_eq!(
                    selected,
                    format!("{provider}/{model}"),
                    "provider and opaque model stay paired"
                );
            }
            let canonical = toml::to_string(&std::collections::BTreeMap::from([(
                "model",
                selected.as_str(),
            )]))?;
            assert_eq!(selection::parse(canonical.as_bytes())?, Some(selected));
        } else {
            assert!(model.is_none(), "a configured model must not disappear");
        }
    }
    Ok(())
}

/// Exercises the production admission contract.
///
/// # Panics
/// Panics if an admission invariant fails.
pub fn exercise(bytes: &[u8]) {
    let result = check_generated(bytes);
    assert!(result.is_ok(), "model admission check failed: {result:?}");
}

fn check_generated(bytes: &[u8]) -> CheckResult {
    check(bytes)?;
    // Original TOML writer exercises quoting without manufacturing host output.
    let name = String::from_utf8_lossy(bytes);
    let opaque = format!("vendor/{name}/model");
    for model in [name.as_ref(), opaque.as_str()] {
        for provider in [None, Some("oracle")] {
            let mut fields = std::collections::BTreeMap::from([("model", model)]);
            if let Some(provider) = provider {
                fields.insert("model_provider", provider);
            }
            let encoded = toml::to_string(&fields)?;
            check(encoded.as_bytes())?;
            if provider.is_some() && !model.trim().is_empty() {
                assert_eq!(
                    selection::parse(encoded.as_bytes())?,
                    Some(format!("oracle/{model}")),
                    "an explicit provider must retain the entire model name"
                );
            }
        }
    }
    static ADMISSION: std::sync::Once = std::sync::Once::new();
    ADMISSION.call_once(|| {
        for (input, expected) in [
            ("", Some(None)),
            ("model = 'tiny'", Some(Some("openai/tiny"))),
            ("model = 'vendor/tiny'", Some(Some("vendor/tiny"))),
            ("model = ''", None),
            ("model = 1", None),
            ("model = 'v/m'\nmodel_provider = ''", None),
            ("model = 'v/m'\nmodel_provider = 'v/m'", None),
            ("model = '/tiny'", None),
            ("model = 'vendor/'", None),
        ] {
            assert_eq!(
                selection::parse(input.as_bytes()).ok(),
                expected.map(|value| value.map(str::to_owned)),
                "model settings admission changed"
            );
        }
    });
    Ok(())
}
