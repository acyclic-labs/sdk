use serde::Deserialize;

#[derive(Deserialize)]
struct ModelSettings {
    model: Option<String>,
    model_provider: Option<String>,
    compaction: Option<crate::context::CompactionPolicy>,
}

struct Selection<'a> {
    provider: &'a str,
    name: &'a str,
}

fn normalize(settings: &ModelSettings) -> Result<Option<Selection<'_>>, String> {
    let Some(model) = settings.model.as_deref() else {
        return Ok(None);
    };
    if model.trim().is_empty() {
        return Err("model must be a nonempty string".into());
    }
    let provider = settings.model_provider.as_deref();
    let (provider, name) = match provider {
        Some(provider) => (provider, model),
        None => match model.split_once('/') {
            Some((provider, name)) => (provider, name),
            None => ("openai", model),
        },
    };
    if provider.trim().is_empty() || provider.contains('/') {
        return Err("model_provider must be a provider ID".into());
    }
    if name.trim().is_empty() {
        return Err("model must be a nonempty string".into());
    }
    Ok(Some(Selection { provider, name }))
}

#[cfg(any(test, feature = "test-support"))]
pub(super) fn parse(bytes: &[u8]) -> Result<Option<String>, String> {
    parse_preferences(bytes, true, false).map(|preferences| preferences.model)
}

pub(super) fn parse_preferences(
    bytes: &[u8],
    model: bool,
    compaction: bool,
) -> Result<super::ModelPreferences, String> {
    let text = std::str::from_utf8(bytes).map_err(|error| error.to_string())?;
    let mut values: toml::Table = toml::from_str(text).map_err(|error| error.to_string())?;
    if !model {
        values.remove("model");
        values.remove("model_provider");
    }
    if !compaction {
        values.remove("compaction");
    }
    let settings: ModelSettings = toml::Value::Table(values)
        .try_into()
        .map_err(|error| error.to_string())?;
    let model =
        normalize(&settings)?.map(|selection| format!("{}/{}", selection.provider, selection.name));
    Ok(super::ModelPreferences {
        model,
        compaction: settings.compaction,
    })
}
