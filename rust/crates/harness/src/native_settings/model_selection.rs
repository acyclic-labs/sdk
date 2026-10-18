use serde::Deserialize;

#[derive(Deserialize)]
struct ModelSettings {
    model: Option<String>,
    model_provider: Option<String>,
}

pub(super) fn parse(bytes: &[u8]) -> Result<Option<String>, String> {
    let text = std::str::from_utf8(bytes).map_err(|error| error.to_string())?;
    let settings: ModelSettings = toml::from_str(text).map_err(|error| error.to_string())?;
    let Some(model) = settings.model else {
        return Ok(None);
    };
    if model.trim().is_empty() {
        return Err("model must be a nonempty string".into());
    }
    if model.contains('/') {
        return Ok(Some(model));
    }
    let provider = settings.model_provider.as_deref().unwrap_or("openai");
    if provider.trim().is_empty() || provider.contains('/') {
        return Err("model_provider must be a provider ID".into());
    }
    Ok(Some(format!("{provider}/{model}")))
}
