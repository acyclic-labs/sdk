use serde::Deserialize;

#[derive(Deserialize)]
struct ModelSettings {
    model: Option<String>,
    model_provider: Option<String>,
}

struct Selection {
    provider: String,
    name: String,
}

fn normalize(settings: ModelSettings) -> Result<Option<Selection>, String> {
    let Some(model) = settings.model else {
        return Ok(None);
    };
    if model.trim().is_empty() {
        return Err("model must be a nonempty string".into());
    }
    let provider = settings.model_provider;
    #[cfg(test)]
    let provider =
        if tests::DROP_CONFIGURED_PROVIDER.with(std::cell::Cell::get) && model.contains('/') {
            None
        } else {
            provider
        };
    let (provider, name) = match provider {
        Some(provider) => (provider, model),
        None => match model.split_once('/') {
            Some((provider, name)) => (provider.to_owned(), name.to_owned()),
            None => ("openai".into(), model),
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

pub(super) fn parse(bytes: &[u8]) -> Result<Option<String>, String> {
    let text = std::str::from_utf8(bytes).map_err(|error| error.to_string())?;
    let settings: ModelSettings = toml::from_str(text).map_err(|error| error.to_string())?;
    Ok(normalize(settings)?.map(|selection| format!("{}/{}", selection.provider, selection.name)))
}

#[cfg(test)]
mod tests;
