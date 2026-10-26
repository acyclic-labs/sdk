//! First-party OpenAI-compatible routes use the shared Chat adapter and request pipeline.
//! Register exact model capacities before resolving. Automatic context admission still
//! requires a verified token counter; generation itself does not require a count endpoint.

use crate::{Adapter, Authentication, ProviderSpec, Result, RetrySpec, TimeoutSpec, invalid};
use std::collections::BTreeMap;

fn chat(base_url: String, authentication: Authentication) -> Result<ProviderSpec> {
    crate::registry::validate_url(&base_url)?;
    Ok(ProviderSpec {
        adapter: Adapter::Chat,
        authentication,
        base_url,
        headers: BTreeMap::new(),
        models: BTreeMap::new(),
        token_counter: None,
        max_event_bytes: 1024 * 1024,
        retry: RetrySpec::DEFAULT,
        timeouts: TimeoutSpec::DEFAULT,
    })
}

/// OpenRouter's Chat API; the key is supplied separately through `Credentials`.
pub fn openrouter() -> Result<ProviderSpec> {
    chat(
        "https://openrouter.ai/api/v1".into(),
        Authentication::ApiKey,
    )
}

/// Amazon Bedrock Runtime's first-party Chat API with a Bedrock bearer API key.
/// Model support is region-specific. SigV4 credentials belong in a signing transport.
pub fn bedrock(region: &str) -> Result<ProviderSpec> {
    component(region)?;
    chat(
        format!("https://bedrock-runtime.{region}.amazonaws.com/openai/v1"),
        Authentication::ApiKey,
    )
}

/// Vertex's first-party Chat route. Supply a short-lived Google OAuth access token
/// as the explicit credential and resolve again after renewing it.
pub fn vertex(project: &str, location: &str) -> Result<ProviderSpec> {
    component(project)?;
    component(location)?;
    let host = if location == "global" {
        "aiplatform.googleapis.com".to_owned()
    } else {
        format!("{location}-aiplatform.googleapis.com")
    };
    chat(
        format!("https://{host}/v1beta1/projects/{project}/locations/{location}/endpoints/openapi"),
        Authentication::ApiKey,
    )
}

/// Ollama, llama.cpp, vLLM or another server exposing OpenAI Chat Completions.
/// HTTP is accepted only on loopback, using the same URL validation as every provider.
pub fn local(base_url: &str) -> Result<ProviderSpec> {
    chat(base_url.to_owned(), Authentication::Optional)
}

fn component(value: &str) -> Result<()> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        return Err(invalid(
            "endpoint component must contain only letters, digits and hyphens",
        ));
    }
    Ok(())
}
