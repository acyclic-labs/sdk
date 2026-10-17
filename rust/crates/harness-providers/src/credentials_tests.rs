use super::*;
use crate::{HttpRequest, HttpResponse, HttpTransport, Registry};
use acyclic_stream::BoxProviderFuture;
use std::{path::Path, sync::Arc};

struct Offline;

impl HttpTransport for Offline {
    fn send(&self, _: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        Box::pin(async { Err(crate::invalid("credential fixtures never send")) })
    }
}

fn io(error: std::io::Error) -> acyclic_harness::Error {
    crate::invalid(error.to_string())
}

fn map(entries: &[(&str, &str)]) -> BTreeMap<String, String> {
    entries
        .iter()
        .map(|(name, value)| ((*name).into(), (*value).into()))
        .collect()
}

/// Fake keychain, environment and home: the only native sources a test build can hold.
fn isolated(
    configured: &[(&str, &str)],
    keychain: &[(&str, &str)],
    environment: &[(&str, &str)],
    home: &Path,
) -> Credentials {
    Credentials {
        native: Some(NativeSources(Sources::Isolated {
            keychain: map(keychain),
            environment: map(environment),
            home: home.into(),
        })),
        ..Credentials::explicit(map(configured))
    }
}

fn login(root: &Path) -> Result<()> {
    std::fs::create_dir_all(root).map_err(io)?;
    std::fs::write(root.join("auth.json"), b"{}").map_err(io)
}

fn uses_codex_login(credentials: Credentials) -> Result<bool> {
    let resolved = Registry::with_transport(credentials, Arc::new(Offline)).resolve("openai/gpt-5");
    match resolved {
        Ok(model) => {
            assert!(model.key.is_none());
            assert_eq!(
                model.config.base_url,
                "https://chatgpt.com/backend-api/codex"
            );
            Ok(model.codex_login.is_some())
        }
        Err(acyclic_harness::Error::Invalid(reason)) => {
            assert_eq!(reason, "model provider API key is unavailable");
            Ok(false)
        }
        Err(error) => Err(error),
    }
}

#[test]
fn native_sources_precede_configuration_and_blank_keys_fall_through() -> Result<()> {
    let home = tempfile::tempdir().map_err(io)?;
    for (provider, variable) in [
        ("anthropic", "ANTHROPIC_API_KEY"),
        ("openai", "OPENAI_API_KEY"),
        ("openrouter", "OPENROUTER_API_KEY"),
        ("gemini", "GEMINI_API_KEY"),
    ] {
        for (keychain, environment, configured, expected) in [
            (
                Some("keychain"),
                Some("env"),
                Some("config"),
                Some("keychain"),
            ),
            (None, Some("env"), Some("config"), Some("env")),
            (Some(" \n"), Some(""), Some("config"), Some("config")),
            (None, None, Some(" \t"), None),
            (None, None, None, None),
        ] {
            let credentials = isolated(
                &configured
                    .map(|key| (provider, key))
                    .into_iter()
                    .collect::<Vec<_>>(),
                &keychain
                    .map(|key| (provider, key))
                    .into_iter()
                    .collect::<Vec<_>>(),
                &environment
                    .map(|key| (variable, key))
                    .into_iter()
                    .collect::<Vec<_>>(),
                home.path(),
            );
            assert_eq!(credentials.api_key(provider)?.as_deref(), expected);
        }
    }
    let gemini = isolated(&[], &[], &[("GOOGLE_API_KEY", "google")], home.path());
    assert_eq!(gemini.api_key("gemini")?.as_deref(), Some("google"));
    // Variables belong to their provider; unknown providers read configuration only.
    let other = isolated(&[], &[], &[("OPENAI_API_KEY", "openai")], home.path());
    assert_eq!(other.api_key("anthropic")?, None);
    assert_eq!(other.api_key("custom")?, None);
    Ok(())
}

#[test]
fn codex_login_is_found_only_through_enabled_sources_and_codex_home_wins() -> Result<()> {
    let home = tempfile::tempdir().map_err(io)?;
    let codex_home = tempfile::tempdir().map_err(io)?;
    let codex_home_text = codex_home
        .path()
        .to_str()
        .ok_or_else(|| crate::invalid("path"))?;
    assert!(!uses_codex_login(isolated(&[], &[], &[], home.path()))?);
    login(&home.path().join(".codex"))?;
    assert!(uses_codex_login(isolated(&[], &[], &[], home.path()))?);
    // Same files on disk: explicit credentials have no sources, so nothing is discovered.
    assert!(!uses_codex_login(Credentials::explicit(BTreeMap::new()))?);
    // An empty CODEX_HOME hides the home login rather than falling back to it.
    let selected = [("CODEX_HOME", codex_home_text)];
    assert!(!uses_codex_login(isolated(
        &[],
        &[],
        &selected,
        home.path()
    ))?);
    login(codex_home.path())?;
    std::fs::remove_file(home.path().join(".codex/auth.json")).map_err(io)?;
    assert!(uses_codex_login(isolated(
        &[],
        &[],
        &selected,
        home.path()
    ))?);
    // An API key from native sources outranks the Codex login.
    let keyed = isolated(&[], &[("openai", "fixture-key")], &selected, home.path());
    let model = Registry::with_transport(keyed, Arc::new(Offline)).resolve("openai/gpt-5")?;
    assert_eq!(model.key.as_deref(), Some("fixture-key"));
    assert!(model.codex_login.is_none());
    Ok(())
}

#[test]
fn credential_files_hold_configured_keys_only() -> Result<()> {
    let directory = tempfile::tempdir().map_err(io)?;
    let missing = Credentials::from_file(&directory.path().join("missing.json"))?;
    assert!(missing.native_sources().is_none());
    assert_eq!(missing.api_key("openai")?, None);
    let path = directory.path().join("credentials.json");
    std::fs::write(&path, br#"{"api_keys":{"openai":"configured"}}"#).map_err(io)?;
    let configured = Credentials::from_file(&path)?;
    assert!(configured.native_sources().is_none());
    assert_eq!(configured.api_key("openai")?.as_deref(), Some("configured"));
    for invalid in [
        &br#"{"api_keys":{},"native_sources":"secret-value"}"#[..],
        br#"{}"#,
    ] {
        std::fs::write(&path, invalid).map_err(io)?;
        let Err(error) = Credentials::from_file(&path) else {
            return Err(crate::invalid("invalid credential file accepted"));
        };
        assert!(!error.to_string().contains("secret-value"));
    }
    Ok(())
}
