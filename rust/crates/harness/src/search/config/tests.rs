use super::*;

fn isolated(
    provider: SearchBackend,
    api_keys: BTreeMap<String, String>,
    keychain: impl Fn(&str) -> Result<Option<String>> + Send + Sync + 'static,
    environment: impl Fn(&str) -> Result<Option<String>> + Send + Sync + 'static,
) -> SearchConfig {
    SearchConfig {
        native: Some(Sources::Isolated {
            keychain: Box::new(keychain),
            environment: Box::new(environment),
        }),
        ..SearchConfig::explicit(provider, api_keys)
    }
}

#[test]
fn config_provider_names_match_keychain_accounts_and_environment_variables() -> Result<()> {
    for (id, variable) in [("exa", "EXA_API_KEY"), ("tinyfish", "TINYFISH_API_KEY")] {
        let directory = tempfile::tempdir().map_err(|e| Error::Storage(e.to_string()))?;
        let path = directory.path().join("search.json");
        std::fs::write(&path, serde_json::json!({"provider":id}).to_string())
            .map_err(|e| Error::Storage(e.to_string()))?;
        let config = SearchConfig::from_file(&path)?;
        assert!(config.native.is_none());
        assert_eq!(config.provider.id(), id);
        assert_eq!(config.provider.env(), variable);
    }
    Ok(())
}

#[test]
fn credential_precedence_is_lazy_and_blank_keys_fall_through() -> Result<()> {
    for provider in [SearchBackend::Exa, SearchBackend::TinyFish] {
        for (keychain_key, env_key, config_key, expected) in [
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
            let config = isolated(
                provider,
                config_key
                    .map(|key| (provider.id().into(), key.into()))
                    .into_iter()
                    .collect(),
                move |id| {
                    assert_eq!(id, provider.id());
                    Ok(keychain_key.map(str::to_owned))
                },
                move |name| {
                    assert!(keychain_key.is_none_or(|key| key.trim().is_empty()));
                    assert_eq!(name, provider.env());
                    Ok(env_key.map(str::to_owned))
                },
            );
            let key = config.key()?;
            assert_eq!(key.as_deref(), expected);
        }
    }
    Ok(())
}

#[test]
fn explicit_configuration_never_reads_native_sources_and_hides_unconfigured_provider() -> Result<()>
{
    for provider in [SearchBackend::Exa, SearchBackend::TinyFish] {
        let config = SearchConfig::explicit(provider, BTreeMap::new());
        assert!(config.native.is_none());
        assert!(config.key()?.is_none());
        assert!(config.provider()?.is_none());
        let config = SearchConfig::explicit(
            provider,
            [(provider.id().into(), "test-only-placeholder".into())].into(),
        );
        assert!(config.provider()?.is_some());
    }
    Ok(())
}

#[test]
fn malformed_config_and_keychain_errors_do_not_disclose_secrets() -> Result<()> {
    let config = isolated(
        SearchBackend::Exa,
        BTreeMap::new(),
        |_| Err(Error::Storage("keychain unavailable".into())),
        |_| Err(Error::Invalid("env must not be called".into())),
    );
    assert!(config.key().is_err());
    let directory = tempfile::tempdir().map_err(|e| Error::Storage(e.to_string()))?;
    let path = directory.path().join("search.json");
    std::fs::write(&path, br#"{"provider":"unknown-secret","api_keys":{}}"#)
        .map_err(|e| Error::Storage(e.to_string()))?;
    let Err(error) = SearchConfig::from_file(&path) else {
        return Err(Error::Invalid("invalid config accepted".into()));
    };
    assert!(!error.to_string().contains("unknown-secret"));
    Ok(())
}
