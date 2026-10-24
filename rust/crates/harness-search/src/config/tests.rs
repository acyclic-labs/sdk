use super::*;

proptest::proptest! {
    #[test]
    fn configured_gateway_preserves_adapter_endpoint_and_rejects_unsafe_urls(port in 1u16..=65535, path in "[a-z]{0,12}") {
        for provider in [SearchBackend::Exa,SearchBackend::TinyFish] {
            let mut config = SearchConfig::explicit(provider,[(provider.id().into(),"test-only-placeholder".into())].into());
            config.endpoint = Some(format!("http://127.0.0.1:{port}/{path}"));
            assert!(config.provider().unwrap().is_some());
            for endpoint in [format!("file:///tmp/{path}"),format!("http://user@127.0.0.1:{port}/{path}"),format!("http://127.0.0.1:{port}/{path}?key=secret"),format!("http://127.0.0.1:{port}/{path}#fragment")] {
                config.endpoint = Some(endpoint);
                assert!(config.provider().is_err(),"invalid configured endpoint bypassed adapter validation");
            }
        }
    }
}

#[test]
fn config_provider_names_match_keychain_accounts_and_environment_variables() -> Result<()> {
    for (id, variable) in [("exa", "EXA_API_KEY"), ("tinyfish", "TINYFISH_API_KEY")] {
        let config: SearchConfig = serde_json::from_value(serde_json::json!({"provider":id}))
            .map_err(|e| Error::Invalid(e.to_string()))?;
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
            let config = SearchConfig {
                endpoint: None,
                provider,
                api_keys: config_key
                    .map(|key| (provider.id().into(), key.into()))
                    .into_iter()
                    .collect(),
                native_sources: true,
            };
            let key = config.resolve_with(
                |id| {
                    assert_eq!(id, provider.id());
                    Ok(keychain_key.map(str::to_owned))
                },
                |name| {
                    assert!(keychain_key.is_none_or(|key| key.trim().is_empty()));
                    assert_eq!(name, provider.env());
                    Ok(env_key.map(str::to_owned))
                },
            )?;
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
        let key = config.resolve_with(
            |_| Err(Error::Invalid("keychain must not be called".into())),
            |_| Err(Error::Invalid("env must not be called".into())),
        )?;
        assert!(key.is_none());
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
    let config = SearchConfig::default();
    assert!(
        config
            .resolve_with(
                |_| Err(Error::Storage("keychain unavailable".into())),
                |_| Err(Error::Invalid("env must not be called".into())),
            )
            .is_err()
    );
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

#[test]
fn isolated_settings_never_consult_native_credentials() -> Result<()> {
    let config = SearchConfig::default().without_native_sources();
    assert_eq!(
        config.resolve_with(
            |_| panic!("isolated settings consulted keychain"),
            |_| panic!("isolated settings consulted environment"),
        )?,
        None
    );
    Ok(())
}
