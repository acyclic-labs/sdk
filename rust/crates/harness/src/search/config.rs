use super::{Exa, SearchProvider, TinyFish};
use crate::{Error, Result};
use serde::Deserialize;
use std::{collections::BTreeMap, path::Path, sync::Arc};

/// The selected search adapter. Selection never silently switches providers.
#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchBackend {
    /// Exa Search.
    #[default]
    Exa,
    /// TinyFish Search.
    #[serde(rename = "tinyfish")]
    TinyFish,
}

impl SearchBackend {
    fn id(self) -> &'static str {
        match self {
            Self::Exa => "exa",
            Self::TinyFish => "tinyfish",
        }
    }

    fn env(self) -> &'static str {
        match self {
            Self::Exa => "EXA_API_KEY",
            Self::TinyFish => "TINYFISH_API_KEY",
        }
    }
}

/// Host-only configuration. Deliberately neither Debug nor Serialize.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchConfig {
    /// Explicit provider, defaulting to Exa.
    #[serde(default)]
    pub provider: SearchBackend,
    /// Config-file fallback keys, indexed by `exa` or `tinyfish`.
    #[serde(default)]
    pub api_keys: BTreeMap<String, String>,
    #[serde(skip, default = "native_sources")]
    native_sources: bool,
}

fn native_sources() -> bool {
    true
}

impl Default for SearchConfig {
    fn default() -> Self {
        Self {
            provider: SearchBackend::default(),
            api_keys: BTreeMap::new(),
            native_sources: true,
        }
    }
}

impl SearchConfig {
    /// Uses supplied keys exclusively, without reading keychain, env, or disk.
    pub fn explicit(provider: SearchBackend, api_keys: BTreeMap<String, String>) -> Self {
        Self {
            provider,
            api_keys,
            native_sources: false,
        }
    }

    /// Reads JSON configuration. Missing configuration means no file fallback.
    pub fn from_file(path: &Path) -> Result<Self> {
        match std::fs::read(path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map_err(|_| Error::Invalid("invalid search configuration".into())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(_) => Err(Error::Storage("cannot read search configuration".into())),
        }
    }

    fn key(&self) -> Result<Option<String>> {
        self.resolve_with(keychain, |variable| {
            Ok(crate::native_environment::variable(variable)
                .and_then(|value| value.into_string().ok()))
        })
    }

    fn resolve_with(
        &self,
        keychain: impl FnOnce(&str) -> Result<Option<String>>,
        environment: impl FnOnce(&str) -> Result<Option<String>>,
    ) -> Result<Option<String>> {
        if self.native_sources {
            if let Some(key) = keychain(self.provider.id())?.filter(|k| !k.trim().is_empty()) {
                return Ok(Some(key));
            }
            if let Some(key) = environment(self.provider.env())?.filter(|k| !k.trim().is_empty()) {
                return Ok(Some(key));
            }
        }
        Ok(self
            .api_keys
            .get(self.provider.id())
            .filter(|k| !k.trim().is_empty())
            .cloned())
    }

    /// Resolves the worker's private keychain → environment → config credential.
    pub fn provider(&self) -> Result<Option<Arc<dyn SearchProvider>>> {
        let Some(key) = self.key()? else {
            return Ok(None);
        };
        let provider: Arc<dyn SearchProvider> = match self.provider {
            SearchBackend::Exa => Arc::new(Exa::new(&key)?),
            SearchBackend::TinyFish => Arc::new(TinyFish::new(&key)?),
        };
        Ok(Some(provider))
    }
}

fn keychain(provider: &str) -> Result<Option<String>> {
    let entry = keyring::Entry::new("acyclic-harness", provider)
        .map_err(|_| Error::Storage("cannot open search keychain entry".into()))?;
    match entry.get_password() {
        Ok(key) => Ok(Some(key)),
        Err(
            keyring::Error::NoEntry
            | keyring::Error::NoStorageAccess(_)
            | keyring::Error::PlatformFailure(_),
        ) => Ok(None),
        Err(_) => Err(Error::Storage(
            "cannot decode search keychain credential".into(),
        )),
    }
}

#[cfg(test)]
mod tests;
