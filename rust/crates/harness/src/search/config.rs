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
///
/// Native sources are never implicit: there is no `Default` or deserialization, so only
/// [`SearchConfig::native`] reads the keychain or environment.
///
/// ```compile_fail
/// let _: acyclic_harness::search::SearchConfig = Default::default();
/// ```
pub struct SearchConfig {
    /// Explicit provider, defaulting to Exa.
    pub provider: SearchBackend,
    /// Config-file fallback keys, indexed by `exa` or `tinyfish`.
    pub api_keys: BTreeMap<String, String>,
    native: Option<Sources>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchFile {
    #[serde(default)]
    provider: SearchBackend,
    #[serde(default)]
    api_keys: BTreeMap<String, String>,
}

/// The only reader of host credential state. Test builds compile just the isolated
/// variant, so fixtures cannot reach a real keychain or environment.
enum Sources {
    #[cfg(not(test))]
    Current,
    #[cfg(test)]
    Isolated {
        keychain: Box<dyn Fn(&str) -> Result<Option<String>> + Send + Sync>,
        environment: Box<dyn Fn(&str) -> Result<Option<String>> + Send + Sync>,
    },
}

impl Sources {
    fn keychain(&self, provider: &str) -> Result<Option<String>> {
        match self {
            #[cfg(not(test))]
            Self::Current => keychain(provider),
            #[cfg(test)]
            Self::Isolated { keychain, .. } => keychain(provider),
        }
    }

    fn variable(&self, name: &str) -> Result<Option<String>> {
        match self {
            #[cfg(not(test))]
            Self::Current => Ok(std::env::var(name).ok()),
            #[cfg(test)]
            Self::Isolated { environment, .. } => environment(name),
        }
    }
}

impl SearchConfig {
    /// Uses supplied keys exclusively, without reading keychain, env, or disk.
    pub fn explicit(provider: SearchBackend, api_keys: BTreeMap<String, String>) -> Self {
        Self {
            provider,
            api_keys,
            native: None,
        }
    }

    /// Consults the current user's keychain, then the provider environment variable,
    /// ahead of configured keys. Not compiled into this crate's tests.
    #[cfg(not(test))]
    #[must_use]
    pub fn native(self) -> Self {
        Self {
            native: Some(Sources::Current),
            ..self
        }
    }

    /// Reads configured provider and keys from JSON. Missing configuration means none.
    pub fn from_file(path: &Path) -> Result<Self> {
        match std::fs::read(path) {
            Ok(bytes) => serde_json::from_slice::<SearchFile>(&bytes)
                .map(|file| Self::explicit(file.provider, file.api_keys))
                .map_err(|_| Error::Invalid("invalid search configuration".into())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(Self::explicit(SearchBackend::default(), BTreeMap::new()))
            }
            Err(_) => Err(Error::Storage("cannot read search configuration".into())),
        }
    }

    fn key(&self) -> Result<Option<String>> {
        if let Some(native) = &self.native {
            if let Some(key) = native
                .keychain(self.provider.id())?
                .filter(|k| !k.trim().is_empty())
            {
                return Ok(Some(key));
            }
            if let Some(key) = native
                .variable(self.provider.env())?
                .filter(|k| !k.trim().is_empty())
            {
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

#[cfg(not(test))]
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
