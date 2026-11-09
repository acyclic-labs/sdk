use acyclic_harness_core::Result;
use std::collections::BTreeMap;

/// API keys loaded from explicitly supplied configuration or native credential sources.
/// Anthropic accepts API keys only; no subscription login is read.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Credentials {
    /// Provider ID to API key. Deliberately not Debug or Serialize.
    pub api_keys: BTreeMap<String, String>,
    #[serde(skip, default = "native_sources")]
    native_sources: bool,
    #[cfg(all(test, not(target_arch = "wasm32")))]
    #[serde(skip)]
    pub(crate) source: Option<std::sync::Arc<dyn Fn(&str, Source) -> KeyResult + Send + Sync>>,
}

pub(crate) type KeyResult = std::result::Result<Option<String>, KeychainDecode>;

/// A failed observation remains attached to its provider in the snapshot.
#[derive(Clone)]
pub(crate) struct KeychainDecode {
    pub(crate) provider: String,
}

impl KeychainDecode {
    pub(crate) fn error(&self) -> acyclic_harness_core::Error {
        super::invalid(format!(
            "provider {}: cannot decode keychain credential",
            self.provider
        ))
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum Source {
    #[cfg(not(target_arch = "wasm32"))]
    Keychain,
    #[cfg(not(target_arch = "wasm32"))]
    Environment,
    Configuration,
    /// The test double's CODEX_HOME, so tests never discover the user's login.
    #[cfg(all(test, not(target_arch = "wasm32")))]
    CodexHome,
}

fn native_sources() -> bool {
    true
}

impl Default for Credentials {
    fn default() -> Self {
        Self {
            api_keys: BTreeMap::new(),
            native_sources: true,
            #[cfg(all(test, not(target_arch = "wasm32")))]
            source: None,
        }
    }
}

impl Credentials {
    /// Read-only availability check. Never formats a backend error, which may
    /// contain protected platform details.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn keychain_available() -> bool {
        keyring::Entry::new("acyclic-harness", "doctor")
            .is_ok_and(|entry| matches!(entry.get_password(), Ok(_) | Err(keyring::Error::NoEntry)))
    }

    /// Stores a host credential in the same OS service used by discovery.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn store_api_key(provider: &str, key: &str) -> Result<()> {
        if key.trim().is_empty() {
            return Err(super::invalid(
                "API key is empty; recovery: paste a provider API key",
            ));
        }
        keyring::Entry::new("acyclic-harness", provider)
            .and_then(|entry| entry.set_password(key.trim()))
            .map_err(|_| super::invalid("cannot store provider key in OS keychain service acyclic-harness; recovery: unlock the OS keychain and run graphcoder again"))
    }

    /// Removes only this provider's entry, for host credential lifecycle owners.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn remove_api_key(provider: &str) -> Result<()> {
        match keyring::Entry::new("acyclic-harness", provider)
            .and_then(|entry| entry.delete_credential())
        {
            Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
            Err(_) => Err(super::invalid(
                "cannot remove provider key; recovery: unlock the OS keychain",
            )),
        }
    }

    /// The source after keychain, environment and configuration: an existing
    /// Codex CLI login, discovered only when native sources are allowed.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn codex_login(&self) -> Option<crate::CodexLogin> {
        if !self.native_sources {
            return None;
        }
        #[cfg(test)]
        if let Some(read) = &self.source {
            let root = read("", Source::CodexHome).ok().flatten()?;
            return crate::CodexLogin::in_home(std::path::Path::new(&root));
        }
        crate::CodexLogin::discover()
    }
    /// Uses only supplied keys, with no keychain, environment or filesystem reads.
    pub fn explicit(api_keys: BTreeMap<String, String>) -> Self {
        Self {
            api_keys,
            native_sources: false,
            #[cfg(all(test, not(target_arch = "wasm32")))]
            source: None,
        }
    }
    /// Resolves OS keychain, then environment, then this configuration.
    pub fn api_key(&self, provider: &str) -> Result<Option<String>> {
        self.observe_key(provider)
            .map_err(|failure| failure.error())
    }

    pub(crate) fn observe_key(&self, provider: &str) -> KeyResult {
        if self.native_sources {
            #[cfg(not(target_arch = "wasm32"))]
            for source in [Source::Keychain, Source::Environment] {
                if let Some(key) = self.read(provider, source)? {
                    if !key.trim().is_empty() {
                        return Ok(Some(key));
                    }
                }
            }
        }
        Ok(self
            .read(provider, Source::Configuration)?
            .filter(|key| !key.trim().is_empty()))
    }

    fn read(&self, provider: &str, source: Source) -> KeyResult {
        #[cfg(all(test, not(target_arch = "wasm32")))]
        if let Some(read) = &self.source {
            return read(provider, source);
        }
        match source {
            #[cfg(not(target_arch = "wasm32"))]
            Source::Keychain => {
                if let Ok(entry) = keyring::Entry::new("acyclic-harness", provider) {
                    match entry.get_password() {
                        Ok(key) if !key.trim().is_empty() => return Ok(Some(key)),
                        Ok(_)
                        | Err(keyring::Error::NoEntry)
                        | Err(keyring::Error::NoStorageAccess(_))
                        | Err(keyring::Error::PlatformFailure(_)) => {}
                        Err(_) => {
                            return Err(KeychainDecode {
                                provider: provider.into(),
                            });
                        }
                    }
                }
                Ok(None)
            }
            #[cfg(not(target_arch = "wasm32"))]
            Source::Environment => {
                let variable = match provider {
                    "anthropic" => "ANTHROPIC_API_KEY",
                    "openai" => "OPENAI_API_KEY",
                    "openrouter" => "OPENROUTER_API_KEY",
                    "gemini" => "GEMINI_API_KEY",
                    _ => "",
                };
                if let Ok(key) = std::env::var(variable) {
                    if !key.trim().is_empty() {
                        return Ok(Some(key));
                    }
                }
                if provider == "gemini" {
                    if let Ok(key) = std::env::var("GOOGLE_API_KEY") {
                        if !key.trim().is_empty() {
                            return Ok(Some(key));
                        }
                    }
                }
                Ok(None)
            }
            Source::Configuration => Ok(self.api_keys.get(provider).cloned()),
            #[cfg(all(test, not(target_arch = "wasm32")))]
            Source::CodexHome => Ok(None),
        }
    }

    /// Reads a JSON credential file. Missing files mean no configured keys.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn from_file(path: &std::path::Path) -> Result<Self> {
        match std::fs::read(path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map_err(|_| super::invalid("invalid provider credential file")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(_) => Err(super::invalid("cannot read provider credential file")),
        }
    }
}
