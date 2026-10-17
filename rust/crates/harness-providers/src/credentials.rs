use acyclic_harness::Result;
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
}

fn native_sources() -> bool {
    true
}

impl Default for Credentials {
    fn default() -> Self {
        Self {
            api_keys: BTreeMap::new(),
            native_sources: true,
        }
    }
}

impl Credentials {
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn allows_native_sources(&self) -> bool {
        self.native_sources
    }
    /// Uses only supplied keys, with no keychain, environment or filesystem reads.
    pub fn explicit(api_keys: BTreeMap<String, String>) -> Self {
        Self {
            api_keys,
            native_sources: false,
        }
    }
    /// Resolves OS keychain, then environment, then this configuration.
    pub fn api_key(&self, provider: &str) -> Result<Option<String>> {
        if self.native_sources {
            #[cfg(not(target_arch = "wasm32"))]
            {
                if let Ok(entry) = keyring::Entry::new("acyclic-harness", provider) {
                    match entry.get_password() {
                        Ok(key) if !key.trim().is_empty() => return Ok(Some(key)),
                        Ok(_)
                        | Err(keyring::Error::NoEntry)
                        | Err(keyring::Error::NoStorageAccess(_))
                        | Err(keyring::Error::PlatformFailure(_)) => {}
                        Err(_) => return Err(super::invalid("cannot decode keychain credential")),
                    }
                }
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
            }
        }
        Ok(self
            .api_keys
            .get(provider)
            .filter(|key| !key.trim().is_empty())
            .cloned())
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
