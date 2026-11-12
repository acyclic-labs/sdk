use acyclic_harness_core::Result;
use std::collections::BTreeMap;

/// API keys loaded from explicitly supplied configuration or native credential sources.
/// Anthropic accepts API keys only; no subscription login is read.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Credentials {
    /// Provider ID to API key. Deliberately not Debug or Serialize.
    pub api_keys: BTreeMap<String, String>,
    /// Native credential namespace shared by discovery and first-run storage.
    /// Defaults to ACYCLIC_KEYCHAIN_SERVICE, then acyclic-harness.
    #[serde(default)]
    pub keychain_service: Option<String>,
    #[serde(skip, default = "native_sources")]
    native_sources: bool,
    #[cfg(not(target_arch = "wasm32"))]
    #[serde(skip)]
    path: Option<std::path::PathBuf>,
    #[cfg(not(target_arch = "wasm32"))]
    #[serde(skip)]
    watch:
        Option<std::sync::Arc<dyn Fn() -> acyclic_auth::credential::CredentialWatch + Send + Sync>>,
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
            keychain_service: None,
            native_sources: true,
            #[cfg(not(target_arch = "wasm32"))]
            path: None,
            #[cfg(not(target_arch = "wasm32"))]
            watch: None,
            #[cfg(all(test, not(target_arch = "wasm32")))]
            source: None,
        }
    }
}

impl Credentials {
    /// Native service selected by explicit configuration or the host environment.
    /// Explicit-only credential resolution never calls this native accessor.
    #[cfg(not(target_arch = "wasm32"))]
    #[must_use]
    pub fn keychain_service(&self) -> std::borrow::Cow<'_, str> {
        if let Some(service) = &self.keychain_service {
            return std::borrow::Cow::Borrowed(service);
        }
        std::env::var("ACYCLIC_KEYCHAIN_SERVICE")
            .map(std::borrow::Cow::Owned)
            .unwrap_or(std::borrow::Cow::Borrowed("acyclic-harness"))
    }

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
        Self::default().store_api_key_in_service(provider, key)
    }

    /// Stores a credential in this configuration's native namespace.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn store_api_key_in_service(&self, provider: &str, key: &str) -> Result<()> {
        if key.trim().is_empty() || key.trim().chars().any(char::is_control) {
            return Err(super::invalid(
                "API key must be nonempty and single-line; recovery: paste a provider API key",
            ));
        }
        keyring::Entry::new(self.keychain_service().as_ref(), provider)
            .and_then(|entry| entry.set_password(key.trim()))
            .map_err(|_| super::invalid("cannot store provider key in configured OS keychain service; recovery: unlock the OS keychain and run graphcoder again"))?;
        Self::changed();
        Ok(())
    }

    /// Removes only this provider's entry, for host credential lifecycle owners.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn remove_api_key(provider: &str) -> Result<()> {
        Self::default().remove_api_key_in_service(provider)
    }

    /// Removes a credential only from this configuration's native namespace.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn remove_api_key_in_service(&self, provider: &str) -> Result<()> {
        match keyring::Entry::new(self.keychain_service().as_ref(), provider)
            .and_then(|entry| entry.delete_credential())
        {
            Ok(()) | Err(keyring::Error::NoEntry) => {
                Self::changed();
                Ok(())
            }
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
            keychain_service: None,
            native_sources: false,
            #[cfg(not(target_arch = "wasm32"))]
            path: None,
            #[cfg(not(target_arch = "wasm32"))]
            watch: None,
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
                if let Ok(entry) = keyring::Entry::new(self.keychain_service().as_ref(), provider) {
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
                    "morph" => "MORPH_API_KEY",
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

    /// Reads an explicitly selected JSON credential file; absence is an error.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn from_file(path: &std::path::Path) -> Result<Self> {
        match acyclic_fs::native_host::read_regular_file(path) {
            Ok(bytes) => {
                let mut credentials: Self = serde_json::from_slice(&bytes)
                    .map_err(|_| super::invalid("invalid provider credential file"))?;
                credentials.path = Some(path.to_owned());
                Ok(credentials)
            }
            Err(_) => Err(super::invalid("cannot read provider credential file")),
        }
    }
}

#[cfg(all(test, unix))]
#[test]
fn explicit_credential_files_refuse_links() -> std::io::Result<()> {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("credentials.json");
    let link = temporary.path().join("credentials-link.json");
    std::fs::write(&path, br#"{"api_keys":{}}"#)?;
    assert!(Credentials::from_file(&path).is_ok());
    std::os::unix::fs::symlink(&path, &link)?;
    assert!(Credentials::from_file(&link).is_err());
    Ok(())
}
pub(crate) mod live;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod watched;
pub(crate) use live::{Credential, CredentialInputs};
