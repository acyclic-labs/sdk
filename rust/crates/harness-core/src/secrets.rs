//! Host-held secrets. Configuration names a secret by [`SecretRef`]; only the
//! host resolves it, through its one [`SecretSource`], so no secret enters
//! guest code, a stream or a volume. A platform implements [`SecretSource`]
//! over its secrets product; self-hosters use [`EnvSecrets`] or
//! [`FileSecrets`].

use acyclic_stream::{BoxProviderFuture, ProviderPlatform};
use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

/// Longest secret name.
pub const MAX_SECRET_NAME_BYTES: usize = 256;

/// The name of a secret, never its value: ASCII letters, digits, `.`, `_`
/// and `-`, not starting with `.`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct SecretRef(String);

impl SecretRef {
    /// Validates `name`.
    ///
    /// # Errors
    /// [`crate::Error::Invalid`] for an empty, overlong or ill-formed name.
    pub fn new(name: impl Into<String>) -> crate::Result<Self> {
        let name = name.into();
        let valid = !name.is_empty()
            && name.len() <= MAX_SECRET_NAME_BYTES
            && !name.starts_with('.')
            && name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'));
        if valid {
            Ok(Self(name))
        } else {
            Err(crate::Error::Invalid("invalid secret name".into()))
        }
    }

    /// The name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for SecretRef {
    type Error = crate::Error;

    fn try_from(name: String) -> crate::Result<Self> {
        Self::new(name)
    }
}

impl From<SecretRef> for String {
    fn from(reference: SecretRef) -> Self {
        reference.0
    }
}

/// A resolved secret, held only by the host: never printed, zeroed on drop.
pub struct Secret(Zeroizing<String>);

impl Secret {
    /// Takes ownership of `value`.
    #[must_use]
    pub fn new(value: String) -> Self {
        Self(Zeroizing::new(value))
    }

    /// The value, for the one host call that presents it.
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Secret(..)")
    }
}

/// The host's one source of secrets.
pub trait SecretSource: ProviderPlatform + 'static {
    /// Resolves `reference`; `None` when the source holds no such secret.
    fn secret<'a>(
        &'a self,
        reference: &'a SecretRef,
    ) -> BoxProviderFuture<'a, crate::Result<Option<Secret>>>;
}

/// Secrets in the host process's environment, one variable per name.
#[cfg(all(
    not(target_arch = "wasm32"),
    any(feature = "mcp-stdio", feature = "mcp-http")
))]
#[derive(Clone, Copy, Debug, Default)]
pub struct EnvSecrets;

#[cfg(all(
    not(target_arch = "wasm32"),
    any(feature = "mcp-stdio", feature = "mcp-http")
))]
impl SecretSource for EnvSecrets {
    fn secret<'a>(
        &'a self,
        reference: &'a SecretRef,
    ) -> BoxProviderFuture<'a, crate::Result<Option<Secret>>> {
        let value = crate::native_environment::variable(reference.as_str())
            .map(|value| {
                value
                    .into_string()
                    .map(Secret::new)
                    .map_err(|_| crate::Error::Invalid("secret is not UTF-8".into()))
            })
            .transpose();
        Box::pin(std::future::ready(value))
    }
}

/// Secrets as files in one directory, one file per name (a mounted secrets
/// volume); a trailing newline is not part of the secret.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Debug)]
pub struct FileSecrets {
    directory: std::path::PathBuf,
}

#[cfg(not(target_arch = "wasm32"))]
impl FileSecrets {
    /// Secrets in `directory`.
    #[must_use]
    pub fn new(directory: impl Into<std::path::PathBuf>) -> Self {
        Self {
            directory: directory.into(),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl SecretSource for FileSecrets {
    fn secret<'a>(
        &'a self,
        reference: &'a SecretRef,
    ) -> BoxProviderFuture<'a, crate::Result<Option<Secret>>> {
        // A validated name is one path component, so it never leaves the directory.
        let path = self.directory.join(reference.as_str());
        Box::pin(async move {
            // A slow mount blocks a blocking worker, never the caller's thread.
            match tokio::runtime::Handle::try_current() {
                Ok(runtime) => runtime
                    .spawn_blocking(move || read_secret(&path))
                    .await
                    .map_err(|_| crate::Error::Storage("secret read did not complete".into()))?,
                Err(_) => read_secret(&path),
            }
        })
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn read_secret(path: &std::path::Path) -> crate::Result<Option<Secret>> {
    match std::fs::read_to_string(path) {
        Ok(mut value) => {
            let kept = value.trim_end_matches(['\r', '\n']).len();
            value.truncate(kept);
            Ok(Some(Secret::new(value)))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(_) => Err(crate::Error::Storage("secret file is unreadable".into())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_one_path_component() {
        for name in ["", ".env", "../key", "a/b", "a b"] {
            assert!(SecretRef::new(name).is_err(), "{name:?}");
        }
        assert!(SecretRef::new("openai.api_key-1").is_ok());
        assert!(SecretRef::new("x".repeat(MAX_SECRET_NAME_BYTES + 1)).is_err());
    }

    #[test]
    fn a_secret_is_never_printed() {
        assert_eq!(format!("{:?}", Secret::new("hunter2".into())), "Secret(..)");
    }
}
