//! Host-only native credential inputs. Configuration supplies an explicit
//! variable or file name; it does not manufacture a published secret version.
//! Recorded credentials use Auth's canonical `SecretRef` and `Resolver`.
use acyclic_auth::secrets::SecretValue;
use acyclic_stream::{BoxProviderFuture, ProviderPlatform};

/// Original native configuration source. Values stay with the host adapter.
pub trait SecretSource: ProviderPlatform + 'static {
    /// Read the explicitly selected native input once. No immutable revision
    /// or authority is inferred from its name, timestamp or content hash.
    fn secret<'a>(
        &'a self,
        name: &'a str,
    ) -> BoxProviderFuture<'a, crate::Result<Option<SecretValue>>>;
}

fn component(name: &str) -> bool {
    !name.is_empty()
        && !name.starts_with('.')
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

/// Explicit credential variables in the original host process.
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
        name: &'a str,
    ) -> BoxProviderFuture<'a, crate::Result<Option<SecretValue>>> {
        let value = if component(name) {
            crate::native_environment::variable(name)
                .map(|value| SecretValue::new(value.into_encoded_bytes()))
        } else {
            None
        };
        Box::pin(std::future::ready(Ok(value)))
    }
}

/// Explicit credential files held by the original host. A trailing newline is
/// not part of the value. This adapter makes no published-version claim.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Debug)]
pub struct FileSecrets {
    directory: std::path::PathBuf,
}

#[cfg(not(target_arch = "wasm32"))]
impl FileSecrets {
    /// Bind the original host's selected directory.
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
        name: &'a str,
    ) -> BoxProviderFuture<'a, crate::Result<Option<SecretValue>>> {
        if !component(name) {
            return Box::pin(std::future::ready(Ok(None)));
        }
        let path = self.directory.join(name);
        Box::pin(async move {
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
fn read_secret(path: &std::path::Path) -> crate::Result<Option<SecretValue>> {
    let mut value = match std::fs::read(path) {
        Ok(value) => value,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(crate::Error::Storage("secret file is unreadable".into())),
    };
    let kept = value.len()
        - value
            .iter()
            .rev()
            .take_while(|byte| matches!(byte, b'\r' | b'\n'))
            .count();
    value[kept..].fill(0);
    value.truncate(kept);
    Ok(Some(SecretValue::new(value)))
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn explicit_native_inputs_do_not_fabricate_published_versions()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("token");
        std::fs::write(&path, b"first\n")?;
        let source = FileSecrets::new(directory.path());
        assert_eq!(
            source
                .secret("token")
                .await?
                .ok_or("missing input")?
                .expose(),
            b"first"
        );
        std::fs::write(&path, b"replacement")?;
        assert_eq!(
            source
                .secret("token")
                .await?
                .ok_or("missing input")?
                .expose(),
            b"replacement"
        );
        for name in ["", "../token", "token/", ".env", "a/b"] {
            assert!(source.secret(name).await?.is_none());
        }
        Ok(())
    }
}
