//! Host-held secrets. Every reference is the canonical
//! [`acyclic_auth::secrets::SecretRef`]; only the host resolves it, through its
//! one [`acyclic_auth::secrets::Resolver`], so no secret enters guest code, a
//! stream or a volume. A platform implements the resolver over its secrets
//! product; self-hosters use [`EnvSecrets`] or [`FileSecrets`]. A caller that
//! holds only a name, such as an MCP credential variable, gets its reference
//! from `Resolver::current` in its host's scope.

/// Secrets in the host process's environment, one variable per name, in the
/// one scope [`EnvSecrets::SCOPE`]. The environment is fixed for the process,
/// so each value has the single version 1.
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
impl EnvSecrets {
    /// The scope naming the host environment.
    pub const SCOPE: &'static str = "env";

    fn variable(scope: &str, name: &str) -> Option<std::ffi::OsString> {
        // A name the platform cannot look up is simply not held.
        (scope == Self::SCOPE && !name.is_empty() && !name.contains('='))
            .then(|| crate::native_environment::variable(name))
            .flatten()
    }
}

#[cfg(all(
    not(target_arch = "wasm32"),
    any(feature = "mcp-stdio", feature = "mcp-http")
))]
impl acyclic_auth::secrets::Resolver for EnvSecrets {
    fn current<'a>(
        &'a self,
        scope: &'a str,
        name: &'a str,
    ) -> acyclic_auth::secrets::CurrentFuture<'a> {
        use acyclic_auth::secrets::{ResolveError, SecretRef};
        let reference = Self::variable(scope, name)
            .ok_or(ResolveError::NotFound)
            .and_then(|_| {
                SecretRef::new(scope.to_owned(), name.to_owned(), 1)
                    .map_err(|_| ResolveError::NotFound)
            });
        Box::pin(std::future::ready(reference))
    }

    fn resolve<'a>(
        &'a self,
        reference: &'a acyclic_auth::secrets::SecretRef,
    ) -> acyclic_auth::secrets::ResolveFuture<'a> {
        use acyclic_auth::secrets::{ResolveError, SecretValue};
        let value = Self::variable(reference.scope(), reference.name())
            .ok_or(ResolveError::NotFound)
            .and_then(|value| {
                if reference.version() == 1 {
                    Ok(SecretValue::new(value.into_encoded_bytes()))
                } else {
                    Err(ResolveError::VersionGone)
                }
            });
        Box::pin(std::future::ready(value))
    }
}

/// Secrets as files in one directory, one file per name (a mounted secrets
/// volume), in the one scope [`FileSecrets::SCOPE`]. A trailing newline is not
/// part of the secret. A value's version is the leading 64 bits of the SHA-256
/// of its bytes, so different content is a different version however and
/// whenever it was written, and an old reference fails with `VersionGone`
/// instead of reading the new bytes.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Debug)]
pub struct FileSecrets {
    directory: std::path::PathBuf,
}

#[cfg(not(target_arch = "wasm32"))]
impl FileSecrets {
    /// The scope naming the mounted directory.
    pub const SCOPE: &'static str = "file";

    /// Secrets in `directory`.
    #[must_use]
    pub fn new(directory: impl Into<std::path::PathBuf>) -> Self {
        Self {
            directory: directory.into(),
        }
    }

    /// The file of `name`: exactly one visible path component, so a name
    /// never leaves the directory.
    fn path(&self, scope: &str, name: &str) -> Option<std::path::PathBuf> {
        let mut components = std::path::Path::new(name).components();
        let single = matches!(
            (components.next(), components.next()),
            (Some(std::path::Component::Normal(part)), None) if part == name
        );
        (scope == Self::SCOPE && single && !name.starts_with('.'))
            .then(|| self.directory.join(name))
    }

    /// Runs one blocking read on a blocking worker, never the caller's thread.
    async fn blocking<T: Send + 'static>(
        work: impl FnOnce() -> Result<T, acyclic_auth::secrets::ResolveError> + Send + 'static,
    ) -> Result<T, acyclic_auth::secrets::ResolveError> {
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => runtime
                .spawn_blocking(work)
                .await
                .map_err(|_| acyclic_auth::secrets::ResolveError::Unavailable)?,
            Err(_) => work(),
        }
    }
}

/// One secret file's value, without its trailing newline, and its version.
#[cfg(not(target_arch = "wasm32"))]
fn read_file(
    path: &std::path::Path,
) -> Result<(acyclic_auth::secrets::SecretValue, u64), acyclic_auth::secrets::ResolveError> {
    use acyclic_auth::secrets::{ResolveError, SecretValue};
    use sha2::Digest as _;
    let mut value = std::fs::read(path).map_err(|error| match error.kind() {
        std::io::ErrorKind::NotFound => ResolveError::NotFound,
        _ => ResolveError::Unavailable,
    })?;
    let kept = value.len()
        - value
            .iter()
            .rev()
            .take_while(|byte| matches!(byte, b'\r' | b'\n'))
            .count();
    value[kept..].fill(0);
    value.truncate(kept);
    let digest = sha2::Sha256::digest(&value);
    let mut leading = [0; 8];
    leading.copy_from_slice(&digest[..8]);
    // Version 0 is never written; its one digest prefix shares version 1.
    let version = u64::from_be_bytes(leading).max(1);
    Ok((SecretValue::new(value), version))
}

#[cfg(not(target_arch = "wasm32"))]
impl acyclic_auth::secrets::Resolver for FileSecrets {
    fn current<'a>(
        &'a self,
        scope: &'a str,
        name: &'a str,
    ) -> acyclic_auth::secrets::CurrentFuture<'a> {
        use acyclic_auth::secrets::{ResolveError, SecretRef};
        let path = self.path(scope, name);
        Box::pin(async move {
            let path = path.ok_or(ResolveError::NotFound)?;
            // The value is dropped, and so zeroed, here; only its version leaves.
            let (_, version) = Self::blocking(move || read_file(&path)).await?;
            SecretRef::new(scope.to_owned(), name.to_owned(), version)
                .map_err(|_| ResolveError::NotFound)
        })
    }

    fn resolve<'a>(
        &'a self,
        reference: &'a acyclic_auth::secrets::SecretRef,
    ) -> acyclic_auth::secrets::ResolveFuture<'a> {
        use acyclic_auth::secrets::ResolveError;
        let path = self.path(reference.scope(), reference.name());
        let expected = reference.version();
        Box::pin(async move {
            let path = path.ok_or(ResolveError::NotFound)?;
            let (value, version) = Self::blocking(move || read_file(&path)).await?;
            // Bytes of any other version are dropped, and so zeroed.
            if version == expected {
                Ok(value)
            } else {
                Err(ResolveError::VersionGone)
            }
        })
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use acyclic_auth::secrets::{ResolveError, Resolver as _};

    #[tokio::test]
    async fn file_references_pin_the_version_they_were_issued_for()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        std::fs::write(directory.path().join("token"), "first\n")?;
        let secrets = FileSecrets::new(directory.path());
        let reference = secrets.current(FileSecrets::SCOPE, "token").await?;
        assert_eq!(secrets.resolve(&reference).await?.expose(), b"first");
        // A trailing newline is not part of the value, so not a new version.
        std::fs::write(directory.path().join("token"), "first")?;
        assert_eq!(secrets.resolve(&reference).await?.expose(), b"first");
        // Negative control: other content never resolves under the old version,
        // even written at the same modification time.
        let written = std::fs::metadata(directory.path().join("token"))?.modified()?;
        std::fs::write(directory.path().join("token"), "second")?;
        std::fs::File::options()
            .write(true)
            .open(directory.path().join("token"))?
            .set_modified(written)?;
        assert_eq!(
            secrets.resolve(&reference).await.err(),
            Some(ResolveError::VersionGone)
        );
        let current = secrets.current(FileSecrets::SCOPE, "token").await?;
        assert_eq!(secrets.resolve(&current).await?.expose(), b"second");
        Ok(())
    }

    #[tokio::test]
    async fn file_names_never_leave_the_directory_or_scope() {
        let secrets = FileSecrets::new("secrets");
        for (scope, name) in [
            (FileSecrets::SCOPE, ".env"),
            (FileSecrets::SCOPE, "../key"),
            (FileSecrets::SCOPE, "a/b"),
            (FileSecrets::SCOPE, "key/"),
            ("other", "key"),
        ] {
            assert_eq!(
                secrets.current(scope, name).await.err(),
                Some(ResolveError::NotFound),
                "{scope}/{name}"
            );
        }
    }
}
