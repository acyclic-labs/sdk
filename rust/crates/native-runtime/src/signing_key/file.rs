//! File adapter for the same seed-entry protocol. Private-file publication owns
//! permissions, atomic installation and durability on every supported OS.
use crate::private_file;
use keyring::{Error, credential::CredentialApi};
use std::{io, io::Write as _, path::PathBuf};

#[derive(Debug)]
struct Credential {
    directory: PathBuf,
    name: String,
}

#[cfg(not(any(test, feature = "test-support")))]
pub(super) fn entry(name: &str) -> io::Result<keyring::Entry> {
    let config = std::env::var_os("ACYCLIC_SIGNING_CUSTODY_DIRECTORY").map(PathBuf::from)
        .or_else(dirs::config_dir).filter(|path| path.is_absolute()).ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound,
            "signing custody configuration directory unavailable; recovery: configure an absolute application configuration directory")
    })?;
    Ok(keyring::Entry::new_with_credential(Box::new(Credential {
        directory: config.join("acyclic/signing-seeds"),
        name: name.into(),
    })))
}

fn failure(error: io::Error) -> Error {
    if error.kind() == io::ErrorKind::NotFound {
        Error::NoEntry
    } else {
        Error::PlatformFailure(Box::new(error))
    }
}

impl CredentialApi for Credential {
    fn get_secret(&self) -> keyring::Result<Vec<u8>> {
        private_file::validate_directory(&self.directory).map_err(failure)?;
        let secret = private_file::read(&self.directory.join(&self.name), 32).map_err(failure)?;
        if secret.0.len() != 32 {
            return Err(failure(io::ErrorKind::InvalidData.into()));
        }
        // Transfer the only secret buffer to the caller's zeroizing custodian.
        let mut secret = secret;
        Ok(std::mem::take(&mut secret.0))
    }

    fn set_secret(&self, secret: &[u8]) -> keyring::Result<()> {
        if secret.len() != 32 {
            return Err(failure(io::ErrorKind::InvalidInput.into()));
        }
        private_file::create_directory_all(&self.directory).map_err(failure)?;
        private_file::validate_directory(&self.directory).map_err(failure)?;
        let mut staged =
            private_file::StagedFile::create(&self.directory, ".seed-").map_err(failure)?;
        staged.as_file_mut().write_all(secret).map_err(failure)?;
        staged
            .publish(
                &self.directory.join(&self.name),
                crate::RenameMode::NoReplace,
            )
            .map_err(failure)
    }

    fn delete_credential(&self) -> keyring::Result<()> {
        private_file::validate_directory(&self.directory).map_err(failure)?;
        std::fs::remove_file(self.directory.join(&self.name)).map_err(failure)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::owner_record::SecretBytes;
    use subtle::ConstantTimeEq as _;

    #[test]
    fn file_custody_is_durable_private_and_refuses_aliases_or_public_keys() -> io::Result<()> {
        let root = tempfile::tempdir()?;
        let entry = Credential {
            directory: root.path().join("custody"),
            name: "seed".into(),
        };
        let seed = SecretBytes(crate::owner_record::new_key()?.to_vec());
        assert!(matches!(entry.get_secret(), Err(Error::NoEntry)));
        entry.set_secret(&seed.0).map_err(io::Error::other)?;
        let path = entry.directory.join("seed");
        assert!(private_file::is_private(&private_file::open(
            &path, false
        )?)?);
        let reopened = SecretBytes(entry.get_secret().map_err(io::Error::other)?);
        assert!(bool::from(reopened.0.ct_eq(&seed.0)));
        assert!(
            entry.set_secret(&seed.0).is_err(),
            "an existing seed cannot be replaced"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::{PermissionsExt as _, symlink};
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))?;
            assert!(
                entry.get_secret().is_err(),
                "public seed reached the signer"
            );
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
            std::fs::rename(&path, entry.directory.join("original"))?;
            symlink("original", &path)?;
            assert!(entry.get_secret().is_err(), "symlink reached the signer");
        }
        Ok(())
    }
}
