//! Linux kernel-keyring custody for one signing seed: the store used when no
//! Secret Service is reachable, which is every headless host.
//!
//! The key lives in the user keyring, and also in the user's persistent keyring
//! when the kernel provides one. Its owning uid holds every permission
//! explicitly, so no process depends on possessing the key through a session
//! keyring. A detached or ended login revokes its session keyring, and a
//! service can run with a private one. The key is created in this thread's
//! keyring, which this thread always possesses, so it is granted its
//! permissions before it is reachable anywhere else.
use keyring::{Error, credential::CredentialApi};
use linux_keyutils::{
    Key, KeyError, KeyPermissionsBuilder, KeyRing, KeyRingIdentifier, Permission,
};

/// One seed entry in the kernel keyrings.
#[derive(Debug)]
pub(crate) struct KernelKey {
    description: String,
}

impl KernelKey {
    pub(crate) fn new(service: &str, name: &str) -> Self {
        Self {
            description: format!("{service}:{name}"),
        }
    }

    /// The user keyring, which must hold the key, and the persistent keyring
    /// when the kernel has one, attached to this thread so it is possessed.
    fn rings() -> Result<(KeyRing, Option<KeyRing>), Error> {
        let user = KeyRing::from_special_id(KeyRingIdentifier::User, false).map_err(failure)?;
        Ok((
            user,
            KeyRing::get_persistent(KeyRingIdentifier::Thread).ok(),
        ))
    }

    /// The key and the keyring it was found in, then the other keyring.
    fn find(&self) -> Result<(Key, KeyRing, Option<KeyRing>), Error> {
        let (user, persistent) = Self::rings()?;
        match (user.search(&self.description), persistent) {
            (Ok(key), persistent) => Ok((key, user, persistent)),
            (Err(KeyError::KeyDoesNotExist), Some(persistent)) => persistent
                .search(&self.description)
                .map(|key| (key, persistent, Some(user)))
                .map_err(absent),
            (Err(error), _) => Err(absent(error)),
        }
    }
}

fn absent(error: KeyError) -> Error {
    match error {
        KeyError::KeyDoesNotExist => Error::NoEntry,
        error => failure(error),
    }
}

fn failure(error: KeyError) -> Error {
    Error::PlatformFailure(Box::new(error))
}

impl CredentialApi for KernelKey {
    fn set_secret(&self, secret: &[u8]) -> keyring::Result<()> {
        let (user, persistent) = Self::rings()?;
        let thread = KeyRing::from_special_id(KeyRingIdentifier::Thread, true).map_err(failure)?;
        let key = thread.add_key(&self.description, secret).map_err(failure)?;
        let all = KeyPermissionsBuilder::builder()
            .posessor(Permission::ALL)
            .user(Permission::ALL)
            .build();
        let published = key.set_perms(all).and_then(|()| user.link_key(key));
        // The persistent link only extends the key's life past the user's last
        // process; the user keyring already holds it.
        if published.is_ok()
            && let Some(persistent) = persistent
        {
            let _ = persistent.link_key(key);
        }
        let unlinked = thread.unlink_key(key);
        if let Err(error) = published {
            let _ = key.invalidate();
            return Err(failure(error));
        }
        unlinked.map_err(failure)
    }

    fn get_secret(&self) -> keyring::Result<Vec<u8>> {
        self.find()?.0.read_to_vec().map_err(failure)
    }

    /// Unlinks the key from both keyrings first, which takes effect at once,
    /// because the kernel reclaims an invalidated key asynchronously and a
    /// search can still return it for a moment.
    fn delete_credential(&self) -> keyring::Result<()> {
        let (key, holder, other) = self.find()?;
        // The other link was best effort, so its removal is too.
        if let Some(other) = other {
            let _ = other.unlink_key(key);
        }
        holder.unlink_key(key).map_err(failure)?;
        key.invalidate().map_err(failure)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
