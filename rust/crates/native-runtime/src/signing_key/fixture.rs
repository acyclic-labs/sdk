//! Shared process-local credential backend for every test/fixture caller. Entry
//! handles reopen the same zeroizing value, just as the production stores do.
use keyring::{Error, credential::CredentialApi};
use std::{
    collections::BTreeMap,
    sync::{Mutex, OnceLock},
};
use zeroize::Zeroizing;

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Fault {
    StoredRead,
    WriteAcknowledgement,
}

#[cfg(test)]
fn faults() -> &'static Mutex<BTreeMap<String, Fault>> {
    static FAULTS: OnceLock<Mutex<BTreeMap<String, Fault>>> = OnceLock::new();
    FAULTS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

#[cfg(test)]
pub(super) fn fail_once(name: &str, fault: Fault) {
    faults()
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .insert(name.into(), fault);
}

#[cfg(test)]
fn check_fault(name: &str, fault: Fault) -> keyring::Result<()> {
    let mut faults = faults().lock().unwrap_or_else(|error| error.into_inner());
    if faults.get(name) == Some(&fault) {
        faults.remove(name);
        return Err(Error::PlatformFailure(Box::new(std::io::Error::other(
            "fixture custody cut",
        ))));
    }
    Ok(())
}

type Secrets = BTreeMap<String, Zeroizing<Vec<u8>>>;

fn secrets() -> &'static Mutex<Secrets> {
    static SECRETS: OnceLock<Mutex<Secrets>> = OnceLock::new();
    SECRETS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

#[derive(Debug)]
struct Credential {
    name: String,
}

pub(super) fn entry(name: &str) -> keyring::Entry {
    keyring::Entry::new_with_credential(Box::new(Credential { name: name.into() }))
}

impl CredentialApi for Credential {
    fn set_secret(&self, secret: &[u8]) -> keyring::Result<()> {
        secrets()
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(self.name.clone(), Zeroizing::new(secret.to_vec()));
        #[cfg(test)]
        check_fault(&self.name, Fault::WriteAcknowledgement)?;
        Ok(())
    }

    fn get_secret(&self) -> keyring::Result<Vec<u8>> {
        let secrets = secrets().lock().unwrap_or_else(|error| error.into_inner());
        let secret = secrets.get(&self.name).ok_or(Error::NoEntry)?;
        #[cfg(test)]
        check_fault(&self.name, Fault::StoredRead)?;
        Ok(secret.to_vec())
    }

    fn delete_credential(&self) -> keyring::Result<()> {
        secrets()
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&self.name)
            .map(|_| ())
            .ok_or(Error::NoEntry)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
