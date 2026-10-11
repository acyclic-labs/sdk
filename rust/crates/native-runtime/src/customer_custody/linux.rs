use super::CustodyError;
use secret_service::{EncryptionType, blocking::SecretService};
use std::collections::HashMap;
use zeroize::Zeroizing;

fn attributes(name: &str) -> HashMap<&str, &str> {
    HashMap::from([("application", "com.acyclic.customer-leaf.v1"), ("namespace", name)])
}

fn failure(error: secret_service::Error) -> CustodyError {
    match error {
        secret_service::Error::Locked => CustodyError::Locked,
        _ => CustodyError::Unavailable,
    }
}

pub(super) fn read(name: &str) -> Result<Zeroizing<Vec<u8>>, CustodyError> {
    let service = SecretService::connect(EncryptionType::Dh).map_err(failure)?;
    let collection = service.get_default_collection().map_err(failure)?;
    collection.ensure_unlocked().map_err(failure)?;
    let items = collection.search_items(attributes(name)).map_err(failure)?;
    match items.as_slice() {
        [] => Err(CustodyError::NotFound),
        [item] => item.get_secret().map(Zeroizing::new).map_err(failure),
        _ => Err(CustodyError::Ambiguous),
    }
}

pub(super) fn write(name: &str, secret: &[u8]) -> Result<(), CustodyError> {
    let service = SecretService::connect(EncryptionType::Dh).map_err(failure)?;
    let collection = service.get_default_collection().map_err(failure)?;
    collection.ensure_unlocked().map_err(failure)?;
    let items = collection.search_items(attributes(name)).map_err(failure)?;
    match items.as_slice() {
        [] => {
            collection.create_item(
                "Acyclic customer leaf and SQL session", attributes(name), secret,
                true, "application/octet-stream",
            ).map_err(failure)?;
            Ok(())
        }
        // Atomic SetSecret of the entire envelope; never delete the old item first.
        [item] => item.set_secret(secret, "application/octet-stream").map_err(failure),
        _ => Err(CustodyError::Ambiguous),
    }
}

pub(super) fn delete(name: &str) -> Result<(), CustodyError> {
    let service = SecretService::connect(EncryptionType::Dh).map_err(failure)?;
    let collection = service.get_default_collection().map_err(failure)?;
    collection.ensure_unlocked().map_err(failure)?;
    let items = collection.search_items(attributes(name)).map_err(failure)?;
    match items.as_slice() {
        [] => Err(CustodyError::NotFound),
        [item] => item.delete().map_err(failure),
        _ => Err(CustodyError::Ambiguous),
    }
}
