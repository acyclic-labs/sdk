use super::CustodyError;
use security_framework::passwords::{
    PasswordOptions, delete_generic_password, generic_password, set_generic_password,
};
use zeroize::Zeroizing;

const SERVICE: &str = "com.acyclic.customer-leaf.v1";

fn failure(error: security_framework::base::Error) -> CustodyError {
    match error.code() {
        -25300 => CustodyError::NotFound,
        code => CustodyError::Platform(i64::from(code)),
    }
}

pub(super) fn read(name: &str) -> Result<Zeroizing<Vec<u8>>, CustodyError> {
    generic_password(PasswordOptions::new_generic_password(SERVICE, name))
        .map(Zeroizing::new)
        .map_err(failure)
}

pub(super) fn write(name: &str, secret: &[u8]) -> Result<(), CustodyError> {
    // SecItemAdd/SecItemUpdate changes one complete item, not delete-then-add.
    set_generic_password(SERVICE, name, secret).map_err(failure)
}

pub(super) fn delete(name: &str) -> Result<(), CustodyError> {
    // This API reports SecItemDelete failures rather than discarding the status.
    delete_generic_password(SERVICE, name).map_err(failure)
}
