//! Process identity checked before Windows execution.
use std::io;
use windows_sys::Win32::Foundation::{
    APPMODEL_ERROR_NO_PACKAGE, ERROR_INSUFFICIENT_BUFFER, HANDLE,
};

#[allow(
    unsafe_code,
    reason = "query the suspended process's OS package identity"
)]
pub(crate) fn require_unpackaged(process: HANDLE) -> io::Result<()> {
    let mut length = 0;
    // SAFETY: the caller owns the suspended child handle, which grants query
    // access. A null buffer with zero length queries only its identity class.
    let status = unsafe {
        windows_sys::Win32::Storage::Packaging::Appx::GetPackageFullName(
            process,
            &raw mut length,
            std::ptr::null_mut(),
        )
    };
    admit_package_status(status)
}

fn admit_package_status(status: u32) -> io::Result<()> {
    match status {
        APPMODEL_ERROR_NO_PACKAGE => Ok(()),
        ERROR_INSUFFICIENT_BUFFER => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "packaged Windows applications cannot preserve process-tree custody",
        )),
        other => Err(io::Error::other(format!(
            "cannot establish unpackaged process identity: {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn only_an_os_proven_unpackaged_process_is_admitted(status: u32) {
            prop_assert_eq!(admit_package_status(status).is_ok(), status == APPMODEL_ERROR_NO_PACKAGE);
        }
    }

    #[test]
    fn package_identity_and_query_errors_fail_closed() {
        assert!(admit_package_status(APPMODEL_ERROR_NO_PACKAGE).is_ok());
        for status in [ERROR_INSUFFICIENT_BUFFER, 0, 5, u32::MAX] {
            assert!(admit_package_status(status).is_err());
        }
    }
}
