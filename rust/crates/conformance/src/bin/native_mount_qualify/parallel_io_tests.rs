use super::{retry_nfs_directory_listing, retryable_nfs_directory_error};
use std::io::{Error, ErrorKind};
use std::time::{Duration, Instant};

#[test]
fn retries_only_transient_nfs_directory_errors() {
    assert!(retryable_nfs_directory_error(&Error::from(
        ErrorKind::StaleNetworkFileHandle
    )));
    assert!(!retryable_nfs_directory_error(&Error::from(
        ErrorKind::PermissionDenied
    )));
    assert_eq!(
        retryable_nfs_directory_error(&Error::from_raw_os_error(5)),
        cfg!(target_os = "macos")
    );
    #[cfg(target_os = "macos")]
    assert!(retryable_nfs_directory_error(&Error::from_raw_os_error(70)));
    #[cfg(target_os = "linux")]
    assert!(retryable_nfs_directory_error(&Error::from_raw_os_error(
        116
    )));
}

#[test]
fn retries_a_transient_listing_then_uses_the_complete_result() {
    let mut attempts = 0;
    let entries = retry_nfs_directory_listing(Instant::now() + Duration::from_secs(1), || {
        attempts += 1;
        if attempts == 1 {
            Err(Error::from(ErrorKind::StaleNetworkFileHandle))
        } else {
            Ok(vec!["peer-0", "peer-1"])
        }
    });
    assert_eq!(attempts, 2);
    assert_eq!(entries.ok(), Some(vec!["peer-0", "peer-1"]));
}

#[test]
fn stops_retrying_after_the_deadline() {
    let mut attempts = 0;
    let result =
        retry_nfs_directory_listing::<()>(Instant::now() + Duration::from_millis(1), || {
            attempts += 1;
            Err(Error::from(ErrorKind::StaleNetworkFileHandle))
        });
    assert_eq!(attempts, 1);
    assert!(matches!(result, Err(error) if error.kind() == ErrorKind::StaleNetworkFileHandle));
}

#[test]
fn fails_immediately_on_an_unrelated_error() {
    let mut attempts = 0;
    let result = retry_nfs_directory_listing::<()>(Instant::now() + Duration::from_secs(1), || {
        attempts += 1;
        Err(Error::from(ErrorKind::PermissionDenied))
    });
    assert_eq!(attempts, 1);
    assert!(matches!(result, Err(error) if error.kind() == ErrorKind::PermissionDenied));
}
