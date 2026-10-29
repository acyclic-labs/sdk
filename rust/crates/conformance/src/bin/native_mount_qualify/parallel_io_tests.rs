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

/// Actual contained child process used by the exit-order and deadline controls.
#[test]
fn exit_wait_child() {
    match std::env::var("ACYCLIC_QUALIFIER_EXIT_CHILD").as_deref() {
        Ok("fail") => {
            println!("complete failed-child stdout");
            eprintln!("complete failed-child stderr");
            std::process::exit(7);
        }
        Ok("blocked") => loop {
            std::thread::park();
        },
        _ => {}
    }
}

fn exit_child(mode: &str) -> Result<Option<super::CapturedChild>, super::Failure> {
    Ok(Some(super::spawn_captured_child(
        std::process::Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "parallel_io_tests::exit_wait_child",
                "--nocapture",
            ])
            .env("ACYCLIC_QUALIFIER_EXIT_CHILD", mode),
    )?))
}

#[tokio::test]
async fn exit_wait_settles_all_successes_and_failure_behind_a_blocked_child()
-> Result<(), super::Failure> {
    let mut children = vec![exit_child("success")?, exit_child("success")?];
    super::wait_captured_children(&mut children, Instant::now() + Duration::from_secs(2)).await?;
    assert!(children.iter().all(Option::is_none));

    // A prefix scheduler would wait for the blocked first child until deadline.
    // The actual later failure must win and retire every accepted sibling.
    let mut children = vec![exit_child("blocked")?, exit_child("fail")?];
    let error =
        super::wait_captured_children(&mut children, Instant::now() + Duration::from_secs(2))
            .await
            .expect_err("a failed actual child must refuse the qualification");
    assert!(
        error.to_string().contains("parallel I/O child 1 failed"),
        "{error}"
    );
    assert!(children.iter().all(Option::is_none));

    let mut children = vec![exit_child("blocked")?, exit_child("blocked")?];
    let error = super::wait_captured_children(&mut children, Instant::now())
        .await
        .expect_err("a deadline must retire blocked children, never invent success");
    assert!(
        error.to_string().contains("exceeded their deadline"),
        "{error}"
    );
    assert!(children.iter().all(Option::is_none));
    Ok(())
}
