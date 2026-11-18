use acyclic_fs::kernel::FileKind;
use acyclic_fs::{AuthoredMutation, CancellationToken, Fs};
use acyclic_fs_format::identity::VolumeId;
use acyclic_fs_format::metadata::FileMetadata;
use acyclic_fs_format::model::FilesystemProfile;
use acyclic_stream::OwnedBytes;
use std::error::Error;

use super::B;
use super::checkout_fixture::{config, path, writable_pinned};
use super::fixtures::open_checkout;

#[tokio::test]
async fn authored_special_operations_are_profile_exact_and_fail_atomically()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut posix = config();
    posix.profile = FilesystemProfile::Posix;
    let volume =
        (fs.create_volume_with_id(VolumeId::from_bytes([115; 16]), posix, B, &cancellation))
            .await?
            .value;
    let mut checkout = open_checkout(&volume, writable_pinned(), &cancellation).await?;
    let receipt = (checkout.apply_authored_transaction(
        vec![
            AuthoredMutation::CreateEmptySpecial {
                file_id: None,
                path: path("fifo")?,
                kind: FileKind::Fifo,
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::CreateEmptySpecial {
                file_id: None,
                path: path("socket")?,
                kind: FileKind::Socket,
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::CreateEmptySpecial {
                file_id: None,
                path: path("mount")?,
                kind: FileKind::MountBoundary,
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::CreateDevice {
                file_id: None,
                path: path("character")?,
                kind: FileKind::CharacterDevice,
                major: 1,
                minor: 3,
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::CreateDevice {
                file_id: None,
                path: path("block")?,
                kind: FileKind::BlockDevice,
                major: 8,
                minor: 0,
                metadata: FileMetadata::default(),
            },
        ],
        B,
        &cancellation,
    ))
    .await?;
    assert!(receipt.value.created_file_ids.iter().all(Option::is_some));

    for (mutation, sentinel) in [
        (
            AuthoredMutation::CreateEmptySpecial {
                file_id: None,
                path: path("invalid-special")?,
                kind: FileKind::Regular,
                metadata: FileMetadata::default(),
            },
            path("invalid-special")?,
        ),
        (
            AuthoredMutation::CreateDevice {
                file_id: None,
                path: path("invalid-device")?,
                kind: FileKind::Regular,
                major: 0,
                minor: 0,
                metadata: FileMetadata::default(),
            },
            path("invalid-device")?,
        ),
    ] {
        let failure = (checkout.apply_authored_transaction(vec![mutation], B, &cancellation)).await;
        assert!(failure.is_err());
        assert!(
            (checkout.lookup_no_follow(&sentinel, B, &cancellation,))
                .await?
                .value
                .record
                .is_none()
        );
    }

    let mut windows = config();
    windows.profile = FilesystemProfile::Windows;
    let windows_volume =
        (fs.create_volume_with_id(VolumeId::from_bytes([116; 16]), windows, B, &cancellation))
            .await?
            .value;
    let mut windows_checkout =
        open_checkout(&windows_volume, writable_pinned(), &cancellation).await?;
    let reparse = path("reparse")?;
    let reparse_payload = OwnedBytes::from_static(b"opaque-reparse-data");
    let receipt = (windows_checkout.apply_authored_transaction(
        vec![AuthoredMutation::CreateReparsePoint {
            file_id: None,
            path: reparse.clone(),
            payload: reparse_payload.clone(),
            metadata: FileMetadata::default(),
        }],
        B,
        &cancellation,
    ))
    .await?;
    assert!(
        receipt
            .value
            .created_file_ids
            .first()
            .is_some_and(Option::is_some)
    );
    assert_eq!(
        (windows_checkout.read_reparse_point(&reparse, B, &cancellation,))
            .await?
            .value,
        reparse_payload
    );
    Ok(())
}
