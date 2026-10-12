use super::*;

#[tokio::test]
async fn authored_special_operations_are_profile_exact_and_fail_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut posix = config();
    posix.profile = FilesystemProfile::Posix;
    let volume =
        (fs.create_volume_with_id(VolumeId::from_bytes([115; 16]), posix, B, &cancellation))
            .await?
            .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        B,
        &cancellation,
    ))
    .await?
    .value;
    let receipt = (checkout.apply_authored_transaction(
        vec![
            AuthoredMutation::CreateEmptySpecial {
                path: path("fifo")?,
                kind: FileKind::Fifo,
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::CreateEmptySpecial {
                path: path("socket")?,
                kind: FileKind::Socket,
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::CreateEmptySpecial {
                path: path("mount")?,
                kind: FileKind::MountBoundary,
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::CreateDevice {
                path: path("character")?,
                kind: FileKind::CharacterDevice,
                major: 1,
                minor: 3,
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::CreateDevice {
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
                path: path("invalid-special")?,
                kind: FileKind::Regular,
                metadata: FileMetadata::default(),
            },
            path("invalid-special")?,
        ),
        (
            AuthoredMutation::CreateDevice {
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
    let mut windows_checkout = (windows_volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        B,
        &cancellation,
    ))
    .await?
    .value;
    let reparse = path("reparse")?;
    let reparse_payload = OwnedBytes::from_static(b"opaque-reparse-data");
    let receipt = (windows_checkout.apply_authored_transaction(
        vec![AuthoredMutation::CreateReparsePoint {
            path: reparse.clone(),
            payload: reparse_payload.clone(),
            metadata: FileMetadata::default(),
        }],
        B,
        &cancellation,
    ))
    .await?;
    assert!(receipt.value.created_file_ids[0].is_some());
    assert_eq!(
        (windows_checkout.read_reparse_point(&reparse, B, &cancellation,))
            .await?
            .value,
        reparse_payload
    );
    Ok(())
}

#[tokio::test]
async fn windows_reparse_payload_round_trips_without_interpretation()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut windows = config();
    windows.profile = FilesystemProfile::Windows;
    let volume =
        (fs.create_volume_with_id(VolumeId::from_bytes([98; 16]), windows, B, &cancellation))
            .await?
            .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        B,
        &cancellation,
    ))
    .await?
    .value;
    let payload = OwnedBytes::from_static(b"opaque-reparse\0payload");
    (checkout.create(
        AuthoredMutation::CreateReparsePoint {
            path: path("junction")?,
            payload: payload.clone(),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?;
    let read = (checkout.read_reparse_point(&path("junction")?, B, &cancellation)).await?;
    assert_eq!(read.value, payload);

    let empty_reparse = path("empty-reparse")?;
    (checkout.create(
        AuthoredMutation::CreateReparsePoint {
            path: empty_reparse.clone(),
            payload: OwnedBytes::new(),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?;
    let empty_reparse_read =
        (checkout.read_reparse_point(&empty_reparse, B, &cancellation)).await?;
    assert!(empty_reparse_read.value.bytes.is_empty());

    let empty_link = path("empty-link")?;
    (checkout.create(
        AuthoredMutation::CreateSymbolicLink {
            path: empty_link.clone(),
            target: OwnedBytes::new(),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?;
    let empty_link_read = (checkout.read_symbolic_link(&empty_link, B, &cancellation)).await?;
    assert!(empty_link_read.value.bytes.is_empty());
    Ok(())
}
