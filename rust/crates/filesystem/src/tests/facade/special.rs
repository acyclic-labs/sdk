use super::*;

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
    let mut checkout = open_checkout(&volume, writable_pinned(), &cancellation).await?;
    let payload = OwnedBytes::from_static(b"opaque-reparse\0payload");
    (checkout.create(
        AuthoredMutation::CreateReparsePoint {
            file_id: None,
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
            file_id: None,
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
    assert!(empty_reparse_read.value.is_empty());

    let empty_link = path("empty-link")?;
    (checkout.create(
        AuthoredMutation::CreateSymbolicLink {
            file_id: None,
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
