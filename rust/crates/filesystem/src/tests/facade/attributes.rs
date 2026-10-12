use super::*;

#[tokio::test]
async fn named_attributes_are_sparse_bounded_and_atomic_with_metadata()
-> Result<(), Box<dyn std::error::Error>> {
    macro_rules! ready {
        ($future:expr) => {
            ($future).await?
        };
    }
    let cancellation = CancellationToken::new();
    let fs = Fs::memory();
    let volume = ready!(fs.create_volume(config(), B, &cancellation,));
    let mut checkout = ready!(volume.value.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        B,
        &cancellation,
    ));
    let path = path("attributes.bin")?;
    ready!(checkout.value.create(
        AuthoredMutation::CreateFile {
            path: path.clone(),
            bytes: OwnedBytes::new(),
            metadata: FileMetadata::default()
        },
        B,
        &cancellation
    ));
    let name = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.acyclic".to_vec(),
        128,
    )?;
    assert!(
        ready!(
            checkout
                .value
                .read_named_attribute(&path, &name, B, &cancellation,)
        )
        .value
        .is_none()
    );
    let empty_listing =
        ready!(
            checkout
                .value
                .list_named_attributes(&path, None, 8, B, &cancellation,)
        );
    assert!(empty_listing.value.entries.is_empty());
    assert!(!empty_listing.value.has_more);
    let absent_remove = (checkout.value.change_named_attribute(
        path.clone(),
        &name.clone(),
        None,
        B,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("attribute-free removal unexpectedly succeeded")?;
    assert!(matches!(absent_remove.error, FsError::NotFound));
    let absent_replace = (checkout.value.change_named_attribute(
        path.clone(),
        &name.clone(),
        Some((
            &OwnedBytes::from_static(b"replacement"),
            NamedAttributeWriteMode::Replace,
        )),
        B,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("absent attribute replacement unexpectedly succeeded")?;
    assert!(matches!(absent_replace.error, FsError::CreationRejected));
    ready!(checkout.value.change_named_attribute(
        path.clone(),
        &name.clone(),
        Some((
            &OwnedBytes::from_static(b"first"),
            NamedAttributeWriteMode::Create
        )),
        B,
        &cancellation
    ));
    let duplicate_create = (checkout.value.change_named_attribute(
        path.clone(),
        &name.clone(),
        Some((
            &OwnedBytes::from_static(b"duplicate"),
            NamedAttributeWriteMode::Create,
        )),
        B,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("duplicate attribute creation unexpectedly succeeded")?;
    assert!(matches!(duplicate_create.error, FsError::CreationRejected));
    assert_eq!(
        ready!(
            checkout
                .value
                .read_named_attribute(&path, &name, B, &cancellation,)
        )
        .value
        .map(|chunk| chunk.bytes)
        .as_deref(),
        Some(b"first".as_slice())
    );
    let listing = ready!(
        checkout
            .value
            .list_named_attributes(&path, None, 8, B, &cancellation,)
    );
    assert_eq!(listing.value.entries.len(), 1);
    assert_eq!(listing.value.entries[0].name, name);
    assert!(!listing.value.has_more);
    ready!(checkout.value.change_named_attribute(
        path.clone(),
        &name.clone(),
        Some((
            &OwnedBytes::from_static(b"second"),
            NamedAttributeWriteMode::Replace
        )),
        B,
        &cancellation
    ));
    ready!(checkout.value.change_named_attribute(
        path.clone(),
        &name.clone(),
        None,
        B,
        &cancellation
    ));
    assert!(
        ready!(
            checkout
                .value
                .read_named_attribute(&path, &name, B, &cancellation,)
        )
        .value
        .is_none()
    );
    let removed_listing =
        ready!(
            checkout
                .value
                .list_named_attributes(&path, None, 8, B, &cancellation,)
        );
    assert!(removed_listing.value.entries.is_empty());
    assert!(!removed_listing.value.has_more);
    Ok(())
}
