use super::*;

#[tokio::test]
async fn named_attributes_are_sparse_bounded_and_atomic_with_metadata()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let fs = Fs::memory();
    let volume = (fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation)).await?;
    let mut checkout = (volume.value.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    let path = path("attributes.bin")?;
    let file_id = (checkout.value.create(
        crate::AuthoredMutation::CreateFile {
            path: path.clone(),
            bytes: (bytes::Bytes::new()).into(),
            metadata: crate::kernel::FileMetadata::default(),
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    let name = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.acyclic".to_vec(),
        128,
    )?;
    assert!(
        (checkout
            .value
            .read_named_attribute(&path, &name, WorkBudget::UNBOUNDED, &cancellation,))
        .await?
        .value
        .is_none()
    );
    let empty_listing = (checkout.value.list_named_attributes(
        &path,
        None,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    assert!(empty_listing.value.entries.is_empty());
    assert!(!empty_listing.value.has_more);
    let absent_remove = (checkout.value.change_named_attribute(
        path.clone(),
        &name,
        None,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("attribute-free removal unexpectedly succeeded")?;
    assert!(matches!(absent_remove.error, FsError::NotFound));
    let absent_replace = (checkout.value.change_named_attribute(
        path.clone(),
        &name,
        Some((
            &bytes::Bytes::from_static(b"replacement"),
            NamedAttributeWriteMode::Replace,
        )),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("absent attribute replacement unexpectedly succeeded")?;
    assert!(matches!(absent_replace.error, FsError::CreationRejected));
    (checkout.value.change_named_attribute(
        path.clone(),
        &name,
        Some((
            &bytes::Bytes::from_static(b"first"),
            NamedAttributeWriteMode::Create,
        )),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    let duplicate_create = (checkout.value.change_named_attribute(
        path.clone(),
        &name,
        Some((
            &bytes::Bytes::from_static(b"duplicate"),
            NamedAttributeWriteMode::Create,
        )),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("duplicate attribute creation unexpectedly succeeded")?;
    assert!(matches!(duplicate_create.error, FsError::CreationRejected));
    assert_eq!(
        (checkout
            .value
            .read_named_attribute(&path, &name, WorkBudget::UNBOUNDED, &cancellation,))
        .await?
        .value
        .map(|chunk| chunk.bytes)
        .as_deref(),
        Some(b"first".as_slice())
    );
    let listing = (checkout.value.list_named_attributes(
        &path,
        None,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    assert_eq!(listing.value.entries.len(), 1);
    assert_eq!(listing.value.entries[0].name, name);
    assert!(!listing.value.has_more);
    (checkout.value.change_named_attribute(
        path.clone(),
        &name,
        Some((
            &bytes::Bytes::from_static(b"second"),
            NamedAttributeWriteMode::Replace,
        )),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    (checkout.value.change_named_attribute(
        path.clone(),
        &name,
        None,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    assert!(
        (checkout
            .value
            .read_named_attribute(&path, &name, WorkBudget::UNBOUNDED, &cancellation,))
        .await?
        .value
        .is_none()
    );
    let removed_listing = (checkout.value.list_named_attributes(
        &path,
        None,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    assert!(removed_listing.value.entries.is_empty());
    assert!(!removed_listing.value.has_more);
    (checkout.value.change_named_attribute_by_id(
        file_id,
        &name,
        Some((&OwnedBytes::new(), NamedAttributeWriteMode::Upsert)),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    assert!(
        (checkout.value.read_named_attribute_by_id(
            file_id,
            &name,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .await?
        .value
        .ok_or("empty attribute missing")?
        .bytes
        .is_empty()
    );
    Ok(())
}
