use super::*;

#[tokio::test]
async fn every_object_backend_cut_preserves_facade_atomicity_and_retry()
-> Result<(), Box<dyn std::error::Error>> {
    let control = Arc::new(FaultControl::disabled());
    let fs = Fs::new(
        crate::memory::MemoryAuthorityStore::default(),
        FaultObjectStore {
            inner: crate::memory::MemoryObjectStore::default(),
            control: Arc::clone(&control),
        },
        EmbeddedCapabilities::MEMORY,
    );
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation)).await?
        .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;

    macro_rules! through_every_object_cut {
        ($operation:expr) => {{
            let original_state = checkout_state(&checkout);
            let mut success = None;
            for cut in 1..=256 {
                control.arm(cut);
                let outcome =
                    ($operation).await;
                match outcome {
                    Ok(receipt) => {
                        success = Some(receipt);
                        break;
                    }
                    Err(failure) => {
                        assert!(
                            failure.work.backend_read_operations
                                + failure.work.backend_write_operations
                                > 0
                        );
                        assert_eq!(checkout_state(&checkout), original_state);
                    }
                }
            }
            control.disable();
            success.ok_or_else(|| {
                format!(
                    "facade operation never passed cut sweep: {}",
                    stringify!($operation)
                )
            })?
        }};
    }

    let large = path("fault-large")?;
    let large_id = through_every_object_cut!(checkout.create(crate::AuthoredMutation::CreateFile { path: large.clone(), bytes: (bytes::Bytes::from(vec![b'x'; 4_096])).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))
    .value;
    let inline = path("fault-inline")?;
    let inline_id = through_every_object_cut!(checkout.create(crate::AuthoredMutation::CreateFile { path: inline.clone(), bytes: (bytes::Bytes::from_static(b"inline")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))
    .value;
    through_every_object_cut!(checkout.create(crate::AuthoredMutation::CreateDirectory { path: path("fault-directory")?, metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation));
    let authored = path("fault-authored-large")?;
    through_every_object_cut!(checkout.apply_authored_transaction(
        vec![AuthoredMutation::CreateFile {
            path: authored.clone(),
            bytes: OwnedBytes::from(vec![b'a'; 4_096]),
            metadata: FileMetadata::default(),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_object_cut!(checkout.apply_authored_transaction(
        vec![AuthoredMutation::CreateDirectory {
            path: path("fault-authored-directory")?,
            metadata: FileMetadata::default(),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_object_cut!(checkout.apply_authored_transaction(
        vec![AuthoredMutation::CreateSymbolicLink {
            path: path("fault-authored-link")?,
            target: OwnedBytes::from_static(b"fault-authored-large"),
            metadata: FileMetadata::default(),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_object_cut!(checkout.apply_authored_transaction(
        vec![AuthoredMutation::Write {
            path: authored.clone(),
            offset: 4_096,
            bytes: OwnedBytes::from_static(b"-fault-swept"),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_object_cut!(checkout.apply_authored_transaction(
        vec![AuthoredMutation::SetMetadata {
            path: authored.clone(),
            metadata: FileMetadata::default(),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    control.disable();
    let mut staged_source = std::io::Cursor::new(OwnedBytes::from_static(b"staged-fault-content"));
    let staged = (checkout.stage_content(
        &mut staged_source,
        64,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    through_every_object_cut!(checkout.apply_authored_transaction(
        vec![AuthoredMutation::CreateFileFromContent {
            path: path("fault-authored-content")?,
            content: staged,
            metadata: FileMetadata::default(),
            file_id: None,
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));

    let range = ByteRange {
        offset: 1,
        length: 64,
    };
    let path_read = through_every_object_cut!(checkout.read_file_range(
        &large,
        range,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!((path_read.value.bytes).as_ref(), OwnedBytes::from(vec![b'x'; 64]).as_ref());
    let identity_read = through_every_object_cut!(checkout.read_file_range_by_id(
        large_id,
        range,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(identity_read.value.bytes, path_read.value.bytes);
    let path_plan = through_every_object_cut!(checkout.plan_file_extents(
        &large,
        range,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let identity_plan = through_every_object_cut!(checkout.plan_file_extents_by_id(
        large_id,
        range,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(
        path_plan.value.as_ref().map(|plan| &plan.spans),
        identity_plan.value.as_ref().map(|plan| &plan.spans)
    );
    assert_eq!(
        path_plan.value.as_ref().map(|plan| plan.spans.len()),
        Some(1)
    );
    let data_seek = through_every_object_cut!(checkout.seek_file_extent(
        &large,
        1,
        ExtentSeekTarget::Data,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(data_seek.value, Some(1));
    let hole_seek = through_every_object_cut!(checkout.seek_file_extent_by_id(
        large_id,
        1,
        ExtentSeekTarget::Hole,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(hole_seek.value, Some(4_096));
    let path_metadata = through_every_object_cut!(checkout.read_metadata(
        &large,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let identity_metadata = through_every_object_cut!(checkout.read_metadata_by_id(
        large_id,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(identity_metadata.value, path_metadata.value);
    let identity_record = through_every_object_cut!(checkout.read_file_record_by_id(
        large_id,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(identity_record.value.file_id, large_id);
    let batch = through_every_object_cut!(checkout.lookup_batch_no_follow(
        &[large.clone(), inline.clone(), path("fault-missing")?],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(batch.value.entries.len(), 3);
    assert!(batch.value.entries[0].record.is_some());
    assert!(batch.value.entries[1].record.is_some());
    assert!(batch.value.entries[2].record.is_none());
    let directory_names = through_every_object_cut!(checkout.list_directory(
        &root_path()?,
        None,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(directory_names.value.entries.len(), 7);
    let directory = through_every_object_cut!(checkout.list_directory_records(
        &root_path()?,
        std::ops::Bound::Unbounded,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(directory.value.entries.len(), 7);
    assert!(!directory.value.has_more);
    assert!(
        directory
            .value
            .entries
            .iter()
            .any(|entry| entry.record.file_id == large_id)
    );

    let attribute = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.fault-cut".to_vec(),
        config().limits.maximum_component_bytes,
    )?;
    through_every_object_cut!(checkout.change_named_attribute(large.clone(), &attribute, Some((&bytes::Bytes::from_static(b"fault-checked-value"), NamedAttributeWriteMode::Upsert)), WorkBudget::UNBOUNDED, &cancellation));
    let attribute_value = through_every_object_cut!(checkout.read_named_attribute(
        &large,
        &attribute,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(
        attribute_value.value.map(|chunk| chunk.bytes),
        Some(OwnedBytes::from_static(b"fault-checked-value"))
    );
    let attributes = through_every_object_cut!(checkout.list_named_attributes(
        &large,
        None,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(attributes.value.entries.len(), 1);
    assert_eq!(attributes.value.entries[0].name, attribute);
    assert!(!attributes.value.has_more);
    let identity_attribute = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.identity-fault-cut".to_vec(),
        config().limits.maximum_component_bytes,
    )?;
    through_every_object_cut!(checkout.change_named_attribute_by_id(large_id, &identity_attribute, Some((&OwnedBytes::from_static(b"identity-fault-checked-value"), NamedAttributeWriteMode::Upsert)), WorkBudget::UNBOUNDED, &cancellation));
    assert_eq!(
        through_every_object_cut!(checkout.read_named_attribute_by_id(
            large_id,
            &identity_attribute,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value.map(|chunk| chunk.bytes),
        Some(OwnedBytes::from_static(b"identity-fault-checked-value"))
    );
    let identity_attributes = through_every_object_cut!(checkout.list_named_attributes_by_id(
        large_id,
        None,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(identity_attributes.value.entries.len(), 2);
    through_every_object_cut!(checkout.change_named_attribute_by_id(large_id, &identity_attribute, None, WorkBudget::UNBOUNDED, &cancellation));
    through_every_object_cut!(checkout.change_named_attribute(large.clone(), &attribute, None, WorkBudget::UNBOUNDED, &cancellation));
    through_every_object_cut!(checkout.change_content(large_id, crate::ContentChange::Write { offset: 128, bytes: (bytes::Bytes::from_static(b"atomic-retry")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation));
    through_every_object_cut!(checkout.change_content(inline_id, crate::ContentChange::CloneFrom { source: large_id, source_offset: 0, offset: 0, length: 4 }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation));
    let cloned = through_every_object_cut!(checkout.read_file_range_by_id(
        inline_id,
        ByteRange {
            offset: 0,
            length: 6,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(cloned.value.bytes.as_ref(), b"xxxxne");
    let mut changed_metadata = FileMetadata::default();
    changed_metadata.modified_ns = MetadataField::Value(101);
    through_every_object_cut!(checkout.set_attributes_by_id(large_id, changed_metadata, None, WorkBudget::UNBOUNDED, &cancellation));
    changed_metadata.modified_ns = MetadataField::Value(102);
    through_every_object_cut!(checkout.set_attributes_by_id(
        large_id,
        changed_metadata,
        Some(4_224),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_object_cut!(checkout.change_content(large_id, crate::ContentChange::ZeroRange { range: ByteRange {
            offset: 4_096,
            length: 32,
        }, allocated: true, extend: true }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation));
    through_every_object_cut!(checkout.change_content(large_id, crate::ContentChange::Preallocate { range: ByteRange {
            offset: 4_160,
            length: 32,
        }, keep_size: true }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation));
    through_every_object_cut!(checkout.change_content(large_id, crate::ContentChange::Resize { logical_bytes: 4_192 }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation));
    changed_metadata.modified_ns = MetadataField::Value(103);
    through_every_object_cut!(checkout.apply_authored_transaction(vec![crate::AuthoredMutation::SetMetadata { path: large.clone(), metadata: changed_metadata }], WorkBudget::UNBOUNDED, &cancellation));
    changed_metadata.modified_ns = MetadataField::Value(104);
    through_every_object_cut!(checkout.set_attributes(
        large.clone(),
        changed_metadata,
        Some(4_224),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_object_cut!(checkout.change_content(large.clone(), crate::ContentChange::ZeroRange { range: ByteRange {
            offset: 4_096,
            length: 16,
        }, allocated: false, extend: false }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation));
    through_every_object_cut!(checkout.change_content(large.clone(), crate::ContentChange::Preallocate { range: ByteRange {
            offset: 4_176,
            length: 16,
        }, keep_size: true }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation));
    through_every_object_cut!(checkout.change_content(large.clone(), crate::ContentChange::Resize { logical_bytes: 4_192 }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation));
    through_every_object_cut!(checkout.change_content(inline.clone(), ContentChange::CloneFrom { source: large.clone(), source_offset: 0, offset: 1, length: 3 }, ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation));
    let mut detached = through_every_object_cut!(checkout.detach_regular_file(
        &large,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;

    macro_rules! through_every_detached_cut {
        ($operation:expr) => {{
            let original_record = detached.record;
            let mut success = None;
            for cut in 1..=256 {
                control.arm(cut);
                let outcome =
                    ($operation).await;
                match outcome {
                    Ok(receipt) => {
                        success = Some(receipt);
                        break;
                    }
                    Err(failure) => {
                        assert!(
                            failure.work.backend_read_operations
                                + failure.work.backend_write_operations
                                > 0
                        );
                        assert_eq!(detached.record, original_record);
                    }
                }
            }
            control.disable();
            success.ok_or_else(|| {
                format!(
                    "detached operation never passed cut sweep: {}",
                    stringify!($operation)
                )
            })?
        }};
    }

    let detached_read = through_every_detached_cut!(detached.read_range(
        range,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!((detached_read.value.bytes).as_ref(), OwnedBytes::from(vec![b'x'; 64]).as_ref());
    let detached_seek = through_every_detached_cut!(detached.seek(
        1,
        ExtentSeekTarget::Data,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(detached_seek.value, Some(1));
    let mut detached_metadata =
        through_every_detached_cut!(detached.read_metadata(WorkBudget::UNBOUNDED, &cancellation,))
            .value;
    detached_metadata.modified_ns = MetadataField::Value(42);
    through_every_detached_cut!(detached.set_attributes(detached_metadata, None, WorkBudget::UNBOUNDED, &cancellation));
    through_every_detached_cut!(detached.change_named_attribute(&attribute, Some((&OwnedBytes::from_static(b"detached fault value"), NamedAttributeWriteMode::Upsert)), WorkBudget::UNBOUNDED, &cancellation));
    let detached_attribute = through_every_detached_cut!(detached.read_named_attribute(
        &attribute,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(
        detached_attribute.value.map(|chunk| chunk.bytes),
        Some(OwnedBytes::from_static(b"detached fault value"))
    );
    let detached_attributes = through_every_detached_cut!(detached.list_named_attributes(
        None,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(detached_attributes.value.entries.len(), 1);
    assert_eq!(detached_attributes.value.entries[0].name, attribute);
    through_every_detached_cut!(detached.change_content(
        ContentChange::Write {
            offset: 256,
            bytes: OwnedBytes::from_static(b"detached atomic retry")
        },
        None,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_detached_cut!(detached.change_content(
        ContentChange::ZeroRange {
            range: ByteRange {
                offset: 512,
                length: 32,
            },
            allocated: true,
            extend: false
        },
        None,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_detached_cut!(detached.change_content(
        ContentChange::Preallocate {
            range: ByteRange {
                offset: 768,
                length: 32,
            },
            keep_size: false
        },
        None,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_detached_cut!(detached.change_content(
        ContentChange::Resize {
            logical_bytes: 2_048
        },
        None,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_detached_cut!(detached.change_named_attribute(&attribute, None, WorkBudget::UNBOUNDED, &cancellation));
    through_every_detached_cut!(detached.set_attributes(
        detached_metadata,
        Some(1_024),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    Ok(())
}
