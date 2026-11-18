use acyclic_fs::{CancellationToken, Fs, FsError, Lifecycle, VolumeConfig, WorkspaceError};
use acyclic_fs_format::identity::OperationId;
use acyclic_fs_work::performance::{OperationFailure, OperationReceipt, WorkBudget};
use acyclic_stream::OwnedBytes;
use std::error::Error;

use crate::creation_budget::{FINITE_BUDGET, READ_ONLY_BUDGET};

#[tokio::test]
async fn creation_publishes_files_in_its_one_commit_and_recovers_only_them()
-> Result<(), Box<dyn Error>> {
    let config = acyclic_fs::VolumeInitialization {
        config: VolumeConfig::portable(Lifecycle::Ephemeral),
        access_policy: Some(acyclic_fs::access::INITIAL_POLICY),
    };
    let operation = OperationId::from_bytes([0xe1; 16]);
    let cancellation = CancellationToken::new();
    let budget = FINITE_BUDGET;
    let files = [
        ("/report.json", OwnedBytes::from_static(b"{\"ok\":true}")),
        ("/request.json", OwnedBytes::from_static(b"{}")),
    ];

    // The zero-files case of the same path is the empty workspace.
    let empty_fs = Fs::memory();
    let empty = empty_fs
        .create_workspace_with_operation::<_, (), WorkspaceError>(
            "journal",
            config,
            operation,
            budget,
            &cancellation,
            None,
        )
        .await?;
    let empty_workspace = unbounded!(empty_fs, open_workspace("journal"))?;
    assert!(unbounded!(empty_workspace, stat("/report.json")).is_err());

    let fs = Fs::memory();
    let created = fs
        .create_workspace_with_operation::<_, (), WorkspaceError>(
            "journal",
            config,
            acyclic_fs::CreationOperation::populated(operation, &files),
            budget,
            &cancellation,
            None,
        )
        .await?;
    // Files cost no authority record beyond the creation commit itself.
    assert_eq!(
        created.work.authority_records_appended,
        empty.work.authority_records_appended
    );
    assert_ne!(created.value.id(), empty.value.id());
    let workspace = unbounded!(fs, open_workspace("journal"))?;
    assert_eq!(unbounded!(workspace, head())?.id(), created.value.id());
    for (path, bytes) in &files {
        assert_eq!(
            unbounded!(workspace, read_range(path, 0, bytes.len() as u64))?.bytes,
            bytes.clone()
        );
    }

    // A retry of the same creation recovers it by reads alone, under a
    // budget that forbids every write.
    let read_only = READ_ONLY_BUDGET;
    let retried = fs
        .create_workspace_with_operation::<_, (), WorkspaceError>(
            "journal",
            config,
            acyclic_fs::CreationOperation::populated(operation, &files),
            read_only,
            &cancellation,
            None,
        )
        .await?;
    assert_eq!(retried.value.id(), created.value.id());
    assert_eq!(retried.work.authority_records_appended, 0);
    assert_eq!(retried.work.backend_write_operations, 0);

    // Negative controls: the same operation with other files, with no files,
    // or other files under another operation never recovers this creation,
    // and each is rejected by reads, not by the forbidden writes.
    let other = [("/report.json", OwnedBytes::from_static(b"{\"ok\":false}"))];
    let superset = [
        files[0].clone(),
        files[1].clone(),
        ("/extra.json", OwnedBytes::from_static(b"{}")),
    ];
    let repeated = [files[0].clone(), files[0].clone()];
    for (operation, files) in [
        (operation, &other[..]),
        (operation, &files[..1]),
        (operation, &superset[..]),
        (operation, &repeated[..]),
        (operation, &[][..]),
        (OperationId::from_bytes([0xe2; 16]), &files[..]),
    ] {
        let failure = fs
            .create_workspace_with_operation::<_, (), WorkspaceError>(
                "journal",
                config,
                acyclic_fs::CreationOperation::populated(operation, files),
                read_only,
                &cancellation,
                None,
            )
            .await
            .err()
            .ok_or("mismatched creation recovered")?;
        assert!(
            matches!(
                failure.error,
                WorkspaceError::Filesystem(FsError::CreationRejected)
            ),
            "{:?}",
            failure.error
        );
        assert_eq!(failure.work()?.authority_records_appended, 0);
    }
    assert_eq!(unbounded!(workspace, head())?.id(), created.value.id());
    Ok(())
}

#[tokio::test]
async fn creation_attributes_are_published_and_recovery_requires_them() -> Result<(), Box<dyn Error>>
{
    let config = acyclic_fs::VolumeInitialization {
        config: VolumeConfig::portable(Lifecycle::Ephemeral),
        access_policy: Some(acyclic_fs::access::INITIAL_POLICY),
    };
    let operation = OperationId::from_bytes([0xe5; 16]);
    let cancellation = CancellationToken::new();
    let files = [("/report.json", OwnedBytes::from_static(b"{\"ok\":true}"))];
    let name = acyclic_fs::kernel::AttributeName::new(
        acyclic_fs::kernel::AttributeClass::PosixXattr,
        b"user.creation".to_vec(),
    )?;
    let value = OwnedBytes::from_static(b"metadata");
    let attributes = vec![acyclic_fs::CreationAttribute {
        path: "/report.json".into(),
        name: name.clone(),
        value: value.clone(),
    }];
    let fs = Fs::memory();
    let created = fs
        .create_workspace_with_operation::<_, (), WorkspaceError>(
            "journal",
            config,
            acyclic_fs::CreationOperation::populated_with_attributes(
                operation,
                &files,
                attributes.clone(),
            ),
            FINITE_BUDGET,
            &cancellation,
            None,
        )
        .await?;
    let workspace = unbounded!(fs, open_workspace("journal"))?;
    let head = unbounded!(workspace, head())?;
    assert_eq!(
        unbounded!(head, read_named_attribute("/report.json", &name, None))?
            .map(|chunk| chunk.bytes),
        Some(value.clone())
    );
    let retried = fs
        .create_workspace_with_operation::<_, (), WorkspaceError>(
            "journal",
            config,
            acyclic_fs::CreationOperation::populated_with_attributes(operation, &files, attributes),
            READ_ONLY_BUDGET,
            &cancellation,
            None,
        )
        .await?;
    assert_eq!(retried.value.id(), created.value.id());
    // Negative controls: a retry that omits the creation's attribute, names
    // an attribute on an unknown path, or repeats one attribute is not this
    // creation, and recovery rejects each by reads alone.
    let omitted = acyclic_fs::CreationOperation::populated(operation, &files);
    let unknown = vec![acyclic_fs::CreationAttribute {
        path: "/missing.json".into(),
        name: name.clone(),
        value: value.clone(),
    }];
    let repeated = vec![
        acyclic_fs::CreationAttribute {
            path: "/report.json".into(),
            name: name.clone(),
            value: value.clone(),
        },
        acyclic_fs::CreationAttribute {
            path: "/report.json".into(),
            name: name.clone(),
            value: value.clone(),
        },
    ];
    for creation in [
        omitted,
        acyclic_fs::CreationOperation::populated_with_attributes(operation, &files, unknown),
        acyclic_fs::CreationOperation::populated_with_attributes(operation, &files, repeated),
    ] {
        let failure = fs
            .create_workspace_with_operation::<_, (), WorkspaceError>(
                "journal",
                config,
                creation,
                READ_ONLY_BUDGET,
                &cancellation,
                None,
            )
            .await
            .err()
            .ok_or("a mismatched attribute creation recovered")?;
        assert!(
            matches!(
                failure.error,
                WorkspaceError::Filesystem(FsError::CreationRejected)
            ),
            "{:?}",
            failure.error
        );
    }
    // Negative control: the same operation and files with another attribute
    // value are not this creation, and recovery rejects them by reads alone.
    let changed = vec![acyclic_fs::CreationAttribute {
        path: "/report.json".into(),
        name,
        value: OwnedBytes::from_static(b"other"),
    }];
    let failure = fs
        .create_workspace_with_operation::<_, (), WorkspaceError>(
            "journal",
            config,
            acyclic_fs::CreationOperation::populated_with_attributes(operation, &files, changed),
            READ_ONLY_BUDGET,
            &cancellation,
            None,
        )
        .await
        .err()
        .ok_or("a creation with another attribute value recovered")?;
    assert!(
        matches!(
            failure.error,
            WorkspaceError::Filesystem(FsError::CreationRejected)
        ),
        "{:?}",
        failure.error
    );
    Ok(())
}

#[tokio::test]
async fn empty_creation_with_attributes_is_rejected() -> Result<(), Box<dyn Error>> {
    let config = acyclic_fs::VolumeInitialization {
        config: VolumeConfig::portable(Lifecycle::Ephemeral),
        access_policy: Some(acyclic_fs::access::INITIAL_POLICY),
    };
    let operation = OperationId::from_bytes([0xe6; 16]);
    let cancellation = CancellationToken::new();
    let name = acyclic_fs::kernel::AttributeName::new(
        acyclic_fs::kernel::AttributeClass::PosixXattr,
        b"user.creation".to_vec(),
    )?;
    let attributes = vec![acyclic_fs::CreationAttribute {
        path: "/report.json".into(),
        name,
        value: OwnedBytes::from_static(b"metadata"),
    }];
    let fs = Fs::memory();
    // An empty creation publishes no file, so it has nowhere to put an
    // attribute. It must be rejected, not published with the attributes lost.
    let failure = fs
        .create_workspace_with_operation::<_, (), WorkspaceError>(
            "journal",
            config,
            acyclic_fs::CreationOperation::populated_with_attributes(operation, &[], attributes),
            FINITE_BUDGET,
            &cancellation,
            None,
        )
        .await
        .err()
        .ok_or("an empty creation with attributes was published")?;
    assert!(
        matches!(
            failure.error,
            WorkspaceError::Filesystem(FsError::CreationRejected)
        ),
        "{:?}",
        failure.error
    );
    // The rejected creation published nothing.
    assert!(unbounded!(fs, open_workspace("journal")).is_err());
    Ok(())
}

#[tokio::test]
async fn recovery_pages_a_root_larger_than_one_directory_page() -> Result<(), Box<dyn Error>> {
    let mut volume = VolumeConfig::portable(Lifecycle::Ephemeral);
    volume.limits.maximum_directory_page_entries = 2;
    let config = acyclic_fs::VolumeInitialization {
        config: volume,
        access_policy: Some(acyclic_fs::access::INITIAL_POLICY),
    };
    let operation = OperationId::from_bytes([0xe3; 16]);
    let cancellation = CancellationToken::new();
    let files = ["/a", "/b", "/c", "/d", "/e"].map(|path| (path, OwnedBytes::from_static(b"x")));
    let fs = Fs::memory();
    let created = fs
        .create_workspace_with_operation::<_, (), WorkspaceError>(
            "journal",
            config,
            acyclic_fs::CreationOperation::populated(operation, &files),
            FINITE_BUDGET,
            &cancellation,
            None,
        )
        .await?;
    // The root spans three pages of two entries, and the retry pages them.
    let retried = fs
        .create_workspace_with_operation::<_, (), WorkspaceError>(
            "journal",
            config,
            acyclic_fs::CreationOperation::populated(operation, &files),
            READ_ONLY_BUDGET,
            &cancellation,
            None,
        )
        .await?;
    assert_eq!(retried.value.id(), created.value.id());
    // Negative control: the entry only a later page lists is still counted.
    let failure = fs
        .create_workspace_with_operation::<_, (), WorkspaceError>(
            "journal",
            config,
            acyclic_fs::CreationOperation::populated(operation, &files[..4]),
            READ_ONLY_BUDGET,
            &cancellation,
            None,
        )
        .await
        .err()
        .ok_or("a creation with an unlisted extra file recovered")?;
    assert!(
        matches!(
            failure.error,
            WorkspaceError::Filesystem(FsError::CreationRejected)
        ),
        "{:?}",
        failure.error
    );
    Ok(())
}

async fn many_file_creation(
    fs: &acyclic_fs::MemoryFs,
    files: &[(&str, OwnedBytes)],
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<acyclic_fs::GenerationId>, Box<OperationFailure<WorkspaceError>>> {
    let config = acyclic_fs::VolumeInitialization {
        config: VolumeConfig::portable(Lifecycle::Ephemeral),
        access_policy: Some(acyclic_fs::access::INITIAL_POLICY),
    };
    let operation = OperationId::from_bytes([0xe4; 16]);
    fs.create_workspace_with_operation::<_, (), WorkspaceError>(
        "linear-retry",
        config,
        acyclic_fs::CreationOperation::populated(operation, files),
        budget,
        cancellation,
        None,
    )
    .await
    .map(|receipt| OperationReceipt {
        value: receipt.value.id(),
        work: receipt.work,
    })
    .map_err(Box::new)
}

fn assert_linear_work(count: usize, work: u64, previous: &mut Option<u64>) {
    assert!(
        work <= 512 * count as u64,
        "creation retry must use linear per-file work: {count} files examined {work} items"
    );
    if let Some(smaller) = *previous {
        assert!(
            work <= smaller * 5 / 2,
            "doubling the files exceeded linear retry work: {smaller} -> {work}"
        );
    }
    *previous = Some(work);
}

/// Exercise public recovery, including parsing, lookups, byte verification and
/// the complete paged root, under a per-file item allowance.
#[tokio::test]
async fn paged_file_creation_retry_has_linear_work() -> Result<(), Box<dyn Error>> {
    let cancellation = CancellationToken::new();
    let mut previous = None;
    let mut previous_reversed = None;
    for count in [1_024_usize, 2_048] {
        // Both sizes fill a directory page; doubling crosses its split boundary.
        // The exact work ceiling and growth ratio reject the pairwise scan.
        let names: Vec<_> = (0..count).map(|index| format!("/f{index:04}")).collect();
        let mut files: Vec<_> = names
            .iter()
            .map(|name| (name.as_str(), OwnedBytes::from_static(b"x")))
            .collect();
        let fs = Fs::memory();
        // Construct and inspect this larger fixture with a finite, linear
        // copy allowance. The independent item-work ceiling below is the
        // regression oracle; the old pairwise scan cannot satisfy it.
        let mut creation_allowance = FINITE_BUDGET;
        creation_allowance.bytes_copied = count as u64 * 64 * 1_024;
        creation_allowance.peak_allocation_bytes = count as u64 * 16 * 1_024;
        let created = many_file_creation(&fs, &files, creation_allowance, &cancellation)
            .await
            .map_err(|failure| format!("creating {count} files: {failure:?}"))?;
        eprintln!("creation files={count} work={:?}", created.work);
        let allowance = WorkBudget {
            bytes_copied: creation_allowance.bytes_copied,
            ..READ_ONLY_BUDGET
        };
        let retried = many_file_creation(&fs, &files, allowance, &cancellation)
            .await
            .map_err(|failure| format!("retrying {count} files: {failure:?}"))?;
        assert_eq!(retried.value, created.value);
        assert_eq!(retried.work.authority_records_appended, 0);
        assert_eq!(retried.work.backend_write_operations, 0);
        let work = retried.work.items_examined;
        eprintln!(
            "creation_retry files={count} created_items={} retry_items={work}",
            created.work.items_examined
        );
        assert_linear_work(count, work, &mut previous);
        assert!(
            work <= created.work.items_examined,
            "a read-only retry must fit the original creation's item allowance"
        );
        // Request order is not an authority fact. The shared sorter also
        // accepts a reverse-order retry and rejects nonadjacent duplicates.
        files.reverse();
        let reversed = many_file_creation(&fs, &files, allowance, &cancellation).await?;
        assert_eq!(reversed.value, created.value);
        eprintln!(
            "creation_retry files={count} reversed_items={}",
            reversed.work.items_examined
        );
        assert_linear_work(count, reversed.work.items_examined, &mut previous_reversed);
        let first = files.first().cloned().ok_or("empty retry fixture")?;
        *files.last_mut().ok_or("empty retry fixture")? = first;
        let failure = many_file_creation(&fs, &files, allowance, &cancellation)
            .await
            .err()
            .ok_or("nonadjacent duplicate creation recovered")?;
        assert!(
            matches!(
                failure.error,
                WorkspaceError::Filesystem(FsError::CreationRejected)
            ),
            "{:?}",
            failure.error
        );
    }
    Ok(())
}

/// Recovery groups attributes by path once, so a retry with one attribute per
/// file grows with the file count instead of scanning every attribute for
/// every file.
#[tokio::test]
async fn paged_attribute_creation_retry_has_linear_work() -> Result<(), Box<dyn Error>> {
    let cancellation = CancellationToken::new();
    let name = acyclic_fs::kernel::AttributeName::new(
        acyclic_fs::kernel::AttributeClass::PosixXattr,
        b"user.creation".to_vec(),
    )?;
    let value = OwnedBytes::from_static(b"metadata");
    let operation = OperationId::from_bytes([0xe6; 16]);
    let config = acyclic_fs::VolumeInitialization {
        config: VolumeConfig::portable(Lifecycle::Ephemeral),
        access_policy: Some(acyclic_fs::access::INITIAL_POLICY),
    };
    let mut previous = None;
    for count in [256_usize, 512] {
        let names: Vec<_> = (0..count).map(|index| format!("/f{index:04}")).collect();
        let files: Vec<_> = names
            .iter()
            .map(|path| (path.as_str(), OwnedBytes::from_static(b"x")))
            .collect();
        let attributes: Vec<_> = names
            .iter()
            .map(|path| acyclic_fs::CreationAttribute {
                path: path.clone(),
                name: name.clone(),
                value: value.clone(),
            })
            .collect();
        let creation = || {
            acyclic_fs::CreationOperation::populated_with_attributes(
                operation,
                &files,
                attributes.clone(),
            )
        };
        let fs = Fs::memory();
        let mut creation_allowance = FINITE_BUDGET;
        creation_allowance.bytes_copied = count as u64 * 64 * 1024;
        creation_allowance.peak_allocation_bytes = count as u64 * 16 * 1024;
        let created = fs
            .create_workspace_with_operation::<_, (), WorkspaceError>(
                "linear-attributes",
                config,
                creation(),
                creation_allowance,
                &cancellation,
                None,
            )
            .await
            .map_err(|failure| format!("creating {count} attributes: {failure:?}"))?;
        let allowance = WorkBudget {
            bytes_copied: creation_allowance.bytes_copied,
            ..READ_ONLY_BUDGET
        };
        let retried = fs
            .create_workspace_with_operation::<_, (), WorkspaceError>(
                "linear-attributes",
                config,
                creation(),
                allowance,
                &cancellation,
                None,
            )
            .await
            .map_err(|failure| format!("retrying {count} attributes: {failure:?}"))?;
        assert_eq!(retried.value.id(), created.value.id());
        assert_eq!(retried.work.authority_records_appended, 0);
        assert_eq!(retried.work.backend_write_operations, 0);
        assert_linear_work(count, retried.work.items_examined, &mut previous);
    }
    Ok(())
}

/// Recovery reads each attribute value and releases it before reading the
/// next, so a retry's peak allocation is bounded by one value, not by every
/// value it compared.
#[tokio::test]
async fn attribute_creation_retry_peak_is_bounded_by_one_value() -> Result<(), Box<dyn Error>> {
    let cancellation = CancellationToken::new();
    let operation = OperationId::from_bytes([0xe8; 16]);
    let config = acyclic_fs::VolumeInitialization {
        config: VolumeConfig::portable(Lifecycle::Ephemeral),
        access_policy: Some(acyclic_fs::access::INITIAL_POLICY),
    };
    let files = [("/f", OwnedBytes::from_static(b"x"))];
    let count = 64_usize;
    let value_bytes = 64 * 1024_usize;
    let value = OwnedBytes::from(vec![0x5a; value_bytes]);
    let attributes = (0..count)
        .map(|index| {
            Ok(acyclic_fs::CreationAttribute {
                path: "/f".to_owned(),
                name: acyclic_fs::kernel::AttributeName::new(
                    acyclic_fs::kernel::AttributeClass::PosixXattr,
                    format!("user.{index:04}").into_bytes(),
                )?,
                value: value.clone(),
            })
        })
        .collect::<Result<Vec<_>, acyclic_fs::kernel::AttributeError>>()?;
    let creation = || {
        acyclic_fs::CreationOperation::populated_with_attributes(
            operation,
            &files,
            attributes.clone(),
        )
    };
    let fs = Fs::memory();
    let created = fs
        .create_workspace_with_operation::<_, (), WorkspaceError>(
            "attribute-values",
            config,
            creation(),
            WorkBudget::UNBOUNDED,
            &cancellation,
            None,
        )
        .await?;
    // One value plus the listing and file reads fit; every value at once does not.
    let mut allowance = READ_ONLY_BUDGET;
    allowance.peak_allocation_bytes = 1024 * 1024;
    let retried = fs
        .create_workspace_with_operation::<_, (), WorkspaceError>(
            "attribute-values",
            config,
            creation(),
            allowance,
            &cancellation,
            None,
        )
        .await
        .map_err(|failure| format!("retrying {count} attribute values: {failure:?}"))?;
    assert_eq!(retried.value.id(), created.value.id());
    Ok(())
}
