use acyclic_fs::kernel::{GenerationMutationError, MAXIMUM_INLINE_FILE_BYTES, Mutation};
use acyclic_fs::{
    AuthoredMutation, ByteRange, CancellationToken, ContentChange, ContentTimes, FileCloneRequest,
    FsError,
};
use acyclic_fs_format::{
    identity::FileId,
    metadata::{FileMetadata, MetadataField},
    object_reference::ObjectKind,
};
use acyclic_fs_work::performance::OperationFailure;
use acyclic_stream::OwnedBytes;

use super::B;
use super::checkout_fixture::{config, path};
use super::fixtures::{TestResult, create, fixture};
use super::volume_read::read;

#[tokio::test]
async fn authored_transaction_is_atomic_and_preserves_result_positions() -> TestResult {
    let cancel = CancellationToken::new();
    let (_, mut checkout) = fixture(config(), &cancel).await?;
    let transaction = checkout
        .apply_authored_transaction(
            vec![
                AuthoredMutation::CreateDirectory {
                    file_id: None,
                    path: path("workspace")?,
                    metadata: FileMetadata::default(),
                },
                AuthoredMutation::CreateFile {
                    file_id: None,
                    path: path("file.txt")?,
                    bytes: OwnedBytes::from_static(b"initial"),
                    metadata: FileMetadata::default(),
                },
                AuthoredMutation::Write {
                    path: path("file.txt")?,
                    offset: 7,
                    bytes: OwnedBytes::from_static(b"-atomic"),
                },
            ],
            B,
            &cancel,
        )
        .await?
        .value;
    let [Some(_), Some(file_id), None] = transaction.created_file_ids.as_slice() else {
        return Err("unexpected authored transaction result positions".into());
    };
    assert_eq!(
        read(&mut checkout, *file_id, 0, 14, &cancel)
            .await?
            .as_ref(),
        b"initial-atomic"
    );
    assert!(
        checkout
            .apply_authored_transaction(
                vec![
                    AuthoredMutation::CreateFile {
                        file_id: None,
                        path: path("orphan")?,
                        bytes: OwnedBytes::from_static(b"never-visible"),
                        metadata: FileMetadata::default()
                    },
                    AuthoredMutation::Remove {
                        path: path("absent")?,
                        expected_file_id: None
                    },
                ],
                B,
                &cancel
            )
            .await
            .is_err()
    );
    assert!(
        checkout
            .lookup_no_follow(&path("orphan")?, B, &cancel)
            .await?
            .value
            .record
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn inverse_private_mutations_restore_the_base_and_clear_replay_state() -> TestResult {
    let cancel = CancellationToken::new();
    let (_, mut checkout) = fixture(config(), &cancel).await?;
    let base = checkout.generation_id();
    create(
        &mut checkout,
        "transient",
        OwnedBytes::from_static(b"temporary"),
        &cancel,
    )
    .await?;
    assert!(checkout.has_pending_mutations());
    checkout
        .mutate(
            vec![Mutation::Remove {
                path: path("transient")?,
                expected_file_id: MetadataField::Unavailable,
            }],
            B,
            &cancel,
        )
        .await?;
    assert!(!checkout.has_pending_mutations());
    assert_eq!(checkout.generation_id(), base);
    Ok(())
}

#[tokio::test]
async fn authored_transactions_preflight_expansion_noops_and_byte_bounds() -> TestResult {
    let cancel = CancellationToken::new();
    let limited = config();
    let (_, mut checkout) = fixture(limited, &cancel).await?;
    assert!(matches!(
        checkout
            .apply_authored_transaction(
                vec![AuthoredMutation::Write {
                    path: path("absent")?,
                    offset: 0,
                    bytes: OwnedBytes::new()
                }],
                B,
                &cancel
            )
            .await,
        Err(OperationFailure {
            error: FsError::Mutation(GenerationMutationError::MissingSource),
            ..
        })
    ));
    let (_, mut noop) = fixture(config(), &cancel).await?;
    assert!(matches!(
        noop.apply_authored_transaction(
            vec![
                AuthoredMutation::Write {
                    path: path("ordered")?,
                    offset: 0,
                    bytes: OwnedBytes::new()
                },
                AuthoredMutation::CreateFile {
                    file_id: None,
                    path: path("ordered")?,
                    bytes: OwnedBytes::new(),
                    metadata: FileMetadata::default()
                }
            ],
            B,
            &cancel
        )
        .await,
        Err(OperationFailure {
            error: FsError::Mutation(GenerationMutationError::MissingSource),
            ..
        })
    ));
    assert!(
        noop.lookup_no_follow(&path("ordered")?, B, &cancel)
            .await?
            .value
            .record
            .is_none()
    );
    assert!(matches!(
        noop.change_content(
            path("missing")?,
            ContentChange::Write {
                offset: 0,
                bytes: OwnedBytes::new()
            },
            ContentTimes::Preserve,
            B,
            &cancel
        )
        .await,
        Err(OperationFailure {
            error: FsError::NotFound,
            ..
        })
    ));
    let id = create(&mut noop, "present", OwnedBytes::new(), &cancel).await?;
    let generation = noop.generation_id();
    assert!(
        noop.apply_authored_transaction(Vec::new(), B, &cancel)
            .await?
            .value
            .created_file_ids
            .is_empty()
    );
    assert_eq!(
        noop.apply_authored_transaction(
            vec![AuthoredMutation::Write {
                path: path("present")?,
                offset: 0,
                bytes: OwnedBytes::new()
            }],
            B,
            &cancel
        )
        .await?
        .value
        .created_file_ids,
        vec![None]
    );
    let mut empty = std::io::Cursor::new(OwnedBytes::new());
    let content = noop.stage_content(&mut empty, 1, B, &cancel).await?.value;
    assert_eq!(content.logical_bytes(), 0);
    assert_eq!(
        noop.apply_authored_transaction(
            vec![AuthoredMutation::WriteFromContent {
                path: path("present")?,
                offset: u64::MAX,
                content
            }],
            B,
            &cancel
        )
        .await?
        .value
        .created_file_ids,
        vec![None]
    );
    noop.change_content(
        path("present")?,
        ContentChange::Write {
            offset: u64::MAX,
            bytes: OwnedBytes::new(),
        },
        ContentTimes::Preserve,
        B,
        &cancel,
    )
    .await?;
    noop.change_content(
        id,
        ContentChange::Write {
            offset: u64::MAX,
            bytes: OwnedBytes::new(),
        },
        ContentTimes::Preserve,
        B,
        &cancel,
    )
    .await?;
    assert_eq!(noop.generation_id(), generation);
    Ok(())
}

#[tokio::test]
async fn authored_transactions_cover_every_portable_operation_without_hidden_paths()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let (_, mut checkout) = fixture(config(), &cancellation).await?;
    let mut create_source = std::io::Cursor::new(OwnedBytes::from_static(b"streamed"));
    let staged_create = (checkout.stage_content(&mut create_source, 64, B, &cancellation))
        .await?
        .value;
    let mut write_source = std::io::Cursor::new(OwnedBytes::from_static(b"TAIL"));
    let staged_write = (checkout.stage_content(&mut write_source, 64, B, &cancellation))
        .await?
        .value;
    assert_eq!(staged_create.logical_bytes(), 8);
    assert_eq!(staged_create.root().kind, ObjectKind::Blob);
    assert_eq!(staged_write.logical_bytes(), 4);
    assert_eq!(staged_write.root().kind, ObjectKind::Blob);

    let source = path("source")?;
    let destination = path("destination")?;
    let streamed = path("streamed")?;
    let streamed_identity = FileId::from_bytes([199; 16]);
    let hard_link = path("hard-link")?;
    let renamed = path("renamed")?;
    let replacement_metadata = FileMetadata {
        modified_ns: MetadataField::Value(42),
        ..FileMetadata::default()
    };
    let large = OwnedBytes::from(vec![0x5a; MAXIMUM_INLINE_FILE_BYTES + 1]);
    let transaction = (checkout.apply_authored_transaction(
        vec![
            AuthoredMutation::CreateDirectory {
                file_id: None,
                path: path("directory")?,
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::CreateFile {
                file_id: None,
                path: source.clone(),
                bytes: OwnedBytes::from_static(b"abcdefgh"),
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::CreateFile {
                file_id: None,
                path: destination.clone(),
                bytes: OwnedBytes::from_static(b"................"),
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::CreateFile {
                file_id: None,
                path: path("large")?,
                bytes: large.clone(),
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::CreateFileFromContent {
                path: streamed.clone(),
                content: staged_create,
                metadata: FileMetadata::default(),
                file_id: Some(streamed_identity),
            },
            AuthoredMutation::CreateSymbolicLink {
                file_id: None,
                path: path("symbolic")?,
                target: OwnedBytes::from_static(b"../source"),
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::Write {
                path: source.clone(),
                offset: 0,
                bytes: OwnedBytes::new(),
            },
            AuthoredMutation::Write {
                path: source.clone(),
                offset: 6,
                bytes: OwnedBytes::from_static(b"XY"),
            },
            AuthoredMutation::WriteFromContent {
                path: streamed.clone(),
                offset: 8,
                content: staged_write,
            },
            AuthoredMutation::SetMetadata {
                path: source.clone(),
                metadata: replacement_metadata,
            },
            AuthoredMutation::Resize {
                path: destination.clone(),
                logical_bytes: 24,
            },
            AuthoredMutation::ZeroRange {
                path: source.clone(),
                range: ByteRange {
                    offset: 2,
                    length: 2,
                },
                allocated: false,
                extend: false,
            },
            AuthoredMutation::Preallocate {
                path: destination.clone(),
                range: ByteRange {
                    offset: 16,
                    length: 8,
                },
                keep_size: false,
            },
            AuthoredMutation::CloneRange(FileCloneRequest {
                source: source.clone(),
                source_offset: 0,
                destination: destination.clone(),
                destination_offset: 4,
                length: 8,
            }),
            AuthoredMutation::HardLink {
                source: source.clone(),
                destination: hard_link.clone(),
            },
            AuthoredMutation::Rename {
                source: hard_link,
                destination: renamed.clone(),
                replace: false,
            },
            AuthoredMutation::Remove {
                path: renamed,
                expected_file_id: None,
            },
        ],
        B,
        &cancellation,
    ))
    .await?;
    assert_eq!(transaction.value.created_file_ids.len(), 17);
    assert_eq!(
        transaction.value.created_file_ids.get(4),
        Some(&Some(streamed_identity))
    );
    assert_eq!(
        transaction
            .value
            .created_file_ids
            .iter()
            .filter(|created| created.is_some())
            .count(),
        6
    );
    assert_eq!(
        (checkout.read_file_range(
            &streamed,
            ByteRange {
                offset: 0,
                length: 12,
            },
            B,
            &cancellation,
        ))
        .await?
        .value
        .bytes
        .as_ref(),
        b"streamedTAIL"
    );
    assert!(
        (checkout.apply_authored_transaction(
            vec![AuthoredMutation::CreateFileFromContent {
                path: path("duplicate-source-identity")?,
                content: staged_create,
                metadata: FileMetadata::default(),
                file_id: Some(streamed_identity),
            }],
            B,
            &cancellation,
        ))
        .await
        .is_err()
    );
    assert_eq!(
        (checkout.read_file_range(
            &path("large")?,
            ByteRange {
                offset: 0,
                length: u64::try_from(large.len())?,
            },
            B,
            &cancellation,
        ))
        .await?
        .value
        .bytes,
        large
    );
    assert_eq!(
        (checkout.read_symbolic_link(&path("symbolic")?, B, &cancellation,))
            .await?
            .value
            .bytes
            .as_ref(),
        b"../source"
    );
    assert_eq!(
        (checkout.read_metadata(&source, B, &cancellation,))
            .await?
            .value
            .modified_ns,
        MetadataField::Value(42)
    );
    Ok(())
}
