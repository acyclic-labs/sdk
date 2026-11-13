use super::*;
use crate::{AgentId, conversation::VolumeOwner, core::Authority};
use crate::{
    Capabilities,
    core::{AggregateKind, AuthorityRoot},
};
use acyclic_fs::{MemoryAuthorityBackend, MemoryObjectBackend};

type Host = FilesystemHost<MemoryAuthorityBackend, MemoryObjectBackend>;

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_file_progress_ignores_storage_layout_and_counts_creation() -> Result<()> {
    let bytes = vec![b'x'; 1_048_576];
    let (host, file) = published(&bytes).await?;
    let volume = file.volume();
    let reference = bounded_io::content_workspace(&host.provider, volume, None)?;
    let workspace = host.open(&reference, None).await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = acyclic_fs::WorkBudget::UNBOUNDED;
    let original = host
        .generation(&workspace, &host.file_generation(&file, None)?, None)
        .await?;
    let mut transaction = workspace
        .begin_transaction(
            acyclic_fs::IdempotencyKey::from_bytes([2; 16]),
            budget,
            &cancellation,
        )
        .await
        .map_err(|failure| failure.error)?
        .value;
    transaction
        .write_range(
            "/file.txt",
            1,
            bytes[1..bytes.len() - 1].to_vec(),
            budget,
            &cancellation,
        )
        .await
        .map_err(|failure| failure.error)?;
    let acyclic_fs::TransactionCommit::Committed(fragmented) = transaction
        .commit::<acyclic_fs::WorkspaceError, _, _>(
            None,
            acyclic_fs::PublicationPermit::Unrestricted,
            budget,
            &cancellation,
            |_| async {
                Ok(acyclic_fs::OperationReceipt {
                    value: (),
                    work: acyclic_fs::WorkCounters::UNCHARGED,
                })
            },
        )
        .await
        .map_err(|failure| failure.error)?
        .value
    else {
        return Err(Error::Invalid("range fixture did not commit".into()));
    };
    assert!(
        !fragmented
            .matches_file_payload(&original, "/file.txt", budget, &cancellation)
            .await
            .map_err(|failure| failure.error)?
            .value,
        "fixture did not change storage layout"
    );
    let (_issuer, write) = grant(volume, VolumeOperation::Write)?;
    let flattened = host
        .put_content(
            volume,
            &write,
            file.path(),
            &bytes,
            "text/plain",
            "file.txt",
            Some(bytes.len() as u64),
            &IdempotencyKey::new("flat-rewrite")?,
            None,
            crate::conversation::ContentAccess::default(),
        )
        .await?;
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(16_000_000, 16_000_000),
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = PreparationIo::from_native_work(&work)?;
    assert!(
        host.original_file_progress(&flattened, &io)
            .await?
            .is_none(),
        "same bytes counted as progress after a layout change"
    );
    assert!(
        work.receipt().source_bytes_read > 0,
        "partial chunk comparison lost its original byte admission"
    );
    let created = host
        .put_content(
            volume,
            &write,
            "new.txt",
            b"new",
            "text/plain",
            "new.txt",
            Some(3),
            &IdempotencyKey::new("new-file")?,
            None,
            crate::conversation::ContentAccess::default(),
        )
        .await?;
    let progress = host.original_file_progress(&created, &io).await?;
    let _hold = io.guard_owned(
        progress
            .as_ref()
            .map_or(Ok(0), GenerationRef::logical_owned_bytes)?,
    )?;
    assert!(progress.is_some(), "creation did not count as progress");
    drop(progress);
    Ok(())
}

async fn published(body: &[u8]) -> Result<(Arc<Host>, FileRef)> {
    let provider = ProviderRef::new("ranges".into(), "filesystem".into(), "1".into(), &mut None)?;
    let volume = VolumeRef::new(
        provider.clone(),
        "range-volume".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(AgentId::from_bytes([1; 16])),
        &mut None,
    )?;
    let host = FilesystemHost::new(Fs::memory(), &provider, None)?;
    host.create_volume(&volume).await?;
    let (_issuer, grant) = grant(&volume, VolumeOperation::Write)?;
    let file = host
        .put_content(
            &volume,
            &grant,
            "file.txt",
            body,
            "text/plain",
            "file.txt",
            Some(u64::MAX),
            &IdempotencyKey::new("publish")?,
            None,
            crate::conversation::ContentAccess::default(),
        )
        .await?;
    Ok((host, file))
}

fn grant(volume: &VolumeRef, operation: VolumeOperation) -> Result<(AuthorityRoot, ContentGrant)> {
    let issuer = AuthorityRoot::new(
        "ranges",
        [3; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "owner".into(),
        },
    );
    let scope = issuer.bootstrap_scope(
        "owner",
        &Capabilities::new([volume.capability(operation, &mut None)?]),
        Some(AgentId::from_bytes([1; 16])),
        &mut None,
    )?;
    let grant = if operation == VolumeOperation::Read {
        ContentGrant::verify_directory_read(
            &issuer.verifier(&mut None)?,
            &scope,
            volume,
            "",
            &mut None,
        )?
    } else {
        ContentGrant::verify(
            &issuer.verifier(&mut None)?,
            &scope,
            volume,
            operation,
            &mut None,
        )?
    };
    Ok((issuer, grant))
}

async fn ordinary(body: &[u8]) -> Result<(Arc<Host>, FileRef)> {
    let (host, uploaded) = published(b"").await?;
    let volume = uploaded.volume();
    let workspace = bounded_io::content_workspace(&host.provider, volume, None)?;
    let generation = host
        .apply(
            &workspace,
            None,
            &[crate::filesystem::WorkspaceMutation::PutFile {
                path: "/ordinary".into(),
                bytes: bytes::Bytes::copy_from_slice(body).into(),
            }],
            &IdempotencyKey::new("ordinary")?,
        )
        .await?;
    let (_issuer, read) = grant(volume, VolumeOperation::Read)?;
    let (reference, actual) = host
        .read_private_path(
            volume,
            &read,
            "ordinary",
            Some(&generation),
            body.len() as u64,
            crate::conversation::ContentAccess::default(),
        )
        .await?;
    assert_eq!(actual.as_ref(), body);
    Ok((host, reference))
}

async fn selection_case(body: Vec<u8>, a: u16, b: u16, mutate: bool) -> Result<()> {
    let (host, file) = published(&body).await?;
    let (a, b) = (
        usize::from(a) % (body.len() + 1),
        usize::from(b) % (body.len() + 1),
    );
    let (start, end) = (a.min(b), a.max(b));
    let range = acyclic_fs::ByteRange {
        offset: start as u64,
        length: (end - start) as u64,
    };
    assert_eq!(
        host.read_pinned_range(&file, range, None).await?.as_ref(),
        &body[start..end]
    );
    let mut sha = *file.descriptor().sha256();
    sha[0] ^= 1;
    let forged = FileRef::new(
        file.volume().clone(),
        file.path().into(),
        file.version().into(),
        FileDescriptor::new(sha, body.len() as u64, "text/plain".into())?,
        file.display_name().into(),
        &mut None,
    )?;
    assert!(host.read_pinned_range(&forged, range, None).await.is_err());
    let mut changed = body.clone();
    if let Some(first) = changed.first_mut() {
        *first ^= 1;
    } else {
        changed.push(1);
    }
    let workspace = bounded_io::content_workspace(&host.provider, file.volume(), None)?;
    let generation = host
        .apply(
            &workspace,
            None,
            &[crate::filesystem::WorkspaceMutation::PutFile {
                path: if mutate { "/file.txt" } else { "/unrelated" }.into(),
                bytes: changed.into(),
            }],
            &IdempotencyKey::new("raw-write")?,
        )
        .await?;
    let selected = FileRef::new(
        file.volume().clone(),
        file.path().into(),
        hex::encode(generation.as_resource().key()),
        file.descriptor().clone(),
        file.display_name().into(),
        &mut None,
    )?;
    let result = host.read_pinned_range(&selected, range, None).await;
    if mutate {
        assert!(result.is_err());
    } else {
        assert_eq!(result?.as_ref(), &body[start..end]);
    }
    assert_eq!(
        host.read_pinned_range(&file, range, None).await?.as_ref(),
        &body[start..end]
    );
    Ok(())
}

proptest::proptest! {
    // All intervals match the immutable publication. Substituted descriptors and
    // stale indexes after raw writes cannot authenticate any interval, even empty.
    #[test]
    fn publication_ranges_preserve_identity(
        body in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..4096),
        a in proptest::prelude::any::<u16>(), b in proptest::prelude::any::<u16>(),
        mutate in proptest::prelude::any::<bool>(),
    ) {
        let result = futures::executor::block_on(selection_case(body, a, b, mutate));
        proptest::prop_assert!(result.is_ok(), "{result:?}");
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn large_publication_range_accounts_authentication_and_reads_only_the_selected_chunk()
-> Result<()> {
    let (host, file) = published(&vec![b'x'; 2 * 1024 * 1024]).await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = acyclic_fs::WorkBudget {
        source_bytes_read: 64 * 1024,
        bytes_hashed: 64 * 1024,
        bytes_copied: 64 * 1024,
        ..acyclic_fs::WorkBudget::UNBOUNDED
    };
    let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = PreparationIo::from_native_work(&work)?;
    let range = acyclic_fs::ByteRange {
        offset: 1024,
        length: 16,
    };
    let denied = host.read_pinned_range(&file, range, Some(&io)).await;
    // Authenticating an immutable blob requires its complete selected chunk.
    // This deliberately insufficient allowance must fail on the original ledger.
    assert!(matches!(
        denied,
        Err(Error::NativeWorkspace { source, .. }) if matches!(source.as_ref(),
            acyclic_fs::WorkspaceError::Work(acyclic_fs::WorkError::BudgetExceeded {
                counter: "bytes_hashed", maximum: 65536, ..
            })
        )
    ));
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget {
            source_bytes_read: 64 * 1024,
            bytes_copied: 64 * 1024,
            ..acyclic_fs::WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = PreparationIo::from_native_work(&work)?;
    let bytes = host.read_pinned_range(&file, range, Some(&io)).await?;
    assert_eq!(bytes.as_ref(), b"xxxxxxxxxxxxxxxx");
    assert!(work.receipt().source_bytes_read < 64 * 1024);
    assert!(work.receipt().bytes_copied < 64 * 1024);
    assert!(work.receipt().bytes_hashed < file.descriptor().byte_length());
    assert!(work.receipt().object_bytes_read < file.descriptor().byte_length());
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn private_generation_selection_preserves_absence_and_authority() -> Result<()> {
    let (host, file) = ordinary(b"existing").await?;
    let volume = file.volume();
    let (_issuer, read) = grant(volume, VolumeOperation::Read)?;
    let selected = host
        .select_private_generation(
            volume,
            &read,
            "absent",
            None,
            crate::conversation::ContentAccess::default(),
        )
        .await?;
    let workspace = bounded_io::content_workspace(&host.provider, volume, None)?;
    host.apply(
        &workspace,
        None,
        &[crate::filesystem::WorkspaceMutation::PutFile {
            path: "/absent".into(),
            bytes: bytes::Bytes::from_static(b"new").into(),
        }],
        &IdempotencyKey::new("publish-after-absence")?,
    )
    .await?;
    let range = acyclic_fs::ByteRange {
        offset: 0,
        length: 1,
    };
    let error = host
        .read_private_range(
            volume,
            &read,
            "absent",
            Some(&selected),
            range,
            crate::conversation::ContentAccess::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), crate::wire::ErrorCode::NotFound);
    let (bytes, current, _) = host
        .read_private_range(
            volume,
            &read,
            "absent",
            None,
            range,
            crate::conversation::ContentAccess::default(),
        )
        .await?;
    assert_eq!(bytes.as_ref(), b"n");
    assert_ne!(host.generation_ref_id(current, None)?, selected);
    let denied = Capabilities::default();
    let error = host
        .select_private_generation(
            volume,
            &read,
            "absent",
            Some(&selected),
            crate::conversation::ContentAccess {
                ceiling: Some(&denied),
                io: None,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), crate::wire::ErrorCode::Unauthorized);
    assert!(
        host.stat_private_path(
            volume,
            &read,
            "absent",
            Some(&selected),
            crate::conversation::ContentAccess::default(),
        )
        .await?
        .is_none()
    );
    let current = host
        .stat_private_path(
            volume,
            &read,
            "absent",
            None,
            crate::conversation::ContentAccess::default(),
        )
        .await?
        .expect("published path");
    assert_eq!(current.logical_bytes, Some(3));
    for path in ["absent", "never-published"] {
        let error = host
            .stat_private_path(
                volume,
                &read,
                path,
                Some(&selected),
                crate::conversation::ContentAccess {
                    ceiling: Some(&denied),
                    io: None,
                },
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), crate::wire::ErrorCode::Unauthorized);
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn private_metadata_does_not_materialize_file_bodies() -> Result<()> {
    let (host, file) = published(&vec![b'x'; 2 * 1024 * 1024]).await?;
    let (_issuer, read) = grant(file.volume(), VolumeOperation::Read)?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget {
            source_bytes_read: 64 * 1024,
            bytes_hashed: 64 * 1024,
            bytes_copied: 64 * 1024,
            ..acyclic_fs::WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = PreparationIo::from_native_work(&work)?;
    let stat = host
        .stat_private_path(
            file.volume(),
            &read,
            file.path(),
            None,
            crate::conversation::ContentAccess {
                ceiling: None,
                io: Some(&io),
            },
        )
        .await?
        .expect("published file");
    assert_eq!(stat.kind, acyclic_fs::kernel::FileKind::Regular);
    assert_eq!(stat.logical_bytes, Some(file.descriptor().byte_length()));
    assert!(work.receipt().source_bytes_read < 64 * 1024);
    assert!(work.receipt().object_bytes_read < file.descriptor().byte_length());
    assert!(work.receipt().bytes_hashed < 64 * 1024);
    let denied_work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget {
            bytes_copied: 0,
            ..acyclic_fs::WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let denied_io = PreparationIo::from_native_work(&denied_work)?;
    let error = host
        .read_private_path_indexed(
            file.volume(),
            &read,
            file.path(),
            None,
            Some(1),
            crate::conversation::ContentAccess {
                ceiling: None,
                io: Some(&denied_io),
            },
        )
        .await
        .unwrap_err();
    // The selected file also exceeds the input read bound, but its original
    // accounting failure must not become a model/control input rejection.
    assert!(!matches!(error, Error::ToolRejected { .. }));
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn a_private_range_by_path_reads_only_its_bytes() -> Result<()> {
    let body: Vec<u8> = (0..2 * 1024 * 1024).map(|index| index as u8).collect();
    let (host, file) = ordinary(&body).await?;
    let volume = file.volume();
    let (_issuer, read) = grant(volume, VolumeOperation::Read)?;
    let workspace = bounded_io::content_workspace(&host.provider, volume, None)?;
    host.apply(
        &workspace,
        None,
        &[crate::filesystem::WorkspaceMutation::CreateDirectory {
            path: "/directory".into(),
        }],
        &IdempotencyKey::new("directory")?,
    )
    .await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = || acyclic_fs::WorkBudget {
        source_bytes_read: 64 * 1024,
        bytes_copied: 64 * 1024,
        ..acyclic_fs::WorkBudget::UNBOUNDED
    };
    let work = acyclic_fs::NativeMaterializationWork::new(budget(), &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = PreparationIo::from_native_work(&work)?;
    let offset = body.len() - 10;
    let (bytes, selected_id, length) = host
        .read_private_range(
            volume,
            &read,
            "ordinary",
            None,
            acyclic_fs::ByteRange {
                offset: offset as u64,
                length: 64,
            },
            crate::conversation::ContentAccess {
                ceiling: None,
                io: Some(&io),
            },
        )
        .await?;
    assert_eq!(
        (bytes.as_ref(), length),
        (&body[offset..], body.len() as u64)
    );
    assert!(work.receipt().source_bytes_read < 64 * 1024);
    let selected = host.generation_ref_id(selected_id, Some(&io))?;
    assert_eq!(selected.as_resource().provider(), volume.provider());
    host.apply(
        &workspace,
        None,
        &[crate::filesystem::WorkspaceMutation::PutFile {
            path: "/ordinary".into(),
            bytes: bytes::Bytes::from_static(b"new head").into(),
        }],
        &IdempotencyKey::new("replace-range-source")?,
    )
    .await?;
    let pinned_work = acyclic_fs::NativeMaterializationWork::new(budget(), &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let pinned_io = PreparationIo::from_native_work(&pinned_work)?;
    let (pinned, generation, pinned_length) = host
        .read_private_range(
            volume,
            &read,
            "ordinary",
            Some(&selected),
            acyclic_fs::ByteRange {
                offset: offset as u64,
                length: 10,
            },
            crate::conversation::ContentAccess {
                ceiling: None,
                io: Some(&pinned_io),
            },
        )
        .await?;
    assert_eq!(generation, selected_id);
    assert_eq!(pinned_length, body.len() as u64);
    assert_eq!(pinned.as_ref(), &body[offset..]);
    assert!(pinned_work.receipt().source_bytes_read < 64 * 1024);
    for (path, maximum, code) in [
        ("missing", u64::MAX, crate::wire::ErrorCode::NotFound),
        ("directory", u64::MAX, crate::wire::ErrorCode::Invalid),
        ("ordinary", 1, crate::wire::ErrorCode::Invalid),
    ] {
        let work = acyclic_fs::NativeMaterializationWork::new(budget(), &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let io = PreparationIo::from_native_work(&work)?;
        let error = host
            .read_private_path(
                volume,
                &read,
                path,
                None,
                maximum,
                crate::conversation::ContentAccess {
                    ceiling: None,
                    io: Some(&io),
                },
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), code);
        assert!(error.is_tool_failure(), "{error:?}");
        assert!(!error.is_native_failure(), "{error:?}");
    }
    // Negative control: reading the same file by path under the same
    // allowance loads all of it, and fails.
    let work = acyclic_fs::NativeMaterializationWork::new(budget(), &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = PreparationIo::from_native_work(&work)?;
    let error = host
        .read_private_path(
            volume,
            &read,
            "ordinary",
            Some(&selected),
            u64::MAX,
            crate::conversation::ContentAccess {
                ceiling: None,
                io: Some(&io),
            },
        )
        .await
        .unwrap_err();
    assert!(
        error.is_native_failure(),
        "actual byte-credit loss must remain terminal: {error:?}"
    );
    assert!(!error.is_tool_failure());
    for offset in [body.len() as u64, body.len() as u64 + 1] {
        let (bytes, _, _) = host
            .read_private_range(
                volume,
                &read,
                "ordinary",
                Some(&selected),
                acyclic_fs::ByteRange { offset, length: 64 },
                crate::conversation::ContentAccess::default(),
            )
            .await?;
        assert!(bytes.is_empty());
    }
    Ok(())
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    // A discovered ordinary file supports every interval without upload
    // metadata. Its full descriptor remains authenticated, including empty ranges.
    #[test]
    fn ordinary_ranges_preserve_identity(
        body in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..8192),
        a in proptest::prelude::any::<u16>(), b in proptest::prelude::any::<u16>(),
    ) {
        futures::executor::block_on(async {
            let (host, file) = ordinary(&body).await.unwrap();
            let a = usize::from(a) % (body.len() + 1);
            let b = usize::from(b) % (body.len() + 1);
            let (start, end) = (a.min(b), a.max(b));
            let range = acyclic_fs::ByteRange { offset: start as u64, length: (end - start) as u64 };
            assert_eq!(host.read_pinned_range(&file, range, None).await.unwrap().as_ref(), &body[start..end]);
            let mut sha = *file.descriptor().sha256(); sha[0] ^= 1;
            let forged = FileRef::new(file.volume().clone(), file.path().into(), file.version().into(),
                FileDescriptor::new(sha, body.len() as u64, "application/octet-stream".into()).unwrap(),
                file.display_name().into(), &mut None).unwrap();
            assert!(host.read_pinned_range(&forged, range, None).await.is_err());
        });
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn ordinary_range_authentication_uses_bounded_live_memory() -> Result<()> {
    let (host, file) = ordinary(&vec![b'x'; 4 * 1024 * 1024]).await?;
    let stop = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget {
            peak_allocation_bytes: 1024 * 1024,
            ..acyclic_fs::WorkBudget::UNBOUNDED
        },
        &stop,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = PreparationIo::from_native_work(&work)?;
    let range = acyclic_fs::ByteRange {
        offset: 65530,
        length: 16,
    };
    assert_eq!(
        host.read_pinned_range(&file, range, Some(&io))
            .await?
            .as_ref(),
        b"xxxxxxxxxxxxxxxx"
    );
    assert!(work.receipt().peak_allocation_bytes < file.descriptor().byte_length());
    Ok(())
}
