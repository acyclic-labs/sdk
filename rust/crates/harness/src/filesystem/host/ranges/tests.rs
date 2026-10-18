use super::*;
use crate::{
    Capabilities,
    core::{AggregateKind, AuthorityIssuer},
};
use acyclic_fs::{MemoryAuthorityBackend, MemoryObjectBackend};

type Host = FilesystemHost<MemoryAuthorityBackend, MemoryObjectBackend>;

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
    let grant = grant(&volume, VolumeOperation::Write)?;
    let file = host
        .put_content(
            &volume,
            &grant,
            "file.txt",
            body,
            "text/plain",
            "file.txt",
            u64::MAX,
            &IdempotencyKey::new("publish")?,
            None,
            None,
        )
        .await?;
    Ok((host, file))
}

fn grant(volume: &VolumeRef, operation: VolumeOperation) -> Result<ContentGrant> {
    let issuer = AuthorityIssuer::new(
        "ranges",
        [3; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "owner".into(),
        },
    );
    let scope = issuer.issue(
        "owner",
        &Capabilities::new([volume.capability(operation, &mut None)?]),
        Some(AgentId::from_bytes([1; 16])),
        None,
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
            u64::MAX,
            &mut None,
        )?
    };
    Ok(grant)
}

async fn ordinary(body: &[u8]) -> Result<(Arc<Host>, FileRef)> {
    let (host, uploaded) = published(b"").await?;
    let volume = uploaded.volume();
    let workspace = fork_preparer_io::content_workspace(&host.provider, volume, None)?;
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
    let (reference, actual) = host
        .read_private_path(
            volume,
            &grant(volume, VolumeOperation::Read)?,
            "ordinary",
            Some(&generation),
            body.len() as u64,
            None,
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
    let workspace = fork_preparer_io::content_workspace(&host.provider, file.volume(), None)?;
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

#[cfg(not(target_arch = "wasm32"))]
async fn bounded_ordinary_path(body: &[u8]) -> Result<()> {
    let (host, file) = ordinary(body).await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget {
            peak_allocation_bytes: 1024 * 1024,
            ..acyclic_fs::WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = PreparationIo::from_native_work(&work)?;
    let (current, bytes) = host
        .current_path(
            file.volume(),
            &grant(file.volume(), VolumeOperation::Write)?,
            file.path(),
            body.len() as u64,
            None,
            Some(&io),
        )
        .await?;
    assert_eq!(bytes.as_ref(), body);
    assert_eq!(current.descriptor(), file.descriptor());
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    // Optional metadata is absent in either error representation; an ordinary
    // file's authenticated contents must not depend on the caller's ledger.
    #[test]
    fn bounded_ordinary_paths_resolve_without_upload_metadata(
        body in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..256),
    ) {
        futures::executor::block_on(bounded_ordinary_path(&body)).unwrap();
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn negative_control_detects_native_absence_rejection() {
    crate::filesystem::fork_preparer::LEGACY_ABSENCE_ONLY.with(|flag| flag.set(true));
    let detected = std::panic::catch_unwind(|| {
        futures::executor::block_on(bounded_ordinary_path(b"ordinary")).unwrap();
    });
    crate::filesystem::fork_preparer::LEGACY_ABSENCE_ONLY.with(|flag| flag.set(false));
    let panic =
        detected.expect_err("restoring legacy-only absence must fail the bounded read oracle");
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        message.contains("NativeWorkspace") && message.contains("NotFound"),
        "{message}"
    );
}

async fn captured_payload(body: &[u8]) -> Result<()> {
    let (host, original) = published(b"uploaded").await?;
    let reference = fork_preparer_io::content_workspace(&host.provider, original.volume(), None)?;
    let workspace = host.open(&reference, None).await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = acyclic_fs::WorkBudget::UNBOUNDED;
    // Native capture authors payload mutations while excluding .system. Use
    // that same SDK transaction boundary, preserving the old metadata verbatim.
    let mut transaction = workspace
        .begin_transaction(FilesystemKey::from_bytes([7; 16]), budget, &cancellation)
        .await
        .map_err(|failure| map_error(failure.error))?
        .value;
    transaction
        .write(
            "/file.txt",
            bytes::Bytes::copy_from_slice(body),
            budget,
            &cancellation,
        )
        .await
        .map_err(|failure| map_error(failure.error))?;
    transaction
        .commit(
            None,
            acyclic_fs::PublicationPermit::Unrestricted,
            budget,
            &cancellation,
            |_| {
                std::future::ready(Ok::<_, acyclic_fs::OperationFailure<Error>>(
                    acyclic_fs::OperationReceipt {
                        value: (),
                        work: WorkCounters::UNCHARGED,
                    },
                ))
            },
        )
        .await
        .map_err(|failure| failure.error)?;
    let (current, bytes) = host
        .read_private_path(
            original.volume(),
            &grant(original.volume(), VolumeOperation::Read)?,
            "file.txt",
            None,
            body.len() as u64,
            None,
        )
        .await?;
    assert_eq!(bytes.as_ref(), body);
    assert_eq!(current.descriptor().byte_length(), body.len() as u64);
    assert_ne!(current.descriptor(), original.descriptor());
    let range = acyclic_fs::ByteRange {
        offset: 0,
        length: body.len() as u64,
    };
    assert_eq!(
        host.read_pinned_range(&current, range, None)
            .await?
            .as_ref(),
        body
    );
    assert_eq!(
        host.read_pinned_range(
            &original,
            acyclic_fs::ByteRange {
                offset: 0,
                length: 8
            },
            None
        )
        .await?
        .as_ref(),
        b"uploaded"
    );
    let forged = FileRef::new(
        original.volume().clone(),
        "file.txt".into(),
        current.version().into(),
        original.descriptor().clone(),
        original.display_name().into(),
        &mut None,
    )?;
    assert!(
        host.read_pinned_range(
            &forged,
            acyclic_fs::ByteRange {
                offset: 0,
                length: 0
            },
            None
        )
        .await
        .is_err()
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    #[test]
    fn captured_payloads_never_select_a_different_publications_descriptor(
        mut body in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..256),
    ) {
        body.push(0);
        futures::executor::block_on(captured_payload(&body)).unwrap();
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn negative_control_detects_stale_metadata_authority() {
    super::super::metadata::TRUST_STALE_METADATA.with(|flag| flag.set(true));
    let detected = std::panic::catch_unwind(|| {
        futures::executor::block_on(captured_payload(b"native edit")).unwrap()
    });
    super::super::metadata::TRUST_STALE_METADATA.with(|flag| flag.set(false));
    let panic = detected.expect_err("stale descriptor selection must fail the same payload oracle");
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(message.contains("descriptor"), "{message}");
}
