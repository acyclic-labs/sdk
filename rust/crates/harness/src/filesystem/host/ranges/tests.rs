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
        &Capabilities::new([volume.capability(VolumeOperation::Write, &mut None)?]),
        Some(AgentId::from_bytes([1; 16])),
        None,
        &mut None,
    )?;
    let grant = ContentGrant::verify(
        &issuer.verifier(&mut None)?,
        &scope,
        &volume,
        VolumeOperation::Write,
        u64::MAX,
        &mut None,
    )?;
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
    let input = crate::tool::files::ReadFileInput {
        file: file.clone(),
        range: Some(crate::tool::text::TextRange {
            start: range.offset,
            end: range.offset + range.length,
        }),
    };
    assert_eq!(
        serde_json::from_slice::<crate::tool::files::ReadFileInput>(
            &serde_json::to_vec(&input).expect("file input encodes")
        )
        .expect("encoded file input decodes"),
        input
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
