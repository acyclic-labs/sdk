use super::*;
use crate::core::{AggregateKind, AuthorityIssuer};
use proptest::prelude::*;

type Host = FilesystemHost<acyclic_fs::MemoryAuthorityBackend, acyclic_fs::MemoryObjectBackend>;
mod legacy;

async fn fixture(bytes: &[u8]) -> Result<(Arc<Host>, FileRef)> {
    let provider = ProviderRef::new("pinned".into(), "filesystem".into(), "2".into(), &mut None)?;
    let host = FilesystemHost::new(Fs::memory(), &provider, None)?;
    let agent = AgentId::from_bytes([1; 16]);
    let volume = VolumeRef::new(
        provider,
        "pinned".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    host.create_volume(&volume).await?;
    let issuer = AuthorityIssuer::new(
        "pinned",
        [2; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "pinned".into(),
        },
    );
    let scope = issuer.issue(
        "owner",
        &crate::Capabilities::new([volume.capability(VolumeOperation::Write, &mut None)?]),
        Some(agent),
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
            "payload",
            bytes,
            "text/plain",
            "payload",
            4096,
            &IdempotencyKey::new("payload")?,
            None,
            None,
        )
        .await?;
    Ok((host, file))
}

async fn read(
    host: &Host,
    file: &FileRef,
    maximum: u64,
    io: Option<&PreparationIo>,
    old: bool,
) -> Result<acyclic_stream::OwnedBytes> {
    if old {
        legacy::read_pinned(host, file, maximum, io).await
    } else {
        host.read_pinned_io(file, maximum, io).await
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // Real pinned reads preserve complete bytes/errors, while a cold read no
    // longer reserves space for the native accounting branch's future.
    #[test]
    fn pinned_reads_use_only_their_unmetered_frame(
        bytes in prop::collection::vec(any::<u8>(), 0..4097), maximum in 0u64..8193,
        tamper in any::<bool>(), budget in 1u64..131073, cancelled in any::<bool>(),
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let (host, mut file) = fixture(&bytes).await.unwrap();
            if tamper {
                let mut digest = *file.descriptor().sha256();
                digest[0] ^= 1;
                file = FileRef::new(file.volume().clone(), file.path().into(), file.version().into(),
                    FileDescriptor::new(digest, file.descriptor().byte_length(), "text/plain".into()).unwrap(),
                    file.display_name().into(), &mut None).unwrap();
            }
            let mut outcomes = Vec::new();
            for old in [true, false] {
                let mut outcome = None;
                let allocation = acyclic_test_support::measure(|| {
                    outcome = Some(futures::executor::block_on(read(&host, &file, maximum, None, old))
                        .map_err(|error| error.to_string()));
                });
                outcomes.push((outcome.unwrap(), allocation));
            }
            let (before, after) = (&outcomes[0], &outcomes[1]);
            assert_eq!(before.0, after.0);
            assert!(after.1.count_total <= before.1.count_total);
            let compact = |bytes| bytes <= after.1.bytes_total;
            assert!(compact(after.1.bytes_total));
            // The unchanged original factory is the negative control for the
            // same storage assertion, on the actual read path.
            assert!(!compact(before.1.bytes_total));

            assert_eq!(std::mem::size_of_val(&host.read_pinned_io(&file, maximum, None)),
                std::mem::size_of_val(&legacy::read_pinned(&host, &file, maximum, None)),
                "accounted callers must retain the original inline future size");
            for allowance in [budget, 64 * 1024 * 1024] {
                let mut outcomes = Vec::new();
                for old in [true, false] {
                    let cancellation = acyclic_fs::CancellationToken::new();
                    let io = PreparationIo::from_budget(
                        acyclic_fs::performance::work_budget(1_000_000, allowance), &cancellation).unwrap();
                    if cancelled { cancellation.cancel(); }
                    let result = read(&host, &file, maximum, Some(&io), old).await
                        .map_err(|error| error.to_string());
                    outcomes.push((result, io.receipt(), io.owned_bytes().unwrap()));
                }
                assert_eq!(outcomes.first(), outcomes.last());
            }
        });
    }
}
