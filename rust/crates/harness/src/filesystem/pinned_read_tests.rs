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

use crate::test_support::accounted::accounted;
use acyclic_test_support::differential;

fn exact_unmetered_allocation(
    original: acyclic_test_support::AllocationInfo,
    candidate: acyclic_test_support::AllocationInfo,
    saved: u64,
) -> proptest::test_runner::TestCaseResult {
    prop_assert_eq!(
        candidate.count_total,
        original.count_total,
        "pinned read allocation count"
    );
    prop_assert_eq!(original.bytes_total - candidate.bytes_total, saved);
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn pinned_reads_use_only_their_unmetered_frame(
        bytes in prop::collection::vec(any::<u8>(), 0..4097), maximum in 0u64..8193,
        tamper in any::<bool>(), budget in 1u64..131073, cancelled in any::<bool>(),
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let initialize = || {
            let (host, mut file) = runtime.block_on(fixture(&bytes)).unwrap();
            if tamper {
                let mut digest = *file.descriptor().sha256();
                digest[0] ^= 1;
                file = FileRef::new(file.volume().clone(), file.path().into(), file.version().into(),
                    FileDescriptor::new(digest, file.descriptor().byte_length(), "text/plain".into()).unwrap(),
                    file.display_name().into(), &mut None).unwrap();
            }
            (host, file)
        };
        let (layout_host, layout_file) = initialize();
        let observe = |(host, file): (Arc<Host>, FileRef), old, extra_box| {
            let mut result = None;
            let allocation = acyclic_test_support::measure(|| {
                let future = read(&host, &file, maximum, None, old);
                let read = if extra_box { runtime.block_on(Box::pin(future)) }
                    else { runtime.block_on(future) };
                result = Some(read.map(|bytes| bytes.as_ref().to_vec()).map_err(|e| e.to_string()));
            });
            let frame = if old { std::mem::size_of_val(&legacy::read_pinned(&host, &file, maximum, None)) }
                else { std::mem::size_of_val(&host.read_pinned_io(&file, maximum, None)) };
            (result.unwrap(), allocation, frame)
        };
        let [before, after] = differential::pair::<_, _, (&'static str, u64)>(initialize,
            |fixture, old| observe(fixture, old, false));
        let saved = legacy::saved_frame_bytes(&layout_host, &layout_file, maximum);
        prop_assert!(saved > 0);
        prop_assert_eq!(&before.0.0, &after.0.0);
        prop_assert_eq!(&before.1, &after.1);
        eprintln!("pinned frame before={} after={} allocations before={:?} after={:?}",
            before.0.2, after.0.2, before.0.1, after.0.1);
        exact_unmetered_allocation(before.0.1, after.0.1, saved)?;
        prop_assert_eq!(after.0.2, before.0.2, "enclosing frame differs from the original concrete type");
        // Restore the old factory on a third, independently initialized store.
        let broken = differential::observe::<_, _, (&'static str, u64)>(initialize,
            |fixture| observe(fixture, true, false));
        prop_assert_eq!(&before.0.0, &broken.0.0);
        prop_assert_eq!(&before.1, &broken.1);
        prop_assert!(exact_unmetered_allocation(before.0.1, broken.0.1, saved).is_err(),
            "restored original factory passed the same allocation guard");
        let boxed = differential::observe::<_, _, (&'static str, u64)>(initialize,
            |fixture| observe(fixture, false, true));
        prop_assert_eq!(&before.0.0, &boxed.0.0);
        prop_assert_eq!(&before.1, &boxed.1);
        let error = exact_unmetered_allocation(before.0.1, boxed.0.1, saved).unwrap_err();
        prop_assert!(error.to_string().contains("pinned read allocation count"),
            "extra factory box bypassed the exact count guard");
        let padded = differential::observe::<_, _, (&'static str, u64)>(initialize,
            |fixture| crate::conversation::BoundedContentFuture::<acyclic_stream::OwnedBytes>::with_padded_frame(true,
                || observe(fixture, false, false)));
        prop_assert_eq!(&before.0.0, &padded.0.0);
        prop_assert_eq!(&before.1, &padded.1);
        prop_assert_eq!(after.0.1.count_total, padded.0.1.count_total,
            "inline padding unexpectedly added an allocation");
        prop_assert!(padded.0.1.bytes_total > after.0.1.bytes_total);
        prop_assert!(exact_unmetered_allocation(before.0.1, padded.0.1, saved).is_err(),
            "retained inline factory padding passed the exact byte guard");
        for allowance in [budget, 64 * 1024 * 1024] {
            for native in [false, true] {
                let [before, after] = differential::pair::<_, _, (&'static str, u64)>(initialize, |(host, file), old|
                    accounted(native, allowance, cancelled, |io|
                        runtime.block_on(read(&host, &file, maximum, Some(io), old))
                            .map(|bytes| bytes.as_ref().to_vec()).map_err(|e| e.to_string())));
                prop_assert_eq!(before, after, "native {}", native);
            }
        }
    }
}

#[test]
fn pinned_read_trace_and_native_custody_reject_broken_paths() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let initialize = || runtime.block_on(fixture(b"independent payload")).unwrap();
    let [before, broken] =
        differential::pair::<_, _, (&'static str, u64)>(initialize, |(host, file), old| {
            let bytes = runtime
                .block_on(read(&host, &file, 4096, None, old))
                .unwrap();
            if !old {
                file.descriptor().verify(&bytes, &mut None).unwrap();
            }
            bytes.as_ref().to_vec()
        });
    assert_eq!(before.0, broken.0);
    assert_ne!(
        before.1, broken.1,
        "extra real digest verification was not detected"
    );
    let [before, broken] =
        differential::pair::<_, _, (&'static str, u64)>(initialize, |(host, file), old| {
            accounted(true, 64 * 1024 * 1024, false, |io| {
                let bytes = runtime
                    .block_on(read(&host, &file, 4096, Some(io), old))
                    .unwrap();
                if !old {
                    io.retain_owned(1).unwrap();
                }
                bytes.as_ref().to_vec()
            })
        });
    assert_eq!(before.0.as_ref().unwrap().0, broken.0.as_ref().unwrap().0);
    assert_ne!(before, broken, "extra native custody was not detected");
}
