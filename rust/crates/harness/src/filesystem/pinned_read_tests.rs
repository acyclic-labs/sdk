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

// (Requested allocation bytes saved, enclosing future bytes). Native mount
// support retains an execution pointer in the native custody layouts.
#[cfg(feature = "native-execution")]
const EXPECTED_BYTES: (u64, usize) = (1824, 392);
#[cfg(not(feature = "native-execution"))]
const EXPECTED_BYTES: (u64, usize) = (1768, 360);

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
        let [before, after] = differential::pair::<_, _, (&'static str, u64)>(initialize, |(host, file), old| {
            let mut result = None;
            let allocation = acyclic_test_support::measure(|| {
                result = Some(runtime.block_on(read(&host, &file, maximum, None, old))
                    .map(|bytes| bytes.as_ref().to_vec()).map_err(|e| e.to_string()));
            });
            let frame = if old { std::mem::size_of_val(&legacy::read_pinned(&host, &file, maximum, None)) }
                else { std::mem::size_of_val(&host.read_pinned_io(&file, maximum, None)) };
            (result.unwrap(), allocation, frame)
        });
        prop_assert_eq!(&before.0.0, &after.0.0);
        prop_assert_eq!(&before.1, &after.1);
        eprintln!("pinned frame before={} after={} allocations before={:?} after={:?}",
            before.0.2, after.0.2, before.0.1, after.0.1);
        prop_assert_eq!(before.0.1.count_total, after.0.1.count_total);
        prop_assert_eq!(before.0.1.bytes_total - after.0.1.bytes_total, EXPECTED_BYTES.0);
        prop_assert_eq!((before.0.2, after.0.2), (EXPECTED_BYTES.1, EXPECTED_BYTES.1));
        // The old factory must fail the identical exact storage bound.
        prop_assert_ne!(before.0.1.bytes_total - before.0.1.bytes_total, EXPECTED_BYTES.0);
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
