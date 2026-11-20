use super::*;
use crate::core::{AggregateKind, AuthorityRoot};
use crate::{AgentId, conversation::VolumeOwner, core::Authority};
use proptest::prelude::*;

type Host = FilesystemHost<acyclic_fs::MemoryAuthorityBackend, acyclic_fs::MemoryObjectBackend>;
mod legacy;
pub(super) mod retained;

async fn fixture(bytes: &[u8]) -> Result<(Arc<Host>, FileRef)> {
    fixture_with_filesystem(bytes, Fs::memory()).await
}

async fn fixture_with_filesystem(
    bytes: &[u8],
    filesystem: Fs<acyclic_fs::MemoryAuthorityBackend, acyclic_fs::MemoryObjectBackend>,
) -> Result<(Arc<Host>, FileRef)> {
    let provider = ProviderRef::new("pinned".into(), "filesystem".into(), "2".into(), &mut None)?;
    let host = FilesystemHost::new(filesystem, &provider, None)?;
    let agent = AgentId::from_bytes([1; 16]);
    let volume = VolumeRef::new(
        provider,
        "pinned".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    host.create_volume(&volume).await?;
    let issuer = AuthorityRoot::new(
        "pinned",
        [2; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "pinned".into(),
        },
    );
    let scope = issuer.bootstrap_scope(
        "owner",
        &crate::Capabilities::new([volume.capability(VolumeOperation::Write, &mut None)?]),
        Some(agent),
        &mut None,
    )?;
    let grant = ContentGrant::verify(
        &issuer.verifier(&mut None)?,
        &scope,
        &volume,
        VolumeOperation::Write,
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
            Some(4096),
            &IdempotencyKey::new("payload")?,
            None,
            crate::conversation::ContentAccess::default(),
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
) -> Result<crate::conversation::Custody> {
    if old {
        // The legacy oracle predates read custody; its bytes carry no charge.
        legacy::read_pinned(host, file, maximum, io)
            .await
            .map(crate::conversation::Custody::Unmetered)
    } else {
        host.read_pinned_io(file, maximum, io)
            .await
            .map(Charged::into_custody)
    }
}

use crate::test_support::accounted::accounted;
use acyclic_fs_work::performance::WorkCounters;
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
    prop_assert_eq!(
        original.bytes_total.checked_sub(candidate.bytes_total),
        Some(saved)
    );
    Ok(())
}

fn equivalent_work(
    original: WorkCounters,
    mut candidate: WorkCounters,
) -> proptest::test_runner::TestCaseResult {
    // The shipping frame and earlier buffer retirement can reduce concurrent
    // residency. All other measured work must remain identical.
    prop_assert!(
        candidate.peak_allocation_bytes <= original.peak_allocation_bytes,
        "candidate peak {} exceeded original peak {}",
        candidate.peak_allocation_bytes,
        original.peak_allocation_bytes,
    );
    candidate.peak_allocation_bytes = original.peak_allocation_bytes;
    prop_assert_eq!(original, candidate);
    Ok(())
}

fn refused_work_is_a_prefix(
    original: WorkCounters,
    candidate: WorkCounters,
    complete: WorkCounters,
    allowance: u64,
) -> proptest::test_runner::TestCaseResult {
    // A smaller frame can admit another step before the same allocation
    // refusal. Both prefixes must stay within the allowance, and the later
    // refusal must not perform more work than the fully funded original.
    prop_assert!(original.peak_allocation_bytes <= allowance);
    prop_assert!(candidate.peak_allocation_bytes <= allowance);
    prop_assert!(candidate.checked_sub_cumulative(&original).is_ok());
    prop_assert!(complete.checked_sub_cumulative(&candidate).is_ok());
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Refusal {
    PeakAllocation { observed: u64, maximum: u64 },
    Other(String),
}

struct CompletedRead {
    result: std::result::Result<Vec<u8>, Refusal>,
    local: WorkCounters,
    native: WorkCounters,
    trace: Vec<(&'static str, u64)>,
}

fn refusal(error: &Error) -> Refusal {
    use acyclic_fs::{FsError, WorkspaceError};
    use acyclic_fs_work::performance::{WorkError, WorkObservationError};
    let filesystem_budget = |source: &FsError| match source {
        FsError::Work(cause) | FsError::WorkObservation(WorkObservationError::Work(cause)) => {
            Some(*cause)
        }
        _ => None,
    };
    let budget = match error {
        Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Work(cause),
            ..
        } => Some(*cause),
        Error::NativeWorkspace { source, .. } => match source.as_ref() {
            WorkspaceError::Work(cause) => Some(*cause),
            WorkspaceError::Filesystem(source) => filesystem_budget(source),
            _ => None,
        },
        Error::NativeFilesystem { source, .. } => filesystem_budget(source),
        _ => None,
    };
    match budget {
        Some(WorkError::BudgetExceeded {
            counter: "peak_allocation_bytes",
            observed,
            maximum,
        }) => Refusal::PeakAllocation { observed, maximum },
        _ => Refusal::Other(error.to_string()),
    }
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
            // Build independent stores with identical generated identities and index
            // layout before comparing the two readers' allocation receipts.
            let (host, mut file) =
                acyclic_native_runtime::testing::seeded_test(0x5049_4e4e, || fixture(&bytes))
                    .unwrap()
                    .unwrap();
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
        exact_unmetered_allocation(before.0.1, after.0.1, saved)?;
        // The caller-owned future may shrink; its original upper bound must not grow.
        // The separately admitted heap factory has exact allocation guards below.
        prop_assert!(after.0.2 <= before.0.2, "enclosing frame exceeds the original upper bound");
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
        for native in [false, true] {
            let baseline = if native {
                acyclic_fs::NativeMaterializationWork::accounting_state_bytes()
            } else {
                0
            };
            let mut complete: Option<CompletedRead> = None;
            for allowance in [64 * 1024 * 1024, budget] {
                let [before, after] = differential::pair::<_, _, (&'static str, u64)>(initialize, |(host, file), old|
                    accounted(native, allowance, cancelled, |io| {
                        let result = runtime.block_on(read(&host, &file, maximum, Some(io), old))
                            .map(|bytes| bytes.as_ref().to_vec()).map_err(|error| refusal(&error));
                        // A longer admitted prefix can warm more backend pages.
                        // Retire the owning fixture before sampling escaped custody.
                        drop(host);
                        result
                    }));
                if let (Ok(original), Ok(candidate)) = (&before.0, &after.0) {
                    if let Err(Refusal::PeakAllocation { observed, maximum }) = &original.0 {
                        // Smaller custody can admit more of the same read with
                        // the same original credit. The funded read authenticates
                        // both the refused prefix and any newly admitted result.
                        prop_assert!(*maximum <= allowance && *observed > *maximum);
                        let Some(funded) = &complete else {
                            return Err(proptest::test_runner::TestCaseError::fail(
                                "the fully funded read refused its allocation",
                            ));
                        };
                        prop_assert!(
                            after.1.starts_with(&before.1) && funded.trace.starts_with(&after.1),
                            "allocation refusal diverged from the original read trace: native {}",
                            native
                        );
                        refused_work_is_a_prefix(original.1, candidate.1, funded.local, allowance)?;
                        refused_work_is_a_prefix(original.3, candidate.3, funded.native, allowance)?;
                        if let Err(Refusal::PeakAllocation { observed, maximum }) = &candidate.0 {
                            prop_assert!(*maximum <= allowance && *observed > *maximum);
                        } else {
                            prop_assert_eq!(&candidate.0, &funded.result);
                            prop_assert_eq!(&after.1, &funded.trace);
                            equivalent_work(funded.local, candidate.1)?;
                            equivalent_work(funded.native, candidate.3)?;
                        }
                    } else {
                        prop_assert_eq!(&before.1, &after.1, "native {}", native);
                        prop_assert_eq!(&original.0, &candidate.0, "native {}", native);
                        equivalent_work(original.1, candidate.1)?;
                        equivalent_work(original.3, candidate.3)?;
                        complete.get_or_insert_with(|| CompletedRead {
                            result: original.0.clone(),
                            local: original.1,
                            native: original.3,
                            trace: before.1.clone(),
                        });
                    }
                    // The original standalone factory kept the buffer's charge on
                    // the caller after returning it. The shipping output releases
                    // that charge when the returned bytes are dropped.
                    prop_assert_eq!(candidate.2, 0, "native {}", native);
                    if native {
                        prop_assert_eq!(original.2, candidate.2);
                    }
                    prop_assert_eq!((original.4, original.5), (baseline, baseline), "native {}", native);
                    prop_assert_eq!((candidate.4, candidate.5), (baseline, baseline), "native {}", native);
                } else {
                    prop_assert_eq!(&before.1, &after.1, "native {}", native);
                    prop_assert_eq!(before.0, after.0, "native {}", native);
                }
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
