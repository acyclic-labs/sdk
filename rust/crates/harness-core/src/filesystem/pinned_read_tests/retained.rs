use super::*;
use acyclic_test_support::{AllocationInfo, differential};
use std::cell::Cell;

thread_local! {
    static REOPEN: Cell<bool> = const { Cell::new(false) };
    static ITEMS: Cell<Option<(bool, [u64; 2])>> = const { Cell::new(None) };
}

pub(in crate::filesystem) fn sample_items(finished: bool, io: Option<&PreparationIo>) {
    let Some(io) = io else {
        ITEMS.set(None);
        return;
    };
    let native = io.original_native_work().is_some();
    let (_, mut items) = ITEMS.get().unwrap_or((native, [0; 2]));
    items[usize::from(finished)] = io.receipt().items_examined;
    ITEMS.set(Some((native, items)));
}

pub(in crate::filesystem) fn reopen() -> bool {
    REOPEN.get()
}

fn with_reopen<T>(old: bool, run: impl FnOnce() -> T) -> T {
    struct Reset(bool);
    impl Drop for Reset {
        fn drop(&mut self) {
            REOPEN.set(self.0);
        }
    }
    let _reset = Reset(REOPEN.replace(old));
    run()
}

fn grant(file: &FileRef) -> Result<ContentGrant> {
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
        &crate::Capabilities::new([
            file.volume()
                .capability(VolumeOperation::Write, &mut None)?,
            file.volume().capability(VolumeOperation::Read, &mut None)?,
        ]),
        Some(AgentId::from_bytes([1; 16])),
        None,
        &mut None,
    )?;
    ContentGrant::verify(
        &issuer.verifier(&mut None)?,
        &scope,
        file.volume(),
        VolumeOperation::Write,
        u64::MAX,
        &mut None,
    )
}

struct Clock;
impl acyclic_stream::UnixMillisClock for Clock {
    fn now_unix_millis(&self) -> u64 {
        7
    }
}

async fn initialize(prefix: usize, bytes: &[u8]) -> Result<(Arc<Host>, FileRef, ContentGrant)> {
    // Authority timestamps are part of the independently initialized read
    // history. A wall clock changes its encoded work between the two runs.
    let stream = Arc::new(acyclic_stream::MemoryStream::new_with_clock(
        acyclic_stream::MemoryLimits::default(),
        Arc::new(Clock),
    ));
    let (objects, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
    let filesystem = Fs::from_memory_providers(stream, Arc::new(objects), bucket);
    let (host, file) = fixture_with_filesystem(b"seed", filesystem).await?;
    let grant = grant(&file)?;
    let workspace = workspace_ref(
        host.provider.clone(),
        &file.volume().storage_name(&mut None)?,
    )?;
    let mut mutations = vec![WorkspaceMutation::CreateDirectory {
        path: "/history".into(),
    }];
    mutations.extend((0..prefix).map(|index| WorkspaceMutation::PutFile {
        path: format!("/history/{index:04}"),
        bytes: b"history".as_slice().into(),
    }));
    host.apply(
        &workspace,
        None,
        &mutations,
        &IdempotencyKey::new("history")?,
    )
    .await?;
    let target = host
        .put_content(
            file.volume(),
            &grant,
            "retained",
            bytes,
            "text/plain",
            "retained",
            4096,
            &IdempotencyKey::new("retained")?,
            None,
            crate::conversation::ContentAccess::default(),
        )
        .await?;
    host.apply(
        &workspace,
        None,
        &[WorkspaceMutation::PutFile {
            path: "/retained".into(),
            bytes: b"later head".as_slice().into(),
        }],
        &IdempotencyKey::new("later-head")?,
    )
    .await?;
    Ok((host, target, grant))
}

#[derive(Clone, Copy, Debug)]
enum Entry {
    Create,
    Retry,
    Observe,
}

async fn execute(
    host: &Host,
    source: &FileRef,
    grant: &ContentGrant,
    entry: Entry,
    bytes: &[u8],
    io: Option<&PreparationIo>,
) -> Result<FileRef> {
    match entry {
        Entry::Create => {
            host.put_content(
                source.volume(),
                grant,
                "next",
                bytes,
                "text/plain",
                "next",
                4096,
                &IdempotencyKey::new("next")?,
                None,
                crate::conversation::ContentAccess { ceiling: None, io },
            )
            .await
        }
        Entry::Retry => {
            host.put_content(
                source.volume(),
                grant,
                "retained",
                bytes,
                "text/plain",
                "retained",
                4096,
                &IdempotencyKey::new("retained")?,
                None,
                crate::conversation::ContentAccess { ceiling: None, io },
            )
            .await
        }
        Entry::Observe => host
            .content_for_operation(
                source.volume(),
                grant,
                &IdempotencyKey::new("retained")?,
                "retained",
                4096,
                crate::conversation::ContentAccess { ceiling: None, io },
            )
            .await?
            .ok_or_else(|| Error::NotFound("retained publication".into())),
    }
}

fn allocation_bound(before: AllocationInfo, after: AllocationInfo) -> bool {
    after.count_total < before.count_total && after.bytes_total < before.bytes_total
}

#[test]
fn publications_reuse_the_retained_generation_through_every_entry() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| Error::Storage(e.to_string()))?;
    let mut observations = Vec::new();
    for entry in [Entry::Create, Entry::Retry, Entry::Observe] {
        for mode in [None, Some(false), Some(true)] {
            for prefix in [16, 32] {
                let [before, after, repeated] = [true, false, false].map(|old| {
                    differential::observe::<_, _, (&'static str, u64)>(
                        || {
                            runtime
                                .block_on(initialize(prefix, b"published bytes"))
                                .unwrap()
                        },
                        |(host, source, grant)| {
                            with_reopen(old, || {
                                ITEMS.set(None);
                                let mut result = None;
                                let mut receipt = WorkCounters::default();
                                let allocations = acyclic_test_support::measure(|| {
                                    result = Some(match mode {
                                        None => runtime
                                            .block_on(execute(
                                                &host,
                                                &source,
                                                &grant,
                                                entry,
                                                b"published bytes",
                                                None,
                                            ))
                                            .and_then(|file| file.copy(&mut None))
                                            .map_err(|e| e.to_string()),
                                        Some(native) => {
                                            accounted(native, 64 * 1024 * 1024, false, |io| {
                                                runtime
                                                    .block_on(execute(
                                                        &host,
                                                        &source,
                                                        &grant,
                                                        entry,
                                                        b"published bytes",
                                                        Some(io),
                                                    ))
                                                    .and_then(|file| file.copy(&mut None))
                                                    .map_err(|e| e.to_string())
                                            })
                                            .map(|result| {
                                                receipt = result.1;
                                                result.0
                                            })
                                            .unwrap()
                                        }
                                    });
                                });
                                let sampled = ITEMS.get();
                                differential::record(("publication verified", 0_u64));
                                let value = result.unwrap();
                                assert!(value.is_ok(), "{entry:?} {mode:?}: {value:?}");
                                let file = value.unwrap();
                                let workspace = workspace_ref(
                                    host.provider.clone(),
                                    &file.volume().storage_name(&mut None).unwrap(),
                                )
                                .unwrap();
                                let generation = host.file_generation(&file, None).unwrap();
                                let actual = runtime
                                    .block_on(host.read(
                                        &workspace,
                                        Some(&generation),
                                        &format!("/{}", file.path()),
                                        4096,
                                        None,
                                    ))
                                    .unwrap();
                                assert_eq!(actual.as_ref(), b"published bytes");
                                assert_eq!(
                                    runtime
                                        .block_on(host.read(
                                            &workspace,
                                            None,
                                            "/retained",
                                            4096,
                                            None
                                        ))
                                        .unwrap()
                                        .as_ref(),
                                    b"later head"
                                );
                                // Recovery may introduce an internal ledger even
                                // when its caller omitted one. Compare its read
                                // against the primitive on the ledger actually
                                // sampled, rather than assuming it did no work.
                                let primitive_items = match sampled {
                                    None => 0,
                                    Some((native, _)) => {
                                        let opened =
                                            runtime.block_on(host.open(&workspace, None)).unwrap();
                                        let pinned = runtime
                                            .block_on(host.generation(&opened, &generation, None))
                                            .unwrap();
                                        accounted(native, 64 * 1024 * 1024, false, |io| {
                                            let bytes = runtime
                                                .block_on(read_generation(
                                                    &pinned,
                                                    &format!("/{}", file.path()),
                                                    4096,
                                                    Some(io),
                                                ))
                                                .unwrap();
                                            assert_eq!(bytes.as_ref(), b"published bytes");
                                        })
                                        .unwrap()
                                        .1
                                        .items_examined
                                    }
                                };
                                let [start, end] = sampled.map_or([0; 2], |(_, items)| items);
                                (
                                    (file, actual.as_ref().to_vec()),
                                    allocations,
                                    receipt,
                                    end.checked_sub(start).unwrap(),
                                    primitive_items,
                                    u64::from(sampled.is_some_and(|(native, _)| !native)) * 2,
                                )
                            })
                        },
                    )
                });
                eprintln!(
                    "retained {entry:?} mode={mode:?} prefix={prefix} before={:?} after={:?} repeated={:?} read_items={}/{}/{} primitive={}/{}/{}",
                    differential::allocation_counts(before.0.1),
                    differential::allocation_counts(after.0.1),
                    differential::allocation_counts(repeated.0.1),
                    before.0.3,
                    after.0.3,
                    repeated.0.3,
                    before.0.4,
                    after.0.4,
                    repeated.0.4,
                );
                observations.push((entry, mode, prefix, [before, after, repeated]));
            }
        }
    }
    let mut prefix_saving = None;
    for (entry, mode, prefix, [before, after, repeated]) in observations {
        if prefix == 16 {
            prefix_saving = None;
        }
        // Pin all six allocation fields on independently initialized histories.
        // The wrapper's work is compared to the required primitive below;
        // mutation work outside this read is not part of that invariant.
        assert_eq!(
            after.0.1, repeated.0.1,
            "{entry:?} {mode:?} prefix={prefix}"
        );
        assert_eq!(after.0.0, repeated.0.0);
        assert_eq!(after.1, repeated.1);
        assert_eq!((after.0.3, after.0.4), (repeated.0.3, repeated.0.4));
        assert_eq!(before.0.0, after.0.0);
        let opens = |trace: &[(&str, u64)]| {
            trace
                .iter()
                .take_while(|event| event.0 != "publication verified")
                .filter(|event| event.0 == "open workspace")
                .count()
        };
        let expected_opens = match (entry, mode) {
            (Entry::Create, _) => 2,
            _ => 1,
        };
        assert_eq!(opens(&after.1), expected_opens, "{entry:?} {mode:?}");
        assert_eq!(opens(&before.1), expected_opens + 1, "{entry:?} {mode:?}");
        let observed = |trace: Vec<(&'static str, u64)>| {
            trace
                .into_iter()
                .filter(|event| {
                    !matches!(
                        event.0,
                        "open workspace"
                            | "retain"
                            | "custody-transfer"
                            | "custody-absorb"
                            | "custody-take"
                    )
                })
                .collect::<Vec<_>>()
        };
        let mut old_trace = observed(before.1);
        let new_trace = observed(after.1);
        // Reopening decodes its document and index again. Remove only
        // one exact duplicate pair, preserving all remaining events.
        if old_trace != new_trace {
            let duplicate = old_trace
                .windows(2)
                .enumerate()
                .position(|(index, events)| {
                    events[0].0 == "decode document"
                        && events[1].0 == "decode index"
                        && old_trace[..index].windows(2).any(|prior| prior == events)
                        && old_trace[..index]
                            .iter()
                            .chain(&old_trace[index + 2..])
                            .eq(new_trace.iter())
                })
                .unwrap_or_else(|| {
                    panic!("unexpected observer difference: {old_trace:?} vs {new_trace:?}")
                });
            old_trace.drain(duplicate..duplicate + 2);
        }
        assert_eq!(old_trace, new_trace);
        assert!(
            allocation_bound(before.0.1, after.0.1),
            "{entry:?} {mode:?}: {:?} -> {:?}",
            before.0.1,
            after.0.1
        );
        assert!(
            !allocation_bound(before.0.1, before.0.1),
            "restoring the real reopen must fail the same bound"
        );
        let saving = (
            before.0.1.count_total - after.0.1.count_total,
            before.0.1.bytes_total - after.0.1.bytes_total,
        );
        let before_allocations = differential::allocation_counts(before.0.1);
        let after_allocations = differential::allocation_counts(after.0.1);
        let allocation_saving = std::array::from_fn::<_, 6, _>(|index| {
            before_allocations[index] - after_allocations[index]
        });
        let work = |r: WorkCounters| {
            [
                r.authority_records_read,
                r.backend_read_operations,
                r.object_probes,
                r.page_reads,
                r.items_examined,
                r.bytes_copied,
                r.allocation_operations,
            ]
        };
        let work_saving = std::array::from_fn::<_, 7, _>(|index| {
            work(before.0.2)[index]
                .checked_sub(work(after.0.2)[index])
                .expect("reuse added work")
        });
        // Tree searches charge actual comparisons, which vary with
        // history. Independently meter the necessary generation read;
        // the retained wrapper must add only its path formatting.
        let overhead = after.0.5;
        assert_eq!(
            after.0.3,
            after.0.4 + overhead,
            "{entry:?} {mode:?}: redundant read work"
        );
        if mode.is_some() {
            assert!(
                before.0.3 > before.0.4 + overhead,
                "the real reopen must fail the same work bound"
            );
        }
        eprintln!(
            "retained {entry:?} mode={mode:?} prefix={prefix} saved={saving:?} allocations={allocation_saving:?} work={work_saving:?} retained_items={}",
            after.0.3 - after.0.4,
        );
        // Peaks are maxima, not additive work: changing which batch
        // phase reaches the peak can change the old/new peak delta.
        // Their exact per-size values are pinned above; N/2N must save
        // identical cumulative and retained allocations and bytes.
        assert!(after.0.1.count_max <= before.0.1.count_max);
        assert!(after.0.1.bytes_max <= before.0.1.bytes_max);
        let invariant = (
            [
                allocation_saving[0],
                allocation_saving[1],
                allocation_saving[3],
                allocation_saving[4],
            ],
            [
                work_saving[0],
                work_saving[1],
                work_saving[2],
                work_saving[3],
                work_saving[5],
                work_saving[6],
            ],
            after.0.3 - after.0.4,
        );
        if let Some(expected) = prefix_saving {
            assert_eq!(invariant, expected, "{entry:?} {mode:?}");
        }
        prefix_saving = Some(invariant);
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(12))]
    #[test]
    fn retained_reads_preserve_bytes_and_errors(
        bytes in prop::collection::vec(any::<u8>(), 0..4096),
        maximum in 0_u64..8193, tamper in any::<bool>(), cancelled in any::<bool>(),
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        for mode in [None, Some(false), Some(true)] {
            let [before, after] = [true, false].map(|old| {
                let (host, mut file) = runtime.block_on(fixture(&bytes)).unwrap();
                if tamper {
                    let mut digest = *file.descriptor().sha256(); digest[0] ^= 1;
                    file = FileRef::new(file.volume().clone(), file.path().into(), file.version().into(),
                        FileDescriptor::new(digest, bytes.len() as u64, "text/plain".into()).unwrap(), file.display_name().into(), &mut None).unwrap();
                }
                let workspace = workspace_ref(host.provider.clone(), &file.volume().storage_name(&mut None).unwrap()).unwrap();
                let opened = runtime.block_on(host.open(&workspace, None)).unwrap();
                let generation_ref = host.file_generation(&file, None).unwrap();
                let generation = runtime.block_on(host.generation(&opened, &generation_ref, None)).unwrap();
                with_reopen(old, || {
                    let read = |io: Option<&PreparationIo>| runtime.block_on(host.read_retained_file(&generation, &file, maximum, io))
                        .map(|bytes| bytes.as_ref().to_vec()).map_err(|e| format!("{e:?}"));
                    match mode {
                        None => read(None),
                        Some(native) => accounted(native, 64 * 1024 * 1024, cancelled, |io| read(Some(io))).unwrap().0,
                    }
                })
            });
            prop_assert_eq!(before, after, "mode {:?}", mode);
        }
    }
}

#[test]
fn retained_native_bytes_keep_original_funding_until_the_last_owner_drops() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| Error::Storage(error.to_string()))?;
    for old in [true, false] {
        let (host, file) = runtime.block_on(fixture(b"original funding"))?;
        let workspace = workspace_ref(
            host.provider.clone(),
            &file.volume().storage_name(&mut None)?,
        )?;
        let opened = runtime.block_on(host.open(&workspace, None))?;
        let generation = runtime.block_on(host.generation(
            &opened,
            &host.file_generation(&file, None)?,
            None,
        ))?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(1_000_000, 64 * 1024 * 1024),
            &cancellation,
        )
        .map_err(|(error, _)| Error::Invalid(error.to_string()))?;
        let baseline = work.live_allocation_bytes();
        assert_eq!(
            baseline,
            acyclic_fs::NativeMaterializationWork::accounting_state_bytes()
        );
        let io = PreparationIo::from_native_work(&work)?;
        let bytes = with_reopen(old, || {
            runtime.block_on(host.read_retained_file(&generation, &file, 4096, Some(&io)))
        })?;
        assert_eq!(bytes.as_ref(), b"original funding");
        drop(io);
        drop(generation);
        drop(opened);
        drop(host);
        let funded = |bytes: &acyclic_stream::OwnedBytes| {
            work.live_allocation_bytes()
                .checked_sub(baseline)
                .is_some_and(|live| live >= bytes.retained_capacity().unwrap() as u64 && live != 0)
        };
        assert!(funded(&bytes), "returned bytes lost their original ledger");
        let unfunded: acyclic_stream::OwnedBytes = bytes.as_ref().to_vec().into();
        drop(bytes);
        assert_eq!(
            work.live_allocation_bytes(),
            baseline,
            "returned custody leaked"
        );
        assert!(
            !funded(&unfunded),
            "an unfunded copy must fail the same predicate"
        );
    }
    Ok(())
}

#[test]
fn published_content_retries_and_recovery_do_not_append_retention_records() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| Error::Storage(error.to_string()))?;
    for entry in [Entry::Create, Entry::Retry, Entry::Observe] {
        for native in [false, true] {
            let (host, source, grant) = runtime.block_on(initialize(0, b"original"))?;
            let (result, receipt, _, _, _, _) = accounted(native, 64 * 1024 * 1024, false, |io| {
                runtime.block_on(execute(
                    &host,
                    &source,
                    &grant,
                    entry,
                    b"original",
                    Some(io),
                ))
            })
            .map_err(|(error, receipt)| Error::Storage(format!("{error}: {receipt:?}")))?;
            let file = result?;
            if matches!(entry, Entry::Create) {
                assert!(receipt.authority_records_appended > 0);
                assert!(
                    receipt.authority_records_appended <= 4,
                    "published content appended redundant retention: {receipt:?}"
                );
            } else {
                assert_eq!(
                    receipt.authority_records_appended, 0,
                    "authenticated content recovery appended retention: {entry:?} {receipt:?}"
                );
                assert_eq!(file, source);
            }
        }
    }
    Ok(())
}
