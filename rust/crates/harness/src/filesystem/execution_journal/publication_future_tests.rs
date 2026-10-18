use super::*;
use crate::test_support::accounted::accounted;
use crate::{AgentId, conversation::VolumeOwner, resources::ProviderRef};
use acyclic_test_support::differential;
use proptest::prelude::*;

type Journal = FilesystemExecutionJournal<
    acyclic_stream::MemoryStream,
    acyclic_fs::MemoryAuthorityBackend,
    acyclic_fs::MemoryObjectBackend,
>;

struct Clock;
impl acyclic_stream::UnixMillisClock for Clock {
    fn now_unix_millis(&self) -> u64 {
        1234
    }
}

async fn fixture() -> Result<Journal> {
    let provider = ProviderRef::new("future".into(), "filesystem".into(), "2".into(), &mut None)?;
    let host = FilesystemHost::new(acyclic_fs::Fs::memory(), &provider, None)?;
    let agent = AgentId::from_bytes([1; 16]);
    let volume = VolumeRef::new(
        provider,
        "future".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    host.create_volume(&volume).await?;
    let issuer = crate::core::AuthorityIssuer::new(
        "future",
        [2; 32],
        crate::core::Authority {
            kind: crate::core::AggregateKind::Conversation,
            id: crate::ConversationId::from_bytes([3; 16]).to_string(),
        },
    );
    let scope = issuer.issue(
        "owner",
        &crate::Capabilities::new([
            volume.capability(VolumeOperation::Read, &mut None)?,
            volume.capability(VolumeOperation::Write, &mut None)?,
        ]),
        Some(agent),
        None,
        &mut None,
    )?;
    FilesystemExecutionJournal::new(
        Arc::new(acyclic_stream::MemoryStream::new_with_clock(
            Default::default(),
            Arc::new(Clock),
        )),
        host,
        &SchemaRegistry::new(),
        crate::filesystem::FilesystemJournalBinding {
            volume: &volume,
            verifier: &issuer.verifier(&mut None)?,
            scope: &scope,
            maximum_bytes: 4096,
            io: None,
        },
    )
}

mod legacy;
use serde_json::Value;

async fn prepare(
    journal: &Journal,
    operation: OperationId,
    event: &ExecutionEvent,
    bytes: &[u8],
) -> Result<FileRef> {
    journal.append(operation, "start", event, None)?.await?;
    journal.append(operation, "start", event, None)?.await?;
    journal
        .stage(
            operation,
            "payload",
            bytes,
            "application/octet-stream",
            None,
        )?
        .await
}

// Both executor runs use the same entrypoints, backed by disjoint stores.
struct Choice<'a>(&'a Journal, bool);
macro_rules! factories {
    ($(fn $name:ident($($arg:ident: $ty:ty),*) -> $out:ty;)*) => {
        impl ExecutionJournal for Choice<'_> {
            $(fn $name<'a>(&'a self, $($arg: $ty,)* io: Option<&'a PreparationIo>)
                -> Result<crate::conversation::BoundedContentFuture<'a, $out>> {
                if self.1 { legacy::$name(self.0, $($arg,)* io) }
                else { self.0.$name($($arg,)* io) }
            })*
            fn append_if_tail<'a>(&'a self, operation: OperationId, tail: u64, key: &'a str,
                event: &'a ExecutionEvent, io: Option<&'a PreparationIo>)
                -> Result<crate::conversation::BoundedContentFuture<'a, bool>> {
                // Control the executor's fresh dispatch nonce BEFORE either
                // factory; stored bytes are compared without normalization.
                let key = if key.starts_with("model:0:claim:") { "model:0:claim:fixed" } else { key };
                if self.1 { legacy::append_if_tail(self.0, operation, tail, key, event, io) }
                else { self.0.append_if_tail(operation, tail, key, event, io) }
            }
            fn open_interaction<'a>(&'a self, id: InteractionId, interaction: &'a Interaction,
                io: Option<&'a PreparationIo>) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
                self.0.open_interaction(id, interaction, io)
            }
            fn interaction_outcome<'a>(&'a self, id: InteractionId, io: Option<&'a PreparationIo>)
                -> Result<crate::conversation::BoundedContentFuture<'a, Option<InteractionOutcome>>> {
                self.0.interaction_outcome(id, io)
            }
        }
    };
}
factories! {
    fn append(operation: OperationId, key: &'a str, event: &'a ExecutionEvent) -> ();
    fn replay(operation: OperationId, tail: u64, maximum: u32) -> Vec<ExecutionRecord>;
    fn load(file: &'a FileRef) -> acyclic_stream::OwnedBytes;
    fn stage(operation: OperationId, key: &'a str, bytes: &'a [u8], media: &'static str) -> FileRef;
}

fn run<'a>(
    journal: &'a dyn ExecutionJournal,
    mode: u8,
    tail: u64,
    (operation, key, event): (OperationId, &'a str, &'a ExecutionEvent),
    file: &'a FileRef,
    bytes: &'a [u8],
) -> Result<BoxFuture<'a, Result<Value>>> {
    use futures::FutureExt as _;
    macro_rules! value {
        ($future:expr) => {
            $future?
                .map(|r| {
                    r.and_then(|v| {
                        serde_json::to_value(v).map_err(|e| Error::Invalid(e.to_string()))
                    })
                })
                .boxed()
        };
    }
    Ok(match mode {
        0 => value!(journal.append(operation, key, event, None)),
        1 => value!(journal.append_if_tail(operation, tail, key, event, None)),
        2 => value!(journal.replay(operation, tail, 64, None)),
        3 => journal
            .load(file, None)?
            .map(|r| r.map(|v| serde_json::json!(v.as_ref())))
            .boxed(),
        _ => value!(journal.stage(operation, key, bytes, "application/octet-stream", None)),
    })
}

fn construct(
    journal: &Journal,
    mode: u8,
    old: bool,
    operation: OperationId,
    event: &ExecutionEvent,
    file: &FileRef,
    io: &PreparationIo,
) -> Result<()> {
    let journal = Choice(journal, old);
    match mode {
        0 => drop(journal.append(operation, "key", event, Some(io))?),
        1 => drop(journal.append_if_tail(operation, 0, "key", event, Some(io))?),
        2 => drop(journal.replay(operation, 0, 64, Some(io))?),
        3 => drop(journal.load(file, Some(io))?),
        _ => drop(journal.stage(operation, "key", b"", "text/plain", Some(io))?),
    }
    Ok(())
}

// Include every raw record field and every referenced publication. The
// independent fixtures use the same trusted clock and admission inputs.
async fn persisted(journal: &Journal, operation: OperationId) -> Result<[Vec<Vec<u8>>; 2]> {
    use futures::TryStreamExt as _;
    let mut raw = match journal.path(operation)?.read(0, 64).await {
        Ok(raw) => raw,
        Err(StreamError::NotFound) => return Ok([Vec::new(), Vec::new()]),
        Err(error) => return Err(Error::Storage(error.to_string())),
    };
    let mut result = [Vec::new(), Vec::new()];
    while let Some(record) = raw
        .try_next()
        .await
        .map_err(|e| Error::Storage(e.to_string()))?
    {
        let mut bytes = record.sequence.to_le_bytes().to_vec();
        bytes.extend_from_slice(record.commit_id.as_bytes());
        bytes.extend_from_slice(&record.committed_at_micros.to_le_bytes());
        bytes.extend_from_slice(&record.value);
        result[0].push(bytes);
        let observation: Value = serde_json::from_slice(&record.value).unwrap();
        for field in observation["event"].as_object().unwrap().values() {
            if let Ok(file) = serde_json::from_value::<FileRef>(field.clone()) {
                result[1].push(crate::contract::canonical_json_bytes(&file)?);
                result[1].push(journal.load_content(&file, None).await?.as_ref().to_vec());
            }
        }
    }
    Ok(result)
}

async fn fixture_state(
    journal: &Journal,
    operation: OperationId,
    volume: &VolumeRef,
) -> Result<(crate::resources::GenerationRef, [Vec<Vec<u8>>; 2])> {
    let workspace = crate::resources::WorkspaceRef::new(
        volume.provider().clone(),
        volume.storage_name(&mut None)?.into_bytes(),
        None,
    )?;
    Ok((
        journal.host.resolve(&workspace, None).await?.generation,
        persisted(journal, operation).await?,
    ))
}

#[tokio::test]
async fn journal_fixture_state_detects_mutation() -> Result<()> {
    let journal = fixture().await?;
    let operation = OperationId::from_bytes([4; 16]);
    let before = fixture_state(&journal, operation, &journal.volume).await?;
    assert_eq!(
        before,
        fixture_state(&journal, operation, &journal.volume).await?
    );
    journal
        .stage(operation, "mutation", b"mutation", "text/plain", None)?
        .await?;
    let staged = fixture_state(&journal, operation, &journal.volume).await?;
    assert_ne!(
        before.0, staged.0,
        "filesystem mutation escaped the state check"
    );
    assert_eq!(before.1, staged.1);
    journal
        .append(
            operation,
            "mutation",
            &ExecutionEvent::Started {
                request_digest: [9; 32],
            },
            None,
        )?
        .await?;
    let appended = fixture_state(&journal, operation, &journal.volume).await?;
    assert_eq!(staged.0, appended.0);
    assert_ne!(
        staged.1, appended.1,
        "journal mutation escaped the state check"
    );
    Ok(())
}

async fn turn(
    journal: &Journal,
    old: bool,
    prompt: &str,
    deltas: usize,
    broken: bool,
    host: Option<Arc<dyn crate::runtime::DurableTaskHost>>,
) -> Result<[Vec<Vec<u8>>; 3]> {
    use crate::{
        context::ContextPipeline,
        executor::{Executor, StockExecutor, TurnInput},
        model::Model,
        test_support::{ScriptedEvent, ScriptedModelProvider, ScriptedTurn},
        tool::ToolRegistry,
    };
    let operation = OperationId::from_bytes([4; 16]);
    let events = (0..deltas)
        .map(|i| {
            ScriptedEvent::Event(ModelEvent::Content {
                delta: format!("{i}:{prompt}"),
            })
        })
        .chain([ScriptedEvent::Event(ModelEvent::Completed {
            metadata: serde_json::json!({"done":true}),
        })]);
    let provider = Arc::new(ScriptedModelProvider::new([ScriptedTurn::Events(
        events.collect(),
    )]));
    let executor = StockExecutor::new(
        Model::new(
            "future".into(),
            "model".into(),
            "1".into(),
            serde_json::json!({"temperature":0}),
        )?,
        provider.clone(),
        ContextPipeline::default(),
        ToolRegistry::default(),
    )
    .with_compaction_policy(crate::context::CompactionPolicy::Disabled);
    let executor = if let Some(host) = host {
        let (task, fence) = journal.owner.as_ref().unwrap().0.task_binding();
        executor.with_durable_task(host, task, fence.clone())
    } else {
        executor
    };
    let input = TurnInput {
        operation_id: operation,
        input: ModelContent::Text(prompt.into()),
        selected_context: None,
        max_steps: 1,
    };
    let selected = Choice(journal, old);
    let output = executor.execute(input.clone(), &selected).await?;
    assert_eq!(executor.execute(input, &selected).await?, output);
    assert_eq!(provider.remaining(), 0);
    let requests = provider
        .requests()
        .iter()
        .map(crate::contract::canonical_json_bytes)
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(requests.len(), 1);
    let [raw, publications] = persisted(journal, operation).await?;
    assert!(!raw.is_empty() && !publications.is_empty());
    if broken {
        let descriptor = crate::conversation::FileDescriptor::from_bytes(
            &requests[0],
            "application/json",
            &mut None,
        )?;
        descriptor.verify(&requests[0], &mut None)?; // Extra real verification, unchanged outputs.
    }
    Ok([requests, raw, publications])
}

#[test]
fn journal_unmetered_task_files_match_main() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let operation = OperationId::from_bytes([4; 16]);
    let initialize = || {
        runtime
            .block_on(async {
                let fixture = crate::durable_host::tests::fixture::Fixture::new(
                    Arc::new(acyclic_stream::MemoryStream::new_with_clock(
                        Default::default(),
                        Arc::new(Clock),
                    )),
                    acyclic_fs::Fs::memory(),
                    serde_json::json!({"type":"integer"}),
                    Some(crate::scheduler::SessionLimits {
                        active_tasks: 1,
                        total_tasks: 1,
                        depth: 0,
                        model_steps: 2,
                    }),
                    &[],
                )
                .await?;
                let journal = fixture.execution(fixture.running(80).await?, operation)?;
                let files = crate::task_volume::TaskFiles::open(
                    fixture.stream.clone(),
                    &fixture.storage,
                    &fixture.issuer.verifier(&mut None)?,
                    &journal.scope,
                    65_536,
                )
                .await?;
                Ok::<_, Error>((journal.with_task_files(Some(Arc::new(files))), fixture.host))
            })
            .unwrap()
    };
    for broken in [false, true] {
        let [expected, actual] =
            differential::pair::<_, _, (&'static str, u64)>(&initialize, |(journal, host), old| {
                runtime.block_on(async {
                    let artifacts = turn(&journal, old, "chunk", 1, false, Some(host))
                        .await
                        .unwrap();
                    if broken && !old {
                        // Restore the erroneous unconditional publication on a fresh
                        // candidate fixture; canonical journal bytes stay unchanged.
                        let records = journal.records(operation, 0, 64, None).await.unwrap();
                        let record = records
                            .iter()
                            .find(|r| matches!(r.event, ExecutionEvent::Model { .. }))
                            .unwrap();
                        journal
                            .publish_chunk("model:0:event:0", &record.event, None)
                            .await
                            .unwrap();
                    }
                    let snapshot = journal
                        .task_files
                        .as_ref()
                        .unwrap()
                        .snapshot(&crate::task_volume::Query::Root, 16)
                        .await
                        .unwrap();
                    (artifacts, snapshot.files)
                })
            });
        assert_eq!(expected.0.0, actual.0.0);
        assert!(expected.0.1.is_empty());
        if broken {
            assert_eq!(actual.0.1.len(), 1);
            assert_eq!(actual.0.1[0].bytes.as_ref(), b"0:chunk");
            assert_ne!(
                expected, actual,
                "unconditional publication was not detected"
            );
        } else {
            assert_eq!(expected, actual);
        }
    }
}

fn journal_wrapper_observations(
    mut digest: [u8; 32],
    restore_box: bool,
    pad_frame: bool,
    mut observe: impl FnMut(
        usize,
        acyclic_test_support::AllocationInfo,
        acyclic_test_support::AllocationInfo,
    ) -> proptest::test_runner::TestCaseResult,
) -> proptest::test_runner::TestCaseResult {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let operation = OperationId::from_bytes([4; 16]);
    digest[0] |= 1;
    let event = ExecutionEvent::Started {
        request_digest: digest,
    };
    let oversized = vec![0; 4097];
    let initializations = std::cell::Cell::new(0);
    let [before, after] = [true, restore_box].map(|old| {
        initializations.set(initializations.get() + 1);
        let mut journal = runtime.block_on(fixture()).unwrap();
        let file = runtime
            .block_on(prepare(&journal, operation, &event, b"payload"))
            .unwrap();
        journal.execution = Some((OperationId::from_bytes([5; 16]), operation));
        journal.volume = VolumeRef::new(
            journal.volume.provider().clone(),
            "foreign".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([1; 16])),
            &mut None,
        )
        .unwrap();
        let state = runtime
            .block_on(fixture_state(&journal, operation, file.volume()))
            .unwrap();
        [0u8, 1, 2, 3, 4].map(|mode| {
            let observed = differential::observe::<_, _, (&'static str, u64)>(
                || (),
                |()| {
                    let mut outcome = None;
                    let allocation = acyclic_test_support::measure(|| {
                        crate::conversation::BoundedContentFuture::<()>::with_padded_frame(
                            pad_frame && !old,
                            || {
                                outcome = Some(
                                    runtime
                                        .block_on(
                                            run(
                                                &Choice(&journal, old),
                                                mode,
                                                0,
                                                (operation, "key", &event),
                                                &file,
                                                &oversized,
                                            )
                                            .unwrap(),
                                        )
                                        .map_err(|e| e.to_string()),
                                );
                            },
                        );
                    });
                    (outcome.unwrap(), allocation)
                },
            );
            assert_eq!(
                &state,
                &runtime
                    .block_on(fixture_state(&journal, operation, file.volume()))
                    .unwrap()
            );
            assert!(observed.1.is_empty(), "factory error reached storage work");
            observed
        })
    });
    prop_assert_eq!(initializations.get(), 2);
    for (mode, (before, after)) in before.into_iter().zip(after).enumerate() {
        prop_assert!(after.0.0.is_err());
        prop_assert_eq!(&before.0.0, &after.0.0);
        prop_assert_eq!(before.1, after.1);
        let (before, after) = (before.0.1, after.0.1);
        prop_assert_eq!(
            after.count_total,
            before.count_total - 1,
            "journal wrapper allocation count"
        );
        observe(mode, before, after)?;
    }
    Ok(())
}

#[test]
fn journal_wrapper_frame_measurements() {
    journal_wrapper_observations([1; 32], false, false, |mode, before, after| {
        eprintln!(
            "JOURNAL_FRAME {}",
            serde_json::json!({
                "mode": mode,
                "before": differential::allocation_counts(before),
                "after": differential::allocation_counts(after),
                "saved": before.bytes_total - after.bytes_total,
            })
        );
        Ok(())
    })
    .unwrap();
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // No warm-up or stores are shared across the two implementations. New
    // append/stage operations start with no matching publication to mask a loss.
    #[test]
    fn journal_independent_operations_preserve_bytes(
        key in "[a-z]{1,16}", mut digest in any::<[u8;32]>(),
        tail in prop_oneof![Just(0), Just(1), Just(u64::MAX), any::<u64>()], failure in 0u8..3,
        bytes in prop::collection::vec(any::<u8>(), 0..1024),
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        digest[0] |= 1;
        let operation = OperationId::from_bytes([4;16]);
        let event = ExecutionEvent::Started { request_digest: if failure == 1 { [0;32] } else { digest } };
        let key = if failure == 2 { "" } else { &key };
        for mode in 0..5 {
            let observations = differential::pair::<_, _, (&'static str, u64)>(|| runtime.block_on(fixture()).unwrap(), |journal, old| {
                let selected = Choice(&journal, old);
                let file = runtime.block_on(selected.stage(operation, "seed-only", b"seed", "application/octet-stream", None).unwrap()).unwrap();
                if mode == 2 { runtime.block_on(selected.append(operation, "start", &ExecutionEvent::Started { request_digest: digest }, None).unwrap()).unwrap(); }
                let result = runtime.block_on(run(&selected, mode, tail,
                    (operation, key, &event), &file, &bytes).unwrap()).map_err(|e| e.to_string());
                let mut artifacts = runtime.block_on(persisted(&journal, operation)).unwrap();
                if mode == 3 { artifacts[1].push(runtime.block_on(journal.load_content(&file, None)).unwrap().as_ref().to_vec()); }
                if mode == 4 && let Ok(value) = &result {
                    let file: FileRef = serde_json::from_value(value.clone()).unwrap();
                    artifacts[1].push(runtime.block_on(journal.load_content(&file, None)).unwrap().as_ref().to_vec());
                }
                (result, artifacts)
            });
            prop_assert_eq!(&observations[0], &observations[1], "method {}", mode);
        }
    }

    #[test]
    fn journal_independent_turns_preserve_complete_requests_and_publications(
        prompt in ".{1,128}", deltas in 0usize..9,
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let [expected, actual] = differential::pair::<_, _, (&'static str, u64)>(|| runtime.block_on(fixture()).unwrap(),
            |journal, old| runtime.block_on(turn(&journal, old, &prompt, deltas, false, None)).unwrap());
        prop_assert_eq!(&actual, &expected);
        for group in 0..3 {
            let mut broken = actual.clone();
            broken.0[group][usize::from(group == 2)][0] ^= 1;
            prop_assert_ne!(&broken, &expected);
        }
        let mut missing = actual;
        missing.0[2].pop();
        prop_assert_ne!(&missing, &expected);
        let broken = differential::observe::<_, _, (&'static str, u64)>(|| runtime.block_on(fixture()).unwrap(),
            |journal| runtime.block_on(turn(&journal, false, &prompt, deltas, true, None)).unwrap());
        prop_assert_eq!(&expected.0, &broken.0);
        prop_assert_ne!(&expected.1, &broken.1, "extra verification was not detected");
    }

    // Fail through each real entrypoint before storage work: isolate the
    // wrapper's exact one-box saving from the full operation above.
    #[test]
    fn journal_wrapper_removes_exactly_one_allocation(digest in any::<[u8;32]>()) {
        exact_wrapper_layout(digest, false, false)?;
    }

    // Every old/new factory is compared at its exact admission boundary, after
    // drop, and with cancellation. The smaller unmetered frame must not change
    // original accounting or move the success/failure threshold.
}

fn exact_wrapper_layout(
    digest: [u8; 32],
    restore_box: bool,
    pad_frame: bool,
) -> proptest::test_runner::TestCaseResult {
    // Old outer box + old nested trace box - independent unmetered frame.
    // Each term comes from a concrete reference type in this feature closure.
    journal_wrapper_observations(digest, restore_box, pad_frame, |mode, before, after| {
        let saved = legacy::expected_saved_bytes(mode) as u64;
        prop_assert!(saved > 0, "original factory layout was not captured");
        prop_assert_eq!(
            before.bytes_total,
            after.bytes_total + saved,
            "journal wrapper allocated bytes"
        );
        Ok(())
    })
}

#[test]
fn restoring_the_nested_factory_box_fails_the_exact_guard() {
    let error = exact_wrapper_layout([1; 32], true, false).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("journal wrapper allocation count")
    );
}

#[test]
fn enlarging_the_inline_factory_frame_fails_the_same_byte_guard() {
    let error = exact_wrapper_layout([1; 32], false, true).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("journal wrapper allocated bytes"),
        "size control did not reach the byte guard: {error}"
    );
}

#[test]
fn journal_accounting_admits_the_original_frames() {
    for cancelled in [false, true] {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let operation = OperationId::from_bytes([4; 16]);
        let event = ExecutionEvent::Started {
            request_digest: [9; 32],
        };
        let initializations = std::cell::Cell::new(0);
        let initialize = || {
            initializations.set(initializations.get() + 1);
            runtime
                .block_on(async {
                    let fixture = crate::durable_host::tests::fixture::Fixture::new(
                        Arc::new(acyclic_stream::MemoryStream::default()),
                        acyclic_fs::Fs::memory(),
                        serde_json::json!({"type":"integer"}),
                        None,
                        &[],
                    )
                    .await?;
                    let journal = fixture.execution(fixture.running(80).await?, operation)?;
                    let file = prepare(&journal, operation, &event, b"payload").await?;
                    Ok::<_, Error>((journal, file))
                })
                .unwrap()
        };
        let observations = [true, false].map(|old| {
            let (journal, file) = initialize();
            let state = runtime
                .block_on(fixture_state(&journal, operation, &journal.volume))
                .unwrap();
            // Constructors are never polled. Keep independent stores per side,
            // with a fresh ledger for every original boundary/cancellation case.
            let mut cases = Vec::with_capacity(30);
            for mode in 0..5 {
                for native in [false, true] {
                    let measured = accounted(native, 1 << 24, false, |io| {
                        construct(&journal, mode, true, operation, &event, &file, io)
                    })
                    .unwrap();
                    measured.0.unwrap();
                    let boundary =
                        if native { measured.3 } else { measured.1 }.peak_allocation_bytes;
                    let ledger = if native {
                        acyclic_fs::NativeMaterializationWork::accounting_state_bytes()
                    } else {
                        0
                    };
                    assert!(boundary > ledger, "factory frame was not admitted");
                    for maximum in [boundary - 1, boundary, boundary + 1] {
                        let observed = differential::observe::<_, _, (&'static str, u64)>(
                            || (),
                            |()| {
                                accounted(native, maximum, cancelled, |io| {
                                    construct(&journal, mode, old, operation, &event, &file, io)
                                        .map_err(|e| e.to_string())
                                })
                            },
                        );
                        assert_eq!(
                            &state,
                            &runtime
                                .block_on(fixture_state(&journal, operation, &journal.volume))
                                .unwrap()
                        );
                        let held = observed.0.as_ref().unwrap();
                        assert_eq!(
                            held.0.is_ok(),
                            !cancelled && maximum >= boundary,
                            "method {mode}, native {native}, limit {maximum}, boundary {boundary}"
                        );
                        let grant_bytes = if native && mode == 4 && !cancelled {
                            journal
                                .volume
                                .capability(VolumeOperation::Write, &mut None)
                                .unwrap()
                                .len() as u64
                        } else {
                            0
                        };
                        assert_eq!(
                            held.2, grant_bytes,
                            "factory retained frame custody after drop"
                        );
                        assert_eq!(held.4, ledger + grant_bytes, "native frame custody leaked");
                        assert_eq!(
                            held.5, ledger,
                            "only the original ledger may survive IO drop"
                        );
                        if !cancelled && maximum >= boundary {
                            assert_eq!(
                                if native { held.3 } else { held.1 }.peak_allocation_bytes,
                                boundary
                            );
                        }
                        cases.push((mode, native, maximum, observed));
                    }
                }
            }
            let loads = [false, true].map(|native| {
                let observed = differential::observe::<_, _, (&'static str, u64)>(
                    || (),
                    |()| {
                        accounted(native, 1 << 24, cancelled, |io| {
                            runtime
                                .block_on(async {
                                    Choice(&journal, old).load(&file, Some(io))?.await
                                })
                                .map(|bytes| bytes.as_ref().to_vec())
                                .map_err(|e| e.to_string())
                        })
                    },
                );
                assert_eq!(
                    &state,
                    &runtime
                        .block_on(fixture_state(&journal, operation, &journal.volume))
                        .unwrap()
                );
                assert_eq!(observed.0.as_ref().unwrap().0.is_ok(), !cancelled);
                observed
            });
            (cases, loads)
        });
        assert_eq!(initializations.get(), 2);
        assert_eq!(observations[0], observations[1]);
        // Cancelled operations can return before emitting any trace event.
        // Use the successful cases (including actual reads) for the transfer control.
        if !cancelled {
            // Moving an event between adjacent cases preserves the aggregate but
            // must fail the very same per-case trace comparison.
            let traces: Vec<_> = observations[1]
                .0
                .iter()
                .map(|case| case.3.1.clone())
                .chain(observations[1].1.iter().map(|load| load.1.clone()))
                .collect();
            let mut moved = traces.clone();
            let case = moved[..moved.len() - 1]
                .iter()
                .position(|trace| !trace.is_empty())
                .unwrap();
            let event = moved[case].pop().unwrap();
            moved[case + 1].insert(0, event);
            assert_eq!(traces.concat(), moved.concat());
            assert_ne!(
                traces, moved,
                "case-to-case trace transfer was not detected"
            );
        }
    }
}

fn records_poll_parent(mode: u8, early: bool) -> Result<bool> {
    use futures::FutureExt as _;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let journal = runtime.block_on(fixture())?;
    let operation = OperationId::from_bytes([4; 16]);
    let event = ExecutionEvent::Started {
        request_digest: [9; 32],
    };
    let file = runtime.block_on(prepare(&journal, operation, &event, b"payload"))?;
    let (seen, _capture) = crate::obs::capture::install();
    let construction = obs_span!(INFO, "acyclic.harness.test.construct");
    let polling = obs_span!(INFO, "acyclic.harness.test.poll");
    let future: Result<BoxFuture<'_, Result<Value>>> = construction.in_scope(|| {
        let future = run(
            &journal,
            mode,
            0,
            (operation, "start", &event),
            &file,
            b"payload",
        )?;
        if early {
            Ok(
                crate::obs::traced_future(obs_span!(INFO, "acyclic.harness.test.early"), future)
                    .boxed(),
            )
        } else {
            Ok(future)
        }
    });
    polling.in_scope(|| runtime.block_on(future?))?;
    let name = ["append", "append_if_tail", "replay", "load", "stage"][mode as usize];
    let name = format!("acyclic.harness.journal.{name}");
    Ok(
        crate::obs::capture::has(&seen, &name, "parent", "acyclic.harness.test.poll")
            && crate::obs::capture::has(&seen, &name, "outcome", "ok"),
    )
}

#[test]
fn journal_tracing_captures_the_poll_context() -> Result<()> {
    for mode in 0..5 {
        assert!(records_poll_parent(mode, false)?);
        assert!(
            !records_poll_parent(mode, true)?,
            "early context control escaped detection"
        );
    }
    Ok(())
}
