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

async fn turn(
    journal: Journal,
    old: bool,
    prompt: &str,
    deltas: usize,
    broken: bool,
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
    let input = TurnInput {
        operation_id: operation,
        input: ModelContent::Text(prompt.into()),
        selected_context: None,
        max_steps: 1,
    };
    let selected = Choice(&journal, old);
    let output = executor.execute(input.clone(), &selected).await?;
    assert_eq!(executor.execute(input, &selected).await?, output);
    assert_eq!(provider.remaining(), 0);
    let requests = provider
        .requests()
        .iter()
        .map(crate::contract::canonical_json_bytes)
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(requests.len(), 1);
    let [raw, publications] = persisted(&journal, operation).await?;
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
            |journal, old| runtime.block_on(turn(journal, old, &prompt, deltas, false)).unwrap());
        prop_assert_eq!(&actual, &expected);
        for group in 0..3 {
            let mut broken = actual.clone();
            broken.0[group][usize::from(group == 2)][0] ^= 1;
            prop_assert_ne!(&broken, &expected);
        }
        let mut missing = actual;
        missing.0[2].pop();
        prop_assert_ne!(missing, expected);
        let [expected, broken] = differential::pair::<_, _, (&'static str, u64)>(|| runtime.block_on(fixture()).unwrap(),
            |journal, old| runtime.block_on(turn(journal, old, &prompt, deltas, !old)).unwrap());
        prop_assert_eq!(&expected.0, &broken.0);
        prop_assert_ne!(&expected.1, &broken.1, "extra verification was not detected");
    }

    // Fail through each real entrypoint before storage work: isolate the
    // wrapper's exact one-box saving from the full operation above.
    #[test]
    fn journal_wrapper_removes_exactly_one_allocation(mut digest in any::<[u8;32]>()) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let operation = OperationId::from_bytes([4;16]);
        digest[0] |= 1;
        let event = ExecutionEvent::Started { request_digest: digest };
        let oversized = vec![0;4097];
        for mode in 0..5 {
            let [before, after] = differential::pair::<_, _, (&'static str, u64)>(|| {
                let mut journal = runtime.block_on(fixture()).unwrap();
                let file = runtime.block_on(prepare(&journal, operation, &event, b"payload")).unwrap();
                journal.execution = Some((OperationId::from_bytes([5;16]), operation));
                journal.volume = VolumeRef::new(journal.volume.provider().clone(), "foreign".into(),
                    VolumeClass::AgentPrivate, VolumeOwner::Agent(AgentId::from_bytes([1;16])), &mut None).unwrap();
                (journal, file)
            }, |(journal, file), old| {
                let mut outcome = None;
                let allocation = acyclic_test_support::measure(|| {
                    outcome = Some(runtime.block_on(run(&Choice(&journal, old), mode, 0,
                        (operation, "key", &event), &file, &oversized).unwrap()).map_err(|e| e.to_string()));
                });
                (outcome.unwrap(), allocation)
            });
            prop_assert!(after.0.0.is_err());
            prop_assert_eq!(&before.0.0, &after.0.0);
            prop_assert_eq!(before.1, after.1);
            let (before, after) = (before.0.1, after.0.1);
            prop_assert_eq!(after.count_total, before.count_total - 1);
            let saved = [21136, 21024, 14304, 11840, 14192][mode as usize];
            eprintln!("journal method={mode} before={before:?} after={after:?} saved={saved}");
            prop_assert_eq!(before.bytes_total - after.bytes_total, saved);
            prop_assert_ne!(before.count_total, before.count_total - 1, "old nested box passed");
        }
    }

    // Every old/new factory is compared at its exact admission boundary, after
    // drop, and with cancellation. The smaller unmetered frame must not change
    // original accounting or move the success/failure threshold.
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
        let initialize = || {
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
        let (journal, file) = initialize();
        for mode in 0..5 {
            for native in [false, true] {
                // Measure the real admission peak with this ledger, including
                // native ledger storage and staging's owner/grant checks.
                let measured = accounted(native, 1 << 24, false, |io| {
                    construct(&journal, mode, true, operation, &event, &file, io)
                })
                .unwrap();
                measured.0.unwrap();
                let boundary = if native { measured.3 } else { measured.1 }.peak_allocation_bytes;
                let ledger = if native {
                    acyclic_fs::NativeMaterializationWork::accounting_state_bytes()
                } else {
                    0
                };
                assert!(boundary > ledger, "factory frame was not admitted");
                for maximum in [boundary - 1, boundary, boundary + 1] {
                    let [before, after] = differential::pair::<_, _, (&'static str, u64)>(
                        &initialize,
                        |(journal, file), old| {
                            accounted(native, maximum, cancelled, |io| {
                                construct(&journal, mode, old, operation, &event, &file, io)
                                    .map_err(|e| e.to_string())
                            })
                        },
                    );
                    let held = before.0.as_ref().unwrap();
                    assert_eq!(
                        held.0.is_ok(),
                        !cancelled && maximum >= boundary,
                        "method {mode}, native {native}, limit {maximum}, boundary {boundary}"
                    );
                    // Native stage constructs the capability label before
                    // admitting its frame; that label stays owned by the IO.
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
                        let receipt = if native { held.3 } else { held.1 };
                        assert_eq!(receipt.peak_allocation_bytes, boundary);
                    }
                    assert_eq!(before, after, "method {mode}, native {native}");
                }
            }
        }
        for native in [false, true] {
            let [before, after] = differential::pair::<_, _, (&'static str, u64)>(
                &initialize,
                |(journal, file), old| {
                    accounted(native, 1 << 24, cancelled, |io| {
                        runtime
                            .block_on(async { Choice(&journal, old).load(&file, Some(io))?.await })
                            .map(|bytes| bytes.as_ref().to_vec())
                            .map_err(|e| e.to_string())
                    })
                },
            );
            assert_eq!(before.0.as_ref().unwrap().0.is_ok(), !cancelled);
            assert_eq!(before, after, "polled load, native {native}");
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
