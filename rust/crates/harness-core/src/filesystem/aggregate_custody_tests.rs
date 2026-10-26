use super::*;
use crate::filesystem::{
    FilesystemContentPublisher, FilesystemContentVerifier, FilesystemExecutionJournal,
    FilesystemInteractionHost, FilesystemJournalBinding, FilesystemWorkflowJournal,
    FsRootProjectSourceProvider, PreparationIo,
};

#[derive(Clone, Copy, Debug)]
enum Constructor {
    Host,
    Content,
    Publisher,
    RootSource,
    Parent,
    Interaction,
    ExecutionJournal,
    WorkflowJournal,
}

impl Constructor {
    fn construct(
        self,
        fixture: &Fixture,
        verifier: &AuthorityVerifier,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        let binding = FilesystemJournalBinding {
            volume: &fixture.private,
            verifier,
            scope: &fixture.scope,
            maximum_bytes: 4096,
            io,
        };
        match self {
            Self::Host => drop(FilesystemHost::new(
                Fs::memory(),
                &fixture.host.provider,
                io,
            )?),
            Self::Content => drop(FilesystemContentVerifier::new(
                Arc::clone(&fixture.host),
                verifier,
                &fixture.scope,
                4096,
                io,
            )?),
            Self::Publisher => drop(FilesystemContentPublisher::new(
                Arc::clone(&fixture.host),
                &fixture.private,
                verifier,
                &fixture.scope,
                4096,
                io,
            )?),
            Self::RootSource => drop(FsRootProjectSourceProvider::new(
                Arc::clone(&fixture.host),
                fixture.parent.reducer().authority(),
                TaskId::from_bytes([9; 16]),
                io,
            )?),
            Self::Parent => drop(ParentProjectController::new(
                &fixture.host,
                fixture.parent.reducer(),
                verifier,
                &fixture.scope,
                &fixture.project,
                io,
            )?),
            Self::Interaction => drop(FilesystemInteractionHost::new(
                Arc::new(MemoryStream::default()),
                Arc::clone(&fixture.host),
                fixture.parent.reducer().authority(),
                &SchemaRegistry::new(),
                binding,
            )?),
            Self::ExecutionJournal => drop(FilesystemExecutionJournal::new(
                Arc::new(MemoryStream::default()),
                Arc::clone(&fixture.host),
                &SchemaRegistry::new(),
                binding,
            )?),
            Self::WorkflowJournal => drop(FilesystemWorkflowJournal::new(
                Arc::new(MemoryStream::default()),
                Arc::clone(&fixture.host),
                "constructor-custody",
                binding,
            )?),
        }
        Ok(())
    }
}

fn ledger(
    native: bool,
    budget: acyclic_fs::WorkBudget,
    cancellation: &acyclic_fs::CancellationToken,
) -> Result<PreparationIo> {
    if native {
        let original = acyclic_fs::NativeMaterializationWork::new(budget, cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        PreparationIo::from_native_work(&original)
    } else {
        PreparationIo::from_budget(budget, cancellation)
    }
}

#[tokio::test]
async fn failed_aggregate_constructors_release_every_allocation_boundary() -> Result<()> {
    let fixture = fixture().await?;
    let verifier = fixture.issuer.verifier(&mut None)?;
    let budget = acyclic_fs::performance::work_budget(1_000_000, 1_000_000);
    let cancellation = acyclic_fs::CancellationToken::new();
    let mut leaks = Vec::new();
    for constructor in [
        Constructor::Host,
        Constructor::Content,
        Constructor::Publisher,
        Constructor::RootSource,
        Constructor::Parent,
        Constructor::Interaction,
        Constructor::ExecutionJournal,
        Constructor::WorkflowJournal,
    ] {
        constructor.construct(&fixture, &verifier, None)?;
        for native in [false, true] {
            let probe = ledger(native, budget, &cancellation)?;
            probe.retain_owned(17)?;
            let setup = budget.allocation_operations - probe.remaining()?.allocation_operations;
            let setup_peak = probe.receipt().peak_allocation_bytes;
            constructor.construct(&fixture, &verifier, Some(&probe))?;
            let required =
                budget.allocation_operations - setup - probe.remaining()?.allocation_operations;
            assert!(required > 0, "{constructor:?} must reach metadata copies");
            let mut cuts = Vec::new();
            for operations in 0..required {
                let mut limited = budget;
                limited.allocation_operations = operations + setup;
                cuts.push((format!("allocation {operations}/{required}"), limited));
            }
            let peak = probe.receipt().peak_allocation_bytes;
            assert!(
                peak > setup_peak,
                "{constructor:?} must fund copied metadata"
            );
            for bytes in [setup_peak, setup_peak + (peak - setup_peak) / 2, peak - 1] {
                let mut limited = budget;
                limited.peak_allocation_bytes = bytes;
                cuts.push((format!("peak {bytes}/{peak}"), limited));
            }
            for (cut, limited) in cuts {
                let io = ledger(native, limited, &cancellation)?;
                // Construction must neither release nor adopt another owner's bytes.
                io.retain_owned(17)?;
                let before = io.owned_bytes()?;
                let result = constructor.construct(&fixture, &verifier, Some(&io));
                assert!(result.is_err(), "{constructor:?} native={native} cut={cut}");
                drop(result);
                let after = io.owned_bytes()?;
                if after != before {
                    leaks.push((format!("{constructor:?}"), native, cut, before, after));
                }
                io.release(17)?;
            }
        }
    }
    if !leaks.is_empty() {
        println!("AGGREGATE_CUSTODY_LEAKS {leaks:?}");
    }
    assert!(leaks.is_empty(), "aggregate custody leaks: {leaks:?}");
    Ok(())
}

#[tokio::test]
async fn rejected_interaction_reads_release_partial_assembly_custody() -> Result<()> {
    let fixture = fixture().await?;
    let verifier = fixture.issuer.verifier(&mut None)?;
    let host = FilesystemInteractionHost::new(
        Arc::new(MemoryStream::default()),
        Arc::clone(&fixture.host),
        fixture.parent.reducer().authority(),
        &SchemaRegistry::new(),
        FilesystemJournalBinding {
            volume: &fixture.private,
            verifier: &verifier,
            scope: &fixture.scope,
            maximum_bytes: 4096,
            io: None,
        },
    )?;
    let id = crate::InteractionId::from_bytes([13; 16]);
    assert!(
        host.read(id, None).await.is_err(),
        "empty history must reject the unbound conversation"
    );
    let budget = acyclic_fs::performance::work_budget(1_000_000, 1_000_000);
    let cancellation = acyclic_fs::CancellationToken::new();
    let mut leaks = Vec::new();
    for native in [false, true] {
        let setup_io = ledger(native, budget, &cancellation)?;
        setup_io.retain_owned(17)?;
        let setup = budget.allocation_operations - setup_io.remaining()?.allocation_operations;
        for (transaction, cut) in [false, true].into_iter().flat_map(|transaction| {
            std::iter::once(None)
                .chain((0..16).map(Some))
                .map(move |cut| (transaction, cut))
        }) {
            let mut limited = budget;
            if let Some(cut) = cut {
                limited.allocation_operations = setup + cut;
            }
            let io = ledger(native, limited, &cancellation)?;
            io.retain_owned(17)?;
            let before = io.remaining()?.peak_allocation_bytes;
            let result = if transaction {
                host.transaction(Some(&io)).await.map(drop)
            } else {
                host.read(id, Some(&io)).await.map(drop)
            };
            assert!(result.is_err(), "unbound interaction read must fail");
            drop(result);
            let after = io.remaining()?.peak_allocation_bytes;
            if before != after {
                leaks.push((native, transaction, cut, before, after));
            }
            io.release(17)?;
        }
    }
    println!("ASYNC_INTERACTION_CUSTODY_LEAKS {leaks:?}");
    assert!(
        leaks.is_empty(),
        "rejected interaction reads retain assembly charges: {leaks:?}"
    );
    Ok(())
}

#[test]
fn preparation_scopes_share_limits_and_release_only_their_own_storage() -> Result<()> {
    for native in [false, true] {
        for cancelled in [false, true] {
            for reverse in [false, true] {
                let cancellation = acyclic_fs::CancellationToken::new();
                let budget = acyclic_fs::performance::work_budget(1_000_000, 1_000_000);
                let parent = ledger(native, budget, &cancellation)?;
                parent.retain_owned(17)?;
                let available = parent.remaining()?.peak_allocation_bytes;
                let first = parent.scoped()?;
                let second = first.scoped()?;
                first.retain_owned(31)?;
                second.retain_owned(43)?;
                assert_eq!(first.owned_bytes()?, 31);
                assert_eq!(second.owned_bytes()?, 43);
                assert_eq!(parent.owned_bytes()?, 17);
                assert_eq!(parent.remaining()?.peak_allocation_bytes, available - 74);
                assert!(
                    first.release(32).is_err(),
                    "a scope cannot release its sibling's bytes"
                );
                let remaining = parent.remaining()?.allocation_operations;
                first.allocation_attempt(1)?;
                assert_eq!(parent.remaining()?.allocation_operations, remaining - 1);
                let mut denied = Vec::<u8>::new();
                let capacity = usize::try_from(available).map_err(|_| {
                    Error::Invalid("scope allowance exceeds addressable storage".into())
                })?;
                assert!(PreparationIo::reserve(&mut denied, capacity, Some(&second)).is_err());
                if cancelled {
                    cancellation.cancel();
                }
                if reverse {
                    drop((second, first));
                } else {
                    drop((first, second));
                }
                assert_eq!(parent.owned_bytes()?, 17);
                // Cancellation prevents further work, but never prevents retirement.
                let live = match &parent {
                    PreparationIo::Owned { work, .. } => work
                        .lock()
                        .map_err(|_| Error::Invalid("scope accounting poisoned".into()))?
                        .1
                        .as_ref()
                        .map_err(|_| Error::Invalid("scope ledger invalid".into()))?
                        .live_bytes(),
                    PreparationIo::Native { work, .. } => work.live_allocation_bytes(),
                };
                assert_eq!(live, budget.peak_allocation_bytes - available);
                parent.release(17)?;
            }
        }
    }
    Ok(())
}

#[test]
fn partial_formatted_metadata_uses_the_shared_copy_quota() -> Result<()> {
    fn assemble(io: &PreparationIo) -> Result<([String; 3], u64)> {
        crate::executor::with_history_io(Some(io), |work| {
            crate::wire_codec::CodecQuota::produce(work, |quota| {
                Ok([
                    quota.format(format_args!(".system/interactions/{}/request.json", 13))?,
                    quota.format(format_args!("{}.json", "request"))?,
                    quota.format(format_args!("interaction:{}:{}", 13, "request"))?,
                ])
            })
        })
    }
    for native in [false, true] {
        let budget = acyclic_fs::performance::work_budget(1_000_000, 1_000_000);
        let cancellation = acyclic_fs::CancellationToken::new();
        let probe = ledger(native, budget, &cancellation)?;
        probe.retain_owned(17)?;
        let setup = probe.remaining()?.allocation_operations;
        let (values, retained) = assemble(&probe)?;
        let required = setup - probe.remaining()?.allocation_operations;
        assert!(
            required >= 3,
            "all three formatted components must be funded"
        );
        drop(values);
        probe.release(retained)?;
        assert_eq!(probe.owned_bytes()?, 17);
        for cut in 0..required {
            let mut limited = budget;
            limited.allocation_operations -= setup - cut;
            let io = ledger(native, limited, &cancellation)?;
            io.retain_owned(17)?;
            let before = io.remaining()?.peak_allocation_bytes;
            let result = assemble(&io);
            assert!(result.is_err(), "native={native} cut={cut}/{required}");
            drop(result);
            assert_eq!(
                io.remaining()?.peak_allocation_bytes,
                before,
                "native={native} cut={cut}/{required}"
            );
            assert_eq!(io.owned_bytes()?, 17);
        }
    }
    Ok(())
}

#[tokio::test]
async fn interaction_read_adapters_release_replayed_projection_custody() -> Result<()> {
    let fixture = fixture().await?;
    let verifier = fixture.issuer.verifier(&mut None)?;
    let host = FilesystemInteractionHost::new(
        Arc::clone(&fixture.stream),
        Arc::clone(&fixture.host),
        fixture.parent.reducer().authority(),
        &SchemaRegistry::new(),
        FilesystemJournalBinding {
            volume: &fixture.private,
            verifier: &verifier,
            scope: &fixture.scope,
            maximum_bytes: 4096,
            io: None,
        },
    )?;
    let id = crate::InteractionId::from_bytes([13; 16]);
    for adapter in 0..6 {
        let io = PreparationIo::from_budget(
            acyclic_fs::performance::work_budget(1_000_000, 1_000_000),
            &acyclic_fs::CancellationToken::new(),
        )?;
        io.retain_owned(17)?;
        let before = io.remaining()?.peak_allocation_bytes;
        match adapter {
            0 => assert!(host.read(id, Some(&io)).await?.is_none()),
            1 => assert!(host.read_request(id, Some(&io)).await?.is_none()),
            _ => {
                let transaction = host.transaction(Some(&io)).await?;
                let aggregate = &transaction.aggregate.0;
                match adapter {
                    2 => assert!(host.read_outcome(aggregate, id, Some(&io))?.is_none()),
                    3 | 4 => assert!(
                        host.read_resolution_file(aggregate, id, adapter == 4, Some(&io))
                            .await?
                            .is_none()
                    ),
                    _ => assert!(
                        host.read_for_publication(
                            aggregate,
                            id,
                            Some((OperationId::from_bytes([14; 16]), [15; 32])),
                            Some(&io),
                        )
                        .is_err()
                    ),
                }
            }
        }
        assert_eq!(io.owned_bytes()?, 17, "adapter={adapter}");
        assert_eq!(
            io.remaining()?.peak_allocation_bytes,
            before,
            "adapter={adapter}"
        );
        io.release(17)?;
    }
    Ok(())
}
