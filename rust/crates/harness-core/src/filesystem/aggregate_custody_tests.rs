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
