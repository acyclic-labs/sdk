use super::*;
use acyclic_fs::{
    MaterializationEdit, MaterializationIntent, MaterializationJournal, MaterializationPhase,
    MaterializationPlan, MaterializationPreimage, WorkBudget, WorkCounters,
};
use proptest::prelude::*;

fn journal(original: Vec<u8>, edits: Vec<MaterializationEdit>) -> MaterializationJournal {
    MaterializationJournal {
        version: 1,
        revision: 1,
        plan: MaterializationPlan {
            operation_id: acyclic_fs::OperationId::from_bytes([7; 16]),
            intent: MaterializationIntent::NativePreparation {
                selection: Vec::new(),
                original,
            },
            edits,
        },
        root_identity: None,
        preparation_identity: None,
        preparation_progress: None,
        work: WorkCounters::default(),
        preimages: Vec::new(),
        phase: MaterializationPhase::Preparing,
        applied: 0,
        restored: 0,
        custody: None,
    }
}

proptest! {
    // Property: the closed journal bound covers the real decoder's peak heap,
    // including rejected/truncated records; both JSON encodings round-trip.
    #[test]
    fn journal_decode_bound_and_wire_round_trip(
        original in prop::collection::vec(any::<u8>(), 0..512),
        paths in prop::collection::vec(".{0,32}", 0..16),
        images in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..64), 0..16),
        providers in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..64), 0..16),
        native in any::<bool>(),
        counter in any::<u64>(),
        cut in any::<usize>(),
    ) {
        let edits = paths.into_iter().enumerate().map(|(index, path)| match index % 4 {
            0 => MaterializationEdit::Install { path, image: original.clone() },
            1 => MaterializationEdit::Remove { path },
            2 => MaterializationEdit::Rename { from: path.clone(), to: path },
            _ => MaterializationEdit::SetMetadata { path, image: original.clone() },
        }).collect();
        let mut journal = journal(original, edits);
        if let MaterializationIntent::NativePreparation { selection, .. } = &mut journal.plan.intent {
            *selection = providers.into_iter().map(|provider_identity| acyclic_fs::MaterializationPreparationSelection {
                provider_identity,
                volume: acyclic_fs::VolumeId::from_bytes([1; 16]),
                generation: acyclic_fs::GenerationId::new(acyclic_fs::Digest::from_bytes([2; 32])),
            }).collect();
        }
        if !native {
            journal.plan.intent = MaterializationIntent::GenerationTransition {
                from: acyclic_fs::GenerationId::new(acyclic_fs::Digest::from_bytes([3; 32])),
                to: acyclic_fs::GenerationId::new(acyclic_fs::Digest::from_bytes([4; 32])),
            };
        }
        journal.revision = counter;
        let mut counters = serde_json::to_value(WorkCounters::default()).unwrap();
        for value in counters.as_object_mut().unwrap().values_mut() { *value = counter.into(); }
        journal.work = serde_json::from_value(counters).unwrap();
        journal.preimages = images.into_iter().map(|image| MaterializationPreimage { image, custody: None }).collect();
        let compact = serde_json::to_vec(&super::journal_encoding::Canonical(&journal)).unwrap();
        let counters = journal.work;
        journal.work = WorkCounters::UNBOUNDED;
        journal.revision = u64::MAX;
        let maximum = serde_json::to_vec(&journal).unwrap().len();
        journal.work = counters;
        journal.revision = counter;
        let mut buffer = vec![0; maximum];
        let mut output = std::io::Cursor::new(buffer.as_mut_slice());
        let mut encoded = Ok(());
        let allocations = allocation_counter::measure(|| {
            encoded = NativePreparationCompletionTemplate::write_journal(&journal, &mut output);
        });
        prop_assert!(encoded.is_ok());
        prop_assert_eq!(allocations.count_total, 0);
        prop_assert_eq!(&output.get_ref()[..output.position() as usize], &compact);
        super::journal_verification::verify_decode(&compact);
        super::journal_verification::verify_decode(&compact[..cut % (compact.len() + 1)]);
    }
}

#[test]
fn opaque_journal_bytes_fit_the_existing_one_megabyte_ledger() -> Result<()> {
    let expected = journal(vec![255; 4096], Vec::new());
    let work = NativeMaterializationWork::new(
        WorkBudget {
            peak_allocation_bytes: 1_000_000,
            ..acyclic_fs::performance::work_budget(1_000_000, 1_000_000)
        },
        &acyclic_fs::CancellationToken::new(),
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = PreparationIo::from_native_work(&work)?;
    let bytes = io.encode(&expected, acyclic_stream::MAX_RECORD_BYTES as u64, false)?;
    let (actual, custody) = crate::filesystem::native_decoded(io.decode(
        std::marker::PhantomData::<MaterializationJournal>,
        &bytes,
        crate::wire::ErrorCode::Storage,
    )?)?;
    assert_eq!(actual, expected);
    assert!(custody.work().shares_allocation_context(&work));
    assert!(work.receipt().peak_allocation_bytes <= 1_000_000);
    Ok(())
}
