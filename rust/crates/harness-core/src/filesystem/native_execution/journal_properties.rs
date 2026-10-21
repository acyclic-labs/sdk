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
        root_identity: Some([1; 16]),
        preparation_identity: Some(acyclic_fs::MaterializationPreparationIdentity {
            operation: [2; 16],
            target: [3; 16],
            backup: [4; 16],
        }),
        preparation_progress: Some(acyclic_fs::MaterializationPreparationProgress {
            parent: [5; 16],
            pending: false,
        }),
        work: WorkCounters::default(),
        preimages: Vec::new(),
        phase: MaterializationPhase::Preparing,
        applied: 0,
        restored: 0,
        custody: None,
    }
}

proptest! {
    // Property: the real container count funds every successful decoder output,
    // and error cleanup releases the original ledger's complete reservation.
    #[test]
    fn bounded_journal_decoder_funds_containers_instead_of_opaque_bytes(
        original in prop::collection::vec(any::<u8>(), 0..128),
        paths in prop::collection::vec(".{0,16}", 0..4),
        cut in any::<usize>(),
    ) {
        let edits = paths.into_iter().map(|path| MaterializationEdit::Install { path, image: original.clone() }).collect();
        let encoded = serde_json::to_vec(&journal(original, edits)).unwrap();
        for input in [encoded.as_slice(), &encoded[..cut % (encoded.len() + 1)]] {
            let work = NativeMaterializationWork::new(
                acyclic_fs::performance::work_budget(1_000_000, 64_000_000),
                &acyclic_fs::CancellationToken::new(),
            ).unwrap();
            let baseline = work.live_allocation_bytes();
            let mut decoded = Ok(());
            let measured = acyclic_test_support::measure(|| {
                decoded = super::preparation_record::fuzz_native_preparation_journal(input, &work);
            });
            prop_assert!(measured.bytes_max as u64 <= work.receipt().peak_allocation_bytes);
            prop_assert_eq!(decoded.is_ok(), serde_json::from_slice::<MaterializationJournal>(input).is_ok());
            drop(decoded);
            prop_assert_eq!(work.live_allocation_bytes(), baseline);
        }
    }

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
        journal.phase = match counter % 7 {
            0 => MaterializationPhase::Preparing,
            1 => MaterializationPhase::Prepared,
            2 => MaterializationPhase::Applying,
            3 => MaterializationPhase::Applied,
            4 => MaterializationPhase::RollingBack,
            5 => MaterializationPhase::RolledBack,
            _ => MaterializationPhase::Preparing,
        };
        let mut counters = serde_json::to_value(WorkCounters::default()).unwrap();
        for value in counters.as_object_mut().unwrap().values_mut() { *value = counter.into(); }
        journal.work = serde_json::from_value(counters).unwrap();
        journal.preimages = images.into_iter().map(|image| MaterializationPreimage { image, custody: None }).collect();
        let compact = serde_json::to_vec(&super::journal_encoding::Canonical(&journal)).unwrap();
        let bounds = crate::executor::json::JsonBounds {
            maximum_bytes: compact.len() as u64, maximum_items: 100_000, maximum_depth: 128,
        };
        let mut copied = Vec::with_capacity(compact.len());
        let measured = acyclic_test_support::measure(|| {
            crate::executor::json::json_write_bounded(&super::journal_encoding::Canonical(&journal), &mut copied, bounds, &mut |_| Ok(())).unwrap();
        });
        prop_assert_eq!(measured.count_total, 0);
        prop_assert_eq!(&copied, &compact);
        let mut identity = Ok(None);
        let measured = acyclic_test_support::measure(|| {
            identity = crate::executor::json::json_digest_bounded(
                &super::journal_encoding::Canonical(&journal),
                None,
                bounds,
                &mut |_| Ok(()),
            );
        });
        prop_assert_eq!(measured.count_total, 0);
        prop_assert_eq!(identity.unwrap(), Some((compact.len() as u64, *blake3::hash(&compact).as_bytes())));
        let mut changed = compact.clone();
        changed[0] ^= 1;
        for expected in [compact.as_slice(), &compact[..compact.len() / 2], changed.as_slice()] {
            let mut matched = Ok(None);
            let measured = acyclic_test_support::measure(|| {
                matched = crate::executor::json::json_digest_bounded(
                    &super::journal_encoding::Canonical(&journal), Some(expected), bounds, &mut |_| Ok(()),
                );
            });
            prop_assert_eq!(measured.count_total, 0);
            prop_assert_eq!(matched.unwrap(), (expected == compact).then(|| (compact.len() as u64, *blake3::hash(&compact).as_bytes())));
        }
        let counters = journal.work;
        journal.revision = counter.min(u64::MAX - 1);
        let current = serde_json::to_vec(&super::journal_encoding::Canonical(&journal)).unwrap();
        let capacity = super::journal_encoding::completion_capacity(&journal, current.len()).unwrap();
        journal.revision += 1;
        journal.work = WorkCounters::UNBOUNDED;
        prop_assert_eq!(capacity, serde_json::to_vec(&journal).unwrap().len());
        journal.work = WorkCounters::UNBOUNDED;
        journal.revision = u64::MAX;
        let maximum = serde_json::to_vec(&journal).unwrap().len();
        journal.work = counters;
        journal.revision = counter;
        journal.work = WorkCounters::UNBOUNDED;
        let mut completed = serde_json::to_vec(&super::journal_encoding::Canonical(&journal)).unwrap();
        let suffix = serde_json::to_vec(&super::journal_encoding::Canonical(&journal.work)).unwrap();
        let offset = completed.len() - suffix.len() - 1;
        journal.work = counters;
        let mut tail = std::io::Cursor::new(completed.as_mut_slice());
        tail.set_position(offset as u64);
        let mut result = Ok(());
        let measured = acyclic_test_support::measure(|| {
            result = NativePreparationCompletionTemplate::write_completed_work(&journal.work, &mut tail);
        });
        prop_assert!(result.is_ok());
        prop_assert_eq!(measured.count_total, 0);
        prop_assert_eq!(&tail.get_ref()[..tail.position() as usize], &compact);
        let mut buffer = vec![0; maximum];
        let mut output = std::io::Cursor::new(buffer.as_mut_slice());
        let mut encoded = Ok(());
        let allocations = acyclic_test_support::measure(|| {
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
    let bytes = io.encode(
        &super::journal_encoding::Canonical(&expected),
        acyclic_stream::MAX_RECORD_BYTES as u64,
        false,
    )?;
    let before = work.receipt();
    let live = work.live_allocation_bytes();
    let (length, digest) = io
        .digest(
            &super::journal_encoding::Canonical(&expected),
            acyclic_stream::MAX_RECORD_BYTES as u64,
            None,
        )?
        .unwrap();
    assert_eq!(length, bytes.len() as u64);
    assert_eq!(&digest, blake3::hash(&bytes).as_bytes());
    assert_eq!(work.receipt().bytes_encoded - before.bytes_encoded, length);
    assert_eq!(work.receipt().bytes_hashed - before.bytes_hashed, length);
    assert_eq!(work.live_allocation_bytes(), live);
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
