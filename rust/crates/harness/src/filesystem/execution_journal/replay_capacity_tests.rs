use super::*;
use crate::{AgentId, conversation::VolumeOwner, core::AuthorityIssuer, resources::ProviderRef};
use proptest::prelude::*;

type Journal = FilesystemExecutionJournal<
    crate::test_stream::LostSessionAck,
    acyclic_fs::MemoryAuthorityBackend,
    acyclic_fs::MemoryObjectBackend,
>;

async fn journal() -> Result<Journal> {
    let provider = ProviderRef::new("replay".into(), "filesystem".into(), "2".into(), &mut None)?;
    let host = FilesystemHost::new(acyclic_fs::Fs::memory(), &provider, None)?;
    let agent = AgentId::from_bytes([1; 16]);
    let volume = VolumeRef::new(
        provider,
        "replay".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    host.create_volume(&volume).await?;
    let issuer = AuthorityIssuer::new(
        "replay",
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
        Arc::new(crate::test_stream::LostSessionAck::default()),
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

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // Exercise production replay: empty/tail pages reserve no slots, and dense
    // pages reserve exactly their returned count, independent of the page cap.
    #[test]
    fn replay_storage_follows_returned_records(
        count in 0usize..140, offset in 0usize..140, maximum in 1u32..257,
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let journal = journal().await.unwrap();
            let operation = OperationId::from_bytes([4; 16]);
            let event = ExecutionEvent::Started { request_digest: [9; 32] };
            if count != 0 {
                let records = (0..count).map(|index| {
                    serde_json::to_vec(&Observation {
                        version: OBSERVATION_VERSION, operation_id: operation,
                        turn_operation: operation, retry_digest: format!("retry-{index}"),
                        event: event.clone(),
                    }).unwrap().into()
                }).collect();
                journal.path(operation).unwrap().append_batch(records, Some(0), None).await.unwrap();
            }
            let bound = (maximum as usize).min(EXECUTION_REPLAY_PAGE_RECORDS as usize);
            for after in [0, count, offset.min(count)] {
                let expected = (count - after).min(bound);
                let mut page = None;
                let allocation = acyclic_test_support::measure(|| {
                    page = Some(futures::executor::block_on(journal.replay_page(
                        operation, after as u64, maximum, true, None,
                    )).unwrap());
                });
                let page = page.unwrap();
                let exact = |capacity| capacity == expected;
                assert!(exact(page.capacity()), "page storage must match the pinned records");
                assert_eq!(page.len(), expected);
                for (index, (record, model)) in page.iter().enumerate() {
                    assert_eq!(record.sequence, (after + index + 1) as u64);
                    assert_eq!(record.idempotency_key, format!("retry-{}", after + index));
                    assert_eq!(record.event, event);
                    assert!(model.is_none());
                }
                if expected < bound {
                    // Old eager reservation fails the identical capacity assertion.
                    let old: Vec<(ExecutionRecord, Option<ModelEvent>)> =
                        crate::model::vec_with_capacity(bound, None).unwrap();
                    assert!(!exact(old.capacity()));
                }
                if expected == 0 && bound > 8 {
                    assert!(allocation.bytes_total <
                        (bound * std::mem::size_of::<(ExecutionRecord, Option<ModelEvent>)>()) as u64);
                }
            }
            if count != 0 {
                for fault in [7, 8, 9] {
                    journal.stream.history_read_fault.store(fault, std::sync::atomic::Ordering::SeqCst);
                    let page = journal.replay_page(operation, 0, 1, true, None).await.unwrap();
                    assert_eq!(page.len(), 1);
                    assert_eq!(page.capacity(), 1);
                    assert_eq!(page.first().unwrap().0.event, event);
                }
                // A producer exceeding the bound must still fail; hints are
                // storage advice, never an admission or sequence authority.
                journal.stream.history_read_fault.store(2, std::sync::atomic::Ordering::SeqCst);
                assert!(matches!(journal.replay_page(operation, 0, 1, true, None).await,
                    Err(Error::Storage(_))));
            }
        });
    }
}
