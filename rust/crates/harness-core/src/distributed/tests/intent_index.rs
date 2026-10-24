use super::*;
use proptest::prelude::*;

thread_local! {
    static RESTORE_SCAN: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static COPY_BEFORE_ADMISSION: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static OMIT_INDEX_LOOKUP: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static REFRESH_SCHEDULER: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}
pub(in crate::distributed) fn restore_scan() -> bool {
    RESTORE_SCAN.get()
}
pub(in crate::distributed) fn copy_before_admission() -> bool {
    COPY_BEFORE_ADMISSION.get()
}
pub(in crate::distributed) fn omit_index_lookup() -> bool {
    OMIT_INDEX_LOOKUP.get()
}
pub(in crate::distributed) fn refresh_scheduler() -> bool {
    REFRESH_SCHEDULER.get()
}
#[cfg(all(acyclic_harness_loom, acyclic_loom))]
pub(super) fn set_omit_index_lookup(value: bool) {
    OMIT_INDEX_LOOKUP.set(value);
}

fn assert_index_matches_map(keys: &[String]) {
    let mut index = super::super::intent_index::IntentIndex::default();
    let mut oracle = BTreeMap::new();
    for (sequence, key) in keys.iter().enumerate() {
        assert_eq!(index.get(key), oracle.get(key).copied());
        let mut work = None;
        let mut quota = CodecQuota::new(&mut work, 0).unwrap();
        if oracle.contains_key(key) {
            assert!(
                index
                    .insert_with(key, sequence as u64, &mut quota, |_| Ok(()))
                    .is_err()
            );
        } else {
            index
                .insert_with(key, sequence as u64, &mut quota, |_| Ok(()))
                .unwrap();
            oracle.insert(key.clone(), sequence as u64);
        }
        for (key, expected) in &oracle {
            assert_eq!(index.get(key), Some(*expected));
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]
    #[test]
    fn compressed_index_matches_independent_map(
        keys in prop::collection::vec(
            prop::collection::vec(any::<char>(), 0..40)
                .prop_map(|chars| chars.into_iter().collect::<String>()),
            0..96,
        )
    ) {
        assert_index_matches_map(&keys);
    }
}

#[test]
#[should_panic(expected = "assertion `left == right` failed")]
fn omitted_lookup_fails_the_same_independent_map_property() {
    struct Restore;
    impl Drop for Restore {
        fn drop(&mut self) {
            OMIT_INDEX_LOOKUP.set(false);
        }
    }
    let _restore = Restore;
    OMIT_INDEX_LOOKUP.set(true);
    assert_index_matches_map(&["".into(), "é".into(), "éx".into()]);
}

#[test]
fn index_insert_allocations_are_exact_and_aborted_preparation_is_invisible() -> Result<()> {
    let mut costs = Vec::new();
    for count in [8_u8, 24] {
        let mut index = super::super::intent_index::IntentIndex::default();
        let mut work = None;
        let mut quota = CodecQuota::new(&mut work, 0)?;
        for value in 0..count {
            let key = format!("history-{value:02}");
            index.insert_with(&key, u64::from(value), &mut quota, |_| Ok(()))?;
        }
        assert!(
            index
                .insert_with("aborted", 100, &mut quota, |_| Err::<(), _>(
                    Error::Storage("aborted".into())
                ))
                .is_err()
        );
        assert_eq!(index.get("aborted"), None);
        let mut inserted = None;
        let cost = acyclic_test_support::measure(|| {
            inserted = Some(index.insert_with("target", 101, &mut quota, |_| Ok(())));
        });
        inserted.unwrap()?;
        assert_eq!(index.get("target"), Some(101));
        costs.push((cost.count_total, cost.bytes_total));
    }
    assert_eq!(costs[0], costs[1]);
    assert_eq!(costs[0].0, 3);
    Ok(())
}

async fn seeded(count: u8) -> Result<(Arc<MemoryStream>, DistributedCoordinator<MemoryStream>)> {
    let client = Arc::new(MemoryStream::default());
    let stream = Stream::new(Arc::clone(&client), COORDINATOR_PATH)?;
    for value in 0..count {
        let operation = OperationId::from_bytes([value + 1; 16]);
        let key = format!("history-{value:02}");
        let event = SchedulerEvent::Declared {
            spec: Box::new(spec(operation, 0)?),
        };
        let canonical = crate::contract::canonical_json_bytes(&event)?;
        let bytes = encode(
            u64::from(value) + 1,
            operation,
            &key,
            *blake3::hash(&canonical).as_bytes(),
            canonical,
            u64::from(value) + 1,
            &mut None,
        )?;
        stream
            .append_batch(
                vec![OwnedBytes::from(bytes).into()],
                Some(u64::from(value)),
                Some(StreamIdempotencyKey::new(OwnedBytes::from(
                    key.as_bytes().to_vec(),
                ))?),
            )
            .await?;
    }
    let coordinator = open(&client).await?;
    Ok((client, coordinator))
}

fn canonical_intent(
    intent: Option<([u8; 32], SchedulerEvent, u64, acyclic_stream::CommitId)>,
) -> Result<Option<([u8; 32], Vec<u8>, u64, acyclic_stream::CommitId)>> {
    intent
        .map(|(digest, event, revision, commit)| {
            Ok((
                digest,
                crate::contract::canonical_json_bytes(&event)?,
                revision,
                commit,
            ))
        })
        .transpose()
}

#[test]
fn indexed_intents_equal_independent_full_replay_and_survive_reopening() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for count in [8_u8, 24] {
        let (baseline, _) = runtime.block_on(seeded(count))?;
        let (candidate, coordinator) = runtime.block_on(seeded(count))?;
        let reopened = runtime.block_on(open(&candidate))?;
        for key in ["history-00", "history-07", "absent"] {
            let (after, costs) = acyclic_test_support::differential(
                || {
                    runtime
                        .block_on(read_intent(
                            &baseline,
                            key,
                            Some(coordinator.revision),
                            &mut None,
                        ))
                        .and_then(canonical_intent)
                        .map_err(|error| format!("{error:?}"))
                },
                || {
                    runtime
                        .block_on(coordinator.read_indexed_intent(key, &mut None))
                        .and_then(canonical_intent)
                        .map_err(|error| format!("{error:?}"))
                },
            );
            eprintln!(
                "retry pair K={count} key={key}: old={:?} new={:?}",
                costs[0], costs[1]
            );
            let restored =
                canonical_intent(runtime.block_on(reopened.read_indexed_intent(key, &mut None))?)?;
            assert_eq!(restored, after.map_err(Error::Storage)?);
        }
    }
    Ok(())
}

#[tokio::test]
async fn indexed_declarations_preserve_full_bytes_and_exact_errors() -> Result<()> {
    let (owner, issuer) = owner_issuer();
    let scope = grant(&issuer, "observe", &["operation:observe"])?;
    let denied = issuer.issue("denied", &Capabilities::default(), None, None, &mut None)?;
    let verifier = issuer.verifier(&mut None)?;
    let foreign = Authority {
        kind: AggregateKind::Task,
        id: "foreign".into(),
    };
    let foreign_issuer = AuthorityIssuer::new("foreign-observer", [7; 32], foreign.clone());
    let foreign_scope = grant(&foreign_issuer, "observe", &["operation:observe"])?;
    let foreign_verifier = foreign_issuer.verifier(&mut None)?;
    for count in [8_u8, 24] {
        let (baseline, mut old) = seeded(count).await?;
        let (_, mut candidate) = seeded(count).await?;
        let operation = OperationId::from_bytes([1; 16]);
        for coordinator in [&mut old, &mut candidate] {
            publish(
                coordinator,
                "not-a-declaration",
                SchedulerEvent::CancellationRequested {
                    operation_id: operation,
                    recursive: false,
                },
            )
            .await?;
        }
        for (owner, scope, verifier) in [
            (&owner, &scope, &verifier),
            (&owner, &denied, &verifier),
            (&foreign, &foreign_scope, &foreign_verifier),
        ] {
            for operation in [operation, OperationId::from_bytes([99; 16])] {
                for key in ["history-00", "history-07", "absent", "not-a-declaration"] {
                    let key = IdempotencyKey::new(key)?;
                    let before = observe_declaration(
                        &baseline, owner, scope, verifier, operation, &key, &mut None,
                    )
                    .await
                    .and_then(|value| crate::contract::canonical_json_bytes(&value))
                    .map_err(|error| format!("{error:?}"));
                    let after = candidate
                        .observe_declaration(owner, scope, verifier, operation, &key, &mut None)
                        .await
                        .and_then(|value| crate::contract::canonical_json_bytes(&value))
                        .map_err(|error| format!("{error:?}"));
                    assert_eq!(before, after);
                }
            }
        }
    }
    Ok(())
}

async fn assert_declaration_tail_equivalence() -> Result<()> {
    let (owner, issuer) = owner_issuer();
    let scope = grant(&issuer, "observe", &["operation:observe"])?;
    let verifier = issuer.verifier(&mut None)?;
    for kind in 0..5 {
        let (baseline, _) = seeded(8).await?;
        let (client, mut candidate) = seeded(8).await?;
        let operation = OperationId::from_bytes([99; 16]);
        let key = if kind == 1 || kind == 3 {
            "history-07"
        } else {
            "new"
        };
        let payload = if kind < 2 {
            b"!".to_vec()
        } else {
            crate::contract::canonical_json_bytes(&SchedulerEvent::Declared {
                spec: Box::new(spec(operation, 0)?),
            })?
        };
        let bytes = if kind == 4 {
            vec![0xff]
        } else {
            encode(
                9,
                operation,
                key,
                *blake3::hash(&payload).as_bytes(),
                payload,
                9,
                &mut None,
            )?
        };
        for client in [&baseline, &client] {
            Stream::new(Arc::clone(client), COORDINATOR_PATH)?
                .append_batch(vec![OwnedBytes::from(bytes.clone()).into()], Some(8), None)
                .await?;
        }
        for (operation, key) in [
            (OperationId::from_bytes([1; 16]), "history-00"),
            (OperationId::from_bytes([8; 16]), "history-07"),
            (operation, "new"),
        ] {
            let key = IdempotencyKey::new(key)?;
            let before = observe_declaration(
                &baseline, &owner, &scope, &verifier, operation, &key, &mut None,
            )
            .await
            .and_then(|value| crate::contract::canonical_json_bytes(&value))
            .map_err(|error| format!("{error:?}"));
            let after = candidate
                .observe_declaration(&owner, &scope, &verifier, operation, &key, &mut None)
                .await
                .and_then(|value| crate::contract::canonical_json_bytes(&value))
                .map_err(|error| format!("{error:?}"));
            assert_eq!(before, after, "tail kind {kind}");
            assert_eq!(candidate.revision, 8);
        }
    }
    Ok(())
}

#[tokio::test]
async fn declaration_catalog_preserves_observation_of_unapplied_and_anomalous_tails() -> Result<()>
{
    assert_declaration_tail_equivalence().await
}

#[tokio::test]
#[should_panic(expected = "tail kind 0")]
async fn full_scheduler_refresh_fails_the_same_observation_equivalence() {
    struct Restore;
    impl Drop for Restore {
        fn drop(&mut self) {
            REFRESH_SCHEDULER.set(false);
        }
    }
    let _restore = Restore;
    REFRESH_SCHEDULER.set(true);
    assert_declaration_tail_equivalence().await.unwrap();
}

#[test]
fn absent_intent_does_not_allocate_or_read_history() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for count in [8_u8, 24] {
        let (_, coordinator) = runtime.block_on(seeded(count))?;
        let cost = acyclic_test_support::measure(|| {
            assert!(
                runtime
                    .block_on(coordinator.read_indexed_intent("absent", &mut None))
                    .unwrap()
                    .is_none()
            );
        });
        assert_eq!(cost.count_total, 0);
        assert_eq!(cost.bytes_total, 0);
    }
    Ok(())
}

#[test]
fn existing_intent_has_exact_equal_cost_at_eight_and_twenty_four() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut costs = Vec::new();
    for count in [8_u8, 24] {
        let (_, coordinator) = runtime.block_on(seeded(count))?;
        let mut result = None;
        let cost = acyclic_test_support::measure(|| {
            result =
                Some(runtime.block_on(coordinator.read_indexed_intent("history-00", &mut None)));
        });
        assert!(result.unwrap()?.is_some());
        eprintln!("indexed retry K={count}: {cost:?}");
        costs.push(acyclic_test_support::differential::allocation_counts(cost));
    }
    assert_eq!(costs[0], costs[1]);
    Ok(())
}

#[test]
#[should_panic(expected = "assertion `left == right` failed")]
fn restored_scan_fails_the_same_existing_intent_cost_pin() {
    struct Restore;
    impl Drop for Restore {
        fn drop(&mut self) {
            RESTORE_SCAN.set(false);
        }
    }
    let _restore = Restore;
    RESTORE_SCAN.set(true);
    existing_intent_has_exact_equal_cost_at_eight_and_twenty_four().unwrap();
}

#[test]
#[should_panic(expected = "assertion `left == right` failed")]
fn restored_full_journal_scan_fails_the_same_absent_intent_pin() {
    struct Restore;
    impl Drop for Restore {
        fn drop(&mut self) {
            RESTORE_SCAN.set(false);
        }
    }
    let _restore = Restore;
    RESTORE_SCAN.set(true);
    absent_intent_does_not_allocate_or_read_history().unwrap();
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn index_native_custody_outlives_its_preparation_and_retires_with_the_tree() -> Result<()> {
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(1024 * 1024, 1024 * 1024),
        &cancellation,
    )
    .expect("original ledger");
    let mut observer =
        crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(&work)?);
    let baseline = work.live_allocation_bytes();
    let mut index = super::super::intent_index::IntentIndex::default();
    {
        let mut work = Some(&mut observer as &mut dyn HistoryIo);
        let mut quota = CodecQuota::new(&mut work, 0)?;
        for (sequence, key) in ["a", "ab", "abc", "b", "é", "éx"].into_iter().enumerate() {
            index.insert_with(key, sequence as u64, &mut quota, |_| Ok(()))?;
        }
        assert_eq!(quota.retained(), 0);
    }
    assert_eq!(observer.into_custody()?.bytes(), 0);
    assert_eq!(index.get("éx"), Some(5));
    drop(index);
    // All native allocations must have returned to this original owner.
    assert_eq!(work.live_allocation_bytes(), baseline);
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn denied_index_claim_does_not_allocate_or_change_the_tree() -> Result<()> {
    let cancellation = acyclic_fs::CancellationToken::new();
    let mut budget = acyclic_fs::performance::work_budget(1024 * 1024, 1024 * 1024);
    // The ledger and the leaf are admitted; the key's claim must be denied.
    budget.allocation_operations = 2;
    let original =
        acyclic_fs::NativeMaterializationWork::new(budget, &cancellation).expect("original ledger");
    let mut observer = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&original)?,
    );
    let baseline = original.live_allocation_bytes();
    let mut index = super::super::intent_index::IntentIndex::default();
    let mut work = Some(&mut observer as &mut dyn HistoryIo);
    let mut quota = CodecQuota::new(&mut work, 0)?;
    let mut rejected = None;
    let cost = acyclic_test_support::measure(|| {
        rejected = Some(index.insert_with("denied", 0, &mut quota, |_| Ok(())));
    });
    assert!(rejected.unwrap().is_err());
    assert_eq!(index.get("denied"), None);
    assert_eq!(cost.count_total, 0);
    assert_eq!(cost.bytes_total, 0);
    drop(quota);
    assert_eq!(original.live_allocation_bytes(), baseline);
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
#[should_panic(expected = "assertion `left == right` failed")]
fn copying_before_admission_fails_the_same_denied_claim_pin() {
    struct Restore;
    impl Drop for Restore {
        fn drop(&mut self) {
            COPY_BEFORE_ADMISSION.set(false);
        }
    }
    let _restore = Restore;
    COPY_BEFORE_ADMISSION.set(true);
    denied_index_claim_does_not_allocate_or_change_the_tree().unwrap();
}
