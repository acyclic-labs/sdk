use super::*;
use acyclic_stream::{assert_requests, testing::Counted};
use std::sync::atomic::{AtomicUsize, Ordering};

struct PhysicalCut {
    fail_at: Option<usize>,
    events: [AtomicUsize; 3],
    crashes: AtomicUsize,
}
impl PhysicalCut {
    fn new(fail_at: Option<usize>) -> Self {
        Self {
            fail_at,
            events: std::array::from_fn(|_| AtomicUsize::new(0)),
            crashes: AtomicUsize::new(0),
        }
    }
    fn event(&self, stage: usize) -> bool {
        self.events[stage].fetch_add(1, Ordering::SeqCst);
        self.fail_at != Some(stage)
    }
    fn counts(&self) -> [usize; 3] {
        std::array::from_fn(|stage| self.events[stage].load(Ordering::SeqCst))
    }
}
impl acyclic_stream::PersistenceCut for PhysicalCut {
    fn admit(&self) -> bool {
        self.event(0)
    }
    fn written(&self) -> bool {
        self.event(1)
    }
    fn acknowledge(&self) -> bool {
        self.event(2)
    }
    fn crashed(&self, simulation: Result<(), acyclic_stream::CutSimulationError>) {
        assert!(
            simulation.is_ok(),
            "physical crash simulation failed: {simulation:?}"
        );
        self.crashes.fetch_add(1, Ordering::SeqCst);
    }
}

/// Count the real provider boundary, recover physical storage, and contrast
/// the joined publication with the independently durable split shape.
#[tokio::test]
async fn actor_state_hot_path_counts_and_reopen() -> TestResult {
    resident_work().await?;
    borrowed_read_guards().await?;
    observation_work().await?;
    let directory = tempfile::tempdir()?;
    let address = actor("hot-path")?;
    {
        let provider = Arc::new(Counted::new(
            LocalStream::open(directory.path(), Default::default()).await?,
        ));
        let store = Store::new(Arc::clone(&provider), state_root()?);
        let mut create = PreparedPublish::foreign(key("hot/create")?);
        for effect in store.create(&address, inbox(&address)?, None)? {
            create.join(effect)?;
        }
        assert_requests!(provider, store.submit(create).await, control = 0, data = 1)?;
        let created =
            assert_requests!(provider, store.load(&address).await, control = 1, data = 0)?;
        let claimed = assert_requests!(
            provider,
            store.claim(&created, 1, key("hot/claim")?).await,
            control = 0,
            data = 1
        )?;
        let physical = Arc::new(PhysicalCut::new(None));
        provider.inner().cut_persistence(physical.clone());
        let mut binding = store.bind_state(claimed, 1)?;
        let before = provider.requests();
        let value = Bytes::from(vec![7; 128]);
        let retained_value = value.clone();
        binding.transaction(|state| {
            state.put("value".into(), value);
            state.append_journal(0, vec![Bytes::from_static(b"journal")])?;
            state.alarm(Some(42))
        })?;
        assert!(binding.read_journal(0, 1).await?.is_empty());
        let mut update = transition(7, AlarmChange::Keep);
        update.checkpoint.bytes = Bytes::from(vec![7; 128]);
        let journal_path = StreamPath::new(format!("{}/journal", store.path(&address)?))?;
        let mut prepared = binding.prepare_publish(&record(0), None, update, key("hot/joined")?)?;
        assert!(matches!(
            prepared.join(Effect {
                conditions: Vec::new(),
                mutation: CommitMutation::Append {
                    path: journal_path.clone(),
                    records: vec![Bytes::from_static(b"forged").into()],
                },
            }),
            Err(Error::Invalid("effect changes the host's actor journal"))
        ));
        let snapshot = store
            .submit_retained(prepared)
            .await?
            .ok_or("missing joined receipt")?;
        assert_eq!(provider.requests().since(before).data(), 1);
        assert_eq!(provider.requests().since(before).control(), 0);
        assert_eq!(
            physical.counts(),
            [1, 1, 1],
            "one frame and one durable acknowledgement"
        );

        let mut frames = provider
            .read(ReadRequest {
                path: journal_path,
                from: 0,
                limit: 1,
                key: None,
                at_least: Default::default(),
            })
            .await?;
        let frame = frames.try_next().await?.ok_or("missing joined journal")?;
        assert_eq!(Some(frame.commit_id), snapshot.anchor);
        let state = snapshot.state().ok_or("missing joined state")?;
        assert_eq!(
            state
                .metadata()
                .next()
                .ok_or("missing joined value")?
                .1
                .as_ptr(),
            retained_value.as_ptr(),
            "receipt retention must not decode the authored bytes again"
        );
        assert_eq!(state.cursor("inbox"), Some(1));
        assert_eq!(
            state.checkpoint().map(|value| value.bytes.as_ref()),
            Some([7; 128].as_slice())
        );
        let mut binding = store.bind_state(snapshot, 1)?;
        assert_requests!(
            provider,
            async {
                binding.transaction(|state| {
                    assert_eq!(state.get("value").map(Bytes::len), Some(128));
                    Ok(())
                })?;
                binding.flush(key("hot/read")?).await
            }
            .await,
            control = 0,
            data = 0
        )?;
        assert_eq!(
            physical.counts(),
            [1, 1, 1],
            "warm read has no persistence events"
        );

        // Negative control: an independently durable flush followed by the
        // checkpoint necessarily spends two provider writes.
        let before = provider.requests();
        binding.transaction(|state| {
            state.put("value".into(), Bytes::from(vec![8; 128]));
            Ok(())
        })?;
        binding.flush(key("hot/split-state")?).await?;
        let snapshot = binding.finish()?;
        store
            .publish(
                &snapshot,
                1,
                &record(1),
                None,
                transition(8, AlarmChange::Keep),
                key("hot/split-checkpoint")?,
            )
            .await?;
        assert_eq!(provider.requests().since(before).data(), 2);
        assert_eq!(provider.requests().since(before).control(), 0);
        assert_eq!(
            physical.counts(),
            [3, 3, 3],
            "split publication adds two durable frames"
        );
    }
    let store = Store::new(
        Arc::new(LocalStream::open(directory.path(), Default::default()).await?),
        state_root()?,
    );
    let snapshot = store.load(&address).await?;
    let state = snapshot.state().ok_or("missing recovered state")?;
    assert_eq!(state.cursor("inbox"), Some(2));
    assert_eq!(
        state.checkpoint().map(|value| value.bytes.as_ref()),
        Some([8].as_slice())
    );
    assert_eq!(
        state.metadata().next().map(|(_, value)| value.as_ref()),
        Some([8; 128].as_slice())
    );
    assert_eq!(state.alarm().pending(), Some(42));
    let binding = store.bind_state(snapshot, 1)?;
    assert_eq!(
        binding.read_journal(0, 1).await?.first().map(AsRef::as_ref),
        Some(b"journal".as_slice())
    );
    alarm_replacement().await?;
    alarm_publication_wakes().await?;
    physical_loss().await?;
    Ok(())
}

async fn observation_work() -> TestResult {
    use futures::FutureExt as _;

    for population in [1_usize, 1_000] {
        let provider = Arc::new(Counted::new(MemoryStream::default()));
        let store = Store::new(provider.clone(), state_root()?);
        let address = actor("observed")?;
        let absent = store.load(&address).await?;
        assert!(store.observe(&absent).await?.is_none());
        place(&store, &address).await?;
        let created = store.observe(&absent).await?.ok_or("unobserved creation")?;
        let mut binding = store.bind_state(created, 1)?;
        binding.transaction(|state| {
            for index in 0..population {
                state.put(format!("key-{index:06}"), Bytes::from_static(b"resident"));
            }
            Ok(())
        })?;
        binding.flush(key("observe/seed")?).await?;
        let snapshot = binding.finish()?;
        let before = provider.requests();
        // With no competing work, MemoryStream completes in one poll. Both
        // paths pay the same real head observation; only the control decodes.
        let head = acyclic_test_support::measure(|| {
            provider
                .head(snapshot.path().clone())
                .now_or_never()
                .unwrap()
                .unwrap();
        });
        let observed = acyclic_test_support::measure(|| {
            assert!(
                store
                    .observe(&snapshot)
                    .now_or_never()
                    .unwrap()
                    .unwrap()
                    .is_none()
            );
        });
        // Negative control executes the old unconditional Actor state decode.
        let loaded = acyclic_test_support::measure(|| {
            store.load(&address).now_or_never().unwrap().unwrap();
        });
        assert_eq!(observed.count_total, head.count_total);
        assert!(loaded.count_total >= observed.count_total + u64::try_from(population)?);
        assert_eq!(provider.requests().since(before).control(), 3);
        assert_eq!(provider.requests().since(before).data(), 0);
        println!(
            "observed_metadata={population} head_allocations={} observe_allocations={} load_allocations={}",
            head.count_total, observed.count_total, loaded.count_total
        );
        store
            .set_paused(&snapshot, 1, true, key("observe/pause")?)
            .await?;
        let paused = assert_requests!(
            provider,
            store.observe(&snapshot).await,
            control = 1,
            data = 0
        )?
        .ok_or("unobserved pause")?;
        assert!(paused.state().ok_or("missing paused state")?.paused());
        let claimed = store.claim(&paused, 2, key("observe/claim")?).await?;
        let replaced = store.observe(&paused).await?.ok_or("unobserved takeover")?;
        assert_eq!(replaced.state(), claimed.state());
        assert!(store.observe(&claimed).await?.is_none());
    }
    Ok(())
}

async fn physical_loss() -> TestResult {
    for fail_at in 0..3 {
        let directory = tempfile::tempdir()?;
        let address = actor("power-loss")?;
        let physical = Arc::new(PhysicalCut::new(Some(fail_at)));
        {
            let provider = Arc::new(LocalStream::open(directory.path(), Default::default()).await?);
            let store = Store::new(provider.clone(), state_root()?);
            place(&store, &address).await?;
            provider.cut_persistence(physical.clone());
            let mut binding = store.bind_state(store.load(&address).await?, 1)?;
            binding.transaction(|state| {
                state.put("value".into(), Bytes::from_static(b"durable"));
                state.append_journal(0, vec![Bytes::from_static(b"durable")])?;
                state.alarm(Some(42))
            })?;
            let prepared = binding.prepare_publish(
                &record(0),
                None,
                transition(7, AlarmChange::Keep),
                key("loss/publication")?,
            )?;
            assert!(matches!(
                store.submit(prepared).await,
                Err(Error::Unknown { .. })
            ));
            assert_eq!(physical.crashes.load(Ordering::SeqCst), 1);
        }
        let store = Store::new(
            Arc::new(LocalStream::open(directory.path(), Default::default()).await?),
            state_root()?,
        );
        let snapshot = store.load(&address).await?;
        let state = snapshot.state().ok_or("missing recovered actor")?;
        let applied = u64::from(fail_at == 2);
        assert_eq!(state.cursor("inbox"), Some(applied));
        assert_eq!(state.journal_tail(), applied);
        assert_eq!(state.metadata().count(), usize::try_from(applied)?);
        assert_eq!(state.checkpoint().is_some(), applied != 0);
        assert_eq!(state.alarm().pending(), (applied != 0).then_some(42));
        let binding = store.bind_state(snapshot, 1)?;
        assert_eq!(
            binding.read_journal(0, 1).await?.len(),
            usize::try_from(applied)?
        );
    }
    Ok(())
}

async fn resident_work() -> TestResult {
    for population in [1_usize, 100_000] {
        let provider = Arc::new(Counted::new(MemoryStream::default()));
        let store = Store::new(provider.clone(), state_root()?);
        let address = actor("resident")?;
        place(&store, &address).await?;
        let mut binding = store.bind_state(store.load(&address).await?, 1)?;
        binding.transaction(|state| {
            for index in 0..population {
                state.put(format!("key-{index:06}"), Bytes::from_static(b"untouched"));
            }
            Ok(())
        })?;
        // This oracle measures resident transactions, including pending work.
        // Publication and recovery are checked separately on persisted state;
        // a large resident map is not evidence that one state record fits.
        // Negative control copies all metadata as the former transaction did.
        let cloned = acyclic_test_support::measure(|| {
            binding
                .transaction(|state| {
                    std::hint::black_box(
                        state
                            .entries()
                            .map(|(key, value)| (key.to_owned(), value.clone()))
                            .collect::<std::collections::BTreeMap<_, _>>(),
                    );
                    Ok(())
                })
                .unwrap();
        });
        assert!(cloned.count_total >= u64::try_from(population)?);
        let before = provider.requests();
        let reads = acyclic_test_support::measure(|| {
            for _ in 0..128 {
                assert_eq!(
                    binding.get("key-000000").unwrap().map(Bytes::as_ref),
                    Some(b"untouched".as_slice())
                );
                binding
                    .transaction(|state| {
                        assert_eq!(
                            state.get("key-000000").map(Bytes::as_ref),
                            Some(b"untouched".as_slice())
                        );
                        Ok(())
                    })
                    .unwrap();
            }
        });
        assert_eq!(reads.count_total, 0, "reads must borrow the resident cut");
        assert_eq!(provider.requests().since(before).data(), 0);
        assert_eq!(provider.requests().since(before).control(), 0);

        binding.transaction(|state| {
            state.append_journal(0, vec![Bytes::from_static(b"accepted"); population])
        })?;
        let staged_tail = u64::try_from(population)?;
        for _ in 0..2 {
            let put_key = "key-000000".to_owned();
            let put = acyclic_test_support::measure(|| {
                binding
                    .transaction(|state| {
                        state.put(put_key, Bytes::from_static(b"accepted"));
                        Ok(())
                    })
                    .unwrap();
            });
            assert_eq!(
                put.count_total, 2,
                "only the touched key and undo storage allocate"
            );
        }
        rollback(&mut binding, staged_tail)?;
        binding.transaction(|state| {
            assert_eq!(state.journal_tail(), staged_tail);
            assert_eq!(state.entries().len(), population);
            Ok(())
        })?;
        assert_eq!(provider.requests().since(before).data(), 0);
        assert_eq!(provider.requests().since(before).control(), 0);
        println!(
            "metadata={population} read_allocations={} clone_allocations={}",
            reads.count_total, cloned.count_total
        );
    }
    Ok(())
}

async fn borrowed_read_guards() -> TestResult {
    let provider = Arc::new(Counted::new(MemoryStream::default()));
    let store = Store::new(provider.clone(), state_root()?);
    let address = actor("borrowed-read")?;
    place(&store, &address).await?;
    let snapshot = store.load(&address).await?;
    store
        .set_paused(&snapshot, 1, true, key("borrowed-read/pause")?)
        .await?;
    let paused = store.bind_state(store.load(&address).await?, 1)?;
    let before = provider.requests();
    assert!(matches!(paused.get("missing"), Err(Error::Paused)));
    assert_eq!(provider.requests().since(before).data(), 0);
    assert_eq!(provider.requests().since(before).control(), 0);
    store
        .set_paused(&paused.finish()?, 1, false, key("borrowed-read/resume")?)
        .await?;
    let resumed = store.load(&address).await?;
    let mut binding = store.bind_state(store.load(&address).await?, 1)?;
    assert!(binding.get("missing")?.is_none());
    binding.transaction(|state| {
        state.put("value".into(), Bytes::from_static(b"pending"));
        Ok(())
    })?;
    assert_eq!(
        binding.get("value")?.map(Bytes::as_ref),
        Some(b"pending".as_slice())
    );
    let refused: Result<(), Error> = binding.transaction(|state| {
        state.put("value".into(), Bytes::from_static(b"discarded"));
        Err(Error::Invalid("rollback borrowed read"))
    });
    assert!(refused.is_err());
    assert_eq!(
        binding.get("value")?.map(Bytes::as_ref),
        Some(b"pending".as_slice())
    );
    store
        .claim(&resumed, 2, key("borrowed-read/replace")?)
        .await?;
    assert!(matches!(
        binding.flush(key("borrowed-read/stale")?).await,
        Err(Error::Conflict)
    ));
    let before = provider.requests();
    assert!(matches!(binding.get("value"), Err(Error::Conflict)));
    assert_eq!(provider.requests().since(before).data(), 0);
    assert_eq!(provider.requests().since(before).control(), 0);
    Ok(())
}

fn rollback<P: StreamProvider>(
    binding: &mut super::super::ActorStateBinding<'_, P>,
    tail: u64,
) -> TestResult {
    let mutate = |state: &mut super::super::ActorStateTransaction<'_>| {
        state.put("key-000000".into(), Bytes::from_static(b"discarded"));
        state.put("new".into(), Bytes::new());
        state.put("key-000000".into(), Bytes::new());
        state.append_journal(tail, vec![Bytes::from_static(b"discarded")])?;
        state.alarm(Some(99))
    };
    let refused: Result<(), Error> = binding.transaction(|state| {
        mutate(state)?;
        Err(Error::Invalid("rollback"))
    });
    assert!(refused.is_err());
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _: Result<(), Error> = binding.transaction(|state| {
            mutate(state)?;
            panic!("rollback on unwind")
        });
    }));
    assert!(panicked.is_err());
    binding.transaction(|state| {
        assert_eq!(
            state.get("key-000000").map(Bytes::as_ref),
            Some(b"accepted".as_slice())
        );
        assert!(state.get("new").is_none());
        assert_eq!(state.journal_tail(), tail);
        Ok(())
    })?;
    Ok(())
}

async fn alarm_replacement() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(provider, state_root()?);
    let address = actor("rearmed")?;
    place(&store, &address).await?;
    let mut binding = store.bind_state(store.load(&address).await?, 1)?;
    binding.transaction(|state| {
        state.put("key-000000".into(), Bytes::from_static(b"accepted"));
        state.alarm(Some(10))
    })?;
    rollback(&mut binding, 0)?;
    binding.flush(key("alarm/first")?).await?;
    let snapshot = binding.finish()?;
    let alarm = snapshot.state().ok_or("missing state")?.alarm();
    assert_eq!(
        alarm.generation(),
        1,
        "refused callbacks retain the alarm fence"
    );
    assert_eq!(
        alarm.pending(),
        Some(10),
        "error and unwind restore the alarm"
    );
    let mut binding = store.bind_state(snapshot, 1)?;
    binding.transaction(|state| state.alarm(Some(20)))?;
    let trigger = Trigger::Alarm {
        generation: 1,
        now_unix_ms: 10,
    };
    let prepared = binding.prepare_publish(
        &trigger,
        None,
        transition(1, AlarmChange::Keep),
        key("alarm/joined")?,
    )?;
    store.submit(prepared).await?;
    let snapshot = store.load(&address).await?;
    let alarm = snapshot.state().ok_or("missing state")?.alarm();
    assert_eq!(alarm.generation(), 2);
    assert_eq!(
        alarm.pending(),
        Some(20),
        "consuming the old alarm must not consume its replacement"
    );
    assert!(matches!(
        store.prepare_publish(
            &snapshot,
            1,
            &trigger,
            None,
            transition(2, AlarmChange::Keep),
            key("alarm/stale")?
        ),
        Err(Error::Conflict)
    ));
    Ok(())
}

async fn alarm_publication_wakes() -> TestResult {
    for staged in [false, true] {
        for change in [
            AlarmChange::Keep,
            AlarmChange::Clear,
            AlarmChange::Set {
                deadline_unix_ms: 30,
            },
        ] {
            let provider = Arc::new(Counted::new(MemoryStream::default()));
            let store = Store::new(provider.clone(), state_root()?);
            let address = actor("alarm-wake")?;
            place(&store, &address).await?;
            let mut binding = store.bind_state(store.load(&address).await?, 1)?;
            binding.transaction(|state| state.alarm(Some(10)))?;
            binding.flush(key("wake/initial")?).await?;
            let mut binding = store.bind_state(binding.finish()?, 1)?;
            if staged {
                binding.transaction(|state| state.alarm(Some(20)))?;
            }
            let from = provider.tail(store.root().clone()).await?;
            let prepared = binding.prepare_publish(
                &record(0),
                None,
                transition(1, change),
                key("wake/joined")?,
            )?;
            let confirmed = assert_requests!(
                provider,
                store.submit_retained(prepared).await,
                control = 0,
                data = 1
            )?
            .ok_or("missing alarm receipt")?;
            let wakes = u64::from(staged || change != AlarmChange::Keep);
            assert_eq!(
                provider.tail(store.root().clone()).await?,
                from + wakes,
                "alarm changes in either source wake once; Keep alone never wakes"
            );
            if wakes != 0 {
                let mut records = provider
                    .read(ReadRequest {
                        path: store.root().clone(),
                        from,
                        limit: 1,
                        key: None,
                        at_least: Default::default(),
                    })
                    .await?;
                let wake = records.try_next().await?.ok_or("missing alarm wake")?;
                assert_eq!(wake.value.as_ref(), address.encode());
                assert_eq!(Some(wake.commit_id), confirmed.anchor);
                assert!(records.try_next().await?.is_none());
            }
        }
    }
    Ok(())
}
