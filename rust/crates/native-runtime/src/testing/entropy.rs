//! A scoped byte stream, shared by the original owned task descendants.
use super::*;

pub(super) struct Source {
    // Sole entropy lock. No nested locks, callbacks, wakes or awaits while held.
    reader: Mutex<blake3::OutputReader>,
}

impl Source {
    pub(super) fn new(seed: u64) -> Self {
        let mut hash = blake3::Hasher::new_derive_key("acyclic.dst.entropy.v1");
        hash.update(&seed.to_le_bytes());
        Self {
            reader: Mutex::new(hash.finalize_xof()),
        }
    }

    fn fill(&self, bytes: &mut [u8]) {
        lock(&self.reader).fill(bytes);
    }
}

/// Fills bytes from the active simulation's seeded source, returning false when
/// no source is installed. Callers then use their ordinary host entropy.
/// Task scopes and owned blocking callbacks retain the same source.
pub fn fill_seeded(bytes: &mut [u8]) -> bool {
    let source = Context::simulation().and_then(|context| context.schedule.0.entropy.clone());
    if let Some(source) = source {
        source.fill(bytes);
        true
    } else {
        false
    }
}

#[cfg(all(test, not(acyclic_native_runtime_loom)))]
mod tests {
    use super::*;

    proptest::proptest! {
        #![proptest_config(proptest::test_runner::Config::with_cases(32))]
        #[test]
        fn byte_stream_is_reproducible_and_independent_of_draw_boundaries(
            seed in proptest::prelude::any::<u64>(), split in 0usize..128,
        ) {
            let first = Source::new(seed);
            let second = Source::new(seed);
            let mut expected = [0; 128];
            let mut actual = [0; 128];
            first.fill(&mut expected);
            let (head, tail) = actual.split_at_mut(split);
            second.fill(head);
            second.fill(tail);
            proptest::prop_assert_eq!(expected, actual);
        }
    }

    #[test]
    fn ordinary_execution_does_not_install_simulation_entropy() {
        let mut bytes = [7; 32];
        assert!(!fill_seeded(&mut bytes));
        assert_eq!(bytes, [7; 32]);
    }

    #[test]
    fn entropy_mutant_preserves_seeded_polling_and_time() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        runtime.block_on(async {
            tokio::time::pause();
            let (schedule, _) = Schedule::without_entropy(71);
            assert!(schedule.0.executor.is_some());
            schedule
                .scope(async {
                    let mut bytes = [7; 32];
                    assert!(!fill_seeded(&mut bytes));
                    assert_eq!(bytes, [7; 32]);
                    assert_eq!(unix_millis(), Some(1_700_000_000_000));
                })
                .await;
            assert!(!schedule.polls().is_empty());
        });
    }

    fn scoped_sources(forget_host: bool) -> std::io::Result<()> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()?;
        let (schedule, mut events) = Schedule::seeded(73);
        runtime.block_on(async {
            tokio::time::pause();
            schedule.set_unix_millis(9876);
            let main_thread = std::thread::current().id();
            let mut expected = [0; 64];
            Source::new(73).fill(&mut expected);
            let mut future = Box::pin(schedule.scope(async {
                let mut first = [0; 16];
                assert!(fill_seeded(&mut first));
                let second = spawn(async {
                    let mut bytes = [0; 16];
                    assert!(fill_seeded(&mut bytes));
                    bytes
                })
                .await
                .unwrap();
                let third = spawn_blocking(|| {
                    let mut bytes = [0; 16];
                    assert!(fill_seeded(&mut bytes));
                    bytes
                })
                .await
                .unwrap();
                if forget_host {
                    FORGET_HOST_CONTEXT.with(|fault| fault.set(true));
                }
                let fourth = crate::run_blocking_io(
                    move || {
                        assert_ne!(
                            std::thread::current().id(),
                            main_thread,
                            "native queue was bypassed"
                        );
                        assert_eq!(
                            unix_millis(),
                            Some(9876),
                            "native worker lost the simulated clock"
                        );
                        let mut bytes = [0; 16];
                        assert!(fill_seeded(&mut bytes), "native worker lost seeded entropy");
                        bytes
                    },
                    None,
                )
                .unwrap()
                .await
                .unwrap();
                [first, second, third, fourth].concat()
            }));
            let actual = loop {
                tokio::select! {
                    actual = &mut future => break actual,
                    event = events.recv() => event.unwrap().release(),
                }
            };
            schedule.drained().await;
            assert_eq!(actual, expected);
            let mut outside = [0; 16];
            assert!(!fill_seeded(&mut outside), "source escaped its scope");
        });
        Ok(())
    }

    #[test]
    fn scoped_entropy_survives_owned_task_and_blocking_handoffs() -> std::io::Result<()> {
        scoped_sources(false)
    }

    #[test]
    #[should_panic(expected = "native worker lost the simulated clock")]
    fn missing_native_host_context_negative_control() {
        scoped_sources(true).unwrap();
    }
}

#[cfg(all(test, acyclic_native_runtime_loom))]
mod models {
    use super::*;

    fn draws(rewind: bool) {
        loom::model(move || {
            let source = Arc::new(Source::new(71));
            let child = source.clone();
            let task = loom::thread::spawn(move || {
                let mut bytes = [0; 16];
                child.fill(&mut bytes);
                bytes
            });
            if rewind {
                // Mutate the real source by resetting its cursor between draws.
                let mut reader = lock(&source.reader);
                reader.set_position(0);
            }
            let mut bytes = [0; 16];
            source.fill(&mut bytes);
            assert_ne!(bytes, task.join().unwrap(), "duplicate entropy draw");
        });
    }

    #[test]
    fn concurrent_draws_cannot_share_bytes() {
        draws(false);
    }

    #[test]
    #[should_panic(expected = "duplicate entropy draw")]
    fn reset_source_negative_control() {
        draws(true);
    }
}
