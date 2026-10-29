use super::*;

struct Handler {
    coordination: std::thread::ThreadId,
    starting: Option<tokio::sync::oneshot::Sender<()>>,
    release: std::sync::mpsc::Receiver<()>,
    wakes: Vec<ProcessWake>,
    extracted: usize,
    observed: bool,
}

impl Handler {
    fn blocking_boundary(&self) -> io::Result<()> {
        if std::thread::current().id() == self.coordination {
            // Finite negative oracle: a synchronous wait here would prevent
            // the sibling coordination task from producing its release event.
            Err(io::ErrorKind::WouldBlock.into())
        } else {
            Ok(())
        }
    }
}

impl CaptureHandler for Handler {
    fn take_wakes(&mut self) -> io::Result<CaptureWakes> {
        self.blocking_boundary()?;
        self.extracted += 1;
        if let Some(starting) = self.starting.take() {
            let _ = starting.send(());
        }
        self.release
            .recv()
            .map_err(|error| io::Error::new(io::ErrorKind::Interrupted, error))?;
        Ok(std::mem::take(&mut self.wakes).into())
    }

    fn check(&mut self) -> io::Result<()> {
        self.blocking_boundary()
    }

    fn observe(&mut self, _: ProcessOutput<'_>) -> io::Result<ProcessInput> {
        self.blocking_boundary()?;
        self.observed = true;
        Ok(ProcessInput::default())
    }
}

#[test]
fn transferring_signal_ownership_has_constant_cost_without_cloning() -> io::Result<()> {
    let signal = ProcessWake::new()?;
    let mut single = Some(signal.clone());
    let allocation = acyclic_test_support::measure(|| {
        let owned = CaptureWakes::from(single.take());
        assert_eq!(std::hint::black_box(owned.as_slice()).len(), 1);
    });
    assert_eq!(allocation.count_total, 0);
    for count in [1, 100_000] {
        let mut signals = vec![signal.clone(); count];
        let mut owned = None;
        let allocation = acyclic_test_support::measure(|| {
            owned = Some(CaptureWakes::from(std::mem::take(&mut signals)));
            assert_eq!(
                std::hint::black_box(owned.as_ref().unwrap().as_slice()).len(),
                count
            );
        });
        assert_eq!(allocation.count_total, 0);
        // Copying the same owners fails the zero-allocation transfer oracle,
        // and its allocated bytes grow with the number of signals.
        let copied = acyclic_test_support::measure(|| {
            std::hint::black_box(owned.as_ref().unwrap().as_slice().to_vec());
        });
        assert_eq!(copied.count_total, 1);
        assert_eq!(
            copied.bytes_total,
            u64::try_from(count * std::mem::size_of::<ProcessWake>()).map_err(io::Error::other)?
        );
    }
    Ok(())
}

#[test]
fn callback_signals_have_a_sibling_producer_and_cleanup_on_producer_loss() -> io::Result<()> {
    crate::coordination()?.block_on(async {
        for release_event in [true, false] {
            let tree = super::capture_events_tests::child("callback")?;
            let (starting, started) = tokio::sync::oneshot::channel();
            let (release, held) = std::sync::mpsc::channel();
            let coordination = crate::spawn(async { std::thread::current().id() }).await?;
            let mut handler = Handler {
                coordination,
                starting: Some(starting),
                release: held,
                wakes: vec![ProcessWake::new()?],
                extracted: 0,
                observed: false,
            };
            // Calling on the previous coordination path fails the exact same
            // boundary oracle, before its sibling could be blocked forever.
            let handler = crate::spawn(async move {
                assert_eq!(
                    handler.take_wakes().err().unwrap().kind(),
                    io::ErrorKind::WouldBlock
                );
                handler
            })
            .await?;
            let outcome = crate::spawn(tree.capture(
                CaptureSchedule::new(Duration::from_secs(2)),
                4096,
                None,
                handler,
            ));
            started.await.map_err(io::Error::other)?;
            crate::spawn(async move {
                if release_event {
                    release.send(()).map_err(io::Error::other)?;
                }
                drop(release);
                Ok::<_, io::Error>(())
            })
            .await??;
            let outcome = outcome.await??;
            assert_eq!(outcome.handler.extracted, 1);
            if release_event {
                assert!(
                    outcome
                        .result
                        .map_err(|failure| failure.error)?
                        .status
                        .success()
                );
                assert!(outcome.handler.observed);
                assert!(outcome.handler.wakes.is_empty());
            } else {
                let failure = outcome.result.unwrap_err();
                assert_eq!(failure.error.kind(), io::ErrorKind::Interrupted);
                assert!(failure.cleanup_completed);
            }
        }
        Ok(())
    })
}
