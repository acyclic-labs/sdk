use super::*;
use std::time::Duration;

#[cfg(unix)]
#[test]
fn registration_cancellation_retires_original_waker_outside_registry() -> io::Result<()> {
    use std::os::fd::AsFd as _;

    struct Retired {
        dropped: Arc<AtomicBool>,
        woken: Arc<AtomicBool>,
        panic: bool,
    }
    impl Wake for Retired {
        fn wake(self: Arc<Self>) {
            self.woken.store(true, Ordering::Release);
        }
    }
    impl Drop for Retired {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::Release);
            if self.panic {
                std::panic::panic_any("original registration waker retirement");
            }
        }
    }

    for panic in [false, true] {
        // Keep the original peer open and empty. Its real native producer
        // cannot consume readiness before the observation is cancelled.
        let (reader, _writer) = std::os::unix::net::UnixStream::pair()?;
        reader.set_nonblocking(true)?;
        let mut observation = descriptor_readiness(reader.as_fd(), DescriptorInterest::Readable)?;
        let hub = observation.wait.first.as_ref().unwrap().hub;
        let dropped = Arc::new(AtomicBool::new(false));
        let woken = Arc::new(AtomicBool::new(false));
        let waker = Waker::from(Arc::new(Retired {
            dropped: Arc::clone(&dropped),
            woken: Arc::clone(&woken),
            panic,
        }));
        assert!(
            Pin::new(&mut observation)
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        drop(waker);
        let retired = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(observation)));
        assert!(
            dropped.load(Ordering::Acquire),
            "original waker was retained"
        );
        // A panic in a destructor poisons exactly the mutexes held by its
        // original thread. Other native producers cannot race this oracle,
        // and the original inverse fails without entering a blocking wait.
        assert!(
            !hub.registry.is_poisoned(),
            "original registration destroyed its waker under the registry lock"
        );
        assert!(
            !woken.load(Ordering::Acquire),
            "cancelled observer was woken"
        );
        if panic {
            assert_eq!(
                retired.unwrap_err().downcast_ref::<&str>(),
                Some(&"original registration waker retirement")
            );
        } else {
            assert!(retired.is_ok());
        }
        // Both ordinary cancellation and callback unwind release the source
        // claim before a new public observation uses the original descriptor.
        drop(descriptor_readiness(
            reader.as_fd(),
            DescriptorInterest::Readable,
        )?);
    }
    Ok(())
}
#[cfg(unix)]
#[test]
fn native_producer_retains_original_consumer_panic() -> io::Result<()> {
    use std::io::Write as _;
    use std::os::fd::AsFd as _;

    struct Payload(Arc<AtomicBool>);
    impl Drop for Payload {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }
    struct Panicking(Arc<AtomicBool>);
    impl Wake for Panicking {
        fn wake(self: Arc<Self>) {
            std::panic::panic_any(Payload(Arc::clone(&self.0)));
        }
    }

    let hub = Hub {
        poller: platform::Poller::new()?,
        registry: Mutex::new(Registry::default()),
        stride: 1,
        service_failure: Arc::new(OnceLock::new()),
        locks: std::sync::atomic::AtomicUsize::new(0),
    };
    let (reader, mut writer) = std::os::unix::net::UnixStream::pair()?;
    let (quiet, _retained_writer) = std::os::unix::net::UnixStream::pair()?;
    let payload_dropped = Arc::new(AtomicBool::new(false));
    let mut trigger = arm_on(&hub, &[Source::Readable(reader.as_fd())], None)?;
    let pending = arm_on(&hub, &[Source::Readable(quiet.as_fd())], None)?;
    let waker = Waker::from(Arc::new(Panicking(Arc::clone(&payload_dropped))));
    assert!(
        Pin::new(&mut trigger)
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let outcome = std::thread::scope(|scope| {
        let owner = scope.spawn(|| hub.run());
        // The real original native queue supplies readiness. The consumer's
        // panic is reached from its production dispatch, not a direct wake.
        let emitted = writer.write_all(b"x");
        drop(writer);
        emitted?;
        let outcome = pending.block();
        assert!(
            owner.join().is_ok(),
            "native producer lost typed panic custody"
        );
        Ok::<_, io::Error>(outcome)
    })?;
    let ProcessWaitOutcome::ServiceFailed(original) = outcome else {
        panic!("native producer omitted original consumer failure");
    };
    assert_eq!(original.kind(), io::ErrorKind::Other);
    assert_eq!(original.to_string(), "TaskPanicked");
    assert!(!payload_dropped.load(Ordering::Acquire));
    let ProcessWaitOutcome::ServiceFailed(late) = arm_on(&hub, &[], None)?.block() else {
        panic!("late native observer lost original consumer failure");
    };
    assert!(Arc::ptr_eq(&original, &late));
    assert!(Arc::ptr_eq(
        &original,
        hub.service_failure.get().ok_or(io::ErrorKind::NotFound)?
    ));
    Ok(())
}

#[test]
fn deadlines_use_the_original_dispatch_cut_and_retained_native_wake() -> io::Result<()> {
    // Dispatch owns deadline admission. Advance that same production cut
    // explicitly, while the actual native queue retains registration wakes.
    let hub = Hub {
        poller: platform::Poller::new()?,
        registry: Mutex::new(Registry::default()),
        stride: 1,
        service_failure: Arc::new(OnceLock::new()),
        locks: std::sync::atomic::AtomicUsize::new(0),
    };
    let now = Instant::now();
    let earlier = now + Duration::from_secs(1);
    let distant = now + Duration::from_secs(2);
    // Exercise the same lifecycle transition used when a native adapter
    // reports immediate readiness. No settled source may lend its timer to a
    // concurrent registration while its original Wait remains alive.
    let immediate = Arc::new(Signal::default());
    {
        let mut registry = hub.registry();
        registry.next += 1;
        let token = registry.next;
        assert!(
            registry
                .registered(&hub.poller, token, &immediate, Some(earlier), true)?
                .is_none()
        );
        assert!(registry.armed.is_empty());
        assert!(registry.positions.is_empty());
        assert!(registry.timers.first().is_none());
    }
    let mut later = arm_on(&hub, &[], Some(distant))?;
    let mut ready = Vec::new();
    hub.poller.wait(None, &mut ready)?;
    assert!(
        ready.is_empty(),
        "the original registration wake carries no source token"
    );

    // The earlier deadline must notify even when the queue was previously
    // waiting for a later one. Its wake remains observable before wait entry.
    let mut first = arm_on(&hub, &[], Some(earlier))?;
    hub.poller.wait(None, &mut ready)?;
    assert!(ready.is_empty());
    struct Notified(AtomicBool);
    impl Wake for Notified {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::Release);
        }
    }
    let notified = Arc::new(Notified(AtomicBool::new(false)));
    let waker = Waker::from(Arc::clone(&notified));
    let mut context = Context::from_waker(&waker);
    hub.dispatch(&mut ready, earlier - Duration::from_nanos(1));
    assert!(!notified.0.load(Ordering::Acquire));
    assert!(Pin::new(&mut first).poll(&mut context).is_pending());
    assert!(Pin::new(&mut later).poll(&mut context).is_pending());

    hub.dispatch(&mut ready, earlier);
    assert!(notified.0.load(Ordering::Acquire));
    assert!(matches!(
        Pin::new(&mut first).poll(&mut context),
        Poll::Ready(ProcessWaitOutcome::Deadline)
    ));
    assert!(Pin::new(&mut later).poll(&mut context).is_pending());
    {
        let registry = hub.registry();
        assert!(
            !registry
                .positions
                .contains_key(&first.first.as_ref().unwrap().token)
        );
        assert_eq!(registry.timers.first().map(|(at, _)| at), Some(distant));
        assert!(
            registry
                .positions
                .contains_key(&later.first.as_ref().unwrap().token)
        );
    }
    // A duplicated source packet cannot replace the retained deadline result.
    notified.0.store(false, Ordering::Release);
    ready.push(first.first.as_ref().unwrap().token);
    hub.dispatch(&mut ready, earlier);
    assert!(!notified.0.load(Ordering::Acquire));
    assert!(matches!(
        Pin::new(&mut first).poll(&mut context),
        Poll::Ready(ProcessWaitOutcome::Deadline)
    ));

    let cancelled = arm_on(&hub, &[], Some(distant))?;
    let cancelled_token = cancelled.first.as_ref().unwrap().token;
    drop(cancelled);
    {
        let registry = hub.registry();
        assert!(!registry.positions.contains_key(&cancelled_token));
        assert_eq!(
            registry.timers.first(),
            Some((distant, later.first.as_ref().unwrap().token))
        );
    }
    hub.dispatch(&mut ready, distant);
    assert!(notified.0.load(Ordering::Acquire));
    assert!(matches!(
        Pin::new(&mut later).poll(&mut context),
        Poll::Ready(ProcessWaitOutcome::Deadline)
    ));
    let registry = hub.registry();
    assert!(registry.armed.is_empty());
    assert!(registry.positions.is_empty());
    assert!(registry.timers.first().is_none());
    Ok(())
}

#[cfg(unix)]
#[test]
fn descriptor_readiness_keeps_the_original_owner_through_data_and_peer_loss() -> io::Result<()> {
    use std::io::{Read as _, Write as _};
    use std::os::fd::AsFd as _;

    let (mut reader, mut writer) = std::os::unix::net::UnixStream::pair()?;
    reader.set_nonblocking(true)?;
    writer.set_nonblocking(true)?;
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    runtime.block_on(async {
        descriptor_readiness(writer.as_fd(), DescriptorInterest::Writable)?
            .await
            .into_result()?;
        let mut byte = [0];
        assert_eq!(
            reader.read(&mut byte).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        let mut wait = descriptor_readiness(reader.as_fd(), DescriptorInterest::Readable)?;
        assert!(
            Pin::new(&mut wait)
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        // Opposite-direction registration must not overwrite this descriptor's
        // original token. Readiness still belongs to its original operation.
        let conflict = descriptor_readiness(reader.as_fd(), DescriptorInterest::Writable)
            .err()
            .expect("a live descriptor observation cannot be replaced");
        assert_eq!(conflict.kind(), io::ErrorKind::AlreadyExists);
        writer.write_all(b"x")?;
        wait.await.into_result()?;
        assert_eq!(reader.read(&mut byte)?, 1);
        assert_eq!(byte, [b'x']);

        // Dropping the original producer wakes the same descriptor future;
        // EOF is observed by the original read, without a readiness probe.
        let wait = descriptor_readiness(reader.as_fd(), DescriptorInterest::Readable)?;
        drop(writer);
        wait.await.into_result()?;
        assert_eq!(reader.read(&mut byte)?, 0);
        Ok(())
    })
}

#[cfg(unix)]
#[test]
fn descriptor_readiness_retires_with_its_original_source() -> io::Result<()> {
    use std::io::{Read as _, Write as _};
    use std::os::fd::{AsFd as _, AsRawFd as _};

    let mut command = crate::ProcessCommand::new("cat");
    command
        .stdin(crate::ProcessStdio::piped())
        .stdout(crate::ProcessStdio::piped())
        .stderr(crate::ProcessStdio::null());
    let mut tree = super::super::ProcessTree::spawn(&mut command)?;
    let mut original = tree.readiness()?;
    assert!(
        Pin::new(&mut original)
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );

    // Transfer the actual descriptor, keeping its exact number occupied.
    // The original wait must retire before the new owner can arm that source.
    let mut reader = tree.take_stdout().expect("original piped stdout");
    let descriptor = reader.as_raw_fd();
    assert!(matches!(
        Pin::new(&mut original).poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(ProcessWaitOutcome::Cancelled)
    ));
    let successor = arm(&[Source::Readable(reader.as_fd())], None)?;
    drop(original);
    assert_eq!(
        arm(&[Source::Readable(reader.as_fd())], None)
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::AlreadyExists,
        "an old wait cannot remove its successor's source claim"
    );
    let mut writer = tree.take_stdin().expect("original piped stdin");
    writer.write_all(b"x")?;
    successor.block().into_result()?;
    let mut byte = [0];
    assert_eq!(reader.read(&mut byte)?, 1);
    assert_eq!(byte, [b'x']);

    // Closing the source also settles and releases it before its number can
    // be reused. Keeping the old future alive must not keep a dead claim.
    let reader = OwnedSource::new(reader);
    let mut original = arm(&[Source::Readable(reader.as_fd())], None)?;
    drop(reader);
    assert!(matches!(
        Pin::new(&mut original).poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(ProcessWaitOutcome::Cancelled)
    ));
    assert_ne!(
        pool()?
            .source(SourceKey::Descriptor(descriptor))
            .registry()
            .sources
            .get(&SourceKey::Descriptor(descriptor)),
        Some(&original.first.as_ref().unwrap().token),
        "a closed source cannot retain its original claim"
    );
    tree.terminate(None)?;
    Ok(())
}

#[cfg(unix)]
#[test]
fn descriptor_readiness_rolls_back_only_its_own_partial_native_admission() -> io::Result<()> {
    use std::io::Write as _;
    use std::os::fd::AsFd as _;

    let hub = Hub {
        poller: platform::Poller::new()?,
        registry: Mutex::new(Registry::default()),
        stride: 1,
        service_failure: Arc::new(OnceLock::new()),
        locks: std::sync::atomic::AtomicUsize::new(0),
    };
    let (reader, mut writer) = std::io::pipe()?;
    let (other, mut other_writer) = std::io::pipe()?;
    let first = arm_on(&hub, &[Source::Readable(reader.as_fd())], None)?;
    let sources = [
        Source::Readable(other.as_fd()),
        Source::Readable(reader.as_fd()),
    ];
    assert_eq!(
        arm_on(&hub, &sources, None)
            .err()
            .expect("second source is already held")
            .kind(),
        io::ErrorKind::AlreadyExists
    );
    // Rollback retires the partially installed native interest without
    // deleting the pre-existing owner's claim or consuming either input.
    let second = arm_on(&hub, &[Source::Readable(other.as_fd())], None)?;
    writer.write_all(b"x")?;
    other_writer.write_all(b"y")?;
    let mut ready = Vec::new();
    // Both writes precede this actual OS wait. Separate packets may arrive
    // in separate batches; each next wait is driven by retained readable data.
    while !first.fired() || !second.fired() {
        hub.poller.wait(None, &mut ready)?;
        hub.dispatch(&mut ready, Instant::now());
    }
    first.block().into_result()?;
    second.block().into_result()?;
    assert!(hub.registry().sources.is_empty());

    // An ambiguous multi-direction request refuses before replacing its own
    // earlier native token, and releases that partial claim on return too.
    let sources = [
        Source::Readable(reader.as_fd()),
        Source::Writable(reader.as_fd()),
    ];
    assert_eq!(
        arm_on(&hub, &sources, None)
            .err()
            .expect("one descriptor has one native owner")
            .kind(),
        io::ErrorKind::AlreadyExists
    );
    assert!(hub.registry().sources.is_empty());
    let mut retained = arm_on(&hub, &[Source::Readable(reader.as_fd())], None)?;
    // The same producer retirement path settles a claimed descriptor and
    // preserves its first cause while the original guard still owns it.
    drop(Retirement(&hub));
    let mut context = Context::from_waker(Waker::noop());
    let Poll::Ready(ProcessWaitOutcome::ServiceFailed(first_cause)) =
        Pin::new(&mut retained).poll(&mut context)
    else {
        panic!("lost native producer must settle its original descriptor waiter")
    };
    hub.fail(io::ErrorKind::Other.into());
    let Poll::Ready(ProcessWaitOutcome::ServiceFailed(cause)) =
        Pin::new(&mut retained).poll(&mut context)
    else {
        panic!("original producer loss must remain terminal")
    };
    assert!(Arc::ptr_eq(&first_cause, &cause));
    drop(retained);
    assert!(hub.registry().sources.is_empty());
    Ok(())
}

#[cfg(unix)]
#[test]
fn an_exit_fires_for_a_child_that_exits_and_one_that_already_has() -> io::Result<()> {
    let mut tree = super::super::ProcessTree::spawn(
        super::super::ProcessCommand::new("true")
            .stdin(super::super::ProcessStdio::null())
            .stdout(super::super::ProcessStdio::null())
            .stderr(super::super::ProcessStdio::null()),
    )?;
    tree.readiness()?.0.block().into_result()?;
    assert!(tree.try_wait(None)?.is_some());
    // The child is now unreaped and exited: the same source still fires.
    tree.readiness()?.0.block().into_result()?;
    assert!(tree.try_wait(None)?.is_some());
    Ok(())
}

/// Set to run [`a_tree_holds_the_hub_before_its_first_wait`]'s probe in a
/// fresh process, where nothing else has started the hub.
const HUB_PROBE: &str = "ACYCLIC_PROCESS_HUB_PROBE";

#[test]
fn hub_probe() -> io::Result<()> {
    if std::env::var_os(HUB_PROBE).is_none() {
        return Ok(());
    }
    assert!(!started(), "nothing in this process has started the hub");
    #[cfg(unix)]
    let mut command = super::super::ProcessCommand::new("true");
    #[cfg(windows)]
    let mut command = {
        let mut command = super::super::ProcessCommand::new("cmd.exe");
        command.args(["/d", "/c", "exit 0"]);
        command
    };
    let tree = super::super::ProcessTree::spawn(
        command
            .stdin(super::super::ProcessStdio::null())
            .stdout(super::super::ProcessStdio::null())
            .stderr(super::super::ProcessStdio::null()),
    )?;
    // No wait has armed anything: the tree itself holds the hub, so a
    // process's service threads and descriptors never depend on whether
    // its children happened to exit before anyone waited for them.
    assert!(started(), "a spawned tree holds the process-event hub");
    drop(tree);
    Ok(())
}

#[test]
fn a_tree_holds_the_hub_before_its_first_wait() -> io::Result<()> {
    let output = std::process::Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "process_tree::events::tests::hub_probe",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(HUB_PROBE, "1")
        .output()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success() && stdout.contains("1 passed"),
        "{stdout}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

#[test]
fn native_wait_failure_settles_live_waits_and_producer_loss_is_terminal() -> io::Result<()> {
    if crate::delivery::isolated()? {
        return Ok(());
    }
    use std::sync::mpsc;

    struct Notified(mpsc::Sender<()>);
    impl Wake for Notified {
        fn wake(self: Arc<Self>) {
            self.0
                .send(())
                .expect("original waiter retains notification");
        }
    }
    fn isolated() -> io::Result<Hub> {
        // A pipe is a live owned OS object, but is not an epoll/kqueue/IOCP
        // queue. The actual platform call supplies the error, never a fixture.
        Ok(Hub {
            poller: platform::Poller::refused_wait()?,
            registry: Mutex::new(Registry::default()),
            stride: 1,
            service_failure: Arc::new(OnceLock::new()),
            locks: std::sync::atomic::AtomicUsize::new(0),
        })
    }
    fn failure(wait: &mut Wait<'_>, context: &mut Context<'_>) -> Arc<io::Error> {
        match Pin::new(wait).poll(context) {
            Poll::Ready(ProcessWaitOutcome::ServiceFailed(error)) => error,
            other => panic!("original wait did not return producer failure: {other:?}"),
        }
    }

    let hub = isolated()?;
    // Grow and retire actual registrations before leaving three live waits.
    // The terminal owner traverses this dense live set, never its history.
    let cancelled = (0..128)
        .map(|_| arm_on(&hub, &[], None))
        .collect::<io::Result<Vec<_>>>()?;
    drop(cancelled);
    #[cfg(unix)]
    {
        use std::os::fd::AsFd as _;
        let (reader, _writer) = std::io::pipe()?;
        // Real native registration failure also removes its original entry.
        assert!(arm_on(&hub, &[Source::Readable(reader.as_fd())], None).is_err());
        assert!(hub.registry().armed.is_empty());
        assert!(hub.registry().positions.is_empty());
        assert!(hub.registry().sources.is_empty());
    }
    let mut pending = arm_on(&hub, &[], None)?;
    let blocked = arm_on(&hub, &[], None)?;
    let mut early = arm_on(&hub, &[], None)?;
    let cancelled = arm_on(&hub, &[], None)?;
    cancelled.cancel();
    assert!(matches!(cancelled.block(), ProcessWaitOutcome::Cancelled));
    assert_eq!(hub.registry().armed.len(), 3);
    let (notify, notified) = mpsc::channel();
    let waker = Waker::from(Arc::new(Notified(notify)));
    let mut context = Context::from_waker(&waker);
    assert!(Pin::new(&mut pending).poll(&mut context).is_pending());

    // Negative control: restore the original discarded physical result for
    // exactly one turn. It fails to settle or wake the already subscribed
    // waiter. Check at this producer's return, with no wall-clock absence test.
    let actual = hub.poller.wait(None, &mut Vec::new()).unwrap_err();
    assert!(Pin::new(&mut pending).poll(&mut context).is_pending());
    assert!(matches!(
        notified.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    let blocked_result = std::thread::scope(|scope| {
        let blocked = scope.spawn(move || blocked.block());
        // The production run ends on its first actual OS refusal, broadcasting
        // that same first cause instead of issuing another native wait.
        hub.run();
        blocked.join().expect("blocking waiter completed")
    });
    notified.recv().expect("original pending waiter was woken");
    for error in [
        failure(&mut pending, &mut context),
        failure(&mut early, &mut context),
        match blocked_result {
            ProcessWaitOutcome::ServiceFailed(error) => error,
            other => panic!("original blocking wait lost failure: {other:?}"),
        },
    ] {
        assert_eq!(error.kind(), actual.kind());
        assert_eq!(error.raw_os_error(), actual.raw_os_error());
    }
    let first = failure(&mut pending, &mut context);
    hub.fail(io::ErrorKind::BrokenPipe.into());
    let mut later = arm_on(&hub, &[], None)?;
    assert!(Arc::ptr_eq(&first, &failure(&mut later, &mut context)));
    let registry = hub.registry();
    assert!(registry.armed.is_empty());
    assert!(registry.positions.is_empty());
    assert!(registry.timers.first().is_none());
    drop(registry);
    drop((pending, early));

    let hub = isolated()?;
    let mut waiter = arm_on(&hub, &[], None)?;
    assert!(Pin::new(&mut waiter).poll(&mut context).is_pending());
    std::thread::scope(|scope| {
        let hub = &hub;
        let owner = scope.spawn(move || {
            let _retirement = Retirement(hub);
            // Unwind the same producer ownership used by Hub::run.
            std::panic::resume_unwind(Box::new("original process event producer lost"));
        });
        assert!(owner.join().is_err());
    });
    notified
        .recv()
        .expect("lost original producer wakes its waiter");
    assert_eq!(
        failure(&mut waiter, &mut context).kind(),
        io::ErrorKind::BrokenPipe
    );
    assert_eq!(
        failure(&mut arm_on(&hub, &[], None)?, &mut context).kind(),
        io::ErrorKind::BrokenPipe
    );

    // A failed native timer notification is a service failure, even before
    // the wait loop runs. Existing and later waiters retain the same cause.
    let hub = isolated()?;
    let mut existing = arm_on(&hub, &[], None)?;
    assert!(Pin::new(&mut existing).poll(&mut context).is_pending());
    let mut timed = arm_on(&hub, &[], Some(Instant::now() + Duration::from_secs(3600)))?;
    notified
        .recv()
        .expect("failed original notification wakes waiter");
    let first = failure(&mut existing, &mut context);
    assert!(first.raw_os_error().is_some());
    assert!(Arc::ptr_eq(&first, &failure(&mut timed, &mut context)));
    assert!(Arc::ptr_eq(
        &first,
        &failure(&mut arm_on(&hub, &[], None)?, &mut context)
    ));
    assert!(hub.registry().armed.is_empty());

    // Tokens cannot wrap onto another live wait or the native WAKE key.
    let hub = isolated()?;
    hub.registry().next = usize::MAX - 1;
    assert!(arm_on(&hub, &[], None).is_err());
    assert!(hub.registry().armed.is_empty());
    assert!(hub.registry().positions.is_empty());

    // A consumer unwind cannot discard terminal outcomes already removed
    // from the registry. Delivery retains the remaining original producers.
    struct Panicking(&'static str);
    impl Wake for Panicking {
        fn wake(self: Arc<Self>) {
            std::panic::panic_any(self.0);
        }
    }
    let panicking = Arc::new(Signal::default());
    let second = Arc::new(Signal::default());
    let following = Arc::new(Signal::default());
    let waker = Waker::from(Arc::new(Panicking(
        "original waiter unwound during terminal delivery",
    )));
    assert!(
        panicking
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    assert!(
        second
            .poll(&mut Context::from_waker(&Waker::from(Arc::new(Panicking(
                "later waiter unwound during terminal delivery",
            )))))
            .is_pending()
    );
    assert!(following.poll(&mut context).is_pending());
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        deliver(
            [panicking, second, Arc::clone(&following)]
                .into_iter()
                .map(|signal| {
                    (
                        signal,
                        ProcessWaitOutcome::ServiceFailed(Arc::clone(&first)),
                    )
                }),
        );
    }));
    assert_eq!(
        result.unwrap_err().downcast_ref::<&str>(),
        Some(&"original waiter unwound during terminal delivery")
    );
    notified
        .recv()
        .expect("remaining original waiter was woken");
    let Poll::Ready(ProcessWaitOutcome::ServiceFailed(error)) = following.poll(&mut context) else {
        panic!("consumer unwind discarded the following original terminal")
    };
    assert!(Arc::ptr_eq(&first, &error));
    Ok(())
}
