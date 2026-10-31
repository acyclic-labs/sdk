use super::*;
use std::time::Duration;

#[test]
fn deadlines_use_the_original_dispatch_cut_and_retained_native_wake() -> io::Result<()> {
    // Dispatch owns deadline admission. Advance that same production cut
    // explicitly, while the actual native queue retains registration wakes.
    let hub = Hub {
        poller: platform::Poller::new()?,
        registry: Mutex::new(Registry::default()),
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
                .registered(&hub.poller, token, &immediate, Some(earlier), true)
                .is_none()
        );
        assert!(registry.armed.is_empty());
        assert!(registry.positions.is_empty());
        assert!(registry.timers.is_empty());
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
        assert!(!registry.positions.contains_key(&first.token));
        assert!(!registry.timers.contains(&(earlier, first.token)));
        assert!(registry.positions.contains_key(&later.token));
    }
    // A duplicated source packet cannot replace the retained deadline result.
    notified.0.store(false, Ordering::Release);
    ready.push(first.token);
    hub.dispatch(&mut ready, earlier);
    assert!(!notified.0.load(Ordering::Acquire));
    assert!(matches!(
        Pin::new(&mut first).poll(&mut context),
        Poll::Ready(ProcessWaitOutcome::Deadline)
    ));

    let cancelled = arm_on(&hub, &[], Some(distant))?;
    let cancelled_token = cancelled.token;
    drop(cancelled);
    {
        let registry = hub.registry();
        assert!(!registry.positions.contains_key(&cancelled_token));
        assert!(!registry.timers.contains(&(distant, cancelled_token)));
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
    assert!(registry.timers.is_empty());
    Ok(())
}

#[cfg(unix)]
#[test]
fn a_pipe_fires_when_its_data_arrives_and_not_before() -> io::Result<()> {
    use std::io::Write as _;
    use std::os::fd::AsFd as _;
    let (reader, mut writer) = std::io::pipe()?;
    let wait = arm(&[Source::Readable(reader.as_fd())], None)?;
    assert!(!wait.fired(), "an empty pipe is not readable");
    writer.write_all(b"x")?;
    wait.block().into_result()?;
    // A writable pipe with room is ready at once.
    arm(&[Source::Writable(writer.as_fd())], None)?
        .block()
        .into_result()?;
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
    assert!(registry.timers.is_empty());
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
    hub.registry().next = u64::MAX - 1;
    assert!(arm_on(&hub, &[], None).is_err());
    assert!(hub.registry().armed.is_empty());
    assert!(hub.registry().positions.is_empty());

    // A consumer unwind cannot discard terminal outcomes already removed
    // from the registry. Delivery retains the remaining original producers.
    struct Panicking;
    impl Wake for Panicking {
        fn wake(self: Arc<Self>) {
            panic!("original waiter unwound during terminal delivery");
        }
    }
    let panicking = Arc::new(Signal::default());
    let following = Arc::new(Signal::default());
    let waker = Waker::from(Arc::new(Panicking));
    assert!(
        panicking
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    assert!(following.poll(&mut context).is_pending());
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        deliver(
            [panicking, Arc::clone(&following)]
                .into_iter()
                .map(|signal| {
                    (
                        signal,
                        ProcessWaitOutcome::ServiceFailed(Arc::clone(&first)),
                    )
                }),
        );
    }));
    assert!(result.is_err());
    notified
        .recv()
        .expect("remaining original waiter was woken");
    let Poll::Ready(ProcessWaitOutcome::ServiceFailed(error)) = following.poll(&mut context) else {
        panic!("consumer unwind discarded the following original terminal")
    };
    assert!(Arc::ptr_eq(&first, &error));
    Ok(())
}
