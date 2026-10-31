use super::*;
use std::time::Duration;

#[test]
fn a_wake_time_alone_fires_once_it_passes() -> io::Result<()> {
    let started = Instant::now();
    let wait = arm(&[], Some(started + Duration::from_millis(20)))?;
    assert!(!wait.fired());
    wait.block()?;
    assert!(started.elapsed() >= Duration::from_millis(20));
    Ok(())
}

#[test]
fn a_dropped_wait_leaves_nothing_armed() -> io::Result<()> {
    let wait = arm(&[], Some(Instant::now() + Duration::from_secs(3600)))?;
    let token = wait.token;
    drop(wait);
    let registry = hub()?.registry();
    assert!(!registry.positions.contains_key(&token));
    assert!(registry.timers.iter().all(|(_, armed)| *armed != token));
    Ok(())
}

#[test]
fn a_later_earlier_wake_time_is_not_delayed_by_a_distant_one() -> io::Result<()> {
    let _distant = arm(&[], Some(Instant::now() + Duration::from_secs(3600)))?;
    let started = Instant::now();
    arm(&[], Some(started + Duration::from_millis(10)))?.block()?;
    assert!(started.elapsed() < Duration::from_secs(60));
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
    wait.block()?;
    // A writable pipe with room is ready at once.
    arm(&[Source::Writable(writer.as_fd())], None)?.block()?;
    Ok(())
}

#[cfg(unix)]
#[test]
fn an_exit_fires_for_a_child_that_exits_and_one_that_already_has() -> io::Result<()> {
    let mut tree = super::super::ProcessTree::spawn(
        std::process::Command::new("true")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null()),
    )?;
    tree.readiness()?.0.block()?;
    assert!(tree.try_wait(None)?.is_some());
    // The child is now unreaped and exited: the same source still fires.
    tree.readiness()?.0.block()?;
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
    let mut command = std::process::Command::new("true");
    #[cfg(windows)]
    let mut command = {
        let mut command = std::process::Command::new("cmd.exe");
        command.args(["/d", "/c", "exit 0"]);
        command
    };
    let tree = super::super::ProcessTree::spawn(
        command
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null()),
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
    fn failure(wait: &mut Wait<'_>, context: &mut Context<'_>) -> io::Error {
        match Pin::new(wait).poll(context) {
            Poll::Ready(Err(error)) => error,
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
    drop(cancelled);
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
        blocked_result.unwrap_err(),
    ] {
        assert_eq!(error.kind(), actual.kind());
        assert_eq!(error.raw_os_error(), actual.raw_os_error());
    }
    let later = arm_on(&hub, &[], None)
        .err()
        .expect("a dead producer cannot arm");
    assert_eq!(later.raw_os_error(), actual.raw_os_error());
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
        arm_on(&hub, &[], None).err().unwrap().kind(),
        io::ErrorKind::BrokenPipe
    );
    Ok(())
}
