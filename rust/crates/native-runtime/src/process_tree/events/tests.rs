use super::*;
use std::time::Duration;

#[test]
fn a_wake_time_alone_fires_once_it_passes() -> io::Result<()> {
    let started = Instant::now();
    let wait = arm(&[], Some(started + Duration::from_millis(20)))?;
    assert!(!wait.fired());
    wait.block();
    assert!(started.elapsed() >= Duration::from_millis(20));
    Ok(())
}

#[test]
fn a_dropped_wait_leaves_nothing_armed() -> io::Result<()> {
    let wait = arm(&[], Some(Instant::now() + Duration::from_secs(3600)))?;
    let token = wait.token;
    drop(wait);
    let registry = hub()?.registry();
    assert!(!registry.armed.contains_key(&token));
    assert!(registry.timers.iter().all(|(_, armed)| *armed != token));
    Ok(())
}

#[test]
fn a_later_earlier_wake_time_is_not_delayed_by_a_distant_one() -> io::Result<()> {
    let _distant = arm(&[], Some(Instant::now() + Duration::from_secs(3600)))?;
    let started = Instant::now();
    arm(&[], Some(started + Duration::from_millis(10)))?.block();
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
    std::thread::sleep(Duration::from_millis(20));
    assert!(!wait.fired(), "an empty pipe is not readable");
    writer.write_all(b"x")?;
    wait.block();
    // A writable pipe with room is ready at once.
    arm(&[Source::Writable(writer.as_fd())], None)?.block();
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
    let deadline = Instant::now() + Duration::from_secs(30);
    tree.arm_exit(deadline)?.block();
    assert!(Instant::now() < deadline, "the exit woke the wait");
    // The child is now unreaped and exited: a new wait still fires.
    tree.arm_exit(deadline)?.block();
    assert!(
        Instant::now() < deadline,
        "an exited child wakes a new wait"
    );
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
