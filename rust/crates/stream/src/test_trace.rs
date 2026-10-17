//! Process isolation for tracing's process-global first-registration cache.

pub(crate) fn isolate(test: &str) -> std::io::Result<bool> {
    const CHILD: &str = "ACYCLIC_TRACE_CAPTURE_TEST";
    if std::env::var(CHILD).as_deref() == Ok(test) {
        return Ok(false);
    }
    let output = command(test)?.env(CHILD, test).output()?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success(), "{stdout}\n{stderr}");
    assert!(stdout.contains("1 passed; 0 failed"), "{stdout}\n{stderr}");
    Ok(true)
}

pub(crate) fn command(test: &str) -> std::io::Result<std::process::Command> {
    let test = test
        .split_once("::")
        .ok_or_else(|| std::io::Error::other("test module"))?
        .1;
    let mut child = std::process::Command::new(std::env::current_exe()?);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt as _;
        child.creation_flags(0x08000000);
    }
    child.args([test, "--exact", "--nocapture"]);
    Ok(child)
}
