use super::*;

const ROOT: &str = "ACYCLIC_TEST_CRASH_ROOT";
const CASE: &str = "ACYCLIC_TEST_CRASH_CASE";

pub(in crate::tests) fn crash_case() -> Option<String> {
    std::env::var(CASE).ok()
}

pub(in crate::tests) struct CrashPaths {
    pub(in crate::tests) root: PathBuf,
    pub(in crate::tests) data: PathBuf,
}

// Keep the real native provider and its fence live until the parent kills the
// child. Dropping ControlPlane models orderly retirement, not a process crash.
pub(in crate::tests) async fn crash_test_process<F>(
    test: &str,
    case: &str,
    prepare: impl FnOnce(CrashPaths) -> F,
) -> Fixture
where
    F: std::future::Future<Output = ControlPlane>,
{
    if let Some(root) = std::env::var_os(ROOT) {
        assert_eq!(crash_case().as_deref(), Some(case));
        let root = PathBuf::from(root);
        let fixture = CrashPaths {
            root: root.join("root"),
            data: root.join("state"),
        };
        let _control = prepare(fixture).await;
        fs::write(root.join("crash-ready"), b"ready").unwrap();
        std::future::pending::<()>().await;
        unreachable!();
    }
    let fixture = Fixture::new();
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", test, "--nocapture", "--test-threads=1"])
        .env(ROOT, fixture.temporary.path())
        .env(CASE, case);
    let mut child = acyclic_native_runtime::ProcessTree::spawn(&mut command).unwrap();
    let started = std::time::Instant::now();
    while !fixture.temporary.path().join("crash-ready").exists() {
        assert!(
            child.try_wait(None).unwrap().is_none(),
            "crash fixture exited before readiness: {case}"
        );
        assert!(
            started.elapsed() < std::time::Duration::from_secs(3),
            "crash fixture did not become ready: {case}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    child.terminate_descendants().unwrap();
    assert!(
        !child
            .wait(std::time::Duration::from_secs(1))
            .unwrap()
            .success()
    );
    child.terminate(None).unwrap();
    assert!(child.is_reaped());
    fixture
}
