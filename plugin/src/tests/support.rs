use super::*;

pub(super) struct Fixture {
    pub temporary: tempfile::TempDir,
    pub root: PathBuf,
    pub data: PathBuf,
}

impl Fixture {
    pub fn new() -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("root");
        fs::create_dir(&root).unwrap();
        let data = temporary.path().join("state");
        fs::create_dir(&data).unwrap();
        Self {
            temporary,
            root,
            data,
        }
    }

    pub fn directory(&self, name: &str) -> PathBuf {
        let path = self.temporary.path().join(name);
        fs::create_dir_all(&path).unwrap();
        path
    }
}

pub(crate) async fn configured_test_service_bootstrap(
    data: &Path,
) -> acyclic_harness::Result<(
    ServiceLock,
    ServiceConfiguredInfrastructure,
    acyclic_fs::materializer::NativeHostPrivateStateRoot,
    acyclic_fs::NativeMaterializationWork,
)> {
    let lock = acquire_service_lock(data)
        .map_err(acyclic_harness::Error::Storage)?
        .ok_or_else(|| acyclic_harness::Error::Conflict("test service lock held".into()))?;
    let configured = ServiceResources::configure(data).await?;
    let work = acyclic_fs::NativeMaterializationWork::new(
        pi_root_provider::allocation_budget(),
        &CancellationToken::new(),
    )
    .map_err(|(cause, work)| acyclic_harness::Error::NativeConstruction { cause, work })?;
    let root = acyclic_fs::materializer::NativeHostPrivateStateRoot::bootstrap_configured_with_native_work(
        data, &work,
    )?;
    Ok((lock, configured, root, work))
}

pub(super) async fn test_concurrent_service(
    data: PathBuf,
) -> acyclic_harness::Result<(ServiceLock, ConcurrentServiceControl)> {
    let (lock, configured, root, work) = configured_test_service_bootstrap(&data).await?;
    Ok((
        lock,
        ConcurrentServiceControl::open(data, configured, root, &work).await?,
    ))
}

pub(super) fn run_large_stack<F>(name: &str, make: impl FnOnce() -> F + Send + 'static)
where
    F: std::future::Future<Output = ()> + 'static,
{
    std::thread::Builder::new()
        .name(name.into())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(make())
        })
        .unwrap()
        .join()
        .unwrap();
}

pub(super) fn native_hook_request(
    host: &str,
    event: &str,
    session_id: &str,
    cwd: &Path,
    extra: Value,
) -> ControlRequest {
    let mut arguments = extra.as_object().cloned().unwrap_or_default();
    arguments.insert("session_id".into(), json!(session_id));
    arguments.insert("cwd".into(), json!(cwd));
    ControlRequest {
        version: 1,
        command: ControlCommand::Hook,
        cwd: cwd.into(),
        argv: Vec::new(),
        name: format!("{host}:{event}"),
        arguments: Value::Object(arguments),
    }
}
