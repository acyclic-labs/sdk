//! Receipt fault injection around the ordinary stream backend, not a process owner.
use super::*;
use tokio::sync::Mutex;
#[derive(Default)]
struct ReceiptFaultStream<P = MemoryStream> {
    inner: P,
    fault: Mutex<Option<(&'static str, bool)>>,
    hide_next_inspection: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl<P: StreamProvider> StreamProvider for ReceiptFaultStream<P> {
    async fn inspect_idempotency(
        &self,
        key: acyclic_stream::IdempotencyKey,
    ) -> std::result::Result<
        Option<acyclic_stream::IdempotencyObservation>,
        acyclic_stream::StreamError,
    > {
        if self
            .hide_next_inspection
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        self.inner.inspect_idempotency(key).await
    }
    async fn tail(
        &self,
        path: acyclic_stream::StreamPath,
    ) -> std::result::Result<u64, acyclic_stream::StreamError> {
        self.inner.tail(path).await
    }
    async fn bounds(
        &self,
        path: acyclic_stream::StreamPath,
    ) -> std::result::Result<acyclic_stream::StreamBounds, acyclic_stream::StreamError> {
        self.inner.bounds(path).await
    }
    async fn append(
        &self,
        request: acyclic_stream::AppendRequest,
    ) -> std::result::Result<acyclic_stream::AppendOutcome, acyclic_stream::StreamError> {
        self.inner.append(request).await
    }
    async fn fork(
        &self,
        request: acyclic_stream::ForkRequest,
    ) -> std::result::Result<acyclic_stream::ForkReceipt, acyclic_stream::StreamError> {
        self.inner.fork(request).await
    }
    async fn read(
        &self,
        request: acyclic_stream::ReadRequest,
    ) -> std::result::Result<acyclic_stream::RecordStream, acyclic_stream::StreamError> {
        self.inner.read(request).await
    }
    async fn follow(
        &self,
        path: acyclic_stream::StreamPath,
        from: u64,
    ) -> std::result::Result<acyclic_stream::RecordStream, acyclic_stream::StreamError> {
        self.inner.follow(path, from).await
    }
    async fn children(
        &self,
        request: acyclic_stream::ChildrenRequest,
    ) -> std::result::Result<acyclic_stream::ChildStream, acyclic_stream::StreamError> {
        self.inner.children(request).await
    }
    async fn children_page(
        &self,
        request: acyclic_stream::ChildrenPageRequest,
    ) -> std::result::Result<acyclic_stream::ChildrenPage, acyclic_stream::StreamError> {
        self.inner.children_page(request).await
    }
    async fn read_commit(
        &self,
        id: acyclic_stream::CommitId,
    ) -> std::result::Result<acyclic_stream::CommittedEnvelope, acyclic_stream::StreamError> {
        self.inner.read_commit(id).await
    }
    async fn commit(
        &self,
        request: acyclic_stream::CommitRequest,
    ) -> std::result::Result<acyclic_stream::CommitOutcome, acyclic_stream::StreamError> {
        let mut armed = self.fault.lock().await;
        let selected = armed.as_ref().is_some_and(|(kind, _)| request.mutations.iter().any(|mutation| {
            matches!(mutation, acyclic_stream::CommitMutation::Append { path, records }
                if path.as_str().starts_with("harness/v2/native-process/") && records.iter().any(|record| serde_json::from_slice::<Value>(record).is_ok_and(|value| value.get("kind").and_then(Value::as_str) == Some(*kind))))
        }));
        let fault = if selected { armed.take() } else { None };
        drop(armed);
        if fault.is_some_and(|(_, lost_reply)| !lost_reply) {
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        let result = self.inner.commit(request).await?;
        if fault.is_some() {
            // Hide the immediate keyed observation as well, so this is
            // an unknown commit rather than a recovered lost reply.
            self.hide_next_inspection
                .store(true, std::sync::atomic::Ordering::SeqCst);
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        Ok(result)
    }
}

pub(super) async fn run() -> std::result::Result<(), Box<dyn std::error::Error>> {
    for (kind, lost_reply) in [
        ("launch", false),
        ("launch", true),
        ("observed", false),
        ("observed", true),
    ] {
        let stream = Arc::new(ReceiptFaultStream::<MemoryStream>::default());
        *stream.fault.lock().await = Some((kind, lost_reply));
        super::run_on(
            Some(McpStdioMethod::CallTool),
            StreamClient::new(stream.clone()),
            Some((kind, lost_reply)),
            false,
        )
        .await?;
        if stream.fault.lock().await.is_some() {
            return Err("native receipt fault was not exercised".into());
        }
        println!("MCP {kind} receipt fault (lost acknowledgement: {lost_reply}) passed");
    }
    super::run_on(
        Some(McpStdioMethod::CallTool),
        StreamClient::new(Arc::new(MemoryStream::default())),
        None,
        true,
    )
    .await?;
    println!(
        "MCP applied call with lost response, bounded cleanup and receipt-only recovery passed"
    );
    #[cfg(feature = "filesystem-local")]
    {
        disk_restart().await?;
        host_death().await?;
    }
    Ok(())
}

#[cfg(feature = "filesystem-local")]
async fn disk_restart() -> std::result::Result<(), Box<dyn std::error::Error>> {
    for (method, receipt_fault, lost_response) in [
        (McpStdioMethod::CallTool, None, false),
        (McpStdioMethod::CallTool, Some(("launch", false)), false),
        (McpStdioMethod::CallTool, Some(("launch", true)), false),
        (McpStdioMethod::CallTool, Some(("observed", false)), false),
        (McpStdioMethod::CallTool, Some(("observed", true)), false),
        (McpStdioMethod::CallTool, None, true),
        (McpStdioMethod::ListTools, None, false),
        (McpStdioMethod::ListTools, None, true),
    ] {
        let storage = tempfile::tempdir()?;
        let filesystem = Fs::local(acyclic_fs::LocalOptions::new(
            storage.path().join("filesystem"),
        ))
        .await?;
        let local = acyclic_stream::LocalStream::open(
            storage.path().join("coordinator"),
            Default::default(),
        )
        .await?;
        let stream = Arc::new(ReceiptFaultStream {
            inner: local,
            fault: Mutex::new(receipt_fault),
            hide_next_inspection: Default::default(),
        });
        let weak_stream = Arc::downgrade(&stream);
        let evidence = super::prepare_on(
            Some(method),
            StreamClient::new(stream.clone()),
            filesystem,
            receipt_fault,
            lost_response,
            None,
        )
        .await?;
        if stream.fault.lock().await.is_some() {
            return Err("disk-backed native receipt fault was not exercised".into());
        }
        drop(stream);
        if weak_stream.upgrade().is_some() || evidence.files.upgrade().is_some() {
            return Err("old runtime or storage handles survived disk restart".into());
        }
        let filesystem = Fs::local(acyclic_fs::LocalOptions::new(
            storage.path().join("filesystem"),
        ))
        .await?;
        let stream = Arc::new(
            acyclic_stream::LocalStream::open(
                storage.path().join("coordinator"),
                Default::default(),
            )
            .await?,
        );
        super::recover_on(
            StreamClient::new(stream),
            filesystem,
            &evidence.restart,
            receipt_fault,
            lost_response,
        )
        .await?;
        println!(
            "MCP {method:?} disk restart (receipt fault: {receipt_fault:?}, lost response: {lost_response}) passed"
        );
    }
    Ok(())
}

#[cfg(feature = "filesystem-local")]
pub(super) async fn host_child() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let storage = std::path::PathBuf::from(
        std::env::var_os("MCP_HOST_STORAGE").ok_or("missing host storage")?,
    );
    let filesystem = Fs::local(acyclic_fs::LocalOptions::new(storage.join("filesystem"))).await?;
    let stream = Arc::new(
        acyclic_stream::LocalStream::open(storage.join("coordinator"), Default::default()).await?,
    );
    let _ = super::prepare_on(
        Some(McpStdioMethod::CallTool),
        StreamClient::new(stream),
        filesystem,
        None,
        true,
        Some(&storage.join("restart.json")),
    )
    .await?;
    Err("host-death controller failed to terminate the host".into())
}

#[cfg(feature = "filesystem-local")]
async fn host_death() -> std::result::Result<(), Box<dyn std::error::Error>> {
    use std::process::{Command, Stdio};
    use std::time::Duration;
    let storage = tempfile::tempdir()?;
    let mut command = Command::new(std::env::current_exe()?);
    command
        .arg("--mcp-host-death-child")
        .env_clear()
        .env("MCP_HOST_STORAGE", storage.path())
        .current_dir(storage.path())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut host = acyclic_native_runtime::spawn_process_tree(&mut command)?;
    let ready = host_ready(&mut host, storage.path()).await;
    // Stop and reap this exact contained host even when setup never becomes ready.
    let status = host.terminate_after(Duration::from_millis(250))?;
    if status.success() || !host.is_reaped() {
        return Err("host termination did not produce a reaped failure status".into());
    }
    let mut seed = ready?;
    super::verify_mcp_calls(&seed.request, true)?;
    seed.applied = true;
    super::remove_physical_output(&seed.request.cwd, true)?;
    let filesystem = Fs::local(acyclic_fs::LocalOptions::new(
        storage.path().join("filesystem"),
    ))
    .await?;
    let stream = StreamClient::new(Arc::new(
        acyclic_stream::LocalStream::open(storage.path().join("coordinator"), Default::default())
            .await?,
    ));
    let before = launch_only(&stream, &seed).await?;
    super::recover_on(stream.clone(), filesystem.clone(), &seed, None, true).await?;
    if launch_only(&stream, &seed).await? != before {
        return Err("host-death recovery changed the launch-only native receipt".into());
    }
    verify_pending_task(stream.clone(), filesystem, &seed).await?;
    if launch_only(&stream, &seed).await? != before {
        return Err(
            "task cancellation converted the unknown native attempt into an observation".into(),
        );
    }
    super::verify_mcp_calls(&seed.request, true)?;
    println!(
        "MCP actual host termination after application, launch-only disk recovery and no replay passed"
    );
    Ok(())
}

#[cfg(feature = "filesystem-local")]
async fn host_ready(
    host: &mut acyclic_native_runtime::ProcessTree,
    storage: &std::path::Path,
) -> std::result::Result<super::Restart, Box<dyn std::error::Error>> {
    use std::time::Duration;
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Ok(bytes) = std::fs::read(storage.join("restart.json"))
                && let Ok(seed) = serde_json::from_slice::<super::Restart>(&bytes)
                && std::fs::read(seed.request.cwd.join("destination/calls.txt"))
                    .is_ok_and(|calls| calls == b"call\n")
                && seed.request.cwd.join("destination/output.txt").exists()
            {
                return Ok(seed);
            }
            if let Some(status) = host.try_wait()? {
                return Err(format!("test host exited before MCP application: {status}").into());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?
}

#[cfg(feature = "filesystem-local")]
async fn launch_only(
    stream: &StreamClient<acyclic_stream::LocalStream>,
    seed: &super::Restart,
) -> std::result::Result<
    (acyclic_stream::StreamPath, acyclic_stream::Record),
    Box<dyn std::error::Error>,
> {
    use futures::TryStreamExt as _;
    let mut children: Vec<_> = stream
        .children(Some("harness/v2/native-process"), 2)
        .await?
        .try_collect()
        .await?;
    if children.len() != 1 {
        return Err("host death did not retain exactly one native attempt".into());
    }
    let child = children.pop().ok_or("missing native attempt")?;
    let receipt = stream.stream(child.path.as_str())?;
    if receipt.bounds().await?.tail != 1 {
        return Err("host death did not retain a launch-only receipt".into());
    }
    let mut records: Vec<_> = receipt.read(0, 2).await?.try_collect().await?;
    if records.len() != 1 {
        return Err("unexpected native receipt record count".into());
    }
    let record = records.pop().ok_or("missing native launch record")?;
    let value: Value = serde_json::from_slice(&record.value)?;
    for (field, expected) in [
        ("kind", json!("launch")),
        ("task", serde_json::to_value(seed.task)?),
        ("command", serde_json::to_value(seed.command)?),
        (
            "approval_digest",
            serde_json::to_value(seed.request.approval_digest(seed.task, seed.command)?)?,
        ),
    ] {
        if value.get(field) != Some(&expected) {
            return Err(format!("native launch receipt differs in {field}").into());
        }
    }
    Ok((child.path, record))
}

#[cfg(feature = "filesystem-local")]
async fn verify_pending_task(
    stream: StreamClient<acyclic_stream::LocalStream>,
    filesystem: Fs<acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>,
    seed: &super::Restart,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let issuer = super::issuer();
    let files = Arc::new(FilesystemHost::new(
        filesystem,
        seed.destination.provider().clone(),
    )?);
    let task = super::runtime(
        stream.clone(),
        files.clone(),
        seed.results_volume.clone(),
        &issuer,
        &seed.scope,
    )
    .await?;
    task.task_host()
        .verify_dispatch_owner(seed.task, seed.fence.clone())
        .await?;
    if task.task_host().outcome(seed.task).await?.is_some() {
        return Err("host-death task acquired a terminal outcome".into());
    }
    task.task_host().cancel(seed.task).await?;
    if !matches!(
        task.task_host()
            .verify_dispatch_owner(seed.task, seed.fence.clone())
            .await,
        Err(acyclic_harness::Error::Conflict(_))
    ) {
        return Err("cancelled host-death task still permits a fresh dispatch".into());
    }
    task.task_host()
        .verify_execution_owner(seed.task, seed.fence.clone())
        .await?;
    let mut wrong_fence = seed.fence.clone();
    wrong_fence.placement.push_str(".different");
    let rejection = task
        .task_host()
        .verify_execution_owner(seed.task, wrong_fence)
        .await;
    if !matches!(&rejection, Err(acyclic_harness::Error::Unauthorized(_))) {
        return Err(format!("wrong-placement settlement check returned {rejection:?}").into());
    }
    if task.task_host().outcome(seed.task).await?.is_some() {
        return Err(
            "cancellation reported a terminal outcome for the unknown native attempt".into(),
        );
    }
    let recovery = super::recovered_effects(stream, files, &task, seed, &issuer).await?;
    if recovery
        .effects
        .reconcile_task_effect(&recovery.owner, seed.command, &seed.plan)
        .await?
        != EffectStatus::Indeterminate
    {
        return Err("cancelled host-death reconciliation became an authoritative result".into());
    }
    Ok(())
}
