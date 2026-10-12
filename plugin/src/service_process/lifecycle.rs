//! Service startup, readiness and shutdown.

use super::*;

pub(crate) async fn run_service(data: PathBuf) -> acyclic_harness::Result<SessionCloseState> {
    // Taken first, so nothing the service prints can reach its starter.
    let ready = acyclic_native_runtime::take_service_ready_signal()
        .map_err(|error| acyclic_harness::Error::Storage(display(error)))?;
    run_service_with_identity(data, None, ready).await
}

pub(crate) async fn shutdown_service_endpoint(
    endpoint: ControlEndpoint,
    control: Arc<ConcurrentServiceControl>,
) -> acyclic_harness::Result<SessionCloseState> {
    let endpoint_result = endpoint.shutdown().await;
    let control_result = match Arc::try_unwrap(control) {
        Ok(control) => control.shutdown().await,
        Err(_) => Err(acyclic_harness::Error::Invalid(
            "control service retained an active request during shutdown".to_owned(),
        )),
    };
    service_cleanup_outcome(control_result, endpoint_result)
}

fn service_cleanup_outcome(
    native: acyclic_harness::Result<SessionCloseState>,
    ordinary: Result<(), String>,
) -> acyclic_harness::Result<SessionCloseState> {
    match native {
        Ok(SessionCloseState::Closed) => ordinary
            .map(|()| SessionCloseState::Closed)
            .map_err(acyclic_harness::Error::Storage),
        outcome => outcome,
    }
}

/// Runs the service. `ready`, when its starter waits on it, is signalled once
/// the service answers requests, and closes unsignalled if it exits first,
/// for instance because another service holds the lock.
pub(crate) async fn run_service_with_identity(
    data: PathBuf,
    identity_override: Option<String>,
    ready: Option<acyclic_native_runtime::ServiceReadySignal>,
) -> acyclic_harness::Result<SessionCloseState> {
    let Some(lock) = acquire_service_lock(&data).map_err(acyclic_harness::Error::Storage)? else {
        return Ok(SessionCloseState::Closed);
    };
    let configured = crate::service::ServiceResources::configure(&data).await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let original_bootstrap_work = acyclic_fs::NativeMaterializationWork::new(
        pi_root_provider::allocation_budget(), &cancellation,
    ).map_err(|(cause, work)| acyclic_harness::Error::NativeConstruction { cause, work })?;
    let private_root = acyclic_fs::materializer::NativeHostPrivateStateRoot::bootstrap_configured_with_native_work(
        &data, &original_bootstrap_work,
    ).map_err(|cause| acyclic_harness::Error::NativeWork { cause, custody: None })?;
    let result = run_locked_service(
        data, identity_override, ready, configured, private_root, &original_bootstrap_work,
    ).await;
    drop(lock);
    result
}

async fn run_locked_service(
    data: PathBuf,
    identity_override: Option<String>,
    ready: Option<acyclic_native_runtime::ServiceReadySignal>,
    configured: crate::service::ServiceConfiguredInfrastructure,
    private_root: acyclic_fs::materializer::NativeHostPrivateStateRoot,
    original_bootstrap_work: &acyclic_fs::NativeMaterializationWork,
) -> acyclic_harness::Result<SessionCloseState> {
    let mut service = ConcurrentServiceControl::open(data.clone(), configured, private_root, original_bootstrap_work).await?;
    if let Some(identity) = identity_override {
        service.resources.binary_identity = identity;
    }
    let instance_id = service.instance_id.clone();
    let binary_identity = service.binary_identity.clone();
    let control = Arc::new(service);
    let endpoint = match start_control_endpoint(Arc::clone(&control), &data).await {
        Ok(endpoint) => endpoint,
        Err(endpoint_error) => {
            let control_result = match Arc::try_unwrap(control) {
                Ok(control) => control.shutdown().await,
                Err(_) => Err(acyclic_harness::Error::Invalid(
                    "failed endpoint retained the control service".to_owned(),
                )),
            };
            return service_cleanup_outcome(control_result, Err(endpoint_error));
        }
    };
    let published = StopRequests::open(&data, &instance_id).and_then(|stops| {
        let marker = ServiceMarker {
            instance_id: instance_id.clone(),
            binary_identity,
        };
        PublishedServiceMarker::create(&data, &marker).map(|marker| (stops, marker))
    });
    let (stops, _marker) = match published {
        Ok(published) => published,
        Err(marker_error) => {
            let cleanup = shutdown_service_endpoint(endpoint, control).await;
            return service_cleanup_outcome(cleanup, Err(marker_error));
        }
    };
    if let Some(ready) = ready {
        // A starter that stopped waiting has nothing to be told.
        let _ = ready.signal();
    }
    let (service_result, requested_drain) = tokio::select! {
        signal = tokio::signal::ctrl_c() => (signal.map_err(display), None),
        drain_id = stops.next() => {
            control.begin_drain();
            (Ok(()), Some(drain_id))
        }
    };
    drop(stops);
    let result = service_cleanup_outcome(
        shutdown_service_endpoint(endpoint, control).await, service_result,
    );
    if let Some(drain_id) = requested_drain {
        let completion = write_service_drain_completion(&data, &instance_id, &drain_id, &result);
        return service_cleanup_outcome(result, completion);
    }
    result
}

pub(crate) async fn service_is_ready_for_identity(
    data: &Path,
    identity: &str,
) -> Result<bool, String> {
    let ping = ping_request()?;
    let answer = send_control_request_once(data, &ping).await;
    if let Ok(active) = &answer
        && active.get("identity").and_then(Value::as_str) == Some(identity)
    {
        return Ok(true);
    }
    // Only a service that holds its lock can still open an endpoint; when
    // none does, nothing needs waiting for.
    if claim_stopped_service(data, None)?.is_some() {
        return Ok(false);
    }
    // A running service says what it is through its marker, whatever
    // protocol it speaks, and a service of another binary is stopped
    // through the same contract.
    match ServiceMarker::read(data) {
        Some(marker) if marker.binary_identity != identity => {
            // Only the service is replaced: its state belongs to the stores,
            // each of which refuses a format it does not know.
            drop(drain_service(data, Some(&marker.instance_id)).await?);
            Ok(false)
        }
        // This binary's service, or one still starting: it answers soon.
        _ => match answer {
            Ok(_) | Err(ControlRequestError::Unavailable(_)) => Ok(false),
            Err(error) => Err(format!(
                "cannot safely identify the Acyclic service: {error}"
            )),
        },
    }
}

#[tracing::instrument(
    target = "acyclic_plugin",
    name = "acyclic.plugin.ensure_service",
    level = "info",
    skip_all
)]
pub(crate) async fn ensure_service(data: &Path) -> Result<(), String> {
    fs::create_dir_all(data).map_err(display)?;
    let identity = service_identity(data)?;
    if service_is_ready_for_identity(data, &identity).await? {
        return Ok(());
    }
    let ping = ping_request()?;
    let readiness = spawn_service_process(&current_executable()?)?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    // The service signals once it answers requests. A channel that closes
    // unsignalled means it exited, typically because another service holds
    // the lock and is starting; only then does this poll for that one.
    wait_for_service_readiness(readiness, deadline).await?;
    let last = loop {
        let last = match send_control_request_once(data, &ping).await {
            Ok(active)
                if active.get("identity").and_then(Value::as_str) == Some(identity.as_str()) =>
            {
                return Ok(());
            }
            Ok(_) => "the previous Acyclic service is still draining".to_owned(),
            Err(error) => error.to_string(),
        };
        if std::time::Instant::now() >= deadline {
            break last;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    };
    Err(format!("Acyclic service did not become ready: {last}"))
}

pub(crate) fn spawn_service_process(
    executable: &Path,
) -> Result<acyclic_native_runtime::ServiceReadiness, String> {
    acyclic_native_runtime::spawn_service_process(executable).map_err(display)
}

/// Waits, until `deadline`, for a started service to signal readiness or
/// exit. The blocking read runs on its own thread, which the process may
/// leave behind when it exits.
pub(crate) async fn wait_for_service_readiness(
    readiness: acyclic_native_runtime::ServiceReadiness,
    deadline: std::time::Instant,
) -> Result<(), String> {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    std::thread::Builder::new()
        .name("acyclic-service-readiness".to_owned())
        .spawn(move || {
            let _ = sender.send(readiness.wait());
        })
        .map_err(display)?;
    let wait = deadline.saturating_duration_since(std::time::Instant::now());
    let _ = tokio::time::timeout(wait, receiver).await;
    Ok(())
}

pub(crate) fn ping_request() -> Result<ControlRequest, String> {
    Ok(ControlRequest {
        version: 1,
        command: ControlCommand::Ping,
        cwd: env::current_dir().map_err(display)?,
        argv: Vec::new(),
        name: String::new(),
        arguments: Value::Null,
    })
}
