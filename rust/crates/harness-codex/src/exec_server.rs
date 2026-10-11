//! Original-bound mediation for the pinned native Codex exec-server protocol.
use acyclic_harness::{Error, OperationId, Result};
use futures::{future::BoxFuture, SinkExt, StreamExt};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::{mpsc, watch, Mutex, Notify, Semaphore};

/// One immutable registry binding for an upstream exec-server method. The
/// registered tool receives the complete upstream request without rewriting its
/// arguments, process IDs, write IDs, or optional telemetry attribution.
pub struct BuiltinToolBinding {
    /// Exact method exported by the pinned exec-server protocol.
    pub method: String,
    /// Exact admitted registry revision, including hidden host-only tools.
    pub tool: acyclic_harness::runtime::ToolRef<serde_json::Value, serde_json::Value>,
}

/// Durable dispatch gate for the real Codex builtin execution transport.
///
/// This opens no listener and grants no peer authority. The process host must
/// attach it to the owned, original-bound exec-server transport, forward genuine
/// provider notifications, and never install an unmediated/local fallback.
/// Root/provider authorization and financial permits belong inside the pinned
/// durable tool executors, before they forward any upstream request.
pub struct BuiltinMediator {
    context: acyclic_harness::runtime::TaskContext,
    bindings: std::collections::BTreeMap<String, acyclic_harness::runtime::ToolRef<serde_json::Value, serde_json::Value>>,
}

impl BuiltinMediator {
    /// Binds only actual admitted tools, never a claimed receipt or live-only
    /// executor. The caller supplies revisions from the accepted factory.
    pub fn new(
        context: acyclic_harness::runtime::TaskContext,
        bindings: Vec<BuiltinToolBinding>,
    ) -> Result<Self> {
        if context.durable_task_id().is_none() {
            return Err(Error::Unauthorized("builtin mediation requires an admitted durable task".into()));
        }
        let mut pinned = std::collections::BTreeMap::new();
        for binding in bindings {
            if !builtin_method(&binding.method) {
                return Err(Error::Unsupported(format!("unsupported exec-server method {}", binding.method)));
            }
            let definition = binding.tool.definition();
            let admitted = context.tool::<serde_json::Value, serde_json::Value>(
                &format!("{}@{}", definition.name, definition.revision),
            )?;
            if admitted.definition() != definition {
                return Err(Error::Conflict("builtin tool differs from its admitted revision".into()));
            }
            if pinned.insert(binding.method, binding.tool).is_some() {
                return Err(Error::Conflict("duplicate builtin method binding".into()));
            }
        }
        for method in BUILTIN_METHODS {
            if !pinned.contains_key(*method) {
                return Err(Error::Unauthorized(format!("exec-server method {method} is not admitted")));
            }
        }
        Ok(Self { context, bindings: pinned })
    }

    /// Original SDK parent, retained across transport reconnects.
    #[must_use]
    pub fn operation_id(&self) -> OperationId { self.context.id() }

    /// Admits/reconciles one actual upstream request through the SDK durable
    /// runner. A repeated ID with different method/arguments conflicts; it does
    /// not acquire a fresh mutation identity after a lost ACK.
    ///
    /// A successful SDK tool outcome is not a correlated protocol receipt until
    /// its actual response envelope validates. Missing, malformed, mismatched,
    /// or notification-only replies remain indeterminate under the original
    /// operation; this gate never dispatches a replacement invocation.
    pub async fn dispatch(
        &self,
        request: codex_exec_server_protocol::JSONRPCRequest,
    ) -> Result<codex_exec_server_protocol::JSONRPCMessage> {
        use acyclic_harness::{Outcome, executor::encode_json, tool::ToolInvocation};
        let binding = self.bindings.get(&request.method)
            .ok_or_else(|| Error::Unauthorized(format!("exec-server method {} is not admitted", request.method)))?;
        // Hash the tagged canonical ID: integer 1 and string "1" remain distinct.
        // Optional thread/tool attribution is telemetry, never a capability.
        let call_id = format!("codex-exec-{}", blake3::hash(&encode_json(&request.id)?).to_hex());
        let request_id = request.id.clone();
        let arguments = serde_json::to_value(request).map_err(|error| Error::Invalid(error.to_string()))?;
        let invocation = ToolInvocation::for_model_call(
            self.context.id(), 0, call_id, binding.definition().name.clone(), arguments,
        );
        let value = match self.context.call_durable(invocation.operation_id, binding, invocation.arguments).await? {
            Outcome::Succeeded(value) => value,
            Outcome::Failed { message } => return Err(Error::Invalid(message)),
            Outcome::Cancelled => return Err(Error::InteractionRejected(acyclic_harness::InteractionRejection::Cancelled)),
            Outcome::Indeterminate { operation_id } => return Err(Error::Indeterminate(operation_id)),
        };
        correlated_reply(value, &request_id, invocation.operation_id)
    }
}

fn correlated_reply(
    value: serde_json::Value,
    request_id: &codex_exec_server_protocol::RequestId,
    operation_id: OperationId,
) -> Result<codex_exec_server_protocol::JSONRPCMessage> {
    use codex_exec_server_protocol::JSONRPCMessage;
    // The durable runtime already owns decoded JSON. Consume that value through
    // the upstream bounded envelope decoder; do not encode and copy the entire
    // file/process/HTTP response merely to parse it a second time.
    let response: JSONRPCMessage = serde_json::from_value(value)
        .map_err(|_| Error::Indeterminate(operation_id))?;
    match &response {
        JSONRPCMessage::Response(reply) if &reply.id == request_id => Ok(response),
        JSONRPCMessage::Error(reply) if &reply.id == request_id => Ok(response),
        _ => Err(Error::Indeterminate(operation_id)),
    }
}

/// Complete builtin request surface of the pinned protocol. Native hosts resolve
/// these bindings from the accepted registry before launching Codex.
pub const BUILTIN_METHODS: &[&str] = {
    use codex_exec_server_protocol::*;
    &[
        INITIALIZE_METHOD, EXEC_METHOD, EXEC_READ_METHOD, EXEC_WRITE_METHOD,
        EXEC_SIGNAL_METHOD, EXEC_TERMINATE_METHOD, ENVIRONMENT_INFO_METHOD,
        ENVIRONMENT_STATUS_METHOD, ENVIRONMENT_CONFIG_READ_METHOD,
        FS_READ_FILE_METHOD, FS_OPEN_METHOD, FS_READ_BLOCK_METHOD, FS_CLOSE_METHOD,
        FS_WRITE_FILE_METHOD, FS_CREATE_DIRECTORY_METHOD, FS_GET_METADATA_METHOD,
        FS_CANONICALIZE_METHOD, FS_READ_DIRECTORY_METHOD, FS_WALK_METHOD,
        FS_REMOVE_METHOD, FS_COPY_METHOD, CAPABILITY_ROOTS_DISCOVER_METHOD,
        HTTP_REQUEST_METHOD,
    ]
};

fn builtin_method(method: &str) -> bool {
    BUILTIN_METHODS.contains(&method)
}

/// Authorization supplied by the trusted native process host holding the actual
/// original FD and Root's canonical admitted task. Implementations check Root's
/// current coordinator lease, task/configuration and physical original fence;
/// neither a URL, `EffectScope`, nor a caller-supplied descriptor is authority.
pub trait ExecServerAuthority: Send + Sync {
    /// Checks the original admission before launch, upgrade and every command.
    fn authorize<'a>(
        &'a self,
        context: &'a acyclic_harness::runtime::TaskContext,
    ) -> BoxFuture<'a, Result<()>>;

    /// Acknowledges the genuine initialized session at its original Root owner.
    fn initialized<'a>(
        &'a self,
        context: &'a acyclic_harness::runtime::TaskContext,
    ) -> BoxFuture<'a, Result<()>>;

    /// Resolves when actual held original/process-host custody closes.
    fn closed(&self) -> BoxFuture<'_, ()>;
}

const MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;
const MAX_PENDING_REQUESTS: usize = 128;

/// Authenticated private endpoint retained by the actual native process host.
///
/// The URL contains a source-generated secret, not a Root credential or SDK
/// grant. Give it only to the pinned Codex child's `CODEX_EXEC_SERVER_URL`;
/// exclude it, the model proxy and MCP credentials from shell inheritance using
/// Codex's supported shell environment policy, never by rewriting exec params.
/// Dropping the endpoint revokes it immediately. `close` also drains admitted
/// durable calls without cancelling a possibly completed original mutation.
pub struct ExecServerEndpoint {
    url: String,
    operation_id: OperationId,
    shared: Arc<SessionShared>,
    task: Option<tokio::task::JoinHandle<Result<()>>>,
}

impl std::fmt::Debug for ExecServerEndpoint {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("ExecServerEndpoint")
            .field("operation_id", &self.operation_id)
            .finish_non_exhaustive()
    }
}

struct SessionShared {
    mediator: BuiltinMediator,
    authority: Arc<dyn ExecServerAuthority>,
    path: String,
    active: AtomicBool,
    stop: watch::Sender<bool>,
    connection: Mutex<Option<watch::Sender<bool>>>,
    notifications: Mutex<mpsc::Receiver<codex_exec_server_protocol::JSONRPCNotification>>,
    permits: Arc<Semaphore>,
    pending: AtomicUsize,
    drained: Notify,
}

impl SessionShared {
    async fn authorize(&self) -> Result<()> {
        if !self.active.load(Ordering::Acquire) {
            return Err(Error::Unauthorized("original exec-server session closed".into()));
        }
        self.authority.authorize(&self.mediator.context).await?;
        if !self.active.load(Ordering::Acquire) {
            return Err(Error::Unauthorized("original exec-server session closed".into()));
        }
        Ok(())
    }

    fn revoke(&self) {
        self.active.store(false, Ordering::Release);
        self.stop.send_replace(true);
    }

    async fn drain(&self) {
        while self.pending.load(Ordering::Acquire) != 0 {
            // notify_one retains a permit if the last call finishes between
            // checking the counter and polling this future.
            self.drained.notified().await;
        }
    }
}

struct PendingCall(Arc<SessionShared>);

impl Drop for PendingCall {
    fn drop(&mut self) {
        if self.0.pending.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.drained.notify_one();
        }
    }
}

impl ExecServerEndpoint {
    /// Starts only inside the held native original. Notifications must come
    /// from that original's real owned process/HTTP producer, never an echo or
    /// another original's event stream. No local builtin fallback is installed.
    pub async fn start(
        mediator: BuiltinMediator,
        authority: Arc<dyn ExecServerAuthority>,
        notifications: mpsc::Receiver<codex_exec_server_protocol::JSONRPCNotification>,
    ) -> Result<Self> {
        authority.authorize(&mediator.context).await?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await
            .map_err(|_| Error::Unsupported("private exec-server bind failed".into()))?;
        let address = listener.local_addr()
            .map_err(|_| Error::Unsupported("private exec-server address unavailable".into()))?;
        // UUID v4 already uses the SDK's OS cryptographic randomness. Three
        // independent values have 366 random bits; hashing produces a 256-bit
        // session secret without a new entropy dependency or reserved UUID bits.
        let mut entropy = blake3::Hasher::new();
        entropy.update(b"acyclic:codex:exec-server:session:v1");
        for _ in 0..3 {
            entropy.update(uuid::Uuid::new_v4().as_bytes());
        }
        let path = format!("/original/{}", entropy.finalize().to_hex());
        let url = format!("ws://{address}{path}");
        let operation_id = mediator.operation_id();
        let (stop, _) = watch::channel(false);
        let shared = Arc::new(SessionShared {
            mediator, authority, path, active: AtomicBool::new(true), stop,
            connection: Mutex::new(None), notifications: Mutex::new(notifications),
            permits: Arc::new(Semaphore::new(MAX_PENDING_REQUESTS)),
            pending: AtomicUsize::new(0), drained: Notify::new(),
        });
        let task = tokio::spawn(serve_private(listener, shared.clone()));
        Ok(Self { url, operation_id, shared, task: Some(task) })
    }

    /// Secret-bearing URL for the trusted native launcher only.
    #[must_use]
    pub fn url(&self) -> &str { &self.url }

    /// The same SDK parent as the held original's builtin mediator.
    #[must_use]
    pub fn operation_id(&self) -> OperationId { self.operation_id }

    /// Observes actual lease closure or an authenticated protocol/producer
    /// failure. A native host selects this against the actual Codex process and
    /// terminates its owned process group; cancelling the wait keeps ownership.
    pub async fn wait_closed(&mut self) -> Result<()> {
        let result = match self.task.as_mut() {
            Some(task) => match task.await {
                Ok(result) => result,
                Err(_) => Err(Error::Indeterminate(self.operation_id)),
            },
            None => return Ok(()),
        };
        self.task.take();
        result
    }

    /// Revokes all channels and waits for original durable calls to settle.
    pub async fn close(mut self) -> Result<()> {
        self.shared.revoke();
        let result = self.wait_closed().await;
        self.shared.drain().await;
        result
    }
}

impl Drop for ExecServerEndpoint {
    fn drop(&mut self) {
        // Do not abort the supervisor or durable request tasks. Revocation
        // wakes it, closes socket owners and lets original calls persist/drain.
        self.shared.revoke();
    }
}

async fn serve_private(
    listener: tokio::net::TcpListener,
    shared: Arc<SessionShared>,
) -> Result<()> {
    let mut stop = shared.stop.subscribe();
    let mut connections = tokio::task::JoinSet::new();
    let handshakes = Arc::new(Semaphore::new(MAX_PENDING_REQUESTS));
    let mut result = Ok(());
    loop {
        if !shared.active.load(Ordering::Acquire) { break; }
        tokio::select! {
            _ = stop.changed() => break,
            _ = shared.authority.closed() => {
                result = Err(Error::Indeterminate(shared.mediator.operation_id()));
                break;
            }
            accepted = listener.accept() => {
                let (stream, _) = match accepted {
                    Ok(value) => value,
                    Err(_) => {
                        result = Err(Error::Unsupported("private exec-server accept failed".into()));
                        break;
                    }
                };
                let Ok(permit) = handshakes.clone().try_acquire_owned() else {
                    drop(stream);
                    continue;
                };
                let shared = shared.clone();
                connections.spawn(async move {
                    let _permit = permit;
                    private_connection(stream, shared).await
                });
            }
            joined = connections.join_next(), if !connections.is_empty() => {
                match joined {
                    Some(Ok(Err(error))) => { result = Err(error); break; }
                    Some(Err(_)) => {
                        result = Err(Error::Indeterminate(shared.mediator.operation_id()));
                        break;
                    }
                    _ => {}
                }
            }
        }
    }
    shared.revoke();
    drop(listener);
    connections.abort_all();
    while connections.join_next().await.is_some() {}
    // Wake the host before draining: it must terminate the actual Codex process
    // group first. Endpoint::close then drains retained original durable calls.
    result
}

fn exact_session_path(actual: &str, expected: &str) -> bool {
    use subtle::ConstantTimeEq;
    bool::from(actual.as_bytes().ct_eq(expected.as_bytes()))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SessionPhase { New, Initializing, AwaitInitialized, Ready }

async fn private_connection(
    stream: tokio::net::TcpStream,
    shared: Arc<SessionShared>,
) -> Result<()> {
    use codex_exec_server_protocol::*;
    use tokio_tungstenite::tungstenite::{
        handshake::server::{Request, Response},
        http::{Response as HttpResponse, StatusCode},
        protocol::WebSocketConfig,
        Message,
    };
    shared.authorize().await?;
    let config = WebSocketConfig::default()
        .max_message_size(Some(MAX_MESSAGE_BYTES))
        .max_frame_size(Some(MAX_MESSAGE_BYTES))
        .max_write_buffer_size(MAX_MESSAGE_BYTES + 128 * 1024);
    let callback_shared = shared.clone();
    let handshake = tokio_tungstenite::accept_hdr_async_with_config(
        stream,
        move |request: &Request, response: Response| {
            if callback_shared.active.load(Ordering::Acquire)
                && request.uri().query().is_none()
                && exact_session_path(request.uri().path(), &callback_shared.path) {
                Ok(response)
            } else {
                let mut denied = HttpResponse::new(Some("private session unavailable".to_owned()));
                *denied.status_mut() = StatusCode::FORBIDDEN;
                Err(denied)
            }
        },
        Some(config),
    );
    // A peer without the private route may only lose its own socket. It must
    // not revoke the original by probing this loopback listener.
    let mut socket = match tokio::time::timeout(std::time::Duration::from_secs(10), handshake).await {
        Ok(Ok(socket)) => socket,
        _ => return Ok(()),
    };
    shared.authorize().await?;
    let (connection_stop, mut replaced) = watch::channel(false);
    if let Some(previous) = shared.connection.lock().await.replace(connection_stop) {
        previous.send_replace(true);
    }
    let mut stop = shared.stop.subscribe();
    let mut notifications = tokio::select! {
        _ = stop.changed() => return Ok(()),
        _ = replaced.changed() => return Ok(()),
        receiver = shared.notifications.lock() => receiver,
    };
    let (completed, mut replies) = mpsc::channel::<(bool, acyclic_harness::Result<JSONRPCMessage>)>(MAX_PENDING_REQUESTS);
    let mut phase = SessionPhase::New;
    loop {
        tokio::select! {
            _ = stop.changed() => break,
            _ = replaced.changed() => break,
            incoming = socket.next() => {
                let message = match incoming {
                    Some(Ok(Message::Text(text))) => serde_json::from_str::<JSONRPCMessage>(&text)
                        .map_err(|_| Error::Invalid("invalid pinned exec-server envelope".into()))?,
                    Some(Ok(Message::Ping(bytes))) => {
                        socket.send(Message::Pong(bytes)).await
                            .map_err(|_| Error::Indeterminate(shared.mediator.operation_id()))?;
                        continue;
                    }
                    Some(Ok(Message::Pong(_))) => continue,
                    Some(Ok(Message::Close(_))) | None => break,
                    _ => return Err(Error::Invalid("unsupported exec-server frame".into())),
                };
                match message {
                    JSONRPCMessage::Request(request) => {
                        let initialize = request.method == INITIALIZE_METHOD;
                        if (initialize && phase != SessionPhase::New)
                            || (!initialize && phase != SessionPhase::Ready) {
                            return Err(Error::Unauthorized("exec-server session is not initialized".into()));
                        }
                        let permit = shared.permits.clone().try_acquire_owned()
                            .map_err(|_| Error::Unsupported("exec-server request capacity exhausted".into()))?;
                        if initialize { phase = SessionPhase::Initializing; }
                        shared.pending.fetch_add(1, Ordering::AcqRel);
                        let shared = shared.clone();
                        let completed = completed.clone();
                        let pending = PendingCall(shared.clone());
                        // This owned task outlives a disconnected/replaced socket.
                        // Dropping its observer must not cancel an original effect
                        // or mint a replacement identity after an ACK was lost.
                        tokio::spawn(async move {
                            let _pending = pending;
                            let _permit = permit;
                            let response = match shared.authorize().await {
                                Ok(()) => shared.mediator.dispatch(request).await,
                                Err(error) => Err(error),
                            };
                            let _ = completed.send((initialize, response)).await;
                        });
                    }
                    JSONRPCMessage::Notification(notification)
                        if notification.method == INITIALIZED_METHOD && phase == SessionPhase::AwaitInitialized => {
                        shared.authorize().await?;
                        shared.authority.initialized(&shared.mediator.context).await?;
                        phase = SessionPhase::Ready;
                    }
                    _ => return Err(Error::Unauthorized("unsupported exec-server client message".into())),
                }
            }
            reply = replies.recv() => {
                let Some((initialize, reply)) = reply else { break; };
                let reply = reply?;
                if initialize {
                    if let JSONRPCMessage::Response(response) = &reply {
                        let _: InitializeResponse = serde::Deserialize::deserialize(&response.result)
                            .map_err(|_| Error::Indeterminate(shared.mediator.operation_id()))?;
                        phase = SessionPhase::AwaitInitialized;
                    } else {
                        send_message(&mut socket, &reply, shared.mediator.operation_id()).await?;
                        break;
                    }
                }
                send_message(&mut socket, &reply, shared.mediator.operation_id()).await?;
            }
            notification = notifications.recv(), if phase == SessionPhase::Ready => {
                let Some(notification) = notification else {
                    return Err(Error::Unsupported("original producer event stream closed".into()));
                };
                if !matches!(notification.method.as_str(),
                    EXEC_OUTPUT_DELTA_METHOD | EXEC_EXITED_METHOD | EXEC_CLOSED_METHOD |
                    HTTP_REQUEST_BODY_DELTA_METHOD) {
                    return Err(Error::Invalid("unsupported original producer notification".into()));
                }
                shared.authorize().await?;
                send_message(&mut socket, &JSONRPCMessage::Notification(notification),
                    shared.mediator.operation_id()).await?;
            }
        }
    }
    let _ = socket.close(None).await;
    Ok(())
}

async fn send_message(
    socket: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    message: &codex_exec_server_protocol::JSONRPCMessage,
    original: OperationId,
) -> Result<()> {
    let text = serde_json::to_string(message).map_err(|_| Error::Indeterminate(original))?;
    if text.len() > MAX_MESSAGE_BYTES {
        return Err(Error::Indeterminate(original));
    }
    socket.send(tokio_tungstenite::tungstenite::Message::Text(text.into())).await
        .map_err(|_| Error::Indeterminate(original))
}

#[cfg(test)]
mod tests {
    use super::*;
    use codex_exec_server_protocol::{
        JSONRPCError, JSONRPCErrorError, JSONRPCMessage, JSONRPCNotification,
        JSONRPCResponse, RequestId,
    };

    #[test]
    fn private_route_rejects_every_changed_secret_byte_and_foreign_original_route() {
        let expected = "/original/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        assert!(exact_session_path(expected, expected));
        for position in "/original/".len()..expected.len() {
            let mut changed = expected.as_bytes().to_vec();
            changed[position] ^= 1;
            let changed = String::from_utf8(changed).expect("ASCII route mutation");
            assert!(!exact_session_path(&changed, expected));
        }
        assert!(!exact_session_path("/original/foreign", expected));
        assert!(!exact_session_path(&format!("{expected}/"), expected));
    }

    #[test]
    fn actual_upstream_response_and_error_keep_the_original_request_identity() -> Result<()> {
        let operation = OperationId::from_bytes([7; 16]);
        let id = RequestId::String("original-process-start".into());
        let response = JSONRPCMessage::Response(JSONRPCResponse {
            id: id.clone(),
            result: serde_json::json!({"processId": "protocol-process-id"}),
        });
        let encoded = serde_json::to_value(&response)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(correlated_reply(encoded, &id, operation)?, response);
        let rejected = JSONRPCMessage::Error(JSONRPCError {
            id: id.clone(),
            error: JSONRPCErrorError {
                code: -32602,
                data: None,
                message: "original request rejected".into(),
            },
        });
        let encoded = serde_json::to_value(&rejected)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(correlated_reply(encoded, &id, operation)?, rejected);
        Ok(())
    }

    #[test]
    fn malformed_mismatched_and_notification_replies_remain_original_indeterminate() -> Result<()> {
        let operation = OperationId::from_bytes([11; 16]);
        let id = RequestId::Integer(1);
        let mismatched = JSONRPCMessage::Response(JSONRPCResponse {
            id: RequestId::String("1".into()),
            result: serde_json::json!({"processId": "original-process"}),
        });
        let notification = JSONRPCMessage::Notification(JSONRPCNotification {
            method: codex_exec_server_protocol::EXEC_EXITED_METHOD.into(),
            params: Some(serde_json::json!({"processId": "original-process", "exitCode": 0})),
        });
        let values = [
            serde_json::Value::Null,
            serde_json::json!({"id": 1, "error": {"code": "not-an-integer", "message": "invalid provider reply"}}),
            serde_json::to_value(mismatched).map_err(|error| Error::Invalid(error.to_string()))?,
            serde_json::to_value(notification).map_err(|error| Error::Invalid(error.to_string()))?,
        ];
        for value in values {
            assert!(matches!(
                correlated_reply(value, &id, operation),
                Err(Error::Indeterminate(actual)) if actual == operation
            ));
        }
        Ok(())
    }
}
