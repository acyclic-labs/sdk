//! Bounded control requests and responses.

use super::*;

#[derive(Debug)]
pub(crate) enum ControlRequestError {
    Unavailable(String),
    Indeterminate(String),
    Response(String),
    NativeWork(acyclic_harness::Error),
}

impl std::fmt::Display for ControlRequestError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unavailable(message) | Self::Indeterminate(message) | Self::Response(message) => {
                formatter.write_str(message)
            }
            Self::NativeWork(error) => std::fmt::Display::fmt(error, formatter),
        }
    }
}

impl std::error::Error for ControlRequestError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NativeWork(error) => Some(error),
            _ => None,
        }
    }
}

pub(crate) async fn send_control_request(
    data: &Path,
    request: &ControlRequest,
) -> Result<Value, ControlRequestError> {
    send_control_envelope(data, &ControlEnvelope::new(request.clone())).await
}

pub(crate) async fn send_control_envelope(
    data: &Path,
    envelope: &ControlEnvelope<ControlRequest>,
) -> Result<Value, ControlRequestError> {
    send_control_envelope_with_attempts(data, envelope, 50, control_request_wait(&envelope.request))
        .await
}


pub(crate) fn control_request_wait(request: &ControlRequest) -> std::time::Duration {
    match request.command {
        ControlCommand::Ping => CONTROL_PROBE_WAIT,
        ControlCommand::Hook if request.name.ends_with(":SessionEnd") => CONTROL_PROBE_WAIT,
        ControlCommand::Hook => CONTROL_HOOK_WAIT,
        ControlCommand::Doctor
        | ControlCommand::Git
        | ControlCommand::Agents
        | ControlCommand::Discard => CONTROL_COMMAND_WAIT,
    }
}

pub(crate) async fn send_control_request_once(
    data: &Path,
    request: &ControlRequest,
) -> Result<Value, ControlRequestError> {
    send_control_envelope_once(data, &ControlEnvelope::new(request.clone())).await
}

pub(crate) async fn send_control_envelope_once(
    data: &Path,
    envelope: &ControlEnvelope<ControlRequest>,
) -> Result<Value, ControlRequestError> {
    send_control_envelope_with_attempts(data, envelope, 1, control_request_wait(&envelope.request))
        .await
}


pub(crate) async fn send_control_envelope_with_attempts(
    data: &Path,
    envelope: &ControlEnvelope<ControlRequest>,
    windows_connect_attempts: usize,
    response_wait: std::time::Duration,
) -> Result<Value, ControlRequestError> {
    let deadline = tokio::time::Instant::now() + response_wait;
    #[cfg(not(windows))]
    let _ = windows_connect_attempts;
    let mut encoded = serde_json::to_vec(envelope)
        .map_err(|error| ControlRequestError::Unavailable(error.to_string()))?;
    if encoded.len() >= MAXIMUM_CONTROL_MESSAGE_BYTES {
        return Err(ControlRequestError::Response(
            "Acyclic request exceeds the maximum frame size".to_owned(),
        ));
    }
    encoded.push(b'\n');
    #[cfg(target_os = "linux")]
    return send_linux_mailbox_request(
        data,
        &encoded,
        &envelope.request_id,
        remaining_control_wait(deadline)?,
    )
    .await;
    #[cfg(not(target_os = "linux"))]
    {
        #[cfg(all(unix, not(target_os = "linux")))]
        let socket_path = unix_control_socket_path(data);
        #[cfg(all(unix, not(target_os = "linux")))]
        let stream = tokio::time::timeout(
            CONTROL_PROBE_WAIT.min(remaining_control_wait(deadline)?),
            tokio::net::UnixStream::connect(socket_path),
        )
        .await
        .map_err(|_| {
            ControlRequestError::Unavailable(
                "Acyclic service connection exceeded the probe deadline".to_owned(),
            )
        })?
        .map_err(|error| {
            ControlRequestError::Unavailable(format!("Acyclic service is not running: {error}"))
        })?;
        // Only this user's own service may answer.
        #[cfg(all(unix, not(target_os = "linux")))]
        if stream
            .peer_cred()
            .map_err(|error| ControlRequestError::Unavailable(error.to_string()))?
            .uid()
            != rustix::process::getuid().as_raw()
        {
            return Err(ControlRequestError::Unavailable(
                "Acyclic service socket is served by another user".to_owned(),
            ));
        }
        #[cfg(windows)]
        let stream = connect_windows_control_pipe(data, windows_connect_attempts, deadline).await?;
        exchange_control_stream(
            stream,
            &encoded,
            &envelope.request_id,
            remaining_control_wait(deadline)?,
        )
        .await
    }
}

pub(crate) fn remaining_control_wait(
    deadline: tokio::time::Instant,
) -> Result<std::time::Duration, ControlRequestError> {
    let now = tokio::time::Instant::now();
    if now >= deadline {
        return Err(ControlRequestError::Unavailable(
            "Acyclic service did not answer before the request deadline".to_owned(),
        ));
    }
    Ok(deadline - now)
}

#[cfg(not(target_os = "linux"))]
pub(crate) async fn exchange_control_stream(
    mut stream: impl AsyncRead + AsyncWrite + Unpin,
    encoded: &[u8],
    request_id: &control_protocol::RequestId,
    maximum_wait: std::time::Duration,
) -> Result<Value, ControlRequestError> {
    let exchange = async {
        stream
            .write_all(encoded)
            .await
            .map_err(|error| ControlRequestError::Indeterminate(error.to_string()))?;
        stream
            .flush()
            .await
            .map_err(|error| ControlRequestError::Indeterminate(error.to_string()))?;
        let mut response = Vec::new();
        let mut reader = BufReader::new(stream).take((MAXIMUM_CONTROL_MESSAGE_BYTES + 1) as u64);
        reader
            .read_until(b'\n', &mut response)
            .await
            .map_err(|error| ControlRequestError::Indeterminate(error.to_string()))?;
        Ok::<_, ControlRequestError>(response)
    };
    let mut response = tokio::time::timeout(maximum_wait, exchange)
        .await
        .map_err(|_| {
            ControlRequestError::Indeterminate(
                "Acyclic service did not answer before the request deadline".to_owned(),
            )
        })??;
    if response.len() > MAXIMUM_CONTROL_MESSAGE_BYTES || response.last() != Some(&b'\n') {
        return Err(ControlRequestError::Indeterminate(
            "invalid response from Acyclic service".to_owned(),
        ));
    }
    response.pop();
    decode_control_response(&response, request_id)
}

pub(crate) fn decode_control_response(
    response: &[u8],
    request_id: &control_protocol::RequestId,
) -> Result<Value, ControlRequestError> {
    let response: Value = serde_json::from_slice(response)
        .map_err(|error| ControlRequestError::Indeterminate(error.to_string()))?;
    if response.get("requestId").and_then(Value::as_str) != Some(request_id.as_str()) {
        return Err(ControlRequestError::Indeterminate(
            "Acyclic control response does not match the request identity".to_owned(),
        ));
    }
    if response.get("ok").and_then(Value::as_bool) == Some(true) {
        Ok(response.get("result").cloned().unwrap_or(Value::Null))
    } else {
        Err(ControlRequestError::Response(
            response
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("Acyclic service request failed")
                .to_owned(),
        ))
    }
}
