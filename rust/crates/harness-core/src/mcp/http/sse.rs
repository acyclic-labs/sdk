use super::{HttpMcpTransport, McpHttpRequest, body::bounded};
use crate::{
    Error, OperationId, Result,
    mcp::{
        callbacks::McpCallbackRequest,
        rpc::{notification, rpc_result},
    },
    runtime::ToolContext,
};
use acyclic_stream::BoxProviderStream;
use bytes::Bytes;
use futures::{FutureExt as _, StreamExt as _};
use serde_json::Value;
use std::collections::VecDeque;

fn tools_changed(value: &Value, generation: &std::sync::atomic::AtomicU64) {
    if value.get("method").and_then(Value::as_str) == Some("notifications/tools/list_changed") {
        generation.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) async fn notifications(
    body: BoxProviderStream<'static, Result<Bytes>>,
    operation: OperationId,
    maximum: Option<u32>,
    generation: &std::sync::atomic::AtomicU64,
    progressed: &mut bool,
) -> Result<()> {
    // The watch lives for the connection, so the protective bound applies to
    // each frame rather than the total number of notifications in its life.
    let frames = crate::sse::frames(
        body,
        maximum.map(|maximum| maximum as usize),
        crate::sse::Decoder::default(),
    );
    futures::pin_mut!(frames);
    while let Some(frame) = frames.next().await {
        let frame = frame.map_err(|error| match error {
            Error::ModelUnavailable {
                failure: crate::ModelTransportFailure::Interrupted,
                ..
            } => Error::Boundary {
                code: crate::wire::ErrorCode::Unavailable,
                reason: "MCP notification event was interrupted",
            },
            error => error,
        });
        if let Some(value) = decode(frame, operation)? {
            notification(&value, None)?;
            tools_changed(&value, generation);
            *progressed = true;
        }
    }
    Ok(())
}

pub(super) async fn response(
    body: BoxProviderStream<'static, Result<Bytes>>,
    operation: OperationId,
    maximum_bytes: Option<u32>,
    callbacks: Option<(&HttpMcpTransport, ToolContext)>,
    tools_generation: &std::sync::atomic::AtomicU64,
) -> Result<Value> {
    let mut spelling = [0; 36];
    let identity = uuid::Uuid::from_bytes(operation.into_bytes());
    let expected: &str = identity.hyphenated().encode_lower(&mut spelling);
    let events = crate::sse::frames(
        bounded(body, maximum_bytes, operation),
        maximum_bytes.map(|maximum| maximum as usize),
        crate::sse::Decoder::default(),
    );
    let mut events = core::pin::pin!(events);
    let mut pending = VecDeque::new();
    let mut requests = 0_u32;
    let mut stream_open = true;
    loop {
        let value = if let Some(value) = pending.pop_front() {
            value
        } else {
            let Some(event) = events.next().await else {
                return Err(Error::Indeterminate(operation));
            };
            let Some(value) = decode(event, operation)? else {
                continue;
            };
            value
        };
        if value.get("id").and_then(Value::as_str) == Some(expected) {
            return rpc_result(value, operation, None);
        }
        if value.get("method").is_none() || value.get("id").is_none() {
            notification(&value, None)?;
            tools_changed(&value, tools_generation);
            continue;
        }
        let (transport, context) = callbacks
            .as_ref()
            .ok_or_else(|| Error::Unsupported("MCP callback router is not installed".into()))?;
        let (policy, admission) = transport
            .callbacks
            .as_ref()
            .ok_or_else(|| Error::Unsupported("MCP callback policy is not installed".into()))?;
        requests = requests
            .checked_add(1)
            .filter(|count| *count <= policy.maximum_requests)
            .ok_or_else(|| Error::Invalid("MCP callback count exceeds admitted bound".into()))?;
        let callback = McpCallbackRequest {
            parent: operation,
            policy: policy.clone(),
            message: value,
        };
        let admitted = admission.admit(context.clone(), &callback).fuse();
        let mut admitted = core::pin::pin!(admitted);
        let (callback_operation, reply) = loop {
            if !stream_open {
                break admitted.await?;
            }
            futures::select! {
                reply = admitted => break reply?,
                event = events.next().fuse() => {
                    let Some(event) = event else {
                        stream_open = false;
                        if !pending.iter().any(|value: &Value| value.get("id").and_then(Value::as_str) == Some(expected)) {
                            return Err(Error::Indeterminate(operation));
                        }
                        continue;
                    };
                    let Some(value) = decode(event, operation)? else { continue; };
                    if value.get("method").and_then(Value::as_str) == Some("notifications/cancelled")
                        && value.pointer("/params/requestId") == callback.message.get("id") {
                        notification(&value, None)?;
                        return Err(Error::Indeterminate(operation));
                    }
                    if value.get("method").is_some() && value.get("id").is_none() { notification(&value, None)?; tools_changed(&value, tools_generation); }
                    else {
                        if pending.len() as u64 >= u64::from(policy.maximum_requests) {
                            return Err(Error::Invalid("MCP pending callback bound exceeded".into()));
                        }
                        pending.push_back(value);
                    }
                }
            }
        };
        let request: McpHttpRequest =
            transport.request("POST", crate::contract::canonical_json_bytes(&reply)?)?;
        let acknowledgement = transport
            .provider
            .exchange(callback_operation, request)
            .await
            .map_err(|_| Error::Indeterminate(operation))?;
        if acknowledgement.status != 202 {
            return Err(Error::Indeterminate(operation));
        }
    }
}

fn decode(event: Result<crate::sse::Frame>, operation: OperationId) -> Result<Option<Value>> {
    let event = event.map_err(|error| match error {
        Error::ModelUnavailable {
            failure: crate::ModelTransportFailure::Interrupted,
            ..
        } => Error::Indeterminate(operation),
        error => error,
    })?;
    if event.data().trim().is_empty() {
        return Ok(None);
    }
    crate::contract::json_from_slice(event.data().as_bytes())
        .map(Some)
        .map_err(|error| Error::Invalid(error.to_string()))
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn only_complete_valid_notifications_reset_recovery(
            width in 1_usize..128,
            valid in any::<bool>(),
            interrupted in any::<bool>(),
        ) {
            let message: &[u8] = if valid {
                b"data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/tools/list_changed\",\"params\":{}}\n\n"
            } else {
                b": keepalive\n\ndata: {\"jsonrpc\":\"2.0\",\"method\":7}\n\n"
            };
            let mut chunks: Vec<Result<Bytes>> = message.chunks(width)
                .map(|chunk| Ok(Bytes::copy_from_slice(chunk))).collect();
            if interrupted {
                chunks.push(Err(Error::Storage("connection ended".into())));
            }
            let generation = std::sync::atomic::AtomicU64::new(0);
            let mut progressed = false;
            let result = futures::executor::block_on(notifications(
                Box::pin(futures::stream::iter(chunks)),
                OperationId::new(),
                Some(8192),
                &generation,
                &mut progressed,
            ));
            prop_assert_eq!(progressed, valid);
            prop_assert_eq!(result.is_ok(), valid && !interrupted);
            prop_assert_eq!(generation.load(std::sync::atomic::Ordering::Acquire), u64::from(valid));
        }
    }
}
