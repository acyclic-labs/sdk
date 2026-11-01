use super::{HttpMcpTransport, McpHttpRequest};
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

pub(super) async fn response(
    body: BoxProviderStream<'static, Result<Bytes>>,
    operation: OperationId,
    maximum_bytes: Option<u32>,
    callbacks: Option<(&HttpMcpTransport, ToolContext)>,
) -> Result<Value> {
    let mut spelling = [0; 36];
    let identity = uuid::Uuid::from_bytes(operation.into_bytes());
    let expected: &str = identity.hyphenated().encode_lower(&mut spelling);
    let mut received = 0_usize;
    // Bound input before the parser buffers it.
    let bounded = body.map(move |chunk| -> Result<Bytes> {
        let chunk = chunk?;
        received = received
            .checked_add(chunk.len())
            .filter(|total| maximum_bytes.is_none_or(|maximum| *total as u64 <= u64::from(maximum)))
            .ok_or(Error::Indeterminate(operation))?;
        Ok(chunk)
    });
    let events = crate::sse::frames(
        bounded,
        maximum_bytes.map(|maximum| maximum as usize),
        crate::sse::Decoder::default(),
    );
    futures::pin_mut!(events);
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
        futures::pin_mut!(admitted);
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
                    if value.get("method").is_some() && value.get("id").is_none() { notification(&value, None)?; }
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
