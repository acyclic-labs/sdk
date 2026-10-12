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
use serde_json::{Value, json};
use std::collections::VecDeque;

pub(super) async fn response(
    body: BoxProviderStream<'static, Result<Bytes>>,
    operation: OperationId,
    maximum_bytes: u32,
    callbacks: Option<(&HttpMcpTransport, ToolContext)>,
) -> Result<Value> {
    let mut received = 0_usize;
    // Bound input before the parser buffers it.
    let bounded = body.map(move |chunk| -> Result<Bytes> {
        let chunk = chunk?;
        received = received
            .checked_add(chunk.len())
            .filter(|total| *total as u64 <= u64::from(maximum_bytes))
            .ok_or(Error::Indeterminate(operation))?;
        Ok(chunk)
    });
    let events = sse_stream::SseByteStream::new(bounded);
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
            let Some(value) = decode(event)? else {
                continue;
            };
            value
        };
        if value.get("id") == Some(&json!(operation.to_string())) {
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
        let callback_operation = callback.operation()?;
        callback.validate()?;
        let admitted = admission.admit(context.clone(), &callback).fuse();
        futures::pin_mut!(admitted);
        let reply = loop {
            if !stream_open {
                break admitted.await?;
            }
            futures::select! {
                reply = admitted => break reply?,
                event = events.next().fuse() => {
                    let Some(event) = event else {
                        stream_open = false;
                        if !pending.iter().any(|value: &Value| value.get("id") == Some(&json!(operation.to_string()))) {
                            return Err(Error::Indeterminate(operation));
                        }
                        continue;
                    };
                    let Some(value) = decode(event)? else { continue; };
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
        callback.validate_reply(&reply)?;
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

fn decode(event: std::result::Result<sse_stream::Sse, sse_stream::Error>) -> Result<Option<Value>> {
    let event = event.map_err(|error| match error {
        sse_stream::Error::Body(error) => error
            .downcast::<Error>()
            .map_or_else(|error| Error::Invalid(error.to_string()), |error| *error),
        error => Error::Invalid(error.to_string()),
    })?;
    event
        .data
        .filter(|data| !data.trim().is_empty())
        .map(|data| {
            crate::contract::json_from_slice(data.as_bytes())
                .map_err(|error| Error::Invalid(error.to_string()))
        })
        .transpose()
}
