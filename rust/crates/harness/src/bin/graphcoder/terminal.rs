//! Transient terminal presentation over the existing Harness wire boundary.

use acyclic_harness::{
    Error, Result, wire,
    wire_api::{
        HarnessWireApi, negotiate, validate_admission, validate_cancel_request,
        validate_cancel_response, validate_command_protocol, validate_observe_request,
        validate_operation_status, validate_resume_protocol,
    },
};
use futures::StreamExt as _;
use prost::Message as _;

pub const MAX_FRAME_BYTES: usize = 65_536;
pub const MAX_PAGE_EVENTS: usize = 16;

/// Dispatch exactly once. A caller must supply already scoped commands; the UI
/// neither creates grants nor retries uncertain admission or cancellation.
pub async fn dispatch(
    api: &dyn HarnessWireApi,
    frame: wire::client_frame::Frame,
) -> Result<Option<wire::server_frame::Frame>> {
    use wire::{client_frame::Frame as Request, server_frame::Frame as Response};
    match frame {
        Request::Handshake(request) => {
            let response = api.handshake(request.clone()).await?;
            let supported = response
                .supported
                .as_ref()
                .ok_or_else(|| Error::Conflict("handshake capabilities missing".into()))?;
            if negotiate(&request, supported)? != response {
                return Err(Error::Conflict("handshake identity mismatch".into()));
            }
            Ok(Some(Response::Handshake(response)))
        }
        Request::Command(command) => {
            validate_command_protocol(&command)?;
            let response = api.submit(command.clone()).await?;
            validate_admission(&command, &response)?;
            Ok(Some(Response::Admission(response)))
        }
        Request::Resume(request) => {
            validate_resume_protocol(&request)?;
            // One delivery per user request. Never collect a live stream or
            // hydrate a complete conversation to display an activity page.
            Ok(api
                .replay(request)
                .await?
                .next()
                .await
                .transpose()?
                .map(Response::Delivery))
        }
        Request::Observe(request) => {
            let control = validate_observe_request(&request)?;
            api.authorize_operation_control(&control).await?;
            let response = api.observe(request.clone()).await?;
            validate_operation_status(&request, &response)?;
            Ok(Some(Response::Status(response)))
        }
        Request::Cancel(request) => {
            let control = validate_cancel_request(&request)?.control;
            api.authorize_operation_control(&control).await?;
            let response = api.cancel(request.clone()).await?;
            validate_cancel_response(&request, &response)?;
            Ok(Some(Response::Cancellation(response)))
        }
        Request::Acknowledge(_) => Err(Error::Unsupported(
            "acknowledgement requires a bound replay transport".into(),
        )),
    }
}

/// Escape control characters from every provider-controlled string. Newlines
/// remain readable for transcripts and diffs; no terminal escape is executable.
pub fn text(value: &str) -> String {
    let mut output = String::new();
    for character in value.chars() {
        if character == '\n' {
            output.push('\n');
        } else {
            output.extend(character.escape_debug());
        }
    }
    output
}

/// Render the durable typed outcome as received. Display never resolves an
/// approval, interprets an effect as successful or changes the recorded bytes.
pub fn render(frame: &wire::ServerFrame) -> Result<String> {
    use std::fmt::Write as _;
    use wire::server_frame::Frame;
    if frame.encoded_len() > MAX_FRAME_BYTES {
        return Err(Error::Invalid("response exceeds terminal bounds".into()));
    }
    let Some(frame) = &frame.frame else {
        return Err(Error::Invalid("response frame is missing".into()));
    };
    match frame {
        Frame::Delivery(delivery) => {
            if delivery.events.len() > MAX_PAGE_EVENTS {
                return Err(Error::Invalid(
                    "activity page exceeds terminal bounds".into(),
                ));
            }
            let mut output = format!(
                "activity {}..{} generation={}\n",
                delivery.from_revision,
                delivery.through_revision,
                text(&delivery.generation),
            );
            for event in &delivery.events {
                let payload = std::str::from_utf8(&event.canonical_payload_json)
                    .map_err(|_| Error::Invalid("event payload is not UTF-8".into()))?;
                // Typed extension/diff/approval payloads remain uninterpreted
                // until the owning public presentation contracts land.
                writeln!(
                    output,
                    "#{} {} operation={}\n{}",
                    event.revision,
                    text(&event.event_type),
                    text(&event.operation_id),
                    text(payload),
                )
                .map_err(|error| Error::Invalid(error.to_string()))?;
            }
            Ok(output)
        }
        // Debug escapes strings and preserves unknown numeric enum values.
        // In particular, accepted admission is never labelled completion.
        frame => Ok(format!("{frame:?}\n")),
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
