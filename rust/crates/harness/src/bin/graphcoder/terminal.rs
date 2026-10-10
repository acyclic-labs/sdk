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
pub const MAX_DISPLAY_BYTES: usize = 262_144;
pub const MAX_DISPLAY_PAGES: usize = 64;

/// A finite, transient scrollback. Evicted pages are recovered by an explicit
/// archival read, never by hydrating all descendant conversations.
#[derive(Default)]
pub struct Scrollback {
    pages: std::collections::VecDeque<String>,
    bytes: usize,
}

impl Scrollback {
    pub fn push(&mut self, page: String) -> Result<()> {
        if page.len() > MAX_DISPLAY_BYTES {
            return Err(Error::Invalid("rendered page exceeds display bounds".into()));
        }
        while self.pages.len() >= MAX_DISPLAY_PAGES
            || self.bytes.saturating_add(page.len()) > MAX_DISPLAY_BYTES
        {
            if let Some(previous) = self.pages.pop_front() {
                self.bytes -= previous.len();
            } else {
                break;
            }
        }
        self.bytes += page.len();
        self.pages.push_back(page);
        Ok(())
    }

    pub fn page(&self, back: usize) -> Option<&str> {
        self.pages.len().checked_sub(back + 1)
            .and_then(|index| self.pages.get(index))
            .map(String::as_str)
    }
}

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
    let output = match frame {
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
                let presentation = render_payload(&event.event_type, &event.canonical_payload_json)?;
                writeln!(
                    output,
                    "#{} {} operation={}\n{}",
                    event.revision,
                    text(&event.event_type),
                    text(&event.operation_id),
                    presentation,
                )
                .map_err(|error| Error::Invalid(error.to_string()))?;
            }
            output
        }
        // Debug escapes strings and preserves unknown numeric enum values.
        // In particular, accepted admission is never labelled completion.
        frame => format!("{frame:?}\n"),
    };
    if output.len() > MAX_DISPLAY_BYTES {
        return Err(Error::Invalid("rendered response exceeds display bounds".into()));
    }
    Ok(output)
}

/// Layout the existing semantic event types without resolving any referenced
/// content, interpreting grants, or conflating task and operation identities.
fn render_payload(event_type: &str, bytes: &[u8]) -> Result<String> {
    use acyclic_harness::core::EventPayload;
    let decoded = acyclic_harness::wire_codec::decode_event_payload(event_type, bytes)?;
    let rendered = match decoded {
        EventPayload::ConversationMessageAppended { message } => format!(
            "transcript {:?} message={} sequence={}\ncontent {}\n",
            message.kind, message.id, message.sequence,
            serde_json::to_string(&message.content)
                .map_err(|error| Error::Invalid(error.to_string()))?,
        ),
        EventPayload::InteractionOpened { ticket } => format!(
            "interaction {:?} id={} approval={:?} deadline={:?}\nrequest {}\n\
             Decision requires an exact scoped response; displaying is not approval.\n",
            ticket.kind, ticket.id, ticket.approval, ticket.deadline_unix_ms,
            serde_json::to_string(&ticket.request)
                .map_err(|error| Error::Invalid(error.to_string()))?,
        ),
        EventPayload::InteractionResolved { resolution } => {
            format!("interaction outcome {resolution:?}\n")
        }
        EventPayload::ForkPublished { seed } => {
            format!("fork {seed:?}\n")
        }
        EventPayload::ProjectMergePublished { receipt } => format!(
            "merge publication={} child={:?}\nsource={:?} target={:?}\n\
             expected={:?} result={:?}\nnotice {}\n",
            receipt.operation_id, receipt.child,
            receipt.source_generation, receipt.target_project,
            receipt.expected_target_generation, receipt.result_generation,
            serde_json::to_string(&receipt.notice.content)
                .map_err(|error| Error::Invalid(error.to_string()))?,
        ),
        _ => return Ok(text(std::str::from_utf8(bytes)
            .map_err(|_| Error::Invalid("event payload is not UTF-8".into()))?)),
    };
    Ok(text(&rendered))
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
