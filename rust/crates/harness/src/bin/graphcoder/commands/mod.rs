use super::*;
#[cfg(feature = "filesystem-local")]
mod native;

pub(super) async fn execute_sdk(
    command: &str,
    arguments: &str,
    settings: &serde_json::Value,
    backend: &mut Option<Backend>,
    cursor: &mut acyclic_harness::wire::ReplayCursor,
    #[cfg(feature = "filesystem-local")] parent: &mut Option<acyclic_harness::TaskId>,
    #[cfg(feature = "filesystem-local")] workers: &mut tokio::task::JoinSet<
        acyclic_harness::Result<String>,
    >,
    output: &mut impl std::io::Write,
) -> acyclic_harness::Result<String> {
    use acyclic_harness::{Error, wire, wire_api::current_protocol};
    let api = backend
        .as_ref()
        .ok_or_else(|| Error::Conflict("session is closed".into()))?;
    #[cfg(not(feature = "filesystem-local"))]
    let _ = (output, settings);
    if command == "reset" {
        match api {
            Backend::Fixture(fixture) => *cursor = fixture.start_cursor(),
            #[cfg(feature = "filesystem-local")]
            Backend::Native(_) => {
                return Err(Error::Unsupported("native sessions cannot reset".into()));
            }
        }
        return Ok("fixture display reset; no session restored\n".into());
    }
    let request = if command == "next" {
        wire::client_frame::Frame::Resume(wire::ResumeRequest {
            protocol: Some(current_protocol()),
            cursors: vec![cursor.clone()],
        })
    } else if command == "cursor" {
        let wire::client_frame::Frame::Resume(request) = read_request(arguments)? else {
            return Err(Error::Invalid("cursor must contain a Resume frame".into()));
        };
        acyclic_harness::wire_api::validate_resume_protocol(&request)?;
        if request.cursors.len() != 1 {
            return Err(Error::Invalid("select exactly one aggregate cursor".into()));
        }
        wire::client_frame::Frame::Resume(request)
    } else if command == "approve" {
        let request = read_request(arguments)?;
        let wire::client_frame::Frame::Command(envelope) = &request else {
            return Err(Error::Invalid(
                "approval must contain a Command frame".into(),
            ));
        };
        if envelope.action_type != "resolve_interaction" {
            return Err(Error::Invalid("approval has the wrong action".into()));
        }
        request
    } else if command == "request" {
        read_request(arguments)?
    } else {
        #[cfg(feature = "filesystem-local")]
        return native::execute_sdk(
            command, arguments, settings, backend, parent, workers, output,
        )
        .await;
        #[cfg(not(feature = "filesystem-local"))]
        return Err(Error::Unsupported(
            "operation requires a durable product host".into(),
        ));
    };
    let selected = match &request {
        wire::client_frame::Frame::Resume(request) => request.cursors.first().cloned(),
        _ => None,
    };
    let Some(frame) = terminal::dispatch(api.api(), request).await? else {
        if let Some(selected) = selected {
            *cursor = selected;
        }
        return Ok(if matches!(api, Backend::Fixture(_)) {
            "end of fixture activity\n"
        } else {
            "end of current activity page\n"
        }
        .into());
    };
    let response = wire::ServerFrame { frame: Some(frame) };
    let rendered = terminal::render(&response)?;
    if let Some(wire::server_frame::Frame::Delivery(delivery)) = response.frame {
        *cursor = wire::ReplayCursor {
            authority: delivery.authority,
            generation: delivery.generation,
            revision: delivery.through_revision,
        };
    }
    Ok(rendered)
}
