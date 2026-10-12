use super::*;
#[cfg(feature = "filesystem-local")]
mod native;
pub(super) async fn execute(
    command: &str,
    backend: &mut Option<Backend>,
    cursor: &mut acyclic_harness::wire::ReplayCursor,
    #[cfg(feature = "filesystem-local")] parent: &mut Option<acyclic_harness::TaskId>,
    #[cfg(feature = "filesystem-local")] workers: &mut tokio::task::JoinSet<
        acyclic_harness::Result<String>,
    >,
    output: &mut impl std::io::Write,
) -> acyclic_harness::Result<String> {
    use acyclic_harness::{Error, wire, wire_api::current_protocol};
    let api = backend.as_ref().ok_or_else(|| {
        Error::Conflict(
            "session is closed after failed reopen; restart with the explicit config".into(),
        )
    })?;
    #[cfg(not(feature = "filesystem-local"))]
    let _ = output;
    if command == "/reset" {
        match api {
            Backend::Fixture(fixture) => *cursor = fixture.start_cursor(),
            #[cfg(feature = "filesystem-local")]
            Backend::Native(_) => {
                return Err(Error::Unsupported(
                    "native sessions never reset authoritative state".into(),
                ));
            }
        }
        return Ok("fixture display reset; no session restored\n".into());
    }
    let request = if command == "/next" {
        wire::client_frame::Frame::Resume(wire::ResumeRequest {
            protocol: Some(current_protocol()),
            cursors: vec![cursor.clone()],
        })
    } else if let Some(path) = command.strip_prefix("/cursor ") {
        let wire::client_frame::Frame::Resume(request) = read_request(path)? else {
            return Err(Error::Invalid(
                "cursor file must contain a Resume frame".into(),
            ));
        };
        acyclic_harness::wire_api::validate_resume_protocol(&request)?;
        if request.cursors.len() != 1 {
            return Err(Error::Invalid("select exactly one aggregate cursor".into()));
        }
        wire::client_frame::Frame::Resume(request)
    } else if let Some(path) = command.strip_prefix("/cancel ") {
        let request = read_request(path)?;
        if !matches!(request, wire::client_frame::Frame::Cancel(_)) {
            return Err(Error::Invalid(
                "cancel file must contain a Cancel frame".into(),
            ));
        }
        request
    } else if let Some(path) = command
        .strip_prefix("/input ")
        .or_else(|| command.strip_prefix("/approve "))
    {
        let request = read_request(path)?;
        let wire::client_frame::Frame::Command(envelope) = &request else {
            return Err(Error::Invalid(
                "input/approval file must contain a scoped Command frame".into(),
            ));
        };
        let expected = if command.starts_with("/input ") {
            "append_conversation_message"
        } else {
            "resolve_interaction"
        };
        if envelope.action_type != expected {
            return Err(Error::Invalid(
                "prepared command has the wrong input/approval action".into(),
            ));
        }
        request
    } else if let Some(path) = command.strip_prefix("/request ") {
        read_request(path)?
    } else {
        #[cfg(feature = "filesystem-local")]
        return native::execute(command, backend, parent, workers, output).await;
        #[cfg(not(feature = "filesystem-local"))]
        return Err(Error::Unsupported(
            "operation requires a durable product host; fixture input is not queued".into(),
        ));
    };
    let requested_cursor = match &request {
        wire::client_frame::Frame::Resume(request) => request.cursors.first().cloned(),
        _ => None,
    };
    let Some(frame) = terminal::dispatch(api.api(), request).await? else {
        if let Some(selected) = requested_cursor {
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
