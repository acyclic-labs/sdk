//! Responses for the service.
use super::*;

pub(crate) fn uncorrelated_control_response(
    error: &str,
) -> acyclic_harness::Result<ControlEncodedResponse> {
    Ok((
        encode_control_response(&json!({"version":2,"ok":false,"error":error}), None),
        None,
        None,
    ))
}

pub(crate) fn invalid_control_request_response(
    request: &[u8],
    error: &serde_json::Error,
) -> acyclic_harness::Result<ControlEncodedResponse> {
    let message = format!("invalid Acyclic control request: {error}");
    let request_id = serde_json::from_slice::<Value>(request)
        .ok()
        .and_then(|value| value.get("requestId")?.as_str().map(str::to_owned))
        .and_then(|value| control_protocol::RequestId::from_wire(value).ok());
    match request_id {
        Some(request_id) => control_response_for(&request_id, Err(Error::Invalid(message)), None),
        None => uncorrelated_control_response(&message),
    }
}

pub(crate) fn serialize_control_error<S: serde::Serializer>(
    error: &Error,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.collect_str(error)
}

/// Encodes the response for one request, bounded to one control message.
pub(crate) fn control_response_for(
    request_id: &control_protocol::RequestId,
    result: acyclic_harness::Result<ControlDispatchResponse>,
    original_native_work: Option<&acyclic_fs::NativeMaterializationWork>,
) -> acyclic_harness::Result<ControlEncodedResponse> {
    obs::record_outcome(&result);
    #[derive(Serialize)]
    struct Response<'a> {
        version: u8,
        #[serde(rename = "requestId")]
        request_id: &'a str,
        ok: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        result: Option<&'a Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<ErrorMessage<'a>>,
    }
    struct ErrorMessage<'a>(&'a Error);
    impl Serialize for ErrorMessage<'_> {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            serialize_control_error(self.0, serializer)
        }
    }
    // Declaration order also retires the payload before its SDK custody on
    // unwind. The original failure remains owned until actual transport send.
    let (failure, source_allocation, payload) = match result {
        Ok((payload, allocation)) => (None, allocation, Some(payload)),
        Err(error) => (Some(error), None, None),
    };
    let response = Response {
        version: 2,
        request_id: request_id.as_str(),
        ok: failure.is_none(),
        result: payload.as_ref(),
        error: failure.as_ref().map(ErrorMessage),
    };
    let encoded = match original_native_work {
        Some(work) => {
            // This is the original query span, not an execution-payload guard's
            // possibly different ledger. Failed admission sends no new frame.
            match service_process::encode_native_control_value(&response, work) {
                Ok((bytes, allocation)) => (bytes, Some(allocation)),
                Err(encoding_error) => {
                    // Close without an unfunded frame, but never replace an
                    // execution failure with a later reporting failure.
                    drop(payload);
                    drop(source_allocation);
                    return Err(failure.unwrap_or(encoding_error));
                }
            }
        }
        None => {
            if source_allocation.is_some() {
                return Err(Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Storage,
                    reason: "native response has no original control-query work",
                });
            }
            if failure.as_ref().is_some_and(|error| {
                !matches!(
                    error,
                    Error::NotFound(_)
                        | Error::Conflict(_)
                        | Error::Unsupported(_)
                        | Error::Invalid(_)
                        | Error::Unauthorized(_)
                        | Error::Storage(_)
                        | Error::InteractionRejected(_)
                        | Error::Indeterminate(_)
                )
            }) {
                return Err(failure.expect("native failure is present"));
            }
            (encode_control_response(&response, Some(request_id)), None)
        }
    };
    drop(payload);
    drop(source_allocation);
    Ok((encoded.0, encoded.1, failure))
}

pub(crate) struct BoundedJsonBuffer {
    pub(crate) bytes: Vec<u8>,
}

impl BoundedJsonBuffer {
    pub(crate) fn new() -> Self {
        Self {
            bytes: Vec::with_capacity(8 * 1024),
        }
    }
}

impl Write for BoundedJsonBuffer {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if buffer.len() > (MAXIMUM_CONTROL_MESSAGE_BYTES - 1).saturating_sub(self.bytes.len()) {
            return Err(io::Error::new(
                io::ErrorKind::FileTooLarge,
                "JSON exceeds its configured byte bound",
            ));
        }
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Encodes `response` in under one control message (leaving room for the
/// frame's newline), or, when it does not fit, the error that replaces it.
pub(crate) fn encode_control_response<T: Serialize + ?Sized>(
    response: &T,
    request_id: Option<&control_protocol::RequestId>,
) -> Vec<u8> {
    let mut bounded = BoundedJsonBuffer::new();
    if serde_json::to_writer(&mut bounded, response).is_ok() {
        return bounded.bytes;
    }
    // Request identities are validated ASCII identifiers, so they need no
    // escaping.
    let correlation = request_id.map_or_else(String::new, |request_id| {
        format!(r#""requestId":"{}","#, request_id.as_str())
    });
    format!(
        r#"{{"version":2,{correlation}"ok":false,"error":"Acyclic control response exceeds the 4 MiB bound"}}"#
    )
    .into_bytes()
}
