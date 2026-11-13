//! Native HTTP source classification, independent of request replay policy.
use crate::wire::ErrorCode;

/// Classify an actual transport cause. Only positively identified connection
/// loss, timeout or temporary HTTP/2 resets are unavailable. TLS admission and
/// malformed protocol data are terminal; unknown causes remain storage faults.
/// A request owner separately decides whether a failed write may have happened.
/// The deepest source is returned for adapters that admit its diagnostic text.
pub fn transport_failure(
    error: &reqwest::Error,
) -> (ErrorCode, &(dyn std::error::Error + 'static)) {
    let mut transient = error.is_timeout() || error.is_connect();
    // bytes_stream labels transport cuts as Decode too; inspect their source
    // rather than treating that outer label as malformed protocol data.
    let mut invalid = error.is_builder();
    let mut tls = false;
    let mut cause: &(dyn std::error::Error + 'static) = error;
    loop {
        tls |= cause.is::<rustls::Error>();
        if let Some(error) = cause.downcast_ref::<hyper::Error>() {
            invalid |= error.is_parse() || error.is_user();
            transient |= error.is_incomplete_message() || error.is_closed() || error.is_canceled();
        }
        if let Some(reason) = cause
            .downcast_ref::<h2::Error>()
            .and_then(h2::Error::reason)
        {
            let temporary = matches!(
                reason,
                h2::Reason::NO_ERROR
                    | h2::Reason::INTERNAL_ERROR
                    | h2::Reason::REFUSED_STREAM
                    | h2::Reason::CANCEL
                    | h2::Reason::ENHANCE_YOUR_CALM
                    | h2::Reason::CONNECT_ERROR
            );
            transient |= temporary;
            invalid |= !temporary;
        }
        let io = cause.downcast_ref::<std::io::Error>();
        if let Some(error) = io {
            use std::io::ErrorKind;
            invalid |= matches!(
                error.kind(),
                ErrorKind::InvalidData | ErrorKind::InvalidInput
            );
            transient |= matches!(
                error.kind(),
                ErrorKind::ConnectionReset
                    | ErrorKind::ConnectionAborted
                    | ErrorKind::ConnectionRefused
                    | ErrorKind::NotConnected
                    | ErrorKind::UnexpectedEof
                    | ErrorKind::TimedOut
                    | ErrorKind::Interrupted
            );
        }
        // IO's source() can skip its wrapped TLS error. Inspect that value first.
        match io
            .and_then(std::io::Error::get_ref)
            .map(|error| error as &(dyn std::error::Error + 'static))
            .or_else(|| cause.source())
        {
            Some(next) => cause = next,
            None => break,
        }
    }
    let code = if tls {
        ErrorCode::Unauthorized
    } else if invalid {
        ErrorCode::Invalid
    } else if transient {
        ErrorCode::Unavailable
    } else {
        ErrorCode::Storage
    };
    (code, cause)
}
