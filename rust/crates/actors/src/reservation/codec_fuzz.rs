//! Exercise the canonical reservation wire and checkpoint decoders on demand.
use super::*;

/// Checks canonical requests and decisions, and validates checkpoint ownership.
pub fn check(bytes: &[u8]) -> bool {
    if let Ok(book) = decode::<Book>(bytes)
        && book.validate().is_ok()
    {
        let Ok(encoded) = encode(&book) else {
            return false;
        };
        let Ok(decoded) = decode::<Book>(&encoded) else {
            return false;
        };
        if decoded != book || decoded.validate().is_err() {
            return false;
        }
    }
    if let Ok(request) = decode::<Request>(bytes) {
        let Ok(actor) = crate::addressing::ActorReference::new(b"account", b"pool", b"codec")
        else {
            return false;
        };
        if let Ok(invocation) = request.invocation(actor, "inbox".into())
            && invocation.request().as_ref() != bytes
        {
            return false;
        }
    }
    if let Ok(decision) = decode::<Decision>(bytes)
        && encode(&decision).as_deref() != Ok(bytes)
    {
        return false;
    }
    true
}

/// Capture actual reservation request, result and initial checkpoint encoders.
pub fn seeds() -> Result<Vec<(&'static str, Vec<u8>)>, GuestError> {
    let owner = OwnerEvidence::new(
        Bytes::from_static(b"owner"),
        Bytes::from_static(b"evidence"),
    )?;
    let request = Request::Reserve {
        request_id: Bytes::from_static(b"request"),
        units: 1,
        owner,
        deadline_unix_ms: 100,
    };
    Ok(vec![
        ("reservation-request", encode(&request)?.to_vec()),
        (
            "reservation-decision",
            encode(&Decision::Exhausted)?.to_vec(),
        ),
        ("reservation-book", initial_checkpoint(1)?.bytes.to_vec()),
    ])
}
