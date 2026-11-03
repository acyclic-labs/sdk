//! Exercise the canonical reservation wire and checkpoint decoders on demand.
use super::*;

/// Checks canonical requests and decisions, and validates checkpoint ownership.
pub fn check(bytes: &[u8]) -> bool {
    static WRITERS: std::sync::LazyLock<()> = std::sync::LazyLock::new(|| {
        seeds().expect("reservation decoders must admit their production writers");
    });
    std::sync::LazyLock::force(&WRITERS);
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
        let Ok(caller) = acyclic_stream::StreamPath::new("reservation-codec/caller") else {
            return false;
        };
        if let Ok(invocation) = request.invocation(caller, actor, "inbox".into())
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
    let request_bytes = encode(&request)?;
    let admitted = decode::<Request>(&request_bytes)?;
    assert_eq!(admitted.operation()?, request.operation()?);
    assert_eq!(encode(&admitted)?, request_bytes);
    let decision = Decision::Exhausted;
    let decision_bytes = encode(&decision)?;
    assert_eq!(decode::<Decision>(&decision_bytes)?, decision);
    let book_bytes = initial_checkpoint(1)?.bytes;
    assert_eq!(decode::<Book>(&book_bytes)?, Book::new(1)?);
    Ok(vec![
        ("reservation-request", request_bytes.to_vec()),
        ("reservation-decision", decision_bytes.to_vec()),
        ("reservation-book", book_bytes.to_vec()),
    ])
}
