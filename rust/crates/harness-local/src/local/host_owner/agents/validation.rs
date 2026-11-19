//! The production session and event codecs, without a second state machine.
use super::*;

/// Checks arbitrary stored bytes, strict version admission and preservation of
/// an admitted completion. Also admits a production-generated empty session,
/// so rejecting every input cannot satisfy the oracle.
pub fn check_native_session_storage(bytes: &[u8]) {
    let result = check_storage(bytes);
    assert!(
        result.is_ok(),
        "native session storage check failed: {result:?}"
    );
}

fn check_storage(bytes: &[u8]) -> Result<()> {
    let empty = serde_json::to_vec(&Session::default()).map_err(storage)?;
    let claim = serde_json::to_vec(&Event::Claim {}).map_err(storage)?;
    assert_eq!(
        Session::decode(&empty).map_err(storage)?,
        Session::default()
    );
    assert!(matches!(Event::decode(&claim), Ok(Event::Claim {})));
    for bytes in [bytes, empty.as_slice(), claim.as_slice()] {
        if let Ok(session) = Session::decode(bytes) {
            let mut value = serde_json::to_value(&session).map_err(storage)?;
            assert_eq!(
                Session::decode(&serde_json::to_vec(&value).map_err(storage)?).map_err(storage)?,
                session
            );
            value["version"] = json!("3");
            assert!(Session::decode(&serde_json::to_vec(&value).map_err(storage)?).is_err());
            value["version"] = json!("2");
            value["unknown"] = Value::Null;
            assert!(Session::decode(&serde_json::to_vec(&value).map_err(storage)?).is_err());
        }
        if let Ok(event) = Event::decode(bytes) {
            let mut value = serde_json::to_value(&event).map_err(storage)?;
            if matches!(event, Event::Complete { .. }) {
                assert_eq!(
                    serde_json::to_value(event.completed()?).map_err(storage)?,
                    value["outcome"],
                    "completion projection must retain every admitted outcome field"
                );
            } else {
                assert!(matches!(event.completed(), Err(Error::Conflict(_))));
            }
            value["unknown"] = Value::Null;
            assert!(Event::decode(&serde_json::to_vec(&value).map_err(storage)?).is_err());
        }
    }
    Ok(())
}
