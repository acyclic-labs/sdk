//! The production session and event codecs, without a second state machine.
use super::*;

/// Checks arbitrary stored bytes, strict version admission and preservation of
/// an admitted completion. Also admits a production-generated empty session,
/// so rejecting every input cannot satisfy the oracle.
pub fn check_native_session_storage(bytes: &[u8]) {
    let empty = serde_json::to_vec(&Session::default()).unwrap();
    let claim = serde_json::to_vec(&Event::Claim {}).unwrap();
    assert_eq!(Session::decode(&empty).unwrap(), Session::default());
    assert!(matches!(Event::decode(&claim), Ok(Event::Claim {})));
    for bytes in [bytes, empty.as_slice(), claim.as_slice()] {
        if let Ok(session) = Session::decode(bytes) {
            let mut value = serde_json::to_value(&session).unwrap();
            assert_eq!(
                Session::decode(&serde_json::to_vec(&value).unwrap()).unwrap(),
                session
            );
            value["version"] = json!("2");
            assert!(Session::decode(&serde_json::to_vec(&value).unwrap()).is_err());
            value["version"] = json!("1");
            value["unknown"] = Value::Null;
            assert!(Session::decode(&serde_json::to_vec(&value).unwrap()).is_err());
        }
        if let Ok(event) = Event::decode(bytes) {
            let mut value = serde_json::to_value(&event).unwrap();
            if matches!(event, Event::Complete { .. }) {
                assert_eq!(
                    serde_json::to_value(event.completed().unwrap()).unwrap(),
                    value["outcome"],
                    "completion projection must retain every admitted outcome field"
                );
            } else {
                assert!(matches!(event.completed(), Err(Error::Conflict(_))));
            }
            value["unknown"] = Value::Null;
            assert!(Event::decode(&serde_json::to_vec(&value).unwrap()).is_err());
        }
    }
}
