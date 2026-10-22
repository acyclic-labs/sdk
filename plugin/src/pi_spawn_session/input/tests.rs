use super::sdk_session_id;

/// A child session id exactly as the pinned Pi SDK (1.1.0) issued it in the e2e tier.
const ISSUED: &str = "01a16136-a7f8-77ea-bd63-fd5a483b1ea4";

#[test]
fn the_pinned_sdk_session_form_is_accepted() {
    let uuid = sdk_session_id(ISSUED).unwrap();
    assert_eq!(uuid.get_version(), Some(uuid::Version::SortRand));
    assert_eq!(uuid.to_string(), ISSUED);
}

#[test]
fn any_other_session_form_is_refused() {
    // Negative control: the random v4 form the check used to require is not what the
    // SDK issues, and neither is any non-canonical spelling of an issued id.
    for session in [
        "6f1c2a8e-3b4d-4c5e-9f60-718293a4b5c6",
        "01A16136-A7F8-77EA-BD63-FD5A483B1EA4",
        "{01a16136-a7f8-77ea-bd63-fd5a483b1ea4}",
        "01a16136a7f877eabd63fd5a483b1ea4",
        "01a16136-a7f8-77ea-dd63-fd5a483b1ea4",
        "",
    ] {
        assert!(sdk_session_id(session).is_err(), "{session}");
    }
}
