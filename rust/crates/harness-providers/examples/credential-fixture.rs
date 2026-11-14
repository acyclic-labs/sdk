//! E2e ownership of a unique native keychain service. No key crosses stdout,
//! argv or environment, including on failed first-run acceptance.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use acyclic_harness_providers::Credentials;
    let mut args = std::env::args().skip(1);
    let service = args.next().ok_or("fixture service missing")?;
    let provider = args.next().ok_or("fixture provider missing")?;
    let credentials = Credentials::default();
    if !service.starts_with("e2e-first-run-")
        || credentials.keychain_service().as_ref() != service.as_str()
        || args.next().is_some()
    {
        return Err("expected unique first-run fixture service".into());
    }
    let stored = credentials.api_key(&provider)?;
    credentials.remove_api_key_in_service(&provider)?;
    if stored.as_deref() != Some("E2E_FIRST_RUN_NONSECRET") {
        return Err("first-run corrected credential was not in OS keychain".into());
    }
    Ok(())
}
