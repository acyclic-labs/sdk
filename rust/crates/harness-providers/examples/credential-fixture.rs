//! E2e ownership of a unique native keychain service. No key crosses stdout,
//! argv or environment, including on failed first-run acceptance.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use acyclic_harness_providers::Credentials;
    let mut args = std::env::args().skip(1);
    let path = args.next().ok_or("fixture credentials missing")?;
    let credentials = Credentials::from_file(std::path::Path::new(&path))?;
    if !credentials.keychain_service.starts_with("e2e-first-run-") || args.next().is_some() {
        return Err("expected unique first-run fixture service".into());
    }
    let stored = credentials.api_key("morph")?;
    credentials.remove_api_key_in_service("morph")?;
    if stored.as_deref() != Some("E2E_FIRST_RUN_NONSECRET") {
        return Err("first-run corrected credential was not in OS keychain".into());
    }
    Ok(())
}
