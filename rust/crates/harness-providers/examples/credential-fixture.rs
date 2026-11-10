//! E2e ownership of a unique native keychain entry. No key crosses stdout,
//! argv or environment, including on failed first-run acceptance.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use acyclic_harness_providers::Credentials;
    let mut args = std::env::args().skip(1);
    let provider = args.next().ok_or("fixture provider missing")?;
    if !provider.starts_with("e2e-first-run-") || args.next().is_some() {
        return Err("expected unique first-run fixture provider".into());
    }
    let stored = Credentials::default().api_key(&provider)?;
    Credentials::remove_api_key(&provider)?;
    if stored.as_deref() != Some("E2E_FIRST_RUN_NONSECRET") {
        return Err("first-run corrected credential was not in OS keychain".into());
    }
    Ok(())
}
