//! E2e ownership of a unique native keychain entry. No key crosses stdout,
//! argv or environment, including on failed first-run acceptance.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use acyclic_harness_providers::Credentials;
    let mut args = std::env::args().skip(1);
    let provider = args.next().ok_or("fixture provider missing")?;
    if !provider.starts_with("e2e-first-run-") || args.next().is_some() {
        return Err("expected unique first-run fixture provider".into());
    }
    let present = Credentials::default().api_key(&provider)?.is_some();
    Credentials::remove_api_key(&provider)?;
    if !present {
        return Err("first-run credential was not in OS keychain".into());
    }
    Ok(())
}
