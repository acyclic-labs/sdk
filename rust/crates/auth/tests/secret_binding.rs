use acyclic_auth::secrets::{
    ResolveError, ResolveFuture, Resolver, SecretRef, SecretValue,
    binding::{BindingError, LocalSecretBindings, SecretBindingHost, VerifyHmacRequest},
    conformance,
};
use futures::executor::block_on;

// A local host source: no guest operation receives or constructs its key.
struct LocalSource;
impl Resolver for LocalSource {
    fn resolve<'a>(&'a self, reference: &'a SecretRef) -> ResolveFuture<'a> {
        Box::pin(async move {
            if reference.version() != 7 {
                return Err(ResolveError::VersionGone);
            }
            Ok(SecretValue::new(b"Jefe".to_vec()))
        })
    }
}

#[test]
fn protected_binding_conformance_on_local_host() -> Result<(), Box<dyn std::error::Error>> {
    let reference = SecretRef::new("webhooks".into(), "github".into(), 7)?;
    let host = LocalSecretBindings::new([("WEBHOOK".into(), reference)], LocalSource)?;
    block_on(conformance::verify_hmac(&host))?;
    Ok(())
}

#[test]
fn protected_binding_does_not_fall_back_from_selected_version()
-> Result<(), Box<dyn std::error::Error>> {
    let reference = SecretRef::new("webhooks".into(), "github".into(), 6)?;
    let host = LocalSecretBindings::new([("WEBHOOK".into(), reference)], LocalSource)?;
    let result = block_on(host.verify_hmac(VerifyHmacRequest {
        secret_binding: "WEBHOOK".into(),
        alg: "HMAC-SHA256".into(),
        body: vec![],
        signature: vec![0; 32],
    }));
    assert_eq!(result, Err(BindingError::Unavailable));
    Ok(())
}
