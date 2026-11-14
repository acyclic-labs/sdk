//! Shared transport custody of Auth's original request; no cache or refresh owner.
use acyclic_auth::credential::{CredentialError, CredentialProvider, ResolvedCredential};

pub(crate) async fn resolve(
    provider: &CredentialProvider,
) -> Result<ResolvedCredential, CredentialError> {
    action(
        "credential.resolve",
        "original_credential_source_admission",
        provider.resolve(),
    )
    .await
}

pub(crate) async fn check(lease: &ResolvedCredential) -> Result<(), CredentialError> {
    action(
        "credential.check",
        "original_credential_lease_valid_at_boundary",
        async { lease.check().await },
    )
    .await
}

pub(crate) async fn reject(lease: &ResolvedCredential) -> Result<bool, CredentialError> {
    action(
        "credential.reject",
        "only_original_rejected_credential_invalidated",
        async { lease.reject().await },
    )
    .await
}

async fn action<T>(
    _name: &'static str,
    _guarantee: &'static str,
    work: impl std::future::Future<Output = Result<T, CredentialError>>,
) -> Result<T, CredentialError> {
    #[cfg(feature = "serde")]
    return crate::trace::observe(
        crate::trace::Request {
            name: _name,
            mechanism: crate::trace::Mechanism::AuthCheck,
            parent: None,
            guarantee: Some(_guarantee),
        },
        work,
        |_| None,
    )
    .await;
    #[cfg(not(feature = "serde"))]
    work.await
}
