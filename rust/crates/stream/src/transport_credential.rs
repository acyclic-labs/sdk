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

/// Original allocation-free action with recording custody through later polls.
pub(crate) struct Check {
    action: acyclic_auth::credential::CredentialAction<()>,
    #[cfg(feature = "serde")]
    recording: Option<crate::trace::OwnedRecording>,
}

pub(crate) fn check(lease: &ResolvedCredential) -> Check {
    #[cfg(feature = "serde")]
    let (action, recording) = crate::trace::accompany_sync(
        crate::trace::Request {
            name: "credential.check",
            mechanism: crate::trace::Mechanism::AuthCheck,
            parent: None,
            guarantee: Some("original_credential_lease_valid_at_boundary"),
        },
        || lease.check(),
    );
    #[cfg(not(feature = "serde"))]
    let action = lease.check();
    Check {
        action,
        #[cfg(feature = "serde")]
        recording,
    }
}

impl std::future::Future for Check {
    type Output = Result<(), CredentialError>;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let state = self.get_mut();
        let mut work = || std::pin::Pin::new(&mut state.action).poll(cx);
        #[cfg(feature = "serde")]
        let result = if let Some(recording) = &state.recording {
            recording.within_sync(work)
        } else {
            work()
        };
        #[cfg(not(feature = "serde"))]
        let result = work();
        #[cfg(feature = "serde")]
        if result.is_ready() {
            if let Some(recording) = state.recording.take() {
                if matches!(result, std::task::Poll::Ready(Ok(()))) {
                    let _completion =
                        recording.finish_submitted(None, crate::trace::Outcome::Succeeded);
                }
            }
        }
        result
    }
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
