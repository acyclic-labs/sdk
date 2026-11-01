//! Live custody retained by the original session credential slot.
use super::*;

/// An issuer's original admission owners, retained independently of bearer
/// evidence. Implementations use their original sources and current policy;
/// they must not manufacture a replacement owner or poll for changes.
pub trait CredentialCustody: crate::ProviderPlatform + 'static {
    /// Fence disclosure and dispatch against the same original owners.
    fn check(&self) -> Result<(), StreamError>;

    /// Wait on the original owners' change/loss events. Dropping a producer
    /// must complete with a typed error, even when record credit is zero.
    fn terminated(&self) -> crate::BoxProviderFuture<'static, StreamError>;
}

/// A sealed credential and the live owners that its portable evidence cannot
/// encode. The original call result installs it in its requested slot; these
/// capabilities are never returned to the client or replayed on reconnect.
pub struct CredentialEnrollment {
    pub(super) capability: acyclic_auth::verify::VerifiedCapability,
    pub(super) custody: Arc<dyn CredentialCustody>,
    pub(super) caller: Option<CallContribution>,
}

impl CredentialEnrollment {
    /// Transfer sealed evidence and its original live custody. The session
    /// rechecks canonical current trust before changing the slot. Verification
    /// of untrusted bytes belongs at their original admission boundary.
    pub fn new(
        capability: acyclic_auth::verify::VerifiedCapability,
        custody: Arc<dyn CredentialCustody>,
    ) -> Self {
        Self {
            capability,
            custody,
            caller: None,
        }
    }

    pub(super) fn bind(mut self, caller: CallContribution) -> Self {
        self.custody = Arc::new(JoinedCustody {
            caller: caller.clone(),
            selected: self.custody,
        });
        self.caller = Some(caller);
        self
    }
}

struct JoinedCustody {
    caller: CallContribution,
    selected: Arc<dyn CredentialCustody>,
}

impl CredentialCustody for JoinedCustody {
    fn check(&self) -> Result<(), StreamError> {
        self.caller.check()?;
        self.selected.check()
    }

    fn terminated(&self) -> crate::BoxProviderFuture<'static, StreamError> {
        let caller = self.caller.terminated();
        let selected = self.selected.terminated();
        Box::pin(async move {
            match future::select(caller, selected).await {
                future::Either::Left((error, _)) | future::Either::Right((error, _)) => error,
            }
        })
    }
}

/// One original Actors result, optionally contributing an admitted credential.
/// Enrollment is applied before any successful reply fragment is disclosed.
pub struct CallReply {
    pub(super) bytes: Bytes,
    pub(super) enrollment: Option<CredentialEnrollment>,
}

impl CallReply {
    /// A finite reply without a credential contribution.
    pub fn new(bytes: Bytes) -> Self {
        Self {
            bytes,
            enrollment: None,
        }
    }

    /// A finite reply whose original result transfers live credential custody.
    pub fn enrolled(bytes: Bytes, enrollment: CredentialEnrollment) -> Self {
        Self {
            bytes,
            enrollment: Some(enrollment),
        }
    }
}
