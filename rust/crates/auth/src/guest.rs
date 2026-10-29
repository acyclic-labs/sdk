//! Invocation-owned guest projections. A descriptor selects authority; it never
//! authenticates a caller or supplies a signing key, grant or trust snapshot.
use crate::trust::{ChangeSource, Changed};
use serde::{Deserialize, Serialize};
use ts_rs::TS;

/// The two projections of the SAME authenticated invocation owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub enum GuestAdmissionKind {
    /// Opaque signing/delegation port, with original admission and stop event.
    PlatformAuthority,
    /// Session verifier, scoped protected credential and exact owner policy.
    HostAccess,
}

impl GuestAdmissionKind {
    /// Canonical manifest spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PlatformAuthority => "platformAuthority",
            Self::HostAccess => "hostAccess",
        }
    }

    /// Refuse unknown projections instead of treating them as resource grants.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "platformAuthority" => Some(Self::PlatformAuthority),
            "hostAccess" => Some(Self::HostAccess),
            _ => None,
        }
    }
}

/// Implemented by the trusted original admission custodian, never by decoded
/// guest input. Its check includes current trust, original expiry/cancellation
/// and exact caller/recipient. Its event is that owner's real trust publication;
/// producer loss resolves SourceClosed. No detached channel is permitted.
pub trait GuestAdmissionOwner: ChangeSource {
    /// Preserve the custodian's typed refusal.
    type Error;
    /// Synchronous, non-dispatching observation of the original admission.
    fn check(&self) -> Result<(), Self::Error>;
}

/// A host-only retained owner. There is deliberately no wire/JSON conversion.
/// Native and hosted adapters use this same admission and projection boundary.
/// Only the selected opaque projection crosses into the guest; this owner does
/// not. Consumers keep its original change source and stop event when serving.
pub struct GuestAdmission<O: GuestAdmissionOwner> {
    owner: O,
}

impl<O: GuestAdmissionOwner> GuestAdmission<O> {
    /// Admit an already authenticated owner, before constructing guest bindings.
    pub fn admit(owner: O) -> Result<Self, O::Error> {
        owner.check()?;
        Ok(Self { owner })
    }

    /// Project the selected capability in trusted host code. The adapter may
    /// only return ports belonging to this owner; guest bytes are not an input.
    /// Recheck after projection so a ready revocation/stop wins over success.
    pub fn project<T>(
        &self,
        kind: GuestAdmissionKind,
        project: impl FnOnce(&O, GuestAdmissionKind) -> Result<T, O::Error>,
    ) -> Result<T, O::Error> {
        self.owner.check()?;
        let value = project(&self.owner, kind)?;
        self.owner.check()?;
        Ok(value)
    }

    /// Register before checking retained authority, using the SAME owner source.
    /// No worker, timer, channel or credential copy is created here.
    pub fn changed(&self) -> Changed {
        self.owner.changed()
    }
}
