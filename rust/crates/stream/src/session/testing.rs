//! Deterministic public trust and signed tokens for protocol qualification.
use super::*;
use acyclic_auth::{
    contract::*,
    testing as signed,
    trust::{ChangeSource, Changed, SourceClosed},
    verify::VerifyError,
};
/// Fixed public trust: its owner never publishes and is never lost.
struct State(Arc<signed::Snapshot>);
impl ChangeSource for State {
    fn check_open(&self) -> Result<(), SourceClosed> {
        Ok(())
    }
    fn changed(&self) -> Changed {
        Box::pin(future::pending())
    }
}
impl AuthorityState for State {
    fn snapshot(&self) -> Result<TrustSnapshot, VerifyError> {
        Ok(TrustSnapshot {
            trust: self.0.clone(),
            now_unix_seconds: signed::NOW,
        })
    }
    fn changes(&self) -> &dyn ChangeSource {
        self
    }
    fn expires_at(&self, _: u64) -> crate::BoxProviderFuture<'_, ()> {
        Box::pin(future::pending())
    }
}
/// Public verifier for the deterministic qualification account.
pub fn authority() -> Arc<Authority> {
    Arc::new(Authority {
        state: Arc::new(State(Arc::new(signed::Snapshot::default()))),
        audience: "workers:/test".into(),
        holder: Some("account".into()),
    })
}

/// Signed workload credential for the deterministic qualification account.
pub fn credential() -> Bytes {
    credential_with_grants(vec![ResourceGrant {
        resource: ResourcePattern {
            name: "streams:/".into(),
            prefix: true,
        },
        actions: vec![
            "streams/follow".into(),
            "streams/list".into(),
            "streams/read".into(),
        ],
    }])
}

/// Signs only the supplied grants for admission and projection qualification.
pub fn credential_with_grants(grants: Vec<ResourceGrant>) -> Bytes {
    signed::root(|block| {
        let Some(key) = block
            .certificates
            .as_mut()
            .and_then(|certificates| certificates.keys.first_mut())
        else {
            return;
        };
        key.key.grants = GrantSet {
            grants: grants
                .iter()
                .cloned()
                .map(|grant| Grant {
                    kind: GrantKind::Resource(grant),
                })
                .collect(),
        };
    })
    .into()
}
#[cfg(test)]
pub(in crate::session) fn slots() -> Slots {
    let auth = authority();
    let mut slots = Slots::default();
    auth.register(
        &mut slots,
        0,
        &credential(),
        super::Limits::default().channels,
    )
    .unwrap();
    slots
}
#[cfg(test)]
pub(in crate::session) fn binding() -> Binding {
    authority()
        .bind(
            &slots(),
            0,
            &StreamPath::new("model").unwrap(),
            Default::default(),
            1,
        )
        .unwrap()
        .0
}

/// Verifies a deterministic test credential through the real session admission
/// path. Product-provider tests receive the same unforgeable context as hosts.
pub fn resource_authorization(
    credential: &[u8],
    resource: &str,
) -> Result<ResourceAuthorization, StreamError> {
    let authority = authority();
    let mut slots = Slots::default();
    authority.register(&mut slots, 0, credential, super::Limits::default().channels)?;
    authority
        .bind_resource(&slots, 0, resource.into(), Default::default(), 1)
        .map(|(_, _, authorization)| authorization)
        .map_err(|_| StreamError::AccessDenied)
}
