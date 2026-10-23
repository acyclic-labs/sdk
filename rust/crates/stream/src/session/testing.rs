//! Deterministic public trust and signed tokens for protocol qualification.
use super::*;
use acyclic_auth::{contract::*, testing as signed, verify::VerifyError};
struct State;
impl AuthorityState for State {
    fn snapshot(&self) -> Result<TrustSnapshot, VerifyError> {
        Ok(TrustSnapshot {
            trust: Arc::new(signed::Snapshot::default()),
            now_unix_seconds: signed::NOW,
            revision: 0,
        })
    }
    fn changed(&self, _: u64) -> crate::BoxProviderFuture<'_, ()> {
        Box::pin(future::pending())
    }
    fn expires_at(&self, _: u64) -> crate::BoxProviderFuture<'_, ()> {
        Box::pin(future::pending())
    }
}
/// Public verifier for the deterministic qualification account.
pub fn authority() -> Arc<Authority> {
    Arc::new(Authority {
        state: Arc::new(State),
        audience: "workers:/test".into(),
        holder: Some("account".into()),
    })
}

/// Signed workload credential for the deterministic qualification account.
pub fn credential() -> Bytes {
    signed::root(|block| {
        let Some(key) = block
            .certificates
            .as_mut()
            .and_then(|certificates| certificates.keys.first_mut())
        else {
            return;
        };
        key.key.grants = GrantSet {
            grants: vec![Grant {
                kind: GrantKind::Resource(ResourceGrant {
                    resource: ResourcePattern {
                        name: "streams:/".into(),
                        prefix: true,
                    },
                    actions: vec!["streams/follow".into(), "streams/list".into()],
                }),
            }],
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
