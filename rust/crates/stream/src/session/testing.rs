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
pub fn authority() -> Arc<Authority> {
    Arc::new(
        Authority::new(
            Arc::new(State),
            "workers:/test".into(),
            Some("account".into()),
        )
        .unwrap(),
    )
}
pub fn credential() -> Bytes {
    signed::root(|block| {
        block.certificates.as_mut().unwrap().keys[0].key.grants = GrantSet {
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
pub(super) fn slots() -> authority::Slots {
    let auth = authority();
    let mut slots = authority::Slots::default();
    auth.register(&mut slots, 0, &credential(), Limits::default().channels)
        .unwrap();
    slots
}
#[cfg(test)]
pub(super) fn binding() -> authority::Binding {
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
