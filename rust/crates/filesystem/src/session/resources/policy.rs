use super::*;
use crate::Volume;
use crate::access::{Capability, Context, Principal, PrincipalKind};
use crate::model::{CheckoutMode, GenerationSelector};
use std::collections::{BTreeMap, BTreeSet};

pub(super) async fn compile<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    volume: &Volume<A, O>,
    authorization: &acyclic_stream::session::ResourceAuthorization,
    cancellation: &CancellationToken,
) -> Result<PathTrie, StreamError> {
    let mut checkout = volume
        .checkout(
            GenerationSelector::Head,
            CheckoutMode::read_only_pinned(),
            WorkBudget::UNBOUNDED,
            cancellation,
        )
        .await
        .map_err(|_| StreamError::AccessDenied)?
        .value;
    let subject = authorization.principal()?;
    let kind = match subject.kind {
        acyclic_auth::contract::PrincipalKind::User => PrincipalKind::User,
        acyclic_auth::contract::PrincipalKind::Agent => PrincipalKind::Agent,
        acyclic_auth::contract::PrincipalKind::Service => PrincipalKind::Service,
        acyclic_auth::contract::PrincipalKind::Group => PrincipalKind::Group,
        acyclic_auth::contract::PrincipalKind::Anonymous => PrincipalKind::Anonymous,
        acyclic_auth::contract::PrincipalKind::Unspecified => {
            return Err(StreamError::AccessDenied);
        }
    };
    let attributes = subject
        .attributes
        .into_iter()
        .map(|attribute| {
            String::from_utf8(attribute.value)
                .map(|value| (attribute.name, value))
                .map_err(|_| StreamError::AccessDenied)
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    let principal = Principal {
        kind,
        id: subject.id,
        lineage: subject.lineage,
        groups: subject.groups.into_iter().collect(),
        roles: subject.roles.into_iter().collect(),
        tags: BTreeSet::new(),
        attributes,
    };
    let volume_name = uuid::Uuid::from_bytes(volume.id.into_bytes()).to_string();
    // This layer only selects the semantic metadata action. The actual prefix
    // grants, pins and filters remain enforced by the verified chain per entry.
    let ceiling = Capability::whole_volume(
        Some(volume_name),
        crate::access::ActionSet::Metadata.actions(),
    );
    checkout
        .compile_access(
            crate::access::AccessBinding {
                principal: &principal,
                capability: &ceiling,
                facts: crate::access::AuthenticatedFacts::MetadataStream,
                generation: None,
                context: Context {
                    now: authorization.now_unix_seconds()?,
                    approvals: 0,
                },
            },
            WorkBudget::UNBOUNDED,
            cancellation,
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|_| StreamError::AccessDenied)
}
