use super::*;
use crate::access::{Capability, Context, Generations, Grant, Policy, Principal, PrincipalKind, Resource, ResourcePattern};
use crate::{CheckoutMode, GenerationSelector, Volume};
use std::collections::{BTreeMap, BTreeSet};

pub(super) async fn compile<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    volume: &Volume<A, O>,
    authorization: &acyclic_stream::session::ResourceAuthorization,
    cancellation: &CancellationToken,
) -> Result<PathTrie, StreamError> {
    let mut checkout = volume.checkout(GenerationSelector::Head, CheckoutMode::read_only_pinned(), WorkBudget::UNBOUNDED, cancellation).await.map_err(|_| StreamError::AccessDenied)?.value;
    let (bytes, revision) = checkout.read_access_policy(WorkBudget::UNBOUNDED, cancellation).await.map_err(|_| StreamError::AccessDenied)?.value;
    let bytes = bytes.ok_or(StreamError::AccessDenied)?;
    let policy: Policy = serde_json::from_slice(&bytes.bytes).map_err(|_| StreamError::AccessDenied)?;
    // This stream has no authenticated resource-task ancestry or approvals.
    // Generation-sensitive views need a provider-owned moving head fence;
    // refuse those policies instead of ignoring conditional denials.
    if policy.layers.iter().flatten().any(|rule| rule.resource.lineage_root.is_some()
        || rule.resource.generations != Generations::All
        || rule.conditions.generations.is_some() || rule.conditions.relation.is_some()
        || rule.conditions.tag.is_some() || rule.conditions.approvals.is_some()) {
        return Err(StreamError::AccessDenied);
    }
    let subject = authorization.principal()?;
    let kind = match subject.kind {
        acyclic_auth::contract::PrincipalKind::User => PrincipalKind::User,
        acyclic_auth::contract::PrincipalKind::Agent => PrincipalKind::Agent,
        acyclic_auth::contract::PrincipalKind::Service => PrincipalKind::Service,
        acyclic_auth::contract::PrincipalKind::Group => PrincipalKind::Group,
        acyclic_auth::contract::PrincipalKind::Anonymous => PrincipalKind::Anonymous,
        acyclic_auth::contract::PrincipalKind::Unspecified => return Err(StreamError::AccessDenied),
    };
    let attributes = subject.attributes.into_iter().map(|attribute| {
        String::from_utf8(attribute.value).map(|value| (attribute.name, value)).map_err(|_| StreamError::AccessDenied)
    }).collect::<Result<BTreeMap<_, _>, _>>()?;
    let principal = Principal { kind, id: subject.id, lineage: subject.lineage,
        groups: subject.groups.into_iter().collect(), roles: subject.roles.into_iter().collect(), tags: BTreeSet::new(), attributes };
    let volume_name = uuid::Uuid::from_bytes(volume.id.into_bytes()).to_string();
    // This layer only selects the semantic metadata action. The actual prefix
    // grants, pins and filters remain enforced by the verified chain per entry.
    let ceiling = Capability { layers: vec![vec![Grant { actions: BTreeSet::from([Action::List]),
        resource: ResourcePattern { lineage_root: None, volume: Some(volume_name.clone()), generations: Generations::All, path: "**".into() } }]], expires_at: None };
    let generation = checkout.generation_id();
    let (position, head) = checkout.generation_position(generation, WorkBudget::UNBOUNDED, cancellation).await.map_err(|_| StreamError::AccessDenied)?.value;
    PathTrie::compile(&policy, &principal, Resource { config: volume.config, lineage: &[], volume: &volume_name,
        generation: position.get(), head: head.get(), path: "" },
        Context { now: authorization.now_unix_seconds()?, approvals: 0 }, &ceiling)
        .map(|trie| trie.with_policy_revision(generation, revision)).map_err(|_| StreamError::AccessDenied)
}
