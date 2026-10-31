//! Contract authority algebra. Inputs must first pass signature and revocation
//! verification. This intersects capability constraints with the owner's policy;
//! it does not implement filesystem policy or perform mutable quota accounting.
use crate::{Error, contract::*};
use prost::Message;

/// Owner policy result. A capability can never override Deny.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PolicyDecision {
    /// Owner policy permits the request.
    Allow,
    /// Owner denial is final.
    Deny,
}

/// Which record facts an operation can check at admission. Deferred facts must
/// be checked by the owning provider before it exposes each record or child.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scope {
    /// Check every grant, filter and pin against the exact request facts.
    Exact,
    /// Admit only `streams/follow`, deferring record filters and pins.
    Follow,
    /// Admit only `streams/list`, including discovery above a granted prefix.
    Children,
    /// Admit only `fs/list` on `fs:/`; each full metadata path still needs an
    /// exact check intersected with the current filesystem owner policy.
    ResourceFollow,
}

/// Fail-closed authorization outcome, including malformed request facts.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Deny {
    /// The product resource is not in its unique encoded spelling.
    #[error("non-canonical request resource")]
    NonCanonicalResource,
    /// The authenticated recipient is not a canonical resource name.
    #[error("non-canonical request audience")]
    NonCanonicalAudience,
    /// The owning product denied the request.
    #[error("owner policy denied the request")]
    OwnerPolicy,
    /// The chain does not cover the request.
    #[error("capability denied the request")]
    Capability,
}

/// Typed operation facts; resource grants never authorize socket destinations.
#[derive(Clone, Copy, Debug)]
pub enum Target<'a> {
    /// Credential context only. Admits no resource, record or socket access;
    /// checks trust, time, audience, holder and usage without a resource grant.
    Context,
    /// Declared semantic action against an owner-canonicalized resource.
    Resource {
        /// Product action from the descriptor catalog.
        action: &'a str,
        /// Canonical product resource name.
        resource: &'a str,
    },
    /// External WebSocket destination, checked against typed socket grants.
    Websocket(crate::socket::Destination<'a>),
}

/// Immutable request facts, including usage charged against the root's ledger.
pub struct Request<'a> {
    /// Resource operation or external socket destination.
    pub target: Target<'a>,
    /// Exact authenticated recipient resource.
    pub audience: &'a str,
    /// Authenticated presenting account, if any.
    pub holder: Option<&'a str>,
    /// Current UTC Unix seconds.
    pub now_unix_seconds: u64,
    /// Every record key must satisfy all key filters.
    pub keys: &'a [Vec<u8>],
    /// Decoded single-valued record headers, shared by every consumer.
    pub attributes: &'a RecordAttributes,
    /// Usage measured against the shared root ledger.
    pub usage: &'a Limits,
    /// Requested CPU ceiling in milliseconds.
    pub cpu_millis: u64,
    /// Requested memory ceiling in bytes.
    pub memory_bytes: u64,
}

/// Strict canonical ingress: unknown fields, duplicate fields and old bearer
/// payloads fail closed rather than becoming an unsigned semantic projection.
pub fn decode_block(bytes: &[u8]) -> Result<Block, Error> {
    let wire = BlockProto::decode(bytes).map_err(|_| Error::Invalid)?;
    if wire.encode_to_vec() != bytes {
        return Err(Error::Invalid);
    }
    wire.try_into()
}

/// Strict single-caveat ingress shared by native and browser holders.
pub fn decode_caveat(bytes: &[u8]) -> Result<Caveat, Error> {
    let wire = CaveatProto::decode(bytes).map_err(|_| Error::Invalid)?;
    if wire.encode_to_vec() != bytes {
        return Err(Error::Invalid);
    }
    wire.try_into()
}

/// Strict envelope ingress, including block position, version and depth checks.
pub fn decode_capability(bytes: &[u8]) -> Result<Capability, Error> {
    let wire = CapabilityProto::decode(bytes).map_err(|_| Error::Invalid)?;
    if wire.version != crate::VERSION {
        return Err(Error::UnsupportedVersion(wire.version));
    }
    if wire.encode_to_vec() != bytes {
        return Err(Error::Invalid);
    }
    let value = Capability::try_from(wire)?;
    let blocks = value
        .blocks
        .iter()
        .map(|b| decode_block(&b.payload))
        .collect::<Result<Vec<_>, _>>()?;
    if !valid_chain(&blocks) {
        return Err(Error::Invalid);
    }
    Ok(value)
}

/// Root is index zero; each `max_depth` is an absolute terminal index ceiling.
/// The minimum across every block applies. Sealing has no block and no cost.
pub fn valid_chain(blocks: &[Block]) -> bool {
    #[cfg(test)]
    acyclic_test_support::differential::record(StructuralAdmission);
    let Some(root) = blocks.first() else {
        return false;
    };
    root.certificates.is_some()
        && blocks.iter().enumerate().all(|(i, b)| {
            (i == 0 || b.certificates.is_none())
            && b.next_public_key.len() == 32
            && b.caveats.iter().all(
                |c| !matches!(c.kind, CaveatKind::MaxDepth(max) if blocks.len() - 1 > max as usize),
            )
            && (i == 0 || narrows(&blocks[..i], b))
        })
}

/// An appended block only narrows: each grant it states lies within every
/// earlier ceiling (the certified leaf's grants and each earlier block's), and
/// each allowance covers a subset of every earlier allowance's products.
/// Requests also meet every block conjunctively; this is what lets a consumer
/// take a typed value (a reservation, a mount's POSIX owner) as effective.
fn narrows(ancestors: &[Block], block: &Block) -> bool {
    let leaf = ancestors
        .first()
        .and_then(|root| root.certificates.as_ref())
        .and_then(|certificates| certificates.keys.last())
        .map(|leaf| &leaf.key.grants);
    let ceilings: Vec<&GrantSet> = leaf
        .into_iter()
        .chain(ancestors.iter().flat_map(|ancestor| {
            ancestor
                .caveats
                .iter()
                .filter_map(|caveat| match &caveat.kind {
                    CaveatKind::Grants(set) => Some(set),
                    _ => None,
                })
        }))
        .collect();
    let allowances: Vec<&Allowance> = ancestors
        .iter()
        .flat_map(|ancestor| &ancestor.caveats)
        .filter_map(|caveat| match &caveat.kind {
            CaveatKind::Allowance(signed) => Some(&signed.allowance),
            _ => None,
        })
        .collect();
    block.caveats.iter().all(|caveat| match &caveat.kind {
        CaveatKind::Grants(set) => set.grants.iter().all(|grant| {
            ceilings
                .iter()
                .all(|ceiling| ceiling.grants.iter().any(|parent| covers(parent, grant)))
        }),
        CaveatKind::Allowance(signed) => allowances.iter().all(|parent| {
            parent.products.is_empty()
                || !signed.allowance.products.is_empty()
                    && signed
                        .allowance
                        .products
                        .iter()
                        .all(|product| parent.products.contains(product))
        }),
        _ => true,
    })
}

// A child bound is within its parent's: unset inherits, set never exceeds.
fn at_most(parent: Option<u64>, child: Option<u64>) -> bool {
    child.is_none_or(|child| parent.is_none_or(|parent| child <= parent))
}

// A pinned value is kept: unset inherits, set never changes a set parent.
fn kept<T: PartialEq>(parent: Option<&T>, child: Option<&T>) -> bool {
    child.is_none_or(|child| parent.is_none_or(|parent| child == parent))
}

fn within(parent: &ResourceGrant, child: &ResourceGrant) -> bool {
    (parent.resource.prefix || !child.resource.prefix)
        && resource_matches(&parent.resource, &child.resource.name)
        && child
            .actions
            .iter()
            .all(|action| parent.actions.contains(action))
}

const fn resource_of(grant: &Grant) -> Option<&ResourceGrant> {
    match &grant.kind {
        GrantKind::Resource(grant) => Some(grant),
        GrantKind::Execution(grant) => Some(&grant.grant),
        GrantKind::Billing(grant) => Some(&grant.grant),
        GrantKind::Filesystem(grant) => Some(&grant.grant),
        GrantKind::WebsocketConnect(_) => None,
    }
}

// A typed ceiling bounds its own kind's values; a plain resource on either
// side bounds only the resource, and the typed side's values still apply to
// every request conjunctively.
fn covers(parent: &Grant, child: &Grant) -> bool {
    match (&parent.kind, &child.kind) {
        (GrantKind::WebsocketConnect(parent), GrantKind::WebsocketConnect(child)) => {
            crate::socket::covers(parent, child)
        }
        (GrantKind::Resource(_), _) | (_, GrantKind::Resource(_)) => {
            match (resource_of(parent), resource_of(child)) {
                (Some(parent), Some(child)) => within(parent, child),
                _ => false,
            }
        }
        (GrantKind::Execution(parent), GrantKind::Execution(child)) => {
            within(&parent.grant, &child.grant)
                && at_most(parent.cpu_millis, child.cpu_millis)
                && at_most(parent.memory_bytes, child.memory_bytes)
                && at_most(parent.max_concurrency, child.max_concurrency)
                && at_most(
                    parent.reserved_capacity_units,
                    child.reserved_capacity_units,
                )
                && kept(
                    parent.policy_generation.as_ref(),
                    child.policy_generation.as_ref(),
                )
                && kept(parent.usage_owner.as_ref(), child.usage_owner.as_ref())
        }
        (GrantKind::Billing(parent), GrantKind::Billing(child)) => {
            within(&parent.grant, &child.grant) && at_most(parent.spend_micros, child.spend_micros)
        }
        (GrantKind::Filesystem(parent), GrantKind::Filesystem(child)) => {
            within(&parent.grant, &child.grant)
                && kept(parent.posix_uid.as_ref(), child.posix_uid.as_ref())
                && kept(parent.posix_gid.as_ref(), child.posix_gid.as_ref())
        }
        _ => false,
    }
}

/// The only offline append form. Public certificates and issuer claims are
/// preserved byte-for-byte; new blocks can only contribute conjuncts/lineage.
pub fn attenuate(
    blocks: &[Block],
    caveats: Vec<Caveat>,
    next_public_key: Vec<u8>,
) -> Result<Vec<Block>, Error> {
    let appended = Block {
        certificates: None,
        caveats,
        next_public_key,
    };
    let wire: BlockProto = appended.into();
    let appended = Block::try_from(wire)?;
    let mut result = blocks.to_vec();
    result.push(appended);
    if !valid_chain(&result) {
        return Err(Error::Invalid);
    }
    Ok(result)
}

/// Match canonical resources using the same whole-segment rule as admission.
/// Callers must validate the resource and pattern with the owning grammar first.
pub fn resource_matches(pattern: &ResourcePattern, name: &str) -> bool {
    let name = generation_open(&pattern.name, name);
    let name = name.as_ref();
    if pattern.prefix {
        let prefix = pattern.name.trim_end_matches('/');
        name == prefix
            || name
                .strip_prefix(prefix)
                .is_some_and(|tail| tail.starts_with('/'))
    } else {
        name == pattern.name
    }
}
/// A filesystem pattern naming a volume without a generation covers that
/// volume at every generation: `fs:/<volume>~<generation>/...` is matched as
/// `fs:/<volume>/...`. A pattern that pins a generation matches only it.
fn generation_open<'a>(pattern: &str, name: &'a str) -> std::borrow::Cow<'a, str> {
    let volume = |value: &'a str| {
        value
            .strip_prefix("fs:/")
            .map(|rest| rest.split('/').next())
    };
    if let (Some(Some(open)), Some(Some(pinned))) = (
        pattern
            .strip_prefix("fs:/")
            .map(|rest| rest.split('/').next()),
        volume(name),
    ) && !open.contains('~')
        && let Some((base, _generation)) = pinned.split_once('~')
        && base == open
    {
        return format!("fs:/{base}{}", &name["fs:/".len() + pinned.len()..]).into();
    }
    name.into()
}

fn grants_allow(set: &GrantSet, r: &Request<'_>, record_facts: bool) -> bool {
    if matches!(r.target, Target::Context) {
        return true;
    }
    set.grants.iter().any(|g| {
        let Target::Resource { action, resource } = r.target else {
            return matches!((&g.kind, r.target), (GrantKind::WebsocketConnect(grant), Target::Websocket(destination)) if crate::socket::allows(grant, destination));
        };
        let grant = match &g.kind {
            GrantKind::Resource(g) => g,
            GrantKind::Execution(g)
                if g.cpu_millis.is_none_or(|n| r.cpu_millis <= n)
                    && g.memory_bytes.is_none_or(|n| r.memory_bytes <= n)
                    && g.max_concurrency
                        .is_none_or(|n| r.usage.concurrency.unwrap_or(0) <= n) =>
            {
                &g.grant
            }
            GrantKind::Billing(g)
                if g.spend_micros
                    .is_none_or(|n| r.usage.spend_micros.unwrap_or(0) <= n) =>
            {
                &g.grant
            }
            GrantKind::Filesystem(g) => &g.grant,
            _ => return false,
        };
        grant.actions.iter().any(|a| a == action)
            && (resource_matches(&grant.resource, resource)
                || !record_facts && matches!(action, "streams/list" | "fs/list")
                    && resource_matches(&ResourcePattern { name: resource.into(), prefix: true }, &grant.resource.name))
    })
}
// An allowance limits spend on the products it covers and admits no other.
fn allowance_covers(allowance: &Allowance, target: Target<'_>) -> bool {
    allowance.products.is_empty()
        || match target {
            Target::Resource { action, .. } => action
                .split_once('/')
                .is_some_and(|(product, _)| allowance.products.iter().any(|p| p == product)),
            Target::Context => true,
            _ => false,
        }
}

fn limits_allow(ceiling: &Limits, usage: &Limits) -> bool {
    let bounds = [
        ceiling.rps,
        ceiling.burst,
        ceiling.bytes_per_s,
        ceiling.channels,
        ceiling.concurrency,
        ceiling.bytes,
        ceiling.spend_micros,
        ceiling.tokens,
    ];
    let used = [
        usage.rps,
        usage.burst,
        usage.bytes_per_s,
        usage.channels,
        usage.concurrency,
        usage.bytes,
        usage.spend_micros,
        usage.tokens,
    ];
    bounds
        .into_iter()
        .zip(used)
        .all(|(max, n)| max.is_none_or(|max| n.unwrap_or(0) <= max))
}
fn acting<'a>(principal: &'a Principal, blocks: &'a [Block], through: usize) -> &'a str {
    blocks
        .iter()
        .take(through.saturating_add(1))
        .rev()
        .flat_map(|b| b.caveats.iter().rev())
        .find_map(|c| {
            if let CaveatKind::LineageAppend(l) = &c.kind {
                l.ids.last().map(String::as_str)
            } else {
                None
            }
        })
        .or_else(|| principal.lineage.last().map(String::as_str))
        .unwrap_or(&principal.id)
}
fn ancestor(principal: &Principal, blocks: &[Block], through: usize, value: &[u8]) -> bool {
    principal.id.as_bytes() == value || principal.lineage.iter().any(|id| id.as_bytes() == value)
        || blocks.iter().take(through.saturating_add(1)).flat_map(|b| &b.caveats).any(|c| matches!(&c.kind, CaveatKind::LineageAppend(l) if l.ids.iter().any(|id| id.as_bytes() == value)))
}
fn filter_allows(f: &Filter, r: &Request<'_>, p: &Principal, blocks: &[Block], i: usize) -> bool {
    (f.keys.is_empty()
        || !r.keys.is_empty()
            && r.keys.iter().all(|k| {
                f.keys
                    .iter()
                    .any(|range| k >= &range.start && (range.end.is_empty() || k < &range.end))
            }))
        && f.attributes.iter().all(|clause| {
            r.attributes.as_slice().iter().any(|a| {
                a.name == clause.name
                    && (clause.any_of.contains(&a.value)
                        || clause.subjects.iter().any(|s| match &s.kind {
                            SubjectReferenceKind::Id(_) => {
                                a.value == acting(p, blocks, i).as_bytes()
                            }
                            SubjectReferenceKind::Lineage(_) => ancestor(p, blocks, i, &a.value),
                        }))
            })
        })
}

/// Conjunction is the contract's monotonicity rule. Subject references are
/// frozen at the declaring block; later lineage cannot expand an older filter.
/// All online ancestor certificates and all offline blocks restrict authority.
/// Deferred scopes cannot admit writes, socket operations or general reads.
pub fn permits(
    blocks: &[Block],
    r: &Request<'_>,
    policy: PolicyDecision,
    scope: Scope,
) -> Result<(), Deny> {
    permits_inner::<false>(blocks, r, policy, scope)
}

// Only VerifiedCapability::authorize calls this after checking its immutable
// admitted chain against current trust and its effective validity interval.
pub(crate) fn permits_current(
    blocks: &[Block],
    r: &Request<'_>,
    policy: PolicyDecision,
    scope: Scope,
) -> Result<(), Deny> {
    permits_inner::<true>(blocks, r, policy, scope)
}

fn permits_inner<const CURRENT: bool>(
    blocks: &[Block],
    r: &Request<'_>,
    policy: PolicyDecision,
    scope: Scope,
) -> Result<(), Deny> {
    let admitted = match (scope, r.target) {
        (Scope::Exact, _) => true,
        (
            Scope::Follow,
            Target::Resource {
                action: "streams/follow",
                ..
            },
        )
        | (
            Scope::Children,
            Target::Resource {
                action: "streams/list",
                ..
            },
        ) => true,
        (
            Scope::ResourceFollow,
            Target::Resource {
                action: "fs/list",
                resource,
            },
        ) => resource.starts_with("fs:/"),
        _ => false,
    };
    if !admitted {
        return Err(Deny::Capability);
    }
    if matches!(r.target, Target::Resource { resource, .. } if !crate::valid_resource(resource)) {
        return Err(Deny::NonCanonicalResource);
    }
    if !crate::valid_resource(r.audience) {
        return Err(Deny::NonCanonicalAudience);
    }
    if policy == PolicyDecision::Deny {
        return Err(Deny::OwnerPolicy);
    }
    if permits_canonical::<CURRENT>(
        blocks,
        r,
        scope == Scope::Exact && !matches!(r.target, Target::Context),
    ) {
        Ok(())
    } else {
        Err(Deny::Capability)
    }
}

fn permits_canonical<const CURRENT: bool>(
    blocks: &[Block],
    r: &Request<'_>,
    record_facts: bool,
) -> bool {
    if !CURRENT && !valid_chain(blocks) {
        return false;
    }
    let Some(certificates) = blocks.first().and_then(|block| block.certificates.as_ref()) else {
        return false;
    };
    let Some(leaf) = certificates.keys.last() else {
        return false;
    };
    if !certificates.keys.iter().all(|cert| {
        let key = &cert.key;
        (CURRENT
            || r.now_unix_seconds >= key.validity.not_before_unix_seconds
                && r.now_unix_seconds < key.validity.expires_at_unix_seconds)
            && grants_allow(&key.grants, r, record_facts)
    }) {
        return false;
    }
    blocks.iter().enumerate().all(|(i, block)| {
        block.caveats.iter().all(|c| match &c.kind {
            CaveatKind::Grants(set) => grants_allow(set, r, record_facts),
            CaveatKind::ExpiresAtUnixSeconds(t) => CURRENT || r.now_unix_seconds < *t,
            CaveatKind::NotBeforeUnixSeconds(t) => CURRENT || r.now_unix_seconds >= *t,
            CaveatKind::Audience(a) => !a.prefix && resource_matches(a, r.audience),
            CaveatKind::Holder(h) => h
                .account
                .as_deref()
                .is_none_or(|account| r.holder == Some(account)),
            CaveatKind::Filter(f) => {
                !record_facts || filter_allows(f, r, &leaf.key.principal, blocks, i)
            }
            CaveatKind::Pin(pin) => {
                !record_facts
                    || r.attributes.as_slice().iter().any(|a| {
                        a.name == pin.name
                            && match &pin.value {
                                PinValue::Literal(v) => a.value == *v,
                                PinValue::SubjectId(_) => {
                                    a.value == acting(&leaf.key.principal, blocks, i).as_bytes()
                                }
                            }
                    })
            }
            CaveatKind::Limits(limits) => limits_allow(limits, r.usage),
            CaveatKind::Allowance(signed) => {
                (CURRENT || r.now_unix_seconds < signed.allowance.expires_at_unix_seconds)
                    && r.usage.spend_micros.unwrap_or(0) <= signed.allowance.spend_micros
                    && allowance_covers(&signed.allowance, r.target)
            }
            CaveatKind::LineageAppend(_) | CaveatKind::MaxDepth(_) | CaveatKind::Label(_) => true,
        })
    })
}

#[cfg(test)]
mod tests;

#[cfg(test)]
#[derive(Debug, PartialEq)]
pub(crate) struct StructuralAdmission;
