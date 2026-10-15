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

/// Immutable request facts, including usage charged against the root's ledger.
pub struct Request<'a> {
    /// Declared semantic product action.
    pub action: &'a str,
    /// Canonical resource after owner encoding.
    pub resource: &'a str,
    /// Exact authenticated recipient resource.
    pub audience: &'a str,
    /// Authenticated presenting account, if any.
    pub holder: Option<&'a str>,
    /// Current UTC Unix seconds.
    pub now_unix_seconds: u64,
    /// Every record key must satisfy all key filters.
    pub keys: &'a [Vec<u8>],
    /// Authenticated record headers.
    pub attributes: &'a [Attribute],
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

/// Root is index zero; each max_depth is an absolute terminal index ceiling.
/// The minimum across every block applies. Sealing has no block and no cost.
pub fn valid_chain(blocks: &[Block]) -> bool {
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
        })
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
    let wire: BlockProto = appended.clone().into();
    let appended = Block::try_from(wire)?;
    let mut result = blocks.to_vec();
    result.push(appended);
    if !valid_chain(&result) {
        return Err(Error::Invalid);
    }
    Ok(result)
}

fn resource_matches(pattern: &ResourcePattern, name: &str) -> bool {
    if pattern.prefix {
        name.starts_with(&pattern.name)
    } else {
        name == pattern.name
    }
}
fn grants_allow(set: &GrantSet, r: &Request<'_>) -> bool {
    set.grants.iter().any(|g| {
        let grant = match &g.kind {
            GrantKind::Resource(g) => g,
            GrantKind::Execution(g)
                if g.cpu_millis.is_none_or(|n| r.cpu_millis <= n)
                    && g.memory_bytes.is_none_or(|n| r.memory_bytes <= n) =>
            {
                &g.grant
            }
            GrantKind::Billing(g)
                if g.spend_micros
                    .is_none_or(|n| r.usage.spend_micros.unwrap_or(0) <= n) =>
            {
                &g.grant
            }
            _ => return false,
        };
        resource_matches(&grant.resource, r.resource) && grant.actions.iter().any(|a| a == r.action)
    })
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
    blocks[..=through]
        .iter()
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
        || blocks[..=through].iter().flat_map(|b| &b.caveats).any(|c| matches!(&c.kind, CaveatKind::LineageAppend(l) if l.ids.iter().any(|id| id.as_bytes() == value)))
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
            r.attributes.iter().any(|a| {
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
pub fn permits(blocks: &[Block], r: &Request<'_>, policy: PolicyDecision) -> bool {
    if policy == PolicyDecision::Deny || !valid_chain(blocks) {
        return false;
    }
    let Some(certificates) = &blocks[0].certificates else {
        return false;
    };
    let Some(leaf) = certificates.keys.last() else {
        return false;
    };
    if !certificates.keys.iter().all(|cert| {
        let key = &cert.key;
        r.now_unix_seconds >= key.validity.not_before_unix_seconds
            && r.now_unix_seconds < key.validity.expires_at_unix_seconds
            && grants_allow(&key.grants, r)
    }) {
        return false;
    }
    blocks.iter().enumerate().all(|(i, block)| {
        block.caveats.iter().all(|c| match &c.kind {
            CaveatKind::Grants(set) => grants_allow(set, r),
            CaveatKind::ExpiresAtUnixSeconds(t) => r.now_unix_seconds < *t,
            CaveatKind::NotBeforeUnixSeconds(t) => r.now_unix_seconds >= *t,
            CaveatKind::Audience(a) => !a.prefix && resource_matches(a, r.audience),
            CaveatKind::Holder(h) => h
                .account
                .as_deref()
                .is_none_or(|account| r.holder == Some(account)),
            CaveatKind::Filter(f) => filter_allows(f, r, &leaf.key.principal, blocks, i),
            CaveatKind::Pin(pin) => r.attributes.iter().any(|a| {
                a.name == pin.name
                    && match &pin.value {
                        PinValue::Literal(v) => a.value == *v,
                        PinValue::SubjectId(_) => {
                            a.value == acting(&leaf.key.principal, blocks, i).as_bytes()
                        }
                    }
            }),
            CaveatKind::Limits(limits) => limits_allow(limits, r.usage),
            CaveatKind::LineageAppend(_) | CaveatKind::MaxDepth(_) | CaveatKind::Label(_) => true,
        })
    })
}
