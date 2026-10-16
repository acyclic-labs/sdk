use super::*;

/// A filter and the block where its subject references were frozen.
pub struct ScopedFilter<'a> {
    /// Zero-based declaring block index.
    pub declaring_block: usize,
    /// Ranges/attributes, conjunctive with every other returned filter.
    pub filter: &'a Filter,
}

/// Borrowed effective authority. Grant sets and filters remain conjunctive;
/// preserving clauses avoids expansion or allocation per caveat on admission.
pub struct Effective<'a> { pub(super) capability: &'a VerifiedCapability }
impl<'a> Effective<'a> {
    /// Issuer claims with all append-only delegation lineage applied.
    pub fn principal(&self) -> &'a Principal { &self.capability.principal }
    /// Every returned union is a ceiling; all unions must allow the request.
    pub fn grants(&self) -> impl Iterator<Item=&'a GrantSet> {
        self.capability.blocks[0].certificates.iter().flat_map(|certificates| &certificates.keys).map(|cert| &cert.key.grants)
            .chain(self.capability.blocks.iter().flat_map(|block| &block.caveats).filter_map(|caveat| match &caveat.kind { CaveatKind::Grants(grants) => Some(grants), _ => None }))
    }
    /// Every returned filter must hold, using its declaring-block identity.
    pub fn filters(&self) -> impl Iterator<Item=ScopedFilter<'a>> {
        self.capability.blocks.iter().enumerate().flat_map(|(index, block)| block.caveats.iter().filter_map(move |caveat| match &caveat.kind { CaveatKind::Filter(filter) => Some(ScopedFilter { declaring_block:index, filter }), _ => None }))
    }
    /// Per-dimension minimum; metering remains at the root's owning ledger.
    pub fn limits(&self) -> &'a Limits { &self.capability.limits }
    /// Inclusive UTC Unix seconds.
    pub fn not_before_unix_seconds(&self) -> u64 { self.capability.not_before }
    /// Exclusive UTC Unix seconds.
    pub fn expires_at_unix_seconds(&self) -> u64 { self.capability.expires_at }
}

pub(super) fn unbounded_limits() -> Limits {
    Limits { rps:None, burst:None, bytes_per_s:None, channels:None, concurrency:None, bytes:None, spend_micros:None, tokens:None }
}
fn minimum(left: &mut Option<u64>, right: Option<u64>) {
    if let Some(right) = right { *left = Some(left.map_or(right, |left| left.min(right))); }
}
fn intersect(left: &mut Limits, right: &Limits) {
    minimum(&mut left.rps, right.rps); minimum(&mut left.burst, right.burst);
    minimum(&mut left.bytes_per_s, right.bytes_per_s); minimum(&mut left.channels, right.channels);
    minimum(&mut left.concurrency, right.concurrency); minimum(&mut left.bytes, right.bytes);
    minimum(&mut left.spend_micros, right.spend_micros); minimum(&mut left.tokens, right.tokens);
}
impl VerifiedCapability {
    pub(super) fn evaluate(&mut self) {
        self.limits = unbounded_limits(); self.not_before = 0; self.expires_at = u64::MAX;
        if let Some(certificates) = &self.blocks[0].certificates {
            self.not_before = certificates.birth.birth.issued_at_unix_seconds;
            if let Some(leaf) = certificates.keys.last() { self.principal = leaf.key.principal.clone(); }
            for cert in &certificates.keys {
                self.not_before = self.not_before.max(cert.key.validity.not_before_unix_seconds);
                self.expires_at = self.expires_at.min(cert.key.validity.expires_at_unix_seconds);
            }
        }
        for block in &self.blocks {
            for caveat in &block.caveats {
                match &caveat.kind {
                    CaveatKind::LineageAppend(lineage) => self.principal.lineage.extend(lineage.ids.iter().cloned()),
                    CaveatKind::NotBeforeUnixSeconds(time) => self.not_before = self.not_before.max(*time),
                    CaveatKind::ExpiresAtUnixSeconds(time) => self.expires_at = self.expires_at.min(*time),
                    CaveatKind::Limits(limits) => intersect(&mut self.limits, limits),
                    _ => (),
                }
            }
        }
    }
}
