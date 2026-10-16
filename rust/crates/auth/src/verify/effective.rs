use super::*;

/// A filter and the block where its subject references were frozen.
pub struct ScopedFilter<'a> {
    declaring_block: usize,
    filter: &'a Filter,
    principal: &'a Principal,
    visible_lineage: usize,
}
impl<'a> ScopedFilter<'a> {
    /// Zero-based declaring block index.
    pub fn declaring_block(&self) -> usize {
        self.declaring_block
    }
    /// Ranges/attributes, conjunctive with every other returned filter.
    pub fn filter(&self) -> &'a Filter {
        self.filter
    }
    /// Issuer and delegation ancestors visible when this filter was declared.
    pub fn lineage(&self) -> impl DoubleEndedIterator<Item = &'a str> {
        let principal = self.principal;
        std::iter::once(principal.id.as_str()).chain(
            principal
                .lineage
                .iter()
                .take(self.visible_lineage)
                .map(String::as_str),
        )
    }
    /// Acting identity frozen at the declaring block, unaffected by later appends.
    pub fn subject_id(&self) -> &'a str {
        self.principal.lineage[..self.visible_lineage]
            .last()
            .map_or(self.principal.id.as_str(), String::as_str)
    }
}

/// Borrowed effective authority. Grant sets and filters remain conjunctive;
/// preserving clauses avoids expansion or allocation per caveat on admission.
pub struct Effective<'a> {
    pub(super) capability: &'a VerifiedCapability,
}
impl<'a> Effective<'a> {
    /// Issuer claims with all append-only delegation lineage applied.
    pub fn principal(&self) -> &'a Principal {
        &self.capability.principal
    }
    /// Every returned union is a ceiling; all unions must allow the request.
    pub fn grants(&self) -> impl Iterator<Item = &'a GrantSet> {
        self.capability.blocks[0]
            .certificates
            .iter()
            .flat_map(|certificates| &certificates.keys)
            .map(|cert| &cert.key.grants)
            .chain(
                self.capability
                    .blocks
                    .iter()
                    .flat_map(|block| &block.caveats)
                    .filter_map(|caveat| match &caveat.kind {
                        CaveatKind::Grants(grants) => Some(grants),
                        _ => None,
                    }),
            )
    }
    /// Every returned filter must hold, using its declaring-block identity.
    pub fn filters(&self) -> impl Iterator<Item = ScopedFilter<'a>> {
        let mut visible_lineage = self.capability.blocks[0]
            .certificates
            .as_ref()
            .and_then(|certificates| certificates.keys.last())
            .map_or(0, |certificate| certificate.key.principal.lineage.len());
        self.capability
            .blocks
            .iter()
            .enumerate()
            .flat_map(move |(index, block)| {
                // Every filter in a block binds after all lineage appends in
                // that block, independent of caveat order. Visit each block
                // twice, never any descendant once per filter.
                for caveat in &block.caveats {
                    #[cfg(test)]
                    filter_work();
                    if let CaveatKind::LineageAppend(lineage) = &caveat.kind {
                        visible_lineage += lineage.ids.len();
                    }
                }
                let visible_lineage = visible_lineage;
                block.caveats.iter().filter_map(move |caveat| {
                    #[cfg(test)]
                    filter_work();
                    match &caveat.kind {
                        CaveatKind::Filter(filter) => Some(ScopedFilter {
                            declaring_block: index,
                            filter,
                            principal: &self.capability.principal,
                            visible_lineage,
                        }),
                        _ => None,
                    }
                })
            })
    }
    /// Per-dimension minimum; metering remains at the root's owning ledger.
    pub fn limits(&self) -> &'a Limits {
        &self.capability.limits
    }
    /// Inclusive UTC Unix seconds.
    pub fn not_before_unix_seconds(&self) -> u64 {
        self.capability.not_before
    }
    /// Exclusive UTC Unix seconds.
    pub fn expires_at_unix_seconds(&self) -> u64 {
        self.capability.expires_at
    }
}

#[cfg(test)]
thread_local! {
    static FILTER_WORK: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}
#[cfg(test)]
fn filter_work() {
    FILTER_WORK.with(|work| work.set(work.get() + 1));
}
#[cfg(test)]
mod tests;

pub(super) fn unbounded_limits() -> Limits {
    Limits {
        rps: None,
        burst: None,
        bytes_per_s: None,
        channels: None,
        concurrency: None,
        bytes: None,
        spend_micros: None,
        tokens: None,
    }
}
fn minimum(left: &mut Option<u64>, right: Option<u64>) {
    if let Some(right) = right {
        *left = Some(left.map_or(right, |left| left.min(right)));
    }
}
fn intersect(left: &mut Limits, right: &Limits) {
    minimum(&mut left.rps, right.rps);
    minimum(&mut left.burst, right.burst);
    minimum(&mut left.bytes_per_s, right.bytes_per_s);
    minimum(&mut left.channels, right.channels);
    minimum(&mut left.concurrency, right.concurrency);
    minimum(&mut left.bytes, right.bytes);
    minimum(&mut left.spend_micros, right.spend_micros);
    minimum(&mut left.tokens, right.tokens);
}
impl VerifiedCapability {
    pub(super) fn evaluate(&mut self) {
        self.limits = unbounded_limits();
        self.not_before = 0;
        self.expires_at = u64::MAX;
        if let Some(certificates) = &self.blocks[0].certificates {
            self.not_before = certificates.birth.birth.issued_at_unix_seconds;
            if let Some(leaf) = certificates.keys.last() {
                self.principal = leaf.key.principal.clone();
            }
            for cert in &certificates.keys {
                self.not_before = self
                    .not_before
                    .max(cert.key.validity.not_before_unix_seconds);
                self.expires_at = self
                    .expires_at
                    .min(cert.key.validity.expires_at_unix_seconds);
            }
        }
        for block in &self.blocks {
            for caveat in &block.caveats {
                match &caveat.kind {
                    CaveatKind::LineageAppend(lineage) => {
                        self.principal.lineage.extend(lineage.ids.iter().cloned())
                    }
                    CaveatKind::NotBeforeUnixSeconds(time) => {
                        self.not_before = self.not_before.max(*time)
                    }
                    CaveatKind::ExpiresAtUnixSeconds(time) => {
                        self.expires_at = self.expires_at.min(*time)
                    }
                    CaveatKind::Limits(limits) => intersect(&mut self.limits, limits),
                    _ => (),
                }
            }
        }
    }
}
