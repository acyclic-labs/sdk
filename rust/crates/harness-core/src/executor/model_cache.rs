//! Cache facts and constant-space projections over committed call observations.
use serde::{Deserialize, Serialize};

/// Requested cache lifetime. Provider-managed residency has no inferred deadline.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum ModelCacheTtl {
    /// The provider chooses both retention and eviction.
    ProviderManaged,
    /// An explicit retention request; early provider eviction remains possible.
    Explicit {
        /// Requested lifetime in seconds.
        seconds: u64,
    },
}

/// A digest at the final input cut in the original projected HTTP request.
/// Generation settings preceding the input are included conservatively: equality
/// proves equal projected prefixes, but inequality does not prove a cache miss.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ModelCacheBreakpoint {
    /// Domain-separated BLAKE3 through the final projected message.
    pub digest: [u8; 32],
    /// Same hash immediately before that message, forming a sparse prefix tree.
    pub parent_digest: [u8; 32],
    /// Count of projected wire messages, rather than canonical harness messages.
    pub messages: u64,
}

/// Original request cache policy, distinct from reported cache token usage.
/// The current projector records one final input cut, including implicit caching.
/// It does not claim that the provider cached or hit that exact cut.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ModelCacheObservation {
    /// Cache observation format, currently one.
    pub version: u32,
    /// Nonsecret account/endpoint scope digest. Unknown forbids residency joins.
    pub credential_scope: Option<[u8; 32]>,
    /// Configured shared profile prefix; distinct from a full prompt digest.
    pub profile_prefix_digest: Option<[u8; 32]>,
    /// Requested retention class, never a claim of actual residency.
    pub ttl: ModelCacheTtl,
    /// Exact projected prefix; None if the original cut was not captured.
    pub breakpoint: Option<ModelCacheBreakpoint>,
}

impl ModelCacheObservation {
    /// Reads only the adapter's fixed-size cache value. Missing, unsupported or
    /// malformed observations remain unknown; usage counters are kept separately.
    pub fn reported(metadata: &serde_json::Value) -> Option<Self> {
        let observation = Self::deserialize(metadata.get("cache")?).ok()?;
        (observation.version == 1).then_some(observation)
    }

    /// Residency joins require both a known account scope and an exact prefix.
    pub fn key(&self) -> Option<ModelCacheKey> {
        (self.version == 1).then_some(ModelCacheKey {
            credential_scope: self.credential_scope?,
            prefix_digest: self.breakpoint?.digest,
        })
    }
}

/// Exact identity of one cache-residency estimate. Different accounts cannot
/// share an estimate even when their projected prompt bytes match.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelCacheKey {
    /// Original nonsecret account/endpoint digest.
    pub credential_scope: [u8; 32],
    /// Original projected prefix digest.
    pub prefix_digest: [u8; 32],
}

/// An estimate for one account-scoped prefix. Indexing and fork joins belong to
/// the existing export/query projection, never a second capture store.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelCacheResidency {
    /// Residency projection format, currently one.
    pub version: u32,
    /// Scope and prefix to which every update must belong.
    pub key: ModelCacheKey,
    /// Last event's original timestamp in a caller-selected monotonic time domain.
    pub observed_at_ns: u64,
    /// Original last write timestamp; None means no observed write.
    pub written_at_ns: Option<u64>,
    /// Original last hit timestamp; None means no observed hit.
    pub last_hit_at_ns: Option<u64>,
    /// Estimated expiry from an explicit lifetime; None means unknown.
    pub expires_at_ns: Option<u64>,
}

/// Evidence attributed to one exact prefix by the source. A positive per-call
/// cache-read count alone cannot identify which shorter prefix the provider hit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ModelCacheAccess {
    /// A write creates a retention estimate.
    Write(ModelCacheTtl),
    /// A hit proves residency only at this instant. Whether it refreshes the
    /// deadline is supplied by the provider policy, never inferred.
    Hit {
        /// Explicit refresh policy; None preserves the previous estimate.
        refresh: Option<ModelCacheTtl>,
    },
    /// A confirmed miss invalidates an earlier retention estimate.
    Miss,
}

/// A refused projection update; the prior contribution remains untouched.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ModelCacheError {
    /// An index must never combine accounts or unrelated prefixes.
    #[error("cache event changes its scope or prefix; select the residency entry by its exact key")]
    KeyMismatch,
    /// Original source events must be consumed in timestamp order.
    #[error("cache event precedes its prior observation; consume events in source order")]
    TimeReversed,
    /// Deadline arithmetic must not wrap into a false residency claim.
    #[error("cache deadline overflows; retain an unknown deadline for this observation")]
    DeadlineOverflow,
    /// Persisted projection layouts are not migrated.
    #[error(
        "unsupported cache residency version; rebuild the projection from original observations"
    )]
    Version,
}

impl ModelCacheResidency {
    /// One fixed-size transition, without IO, allocation, copying payloads,
    /// history scans, clocks or waits. A dropped source requires no cleanup task.
    pub fn observe(
        key: ModelCacheKey,
        previous: Option<Self>,
        at_ns: u64,
        access: ModelCacheAccess,
    ) -> Result<Self, ModelCacheError> {
        if let Some(previous) = previous {
            if previous.version != 1 {
                return Err(ModelCacheError::Version);
            }
            if at_ns < previous.observed_at_ns {
                return Err(ModelCacheError::TimeReversed);
            }
            if key != previous.key {
                return Err(ModelCacheError::KeyMismatch);
            }
        }
        let mut next = previous.unwrap_or(Self {
            version: 1,
            key,
            observed_at_ns: at_ns,
            written_at_ns: None,
            last_hit_at_ns: None,
            expires_at_ns: None,
        });
        next.observed_at_ns = at_ns;
        match access {
            ModelCacheAccess::Write(ttl) => {
                next.written_at_ns = Some(at_ns);
                next.expires_at_ns = deadline(at_ns, ttl)?;
            }
            ModelCacheAccess::Hit { refresh } => {
                next.last_hit_at_ns = Some(at_ns);
                if let Some(ttl) = refresh {
                    next.expires_at_ns = deadline(at_ns, ttl)?;
                } else if next.expires_at_ns.is_some_and(|expiry| expiry <= at_ns) {
                    // The hit disproves an expired estimate, but supplies no
                    // evidence for a new retention deadline.
                    next.expires_at_ns = None;
                }
            }
            ModelCacheAccess::Miss => next.expires_at_ns = Some(at_ns),
        }
        Ok(next)
    }

    /// An unexpired estimate is still subject to provider eviction. Unknown
    /// retention stays unknown; a deadline is exclusive.
    pub fn estimated_resident_at(&self, at_ns: u64) -> Option<bool> {
        (self.version == 1 && at_ns >= self.observed_at_ns)
            .then(|| self.expires_at_ns.map(|expiry| at_ns < expiry))
            .flatten()
    }
}

fn deadline(at_ns: u64, ttl: ModelCacheTtl) -> Result<Option<u64>, ModelCacheError> {
    match ttl {
        ModelCacheTtl::ProviderManaged => Ok(None),
        ModelCacheTtl::Explicit { seconds } => seconds
            .checked_mul(1_000_000_000)
            .and_then(|duration| at_ns.checked_add(duration))
            .map(Some)
            .ok_or(ModelCacheError::DeadlineOverflow),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn cache_residency_transitions_preserve_unknowns_and_source_order(
            start in 1u64..1_000_000_000, seconds in 1u64..100_000,
        ) {
            let key = ModelCacheKey { credential_scope: [1; 32], prefix_digest: [2; 32] };
            let ttl = ModelCacheTtl::Explicit { seconds };
            let expiry = start + seconds * 1_000_000_000;
            let written = ModelCacheResidency::observe(key, None, start, ModelCacheAccess::Write(ttl)).unwrap();
            prop_assert_eq!(written.estimated_resident_at(start), Some(true));
            prop_assert_eq!(written.estimated_resident_at(expiry), Some(false));
            prop_assert_eq!(written.estimated_resident_at(start - 1), None);
            let hit = ModelCacheResidency::observe(key, Some(written), start + 1,
                ModelCacheAccess::Hit { refresh: None }).unwrap();
            prop_assert_eq!(hit.expires_at_ns, written.expires_at_ns);
            prop_assert_eq!(hit.last_hit_at_ns, Some(start + 1));
            let refreshed = ModelCacheResidency::observe(key, Some(hit), start + 2,
                ModelCacheAccess::Hit { refresh: Some(ttl) }).unwrap();
            prop_assert_eq!(refreshed.expires_at_ns, Some(expiry + 2));
            let miss = ModelCacheResidency::observe(key, Some(refreshed), start + 3, ModelCacheAccess::Miss).unwrap();
            prop_assert_eq!(miss.estimated_resident_at(start + 3), Some(false));
            let unknown = ModelCacheResidency::observe(key, Some(miss), start + 4,
                ModelCacheAccess::Write(ModelCacheTtl::ProviderManaged)).unwrap();
            prop_assert_eq!(unknown.estimated_resident_at(start + 4), None);
            prop_assert_eq!(ModelCacheResidency::observe(key, Some(hit), start,
                ModelCacheAccess::Miss), Err(ModelCacheError::TimeReversed));
            let foreign = ModelCacheKey { credential_scope: [3; 32], ..key };
            prop_assert_eq!(ModelCacheResidency::observe(foreign, Some(hit), start + 2,
                ModelCacheAccess::Miss), Err(ModelCacheError::KeyMismatch));
            let foreign = ModelCacheKey { prefix_digest: [3; 32], ..key };
            prop_assert_eq!(ModelCacheResidency::observe(foreign, Some(hit), start + 2,
                ModelCacheAccess::Miss), Err(ModelCacheError::KeyMismatch));
            let late_hit = ModelCacheResidency::observe(key, Some(written), expiry,
                ModelCacheAccess::Hit { refresh: None }).unwrap();
            prop_assert_eq!(late_hit.estimated_resident_at(expiry), None);
            prop_assert_eq!(ModelCacheResidency::observe(key, Some(hit), u64::MAX,
                ModelCacheAccess::Write(ttl)), Err(ModelCacheError::DeadlineOverflow));
            let mut unsupported = hit;
            unsupported.version = 2;
            prop_assert_eq!(ModelCacheResidency::observe(key, Some(unsupported), start + 2,
                ModelCacheAccess::Miss), Err(ModelCacheError::Version));
            let measured = acyclic_test_support::measure(|| {
                std::hint::black_box(ModelCacheResidency::observe(key, Some(hit), start + 2,
                    ModelCacheAccess::Hit { refresh: Some(ttl) }).unwrap());
            });
            prop_assert_eq!((measured.count_total, measured.bytes_total), (0, 0));
            let control = acyclic_test_support::measure(|| {
                std::hint::black_box(Box::new(hit));
            });
            prop_assert!(control.count_total > 0);
        }
    }
}
