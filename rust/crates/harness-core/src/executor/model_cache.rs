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

/// A digest at the final input cut in the original projected HTTP request,
/// bound to its resolved provider family, endpoint base and model.
/// Generation settings preceding the input are included conservatively: equality
/// proves equal projected prefixes, but inequality does not prove a cache miss.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ModelCacheBreakpoint {
    /// Domain-separated BLAKE3 of the resolved identity and projected prefix.
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
    /// The one layout accepted by request producers and observation readers.
    pub const VERSION: u32 = 1;

    /// Reads only the adapter's fixed-size cache value. Missing, unsupported or
    /// malformed observations remain unknown; usage counters are kept separately.
    pub fn reported(metadata: &serde_json::Value) -> Option<Self> {
        let observation = Self::deserialize(metadata.get("cache")?).ok()?;
        (observation.version == Self::VERSION).then_some(observation)
    }

    /// Residency joins require both a known account scope and an exact prefix.
    pub fn key(&self) -> Option<ModelCacheKey> {
        (self.version == Self::VERSION).then_some(ModelCacheKey {
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
    /// Original prefix digest, including resolved provider, endpoint and model.
    pub prefix_digest: [u8; 32],
}

/// An estimate for one account-scoped prefix. Indexing and fork joins belong to
/// the existing export/query projection, never a second capture store.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelCacheResidency {
    /// Residency projection format; older layouts are rebuilt, never migrated.
    pub version: u32,
    /// Scope and prefix to which every update must belong.
    pub key: ModelCacheKey,
    /// Original monotonic clock domain for every timestamp in this estimate.
    /// Equal prefixes do not make different machines' clocks comparable.
    pub clock_domain: [u8; 16],
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
    /// An unrelated clock cannot update a retained deadline or last-hit time.
    #[error(
        "cache event changes its clock domain; select an estimate in the original clock domain"
    )]
    ClockMismatch,
    /// Original source events must be consumed in timestamp order.
    #[error("cache event precedes its prior observation; consume events in source order")]
    TimeReversed,
    /// Deadline arithmetic must not wrap into a false residency claim.
    #[error("cache deadline overflows; retain an unknown deadline for this observation")]
    DeadlineOverflow,
    /// Persisted projection layouts are not migrated.
    #[error(
        "unsupported cache residency version; rebuild only with original clock-domain observations"
    )]
    Version,
}

impl ModelCacheResidency {
    /// The one persisted residency layout; version one lacked clock identity.
    pub const VERSION: u32 = 2;

    /// One fixed-size transition, without IO, allocation, copying payloads,
    /// history scans, clocks or waits. A dropped source requires no cleanup task.
    /// The caller supplies the original clock identity, not a domain inferred
    /// from an agent, provider or prefix. Missing clock evidence cannot create
    /// an estimate. No synchronization between domains is inferred.
    pub fn observe(
        key: ModelCacheKey,
        previous: Option<Self>,
        clock_domain: [u8; 16],
        at_ns: u64,
        access: ModelCacheAccess,
    ) -> Result<Self, ModelCacheError> {
        if let Some(previous) = previous {
            if previous.version != Self::VERSION {
                return Err(ModelCacheError::Version);
            }
            if previous.clock_domain != clock_domain {
                return Err(ModelCacheError::ClockMismatch);
            }
            if at_ns < previous.observed_at_ns {
                return Err(ModelCacheError::TimeReversed);
            }
            if key != previous.key {
                return Err(ModelCacheError::KeyMismatch);
            }
        }
        let mut next = previous.unwrap_or(Self {
            version: Self::VERSION,
            key,
            clock_domain,
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
    /// Queries from a foreign clock remain unknown, even at equal timestamps.
    pub fn estimated_resident_at(&self, clock_domain: [u8; 16], at_ns: u64) -> Option<bool> {
        if self.version != Self::VERSION
            || self.clock_domain != clock_domain
            || at_ns < self.observed_at_ns
        {
            return None;
        }
        self.expires_at_ns.map(|expiry| at_ns < expiry)
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

#[cfg(kani)]
mod proofs {
    use super::*;

    fn access(kind: u8, ttl: ModelCacheTtl) -> ModelCacheAccess {
        match kind {
            0 => ModelCacheAccess::Write(ttl),
            1 => ModelCacheAccess::Hit { refresh: None },
            2 => ModelCacheAccess::Hit { refresh: Some(ttl) },
            _ => ModelCacheAccess::Miss,
        }
    }

    #[kani::proof]
    fn residency_preserves_original_clock_and_checked_deadline() {
        let key = ModelCacheKey {
            credential_scope: kani::any(),
            prefix_digest: kani::any(),
        };
        let clock_domain: [u8; 16] = kani::any();
        let at_ns: u64 = kani::any();
        let seconds: u64 = kani::any();
        let explicit: bool = kani::any();
        let ttl = if explicit {
            ModelCacheTtl::Explicit { seconds }
        } else {
            ModelCacheTtl::ProviderManaged
        };
        let kind: u8 = kani::any();
        kani::assume(kind < 4);
        let first_access = access(kind, ttl);
        // Wider arithmetic independently detects either u64 overflow boundary.
        let wide_deadline = u128::from(at_ns) + u128::from(seconds) * 1_000_000_000;
        let result = ModelCacheResidency::observe(key, None, clock_domain, at_ns, first_access);
        if matches!(kind, 0 | 2) && explicit && wide_deadline > u128::from(u64::MAX) {
            assert_eq!(result, Err(ModelCacheError::DeadlineOverflow));
            return;
        }
        let observed = result.unwrap();
        assert_eq!(observed.version, ModelCacheResidency::VERSION);
        assert_eq!(observed.key, key);
        assert_eq!(observed.clock_domain, clock_domain);
        assert_eq!(observed.observed_at_ns, at_ns);
        assert_eq!(observed.written_at_ns, (kind == 0).then_some(at_ns));
        assert_eq!(
            observed.last_hit_at_ns,
            matches!(kind, 1 | 2).then_some(at_ns)
        );
        let expiry = if kind == 3 {
            Some(at_ns)
        } else if matches!(kind, 0 | 2) && explicit {
            Some(u64::try_from(wide_deadline).unwrap())
        } else {
            None
        };
        assert_eq!(observed.expires_at_ns, expiry);
        assert_eq!(
            ModelCacheResidency::observe(key, Some(observed), clock_domain, at_ns, first_access),
            Ok(observed)
        );
        let query: u64 = kani::any();
        let expected = if query < at_ns {
            None
        } else {
            expiry.map(|expiry| query < expiry)
        };
        assert_eq!(
            observed.estimated_resident_at(clock_domain, query),
            expected
        );

        // Check every access against the actual retained result. The wider
        // deadline and field-preservation assertions are independent contracts.
        let next_kind: u8 = kani::any();
        kani::assume(next_kind < 4);
        let next_seconds: u64 = kani::any();
        let next_explicit: bool = kani::any();
        let next_ttl = if next_explicit {
            ModelCacheTtl::Explicit {
                seconds: next_seconds,
            }
        } else {
            ModelCacheTtl::ProviderManaged
        };
        let next = ModelCacheResidency::observe(
            key,
            Some(observed),
            clock_domain,
            query,
            access(next_kind, next_ttl),
        );
        let refreshed_deadline = u128::from(query) + u128::from(next_seconds) * 1_000_000_000;
        kani::cover!(query < at_ns);
        if query < at_ns {
            assert_eq!(next, Err(ModelCacheError::TimeReversed));
        } else if matches!(next_kind, 0 | 2)
            && next_explicit
            && refreshed_deadline > u128::from(u64::MAX)
        {
            assert_eq!(next, Err(ModelCacheError::DeadlineOverflow));
        } else {
            let next = next.unwrap();
            assert_eq!(next.version, observed.version);
            assert_eq!(next.key, observed.key);
            assert_eq!(next.clock_domain, observed.clock_domain);
            assert_eq!(next.observed_at_ns, query);
            assert_eq!(
                next.written_at_ns,
                if next_kind == 0 {
                    Some(query)
                } else {
                    observed.written_at_ns
                },
            );
            assert_eq!(
                next.last_hit_at_ns,
                if matches!(next_kind, 1 | 2) {
                    Some(query)
                } else {
                    observed.last_hit_at_ns
                },
            );
            let expiry = match next_kind {
                1 => expiry.filter(|expiry| *expiry > query),
                3 => Some(query),
                _ if next_explicit => Some(u64::try_from(refreshed_deadline).unwrap()),
                _ => None,
            };
            assert_eq!(next.expires_at_ns, expiry);
            kani::cover!(next_kind == 1 && observed.expires_at_ns.is_some_and(|end| end <= query));
            kani::cover!(next_kind == 0);
            kani::cover!(next_kind == 2);
            kani::cover!(next_kind == 3);
            kani::cover!(next_kind == 2 && !next_explicit && observed.expires_at_ns.is_some());
        }

        let mut foreign_domain = clock_domain;
        foreign_domain[0] ^= 1;
        assert_eq!(observed.estimated_resident_at(foreign_domain, query), None);
        assert_eq!(
            ModelCacheResidency::observe(
                key,
                Some(observed),
                foreign_domain,
                at_ns,
                ModelCacheAccess::Miss,
            ),
            Err(ModelCacheError::ClockMismatch)
        );
        let mut foreign_key = key;
        if kani::any() {
            foreign_key.credential_scope[0] ^= 1;
        } else {
            foreign_key.prefix_digest[0] ^= 1;
        }
        assert_eq!(
            ModelCacheResidency::observe(
                foreign_key,
                Some(observed),
                clock_domain,
                at_ns,
                first_access
            ),
            Err(ModelCacheError::KeyMismatch)
        );
        let version: u32 = kani::any();
        if version != ModelCacheResidency::VERSION {
            let unsupported = ModelCacheResidency {
                version,
                ..observed
            };
            assert_eq!(unsupported.estimated_resident_at(clock_domain, query), None);
            assert_eq!(
                ModelCacheResidency::observe(
                    key,
                    Some(unsupported),
                    clock_domain,
                    at_ns,
                    ModelCacheAccess::Miss,
                ),
                Err(ModelCacheError::Version)
            );
        }
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
            clock_domain in any::<[u8; 16]>(),
        ) {
            let key = ModelCacheKey { credential_scope: [1; 32], prefix_digest: [2; 32] };
            let ttl = ModelCacheTtl::Explicit { seconds };
            let expiry = start + seconds * 1_000_000_000;
            let written = ModelCacheResidency::observe(key, None, clock_domain, start, ModelCacheAccess::Write(ttl)).unwrap();
            prop_assert_eq!(written.estimated_resident_at(clock_domain, start), Some(true));
            prop_assert_eq!(written.estimated_resident_at(clock_domain, expiry), Some(false));
            prop_assert_eq!(written.estimated_resident_at(clock_domain, start - 1), None);
            let mut foreign_domain = clock_domain;
            foreign_domain[0] ^= 1;
            prop_assert_eq!(written.estimated_resident_at(foreign_domain, start), None);
            prop_assert_eq!(ModelCacheResidency::observe(key, Some(written), foreign_domain, start,
                ModelCacheAccess::Miss), Err(ModelCacheError::ClockMismatch));
            let hit = ModelCacheResidency::observe(key, Some(written), clock_domain, start + 1,
                ModelCacheAccess::Hit { refresh: None }).unwrap();
            prop_assert_eq!(hit.expires_at_ns, written.expires_at_ns);
            prop_assert_eq!(hit.last_hit_at_ns, Some(start + 1));
            let refreshed = ModelCacheResidency::observe(key, Some(hit), clock_domain, start + 2,
                ModelCacheAccess::Hit { refresh: Some(ttl) }).unwrap();
            prop_assert_eq!(refreshed.expires_at_ns, Some(expiry + 2));
            let miss = ModelCacheResidency::observe(key, Some(refreshed), clock_domain, start + 3, ModelCacheAccess::Miss).unwrap();
            prop_assert_eq!(miss.estimated_resident_at(clock_domain, start + 3), Some(false));
            let unknown = ModelCacheResidency::observe(key, Some(miss), clock_domain, start + 4,
                ModelCacheAccess::Write(ModelCacheTtl::ProviderManaged)).unwrap();
            prop_assert_eq!(unknown.estimated_resident_at(clock_domain, start + 4), None);
            prop_assert_eq!(ModelCacheResidency::observe(key, Some(hit), clock_domain, start,
                ModelCacheAccess::Miss), Err(ModelCacheError::TimeReversed));
            let foreign = ModelCacheKey { credential_scope: [3; 32], ..key };
            prop_assert_eq!(ModelCacheResidency::observe(foreign, Some(hit), clock_domain, start + 2,
                ModelCacheAccess::Miss), Err(ModelCacheError::KeyMismatch));
            let foreign = ModelCacheKey { prefix_digest: [3; 32], ..key };
            prop_assert_eq!(ModelCacheResidency::observe(foreign, Some(hit), clock_domain, start + 2,
                ModelCacheAccess::Miss), Err(ModelCacheError::KeyMismatch));
            let late_hit = ModelCacheResidency::observe(key, Some(written), clock_domain, expiry,
                ModelCacheAccess::Hit { refresh: None }).unwrap();
            prop_assert_eq!(late_hit.estimated_resident_at(clock_domain, expiry), None);
            prop_assert_eq!(ModelCacheResidency::observe(key, Some(hit), clock_domain, u64::MAX,
                ModelCacheAccess::Write(ttl)), Err(ModelCacheError::DeadlineOverflow));
            for version in [1, ModelCacheResidency::VERSION + 1] {
                let unsupported = ModelCacheResidency { version, ..hit };
                prop_assert_eq!(unsupported.estimated_resident_at(clock_domain, start + 2), None);
                prop_assert_eq!(ModelCacheResidency::observe(key, Some(unsupported), clock_domain, start + 2,
                    ModelCacheAccess::Miss), Err(ModelCacheError::Version));
            }
            let measured = acyclic_test_support::measure(|| {
                std::hint::black_box(ModelCacheResidency::observe(key, Some(hit), clock_domain, start + 2,
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
