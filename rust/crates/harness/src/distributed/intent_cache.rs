// Derived from fully applied immutable records, never a second authority.
// Fingerprints use the same collision-resistance assumption as stream keys.
pub(super) const CAPACITY: usize = 64;

pub(super) struct IntentCache {
    entries: [Option<([u8; 32], [u8; 32])>; CAPACITY],
    revision: u64,
    complete: bool,
}

impl Default for IntentCache {
    fn default() -> Self {
        Self {
            entries: [None; CAPACITY],
            revision: 0,
            complete: true,
        }
    }
}

impl IntentCache {
    pub(super) fn len(&self) -> usize {
        if self.complete {
            self.revision as usize
        } else {
            CAPACITY
        }
    }

    // Outer None requires a history lookup. Inner None proves absence only
    // while every record in the exact verified prefix remains represented.
    pub(super) fn get(&self, key: &[u8; 32], revision: u64) -> Option<Option<[u8; 32]>> {
        if revision != self.revision {
            return None;
        }
        self.entries
            .iter()
            .take(self.len())
            .flatten()
            .find(|(stored, _)| stored == key)
            .map(|(_, digest)| Some(Some(*digest)))
            .unwrap_or_else(|| self.complete.then_some(None))
    }

    pub(super) fn record(&mut self, revision: u64, key: [u8; 32], digest: [u8; 32]) {
        if self.revision.checked_add(1) != Some(revision) {
            self.entries.fill(None);
            self.complete = false;
        }
        self.complete &= revision <= CAPACITY as u64;
        self.entries[(revision.saturating_sub(1) % CAPACITY as u64) as usize] = Some((key, digest));
        self.revision = revision;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        // Every cache answer equals the verified prefix. A miss is definitive
        // only before eviction; stale prefixes and gaps always fall back.
        #[test]
        fn answers_belong_to_the_complete_verified_prefix(length in 0u64..100, query in 0u64..110) {
            let fingerprint = |number: u64| *blake3::hash(&number.to_le_bytes()).as_bytes();
            let mut cache = IntentCache::default();
            for revision in 1..=length {
                cache.record(revision, fingerprint(revision), fingerprint(revision + 1000));
            }
            let expected = (1..=length).contains(&query).then(|| fingerprint(query + 1000));
            if let Some(found) = cache.get(&fingerprint(query), length) {
                prop_assert_eq!(found, expected);
            } else {
                prop_assert!(length > CAPACITY as u64);
            }
            prop_assert_eq!(cache.get(&fingerprint(query), length + 1), None);
            cache.record(length + 2, fingerprint(200), fingerprint(201));
            prop_assert_eq!(cache.get(&fingerprint(0), length + 2), None);
            prop_assert_eq!(cache.get(&fingerprint(200), length + 2), Some(Some(fingerprint(201))));
        }
    }
}
