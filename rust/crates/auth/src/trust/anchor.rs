//! The configured environment anchor: one immutable snapshot per rotation.
use crate::crypto::BlockId;
use std::sync::Arc;

/// Keys that authenticate an environment's trust Stream without a
/// predecessor record: Global's configured public keys. Each value is an
/// immutable snapshot. Its owner publishes every rotation as a new snapshot
/// on a `tokio::sync::watch` channel, and a [`super::Follower`] reads exactly
/// one snapshot per authentication, so it can neither keep keys its owner
/// rotated away nor combine two rotations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AnchorKeys(Arc<Inner>);

#[derive(Debug, PartialEq, Eq)]
struct Inner {
    current: Vec<BlockId>,
    /// Keys rotated out, each valid while `now < until`.
    retiring: Vec<(BlockId, u64)>,
    /// `current` then `retiring`: every key while no overlap has ended.
    all: Arc<[BlockId]>,
}

impl AnchorKeys {
    /// An anchor of exactly `keys`.
    #[must_use]
    pub fn new(keys: Vec<BlockId>) -> Self {
        Self::build(keys, Vec::new())
    }

    /// The live anchor of an owner that never rotates it: fixed host
    /// configuration, or a test.
    #[must_use]
    pub fn fixed(keys: Vec<BlockId>) -> tokio::sync::watch::Receiver<Self> {
        tokio::sync::watch::Sender::new(Self::new(keys)).subscribe()
    }

    /// The snapshot after the owner replaced its keys with `keys` at `now`.
    /// A key leaving the current set stays valid for `overlap_seconds` from
    /// `now`, and never longer; a key named again is current again.
    #[must_use]
    pub fn rotate(&self, keys: Vec<BlockId>, now: u64, overlap_seconds: u64) -> Self {
        let until = now.saturating_add(overlap_seconds);
        let retiring = self
            .0
            .retiring
            .iter()
            .copied()
            .filter(|(key, end)| now < *end && !keys.contains(key))
            .chain(
                self.0
                    .current
                    .iter()
                    .filter(|key| !keys.contains(key))
                    .map(|key| (*key, until)),
            )
            .collect();
        Self::build(keys, retiring)
    }

    /// The keys the owner currently names, without retiring keys.
    #[must_use]
    pub fn current(&self) -> &[BlockId] {
        &self.0.current
    }

    /// Every key valid at `now`: current keys and retiring keys still inside
    /// their overlap window. Shared, not copied, while no overlap has ended.
    #[must_use]
    pub fn at(&self, now: u64) -> Arc<[BlockId]> {
        if self.0.retiring.iter().all(|(_, until)| now < *until) {
            return Arc::clone(&self.0.all);
        }
        self.0
            .current
            .iter()
            .copied()
            .chain(
                self.0
                    .retiring
                    .iter()
                    .filter(|(_, until)| now < *until)
                    .map(|(key, _)| *key),
            )
            .collect()
    }

    fn build(current: Vec<BlockId>, retiring: Vec<(BlockId, u64)>) -> Self {
        let all = current
            .iter()
            .copied()
            .chain(retiring.iter().map(|(key, _)| *key))
            .collect();
        Self(Arc::new(Inner {
            current,
            retiring,
            all,
        }))
    }
}
