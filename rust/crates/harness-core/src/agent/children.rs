//! Persistent child membership. Checkpoints retain two roots and counts;
//! point updates copy at most one path in a 128-bit Patricia tree. Immutable
//! nodes are staged before the checkpoint publishes their root, so a failed
//! actor commit can leave only unreachable nodes, never visible membership.
use super::{AgentId, AgentPath, Error};
use acyclic_stream::StreamProviderExt as _;
use acyclic_stream::{
    AppendOutcome, AppendRequest, BoxProviderFuture, ProviderPlatform, ReadRequest, StreamPath,
    StreamProvider,
};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, PoisonError},
};

type Digest = [u8; 32];

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Node {
    Leaf([u8; 16]),
    Branch {
        bit: u8,
        left: Digest,
        right: Digest,
        max: [u8; 16],
    },
}

trait Storage: ProviderPlatform {
    fn load(&self, digest: Digest) -> BoxProviderFuture<'_, Result<Node, Error>>;
    fn save(&self, digest: Digest, bytes: Vec<u8>) -> BoxProviderFuture<'_, Result<(), Error>>;
}
struct Provider<P: ?Sized>(Arc<P>);

fn path(digest: Digest) -> Result<StreamPath, Error> {
    StreamPath::new(format!(
        "harness/child-nodes/{}",
        blake3::Hash::from_bytes(digest).to_hex()
    ))
    .map_err(Error::Stream)
}

impl<P: StreamProvider + ?Sized> Storage for Provider<P> {
    fn load(&self, digest: Digest) -> BoxProviderFuture<'_, Result<Node, Error>> {
        Box::pin(async move {
            let mut records = self
                .0
                .read(ReadRequest {
                    path: path(digest)?,
                    from: 0,
                    limit: 1,
                    key: None,
                    at_least: Default::default(),
                })
                .await
                .map_err(Error::Stream)?;
            let record = records
                .try_next()
                .await
                .map_err(Error::Stream)?
                .ok_or(Error::Record("child index node is missing"))?;
            if record.sequence != 0 || *blake3::hash(&record.value).as_bytes() != digest {
                return Err(Error::Record("child index node digest differs"));
            }
            serde_json::from_slice(&record.value)
                .map_err(|_| Error::Record("child index node is malformed"))
        })
    }
    fn save(&self, digest: Digest, bytes: Vec<u8>) -> BoxProviderFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            let request = AppendRequest {
                path: path(digest)?,
                records: vec![bytes.into()],
                if_tail: Some(0),
                idempotency_key: super::intent_key(
                    "child-node",
                    &[&blake3::Hash::from_bytes(digest).to_hex()],
                )?,
            };
            match self.0.append(request).await.map_err(Error::Stream)? {
                AppendOutcome::Committed(_) => Ok(()),
                AppendOutcome::TailConflict { .. } => {
                    // A node shared with an earlier root already exists. Its
                    // digest binds the complete immutable contents.
                    self.load(digest).await.map(|_| ())
                }
            }
        })
    }
}

/// Child identities at an immutable publication cut. Cloning a set shares its
/// root; neither checkpoints nor turns copy the complete membership list.
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Children {
    root: Option<Digest>,
    count: usize,
    #[serde(skip)]
    nodes: Arc<Mutex<BTreeMap<Digest, Node>>>,
    #[serde(skip)]
    storage: Option<Arc<dyn Storage>>,
}
impl std::fmt::Debug for Children {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Children")
            .field("root", &self.root)
            .field("count", &self.count)
            .finish()
    }
}
impl PartialEq for Children {
    fn eq(&self, other: &Self) -> bool {
        self.root == other.root && self.count == other.count
    }
}
impl Eq for Children {}

fn right(id: [u8; 16], bit: u8) -> bool {
    id[usize::from(bit) / 8] & (1 << (7 - bit % 8)) != 0
}
type Branch = (Digest, u8, Digest, bool);

impl Children {
    /// Empty membership, without a storage binding.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
    /// Number of open children, without reading a membership page.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.count
    }
    /// Whether the set is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.count == 0
    }
    /// Discards the root without walking any child.
    pub fn clear(&mut self) {
        self.root = None;
        self.count = 0;
    }
    /// Bind immutable node IO after decoding a checkpoint.
    pub(crate) fn bind<P: StreamProvider + ?Sized>(&mut self, provider: Arc<P>) {
        self.storage = Some(Arc::new(Provider(provider)));
    }
    /// Flush nodes created before storage was bound, as during adoption.
    pub(crate) async fn persist(&self) -> Result<(), Error> {
        if let Some(storage) = &self.storage {
            let nodes = self
                .nodes
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone();
            for (digest, node) in nodes {
                let bytes = serde_json::to_vec(&node)
                    .map_err(|_| Error::Record("child index does not encode"))?;
                storage.save(digest, bytes).await?;
            }
            self.nodes
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clear();
        }
        Ok(())
    }
    async fn node(&self, digest: Digest) -> Result<Node, Error> {
        let cached = self
            .nodes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&digest)
            .cloned();
        if let Some(node) = cached {
            return Ok(node);
        }
        self.storage
            .as_ref()
            .ok_or(Error::Record("child index storage is not bound"))?
            .load(digest)
            .await
    }
    async fn stage(&self, node: Node) -> Result<Digest, Error> {
        let bytes =
            serde_json::to_vec(&node).map_err(|_| Error::Record("child index does not encode"))?;
        let digest = *blake3::hash(&bytes).as_bytes();
        if let Some(storage) = &self.storage {
            storage.save(digest, bytes).await?;
        } else {
            self.nodes
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(digest, node);
        }
        Ok(digest)
    }
    async fn walk(
        &self,
        id: [u8; 16],
        mut descend: impl FnMut(Branch),
    ) -> Result<Option<([u8; 16], Digest)>, Error> {
        let mut previous = None;
        let mut next = self.root;
        while let Some(digest) = next {
            match self.node(digest).await? {
                Node::Leaf(found) => return Ok(Some((found, digest))),
                Node::Branch {
                    bit,
                    left,
                    right: other,
                    ..
                } => {
                    if bit >= 128 || previous.is_some_and(|previous| previous >= bit) {
                        return Err(Error::Record("child index branch order is invalid"));
                    }
                    previous = Some(bit);
                    let direction = right(id, bit);
                    descend((digest, bit, if direction { left } else { other }, direction));
                    next = Some(if direction { other } else { left });
                }
            }
        }
        Ok(None)
    }
    /// Membership needs at most 128 branches and one leaf, independent of agent count.
    pub async fn contains(&self, child: &AgentPath) -> Result<bool, Error> {
        let id = child.id_bytes();
        Ok(self
            .walk(id, |_| {})
            .await?
            .is_some_and(|(found, _)| found == id))
    }
    async fn branch(&self, bit: u8, left: Digest, right: Digest) -> Result<Digest, Error> {
        let max = match self.node(right).await? {
            Node::Leaf(id) | Node::Branch { max: id, .. } => id,
        };
        self.stage(Node::Branch {
            bit,
            left,
            right,
            max,
        })
        .await
    }
    async fn rebuild(&self, mut root: Digest, branches: Vec<Branch>) -> Result<Digest, Error> {
        for (_, bit, sibling, direction) in branches.into_iter().rev() {
            let (left, right) = if direction {
                (sibling, root)
            } else {
                (root, sibling)
            };
            root = self.branch(bit, left, right).await?;
        }
        Ok(root)
    }
    /// Inserts one child and stages only the nodes on its changed path.
    pub async fn insert(&mut self, child: AgentPath) -> Result<bool, Error> {
        let id = child.id_bytes();
        let mut branches = Vec::new();
        let found = self.walk(id, |branch| branches.push(branch)).await?;
        if found.is_some_and(|(found, _)| found == id) {
            return Ok(false);
        }
        let leaf = self.stage(Node::Leaf(id)).await?;
        let root = if let Some((found, old_leaf)) = found {
            let bit = (0u8..128)
                .find(|bit| right(id, *bit) != right(found, *bit))
                .ok_or(Error::Record("child identities are indistinguishable"))?;
            let split = branches.partition_point(|(_, previous, _, _)| *previous < bit);
            let old = branches
                .get(split)
                .map_or(old_leaf, |(digest, _, _, _)| *digest);
            branches.truncate(split);
            let (left, right) = if right(id, bit) {
                (old, leaf)
            } else {
                (leaf, old)
            };
            self.branch(bit, left, right).await?
        } else {
            leaf
        };
        self.root = Some(self.rebuild(root, branches).await?);
        self.count = self
            .count
            .checked_add(1)
            .ok_or(Error::Record("child count overflows this platform"))?;
        Ok(true)
    }
    /// Removes one child; duplicate completion leaves membership unchanged.
    pub async fn remove(&mut self, child: &AgentPath) -> Result<bool, Error> {
        let id = child.id_bytes();
        let mut branches = Vec::new();
        let found = self.walk(id, |branch| branches.push(branch)).await?;
        if !found.is_some_and(|(found, _)| found == id) {
            return Ok(false);
        }
        self.root = match branches.pop() {
            Some((_, _, sibling, _)) => Some(self.rebuild(sibling, branches).await?),
            None => None,
        };
        self.count = self
            .count
            .checked_sub(1)
            .ok_or(Error::Record("child index count differs"))?;
        Ok(true)
    }
    /// The first identity, for a bounded archive continuation.
    pub async fn first(&self) -> Result<Option<AgentId>, Error> {
        let mut next = self.root;
        let mut previous = None;
        while let Some(digest) = next {
            match self.node(digest).await? {
                Node::Leaf(id) => return Ok(Some(AgentId::from_bytes(id))),
                Node::Branch { bit, left, .. } => {
                    if bit >= 128 || previous.is_some_and(|previous| previous >= bit) {
                        return Err(Error::Record("child index branch order is invalid"));
                    }
                    previous = Some(bit);
                    next = Some(left);
                }
            }
        }
        Ok(None)
    }
    /// Up to `limit` child identities after `after`, in identity order. A
    /// caller continues from the last returned identity until the page empties.
    pub async fn page(&self, after: Option<&AgentId>, limit: usize) -> Result<Vec<AgentId>, Error> {
        let after = after.map(AgentId::to_bytes);
        let mut pending = self
            .root
            .map(|root| (root, None))
            .into_iter()
            .collect::<Vec<_>>();
        let mut page = Vec::new();
        while page.len() < limit {
            let Some((digest, previous)) = pending.pop() else {
                break;
            };
            match self.node(digest).await? {
                Node::Leaf(id) if after.is_none_or(|after| id > after) => {
                    page.push(AgentId::from_bytes(id))
                }
                Node::Leaf(_) => {}
                Node::Branch {
                    bit,
                    left,
                    right,
                    max,
                } => {
                    if bit >= 128 || previous.is_some_and(|previous| previous >= bit) {
                        return Err(Error::Record("child index branch order is invalid"));
                    }
                    // Skip complete subtrees before the cursor. Each page
                    // visits one search path plus its own returned nodes.
                    if after.is_none_or(|after| max > after) {
                        pending.push((right, Some(bit)));
                        pending.push((left, Some(bit)));
                    }
                }
            }
        }
        Ok(page)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt as _;
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // Count calls into the original provider adapter, which still writes and
    // decodes the actual immutable node records on MemoryStream.
    struct Counted {
        provider: Provider<acyclic_stream::MemoryStream>,
        loads: AtomicUsize,
        saves: AtomicUsize,
    }

    impl Storage for Counted {
        fn load(&self, digest: Digest) -> BoxProviderFuture<'_, Result<Node, Error>> {
            self.loads.fetch_add(1, Ordering::Relaxed);
            self.provider.load(digest)
        }

        fn save(&self, digest: Digest, bytes: Vec<u8>) -> BoxProviderFuture<'_, Result<(), Error>> {
            self.saves.fetch_add(1, Ordering::Relaxed);
            self.provider.save(digest, bytes)
        }
    }

    impl Counted {
        fn cost(&self, loads: usize, saves: usize) {
            assert!(
                self.loads.swap(0, Ordering::Relaxed) <= loads,
                "membership operation scanned beyond its fixed node credit"
            );
            assert!(
                self.saves.swap(0, Ordering::Relaxed) <= saves,
                "membership operation copied beyond its fixed node credit"
            );
        }
    }

    #[tokio::test]
    async fn persistent_pages_and_point_updates_match_membership_after_reopen()
    -> Result<(), Box<dyn std::error::Error>> {
        let parent = AgentPath::root(&AgentId::from_bytes([1; 16]));
        let provider = Arc::new(acyclic_stream::MemoryStream::default());
        let counted = Arc::new(Counted {
            provider: Provider(provider.clone()),
            loads: AtomicUsize::new(0),
            saves: AtomicUsize::new(0),
        });
        let mut children = Children::new();
        let mut expected = BTreeSet::new();
        for n in 0u64..513 {
            let id = AgentId::derive(&parent.id(), &n.to_string());
            let child = parent.child(&id)?;
            assert!(children.insert(child.clone()).await?);
            counted.cost(258, 130);
            assert!(!children.insert(child).await?);
            counted.cost(129, 0);
            expected.insert(id);
            if n == 2 {
                // Adoption stages real nodes before binding their provider.
                // Cached point reads must not allocate an identity or trail.
                for requested in 0..=3 {
                    let probe =
                        parent.child(&AgentId::derive(&parent.id(), &requested.to_string()))?;
                    let mut result = None;
                    let allocations = acyclic_test_support::measure(|| {
                        result = children.contains(&probe).now_or_never();
                    });
                    assert_eq!(result.transpose()?, Some(requested < 3));
                    assert_eq!(
                        allocations.count_total, 0,
                        "cached membership lookup allocated identity or branch storage"
                    );
                }
                children.storage = Some(counted.clone());
                children.persist().await?;
                counted.cost(0, 130);
            }
        }
        let snapshot = children.clone();
        for (n, id) in expected.clone().into_iter().enumerate() {
            if n % 3 == 0 {
                let child = parent.child(&id)?;
                assert!(children.remove(&child).await?);
                counted.cost(258, 130);
                assert!(!children.remove(&child).await?);
                counted.cost(129, 0);
                expected.remove(&id);
            }
        }
        assert_eq!(snapshot.len(), 513);
        let encoded = serde_json::to_vec(&children)?;
        assert!(
            encoded.len() < 256,
            "checkpoint holds only a root and count"
        );
        let mut reopened: Children = serde_json::from_slice(&encoded)?;
        reopened.storage = Some(counted.clone());
        assert_eq!(reopened.len(), expected.len());
        let mut actual = Vec::new();
        loop {
            let page = reopened.page(actual.last(), 17).await?;
            counted.cost(2 * 128 + 2 * 17 + 1, 0);
            if page.is_empty() {
                break;
            }
            for id in &page {
                assert!(reopened.contains(&parent.child(id)?).await?);
                counted.cost(129, 0);
            }
            actual.extend(page);
        }
        assert_eq!(actual, expected.into_iter().collect::<Vec<_>>());
        assert_eq!(reopened.first().await?, actual.first().cloned());
        counted.cost(2 * 128 + 3, 0);
        assert!(reopened.page(None, 0).await?.is_empty());
        counted.cost(0, 0);
        // Losing the source of an immutable root is a typed error on the
        // same lookup, with no retry or readiness wait.
        reopened.storage = None;
        assert!(matches!(
            reopened.first().await,
            Err(Error::Record("child index storage is not bound"))
        ));
        Ok(())
    }
}
