use crate::{
    FollowFilter, Record, StreamError,
    query_sync::{SyncMutex, watch},
};
use bytes::Bytes;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Weak},
};

/// One shared publication index per stream, with one registration per follow.
#[derive(Default)]
pub(crate) struct FollowIndex {
    next: u64,
    entries: BTreeMap<u64, Entry>,
    ranges: Tree,
    attributes: BTreeMap<String, BTreeMap<Bytes, BTreeSet<u64>>>,
    all: BTreeSet<u64>,
}
struct Entry {
    filter: FollowFilter,
    changed: watch::Sender<u64>,
    notified: u64,
}

pub(crate) struct FollowInterest {
    index: Weak<SyncMutex<FollowIndex>>,
    id: u64,
    pub(crate) changed: watch::Receiver<u64>,
}

impl FollowIndex {
    pub(crate) fn subscribe(
        index: &Arc<SyncMutex<Self>>,
        filter: FollowFilter,
        tail: u64,
    ) -> Result<FollowInterest, StreamError> {
        let mut state = index.lock().map_err(|_| StreamError::Unavailable)?;
        let id = state.next.checked_add(1).ok_or(StreamError::Capacity)?;
        let (changed, receiver) = watch::channel(tail);
        if let Some(ranges) = filter.ranges() {
            for (ordinal, range) in ranges.iter().enumerate() {
                let key = (range.start.clone(), id, ordinal);
                state.ranges = Node::insert(state.ranges.take(), key, range.end.clone());
            }
        } else if let Some(attribute) = filter.attribute() {
            for value in &attribute.any_of {
                state
                    .attributes
                    .entry(attribute.name.clone())
                    .or_default()
                    .entry(value.clone())
                    .or_default()
                    .insert(id);
            }
        } else {
            state.all.insert(id);
        }
        state.entries.insert(
            id,
            Entry {
                filter,
                changed,
                notified: tail,
            },
        );
        state.next = id;
        Ok(FollowInterest {
            index: Arc::downgrade(index),
            id,
            changed: receiver,
        })
    }

    pub(crate) fn publish(&mut self, records: &[Record], tail: u64) {
        let entries = &mut self.entries;
        let mut candidate = |id: u64, record: &Record| {
            if let Some(entry) = entries.get_mut(&id)
                && entry.notified != tail
                && entry.filter.matches(&record.headers)
            {
                entry.notified = tail;
                entry.changed.send_replace(tail);
            }
        };
        for record in records {
            for key in record.headers.keys() {
                Node::query(&self.ranges, key.as_ref(), &mut |id| candidate(id, record));
            }
            for attribute in record.headers.attributes() {
                if let Some(ids) = self
                    .attributes
                    .get(&attribute.name)
                    .and_then(|values| values.get(attribute.value.as_ref()))
                {
                    for &id in ids {
                        candidate(id, record);
                    }
                }
            }
            for &id in &self.all {
                candidate(id, record);
            }
        }
    }

    fn remove(&mut self, id: u64) {
        let Some(entry) = self.entries.remove(&id) else {
            return;
        };
        if let Some(ranges) = entry.filter.ranges() {
            for (ordinal, range) in ranges.iter().enumerate() {
                self.ranges = Node::remove(self.ranges.take(), &(range.start.clone(), id, ordinal));
            }
        } else if let Some(attribute) = entry.filter.attribute() {
            for value in &attribute.any_of {
                if let Some(values) = self.attributes.get_mut(&attribute.name) {
                    if let Some(ids) = values.get_mut(value) {
                        ids.remove(&id);
                        if ids.is_empty() {
                            values.remove(value);
                        }
                    }
                    if values.is_empty() {
                        self.attributes.remove(&attribute.name);
                    }
                }
            }
        } else {
            self.all.remove(&id);
        }
    }
}
impl Drop for FollowInterest {
    fn drop(&mut self) {
        if let Some(index) = self.index.upgrade() {
            index
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(self.id);
        }
    }
}

type Key = (Bytes, u64, usize);
type Tree = Option<Box<Node>>;
struct Node {
    key: Key,
    end: Bytes,
    maximum: Bytes,
    height: u32,
    left: Tree,
    right: Tree,
}
impl Node {
    fn height(tree: &Tree) -> u32 {
        tree.as_ref().map_or(0, |node| node.height)
    }
    fn refresh(&mut self) {
        self.height = 1 + Self::height(&self.left).max(Self::height(&self.right));
        self.maximum = self.end.clone();
        for child in [&self.left, &self.right].into_iter().flatten() {
            if !self.maximum.is_empty()
                && (child.maximum.is_empty() || child.maximum > self.maximum)
            {
                self.maximum = child.maximum.clone();
            }
        }
    }
    fn rotate_left(mut node: Box<Self>) -> Box<Self> {
        let Some(mut root) = node.right.take() else {
            return node;
        };
        node.right = root.left.take();
        node.refresh();
        root.left = Some(node);
        root.refresh();
        root
    }
    fn rotate_right(mut node: Box<Self>) -> Box<Self> {
        let Some(mut root) = node.left.take() else {
            return node;
        };
        node.left = root.right.take();
        node.refresh();
        root.right = Some(node);
        root.refresh();
        root
    }
    fn balance(mut node: Box<Self>) -> Box<Self> {
        node.refresh();
        if Self::height(&node.left) > Self::height(&node.right) + 1 {
            if let Some(left) = &node.left
                && Self::height(&left.right) > Self::height(&left.left)
            {
                node.left = node.left.take().map(Self::rotate_left);
            }
            return Self::rotate_right(node);
        }
        if Self::height(&node.right) > Self::height(&node.left) + 1 {
            if let Some(right) = &node.right
                && Self::height(&right.left) > Self::height(&right.right)
            {
                node.right = node.right.take().map(Self::rotate_right);
            }
            return Self::rotate_left(node);
        }
        node
    }
    fn insert(tree: Tree, key: Key, end: Bytes) -> Tree {
        let Some(mut node) = tree else {
            return Some(Box::new(Self {
                key,
                maximum: end.clone(),
                end,
                height: 1,
                left: None,
                right: None,
            }));
        };
        if key < node.key {
            node.left = Self::insert(node.left.take(), key, end);
        } else {
            node.right = Self::insert(node.right.take(), key, end);
        }
        Some(Self::balance(node))
    }
    fn take_min(mut node: Box<Self>) -> (Box<Self>, Tree) {
        let Some(left) = node.left.take() else {
            let right = node.right.take();
            return (node, right);
        };
        let (minimum, left) = Self::take_min(left);
        node.left = left;
        (minimum, Some(Self::balance(node)))
    }
    fn remove(tree: Tree, key: &Key) -> Tree {
        let mut node = tree?;
        match key.cmp(&node.key) {
            std::cmp::Ordering::Less => node.left = Self::remove(node.left.take(), key),
            std::cmp::Ordering::Greater => node.right = Self::remove(node.right.take(), key),
            std::cmp::Ordering::Equal => {
                let Some(right) = node.right.take() else {
                    return node.left.take();
                };
                let (mut root, right) = Self::take_min(right);
                root.left = node.left.take();
                root.right = right;
                node = root;
            }
        }
        Some(Self::balance(node))
    }
    fn query(tree: &Tree, key: &[u8], candidate: &mut impl FnMut(u64)) {
        let Some(node) = tree else {
            return;
        };
        #[cfg(test)]
        VISITS.with(|visits| visits.set(visits.get() + 1));
        if !node.maximum.is_empty() && node.maximum.as_ref() <= key {
            return;
        }
        Self::query(&node.left, key, candidate);
        if node.key.0.as_ref() <= key {
            if node.end.is_empty() || key < node.end.as_ref() {
                candidate(node.key.1);
            }
            Self::query(&node.right, key, candidate);
        }
    }
}

#[cfg(test)]
thread_local! { static VISITS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CommitId, RecordHeaders, wire};
    #[test]
    fn unrelated_publications_never_notify_filtered_interests() {
        let index = Arc::new(SyncMutex::new(FollowIndex::default()));
        let mut followed = FollowIndex::subscribe(
            &index,
            FollowFilter::new(wire::SessionFilter {
                keys: vec![wire::KeyRange {
                    start: b"a".as_slice().into(),
                    end: b"c".as_slice().into(),
                }],
                attributes: vec![],
            })
            .unwrap(),
            0,
        )
        .unwrap();
        let record = |key: &'static str, sequence| Record {
            sequence,
            value: "body".into(),
            headers: RecordHeaders::new(vec![key.into()], vec![]).unwrap(),
            commit_id: CommitId::default(),
            committed_at_micros: 0,
        };
        index.lock().unwrap().publish(&[record("z", 0)], 1);
        assert!(!followed.changed.has_changed().unwrap());
        index.lock().unwrap().publish(&[record("b", 1)], 2);
        assert!(followed.changed.has_changed().unwrap());
        assert_eq!(*followed.changed.borrow_and_update(), 2);
        drop(followed);
        assert!(index.lock().unwrap().entries.is_empty());
        assert!(index.lock().unwrap().ranges.is_none());
    }
    #[test]
    fn interval_index_stays_balanced_and_matches_a_reference_after_removal() {
        let mut tree = None;
        for id in 0..4096_u64 {
            let start = Bytes::copy_from_slice(&id.to_be_bytes());
            let end = Bytes::copy_from_slice(&(id + 1).to_be_bytes());
            tree = Node::insert(tree, (start, id, 0), end);
        }
        assert!(Node::height(&tree) <= 14);
        for id in (0..4096_u64).step_by(2) {
            tree = Node::remove(tree, &(Bytes::copy_from_slice(&id.to_be_bytes()), id, 0));
        }
        for id in 0..4096_u64 {
            VISITS.with(|visits| visits.set(0));
            let mut matches = vec![];
            Node::query(&tree, &id.to_be_bytes(), &mut |id| matches.push(id));
            assert_eq!(matches, if id % 2 == 1 { vec![id] } else { vec![] });
            assert!(VISITS.with(std::cell::Cell::get) <= 30);
        }
        for id in (1..4096_u64).step_by(2) {
            tree = Node::remove(tree, &(Bytes::copy_from_slice(&id.to_be_bytes()), id, 0));
        }
        assert!(tree.is_none());
    }
}
