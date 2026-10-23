use super::{State, ObservationSignal};
use crate::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct Volume {
    pub(super) owner: [u8; 16],
    pub(super) id: crate::VolumeId,
}

pub(super) type Key = (Volume, u64);
type Tree = Option<Box<Node>>;

// Each registration owns one node. Tree rotations and removal move boxes,
// never allocate or retire a different lease's storage. A shared BTreeMap node
// instead puts allocation and retirement on whichever publisher runs first or
// last, crossing otherwise independent operations' allocation lifetimes.
#[derive(Default)]
pub(super) struct Registry {
    root: Tree,
    next: u64,
}

pub(super) struct Node {
    key: Key,
    projection: bool,
    observer: Option<Arc<ObservationSignal>>,
    height: usize,
    total: State,
    left: Tree,
    right: Tree,
}

impl Registry {
    pub(super) fn counts(&self, volume: Volume) -> State {
        let lower = prefix(&self.root, volume, false);
        let upper = prefix(&self.root, volume, true);
        State {
            projections: upper.projections - lower.projections,
            editors: upper.editors - lower.editors,
            observers: upper.observers - lower.observers,
        }
    }

    pub(super) fn insert(
        &mut self,
        volume: Volume,
        projection: bool,
    ) -> Result<Key, crate::FsError> {
        self.insert_kind(volume, projection, None)
    }

    pub(super) fn insert_observer(&mut self, volume: Volume, observer: Arc<ObservationSignal>) -> Result<Key, crate::FsError> {
        self.insert_kind(volume, false, Some(observer))
    }

    fn insert_kind(&mut self, volume: Volume, projection: bool, observer: Option<Arc<ObservationSignal>>) -> Result<Key, crate::FsError> {
        let next = self.next.checked_add(1).ok_or(crate::WorkError::Overflow)?;
        let key = (volume, self.next);
        let node = Box::new(Node {
            key,
            projection,
            total: State {
                projections: usize::from(projection),
                editors: usize::from(!projection && observer.is_none()),
                observers: usize::from(observer.is_some()),
            },
            observer,
            height: 1,
            left: None,
            right: None,
        });
        self.root = Some(insert(self.root.take(), node));
        self.next = next;
        Ok(key)
    }

    pub(super) fn invalidate_observers(&self, volume: Volume) {
        fn invalidate(tree: &Tree, volume: Volume) {
            let Some(node) = tree else { return; };
            if node.key.0 >= volume { invalidate(&node.left, volume); }
            if node.key.0 == volume && let Some(observer) = &node.observer {
                observer.invalid.store(true, crate::sync::atomic::Ordering::Release);
            }
            if node.key.0 <= volume { invalidate(&node.right, volume); }
        }
        invalidate(&self.root, volume);
    }

    pub(super) fn next_observer(&self, volume: Volume, after: Option<Key>) -> Option<(Key, Arc<ObservationSignal>)> {
        fn next(tree: &Tree, volume: Volume, after: Option<Key>) -> Option<(Key, Arc<ObservationSignal>)> {
            let node = tree.as_ref()?;
            if node.key.0 < volume || after.is_some_and(|after| node.key <= after) { return next(&node.right, volume, after); }
            if let Some(found) = next(&node.left, volume, after) { return Some(found); }
            if node.key.0 == volume && let Some(observer) = &node.observer { return Some((node.key, Arc::clone(observer))); }
            if node.key.0 > volume { return None; }
            next(&node.right, volume, after)
        }
        next(&self.root, volume, after)
    }

    // Drop the detached node after the registry lock has been released.
    pub(super) fn remove(&mut self, key: Key) -> Option<Box<Node>> {
        let (root, removed) = remove(self.root.take(), key);
        self.root = root;
        removed
    }

    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.root.is_none()
    }
}

fn height(tree: &Tree) -> usize {
    tree.as_ref().map_or(0, |node| node.height)
}

fn totals(tree: &Tree) -> State {
    tree.as_ref().map_or_else(State::default, |node| node.total)
}

impl Node {
    fn refresh(&mut self) {
        let left = totals(&self.left);
        let right = totals(&self.right);
        // Each count is bounded by the number of live, separately allocated
        // nodes, so neither their sum nor the height can overflow usize.
        self.total = State {
            projections: left.projections + right.projections + usize::from(self.projection),
            editors: left.editors + right.editors + usize::from(!self.projection && self.observer.is_none()),
            observers: left.observers + right.observers + usize::from(self.observer.is_some()),
        };
        self.height = 1 + height(&self.left).max(height(&self.right));
    }
}

fn left(mut node: Box<Node>) -> Box<Node> {
    let Some(mut pivot) = node.right.take() else {
        return node;
    };
    node.right = pivot.left.take();
    node.refresh();
    pivot.left = Some(node);
    pivot.refresh();
    pivot
}

fn right(mut node: Box<Node>) -> Box<Node> {
    let Some(mut pivot) = node.left.take() else {
        return node;
    };
    node.left = pivot.right.take();
    node.refresh();
    pivot.right = Some(node);
    pivot.refresh();
    pivot
}

fn balance(mut node: Box<Node>) -> Box<Node> {
    node.refresh();
    if height(&node.left) > height(&node.right) + 1 {
        if node
            .left
            .as_ref()
            .is_some_and(|child| height(&child.right) > height(&child.left))
        {
            node.left = node.left.take().map(left);
        }
        return right(node);
    }
    if height(&node.right) > height(&node.left) + 1 {
        if node
            .right
            .as_ref()
            .is_some_and(|child| height(&child.left) > height(&child.right))
        {
            node.right = node.right.take().map(right);
        }
        return left(node);
    }
    node
}

fn insert(tree: Tree, inserted: Box<Node>) -> Box<Node> {
    let Some(mut node) = tree else {
        return inserted;
    };
    if inserted.key < node.key {
        node.left = Some(insert(node.left.take(), inserted));
    } else {
        node.right = Some(insert(node.right.take(), inserted));
    }
    balance(node)
}

fn detach_first(mut node: Box<Node>) -> (Tree, Box<Node>) {
    let Some(first) = node.left.take() else {
        return (node.right.take(), node);
    };
    let (remaining, first) = detach_first(first);
    node.left = remaining;
    (Some(balance(node)), first)
}

fn remove(tree: Tree, key: Key) -> (Tree, Option<Box<Node>>) {
    let Some(mut node) = tree else {
        return (None, None);
    };
    let removed = match key.cmp(&node.key) {
        std::cmp::Ordering::Less => {
            let (remaining, removed) = remove(node.left.take(), key);
            node.left = remaining;
            removed
        }
        std::cmp::Ordering::Greater => {
            let (remaining, removed) = remove(node.right.take(), key);
            node.right = remaining;
            removed
        }
        std::cmp::Ordering::Equal => {
            let Some(right) = node.right.take() else {
                return (node.left.take(), Some(node));
            };
            let (remaining, mut first) = detach_first(right);
            first.left = node.left.take();
            first.right = remaining;
            return (Some(balance(first)), Some(node));
        }
    };
    (Some(balance(node)), removed)
}

fn prefix(tree: &Tree, volume: Volume, inclusive: bool) -> State {
    let mut current = tree;
    let mut sum = State::default();
    while let Some(node) = current {
        if node.key.0 < volume || inclusive && node.key.0 == volume {
            let left = totals(&node.left);
            sum.projections += left.projections + usize::from(node.projection);
            sum.editors += left.editors + usize::from(!node.projection && node.observer.is_none());
            sum.observers += left.observers + usize::from(node.observer.is_some());
            current = &node.right;
        } else {
            current = &node.left;
        }
    }
    sum
}

#[cfg(all(test, not(acyclic_fs_loom)))]
mod tests;
