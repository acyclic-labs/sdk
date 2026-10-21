use super::{MemoryStoreEntry, MemoryStoreEntryKind, MemoryStorePage};
use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Probe {
    RootSeek,
    Read,
}

const NONE: usize = usize::MAX;
// An AVL tree containing addressable nodes has height < 2 * usize::BITS.
const HEIGHT: usize = 2 * usize::BITS as usize;

#[derive(Clone, Copy, PartialEq, Eq)]
struct Links {
    left: usize,
    right: usize,
    kind: MemoryStoreEntryKind,
}
#[derive(Clone, Copy)]
struct Change {
    generation: u64,
    links: Links,
}
struct Node {
    name: usize,
    born: u64,
    initial: Links,
    changes: [Change; 2],
    // Heights are needed only while updating the current tree.
    height: u8,
}
impl Node {
    fn links(&self, generation: u64) -> Links {
        let mut links = self.initial;
        for change in self.changes {
            if change.generation != 0 && change.generation <= generation {
                links = change.links;
            }
        }
        links
    }
}
struct Version {
    generation: u64,
    root: usize,
    count: usize,
}
struct DirectoryPath {
    source: Arc<str>,
    end: usize,
}
impl Borrow<str> for DirectoryPath {
    fn borrow(&self) -> &str {
        &self.source[..self.end]
    }
}
impl PartialEq for DirectoryPath {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}
impl Eq for DirectoryPath {}
impl PartialOrd for DirectoryPath {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for DirectoryPath {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let left: &str = self.borrow();
        let right: &str = other.borrow();
        left.cmp(right)
    }
}

/// Partially persistent AVL forest. Two modification slots per node give
/// constant amortized node copies per insertion: copying a full node releases
/// two occupied slots and redirects at most one current parent. Roots and names
/// are retained once per changed directory entry, never once per file revision.
/// Historical queries select a root before seeking, so later publications do
/// not introduce invisible entries into the iterator.
#[derive(Default)]
pub(crate) struct DirectoryIndex {
    directories: BTreeMap<DirectoryPath, usize>,
    versions: Vec<Vec<Version>>,
    nodes: Vec<Node>,
    names: Vec<Box<str>>,
}
impl DirectoryIndex {
    pub(crate) fn publish(&mut self, path: &str, generation: u64) {
        if self.versions.is_empty() {
            self.versions.push(Vec::new());
        }
        let mut directory = 0;
        let mut start = 0;
        let mut source = None;
        for end in path
            .match_indices('/')
            .map(|(offset, _)| offset)
            .chain(std::iter::once(path.len()))
        {
            let segment = &path[start..end];
            let last = end == path.len();
            if !segment.is_empty() && !(directory == 0 && segment == ".system") {
                self.insert_entry(
                    directory,
                    segment,
                    if last {
                        MemoryStoreEntryKind::File
                    } else {
                        MemoryStoreEntryKind::Directory
                    },
                    generation,
                );
            }
            if last {
                break;
            }
            let prefix = &path[..end];
            directory = match self.directories.get(prefix) {
                Some(id) => *id,
                None => {
                    let id = self.versions.len();
                    self.versions.push(Vec::new());
                    let source = source.get_or_insert_with(|| {
                        Arc::<str>::from(&path[..path.rfind('/').expect("directory separator")])
                    });
                    self.directories.insert(
                        DirectoryPath {
                            source: Arc::clone(source),
                            end,
                        },
                        id,
                    );
                    id
                }
            };
            start = end + 1;
        }
    }

    fn insert_entry(
        &mut self,
        directory: usize,
        name: &str,
        kind: MemoryStoreEntryKind,
        generation: u64,
    ) {
        let (root, count) = self.versions[directory]
            .last()
            .map_or((NONE, 0), |version| {
                debug_assert!(generation >= version.generation);
                (version.root, version.count)
            });
        let (root, changed, added) = self.insert(root, name, kind, generation);
        if changed {
            let version = Version {
                generation,
                root,
                count: count + usize::from(added),
            };
            let versions = &mut self.versions[directory];
            if versions
                .last()
                .is_some_and(|prior| prior.generation == generation)
            {
                *versions.last_mut().expect("existing directory version") = version;
            } else {
                versions.push(version);
            }
        }
    }

    fn insert(
        &mut self,
        root: usize,
        name: &str,
        kind: MemoryStoreEntryKind,
        generation: u64,
    ) -> (usize, bool, bool) {
        if root == NONE {
            let name_id = self.names.len();
            self.names.push(name.into());
            let links = Links {
                left: NONE,
                right: NONE,
                kind,
            };
            let id = self.nodes.len();
            self.nodes.push(Node {
                name: name_id,
                born: generation,
                initial: links,
                changes: [Change {
                    generation: 0,
                    links,
                }; 2],
                height: 1,
            });
            return (id, true, true);
        }
        let mut links = self.nodes[root].links(u64::MAX);
        let (changed, added) = match name.cmp(&self.names[self.nodes[root].name]) {
            std::cmp::Ordering::Equal => {
                if links.kind == MemoryStoreEntryKind::File
                    || kind == MemoryStoreEntryKind::Directory
                {
                    return (root, false, false);
                }
                links.kind = kind;
                (true, false)
            }
            std::cmp::Ordering::Less => {
                let (child, changed, added) = self.insert(links.left, name, kind, generation);
                links.left = child;
                (changed, added)
            }
            std::cmp::Ordering::Greater => {
                let (child, changed, added) = self.insert(links.right, name, kind, generation);
                links.right = child;
                (changed, added)
            }
        };
        if !changed {
            return (root, false, false);
        }
        let root = self.change(root, generation, links);
        (self.balance(root, generation), true, added)
    }

    fn change(&mut self, root: usize, generation: u64, links: Links) -> usize {
        let node = &mut self.nodes[root];
        if node.links(u64::MAX) == links {
            return root;
        }
        if node.born == generation {
            node.initial = links;
            return root;
        }
        if let Some(change) = node
            .changes
            .iter_mut()
            .find(|change| change.generation == generation || change.generation == 0)
        {
            *change = Change { generation, links };
            return root;
        }
        let node = Node {
            name: node.name,
            born: generation,
            initial: links,
            changes: [Change {
                generation: 0,
                links,
            }; 2],
            height: node.height,
        };
        let id = self.nodes.len();
        self.nodes.push(node);
        id
    }

    fn height(&self, root: usize) -> u8 {
        if root == NONE {
            0
        } else {
            self.nodes[root].height
        }
    }
    fn update_height(&mut self, root: usize) {
        let links = self.nodes[root].links(u64::MAX);
        self.nodes[root].height = 1 + self.height(links.left).max(self.height(links.right));
    }
    fn rotate_left(&mut self, root: usize, generation: u64) -> usize {
        let mut left = self.nodes[root].links(u64::MAX);
        let right_root = left.right;
        let mut right = self.nodes[right_root].links(u64::MAX);
        left.right = right.left;
        let root = self.change(root, generation, left);
        self.update_height(root);
        right.left = root;
        let right_root = self.change(right_root, generation, right);
        self.update_height(right_root);
        right_root
    }
    fn rotate_right(&mut self, root: usize, generation: u64) -> usize {
        let mut right = self.nodes[root].links(u64::MAX);
        let left_root = right.left;
        let mut left = self.nodes[left_root].links(u64::MAX);
        right.left = left.right;
        let root = self.change(root, generation, right);
        self.update_height(root);
        left.right = root;
        let left_root = self.change(left_root, generation, left);
        self.update_height(left_root);
        left_root
    }
    fn balance(&mut self, mut root: usize, generation: u64) -> usize {
        self.update_height(root);
        let mut links = self.nodes[root].links(u64::MAX);
        let difference = i16::from(self.height(links.left)) - i16::from(self.height(links.right));
        if difference > 1 {
            let left = self.nodes[links.left].links(u64::MAX);
            if self.height(left.left) < self.height(left.right) {
                links.left = self.rotate_left(links.left, generation);
                root = self.change(root, generation, links);
            }
            return self.rotate_right(root, generation);
        }
        if difference < -1 {
            let right = self.nodes[links.right].links(u64::MAX);
            if self.height(right.right) < self.height(right.left) {
                links.right = self.rotate_right(links.right, generation);
                root = self.change(root, generation, links);
            }
            return self.rotate_left(root, generation);
        }
        root
    }

    #[cfg(all(test, not(target_arch = "wasm32")))]
    pub(crate) fn assert_linear_storage(&self) {
        assert!(
            self.nodes.len() <= 8 * self.names.len(),
            "persistent node storage must be linear in distinct entries"
        );
        let roots: usize = self.versions.iter().map(Vec::len).sum();
        assert!(
            roots <= 2 * self.names.len(),
            "rewrites must not accumulate directory roots"
        );
    }
    #[cfg(all(test, not(target_arch = "wasm32")))]
    pub(crate) fn assert_invariants(&self) {
        fn check(
            index: &DirectoryIndex,
            root: usize,
            generation: u64,
            lower: Option<&str>,
            upper: Option<&str>,
        ) -> (u8, usize) {
            if root == NONE {
                return (0, 0);
            }
            let node = &index.nodes[root];
            assert!(node.born <= generation);
            let name = index.names[node.name].as_ref();
            assert!(lower.is_none_or(|lower| lower < name));
            assert!(upper.is_none_or(|upper| name < upper));
            let links = node.links(generation);
            let (left, left_count) = check(index, links.left, generation, lower, Some(name));
            let (right, right_count) = check(index, links.right, generation, Some(name), upper);
            assert!(
                left.abs_diff(right) <= 1,
                "historical root must remain AVL balanced"
            );
            let height = 1 + left.max(right);
            assert!(usize::from(height) < HEIGHT);
            (height, 1 + left_count + right_count)
        }
        self.assert_linear_storage();
        for versions in &self.versions {
            assert!(
                versions
                    .windows(2)
                    .all(|pair| pair[0].generation < pair[1].generation)
            );
            for version in versions {
                let (_, count) = check(self, version.root, version.generation, None, None);
                assert_eq!(count, version.count);
            }
        }
    }

    pub(crate) fn page(
        &self,
        path: &str,
        generation: u64,
        after: Option<&str>,
        maximum: usize,
        mut work: impl FnMut(Probe),
    ) -> MemoryStorePage {
        let empty = || MemoryStorePage {
            generation,
            entries: Vec::new(),
            has_more: false,
        };
        let directory = if path.is_empty() {
            0
        } else if let Some(id) = self.directories.get(path) {
            *id
        } else {
            return empty();
        };
        let Some(versions) = self.versions.get(directory) else {
            return empty();
        };
        let end = versions.partition_point(|version| {
            work(Probe::Read);
            version.generation <= generation
        });
        let Some(version) = end.checked_sub(1).map(|index| &versions[index]) else {
            return empty();
        };
        let mut iterator = Cursor {
            index: self,
            generation,
            stack: [NONE; HEIGHT],
            depth: 0,
        };
        iterator.seek_root(version.root, after, &mut work);
        // Stack entries are distinct remaining children. Starting with this lower
        // bound avoids allocating for children discarded by a cursor. Grow only
        // as entries are emitted; historical subtree counts would update every
        // ancestor on publication and lose the index's linear storage bound.
        let capacity = maximum.min(if after.is_none() {
            version.count
        } else {
            iterator.depth
        });
        let mut entries = Vec::with_capacity(capacity);
        while entries.len() < maximum {
            let Some((name, kind)) = iterator.next(&mut work) else {
                break;
            };
            if entries.len() == entries.capacity() {
                entries.reserve_exact(entries.capacity().max(1).min(maximum - entries.len()));
            }
            entries.push(MemoryStoreEntry {
                name: name.to_owned(),
                kind,
            });
        }
        let has_more = iterator.depth != 0;
        MemoryStorePage {
            generation,
            entries,
            has_more,
        }
    }

    #[cfg(all(test, not(target_arch = "wasm32")))]
    pub(super) fn counted_page(
        &self,
        path: &str,
        generation: u64,
        after: Option<&str>,
        maximum: usize,
        mut work: impl FnMut(Probe),
    ) -> MemoryStorePage {
        let empty = || MemoryStorePage {
            generation,
            entries: Vec::new(),
            has_more: false,
        };
        let directory = if path.is_empty() {
            0
        } else if let Some(id) = self.directories.get(path) {
            *id
        } else {
            return empty();
        };
        let Some(versions) = self.versions.get(directory) else {
            return empty();
        };
        let end = versions.partition_point(|version| {
            work(Probe::Read);
            version.generation <= generation
        });
        let Some(version) = end.checked_sub(1).map(|index| &versions[index]) else {
            return empty();
        };
        let mut iterator = Cursor {
            index: self,
            generation,
            stack: [NONE; HEIGHT],
            depth: 0,
        };
        iterator.seek_root(version.root, after, &mut work);
        let (count, has_more) = if after.is_none() {
            let count = maximum.min(version.count);
            (count, version.count > count)
        } else {
            let mut count_cursor = iterator.clone();
            let mut count = 0;
            while count < maximum && count_cursor.next(&mut work).is_some() {
                count += 1;
            }
            (count, count_cursor.next(&mut work).is_some())
        };
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            let (name, kind) = iterator.next(&mut work).expect("counted directory entry");
            entries.push(MemoryStoreEntry {
                name: name.to_owned(),
                kind,
            });
        }
        MemoryStorePage {
            generation,
            entries,
            has_more,
        }
    }
}

#[cfg_attr(all(test, not(target_arch = "wasm32")), derive(Clone))]
struct Cursor<'a> {
    index: &'a DirectoryIndex,
    generation: u64,
    stack: [usize; HEIGHT],
    depth: usize,
}
impl<'a> Cursor<'a> {
    fn push(&mut self, root: usize) {
        self.stack[self.depth] = root;
        self.depth += 1;
    }
    fn seek_root(&mut self, root: usize, after: Option<&str>, work: &mut impl FnMut(Probe)) {
        work(Probe::RootSeek);
        self.descend(root, after, work);
    }
    fn descend(&mut self, mut root: usize, after: Option<&str>, work: &mut impl FnMut(Probe)) {
        while root != NONE {
            work(Probe::Read);
            let node = &self.index.nodes[root];
            let links = node.links(self.generation);
            if after.is_none_or(|after| self.index.names[node.name].as_ref() > after) {
                self.push(root);
                root = links.left;
            } else {
                root = links.right;
            }
        }
    }
    fn next(&mut self, work: &mut impl FnMut(Probe)) -> Option<(&'a str, MemoryStoreEntryKind)> {
        if self.depth == 0 {
            return None;
        }
        self.depth -= 1;
        let node = &self.index.nodes[self.stack[self.depth]];
        work(Probe::Read);
        let links = node.links(self.generation);
        self.descend(links.right, None, work);
        Some((&self.index.names[node.name], links.kind))
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[test]
fn repeated_root_seek_negative_control() {
    let mut index = DirectoryIndex::default();
    for child in 0..4096 {
        index.publish(&format!("files/{child:08}"), 1);
    }
    let root = index.versions[*index.directories.get("files").unwrap()][0].root;
    let mut cursor = Cursor {
        index: &index,
        generation: 1,
        stack: [NONE; HEIGHT],
        depth: 0,
    };
    let mut seeks = 0;
    let mut work = |probe| {
        if probe == Probe::RootSeek {
            seeks += 1;
        }
    };
    cursor.seek_root(root, Some("00002047"), &mut work);
    // Repeat the real root seek, discarding its previous stack, as a broken page
    // selector would. It must fail the same one-seek contract as production.
    cursor.depth = 0;
    cursor.seek_root(root, Some("00002047"), &mut work);
    assert_eq!(seeks, 2);
    assert!(std::panic::catch_unwind(|| super::tests::assert_one_root_seek(seeks)).is_err());
}
