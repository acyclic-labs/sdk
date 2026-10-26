// Immutable segment trie with a persistent AVL sibling index. Every allocation
// is admitted before publication; fork shares one node, including all histories.
struct NamespaceReserve {
    nodes: Vec<Arc<NamespaceNode>>,
    branches: Vec<Arc<Branch>>,
    _allocation: Allocation,
}
struct NamespaceBuilder<'a> {
    allocate: &'a dyn Fn(usize) -> Result<Allocation, StreamError>,
    reserve: Option<&'a mut NamespaceReserve>,
}
impl<'a> NamespaceBuilder<'a> {
    fn new(allocate: &'a dyn Fn(usize) -> Result<Allocation, StreamError>) -> Self {
        Self {
            allocate,
            reserve: None,
        }
    }
    fn node(
        &mut self,
        data: PathState,
        children: Index,
    ) -> Result<Arc<NamespaceNode>, StreamError> {
        if let Some(pool) = &mut self.reserve {
            let mut node = pool.nodes.pop().ok_or(StreamError::Capacity)?;
            let slot = Arc::get_mut(&mut node).ok_or(StreamError::Unavailable)?;
            slot.data = data;
            slot.children = children;
            Ok(node)
        } else {
            let allocation = (self.allocate)(
                std::mem::size_of::<NamespaceNode>() + 2 * std::mem::size_of::<usize>(),
            )?;
            Ok(Arc::new(NamespaceNode {
                data,
                children,
                _allocation: allocation,
            }))
        }
    }
}
impl MemoryStream {
    #[cfg(not(target_arch = "wasm32"))]
    fn namespace_reserve(&self, path: &StreamPath) -> Result<NamespaceReserve, StreamError> {
        // Minimum AVL nodes at height h are F(h+2)-1. Prepay the maximum
        // possible sibling spine while other paths are inserted after acceptance.
        let (mut low, mut high, mut height) = (0usize, 1usize, 0usize);
        while high <= self.limits.paths {
            height += 1;
            let next = low.saturating_add(high).saturating_add(1);
            if next == usize::MAX {
                height += 1;
                break;
            }
            low = high;
            high = next;
        }
        let depth = path.as_str().split('/').count();
        let (mut nodes, mut allocation) = self.vector(depth)?;
        let (mut branches, claim) = self.vector(depth.saturating_mul(height))?;
        self.absorb(&mut allocation, claim)?;
        let claim = |bytes| self.allocate(bytes);
        let mut builder = NamespaceBuilder::new(&claim);
        let blank = || PathState {
            history: None,
            prefix: [None, None],
            tail: 0,
        };
        let dummy = builder.node(blank(), None)?;
        let name = SegmentText::new("", &mut builder)?;
        for _ in 0..depth {
            nodes.push(builder.node(blank(), None)?);
        }
        for _ in 0..depth.saturating_mul(height) {
            branches.push(branch(
                name.clone(),
                dummy.clone(),
                None,
                None,
                &mut builder,
            )?);
        }
        Ok(NamespaceReserve {
            nodes,
            branches,
            _allocation: allocation,
        })
    }
}

type Index = Option<Arc<Branch>>;
type Segment = Arc<SegmentText>;
struct SegmentText {
    text: String,
    _allocation: Allocation,
}
impl SegmentText {
    fn new(text: &str, build: &mut NamespaceBuilder<'_>) -> Result<Segment, StreamError> {
        let allocation = (build.allocate)(
            text.len() + std::mem::size_of::<Self>() + 2 * std::mem::size_of::<usize>(),
        )?;
        Ok(Arc::new(Self {
            text: text.to_owned(),
            _allocation: allocation,
        }))
    }
    fn len(&self) -> usize {
        self.text.len()
    }
}
struct NamespaceNode {
    data: PathState,
    children: Index,
    _allocation: Allocation,
}
struct Branch {
    name: Segment,
    node: Arc<NamespaceNode>,
    left: Index,
    right: Index,
    height: u8,
    count: usize,
    bytes: usize,
    max_suffix: usize,
    depth: usize,
    _allocation: Allocation,
}
#[derive(Clone, Default)]
struct Namespace {
    roots: Index,
}
impl NamespaceNode {
    fn count(&self) -> usize {
        1 + self.children.as_ref().map_or(0, |n| n.count)
    }
    fn suffix_bytes(&self) -> usize {
        self.children.as_ref().map_or(0, |n| n.bytes + n.count)
    }
    fn max_suffix(&self) -> usize {
        self.children.as_ref().map_or(0, |n| n.max_suffix + 1)
    }
    fn depth(&self) -> usize {
        1 + self.children.as_ref().map_or(0, |n| n.depth)
    }
}
fn height(index: &Index) -> u8 {
    index.as_ref().map_or(0, |n| n.height)
}
fn branch(
    name: Segment,
    node: Arc<NamespaceNode>,
    left: Index,
    right: Index,
    build: &mut NamespaceBuilder<'_>,
) -> Result<Arc<Branch>, StreamError> {
    let count =
        node.count() + left.as_ref().map_or(0, |n| n.count) + right.as_ref().map_or(0, |n| n.count);
    let bytes = node.count() * name.len()
        + node.suffix_bytes()
        + left.as_ref().map_or(0, |n| n.bytes)
        + right.as_ref().map_or(0, |n| n.bytes);
    let max_suffix = (name.len() + node.max_suffix())
        .max(left.as_ref().map_or(0, |n| n.max_suffix))
        .max(right.as_ref().map_or(0, |n| n.max_suffix));
    let depth = node
        .depth()
        .max(left.as_ref().map_or(0, |n| n.depth))
        .max(right.as_ref().map_or(0, |n| n.depth));
    let mut spare = if let Some(pool) = &mut build.reserve {
        Some(pool.branches.pop().ok_or(StreamError::Capacity)?)
    } else {
        None
    };
    let allocation = if let Some(slot) = &mut spare {
        Arc::get_mut(slot)
            .ok_or(StreamError::Unavailable)?
            ._allocation
            .take()
    } else {
        (build.allocate)(std::mem::size_of::<Branch>() + 2 * std::mem::size_of::<usize>())?
    };
    let value = Branch {
        name,
        node,
        height: 1 + height(&left).max(height(&right)),
        left,
        right,
        count,
        bytes,
        max_suffix,
        depth,
        _allocation: allocation,
    };
    if let Some(mut spare) = spare {
        *Arc::get_mut(&mut spare).ok_or(StreamError::Unavailable)? = value;
        Ok(spare)
    } else {
        Ok(Arc::new(value))
    }
}
fn balance(
    name: Segment,
    node: Arc<NamespaceNode>,
    left: Index,
    right: Index,
    build: &mut NamespaceBuilder<'_>,
) -> Result<Arc<Branch>, StreamError> {
    if height(&left) > height(&right) + 1 {
        let l = left.as_ref().ok_or(StreamError::Unavailable)?;
        if height(&l.left) >= height(&l.right) {
            let r = branch(name, node, l.right.clone(), right, build)?;
            return branch(
                l.name.clone(),
                l.node.clone(),
                l.left.clone(),
                Some(r),
                build,
            );
        }
        let middle = l.right.as_ref().ok_or(StreamError::Unavailable)?;
        let l2 = branch(
            l.name.clone(),
            l.node.clone(),
            l.left.clone(),
            middle.left.clone(),
            build,
        )?;
        let r2 = branch(name, node, middle.right.clone(), right, build)?;
        return branch(
            middle.name.clone(),
            middle.node.clone(),
            Some(l2),
            Some(r2),
            build,
        );
    }
    if height(&right) > height(&left) + 1 {
        let r = right.as_ref().ok_or(StreamError::Unavailable)?;
        if height(&r.right) >= height(&r.left) {
            let l = branch(name, node, left, r.left.clone(), build)?;
            return branch(
                r.name.clone(),
                r.node.clone(),
                Some(l),
                r.right.clone(),
                build,
            );
        }
        let middle = r.left.as_ref().ok_or(StreamError::Unavailable)?;
        let l2 = branch(name, node, left, middle.left.clone(), build)?;
        let r2 = branch(
            r.name.clone(),
            r.node.clone(),
            middle.right.clone(),
            r.right.clone(),
            build,
        )?;
        return branch(
            middle.name.clone(),
            middle.node.clone(),
            Some(l2),
            Some(r2),
            build,
        );
    }
    branch(name, node, left, right, build)
}
fn segment(mut index: &Index, name: &str) -> Option<Segment> {
    while let Some(n) = index {
        match name.cmp(n.name.text.as_str()) {
            std::cmp::Ordering::Less => index = &n.left,
            std::cmp::Ordering::Greater => index = &n.right,
            std::cmp::Ordering::Equal => return Some(n.name.clone()),
        }
    }
    None
}
fn lookup<'a>(mut index: &'a Index, name: &str) -> Option<&'a Arc<NamespaceNode>> {
    while let Some(n) = index {
        match name.cmp(n.name.text.as_str()) {
            std::cmp::Ordering::Less => index = &n.left,
            std::cmp::Ordering::Greater => index = &n.right,
            std::cmp::Ordering::Equal => return Some(&n.node),
        }
    }
    None
}
fn index_put(
    index: &Index,
    name: Segment,
    node: Option<Arc<NamespaceNode>>,
    build: &mut NamespaceBuilder<'_>,
) -> Result<Index, StreamError> {
    let Some(current) = index else {
        return node.map(|n| branch(name, n, None, None, build)).transpose();
    };
    let (mut left, mut right) = (current.left.clone(), current.right.clone());
    match name.text.cmp(&current.name.text) {
        std::cmp::Ordering::Less => left = index_put(&left, name, node, build)?,
        std::cmp::Ordering::Greater => right = index_put(&right, name, node, build)?,
        std::cmp::Ordering::Equal => {
            if let Some(node) = node {
                return branch(current.name.clone(), node, left, right, build).map(Some);
            }
            if left.is_none() {
                return Ok(right);
            }
            let Some(mut successor) = right.as_ref() else {
                return Ok(left);
            };
            while let Some(next) = successor.left.as_ref() {
                successor = next;
            }
            let name = successor.name.clone();
            let node = successor.node.clone();
            right = index_put(&right, name.clone(), None, build)?;
            return balance(name, node, left, right, build).map(Some);
        }
    }
    balance(
        current.name.clone(),
        current.node.clone(),
        left,
        right,
        build,
    )
    .map(Some)
}
impl Namespace {
    fn node<Q: std::borrow::Borrow<str> + ?Sized>(&self, path: &Q) -> Option<&Arc<NamespaceNode>> {
        let mut parts = path.borrow().split('/');
        let mut node = lookup(&self.roots, parts.next()?)?;
        for part in parts {
            node = lookup(&node.children, part)?;
        }
        Some(node)
    }
    fn get<Q: std::borrow::Borrow<str> + ?Sized>(&self, path: &Q) -> Option<&PathState> {
        Some(&self.node(path)?.data)
    }
    fn contains_key<Q: std::borrow::Borrow<str> + ?Sized>(&self, path: &Q) -> bool {
        self.node(path).is_some()
    }
    fn len(&self) -> usize {
        self.roots.as_ref().map_or(0, |n| n.count)
    }
    fn bytes(&self) -> usize {
        self.roots.as_ref().map_or(0, |n| n.bytes)
    }
    fn put(
        &mut self,
        path: &StreamPath,
        node: Arc<NamespaceNode>,
        build: &mut NamespaceBuilder<'_>,
    ) -> Result<(), StreamError> {
        fn put(
            index: &Index,
            mut parts: std::str::Split<'_, char>,
            node: Arc<NamespaceNode>,
            build: &mut NamespaceBuilder<'_>,
        ) -> Result<Index, StreamError> {
            let name = parts.next().ok_or(StreamError::InvalidArgument)?;
            let existing = lookup(index, name);
            let next = if parts.clone().next().is_none() {
                node
            } else {
                let parent = existing.ok_or(StreamError::Unavailable)?;
                let children = put(&parent.children, parts, node, build)?;
                build.node(parent.data.clone(), children)?
            };
            // Share existing segment text; charge a fresh segment with its edge.
            let name = segment(index, name).map_or_else(|| SegmentText::new(name, build), Ok)?;
            index_put(index, name, Some(next), build)
        }
        self.roots = put(&self.roots, path.as_str().split('/'), node, build)?;
        Ok(())
    }
    #[cfg(test)]
    fn insert(&mut self, path: &StreamPath, data: PathState) {
        self.put_data(path, data, &mut NamespaceBuilder::new(&|_| Ok(None)))
            .expect("validated snapshot namespace");
    }
    fn put_data(
        &mut self,
        path: &StreamPath,
        data: PathState,
        build: &mut NamespaceBuilder<'_>,
    ) -> Result<(), StreamError> {
        let children = self.node(path).and_then(|n| n.children.clone());
        let node = build.node(data, children)?;
        self.put(path, node, build)
    }
    fn excluded(
        node: &Arc<NamespaceNode>,
        excludes: &[crate::RelativePath],
        build: &mut NamespaceBuilder<'_>,
    ) -> Result<Arc<NamespaceNode>, StreamError> {
        fn remove(
            node: &Arc<NamespaceNode>,
            mut parts: std::str::Split<'_, char>,
            build: &mut NamespaceBuilder<'_>,
        ) -> Result<Arc<NamespaceNode>, StreamError> {
            let name = parts.next().ok_or(StreamError::InvalidArgument)?;
            let Some(child) = lookup(&node.children, name) else {
                return Ok(node.clone());
            };
            let child = if parts.clone().next().is_none() {
                None
            } else {
                Some(remove(child, parts, build)?)
            };
            let children = index_put(
                &node.children,
                segment(&node.children, name).ok_or(StreamError::Unavailable)?,
                child,
                build,
            )?;
            build.node(node.data.clone(), children)
        }
        let mut node = node.clone();
        for exclude in excludes {
            node = remove(&node, exclude.as_str().split('/'), build)?;
        }
        Ok(node)
    }
    fn entries(&self) -> Vec<(String, &PathState)> {
        fn walk<'a>(prefix: &str, index: &'a Index, out: &mut Vec<(String, &'a PathState)>) {
            if let Some(edge) = index {
                walk(prefix, &edge.left, out);
                let name = if prefix.is_empty() {
                    edge.name.text.to_string()
                } else {
                    format!("{prefix}/{}", edge.name.text)
                };
                out.push((name.clone(), &edge.node.data));
                walk(&name, &edge.node.children, out);
                walk(prefix, &edge.right, out);
            }
        }
        let mut out = Vec::with_capacity(self.len());
        walk("", &self.roots, &mut out);
        out.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
        out
    }
    #[cfg(any(test, feature = "local"))]
    fn values(&self) -> impl Iterator<Item = &PathState> {
        self.entries().into_iter().map(|(_, data)| data)
    }
}
impl<'a> IntoIterator for &'a Namespace {
    type Item = (String, &'a PathState);
    type IntoIter = std::vec::IntoIter<Self::Item>;
    fn into_iter(self) -> Self::IntoIter {
        self.entries().into_iter()
    }
}
// Exact absolute-path subscriptions are never shared with a forked node.
struct Signals {
    changed: watch::Sender<u64>,
    follows: Arc<crate::query_sync::SyncMutex<crate::follow_index::FollowIndex>>,
}
impl Signals {
    fn new(tail: u64) -> Self {
        Self {
            changed: watch::channel(tail).0,
            follows: Arc::new(crate::query_sync::SyncMutex::new(
                crate::follow_index::FollowIndex::default(),
            )),
        }
    }
}
