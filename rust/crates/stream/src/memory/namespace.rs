// Immutable segment trie with a persistent AVL sibling index. Every allocation
// is admitted before publication; fork shares one node, including all histories.
struct NamespaceReserve {
    nodes: Vec<Arc<NamespaceNode>>,
    branches: Vec<Arc<Branch>>,
    frames: Vec<NamespaceFrame>,
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
    fn with_frames<T>(
        &mut self,
        depth: usize,
        operation: impl FnOnce(&mut Self, &mut Vec<NamespaceFrame>) -> Result<T, StreamError>,
    ) -> Result<T, StreamError> {
        let (mut frames, _allocation) = if let Some(pool) = &mut self.reserve {
            if pool.frames.capacity() < depth {
                return Err(StreamError::Capacity);
            }
            (std::mem::take(&mut pool.frames), None)
        } else {
            let allocation =
                (self.allocate)(depth.saturating_mul(std::mem::size_of::<NamespaceFrame>()))?;
            let mut frames = Vec::new();
            frames
                .try_reserve_exact(depth)
                .map_err(|_| StreamError::Capacity)?;
            (frames, allocation)
        };
        let result = operation(self, &mut frames);
        frames.clear();
        if let Some(pool) = &mut self.reserve {
            pool.frames = frames;
        }
        result
    }
    fn node(
        &mut self,
        data: PathState,
        children: Index,
        origin: Option<ForkMarker>,
    ) -> Result<Arc<NamespaceNode>, StreamError> {
        if let Some(pool) = &mut self.reserve {
            let mut node = pool.nodes.pop().ok_or(StreamError::Capacity)?;
            let slot = Arc::get_mut(&mut node).ok_or(StreamError::Unavailable)?;
            slot.data = data;
            slot.children = children;
            slot.origin = origin;
            Ok(node)
        } else {
            let allocation = (self.allocate)(namespace_owned_bytes::<NamespaceNode>())?;
            Ok(Arc::new(NamespaceNode {
                data,
                children,
                origin,
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
        while self.limits.paths.is_none_or(|limit| high <= limit) {
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
        let (frames, mut allocation) = self.vector(depth)?;
        let (mut nodes, claim) = self.vector(depth)?;
        self.absorb(&mut allocation, claim)?;
        let (mut branches, claim) = self.vector(depth.saturating_mul(height))?;
        self.absorb(&mut allocation, claim)?;
        let claim = |bytes| self.allocate(bytes);
        let mut builder = NamespaceBuilder::new(&claim);
        let blank = || PathState {
            history: None,
            ranges: HistoryRanges::default(),
            prefix: [None, None],
            tail: 0,
        };
        let dummy = builder.node(blank(), None, None)?;
        let name = SegmentText::new("", &mut builder)?;
        for _ in 0..depth {
            nodes.push(builder.node(blank(), None, None)?);
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
            frames,
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
    origin: Option<ForkMarker>,
    _allocation: Allocation,
}
// Each owner prepays the geometric work-buffer capacity for its outgoing
// edges and retained funding. Charges stay in the buffer until it is freed,
// including after the original node allocation has been released.
fn namespace_owned_bytes<T>() -> usize {
    std::mem::size_of::<T>()
        + 2 * std::mem::size_of::<usize>()
        + 8 * std::mem::size_of::<NamespaceRetirement>()
}
enum NamespaceRetirement {
    Node(Arc<NamespaceNode>),
    Branch(Arc<Branch>),
    Funding { _allocation: Allocation },
}
fn retire_namespace(initial: [Option<NamespaceRetirement>; 3], allocation: &mut Allocation) {
    fn retire(item: NamespaceRetirement, work: &mut Vec<NamespaceRetirement>) -> Allocation {
        match item {
            NamespaceRetirement::Node(node) => {
                if let Some(mut node) = Arc::into_inner(node) {
                    let allocation = node._allocation.take();
                    node.retire_edges(work);
                    return allocation;
                }
            }
            NamespaceRetirement::Branch(edge) => {
                if let Some(mut edge) = Arc::into_inner(edge) {
                    let allocation = edge._allocation.take();
                    edge.retire_edges(work);
                    return allocation;
                }
            }
            NamespaceRetirement::Funding { _allocation } => return _allocation,
        }
        None
    }
    // Releasing only shared edges needs no scratch allocation. into_inner,
    // rather than a uniqueness probe, also handles racing last owners.
    let mut work = Vec::new();
    for item in initial.into_iter().flatten() {
        let funding = retire(item, &mut work);
        if funding.is_some() {
            work.push(NamespaceRetirement::Funding {
                _allocation: funding,
            });
        }
    }
    if work.is_empty() {
        return;
    }
    work.push(NamespaceRetirement::Funding {
        _allocation: allocation.take(),
    });
    let mut cursor = 0;
    while cursor < work.len() {
        let item = std::mem::replace(
            &mut work[cursor],
            NamespaceRetirement::Funding { _allocation: None },
        );
        work[cursor] = NamespaceRetirement::Funding {
            _allocation: retire(item, &mut work),
        };
        cursor += 1;
    }
}
impl NamespaceNode {
    fn retire_edges(&mut self, work: &mut Vec<NamespaceRetirement>) {
        if let Some(children) = self.children.take() {
            work.push(NamespaceRetirement::Branch(children));
        }
        if let Some(origin) = self.origin.take() {
            work.push(NamespaceRetirement::Node(origin.snapshot));
        }
    }
}
impl Drop for NamespaceNode {
    fn drop(&mut self) {
        if self.children.is_none() && self.origin.is_none() {
            return;
        }
        let initial = [
            self.children.take().map(NamespaceRetirement::Branch),
            self.origin
                .take()
                .map(|origin| NamespaceRetirement::Node(origin.snapshot)),
            None,
        ];
        retire_namespace(initial, &mut self._allocation);
    }
}
#[derive(Clone)]
struct ForkMarker {
    source: StreamPath,
    snapshot: Arc<NamespaceNode>,
    forked_at: u64,
    commit_id: CommitId,
    // Existing canonical decision order distinguishes a new nested graft from
    // markers shared inside its source. It is not a separate history or index.
    decision: u64,
}
struct Branch {
    name: Segment,
    node: Option<Arc<NamespaceNode>>,
    left: Index,
    right: Index,
    height: u8,
    count: usize,
    bytes: usize,
    depth: usize,
    _allocation: Allocation,
}
impl Branch {
    fn node(&self) -> &Arc<NamespaceNode> {
        self.node.as_ref().expect("published namespace edge")
    }
    fn retire_edges(&mut self, work: &mut Vec<NamespaceRetirement>) {
        if let Some(node) = self.node.take() {
            work.push(NamespaceRetirement::Node(node));
        }
        if let Some(left) = self.left.take() {
            work.push(NamespaceRetirement::Branch(left));
        }
        if let Some(right) = self.right.take() {
            work.push(NamespaceRetirement::Branch(right));
        }
    }
}
impl Drop for Branch {
    fn drop(&mut self) {
        if self.node.is_none() && self.left.is_none() && self.right.is_none() {
            return;
        }
        let initial = [
            self.node.take().map(NamespaceRetirement::Node),
            self.left.take().map(NamespaceRetirement::Branch),
            self.right.take().map(NamespaceRetirement::Branch),
        ];
        retire_namespace(initial, &mut self._allocation);
    }
}
struct NamespaceFrame {
    index: Index,
    name: Segment,
    parent: Option<Arc<NamespaceNode>>,
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
        (build.allocate)(namespace_owned_bytes::<Branch>())?
    };
    let value = Branch {
        name,
        node: Some(node),
        height: 1 + height(&left).max(height(&right)),
        left,
        right,
        count,
        bytes,
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
                l.node().clone(),
                l.left.clone(),
                Some(r),
                build,
            );
        }
        let middle = l.right.as_ref().ok_or(StreamError::Unavailable)?;
        let l2 = branch(
            l.name.clone(),
            l.node().clone(),
            l.left.clone(),
            middle.left.clone(),
            build,
        )?;
        let r2 = branch(name, node, middle.right.clone(), right, build)?;
        return branch(
            middle.name.clone(),
            middle.node().clone(),
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
                r.node().clone(),
                Some(l),
                r.right.clone(),
                build,
            );
        }
        let middle = r.left.as_ref().ok_or(StreamError::Unavailable)?;
        let l2 = branch(name, node, left, middle.left.clone(), build)?;
        let r2 = branch(
            r.name.clone(),
            r.node().clone(),
            middle.right.clone(),
            r.right.clone(),
            build,
        )?;
        return branch(
            middle.name.clone(),
            middle.node().clone(),
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
            std::cmp::Ordering::Equal => return Some(n.node()),
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
            let node = successor.node().clone();
            right = index_put(&right, name.clone(), None, build)?;
            return balance(name, node, left, right, build).map(Some);
        }
    }
    balance(
        current.name.clone(),
        current.node().clone(),
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
        build.with_frames(path.as_str().split('/').count(), |build, frames| {
            let mut index = &self.roots;
            let mut parts = path.as_str().split('/').peekable();
            while let Some(text) = parts.next() {
                let name =
                    segment(index, text).map_or_else(|| SegmentText::new(text, build), Ok)?;
                let existing = lookup(index, text);
                let parent = if parts.peek().is_some() {
                    Some(existing.ok_or(StreamError::Unavailable)?.clone())
                } else {
                    None
                };
                frames.push(NamespaceFrame {
                    index: index.clone(),
                    name,
                    parent,
                });
                if parts.peek().is_some() {
                    index = &existing.ok_or(StreamError::Unavailable)?.children;
                }
            }
            let mut next = node;
            let mut root = None;
            while let Some(frame) = frames.pop() {
                root = index_put(&frame.index, frame.name, Some(next), build)?;
                if let Some(frame) = frames.last() {
                    let parent = frame.parent.as_ref().ok_or(StreamError::Unavailable)?;
                    next = build.node(parent.data.clone(), root.take(), parent.origin.clone())?;
                } else {
                    break;
                }
            }
            self.roots = root;
            Ok(())
        })
    }
    #[cfg(test)]
    fn insert(&mut self, path: &StreamPath, data: PathState) {
        self.put_data(path, data, None, &mut NamespaceBuilder::new(&|_| Ok(None)))
            .expect("validated snapshot namespace");
    }
    fn put_data(
        &mut self,
        path: &StreamPath,
        data: PathState,
        origin: Option<ForkMarker>,
        build: &mut NamespaceBuilder<'_>,
    ) -> Result<(), StreamError> {
        let existing = self.node(path);
        let children = existing.and_then(|n| n.children.clone());
        let origin = origin.or_else(|| existing.and_then(|n| n.origin.clone()));
        let node = build.node(data, children, origin)?;
        self.put(path, node, build)
    }
    fn excluded(
        node: &Arc<NamespaceNode>,
        excludes: &[crate::RelativePath],
        build: &mut NamespaceBuilder<'_>,
    ) -> Result<Arc<NamespaceNode>, StreamError> {
        let mut result = node.clone();
        for exclude in excludes {
            result = build.with_frames(exclude.as_str().split('/').count(), |build, frames| {
                let mut cursor = &result;
                for text in exclude.as_str().split('/') {
                    let Some(child) = lookup(&cursor.children, text) else {
                        return Ok(result.clone());
                    };
                    frames.push(NamespaceFrame {
                        index: cursor.children.clone(),
                        name: segment(&cursor.children, text).ok_or(StreamError::Unavailable)?,
                        parent: Some(cursor.clone()),
                    });
                    cursor = child;
                }
                let mut child = None;
                while let Some(frame) = frames.pop() {
                    let children = index_put(&frame.index, frame.name, child, build)?;
                    let parent = frame.parent.ok_or(StreamError::Unavailable)?;
                    child =
                        Some(build.node(parent.data.clone(), children, parent.origin.clone())?);
                }
                child.ok_or(StreamError::InvalidArgument)
            })?;
        }
        Ok(result)
    }
    fn entries(&self) -> Vec<(String, &PathState)> {
        let mut out = Vec::with_capacity(self.len());
        let mut pending = vec![(String::new(), &self.roots)];
        while let Some((prefix, index)) = pending.pop() {
            let Some(edge) = index else { continue };
            let name = if prefix.is_empty() {
                edge.name.text.to_string()
            } else {
                format!("{prefix}/{}", edge.name.text)
            };
            out.push((name.clone(), &edge.node().data));
            pending.push((prefix.clone(), &edge.right));
            pending.push((name, &edge.node().children));
            pending.push((prefix, &edge.left));
        }
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
#[derive(Clone)]
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
