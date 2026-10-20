#[cfg(any(test, feature = "fuzz-support"))]
use std::collections::HashSet;
use std::{borrow::Cow, fmt, ops::Range, sync::Arc};

/// Immutable text snapshots with shared backing chunks.
#[derive(Default)]
pub struct Text {
    root: Option<Arc<Node>>,
    range: Range<usize>,
}

struct Node(Kind);
enum Kind {
    Chunk { prefix: Text, data: String },
    Concat { left: Text, right: Text },
}

impl Text {
    /// The visible UTF-8 byte length.
    pub fn len(&self) -> usize {
        self.range.len()
    }

    /// Whether the text is empty.
    pub fn is_empty(&self) -> bool {
        self.range.is_empty()
    }

    /// Borrow contiguous text, or join shared chunks when a contiguous value is required.
    pub fn as_str(&self) -> Cow<'_, str> {
        let mut chunks = self.reverse_chunks();
        let Some(first) = chunks.next() else {
            return Cow::Borrowed("");
        };
        let Some(second) = chunks.next() else {
            return Cow::Borrowed(first);
        };
        let mut bytes = vec![0; self.len()];
        copy_chunks([first, second].into_iter().chain(chunks), &mut bytes);
        Cow::Owned(match String::from_utf8(bytes) {
            Ok(text) => text,
            Err(_) => unreachable!("text chunks are UTF-8"),
        })
    }

    // Write chunks backwards into their final positions, without flattening
    // any intermediate Text or collecting a directory of borrowed pieces.
    pub(crate) fn copy_to(&self, destination: &mut [u8]) {
        assert_eq!(destination.len(), self.len());
        copy_chunks(self.reverse_chunks(), destination);
    }

    /// Append a string without changing existing snapshots.
    pub fn push_str(&mut self, text: &str) {
        if text.is_empty() {
            return;
        }
        #[cfg(test)]
        crate::work::copy(text.len());
        let mut capacity = text.len();
        if let Some(end) = self.range.end.checked_add(text.len())
            && let Some(Node(Kind::Chunk { prefix, data })) =
                self.root.as_mut().and_then(Arc::get_mut)
            && self.range.end >= prefix.len()
        {
            data.truncate(self.range.end - prefix.len());
            if data.capacity() - data.len() >= text.len() {
                data.push_str(text);
                self.range.end = end;
                return;
            }
            // Seal a full backing chunk instead of reallocating and copying
            // all its prior bytes. Geometric tail capacity bounds both the
            // number of chunks and their unused space during unique appends.
            capacity = capacity.max(data.capacity().saturating_mul(2));
        }
        let mut data = String::with_capacity(capacity);
        data.push_str(text);
        self.append_owned(data);
    }

    fn append_owned(&mut self, data: String) {
        if data.is_empty() {
            return;
        }
        assert!(
            data.len() <= usize::MAX - self.len(),
            "text length overflow"
        );
        let length = self.len() + data.len();
        let prefix = std::mem::take(self);
        #[cfg(test)]
        crate::work::scan(1);
        self.root = Some(Arc::new(Node(Kind::Chunk { prefix, data })));
        self.range = 0..length;
    }

    fn append_shared(&mut self, right: Text) {
        if right.is_empty() {
            return;
        }
        if self.is_empty() {
            *self = right;
            return;
        }
        assert!(
            right.len() <= usize::MAX - self.len(),
            "text length overflow"
        );
        let length = self.len() + right.len();
        let left = std::mem::take(self);
        #[cfg(test)]
        crate::work::scan(1);
        self.root = Some(Arc::new(Node(Kind::Concat { left, right })));
        self.range = 0..length;
    }

    /// Remove all text from this value, preserving other snapshots.
    pub fn clear(&mut self) {
        *self = Self::default();
    }

    /// Shorten at a UTF-8 character boundary, as with `String::truncate`.
    ///
    /// # Panics
    /// Panics if `length` is inside a UTF-8 code point.
    pub fn truncate(&mut self, length: usize) {
        if length >= self.len() {
            return;
        }
        if length == 0 {
            self.clear();
            return;
        }
        assert!(
            self.boundary(length),
            "text truncation must be on a UTF-8 boundary"
        );
        self.range.end = self.range.start + length;
    }

    fn boundary(&self, offset: usize) -> bool {
        if offset == 0 || offset == self.len() {
            return true;
        }
        if offset > self.len() {
            return false;
        }
        let mut position = self.range.start + offset;
        let mut current = self.root.as_deref();
        while let Some(Node(kind)) = current {
            #[cfg(test)]
            crate::work::scan(1);
            let prefix = match kind {
                Kind::Chunk { prefix, data } => {
                    if position >= prefix.len() {
                        return data.is_char_boundary(position - prefix.len());
                    }
                    prefix
                }
                Kind::Concat { left, right } => {
                    if position == left.len() {
                        return true;
                    }
                    if position > left.len() {
                        position = right.range.start + position - left.len();
                        current = right.root.as_deref();
                        continue;
                    }
                    left
                }
            };
            position += prefix.range.start;
            current = prefix.root.as_deref();
        }
        false
    }

    /// A shared substring at UTF-8 boundaries.
    pub fn get(&self, range: Range<usize>) -> Option<Self> {
        if range.start > range.end || !self.boundary(range.start) || !self.boundary(range.end) {
            return None;
        }
        if range.is_empty() {
            return Some(Self::default());
        }
        Some(Self {
            root: self.root.clone(),
            range: self.range.start + range.start..self.range.start + range.end,
        })
    }

    // The caller retains a contiguous source with the same UTF-8 boundaries.
    // CR/LF-to-space code normalization preserves those boundaries and offsets.
    pub(crate) fn source_range(&self, range: Range<usize>, source: &str) -> Option<Self> {
        if self.len() != source.len() || source.get(range.clone()).is_none() {
            return None;
        }
        if range.is_empty() {
            return Some(Self::default());
        }
        Some(Self {
            root: self.root.clone(),
            range: self.range.start + range.start..self.range.start + range.end,
        })
    }

    /// Whether any of the given characters occurs.
    pub fn contains<const N: usize>(&self, characters: [char; N]) -> bool {
        self.reverse_chunks().any(|chunk| {
            chunk.chars().any(|character| {
                #[cfg(test)]
                crate::work::scan(character.len_utf8());
                characters.contains(&character)
            })
        })
    }

    fn reverse_chunks(&self) -> Chunks<'_> {
        Chunks {
            current: self.root.as_deref().map(|node| (node, self.range.clone())),
            pending: Vec::new(),
        }
    }

    /// Count backing payload and metadata once per node address in `seen`.
    #[cfg(any(test, feature = "fuzz-support"))]
    pub fn retained(&self, seen: &mut HashSet<usize>) -> (usize, usize) {
        let mut pending: Vec<_> = self.root.iter().collect();
        let (mut payload, mut metadata) = (0, 0);
        while let Some(node) = pending.pop() {
            if !seen.insert(Arc::as_ptr(node) as usize) {
                continue;
            }
            metadata += std::mem::size_of::<Node>() + 2 * std::mem::size_of::<usize>();
            match &node.0 {
                Kind::Chunk { prefix, data } => {
                    payload += data.capacity();
                    pending.extend(prefix.root.iter());
                }
                Kind::Concat { left, right } => {
                    pending.extend(left.root.iter());
                    pending.extend(right.root.iter());
                }
            }
        }
        (payload, metadata)
    }

    #[cfg(test)]
    pub(crate) fn allocation(&self) -> Option<*const u8> {
        self.reverse_chunks().next().map(str::as_ptr)
    }
}

fn copy_chunks<'a>(chunks: impl Iterator<Item = &'a str>, destination: &mut [u8]) {
    let mut end = destination.len();
    for chunk in chunks {
        let start = end - chunk.len();
        destination
            .get_mut(start..end)
            .unwrap_or_default()
            .copy_from_slice(chunk.as_bytes());
        end = start;
    }
    debug_assert_eq!(end, 0);
    #[cfg(test)]
    crate::work::copy(destination.len());
}

#[cfg(test)]
thread_local! {
    static COPY_SNAPSHOTS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

impl Clone for Text {
    fn clone(&self) -> Self {
        #[cfg(test)]
        if COPY_SNAPSHOTS.get() {
            crate::work::copy(self.len());
            return Self::from(self.as_str().into_owned());
        }
        Self {
            root: self.root.clone(),
            range: self.range.clone(),
        }
    }
}

struct Chunks<'a> {
    current: Option<(&'a Node, Range<usize>)>,
    pending: Vec<(&'a Node, Range<usize>)>,
}
impl<'a> Iterator for Chunks<'a> {
    type Item = &'a str;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (node, range) = self.current.take().or_else(|| self.pending.pop())?;
            #[cfg(test)]
            crate::work::scan(1);
            match &node.0 {
                Kind::Chunk { prefix, data } => {
                    if range.start < prefix.len() {
                        self.current = prefix.root.as_deref().map(|node| {
                            (
                                node,
                                prefix.range.start + range.start
                                    ..prefix.range.start + range.end.min(prefix.len()),
                            )
                        });
                    }
                    if range.end > prefix.len() {
                        return data.get(
                            range.start.saturating_sub(prefix.len())..range.end - prefix.len(),
                        );
                    }
                }
                Kind::Concat { left, right } => {
                    if range.start < left.len()
                        && let Some(node) = left.root.as_deref()
                    {
                        #[cfg(test)]
                        if self.pending.len() == self.pending.capacity() {
                            crate::work::copy(
                                self.pending.len() * std::mem::size_of::<(&Node, Range<usize>)>(),
                            );
                        }
                        self.pending.push((
                            node,
                            left.range.start + range.start
                                ..left.range.start + range.end.min(left.len()),
                        ));
                    }
                    if range.end > left.len() {
                        self.current = right.root.as_deref().map(|node| {
                            (
                                node,
                                right.range.start + range.start.saturating_sub(left.len())
                                    ..right.range.start + range.end - left.len(),
                            )
                        });
                    }
                }
            }
        }
    }
}

impl Drop for Text {
    fn drop(&mut self) {
        let mut current = self.root.take();
        let mut pending = Vec::new();
        while let Some(node) = current.take().or_else(|| pending.pop()) {
            let Some(mut node) = Arc::into_inner(node) else {
                continue;
            };
            #[cfg(test)]
            crate::work::scan(1);
            match &mut node.0 {
                Kind::Chunk { prefix, .. } => current = prefix.root.take(),
                Kind::Concat { left, right } => {
                    current = left.root.take();
                    pending.extend(right.root.take());
                }
            }
        }
    }
}

impl From<String> for Text {
    fn from(text: String) -> Self {
        let mut result = Self::default();
        result.append_owned(text);
        result
    }
}
impl From<&str> for Text {
    fn from(text: &str) -> Self {
        #[cfg(test)]
        crate::work::copy(text.len());
        Self::from(text.to_owned())
    }
}
impl Extend<char> for Text {
    fn extend<T: IntoIterator<Item = char>>(&mut self, iter: T) {
        for character in iter {
            let mut bytes = [0; 4];
            self.push_str(character.encode_utf8(&mut bytes));
        }
    }
}
impl Extend<Text> for Text {
    fn extend<T: IntoIterator<Item = Text>>(&mut self, iter: T) {
        for text in iter {
            self.append_shared(text);
        }
    }
}
impl FromIterator<Text> for Text {
    fn from_iter<T: IntoIterator<Item = Text>>(iter: T) -> Self {
        let mut text = Self::default();
        text.extend(iter);
        text
    }
}
impl PartialEq for Text {
    fn eq(&self, other: &Self) -> bool {
        if self.range == other.range
            && self
                .root
                .as_ref()
                .zip(other.root.as_ref())
                .is_some_and(|(left, right)| Arc::ptr_eq(left, right))
        {
            return true;
        }
        self.len() == other.len()
            && self
                .reverse_chunks()
                .flat_map(|s| s.bytes().rev())
                .eq(other.reverse_chunks().flat_map(|s| s.bytes().rev()))
    }
}
impl Eq for Text {}
impl PartialEq<str> for Text {
    fn eq(&self, other: &str) -> bool {
        self.len() == other.len()
            && self
                .reverse_chunks()
                .flat_map(|s| s.bytes().rev())
                .eq(other.bytes().rev())
    }
}
impl PartialEq<&str> for Text {
    fn eq(&self, other: &&str) -> bool {
        self == *other
    }
}
impl PartialEq<String> for Text {
    fn eq(&self, other: &String) -> bool {
        self == other.as_str()
    }
}
impl fmt::Display for Text {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.as_str())
    }
}
impl fmt::Debug for Text {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.as_str(), f)
    }
}

#[cfg(test)]
#[path = "../tests/text.rs"]
mod tests;
