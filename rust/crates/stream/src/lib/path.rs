use crate::{MAX_ITEMS, MAX_PATH_BYTES, StreamError};
use std::{fmt, sync::Arc};

/// Permanent account-relative slash-separated ASCII path.
pub struct StreamPath {
    text: PathText,
    length: usize,
}

#[derive(Clone)]
enum PathText {
    Plain(Arc<str>),
    #[cfg(not(target_arch = "wasm32"))]
    Native(Arc<NativePathText>),
}

#[cfg(not(target_arch = "wasm32"))]
// Text storage retires before its metadata and original payload allowance.
struct NativePathText {
    text: String,
    _metadata: acyclic_native_runtime::ReadAllocation,
    _payload: acyclic_native_runtime::ReadAllocation,
}

#[cfg(feature = "serde")]
impl serde::Serialize for StreamPath {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for StreamPath {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct PathVisitor;
        impl serde::de::Visitor<'_> for PathVisitor {
            type Value = StreamPath;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a canonical stream path")
            }

            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<StreamPath, E> {
                StreamPath::new(value).map_err(E::custom)
            }
        }
        deserializer.deserialize_str(PathVisitor)
    }
}

impl StreamPath {
    /// Validates and owns one path.
    pub fn new(path: impl AsRef<str>) -> Result<Self, StreamError> {
        let path = path.as_ref();
        Self::validate(path)?;
        Ok(Self {
            text: PathText::Plain(Arc::from(path)),
            length: path.len(),
        })
    }

    pub(crate) fn validate(path: &str) -> Result<(), StreamError> {
        if path.is_empty()
            || path.len() > MAX_PATH_BYTES
            || !path.is_ascii()
            || path.starts_with('/')
            || path.ends_with('/')
        {
            return Err(StreamError::InvalidPath);
        }
        let mut count = 0_usize;
        for segment in path.split('/') {
            count = count.checked_add(1).ok_or(StreamError::LimitExceeded)?;
            if segment.is_empty()
                || segment == "."
                || segment == ".."
                || segment
                    .bytes()
                    .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
                || segment.as_bytes().contains(&b'\\')
            {
                return Err(StreamError::InvalidPath);
            }
        }
        if count > MAX_ITEMS {
            return Err(StreamError::LimitExceeded);
        }
        Ok(())
    }

    /// Validates and owns path storage on the original native ledger.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new_native(
        path: impl AsRef<str>,
        meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> std::io::Result<Result<Self, StreamError>> {
        let path = path.as_ref();
        if let Err(error) = Self::validate(path) {
            return Ok(Err(error));
        }
        let mut allocation = acyclic_native_runtime::ReadAllocation::new(meter, path.len())?;
        let mut data = String::new();
        data.try_reserve_exact(path.len())
            .map_err(|error| meter.allocation_failed(error))?;
        allocation.reconcile_capacity(data.capacity())?;
        meter.admit_codec(0, path.len() as u64, 0)?;
        data.push_str(path);
        meter.record_codec(0, path.len() as u64, 0)?;
        let metadata = acyclic_native_runtime::ReadAllocation::new(
            meter,
            std::mem::size_of::<NativePathText>(),
        )?;
        Ok(Ok(Self {
            text: PathText::Native(Arc::new(NativePathText {
                text: data,
                _metadata: metadata,
                _payload: allocation,
            })),
            length: path.len(),
        }))
    }

    /// Canonical path text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        let text: &str = match &self.text {
            PathText::Plain(text) => text,
            #[cfg(not(target_arch = "wasm32"))]
            PathText::Native(text) => &text.text,
        };
        // `length` always ends on a segment boundary of `text`.
        text.get(..self.length).unwrap_or(text)
    }

    fn prefix(&self, length: usize) -> Self {
        assert!(length <= self.length, "path prefix exceeds its source");
        Self {
            text: self.text.clone(),
            length,
        }
    }

    /// Shares this path or a complete ancestor; rejects partial segments.
    pub(crate) fn ancestor(&self, ancestor: &str) -> Option<Self> {
        let path = self.as_str();
        if ancestor.is_empty()
            || ancestor.len() > path.len()
            || (!std::ptr::eq(path.as_ptr(), ancestor.as_ptr()) && !path.starts_with(ancestor))
            || path
                .as_bytes()
                .get(ancestor.len())
                .is_some_and(|byte| *byte != b'/')
        {
            return None;
        }
        Some(self.prefix(ancestor.len()))
    }

    /// Immediate parent, if any.
    #[must_use]
    pub fn parent(&self) -> Option<Self> {
        self.as_str().rfind('/').map(|index| self.prefix(index))
    }
}

impl fmt::Display for StreamPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl fmt::Debug for StreamPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("StreamPath")
            .field(&self.as_str())
            .finish()
    }
}
impl std::hash::Hash for StreamPath {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(self.as_str(), state);
    }
}
impl std::borrow::Borrow<str> for StreamPath {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl Clone for StreamPath {
    fn clone(&self) -> Self {
        #[cfg(all(test, not(target_arch = "wasm32")))]
        if matches!(&self.text, PathText::Plain(_)) && crate::path_text_tests::copy_on_clone() {
            return Self::new(self.as_str())
                .unwrap_or_else(|_| unreachable!("validated path clone"));
        }
        Self {
            text: self.text.clone(),
            length: self.length,
        }
    }
}

impl PartialEq for StreamPath {
    fn eq(&self, other: &Self) -> bool {
        let (left, right) = (self.as_str(), other.as_str());
        std::ptr::eq(left, right) || left == right
    }
}
impl Eq for StreamPath {}
impl PartialOrd for StreamPath {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for StreamPath {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let (left, right) = (self.as_str(), other.as_str());
        if std::ptr::eq(left, right) {
            std::cmp::Ordering::Equal
        } else {
            crate::owned_bytes::compare_bytes(left.as_bytes(), right.as_bytes())
        }
    }
}
