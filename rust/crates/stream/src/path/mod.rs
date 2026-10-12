use super::*;

/// Permanent account-relative slash-separated ASCII path.
#[derive(Clone)]
pub struct StreamPath(StreamPathStorage);

#[derive(Clone)]
enum StreamPathStorage {
    Plain(Arc<str>),
    #[cfg(not(target_arch = "wasm32"))]
    Native { owner: Arc<NativePathText>, length: usize },
}

#[cfg(not(target_arch = "wasm32"))]
struct NativePathText {
    text: String,
    _allocation: acyclic_native_runtime::ReadAllocation,
}

impl StreamPath {
    /// Validates and owns one path.
    pub fn new(path: impl AsRef<str>) -> Result<Self, StreamError> {
        let path = path.as_ref();
        Self::validate(path)?;
        Ok(Self(StreamPathStorage::Plain(Arc::from(path))))
    }

    fn validate(path: &str) -> Result<(), StreamError> {
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

    /// Owns canonical path text with last-alias original-ledger custody.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new_native(
        path: impl AsRef<str>,
        meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> std::io::Result<Result<Self, StreamError>> {
        let path = path.as_ref();
        if let Err(error) = Self::validate(path) { return Ok(Err(error)); }
        let mut allocation = acyclic_native_runtime::ReadAllocation::new(meter, path.len())?;
        let mut text = String::new();
        if let Err(error) = text.try_reserve_exact(path.len()) {
            return Err(meter.allocation_failed(error));
        }
        allocation.reconcile_capacity(text.capacity())?;
        meter.admit_codec(0, path.len() as u64, 0)?;
        text.push_str(path);
        meter.record_codec(0, path.len() as u64, 0)?;
        let metadata = std::mem::size_of::<NativePathText>();
        meter.admit_codec(0, metadata as u64, 0)?;
        let mut owner_allocation = acyclic_native_runtime::ReadAllocation::new(meter, metadata)?;
        allocation.absorb(&mut owner_allocation)?;
        let owner = Arc::new(NativePathText { text, _allocation: allocation });
        meter.record_codec(0, metadata as u64, 0)?;
        Ok(Ok(Self(StreamPathStorage::Native { owner, length: path.len() })))
    }

    /// Canonical path text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        match &self.0 {
            StreamPathStorage::Plain(text) => text,
            #[cfg(not(target_arch = "wasm32"))]
            StreamPathStorage::Native { owner, length } => &owner.text[..*length],
        }
    }

    /// Immediate parent, if any.
    #[must_use]
    #[allow(
        clippy::string_slice,
        reason = "`new` rejects any path that is not `is_ascii()`, so every byte offset from \
                  `rfind` is a valid char boundary and this can never panic"
    )]
    pub fn parent(&self) -> Option<Self> {
        let index = self.as_str().rfind('/')?;
        Some(Self(match &self.0 {
            StreamPathStorage::Plain(text) => StreamPathStorage::Plain(Arc::from(&text[..index])),
            #[cfg(not(target_arch = "wasm32"))]
            StreamPathStorage::Native { owner, .. } =>
                StreamPathStorage::Native { owner: Arc::clone(owner), length: index },
        }))
    }

    /// Produces an implicit parent without an unmetered path allocation.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn parent_native(
        &self,
        meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> std::io::Result<Option<Self>> {
        meter.check()?;
        let Some(index) = self.as_str().rfind('/') else { return Ok(None); };
        match &self.0 {
            StreamPathStorage::Native { owner, .. } => Ok(Some(Self(
                StreamPathStorage::Native { owner: Arc::clone(owner), length: index },
            ))),
            StreamPathStorage::Plain(_) => match Self::new_native(&self.as_str()[..index], meter)? {
                Ok(parent) => Ok(Some(parent)),
                // A prefix of an already valid ASCII path is itself valid.
                Err(_) => unreachable!("canonical stream path parent was invalid"),
            },
        }
    }

    /// Retains an existing native owner, or funds a real copy of plain text before it can outlive the caller's operation-scoped input claim.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn retain_native(
        &self,
        meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> std::io::Result<Self> {
        meter.check()?;
        match &self.0 {
            StreamPathStorage::Native { .. } => Ok(self.clone()),
            StreamPathStorage::Plain(_) => match Self::new_native(self.as_str(), meter)? {
                Ok(path) => Ok(path),
                Err(_) => unreachable!("canonical stream path became invalid"),
            },
        }
    }
}

impl fmt::Debug for StreamPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("StreamPath").field(&self.as_str()).finish()
    }
}

impl PartialEq for StreamPath {
    fn eq(&self, other: &Self) -> bool { self.as_str() == other.as_str() }
}
impl Eq for StreamPath {}
impl PartialOrd for StreamPath {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) }
}
impl Ord for StreamPath {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering { self.as_str().cmp(other.as_str()) }
}
impl std::hash::Hash for StreamPath {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(self.as_str(), state);
    }
}
impl std::borrow::Borrow<str> for StreamPath {
    fn borrow(&self) -> &str { self.as_str() }
}

impl fmt::Display for StreamPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}
