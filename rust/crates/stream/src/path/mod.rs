use super::*;

/// Permanent account-relative slash-separated ASCII path.
#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
pub struct StreamPath(OwnedBytes);

impl StreamPath {
    /// Validates and owns one path.
    pub fn new(path: impl AsRef<str>) -> Result<Self, StreamError> {
        let path = path.as_ref();
        Self::validate(path)?;
        Ok(Self(OwnedBytes::copy_from_slice(path.as_bytes())))
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

    /// Owns canonical path text on the original native ledger.
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
        let mut data = Vec::new();
        data.try_reserve_exact(path.len())
            .map_err(|error| meter.allocation_failed(error))?;
        allocation.reconcile_capacity(data.capacity())?;
        meter.admit_codec(0, path.len() as u64, 0)?;
        data.extend_from_slice(path.as_bytes());
        meter.record_codec(0, path.len() as u64, 0)?;
        OwnedBytes::from_vec_with_guard(data, allocation, Arc::clone(meter))
            .map(|data| Ok(Self(data)))
            .map_err(|failure| match failure.into_error() {
                PreparedOwnedBytesError::Admission(error) => error,
                PreparedOwnedBytesError::NotFullyInitialized => {
                    unreachable!("immutable storage has no initialization requirement")
                }
            })
    }

    /// Canonical path text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.0).expect("validated paths and their prefixes are ASCII")
    }

    /// Immediate parent, if any.
    #[must_use]
    pub fn parent(&self) -> Option<Self> {
        self.as_str()
            .rfind('/')
            .map(|length| Self(self.0.slice(..length)))
    }

    /// Produces an implicit parent with funded text storage.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn parent_native(
        &self,
        meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> std::io::Result<Option<Self>> {
        meter.check()?;
        self.as_str()
            .rfind('/')
            .map(|length| self.retain_prefix(length, meter))
            .transpose()
    }

    /// Retains an existing owner or funds the ordinary text copy.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn retain_native(
        &self,
        meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> std::io::Result<Self> {
        meter.check()?;
        self.retain_prefix(self.0.len(), meter)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn retain_prefix(
        &self,
        length: usize,
        meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> std::io::Result<Self> {
        if self.0.is_native() {
            return Ok(Self(self.0.slice(..length)));
        }
        Self::new_native(&self.as_str()[..length], meter)?
            .map_err(|_| unreachable!("a canonical path prefix remains valid"))
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
impl fmt::Display for StreamPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}
