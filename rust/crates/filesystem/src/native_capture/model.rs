use super::*;

#[cfg(feature = "native-mount")]
pub(crate) const MAX_NATIVE_EXACT_CAPTURE_PATHS: usize = 65_536;

/// A native view may have host-generated timestamps that are not SDK metadata.
#[derive(Clone, Copy)]
pub(crate) struct NativeViewBaseline {
    pub(super) root_identity: NativeRootIdentity,
    pub(super) read_only: bool,
}

impl NativeViewBaseline {
    /// Records the identity and access mode of the materialized view.
    #[cfg(any(feature = "native-mount", all(test, unix)))]
    pub(crate) const fn new(root_identity: NativeRootIdentity, read_only: bool) -> Self {
        Self {
            root_identity,
            read_only,
        }
    }
    pub(super) fn restore_canonical_stamps(
        &self,
        root_identity: NativeRootIdentity,
        prior: FileMetadata,
        observed: &mut FileMetadata,
    ) {
        if self.root_identity != root_identity {
            return;
        }
        // Fields absent from the selected SDK generation were not restored by materialization. Host-created defaults cannot become an apparent source edit merely because the view was created or read.
        macro_rules! retain_unavailable {
            ($($field:ident),+ $(,)?) => {$(
                if matches!(prior.$field, MetadataField::Unavailable) {
                    observed.$field = MetadataField::Unavailable;
                }
            )+};
        }
        retain_unavailable!(
            posix_mode,
            posix_uid,
            posix_gid,
            posix_flags,
            windows_attributes,
            created_ns,
            modified_ns,
            accessed_ns,
            changed_ns,
            named_attributes,
            acl,
            security_descriptor
        );
        if self.read_only {
            observed.accessed_ns = prior.accessed_ns;
        }
        #[cfg(unix)]
        {
            observed.changed_ns = prior.changed_ns;
        }
        #[cfg(target_os = "linux")]
        {
            observed.created_ns = prior.created_ns;
        }
    }
}

/// Exact native capture options.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CaptureOptions {
    /// Materialized checkout root owned by the caller.
    pub source_root: PathBuf,
    /// Stable identity that the capability actually used for capture must own.
    pub expected_root_identity: NativeRootIdentity,
    /// Maximum changed paths admitted in one atomic transaction.
    pub maximum_paths: u32,
    /// Maximum physically allocated host ranges admitted per regular file.
    pub maximum_extent_spans: u32,
}

/// Canonical path eligibility for native capture.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CapturePolicy {
    pub(super) excluded_prefixes: Vec<NamespacePath>,
}

impl CapturePolicy {
    /// Creates a policy from canonical namespace prefixes.
    pub fn excluding(mut prefixes: Vec<NamespacePath>) -> Result<Self, CaptureError> {
        if prefixes.iter().any(NamespacePath::is_root) {
            return Err(CaptureError::InvalidOptions);
        }
        prefixes.sort_unstable();
        prefixes.dedup_by(|prefix, ancestor| prefix.is_within(ancestor));
        Ok(Self {
            excluded_prefixes: prefixes,
        })
    }

    /// Returns whether a path is excluded by an exact prefix.
    #[must_use]
    pub fn excludes(&self, path: &NamespacePath) -> bool {
        let candidate = self
            .excluded_prefixes
            .partition_point(|prefix| prefix <= path)
            .checked_sub(1)
            .and_then(|index| self.excluded_prefixes.get(index));
        candidate.is_some_and(|prefix| path.is_within(prefix))
    }

    /// Stable semantic identity used by durable source binding.
    #[must_use]
    pub fn fingerprint(&self) -> crate::foundation::Digest {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"acyclic-fs-capture-policy-v1\0");
        for prefix in &self.excluded_prefixes {
            hasher.update(&(prefix.depth() as u64).to_le_bytes());
            for component in prefix.components() {
                hasher.update(&[match component.encoding() {
                    NameEncoding::Utf8 => 1,
                    NameEncoding::PosixBytes => 2,
                    NameEncoding::WindowsUtf16Le => 3,
                }]);
                hasher.update(&(component.as_bytes().len() as u64).to_le_bytes());
                hasher.update(component.as_bytes());
            }
        }
        crate::foundation::Digest::from_bytes(*hasher.finalize().as_bytes())
    }
}

/// Successful authored host-state capture.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CaptureReceipt {
    /// Exact paths examined from the caller's bounded set.
    pub examined_paths: u64,
    /// Paths whose final host state required a checkout mutation.
    pub changed_paths: u64,
    /// Logical regular-file bytes streamed into immutable objects.
    pub staged_file_bytes: u64,
}

/// A watcher interval acknowledged only after its complete capture transaction changes the checkout candidate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WatchCaptureReceipt {
    /// Watcher epoch against which every path was interpreted.
    pub epoch: u64,
    /// First process-local sequence represented by the batch.
    pub first_sequence: u64,
    /// Sequence safe to request after this transaction succeeds.
    pub next_sequence: u64,
    /// Exact capture and movement facts.
    pub capture: CaptureReceipt,
}

/// Fail-closed native capture errors.
#[derive(Debug, Error, strum::IntoStaticStr)]
pub enum CaptureError {
    /// Capture bounds or source root are invalid.
    #[error("native capture options are invalid")]
    InvalidOptions,
    /// The exact capability opened for capture is not the admitted root.
    #[error("native capture root identity changed")]
    RootChanged,
    /// Watcher hints are invalid until the caller completes a fresh baseline.
    #[error("watcher epoch {epoch} requires a new baseline: {reason}")]
    RescanRequired {
        /// Invalidated watcher epoch.
        epoch: u64,
        /// Exact invalidation reason.
        reason: WatchInvalidationReason,
    },
    /// A complete baseline cannot replace an existing private mutation set.
    #[error("baseline capture requires a clean checkout")]
    DirtyCheckout,
    /// One requested path cannot be represented exactly on this host.
    #[error("native capture path is not exactly representable")]
    UnrepresentablePath,
    /// The host kind cannot be represented exactly by this volume adapter.
    #[error("host file kind is unsupported for exact capture")]
    UnsupportedKind,
    /// The canonical original filesystem operation retained its typed cause.
    #[error(transparent)]
    Kernel(#[from] crate::FsError),
}

impl From<crate::CancellationError> for CaptureError {
    fn from(error: crate::CancellationError) -> Self {
        Self::Kernel(error.into())
    }
}

impl From<crate::NativeTreeMaterializationError> for CaptureError {
    fn from(error: crate::NativeTreeMaterializationError) -> Self {
        Self::Kernel(crate::FsError::NativeKernel(error))
    }
}

impl From<WorkError> for CaptureError {
    fn from(error: WorkError) -> Self {
        Self::Kernel(crate::FsError::Work(error))
    }
}

impl From<std::io::Error> for CaptureError {
    fn from(error: std::io::Error) -> Self {
        crate::NativeTreeMaterializationError::Io(error).into()
    }
}

impl From<std::collections::TryReserveError> for CaptureError {
    fn from(error: std::collections::TryReserveError) -> Self {
        crate::NativeTreeMaterializationError::AllocationFailed(error).into()
    }
}

impl From<crate::kernel::NamespacePathError> for CaptureError {
    fn from(error: crate::kernel::NamespacePathError) -> Self {
        Self::Kernel(error.into())
    }
}
impl From<crate::kernel::TreePageError> for CaptureError {
    fn from(error: crate::kernel::TreePageError) -> Self {
        crate::kernel::NamespacePathError::Name(error).into()
    }
}
