use super::*;
pub use acyclic_fs::access::VolumeOperation;
pub(super) mod canonical;

/// The three independently governed file namespaces.
#[derive(
    Clone,
    Copy,
    Debug,
    Eq,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
    Deserialize,
    schemars::JsonSchema,
    crate::HeapLayout,
)]
#[serde(rename_all = "snake_case")]
pub enum VolumeClass {
    /// Consumer's project role. Lineage and merge semantics belong to Filesystem.
    Project,
    /// Scratch files owned by exactly one agent.
    AgentPrivate,
    /// Files shared through explicit session grants.
    SessionShared,
}

/// Logical owner used for routing; access still requires an authenticated grant.
#[derive(
    Clone,
    Debug,
    Eq,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
    Deserialize,
    schemars::JsonSchema,
    crate::HeapLayout,
)]
#[serde(
    tag = "kind",
    content = "id",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum VolumeOwner {
    /// Project workspace owner.
    Project(
        #[serde(deserialize_with = "deserialize_label")]
        #[heap_layout(scalar)]
        String,
    ),
    /// Agent private-volume owner.
    Agent(#[schemars(with = "String")] AgentId),
    /// Shared session-volume owner.
    Session(
        #[serde(deserialize_with = "deserialize_label")]
        #[heap_layout(scalar)]
        String,
    ),
}

/// Globally addressable volume identity with no embedded capability or endpoint.
#[derive(
    Clone,
    Debug,
    Eq,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
    Deserialize,
    schemars::JsonSchema,
    crate::HeapLayout,
)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct VolumeRef {
    pub(super) provider: ProviderRef,
    #[serde(deserialize_with = "deserialize_label")]
    #[heap_layout(scalar)]
    pub(super) id: String,
    pub(super) class: VolumeClass,
    pub(super) owner: VolumeOwner,
}

impl VolumeRef {
    pub(crate) fn identity_digest(
        &self,
        maximum_bytes: u64,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<[u8; 32]> {
        crate::executor::json::borrowed_json_digest(
            &canonical::Canonical(self),
            maximum_bytes,
            work,
        )
    }

    fn directory_digest(
        &self,
        prefix: &str,
        maximum_bytes: u64,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<[u8; 32]> {
        crate::executor::json::borrowed_json_digest(
            &(canonical::Canonical(self), prefix),
            maximum_bytes,
            work,
        )
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        let owner = match &self.owner {
            VolumeOwner::Project(owner) | VolumeOwner::Session(owner) => owner.capacity() as u64,
            VolumeOwner::Agent(_) => 0,
        };
        self.provider
            .logical_owned_bytes()?
            .checked_add(self.id.capacity() as u64)
            .and_then(|bytes| bytes.checked_add(owner))
            .ok_or_else(|| Error::Invalid("volume identity ownership overflow".into()))
    }

    /// Constructs a volume from owned identity fields.
    pub fn new(
        provider: ProviderRef,
        id: String,
        class: VolumeClass,
        owner: VolumeOwner,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        let value = Self {
            provider,
            id,
            class,
            owner,
        };
        value.validate(work)?;
        Ok(value)
    }

    /// Validates metadata under the caller's optional work observer.
    pub fn validate(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let Self {
            provider,
            id,
            owner,
            ..
        } = self;
        provider.validate(work)?;
        validate_label(id, work)?;
        match owner {
            VolumeOwner::Project(id) | VolumeOwner::Session(id) => {
                validate_label(id, work)?;
            }
            VolumeOwner::Agent(_) => {}
        }
        Ok(())
    }

    /// Storage implementation identity.
    #[must_use]
    pub const fn provider(&self) -> &ProviderRef {
        &self.provider
    }

    /// Provider-owned volume identity.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Consumer role of this namespace; grants determine access.
    #[must_use]
    pub const fn class(&self) -> VolumeClass {
        self.class
    }

    /// Logical owner, used to route authorized reads.
    #[must_use]
    pub const fn owner(&self) -> &VolumeOwner {
        &self.owner
    }

    /// Enforces owner-only agent mutations.
    pub(crate) fn require_writer(
        &self,
        agent: Option<AgentId>,
        work: &Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let VolumeOwner::Agent(owner) = self.owner()
            && agent != Some(*owner)
        {
            return Err(conversation_error(
                work,
                crate::wire::ErrorCode::Unauthorized,
                "only the owning agent may write this volume",
            ));
        }
        Ok(())
    }

    /// Exact capability for one operation on this volume.
    pub fn capability(
        &self,
        operation: VolumeOperation,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<String> {
        content_label(
            operation.capability_prefix(),
            self.identity_digest(
                crate::store::default_projection_read_limits().maximum_bytes,
                work,
            )?,
            work,
        )
    }

    /// Copies identity fields under the optional work observer.
    pub fn copy(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        crate::wire_codec::CodecQuota::produce(work, |quota| self.copy_in_quota(quota))
            .map(|(volume, _)| volume)
    }

    pub(crate) fn copy_in_quota(
        &self,
        quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
    ) -> Result<Self> {
        if quota.work().is_none() {
            return Ok(self.clone());
        }
        self.validate(quota.work())?;
        self.clone_in_quota(quota)
    }

    pub(crate) fn clone_in_quota(
        &self,
        quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
    ) -> Result<Self> {
        let provider = crate::core::copy_provider(&self.provider, quota)?;
        Ok(Self {
            provider,
            id: crate::core::copy_text(&self.id, quota)?,
            class: self.class,
            owner: match &self.owner {
                VolumeOwner::Project(text) => {
                    VolumeOwner::Project(crate::core::copy_text(text, quota)?)
                }
                VolumeOwner::Session(text) => {
                    VolumeOwner::Session(crate::core::copy_text(text, quota)?)
                }
                VolumeOwner::Agent(id) => VolumeOwner::Agent(*id),
            },
        })
    }

    /// Owner-issued read capability for a directory and its descendants.
    pub fn directory_read_capability(
        &self,
        prefix: &str,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<String> {
        if !prefix.is_empty() {
            validate_content_path(prefix, work)?;
        }
        if is_internal_path(prefix) {
            return Err(conversation_error(
                work,
                crate::wire::ErrorCode::Invalid,
                "internal storage paths cannot be delegated",
            ));
        }
        content_label(
            acyclic_fs::access::DIRECTORY_READ_PREFIX,
            self.directory_digest(
                prefix,
                crate::store::default_projection_read_limits().maximum_bytes,
                work,
            )?,
            work,
        )
    }

    /// Collision-resistant physical namespace for Filesystem adapters.
    pub fn storage_name(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<String> {
        content_label(
            "harness-",
            self.identity_digest(
                crate::store::default_projection_read_limits().maximum_bytes,
                work,
            )?,
            work,
        )
    }
}

fn label_bytes(prefix: &str, digest: [u8; 32]) -> Option<([u8; 80], usize)> {
    let hash = blake3::Hash::from_bytes(digest).to_hex();
    let mut label = [0_u8; 80];
    let length = prefix.len().checked_add(hash.len())?;
    let target = label.get_mut(..length)?;
    let (prefix_bytes, hash_bytes) = target.split_at_mut(prefix.len());
    prefix_bytes.copy_from_slice(prefix.as_bytes());
    hash_bytes.copy_from_slice(hash.as_bytes());
    Some((label, length))
}

#[doc(hidden)]
pub fn content_label(
    prefix: &str,
    digest: [u8; 32],
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<String> {
    let invalid = || {
        conversation_error(
            work,
            crate::wire::ErrorCode::Invalid,
            "invalid content identity label",
        )
    };
    let (label, length) = label_bytes(prefix, digest).ok_or_else(invalid)?;
    let text = label
        .get(..length)
        .and_then(|bytes| std::str::from_utf8(bytes).ok())
        .ok_or_else(invalid)?;
    match work.as_deref_mut() {
        Some(io) => io.clone_text(text),
        None => Ok(text.to_owned()),
    }
}

#[doc(hidden)]
pub fn contains_content_label(
    grants: &crate::Capabilities,
    prefix: &str,
    digest: [u8; 32],
) -> bool {
    let Some((label, length)) = label_bytes(prefix, digest) else {
        return false;
    };
    label
        .get(..length)
        .and_then(|bytes| std::str::from_utf8(bytes).ok())
        .is_some_and(|label| grants.contains(label))
}

/// Verified, exact-volume authority for a single storage operation.
#[derive(Clone, Debug)]
pub struct ContentGrant {
    authority: crate::core::VerifiedScope,
    pub(super) volume: VolumeRef,
    pub(super) operation: VolumeOperation,
    pub(super) exact_file: Option<(String, String, FileDescriptor, String)>,
    pub(super) directory_prefix: Option<String>,
}

struct Boundary {
    volume: VolumeRef,
    operation: VolumeOperation,
    exact_file: Option<(String, String, FileDescriptor, String)>,
    directory_prefix: Option<String>,
}

impl ContentGrant {
    // The guard owns every retained component until a complete grant is returned.
    fn build(
        verifier: &AuthorityVerifier,
        scope: &Scope,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
        boundary: impl FnOnce(&Scope, &mut crate::wire_codec::CodecQuota<'_, '_>) -> Result<Boundary>,
    ) -> Result<Self> {
        crate::wire_codec::CodecQuota::produce(work, |quota| {
            Self::build_in_quota(verifier, scope, quota, boundary)
        })
        .map(|(grant, _)| grant)
    }

    fn build_in_quota(
        verifier: &AuthorityVerifier,
        scope: &Scope,
        quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
        boundary: impl FnOnce(&Scope, &mut crate::wire_codec::CodecQuota<'_, '_>) -> Result<Boundary>,
    ) -> Result<Self> {
        let authority = verifier.authenticate_scope(scope, quota.work())?;
        quota.adopt(authority.logical_owned_bytes()?)?;
        let boundary = boundary(authority.scope(), quota)?;
        Ok(Self {
            authority,
            volume: boundary.volume,
            operation: boundary.operation,
            exact_file: boundary.exact_file,
            directory_prefix: boundary.directory_prefix,
        })
    }

    /// Borrows the authenticated grant for the persisted-policy decision.
    #[cfg(feature = "filesystem")]
    pub(crate) const fn policy_scope(&self) -> &Scope {
        self.authority.scope()
    }

    /// Resolves an authenticated read of `file`: the one read decision
    /// ([`crate::core::file_read_grant`]) every content path shares, signed.
    pub fn verify_read(
        verifier: &AuthorityVerifier,
        scope: &Scope,
        file: &FileRef,
    ) -> Result<Self> {
        Self::verify_read_with_work(verifier, scope, file, &mut None)
    }

    /// [`Self::verify_read`], charged to `work`.
    pub(crate) fn verify_read_with_work(
        verifier: &AuthorityVerifier,
        scope: &Scope,
        file: &FileRef,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        file.validate(work)?;
        if is_internal_path(file.path()) && !is_inherited_context_path(file.path()) {
            return Err(Error::Unauthorized(
                "internal storage file is not public content".into(),
            ));
        }
        let grant = Self::build(verifier, scope, work, |scope, quota| {
            let exact = || {
                let FileRef {
                    volume,
                    path,
                    version,
                    descriptor,
                    display_name,
                    ..
                } = file.clone();
                Boundary {
                    volume,
                    operation: VolumeOperation::Read,
                    exact_file: Some((path, version.into_string(), descriptor, display_name)),
                    directory_prefix: None,
                }
            };
            match crate::core::file_read_grant(scope.capabilities(), file, quota.work())? {
                Some(crate::core::ReadGrant::Volume) => Ok(Boundary {
                    volume: file.volume.clone_in_quota(quota)?,
                    operation: VolumeOperation::Read,
                    exact_file: None,
                    directory_prefix: None,
                }),
                // A structured scope admits the request; the volume's
                // persisted policy decides the path, and the grant reaches
                // only this pinned version.
                Some(crate::core::ReadGrant::Exact | crate::core::ReadGrant::Structured) => {
                    Ok(exact())
                }
                Some(crate::core::ReadGrant::Directory(prefix)) => {
                    if scope.agent().is_none() {
                        return Err(Error::Unauthorized(
                            "private directory read is not granted".into(),
                        ));
                    }
                    Ok(Boundary {
                        volume: file.volume.clone_in_quota(quota)?,
                        operation: VolumeOperation::Read,
                        exact_file: None,
                        directory_prefix: Some(crate::core::copy_text(prefix, quota)?),
                    })
                }
                None => Err(Error::Unauthorized(
                    "file read is not granted by its owner".into(),
                )),
            }
        })?;
        grant.require_file_read(file, None)?;
        Ok(grant)
    }

    /// Verifies signed authority and copies the exact granted volume.
    pub fn verify(
        verifier: &AuthorityVerifier,
        scope: &Scope,
        volume: &VolumeRef,
        operation: VolumeOperation,
        maximum_bytes: u64,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        crate::wire_codec::CodecQuota::produce(work, |quota| {
            Self::verify_in_quota(verifier, scope, volume, operation, maximum_bytes, quota)
        })
        .map(|(grant, _)| grant)
    }

    pub(crate) fn verify_in_quota(
        verifier: &AuthorityVerifier,
        scope: &Scope,
        volume: &VolumeRef,
        operation: VolumeOperation,
        maximum_bytes: u64,
        quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
    ) -> Result<Self> {
        Self::build_in_quota(verifier, scope, quota, |scope, quota| {
            Self::check_volume(scope, volume, operation, maximum_bytes, quota.work())?;
            Ok(Boundary {
                volume: volume.clone_in_quota(quota)?,
                operation,
                exact_file: None,
                directory_prefix: None,
            })
        })
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        let mut bytes = self
            .volume
            .logical_owned_bytes()?
            .checked_add(self.authority.logical_owned_bytes()?)
            .ok_or_else(|| Error::Invalid("grant scope ownership overflow".into()))?;
        if let Some((path, version, descriptor, display_name)) = &self.exact_file {
            bytes = [path.capacity(), version.capacity(), display_name.capacity()]
                .into_iter()
                .try_fold(bytes, |bytes, capacity| bytes.checked_add(capacity as u64))
                .and_then(|bytes| bytes.checked_add(descriptor.logical_owned_bytes()))
                .ok_or_else(|| Error::Invalid("grant field ownership overflow".into()))?;
        }
        bytes
            .checked_add(
                self.directory_prefix
                    .as_ref()
                    .map_or(0, |prefix| prefix.capacity() as u64),
            )
            .ok_or_else(|| Error::Invalid("grant prefix ownership overflow".into()))
    }

    /// Checks signed authority without constructing an owned grant.
    #[cfg(feature = "filesystem")]
    pub(crate) fn authorize_volume(
        verifier: &AuthorityVerifier,
        scope: &Scope,
        volume: &VolumeRef,
        operation: VolumeOperation,
        maximum_bytes: u64,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        verifier.verify(scope, work)?;
        Self::check_volume(scope, volume, operation, maximum_bytes, work)
    }

    /// Validate a proposed acting agent against its committed volume owner,
    /// before the custodian issues that agent's attestation. This creates no
    /// grant and never substitutes for the later signed acting-agent scope.
    #[cfg(feature = "filesystem")]
    pub(crate) fn authorize_agent_volume(
        verifier: &AuthorityVerifier,
        scope: &Scope,
        agent: AgentId,
        volume: &VolumeRef,
        operation: VolumeOperation,
        maximum_bytes: u64,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        verifier.verify(scope, work)?;
        if scope.agent().is_some_and(|admitted| admitted != agent) {
            return Err(Error::Unauthorized(
                "admission belongs to another acting agent".into(),
            ));
        }
        Self::check_agent_volume(scope, Some(agent), volume, operation, maximum_bytes, work)
    }

    fn check_volume(
        scope: &Scope,
        volume: &VolumeRef,
        operation: VolumeOperation,
        maximum_bytes: u64,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        Self::check_agent_volume(scope, scope.agent(), volume, operation, maximum_bytes, work)
    }

    fn check_agent_volume(
        scope: &Scope,
        agent: Option<AgentId>,
        volume: &VolumeRef,
        operation: VolumeOperation,
        maximum_bytes: u64,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        volume.validate(work)?;
        if volume.provider().family() == "filesystem"
            && crate::core::filesystem_scope_label(scope.capabilities(), work)?.is_some()
        {
            return Ok(());
        }
        if operation == VolumeOperation::Write {
            volume.require_writer(agent, work)?;
        }
        volume.validate(work)?;
        let digest = volume.identity_digest(
            maximum_bytes.min(crate::store::default_projection_read_limits().maximum_bytes),
            work,
        )?;
        if !contains_content_label(scope.capabilities(), operation.capability_prefix(), digest) {
            return Err(conversation_error(
                work,
                crate::wire::ErrorCode::Unauthorized,
                "volume operation is not granted",
            ));
        }
        Ok(())
    }

    /// Verifies a signed private-directory or whole-volume read grant.
    pub fn verify_directory_read(
        verifier: &AuthorityVerifier,
        scope: &Scope,
        volume: &VolumeRef,
        prefix: &str,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        Self::build(verifier, scope, work, |scope, quota| {
            volume.validate(quota.work())?;
            if !prefix.is_empty() {
                validate_content_path(prefix, quota.work())?;
            }
            if is_internal_path(prefix) {
                return Err(conversation_error(
                    quota.work(),
                    crate::wire::ErrorCode::Invalid,
                    "internal storage paths cannot be delegated",
                ));
            }
            let structured = volume.provider().family() == "filesystem"
                && crate::core::filesystem_scope_label(scope.capabilities(), quota.work())?
                    .is_some();
            if (!structured && scope.agent().is_none())
                || !crate::core::directory_read_granted_with_work(
                    scope.capabilities(),
                    volume,
                    prefix,
                    quota.work(),
                )?
            {
                return Err(conversation_error(
                    quota.work(),
                    crate::wire::ErrorCode::Unauthorized,
                    "private directory read is not granted",
                ));
            }
            Ok(Boundary {
                volume: volume.clone_in_quota(quota)?,
                operation: VolumeOperation::Read,
                exact_file: None,
                directory_prefix: Some(crate::core::copy_text(prefix, quota)?),
            })
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn copy_verifier(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<AuthorityVerifier> {
        self.authority.copy_verifier(work)
    }

    /// Revalidates expiry and revocation immediately before authority is used.
    pub(crate) fn recheck(&self, io: Option<&ContentIo>) -> Result<()> {
        crate::executor::with_history_io(io, |work| self.authority.recheck(work))
    }

    /// Checks exact volume and operation authority.
    pub fn require(
        &self,
        volume: &VolumeRef,
        operation: VolumeOperation,
        io: Option<&ContentIo>,
    ) -> Result<()> {
        self.recheck(io)?;
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            io.check_cancellation()?;
        }
        if self.volume != *volume
            || self.operation != operation
            || self.exact_file.is_some()
            || self.directory_prefix.is_some()
        {
            return Err(content_error(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "content grant does not match the volume",
            ));
        }
        Ok(())
    }

    /// Checks whole-volume, exact-file, or delegated-directory read authority.
    pub fn require_file_read(&self, file: &FileRef, io: Option<&ContentIo>) -> Result<()> {
        self.recheck(io)?;
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            io.check_cancellation()?;
        }
        if self.volume != *file.volume()
            || self.operation != VolumeOperation::Read
            || self
                .exact_file
                .as_ref()
                .is_some_and(|(path, version, descriptor, display_name)| {
                    (
                        path.as_str(),
                        version.as_str(),
                        descriptor,
                        display_name.as_str(),
                    ) != (
                        file.path(),
                        file.version(),
                        file.descriptor(),
                        file.display_name(),
                    )
                })
            || self.directory_prefix.as_deref().is_some_and(|prefix| {
                is_internal_path(file.path()) || !path_below(file.path(), prefix)
            })
        {
            return Err(content_error(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "content grant does not match the file",
            ));
        }
        Ok(())
    }

    /// Checks any lazy directory path against the owner-issued read boundary.
    pub fn require_directory_path(
        &self,
        volume: &VolumeRef,
        prefix: &str,
        io: Option<&ContentIo>,
    ) -> Result<()> {
        self.recheck(io)?;
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            io.check_cancellation()?;
        }
        if self.volume != *volume
            || self.operation != VolumeOperation::Read
            || is_internal_path(prefix)
            || !self
                .directory_prefix
                .as_deref()
                .is_some_and(|granted| path_below(prefix, granted))
        {
            return Err(content_error(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "directory listing is not granted",
            ));
        }
        Ok(())
    }
}

#[doc(hidden)]
pub fn is_internal_path(path: &str) -> bool {
    path == ".system" || path.starts_with(".system/")
}

#[doc(hidden)]
pub fn is_inherited_context_path(path: &str) -> bool {
    path.starts_with(".system/inherited-conversation/")
}

fn path_below(path: &str, prefix: &str) -> bool {
    prefix.is_empty()
        || path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|tail| tail.starts_with('/'))
}

crate::core::ownership::owned_enum!(VolumeClass {
    Project,
    AgentPrivate,
    SessionShared,
});
crate::core::ownership::owned_enum!(VolumeOwner {
    Project(field0),
    Agent(field0),
    Session(field0),
});
crate::core::ownership::owned_struct!(VolumeRef {
    provider,
    id,
    class,
    owner
});
