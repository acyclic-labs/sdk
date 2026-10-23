use super::*;
pub use acyclic_fs::access::VolumeOperation;
pub(super) mod canonical;

/// The three independently governed file namespaces.
#[derive(
    Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize, schemars::JsonSchema,
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
    Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(
    tag = "kind",
    content = "id",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum VolumeOwner {
    /// Project workspace owner.
    Project(#[serde(deserialize_with = "deserialize_label")] String),
    /// Agent private-volume owner.
    Agent(#[schemars(with = "String")] AgentId),
    /// Shared session-volume owner.
    Session(#[serde(deserialize_with = "deserialize_label")] String),
}

/// Globally addressable volume identity with no embedded capability or endpoint.
#[derive(
    Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct VolumeRef {
    pub(super) provider: ProviderRef,
    #[serde(deserialize_with = "deserialize_label")]
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
        if work.is_none() {
            return Ok(self.clone());
        }
        self.validate(work)?;
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let volume = self.clone_in_quota(&mut quota)?;
        quota.transfer();
        Ok(volume)
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
    verifier: AuthorityVerifier,
    scope: Scope,
    pub(super) volume: VolumeRef,
    pub(super) operation: VolumeOperation,
    pub(super) exact_file: Option<(String, String, FileDescriptor, String)>,
    pub(super) directory_prefix: Option<String>,
}

impl ContentGrant {
    /// Borrows the authenticated grant for the persisted-policy decision.
    #[cfg(feature = "filesystem")]
    pub(crate) const fn policy_scope(&self) -> &Scope {
        &self.scope
    }

    /// Constructs read authority from authenticated project-source authorization.
    #[cfg(feature = "filesystem")]
    pub(crate) fn from_project_source(
        authorization: &crate::source_authorization::ProjectSourceAuthorization,
        verifier: &AuthorityVerifier,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        authorization.verify_scope(verifier, work)?;
        Ok(Self {
            verifier: verifier.copy(work)?,
            scope: authorization.policy_scope.copy(work)?,
            volume: authorization.project.copy(work)?,
            operation: VolumeOperation::Read,
            exact_file: None,
            directory_prefix: Some(String::new()),
        })
    }

    /// Resolves an authenticated whole-volume, exact-file, or directory read.
    pub fn verify_read(
        verifier: &AuthorityVerifier,
        scope: &Scope,
        file: &FileRef,
    ) -> Result<Self> {
        file.validate(&mut None)?;
        if is_internal_path(file.path()) && !is_inherited_context_path(file.path()) {
            return Err(Error::Unauthorized(
                "internal storage file is not public content".into(),
            ));
        }
        verifier.verify(scope, &mut None)?;
        if contains_content_label(
            scope.capabilities(),
            VolumeOperation::Read.capability_prefix(),
            file.volume().identity_digest(u64::MAX, &mut None)?,
        ) {
            return Ok(Self {
                verifier: verifier.copy(&mut None)?,
                scope: scope.copy(&mut None)?,
                volume: file.volume.clone(),
                operation: VolumeOperation::Read,
                exact_file: None,
                directory_prefix: None,
            });
        }
        if contains_content_label(
            scope.capabilities(),
            acyclic_fs::access::FILE_READ_PREFIX,
            file.identity_digest(u64::MAX, &mut None)?,
        ) {
            let FileRef {
                volume,
                path,
                version,
                descriptor,
                display_name,
            } = file.clone();
            return Ok(Self {
                verifier: verifier.copy(&mut None)?,
                scope: scope.copy(&mut None)?,
                volume,
                operation: VolumeOperation::Read,
                exact_file: Some((path, version, descriptor, display_name)),
                directory_prefix: None,
            });
        }
        if file.volume().class() == VolumeClass::AgentPrivate {
            for prefix in std::iter::once("").chain(
                file.path()
                    .match_indices('/')
                    .filter_map(|(end, _)| file.path().get(..end)),
            ) {
                if file
                    .volume()
                    .directory_read_capability(prefix, &mut None)
                    .is_ok_and(|capability| scope.capabilities().contains(&capability))
                {
                    if scope.agent().is_none() {
                        return Err(Error::Unauthorized(
                            "private directory read is not granted".into(),
                        ));
                    }
                    let grant = Self {
                        verifier: verifier.copy(&mut None)?,
                        scope: scope.copy(&mut None)?,
                        volume: file.volume.clone(),
                        operation: VolumeOperation::Read,
                        exact_file: None,
                        directory_prefix: Some(prefix.to_owned()),
                    };
                    grant.require_file_read(file, None)?;
                    return Ok(grant);
                }
            }
        }
        Err(Error::Unauthorized(
            "file read is not granted by its owner".into(),
        ))
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
        Self::authorize_volume(verifier, scope, volume, operation, maximum_bytes, work)?;
        Ok(Self {
            verifier: verifier.copy(work)?,
            scope: scope.copy(work)?,
            volume: volume.copy(work)?,
            operation,
            exact_file: None,
            directory_prefix: None,
        })
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        let mut bytes = self
            .volume
            .logical_owned_bytes()?
            .checked_add(self.scope.logical_owned_bytes()?)
            .and_then(|bytes| bytes.checked_add(self.verifier.logical_owned_bytes().ok()?))
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
    pub(crate) fn authorize_volume(
        verifier: &AuthorityVerifier,
        scope: &Scope,
        volume: &VolumeRef,
        operation: VolumeOperation,
        maximum_bytes: u64,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        verifier.verify(scope, work)?;
        volume.validate(work)?;
        if volume.provider().family() == "filesystem"
            && crate::core::filesystem_scope_label(scope.capabilities(), work)?.is_some()
        {
            return Ok(());
        }
        if operation == VolumeOperation::Write {
            volume.require_writer(scope.agent(), work)?;
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
        verifier.verify(scope, work)?;
        volume.validate(work)?;
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
        let structured = volume.provider().family() == "filesystem"
            && crate::core::filesystem_scope_label(scope.capabilities(), work)?.is_some();
        if (!structured && scope.agent().is_none())
            || !crate::core::directory_read_granted_with_work(
                scope.capabilities(),
                volume,
                prefix,
                work,
            )?
        {
            return Err(conversation_error(
                work,
                crate::wire::ErrorCode::Unauthorized,
                "private directory read is not granted",
            ));
        }
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let scope = scope.copy(quota.work())?;
        quota.adopt(scope.logical_owned_bytes()?)?;
        let verifier = verifier.copy(quota.work())?;
        quota.adopt(verifier.logical_owned_bytes()?)?;
        let grant = Self {
            verifier,
            scope,
            volume: volume.clone_in_quota(&mut quota)?,
            operation: VolumeOperation::Read,
            exact_file: None,
            directory_prefix: Some(crate::core::copy_text(prefix, &mut quota)?),
        };
        quota.transfer();
        Ok(grant)
    }

    /// Authorizes only one pinned file version, including across an owner boundary.
    pub fn verify_file_read(
        verifier: &AuthorityVerifier,
        scope: &Scope,
        file: &FileRef,
    ) -> Result<Self> {
        file.validate(&mut None)?;
        if is_internal_path(file.path()) && !is_inherited_context_path(file.path()) {
            return Err(Error::Unauthorized(
                "internal storage file is not public content".into(),
            ));
        }
        verifier.verify(scope, &mut None)?;
        if !contains_content_label(
            scope.capabilities(),
            acyclic_fs::access::FILE_READ_PREFIX,
            file.identity_digest(u64::MAX, &mut None)?,
        ) {
            return Err(Error::Unauthorized("exact file read is not granted".into()));
        }
        let FileRef {
            volume,
            path,
            version,
            descriptor,
            display_name,
        } = file.clone();
        Ok(Self {
            verifier: verifier.copy(&mut None)?,
            scope: scope.copy(&mut None)?,
            volume,
            operation: VolumeOperation::Read,
            exact_file: Some((path, version, descriptor, display_name)),
            directory_prefix: None,
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn copy_verifier(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<AuthorityVerifier> {
        self.verifier.copy(work)
    }

    /// Revalidates expiry and revocation immediately before authority is used.
    pub(crate) fn recheck(&self, io: Option<&ContentIo>) -> Result<()> {
        crate::executor::with_history_io(io, |work| self.verifier.verify(&self.scope, work))
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
                    (path, version, descriptor, display_name)
                        != (
                            &file.path,
                            &file.version,
                            &file.descriptor,
                            &file.display_name,
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
