//! Guest sources derive from the same Bound as hosted turns and publication.

use super::*;
use crate::filesystem::{
    PreparationIo,
    native_access::{NativeMountAuthority, owner_mount_options},
};
use acyclic_fs::native_mount::{
    GuestMappingAdmission, GuestMappingFence, GuestPosixPolicy, GuestSessionAdmission, GuestSource,
    MountFilesystem,
};
use acyclic_fs::{NativeAllocation, NativeMaterializationWork, WorkCounters};

/// Trusted self-host enrollment loaded by the local owner, never a guest
/// request. Cache behavior is chosen by the transport admission itself.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalGuestEnrollment {
    /// Exact volume in Bound's canonical filesystem host.
    pub volume: crate::conversation::VolumeRef,
    /// Source-owned POSIX identities and permitted metadata operations.
    pub posix: GuestPosixPolicy,
    /// Permit authored effects, subject to persisted volume policy.
    pub writable: bool,
}

/// Original verified selection passed only to the trusted physical custodian.
/// It is borrowed, nonserializable, and carries no replacement signing key.
pub struct GuestAuthority<'a> {
    /// Original Bound verifier, including its authenticated audience.
    pub verifier: &'a crate::core::AuthorityVerifier,
    /// Original caller's signed scope and ancestry.
    pub scope: &'a Scope,
    /// Exact canonical selected volume; public requests carry this reference.
    pub volume: &'a crate::conversation::VolumeRef,
    /// Requested authored effects, still bounded by scope and persisted policy.
    pub writable: bool,
    /// Authenticated half-open Unix expiry, including every ancestor.
    pub expires_at: u64,
}

/// Trusted server-side factory for the SAME account/Machine/incarnation's
/// physical fence. Implementors compare the original authority and selection
/// with their held custodian; a serialized Machine ID is not a custodian.
pub trait GuestMappingSource: Send + 'static {
    /// Consumes the held physical owner under the original verified selection.
    /// Refuses account/incarnation mismatch without disclosing volume contents.
    fn bind(
        self: Box<Self>,
        authority: GuestAuthority<'_>,
        work: &NativeMaterializationWork,
    ) -> crate::Result<Box<dyn GuestMappingFence>>;
}

struct GuestAuthorityLifetime {
    cancellation: CancellationToken,
    task: tokio::task::JoinHandle<()>,
    _alarm: Option<acyclic_native_runtime::WallAlarm>,
}

struct CancelGuest(CancellationToken);

impl Drop for CancelGuest {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

impl GuestAuthorityLifetime {
    fn new(
        issuer: &crate::core::AuthorityRoot,
        verifier: &crate::core::AuthorityVerifier,
        scope: &Scope,
        expires_at: u64,
        work: &NativeMaterializationWork,
    ) -> crate::Result<Self> {
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| {
            crate::Error::Unsupported("guest authority requires an event runtime".into())
        })?;
        let io = PreparationIo::from_native_work(work)?;
        let verified = verifier.authenticate_scope(scope, &mut Some(&mut io.history_io()))?;
        let owned_bytes = verified.logical_owned_bytes()?;
        // Subscribe before checking: revocation cannot fall between the check
        // and event registration. Each wake is an actual trust publication.
        let mut revisions = issuer.trust_revisions();
        let cancellation = work.cancellation().clone();
        let cancelled = work.cancellation_wait()?;
        let original = work.clone();
        let stop = CancelGuest(cancellation.clone());
        let future = async move {
            tokio::pin!(cancelled);
            loop {
                let io = match PreparationIo::from_native_work(&original) {
                    Ok(io) => io,
                    Err(_) => {
                        stop.0.cancel();
                        return;
                    }
                };
                if verified.recheck(&mut Some(&mut io.history_io())).is_err() {
                    stop.0.cancel();
                    return;
                }
                tokio::select! {
                    _ = &mut cancelled => return,
                    changed = revisions.changed() => {
                        if changed.is_err() { stop.0.cancel(); return; }
                    }
                }
            }
        };
        let frame = work.claim(
            owned_bytes
                .checked_add(std::mem::size_of_val(&future) as u64)
                .ok_or_else(|| {
                    crate::Error::Invalid("guest authority ownership overflow".into())
                })?,
            1,
            WorkCounters::UNCHARGED,
        )?;
        let alarm = if expires_at == u64::MAX {
            None
        } else {
            let stop = cancellation.clone();
            let alarm_custody = work.claim(
                (std::mem::size_of_val(&stop)
                    + std::mem::size_of::<NativeAllocation>()
                    + 2 * std::mem::size_of::<usize>()) as u64,
                1,
                WorkCounters::UNCHARGED,
            )?;
            let alarm = acyclic_native_runtime::WallAlarm::register(
                expires_at.saturating_mul(1000),
                Arc::new(move || {
                    let _held = &alarm_custody;
                    stop.cancel();
                }),
            )
            .map_err(|error| {
                crate::Error::Storage(format!("guest expiry registration: {error}"))
            })?;
            Some(alarm)
        };
        let task = runtime.spawn(async move {
            let _frame = frame;
            future.await;
        });
        Ok(Self {
            cancellation,
            task,
            _alarm: alarm,
        })
    }
}

impl Drop for GuestAuthorityLifetime {
    fn drop(&mut self) {
        self.cancellation.cancel();
        self.task.abort();
    }
}

struct EnrolledSource {
    source: Arc<dyn MountFilesystem>,
    _volume: crate::conversation::VolumeRef,
    posix: GuestPosixPolicy,
    writable: bool,
    _authority: Option<GuestAuthorityLifetime>,
    _custody: NativeAllocation,
}

impl GuestSource for EnrolledSource {
    fn filesystem(&self) -> &Arc<dyn MountFilesystem> {
        &self.source
    }
    fn posix(&self) -> &GuestPosixPolicy {
        &self.posix
    }
    fn writable(&self) -> bool {
        self.writable
    }
}

impl<P, O> Bound<P, O>
where
    P: AuthorityProvider + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Admits the exact canonical volume under this binding's original caller.
    /// `volume` is the public selection. Identity policy and the nonserialized
    /// physical owner are supplied only by the trusted server composition.
    /// A hosted caller without a physical owner is refused before volume IO.
    pub async fn guest_session(
        &self,
        volume: crate::conversation::VolumeRef,
        posix: GuestPosixPolicy,
        writable: bool,
        mappings: Option<Box<dyn GuestMappingSource>>,
        work: &NativeMaterializationWork,
    ) -> crate::Result<GuestSessionAdmission> {
        if matches!(self.admission, TurnAdmission::Admitted { .. }) && mappings.is_none() {
            return Err(crate::Error::Unsupported(
                "guest transport requires the original hosted mapping fence".into(),
            ));
        }
        self.volumes.validate_provider(volume.provider())?;
        if posix.identities.is_empty() {
            return Err(crate::Error::Unauthorized(
                "guest identity map is empty".into(),
            ));
        }
        let io = PreparationIo::from_native_work(work)?;
        volume.validate(&mut Some(&mut io.history_io()))?;
        let (authority, lifetime, mappings) = match &self.admission {
            TurnAdmission::LocalRoot => {
                if mappings.is_some() {
                    return Err(crate::Error::Unauthorized(
                        "hosted mapping needs a verified caller".into(),
                    ));
                }
                (None, None, None)
            }
            TurnAdmission::Admitted { audience, scope } => {
                let issuer = self.issuer.with_audience(audience.clone());
                let verifier = issuer.verifier(&mut Some(&mut io.history_io()))?;
                let expires_at =
                    verifier.scope_expiry(audience, scope, &mut Some(&mut io.history_io()))?;
                for operation in [VolumeOperation::Read]
                    .into_iter()
                    .chain(writable.then_some(VolumeOperation::Write))
                {
                    ContentGrant::authorize_volume(
                        &verifier,
                        scope,
                        &volume,
                        operation,
                        self.maximum_bytes,
                        &mut Some(&mut io.history_io()),
                    )?;
                }
                let physical = mappings
                    .ok_or_else(|| {
                        crate::Error::Unsupported("guest mapping owner is absent".into())
                    })?
                    .bind(
                        GuestAuthority {
                            verifier: &verifier,
                            scope,
                            volume: &volume,
                            writable,
                            expires_at,
                        },
                        work,
                    )?;
                let mappings = GuestMappingAdmission::new(physical, work)?;
                let lifetime =
                    GuestAuthorityLifetime::new(&issuer, &verifier, scope, expires_at, work)?;
                let authority = NativeMountAuthority::admitted(
                    verifier,
                    scope.copy(&mut Some(&mut io.history_io()))?,
                );
                (Some(authority), Some(lifetime), Some(mappings))
            }
        };
        let bytes =
            (std::mem::size_of::<EnrolledSource>() + 2 * std::mem::size_of::<usize>()) as u64;
        let bytes = bytes
            .checked_add(volume.logical_owned_bytes()?)
            .and_then(|bytes| {
                bytes.checked_add(
                    (posix.identities.capacity() * std::mem::size_of::<(u32, u32)>()) as u64,
                )
            })
            .ok_or(crate::Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "guest enrollment ownership overflow",
            })?;
        let custody = work.claim(bytes, 1, WorkCounters::UNCHARGED)?;
        let name = volume.storage_name(&mut Some(&mut io.history_io()))?;
        let reference = crate::resources::WorkspaceRef::from_resource(
            crate::resources::ResourceRef::new(
                crate::resources::ResourceKind::Workspace,
                volume.provider().clone(),
                name.into_bytes(),
                None,
                &mut Some(&mut io.history_io()),
            )?,
            &mut Some(&mut io.history_io()),
        )?;
        let workspace = self.volumes.open(&reference, Some(&io)).await?;
        let options = match authority {
            Some(authority) => {
                authority
                    .mount_options(&workspace, Some(&volume), writable, work)
                    .await?
            }
            None => owner_mount_options(&workspace, &self.issuer, None, writable, work).await?,
        };
        let source = workspace.native_source(&options).await?;
        GuestSessionAdmission::admit(
            Arc::new(EnrolledSource {
                source,
                _volume: volume,
                posix,
                writable,
                _authority: lifetime,
                _custody: custody,
            }),
            mappings,
        )
        .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn idle_guest_revocation_and_monitor_loss_cancel_original_work() -> crate::Result<()> {
        acyclic_native_runtime::initialize_native_io_services()
            .map_err(|error| crate::Error::Storage(error.to_string()))?;
        let issuer = crate::core::AuthorityRoot::new(
            "guest",
            [71; 32],
            crate::core::Authority {
                kind: crate::core::AggregateKind::Task,
                id: "guest".into(),
            },
        );
        let scope =
            issuer.bootstrap_scope("guest", &crate::Capabilities::default(), None, &mut None)?;
        let verifier = issuer.verifier(&mut None)?;
        let cancellation = CancellationToken::new();
        let work =
            NativeMaterializationWork::new(acyclic_fs::performance::BOUNDARY_BUDGET, &cancellation)
                .map_err(|(cause, work)| crate::Error::NativeConstruction { cause, work })?;
        let lifetime = GuestAuthorityLifetime::new(
            &issuer,
            &verifier,
            &scope,
            verifier.expiry(&scope)?,
            &work,
        )?;
        assert!(cancellation.check().is_ok());
        let cancelled = work.cancellation_wait()?;
        issuer.revoke_workload(&scope)?;
        cancelled.await;
        assert!(cancellation.check().is_err());
        assert_eq!(work.receipt().backend_read_operations, 0);
        assert_eq!(work.receipt().backend_write_operations, 0);
        drop(lifetime);

        let scope = issuer.bootstrap_scope(
            "replacement",
            &crate::Capabilities::default(),
            None,
            &mut None,
        )?;
        let cancellation = CancellationToken::new();
        let replacement =
            NativeMaterializationWork::new(acyclic_fs::performance::BOUNDARY_BUDGET, &cancellation)
                .map_err(|(cause, work)| crate::Error::NativeConstruction { cause, work })?;
        let lifetime = GuestAuthorityLifetime::new(
            &issuer,
            &verifier,
            &scope,
            verifier.expiry(&scope)?,
            &replacement,
        )?;
        let cancelled = replacement.cancellation_wait()?;
        lifetime.task.abort();
        cancelled.await;
        assert!(cancellation.check().is_err());
        Ok(())
    }
}
