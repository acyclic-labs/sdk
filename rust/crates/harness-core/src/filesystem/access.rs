use super::*;
use acyclic_fs::access::{Action, Capability, Context, PathTrie, Principal, PrincipalKind};
use acyclic_fs::model::{CheckoutMode, GenerationSelector};
use acyclic_stream::UnixMillisClock as _;
use std::marker::PhantomData;

/// Explicit initial policy: signed capabilities supply the authority ceiling.
/// Reads never synthesize this record when it is missing from an existing volume.
pub use acyclic_fs::access::INITIAL_POLICY;
mod read_commit;
pub(in crate::filesystem) use read_commit::ReadObservation;
pub(in crate::filesystem) use read_commit::require_current;
pub(in crate::filesystem) type CompiledAccess<'a> =
    (PathTrie, [Option<DecodeReservation<'a>>; 4], GenerationId);

pub(crate) fn capability_payload<'a>(
    label: Option<&'a str>,
    operation: VolumeOperation,
    io: &PreparationIo,
) -> Result<&'a [u8]> {
    match label {
        Some(label) => label
            .strip_prefix(acyclic_fs::access::SCOPE_PREFIX)
            .map(str::as_bytes)
            .ok_or_else(|| io.invalid("filesystem capability label is invalid")),
        None => match operation {
            VolumeOperation::Read => {
                whole_volume_payload(acyclic_fs::access::ActionSet::SourceRead)
            }
            VolumeOperation::Write => {
                whole_volume_payload(acyclic_fs::access::ActionSet::DestinationWrite)
            }
        },
    }
}

/// Fixed host ceilings are encoded by the same typed builder as signed scopes.
/// This bounded immutable vocabulary has one process-wide encoding per kind.
pub(in crate::filesystem) fn whole_volume_payload(
    actions: acyclic_fs::access::ActionSet,
) -> Result<&'static [u8]> {
    use acyclic_fs::access::ActionSet;
    use std::sync::OnceLock;
    static READ: OnceLock<std::result::Result<Vec<u8>, serde_json::Error>> = OnceLock::new();
    static WRITE: OnceLock<std::result::Result<Vec<u8>, serde_json::Error>> = OnceLock::new();
    static MOUNT_READ: OnceLock<std::result::Result<Vec<u8>, serde_json::Error>> = OnceLock::new();
    static MOUNT_WRITE: OnceLock<std::result::Result<Vec<u8>, serde_json::Error>> = OnceLock::new();
    static OWNER: OnceLock<std::result::Result<Vec<u8>, serde_json::Error>> = OnceLock::new();
    static METADATA: OnceLock<std::result::Result<Vec<u8>, serde_json::Error>> = OnceLock::new();
    let cell = match actions {
        ActionSet::SourceRead => &READ,
        ActionSet::DestinationWrite => &WRITE,
        ActionSet::MountRead => &MOUNT_READ,
        ActionSet::MountWrite => &MOUNT_WRITE,
        ActionSet::Owner => &OWNER,
        ActionSet::Metadata => &METADATA,
    };
    cell.get_or_init(|| serde_json::to_vec(&Capability::whole_volume(None, actions.actions())))
        .as_ref()
        .map(Vec::as_slice)
        .map_err(|_| {
            Error::FilesystemAccessConfiguration("whole-volume capability encoding failed")
        })
}

pub(crate) fn access_now() -> Result<u64> {
    acyclic_fs::access::unix_seconds(acyclic_stream::SystemUnixMillisClock.now_unix_millis())
        .map_err(|_| Error::FilesystemAccessConfiguration("access clock is unavailable"))
}

pub(crate) fn require_unrestricted(
    view: &PathTrie,
    actions: &[Action],
    surface: &'static str,
) -> Result<()> {
    if view.allows_every_path(actions, access_now()?) {
        Ok(())
    } else {
        Err(Error::FilesystemAccessConfiguration(surface))
    }
}

pub(in crate::filesystem) fn require(
    view: &PathTrie,
    action: Action,
    path: &str,
    io: &PreparationIo,
) -> Result<()> {
    if view.decide(action, path, access_now()?).allowed {
        Ok(())
    } else {
        Err(stock_boundary(
            Some(io),
            crate::wire::ErrorCode::Unauthorized,
            "filesystem access decision denied request",
        ))
    }
}

pub(in crate::filesystem) fn require_directory_ceiling(
    ceiling: Option<&crate::Capabilities>,
    volume: &VolumeRef,
    path: &str,
    io: Option<&PreparationIo>,
) -> Result<()> {
    if let Some(grants) = ceiling
        && !crate::executor::with_history_io(io, |work| {
            crate::core::directory_read_granted_with_work(grants, volume, path, work)
        })?
    {
        return Err(stock_boundary(
            io,
            crate::wire::ErrorCode::Unauthorized,
            "content caller lacks a filesystem directory grant",
        ));
    }
    Ok(())
}

fn record<'a, T: PreparationRecord>(
    io: &'a PreparationIo,
    bytes: &[u8],
    code: crate::wire::ErrorCode,
) -> Result<(T, DecodeReservation<'a>)> {
    if bytes.len() as u64 > acyclic_fs::access::MAXIMUM_POLICY_RECORD_BYTES {
        return Err(stock_boundary(
            Some(io),
            code,
            "filesystem access record exceeds bounds",
        ));
    }
    io.decode(PhantomData::<T>, bytes, code)
}

#[cfg(any(test, feature = "test-support"))]
mod verification;
#[cfg(feature = "test-support")]
pub use verification::check_access_record;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    pub(in crate::filesystem) async fn read_selection_with_access<'a>(
        &self,
        scope: &Scope,
        reference: &'a FileRef,
        maximum_bytes: u64,
        inherited_reference: bool,
        ceiling: Option<&crate::Capabilities>,
        io: &'a PreparationIo,
    ) -> Result<ReadObservation<'a, A, O>> {
        reference.validate(&mut Some(&mut io.history_io()))?;
        if reference.volume().provider() != &self.provider
            || reference.descriptor().byte_length() > maximum_bytes
        {
            return Err(io.invalid("content provider or size limit mismatch"));
        }
        if let Some(grants) = ceiling
            && !crate::runtime::read_granted_with_work(
                grants,
                reference,
                &mut Some(&mut io.history_io()),
            )?
        {
            return Err(stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Unauthorized,
                "content caller lacks a filesystem read grant",
            ));
        }
        let label = crate::core::filesystem_scope_label(
            scope.capabilities(),
            &mut Some(&mut io.history_io()),
        )?;
        // A legacy grant is authority for this exact request only. A structured
        // payload is always its ceiling, even when legacy labels are also present.
        if label.is_none() && !inherited_reference {
            super::content_verifier::require_file_read(scope, reference, io)?;
        }
        let workspace_ref =
            bounded_io::content_workspace(&self.provider, reference.volume(), Some(io))?;
        let workspace_hold = io.guard_owned(workspace_ref.logical_owned_bytes()?)?;
        let workspace_ref = (workspace_ref, workspace_hold);
        let workspace = self.open(&workspace_ref.0, Some(io)).await?;
        let generation = bounded_io::generation_version(reference.version(), Some(io))?;
        let selected = self.select_generation(&workspace, generation, io).await?;
        let compiled = crate::filesystem::access::access_view(
            &workspace,
            scope,
            Some(generation),
            capability_payload(label, VolumeOperation::Read, io)?,
            ceiling,
            io,
        )
        .await?;
        require(&compiled.0, Action::Read, reference.path(), io)?;
        let mut observation =
            ReadObservation::new(workspace.clone(), selected, compiled, reference, io);
        if observation.data_workspace_id() != workspace.id() {
            observation.authorize_data(scope, ceiling).await?;
        }
        Ok(observation)
    }
    pub(in crate::filesystem) async fn read_file_with_access(
        &self,
        scope: &Scope,
        reference: &FileRef,
        maximum_bytes: u64,
        inherited_reference: bool,
        ceiling: Option<&crate::Capabilities>,
        io: &PreparationIo,
    ) -> Result<acyclic_stream::OwnedBytes> {
        self.read_selection_with_access(
            scope,
            reference,
            maximum_bytes,
            inherited_reference,
            ceiling,
            io,
        )
        .await?
        .read_file(maximum_bytes)
        .await
    }
}

// Scope is immutable and authenticated by the owning verifier/grant constructor.
// The caller checks its request-specific legacy authority before using this view.
pub(crate) async fn access_view<'a, A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    scope: &Scope,
    generation: Option<GenerationId>,
    payload: &[u8],
    ceiling: Option<&crate::Capabilities>,
    io: &'a PreparationIo,
) -> Result<CompiledAccess<'a>> {
    crate::conversation::BoundedContentFuture::new(Some(io), || async move {
        let capability = intersect_capabilities(payload, ceiling, io)?;
        let head = io.sdk(
            workspace
                .checkout(
                    GenerationSelector::Head,
                    CheckoutMode::read_only_pinned(),
                    io,
                    io.cancellation(),
                )
                .await,
        )?;
        let head_hold = if io.original_native_work().is_none() {
            Some(io.reserve_decode(head.logical_owned_bytes())?)
        } else {
            None
        };
        let mut head = (head, head_hold);
        let policy_generation = head.0.generation_id();
        let principal = authenticated_principal(scope, io)?;
        let view = io.sdk(
            head.0
                .compile_access(
                    acyclic_fs::access::AccessBinding {
                        principal: &principal.0,
                        capability: &capability.0,
                        facts: acyclic_fs::access::AuthenticatedFacts::Identity,
                        generation,
                        context: Context {
                            now: access_now()?,
                            approvals: 0,
                        },
                    },
                    io,
                    io.cancellation(),
                )
                .await,
        )?;
        let bytes = view.logical_owned_bytes();
        let hold = if io.original_native_work().is_some() {
            io.guard_owned(bytes)?
        } else {
            io.reserve_decode(bytes)?
        };
        Ok((view, [Some(hold), None, None, None], policy_generation))
    })?
    .await
}

mod writes;

fn intersect_capabilities<'a>(
    payload: &[u8],
    ceiling: Option<&crate::Capabilities>,
    io: &'a PreparationIo,
) -> Result<(Capability, [Option<DecodeReservation<'a>>; 3])> {
    let (capability, hold) =
        record::<Capability>(io, payload, crate::wire::ErrorCode::Unauthorized)?;
    let mut capability = (capability, [Some(hold), None, None]);
    let caller = ceiling
        .map(|grants| crate::core::filesystem_scope_label(grants, &mut Some(&mut io.history_io())))
        .transpose()?
        .flatten();
    let caller = caller
        .map(|label| capability_payload(Some(label), VolumeOperation::Read, io))
        .transpose()?;
    if let Some(caller) = caller.filter(|caller| *caller != payload) {
        let (mut additional, hold) =
            record::<Capability>(io, caller, crate::wire::ErrorCode::Unauthorized)?;
        capability.1[1] = Some(hold);
        let empty = capability.0.layers.is_empty() || additional.layers.is_empty();
        let count = capability
            .0
            .layers
            .len()
            .checked_add(additional.layers.len())
            .and_then(|count| count.checked_add(usize::from(empty)))
            .ok_or_else(|| io.invalid("filesystem ceiling layer count overflow"))?;
        let mut history = io.history_io();
        let mut work = Some(&mut history as &mut dyn crate::store::HistoryIo);
        let mut quota = crate::wire_codec::CodecQuota::new(&mut work, 0)?;
        let mut layers = crate::core::reserve_copy_vector(count, &mut quota)?;
        let hold = io.guard_owned(quota.retained())?;
        quota.transfer();
        capability.1[2] = Some(hold);
        layers.append(&mut capability.0.layers);
        layers.append(&mut additional.layers);
        if empty {
            layers.push(Vec::new());
        }
        capability.0.layers = layers;
        capability.0.expires_at = match (capability.0.expires_at, additional.expires_at) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (left, right) => left.or(right),
        };
    }
    Ok(capability)
}

fn authenticated_principal<'a>(
    scope: &Scope,
    io: &'a PreparationIo,
) -> Result<(Principal, DecodeReservation<'a>)> {
    let subject = crate::executor::with_history_io(Some(io), |work| match scope.agent() {
        Some(agent) => crate::executor::preparation_format(format_args!("{agent}"), work),
        None => crate::executor::preparation_format(format_args!("{}", scope.issuer()), work),
    })?;
    let subject_hold = io.guard_owned(subject.capacity() as u64)?;
    let principal = (
        Principal {
            kind: if scope.agent().is_some() {
                PrincipalKind::Agent
            } else {
                PrincipalKind::Service
            },
            id: subject,
            lineage: vec![],
            groups: Default::default(),
            roles: Default::default(),
            tags: Default::default(),
            attributes: Default::default(),
        },
        subject_hold,
    );
    Ok(principal)
}
