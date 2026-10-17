use super::*;
use acyclic_fs::access::{
    Action, Capability, Context, InvalidPolicy, PathTrie, Policy, Principal, PrincipalKind,
    Resource,
};
use acyclic_fs::model::{CheckoutMode, GenerationSelector};
use acyclic_stream::UnixMillisClock as _;
use std::marker::PhantomData;

/// Explicit initial policy: signed capabilities supply the authority ceiling.
/// Reads never synthesize this record when it is missing from an existing volume.
pub use acyclic_fs::access::INITIAL_POLICY;
const LEGACY_READ: &[u8] = include_bytes!("access/legacy-read.json");
const LEGACY_WRITE: &[u8] = include_bytes!("access/legacy-write.json");
mod read_commit;
pub(in crate::filesystem) use read_commit::ReadObservation;
pub(in crate::filesystem) use read_commit::require_current;
pub(in crate::filesystem) type CompiledAccess<'a> =
    (PathTrie, [Option<DecodeReservation<'a>>; 2], GenerationId);

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
        None => Ok(match operation {
            VolumeOperation::Read => LEGACY_READ,
            VolumeOperation::Write => LEGACY_WRITE,
        }),
    }
}

pub(crate) fn access_now() -> u64 {
    acyclic_stream::SystemUnixMillisClock.now_unix_millis() / 1000
}

pub(crate) fn require_unrestricted(
    view: &PathTrie,
    actions: &[Action],
    surface: &'static str,
) -> Result<()> {
    if view.allows_every_path(actions, access_now()) {
        Ok(())
    } else {
        Err(Error::FilesystemAccessConfiguration(surface))
    }
}

fn require_authenticated_facts(policy: &Policy, io: &PreparationIo) -> Result<()> {
    for rule in policy.layers.iter().flatten() {
        io.charge(WorkCounters {
            items_examined: 1,
            ..Default::default()
        })?;
        if rule.principal.group.is_some()
            || !rule.principal.attributes.is_empty()
            || rule.conditions.role.is_some()
            || rule.conditions.tag.is_some()
            || rule.conditions.relation.is_some()
            || rule.resource.lineage_root.is_some()
        {
            return Err(Error::FilesystemAccessConfiguration(
                "unauthenticated principal facts",
            ));
        }
    }
    Ok(())
}

pub(in crate::filesystem) fn require(
    view: &PathTrie,
    action: Action,
    path: &str,
    io: &PreparationIo,
) -> Result<()> {
    if view.decide(action, path, access_now()).allowed {
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
    if let Some(grants) = ceiling {
        if !crate::executor::with_history_io(io, |work| {
            crate::core::directory_read_granted_with_work(grants, volume, path, work)
        })? {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "content caller lacks a filesystem directory grant",
            ));
        }
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
            || maximum_bytes == 0
            || reference.descriptor().byte_length() > maximum_bytes
        {
            return Err(io.invalid("content provider or size limit mismatch"));
        }
        if let Some(grants) = ceiling {
            if !crate::runtime::read_granted_with_work(
                grants,
                reference,
                &mut Some(&mut io.history_io()),
            )? {
                return Err(stock_boundary(
                    Some(io),
                    crate::wire::ErrorCode::Unauthorized,
                    "content caller lacks a filesystem read grant",
                ));
            }
        }
        let label = crate::core::filesystem_scope_label(
            scope.capabilities(),
            &mut Some(&mut io.history_io()),
        )?;
        // A legacy grant is authority for this exact request only. A structured
        // payload is always its ceiling, even when legacy labels are also present.
        if label.is_none() && !inherited_reference {
            super::require_file_read(scope, reference, io)?;
        }
        let workspace_ref =
            fork_preparer_io::content_workspace(&self.provider, reference.volume(), Some(io))?;
        let workspace_hold = io.guard_owned(workspace_ref.logical_owned_bytes()?)?;
        let workspace_ref = (workspace_ref, workspace_hold);
        let workspace = self.open(&workspace_ref.0, Some(io)).await?;
        let mut generation = [0; 32];
        io.charge(WorkCounters {
            source_bytes_read: reference.version().len() as u64,
            items_examined: reference.version().len() as u64,
            ..Default::default()
        })?;
        hex::decode_to_slice(reference.version(), &mut generation)
            .map_err(|_| io.invalid("file version is not a Filesystem generation"))?;
        let generation = GenerationId::new(Digest::from_bytes(generation));
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
        let generation = io.sdk(
            workspace
                .generation(generation, io, io.cancellation())
                .await,
        )?;
        fork_preparer_io::retain(&generation, Some(io)).await?;
        Ok(ReadObservation::new(
            workspace, generation, compiled, reference, io,
        ))
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
        let (capability, hold) =
            record::<Capability>(io, payload, crate::wire::ErrorCode::Unauthorized)?;
        let mut capability = (capability, [Some(hold), None, None]);
        let caller = ceiling
            .map(|grants| {
                crate::core::filesystem_scope_label(grants, &mut Some(&mut io.history_io()))
            })
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
        let (bytes, policy_revision) =
            io.sdk(head.0.read_access_policy(io, io.cancellation()).await)?;
        let bytes = bytes.ok_or_else(|| {
            stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Unauthorized,
                "volume has no access policy",
            )
        })?;
        if io.original_native_work().is_none() {
            io.retain_owned(bytes.logical_owned_bytes)?;
        }
        let bytes_hold = io.guard_bytes(&bytes.bytes)?;
        let bytes = (bytes, bytes_hold);
        let policy = record::<Policy>(io, &bytes.0.bytes, crate::wire::ErrorCode::Storage)?;
        require_authenticated_facts(&policy.0, io)?;
        let policy_generation = head.0.generation_id();
        let (selected, latest) = io.sdk(
            head.0
                .generation_position(
                    generation.unwrap_or(policy_generation),
                    io,
                    io.cancellation(),
                )
                .await,
        )?;
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
        let volume = crate::executor::with_history_io(Some(io), |work| {
            crate::executor::preparation_format(
                format_args!(
                    "{}",
                    uuid::Uuid::from_bytes(head.0.volume_id().into_bytes())
                ),
                work,
            )
        })?;
        let volume_hold = io.guard_owned(volume.capacity() as u64)?;
        let volume = (volume, volume_hold);
        let context = Context {
            now: access_now(),
            approvals: 0,
        };
        let resource = Resource {
            config: head.0.volume_config(),
            lineage: &[],
            volume: &volume.0,
            generation: selected.get(),
            head: latest.get(),
            path: "",
        };
        let mut remaining = usize::try_from(io.remaining()?.items_examined).unwrap_or(usize::MAX);
        let before = remaining;
        let mut holds = [None, None];
        let mut stage = 0;
        let mut admission_error = None;
        let compiled = PathTrie::compile_with_work(
            &policy.0,
            &principal.0,
            resource,
            context,
            &capability.0,
            &mut remaining,
            |bytes| match io.reserve_decode(bytes as u64) {
                Ok(hold) => {
                    *holds
                        .get_mut(stage)
                        .ok_or(InvalidPolicy("invalid compilation stage"))? = Some(hold);
                    stage += 1;
                    Ok(())
                }
                Err(error) => {
                    admission_error = Some(error);
                    Err(InvalidPolicy("compilation storage was not admitted"))
                }
            },
        );
        io.charge(WorkCounters {
            items_examined: (before - remaining) as u64,
            ..Default::default()
        })?;
        if let Some(error) = admission_error {
            return Err(error);
        }
        let mut compiled = (
            compiled.map_err(|_| {
                stock_boundary(
                    Some(io),
                    crate::wire::ErrorCode::Unauthorized,
                    "filesystem policy compilation refused request",
                )
            })?,
            holds,
        );
        let mut retained = compiled.0.logical_owned_bytes();
        for hold in compiled.1.iter_mut().flatten() {
            let bytes = retained.min(hold.bytes());
            hold.substitute(bytes)?;
            retained -= bytes;
        }
        if retained != 0 {
            return Err(io.invalid("compiled access storage exceeds admission"));
        }
        drop(volume);
        drop(principal);
        drop(policy);
        drop(capability);
        drop(bytes);
        drop(head);
        let view = compiled
            .0
            .with_policy_revision(policy_generation, policy_revision);
        let view = match generation {
            Some(selected) => view.with_data_generation(selected),
            None => view,
        };
        Ok((view, compiled.1, policy_generation))
    })?
    .await
}

mod writes;
