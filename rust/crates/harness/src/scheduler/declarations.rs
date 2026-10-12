use super::*;
use crate::wire_codec::CodecQuota;
use physical::{add, claim_slots, closed_clone, copy_text, resources_bytes, slots};

/// Pinned resumable entrypoint and result schema.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct EntrypointRef {
    /// Namespaced entrypoint name.
    pub name: String,
    /// Implementation version.
    pub version: String,
    /// Implementation and schema digest.
    pub digest: [u8; 32],
    /// Durable result schema.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmModelJsonSchema"))]
    pub result_schema: Value,
}

/// Provider-neutral logical resource quantities.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(
    feature = "wasm",
    tsify(large_number_types_as_bigints, hashmap_as_object)
)]
pub struct ResourceRequest(pub BTreeMap<String, u64>);
/// Immutable root-session ceilings shared by descendants.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionLimits {
    /// Maximum simultaneously reserved tasks.
    pub active_tasks: u64,
    /// Maximum retained tasks, including terminal tasks.
    pub total_tasks: u64,
    /// Maximum parent edges from the root.
    pub depth: u32,
    /// Maximum distinct admitted model attempts.
    pub model_steps: u64,
}
impl SessionLimits {
    /// Validates finite portable session ceilings.
    pub fn validate(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<()> {
        observe(work)?;
        if self.active_tasks == 0
            || self.total_tasks == 0
            || self.model_steps == 0
            || self.active_tasks > self.total_tasks
            || [self.active_tasks, self.total_tasks, self.model_steps]
                .iter()
                .any(|n| *n > crate::conversation::MAX_EXACT_JS_INTEGER)
        {
            return Err(diagnostic(work, Invalid, "session limits are invalid"));
        }
        Ok(())
    }
}

/// Lifetime owner of durable work.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DurableOwner {
    /// Structurally owned child.
    Attached {
        /// Owning aggregate.
        authority: Authority,
    },
    /// Work surviving its initiating call or session.
    Detached {
        /// Durable aggregate owner.
        authority: Authority,
    },
}
impl DurableOwner {
    pub(crate) fn authority(&self) -> &Authority {
        match self {
            Self::Attached { authority } | Self::Detached { authority } => authority,
        }
    }
}

/// Structured parent and stable logical child slot.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ParentLink {
    /// Parent operation.
    #[cfg_attr(feature = "wasm", tsify(type = "string"))]
    pub operation_id: OperationId,
    /// Stable slot, independent of observation order.
    pub slot: String,
}

/// Durable structured-concurrency policy.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Orchestration {
    /// Executor supplies the result.
    Leaf,
    /// All children must complete.
    Join,
    /// First observed successful child wins.
    Race,
    /// Require a number of successful children.
    Quorum {
        /// Required successes.
        required: u32,
    },
    /// Reduce ordered child values with a pinned implementation.
    Reduce {
        /// Reducer entrypoint.
        reducer: EntrypointRef,
    },
}

/// Immutable declaration of one durable operation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(hashmap_as_object))]
pub struct OperationSpec {
    /// Stable operation identity.
    #[cfg_attr(feature = "wasm", tsify(type = "string"))]
    pub operation_id: OperationId,
    /// Optional structured parent.
    #[cfg_attr(feature = "wasm", tsify(type = "ParentLink | null"))]
    pub parent: Option<ParentLink>,
    /// Lifetime owner.
    pub owner: DurableOwner,
    /// Original signed owner grant.
    pub owner_scope: crate::core::Scope,
    /// Restartable implementation.
    pub entrypoint: EntrypointRef,
    /// Dependencies that must succeed before admission.
    #[cfg_attr(feature = "wasm", tsify(type = "string[]"))]
    pub dependencies: BTreeSet<OperationId>,
    /// Required logical capacity.
    pub resources: ResourceRequest,
    /// Provider-neutral placement labels.
    pub placement: BTreeMap<String, String>,
    /// Structured-concurrency policy.
    pub orchestration: Orchestration,
    /// Pinned initial state.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
    pub state: FileRef,
}
impl OperationSpec {
    pub(crate) fn verify_owner(
        &self,
        owner: &Authority,
        verifier: &crate::core::AuthorityVerifier,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
        if self.owner.authority() != owner {
            return Err(diagnostic(
                work,
                Unauthorized,
                "declaration belongs to another owner",
            ));
        }
        verifier.verify_audience(owner, work)?;
        verifier.verify_scope_for_with_work(owner, &self.owner_scope, work)?;
        if !self
            .owner_scope
            .capabilities()
            .contains("operation:declare")
        {
            return Err(diagnostic(
                work,
                Unauthorized,
                "original owner scope lacks operation:declare",
            ));
        }
        Ok(())
    }
}

/// Pinned logical resource allocation and worker placement.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct Reservation {
    /// Provider lease identity.
    pub id: String,
    /// Worker or placement identity.
    pub placement: String,
    /// Actually admitted resources, including partial admission.
    pub admitted: ResourceRequest,
}
/// Exact durable reservation fence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LeaseFence {
    /// Provider lease identity.
    pub reservation_id: String,
    /// Worker or placement owning the lease.
    pub placement: String,
}
impl From<&Reservation> for LeaseFence {
    fn from(reservation: &Reservation) -> Self {
        Self {
            reservation_id: reservation.id.clone(),
            placement: reservation.placement.clone(),
        }
    }
}

impl EntrypointRef {
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        add(
            add(self.name.capacity() as u64, self.version.capacity() as u64)?,
            crate::contract::json_value_logical_owned_bytes(&self.result_schema)?,
        )
    }
    pub(super) fn copy_fields(&self, quota: &mut CodecQuota<'_, '_>) -> Result<Self> {
        let result_schema = crate::core::copy_value(&self.result_schema, quota)?;
        Ok(Self {
            name: copy_text(&self.name, quota)?,
            version: copy_text(&self.version, quota)?,
            digest: self.digest,
            result_schema,
        })
    }
}
impl LeaseFence {
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        add(
            self.reservation_id.capacity() as u64,
            self.placement.capacity() as u64,
        )
    }
    pub(super) fn copy_fields(&self, quota: &mut CodecQuota<'_, '_>) -> Result<Self> {
        observe(quota.work())?;
        Ok(Self {
            reservation_id: copy_text(&self.reservation_id, quota)?,
            placement: copy_text(&self.placement, quota)?,
        })
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn clone_with_native_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        let mut io =
            crate::store::history::native_work_io(work).map_err(|cause| Error::NativeWork {
                cause,
                custody: None,
            })?;
        io.admit_owned(std::mem::size_of::<Self>() as u64)?;
        io.retain_owned(std::mem::size_of::<Self>() as u64)?;
        let fence = self.clone_with_work(&mut Some(&mut io))?;
        Ok((fence, io.into_custody()))
    }
}
impl Reservation {
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        add(
            add(self.id.capacity() as u64, self.placement.capacity() as u64)?,
            resources_bytes(&self.admitted.0)?,
        )
    }
    pub(super) fn copy_fields(&self, quota: &mut CodecQuota<'_, '_>) -> Result<Self> {
        observe(quota.work())?;
        Ok(Self {
            id: copy_text(&self.id, quota)?,
            placement: copy_text(&self.placement, quota)?,
            admitted: ResourceRequest(copy_resources(&self.admitted.0, quota)?),
        })
    }
}
impl OperationSpec {
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        let parent = self
            .parent
            .as_ref()
            .map_or(0, |parent| parent.slot.capacity() as u64);
        let reducer = match &self.orchestration {
            Orchestration::Reduce { reducer } => reducer.logical_owned_bytes()?,
            _ => 0,
        };
        let bytes = [
            self.owner.authority().id.capacity() as u64,
            self.owner_scope.logical_owned_bytes()?,
            self.entrypoint.logical_owned_bytes()?,
            slots::<OperationId>(self.dependencies.len())?,
            resources_bytes(&self.resources.0)?,
            slots::<(String, String)>(self.placement.len())?,
            reducer,
            self.state.logical_owned_bytes()?,
        ]
        .into_iter()
        .try_fold(parent, add)?;
        self.placement
            .iter()
            .try_fold(bytes, |bytes, (key, value)| {
                add(add(bytes, key.capacity() as u64)?, value.capacity() as u64)
            })
    }
    pub(super) fn copy_fields(&self, quota: &mut CodecQuota<'_, '_>) -> Result<Self> {
        observe(quota.work())?;
        let parent = self
            .parent
            .as_ref()
            .map(|parent| {
                copy_text(&parent.slot, quota).map(|slot| ParentLink {
                    operation_id: parent.operation_id,
                    slot,
                })
            })
            .transpose()?;
        observe(quota.work())?;
        let authority = crate::core::copy_authority(self.owner.authority(), quota)?;
        let owner = match self.owner {
            DurableOwner::Attached { .. } => DurableOwner::Attached { authority },
            DurableOwner::Detached { .. } => DurableOwner::Detached { authority },
        };
        let owner_scope = self.owner_scope.copy(quota.work())?;
        quota.adopt(owner_scope.logical_owned_bytes()?)?;
        let entrypoint = self.entrypoint.copy_fields(quota)?;
        claim_slots::<OperationId>(self.dependencies.len(), quota)?;
        let mut dependencies = BTreeSet::new();
        for id in &self.dependencies {
            observe(quota.work())?;
            dependencies.insert(*id);
        }
        let resources = ResourceRequest(copy_resources(&self.resources.0, quota)?);
        claim_slots::<(String, String)>(self.placement.len(), quota)?;
        let mut placement = BTreeMap::new();
        for (key, value) in &self.placement {
            observe(quota.work())?;
            let key = copy_text(key, quota)?;
            let value = copy_text(value, quota)?;
            placement.insert(key, value);
        }
        let orchestration = match &self.orchestration {
            Orchestration::Leaf => Orchestration::Leaf,
            Orchestration::Join => Orchestration::Join,
            Orchestration::Race => Orchestration::Race,
            Orchestration::Quorum { required } => Orchestration::Quorum {
                required: *required,
            },
            Orchestration::Reduce { reducer } => Orchestration::Reduce {
                reducer: reducer.copy_fields(quota)?,
            },
        };
        Ok(Self {
            operation_id: self.operation_id,
            parent,
            owner,
            owner_scope,
            entrypoint,
            dependencies,
            resources,
            placement,
            orchestration,
            state: crate::core::copy_file(&self.state, quota)?,
        })
    }
}
impl ResourceRequest {
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        resources_bytes(&self.0)
    }
    fn copy_fields(&self, quota: &mut CodecQuota<'_, '_>) -> Result<Self> {
        Ok(Self(copy_resources(&self.0, quota)?))
    }
}
closed_clone!(ResourceRequest);
closed_clone!(EntrypointRef);
closed_clone!(OperationSpec);
closed_clone!(Reservation);
closed_clone!(LeaseFence);

fn copy_resources(
    source: &BTreeMap<String, u64>,
    quota: &mut CodecQuota<'_, '_>,
) -> Result<BTreeMap<String, u64>> {
    claim_slots::<(String, u64)>(source.len(), quota)?;
    let mut output = BTreeMap::new();
    for (key, value) in source {
        observe(quota.work())?;
        let key = copy_text(key, quota)?;
        output.insert(key, *value);
    }
    Ok(output)
}
