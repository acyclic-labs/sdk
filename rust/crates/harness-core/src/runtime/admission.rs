use super::*;

/// Provider-neutral, owner-attested execution route pinned at admission.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionPlacement {
    /// Versioned execution provider that qualified and owns this route.
    pub provider: ComponentIdentity,
    /// Immutable registered task build, distinct from the task's logical identity.
    pub build: ArtifactRef,
    /// Ready environment when the route is tied to a specific sandbox.
    pub environment: Option<SandboxRef>,
    /// Provider readiness attestation.
    pub readiness_revision: [u8; 32],
}

impl ExecutionPlacement {
    /// Rejects unqualified or invalid placement before admission.
    pub fn validate(&self, work: &mut Option<&mut dyn crate::store::HistoryIo>) -> Result<()> {
        if let Some(io) = work.as_deref() {
            io.check_cancellation()?;
        }
        if !crate::contract::is_valid_component_label(&self.provider.name)
            || !crate::contract::is_valid_component_label(&self.provider.version)
            || self.provider.digest == [0; 32]
            || self.readiness_revision == [0; 32]
        {
            return Err(crate::store::guarded_boundary(
                work,
                crate::wire::ErrorCode::Invalid,
                "execution qualification is invalid",
            ));
        }
        self.build.validate(work)?;
        if let Some(environment) = &self.environment {
            environment.validate(work)?;
        }
        Ok(())
    }
}

/// Exact owner-retained task admission.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskAdmissionRecord {
    /// Idempotent admission operation.
    pub operation_id: OperationId,
    /// Registered task implementation.
    pub task: ComponentIdentity,
    /// Pinned resumable machine.
    pub machine: MachineIdentity,
    /// Schema-validated input, retained outside event history.
    pub input: Value,
    /// Exact schema used to admit the input.
    pub input_schema: Value,
    /// Pinned output contract.
    pub output_schema: Value,
    /// Owning parent, if any.
    pub parent: Option<TaskId>,
    /// Exact created-resource evidence, authenticated by the bound provider.
    pub resource_proofs: Vec<crate::resource_preparation::ResourceCreationProofRef>,
    /// Effective grants at admission.
    pub grants: Capabilities,
    /// Effective numeric limits at admission.
    pub limits: Limits,
    /// Effective task concurrency, step, and deadline bounds.
    pub run_limits: TaskRunLimits,
    /// Exact dispatch policy revision.
    pub policy: Option<ComponentIdentity>,
    /// Pinned extension selection.
    pub extensions: Option<ExtensionAdmission>,
    /// Qualified immutable execution route, or the bound local host.
    pub execution: Option<ExecutionPlacement>,
}

#[derive(Serialize)]
pub(super) struct CanonicalTaskAdmission<'a> {
    pub(super) contract: &'static str,
    #[serde(flatten)]
    pub(super) admission: &'a TaskAdmissionRecord,
}

/// One owner-retained direct task child, independent of fork ancestry.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TaskChild {
    /// Stable parent slot, independent of completion order.
    pub slot: String,
    /// Exact admitted child identity.
    pub task_id: TaskId,
}

/// Bounded hierarchy observation; a changed revision invalidates continuation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TaskChildrenPage {
    /// Retained owner revision observed for this page.
    pub revision: u64,
    /// Direct, same-owner children in slot order.
    pub entries: Vec<TaskChild>,
    /// Last slot to pass with `revision` for the next page.
    pub next_after: Option<String>,
}

/// Canonical ordinary task identity for a caller-retained admission operation.
#[must_use]
pub fn task_id_for_admission(operation: OperationId) -> TaskId {
    TaskId::from_bytes(operation.into_bytes())
}

impl TaskAdmissionRecord {
    /// Canonical task identity of this ordinary admission.
    #[must_use]
    pub fn task_id(&self) -> TaskId {
        task_id_for_admission(self.operation_id)
    }

    /// Concrete admission payload ownership; allocator-node backing is excluded.
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        let overflow = || {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            {
                Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "task admission ownership overflows",
                }
            }
            #[cfg(any(not(feature = "filesystem"), target_arch = "wasm32"))]
            {
                Error::Invalid("task admission ownership overflows".into())
            }
        };
        let mut owned = crate::core::owned_sum([
            crate::core::owned_buffer::<crate::resource_preparation::ResourceCreationProofRef>(
                self.resource_proofs.capacity(),
            )
            .map_err(|_| overflow())?,
            self.task.name.capacity() as u64,
            self.task.version.capacity() as u64,
            self.machine.name.capacity() as u64,
            self.machine.version.capacity() as u64,
            self.grants.logical_owned_bytes()?,
            crate::executor::json::json_value_logical_owned_bytes(&self.input)?,
            crate::executor::json::json_value_logical_owned_bytes(&self.input_schema)?,
            crate::executor::json::json_value_logical_owned_bytes(&self.output_schema)?,
        ])
        .map_err(|_| overflow())?;
        for proof in &self.resource_proofs {
            owned = crate::core::owned_sum([
                owned,
                proof.provider.logical_owned_bytes()?,
                proof.resource.logical_owned_bytes()?,
                proof.evidence.logical_owned_bytes()?,
            ])
            .map_err(|_| overflow())?;
        }
        for identity in self
            .policy
            .iter()
            .chain(self.execution.iter().map(|route| &route.provider))
        {
            owned = crate::core::owned_sum([
                owned,
                identity.name.capacity() as u64,
                identity.version.capacity() as u64,
            ])
            .map_err(|_| overflow())?;
        }
        if let Some(extensions) = &self.extensions {
            owned = owned
                .checked_add(extensions.logical_owned_bytes()?)
                .ok_or_else(overflow)?;
        }
        if let Some(execution) = &self.execution {
            owned = owned
                .checked_add(execution.build.as_resource().logical_owned_bytes()?)
                .ok_or_else(overflow)?;
            if let Some(environment) = &execution.environment {
                owned = owned
                    .checked_add(environment.as_resource().logical_owned_bytes()?)
                    .ok_or_else(overflow)?;
            }
        }
        Ok(owned)
    }

    /// Copies the original admission fields on the caller's allocation context.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn clone_with_native_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        use crate::store::HistoryIo as _;
        let mut io = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(work)?,
        );
        let mut observer = Some(&mut io as &mut dyn crate::store::HistoryIo);
        let mut quota =
            crate::wire_codec::CodecQuota::new(&mut observer, std::mem::size_of::<Self>() as u64)?;
        let copied = crate::core::copy(self, &mut quota, |record, work| {
            let bytes = record.logical_owned_bytes()?;
            if bytes != 0
                && let Some(io) = work.as_deref_mut()
            {
                io.admit_allocation(bytes)?;
            }
            Ok(bytes)
        })?;
        let held = quota.retained();
        quota.transfer();
        drop(quota);
        Ok((copied, io.take_native_custody(held)?))
    }

    /// Constructs and validates an exact admission envelope from the fields supplied by an SDK boundary.
    #[allow(
        clippy::too_many_arguments,
        reason = "flat SDK-boundary constructor taking each envelope field"
    )]
    pub fn from_parts(
        operation_id: OperationId,
        name: &str,
        version: &str,
        input: Value,
        input_schema: Value,
        output_schema: Value,
        requirements: &BTreeSet<String>,
        machine_digest: &[u8],
        parent: Option<TaskId>,
        grants: Capabilities,
        limits: Limits,
        run_limits: TaskRunLimits,
        policy: Option<ComponentIdentity>,
        extensions: Option<ExtensionAdmission>,
        execution: Option<ExecutionPlacement>,
    ) -> Result<Self> {
        let (task, machine) = task_admission_identities(
            name,
            version,
            &input_schema,
            &output_schema,
            requirements,
            machine_digest,
        )?;
        let record = Self {
            operation_id,
            task,
            machine,
            input,
            input_schema,
            output_schema,
            parent,
            resource_proofs: Vec::new(),
            grants,
            limits,
            run_limits,
            policy,
            extensions,
            execution,
        };
        record.validate(&mut None)?;
        Ok(record)
    }

    /// Binds exact provider evidence into this immutable child admission.
    pub fn with_resource_proofs(
        mut self,
        proofs: Vec<crate::resource_preparation::ResourceCreationProofRef>,
    ) -> Result<Self> {
        self.resource_proofs = proofs;
        self.validate(&mut None)?;
        Ok(self)
    }

    /// Validates the admission on the optional history observer.
    pub fn validate(&self, work: &mut Option<&mut dyn crate::store::HistoryIo>) -> Result<()> {
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
        if [
            &self.task.name,
            &self.task.version,
            &self.machine.name,
            &self.machine.version,
        ]
        .into_iter()
        .any(|label| !crate::contract::is_valid_component_label(label))
            || self.task.name.contains('@')
            || self.task.version.contains('@')
        {
            return Err(crate::store::guarded_boundary(
                work,
                crate::wire::ErrorCode::Invalid,
                "task or machine identity is invalid",
            ));
        }
        if self.task.digest == [0; 32]
            || self.machine.digest == [0; 32]
            || self.machine.name != self.task.name
            || self.machine.version != self.task.version
        {
            return Err(crate::store::guarded_boundary(
                work,
                crate::wire::ErrorCode::Invalid,
                "task and machine identities disagree",
            ));
        }
        if !self.resource_proofs.is_empty() && self.parent.is_none() {
            return Err(crate::store::guarded_boundary(
                work,
                crate::wire::ErrorCode::Invalid,
                "resource-derived admission requires a parent",
            ));
        }
        for (index, proof) in self.resource_proofs.iter().enumerate() {
            for previous in self.resource_proofs.iter().take(index) {
                if let Some(io) = work.as_deref_mut() {
                    io.codec_charge(crate::executor::json::JsonCodecCharge::Inspect { items: 1 })?;
                }
                if previous.resource == proof.resource {
                    return Err(crate::store::guarded_boundary(
                        work,
                        crate::wire::ErrorCode::Invalid,
                        "task admission repeats a created resource proof",
                    ));
                }
            }
        }
        validate_task_schemas(&self.input_schema, &self.output_schema, true, work)?;
        crate::executor::validate_json_schema_value(
            &self.input_schema,
            &self.input,
            "task input",
            work,
        )?;
        self.limits.validate(work)?;
        self.run_limits.validate(work)?;
        if let Some(policy) = &self.policy {
            if !crate::contract::is_valid_component_label(&policy.name)
                || !crate::contract::is_valid_component_label(&policy.version)
            {
                return Err(crate::store::guarded_boundary(
                    work,
                    crate::wire::ErrorCode::Invalid,
                    "policy identity is invalid",
                ));
            }
            if policy.digest == [0; 32] {
                return Err(crate::store::guarded_boundary(
                    work,
                    crate::wire::ErrorCode::Invalid,
                    "policy digest is empty",
                ));
            }
        }
        if let Some(extensions) = &self.extensions {
            extensions.validate(work)?;
        }
        if let Some(execution) = &self.execution {
            execution.validate(work)?;
        }
        Ok(())
    }

    pub(crate) fn canonical_payload(&self) -> impl Serialize + '_ {
        CanonicalTaskAdmission {
            contract: "harness.task-admission.v2",
            admission: self,
        }
    }

    pub(crate) fn canonical_digest_with_work(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<[u8; 32]> {
        let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
        let bytes = crate::store::history::canonical_bytes_with_work(
            &self.canonical_payload(),
            acyclic_stream::MAX_RECORD_BYTES as u64,
            custody.work(),
        )?;
        custody.adopt(bytes.capacity() as u64)?;
        if let Some(io) = custody.work().as_deref_mut() {
            io.hashed_bytes(bytes.len() as u64)?;
        }
        Ok(*blake3::hash(&bytes).as_bytes())
    }

    /// Ref-only v2 envelope retained by the owner outside event history.
    #[must_use]
    #[allow(
        clippy::expect_used,
        clippy::missing_panics_doc,
        reason = "This closed admission payload contains only infallible JSON-serializable fields."
    )]
    pub fn canonical_value(&self) -> Value {
        serde_json::to_value(self.canonical_payload()).expect("task admission serializes")
    }

    /// Decodes the canonical admission and reports its new container bytes.
    pub fn from_canonical_value(
        value: Value,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<(Self, u64)> {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let mut body = value;
        let Some(fields) = body.as_object_mut() else {
            return Err(crate::store::guarded_boundary(
                quota.work(),
                crate::wire::ErrorCode::Invalid,
                "task admission must be an object",
            ));
        };
        if !matches!(fields.remove("contract"), Some(Value::String(contract)) if contract == "harness.task-admission.v2")
        {
            return Err(crate::store::guarded_boundary(
                quota.work(),
                crate::wire::ErrorCode::Invalid,
                "unsupported task admission contract",
            ));
        }
        let (admission, added) = Self::decode_body(&body, quota.work())?;
        quota.adopt(added)?;
        admission.validate(quota.work())?;
        let expected = match quota.work().as_deref_mut() {
            Some(io) => {
                let code = crate::store::operations::original_native_decode_mode(io)
                    .then_some(crate::wire::ErrorCode::Invalid);
                crate::executor::json::json_value_bounded(
                    &admission,
                    crate::executor::json::JsonBounds {
                        maximum_bytes: acyclic_stream::MAX_RECORD_BYTES as u64,
                        maximum_items: acyclic_stream::MAX_RECORD_BYTES as u64 + 1,
                        maximum_depth: 256,
                    },
                    code,
                    &mut |charge| io.codec_charge(charge),
                )?
            }
            None => serde_json::to_value(&admission)
                .map_err(|error| Error::Invalid(error.to_string()))?,
        };
        quota.adopt(crate::executor::json::json_value_logical_owned_bytes(
            &expected,
        )?)?;
        if expected != body {
            return Err(crate::store::guarded_boundary(
                quota.work(),
                crate::wire::ErrorCode::Invalid,
                "task admission is not canonical v2",
            ));
        }
        drop(expected);
        drop(body);
        quota.replace(added)?;
        quota.transfer();
        Ok((admission, added))
    }

    fn decode_body(
        body: &Value,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(Self, u64)> {
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let _ = work;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if work
            .as_deref()
            .is_some_and(|io| io.original_native_work().is_some())
        {
            let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
            let decoded = crate::executor::json::json_from_value_bounded(
                std::borrow::Cow::Borrowed(body),
                crate::executor::json::JsonBounds {
                    maximum_bytes: acyclic_stream::MAX_RECORD_BYTES as u64,
                    maximum_items: acyclic_stream::MAX_RECORD_BYTES as u64,
                    maximum_depth: 256,
                },
                0,
                Some(crate::wire::ErrorCode::Invalid),
                crate::executor::json::JsonValueSchema::TaskAdmissionRecord,
                &mut |charge| match charge {
                    crate::executor::json::JsonCodecCharge::Admission { value_shape, .. } => {
                        let containers =
                            crate::executor::json::json_value_decode_owned_bytes(value_shape)?;
                        let diagnostics = crate::executor::json::json_typed_diagnostic_owned_bytes(
                            value_shape,
                            crate::executor::json::preparation_serde_diagnostic_label_bytes(),
                            Some(crate::wire::ErrorCode::Invalid),
                        )?;
                        let bytes = containers
                            .checked_add(diagnostics)
                            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Self>() as u64))
                            .ok_or_else(|| {
                                crate::store::guarded_boundary(
                                    quota.work(),
                                    crate::wire::ErrorCode::Invalid,
                                    "task admission conversion overflows",
                                )
                            })?;
                        quota.replace(bytes)
                    }
                    charge => match quota.work().as_deref_mut() {
                        Some(io)
                            if matches!(
                                charge,
                                crate::executor::json::JsonCodecCharge::Inspect { .. }
                            ) =>
                        {
                            io.check_cancellation()
                        }
                        Some(io) => io.codec_charge(charge),
                        None => Err(Error::NativeBoundary {
                            code: crate::wire::ErrorCode::Invalid,
                            reason: "native admission observer is missing",
                        }),
                    },
                },
            );
            let admission: Self = match decoded {
                Ok(admission) => admission,
                Err(mut error) => {
                    if let Error::NativeJson { custody, .. } = &mut error {
                        let retained = quota.retained();
                        let held = quota
                            .work()
                            .as_deref_mut()
                            .ok_or(Error::NativeBoundary {
                                code: crate::wire::ErrorCode::Invalid,
                                reason: "native admission observer is missing",
                            })?
                            .take_native_custody(retained)?;
                        *custody = Some(held);
                        quota.transfer();
                    }
                    return Err(error);
                }
            };
            let bytes = crate::core::owned_sum([
                std::mem::size_of::<Self>() as u64,
                admission.logical_owned_bytes()?,
            ])?;
            quota.replace(bytes)?;
            quota.transfer();
            return Ok((admission, bytes));
        }
        Ok((
            Self::deserialize(body).map_err(|source| Error::Invalid(source.to_string()))?,
            0,
        ))
    }
}
