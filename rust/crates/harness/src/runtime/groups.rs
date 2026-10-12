use super::*;

/// Typed live group whose cancellation is independent of sibling groups.
pub struct RuntimeGroup {
    pub(super) harness: Arc<AgentHarness>,
    pub(super) group: TaskGroup,
    pub(super) scope: RuntimeScope,
    pub(super) policy_overrides: Vec<(ComponentIdentity, Arc<dyn ToolPolicy>)>,
    pub(super) durable_parent: Option<TaskId>,
}

/// Ordered immutable batch selection.
pub struct Batch<I> {
    /// Caller-retained identity allocated before dispatch.
    pub id: BatchId,
    /// Complete ordered input list.
    pub inputs: Vec<I>,
}

impl<I> Batch<I> {
    /// Creates an ordered batch with a stable identity.
    #[must_use]
    pub fn new(id: BatchId, inputs: Vec<I>) -> Self {
        Self { id, inputs }
    }
}

/// One unambiguous slot in an admitted batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InputKey {
    /// Batch owning this input.
    pub batch_id: BatchId,
    /// Zero-based position in the immutable ordered request.
    pub index: usize,
}

/// Group behavior retained with the exact batch identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum BatchGroupPolicy {
    /// Preserve every admission and terminal outcome without cancelling siblings.
    CollectAll,
    /// Request owner-retained sibling cancellation after a definitive failure.
    CancelOnFailure,
}

/// Exact immutable batch request presented to a durable owner.
#[derive(Clone)]
pub struct DurableBatchRequest {
    /// Stable group identity.
    pub group_id: GroupId,
    /// Stable batch identity.
    pub batch_id: BatchId,
    /// Group completion behavior pinned at admission.
    pub group_policy: BatchGroupPolicy,
    /// Pinned task definition.
    pub task: ComponentIdentity,
    /// Pinned resumable implementation.
    pub machine: MachineIdentity,
    /// Ordered, schema-validated inputs.
    pub inputs: Vec<Value>,
    /// Pinned input schema checked again by the durable owner.
    pub input_schema: Value,
    /// Pinned output schema.
    pub output_schema: Value,
    /// Durable parent owning descendants, or none for a root-owned batch.
    pub parent: Option<TaskId>,
    /// Effective immutable authority at batch admission.
    pub scope: RuntimeScope,
    /// Exact policy implementation enforced by the host.
    pub policy: Option<ComponentIdentity>,
    /// Qualified immutable route shared by the batch's indexed members.
    pub execution: Option<ExecutionPlacement>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DurableBatchWire {
    pub(super) contract: String,
    pub(super) group_id: GroupId,
    pub(super) batch_id: BatchId,
    pub(super) group_policy: BatchGroupPolicy,
    pub(super) task: ComponentIdentity,
    pub(super) machine: MachineIdentity,
    pub(super) inputs: Vec<Value>,
    pub(super) input_schema: Value,
    pub(super) output_schema: Value,
    pub(super) parent: Option<TaskId>,
    pub(super) resource_proofs: Vec<crate::resource_preparation::ResourceCreationProofRef>,
    pub(super) grants: Capabilities,
    pub(super) limits: Limits,
    pub(super) run_limits: TaskRunLimits,
    pub(super) extensions: Option<ExtensionAdmission>,
    pub(super) policy: Option<ComponentIdentity>,
    pub(super) execution: Option<ExecutionPlacement>,
}

impl DurableBatchRequest {
    /// Constructs and validates an exact immutable batch envelope from SDK boundary fields.
    #[allow(
        clippy::too_many_arguments,
        reason = "flat SDK-boundary constructor taking each envelope field"
    )]
    pub fn from_parts(
        group_id: GroupId,
        batch_id: BatchId,
        group_policy: BatchGroupPolicy,
        name: &str,
        version: &str,
        inputs: Vec<Value>,
        input_schema: Value,
        output_schema: Value,
        requirements: &BTreeSet<String>,
        machine_digest: &[u8],
        parent: Option<TaskId>,
        grants: Capabilities,
        limits: Limits,
        run_limits: TaskRunLimits,
        extensions: Option<ExtensionAdmission>,
        policy: Option<ComponentIdentity>,
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
        let scope = RuntimeScope::new(grants, limits)?
            .with_run_limits(run_limits, &mut None)?
            .with_replayed_extensions(extensions)?;
        let request = Self {
            group_id,
            batch_id,
            group_policy,
            task,
            machine,
            inputs,
            input_schema,
            output_schema,
            parent,
            scope,
            policy,
            execution,
        };
        request.validate()?;
        Ok(request)
    }

    /// Retains proof references in a request without granting a scope or installing content handles.
    pub fn with_resource_proofs(
        mut self,
        proofs: Vec<crate::resource_preparation::ResourceCreationProofRef>,
    ) -> Result<Self> {
        if self.scope.resource_proofs != proofs {
            self.scope.created_access = None;
        }
        self.scope.resource_proofs = proofs;
        self.validate()?;
        Ok(self)
    }

    /// Validates the complete retained manifest before any child is observed or admitted.
    pub fn validate(&self) -> Result<()> {
        validate_identity(&self.task.name, &self.task.version, None)?;
        validate_component_label(&self.machine.name, "machine name")?;
        validate_component_label(&self.machine.version, "machine version")?;
        if self.task.digest == [0; 32]
            || self.machine.digest == [0; 32]
            || self.machine.name != self.task.name
            || self.machine.version != self.task.version
        {
            return Err(Error::Invalid(
                "batch implementation identity is invalid".into(),
            ));
        }
        if !self.scope.resource_proofs.is_empty() && self.parent.is_none() {
            return Err(Error::Invalid("created batch scope has no parent".into()));
        }
        let mut resources = BTreeSet::new();
        for proof in &self.scope.resource_proofs {
            if !resources.insert(crate::contract::canonical_json_bytes(&proof.resource)?) {
                return Err(Error::Invalid(
                    "batch repeats a created resource proof".into(),
                ));
            }
        }
        validate_task_schemas(&self.input_schema, &self.output_schema, true, &mut None)?;
        self.scope.limits().validate(&mut None)?;
        self.scope.run_limits().validate(&mut None)?;
        if let Some(policy) = &self.policy {
            validate_policy_identity(policy, None)?;
        }
        if let Some(execution) = &self.execution {
            execution.validate(&mut None)?;
        }
        for input in &self.inputs {
            crate::contract::validate_json_schema_value(
                &self.input_schema,
                input,
                "batch input",
                &mut None,
            )?;
        }
        if crate::contract::canonical_json_bytes(&self.canonical_value())?.len() as u64
            > self.scope.limits().file_bytes
        {
            return Err(Error::Invalid("batch request exceeds file limit".into()));
        }
        Ok(())
    }

    /// Reconstructs only the exact v2 request stored by its owner; unknown or omitted fields cannot silently change the admitted child identities.
    pub fn from_canonical_value(value: Value) -> Result<Self> {
        let canonical = value.clone();
        let wire: DurableBatchWire =
            serde_json::from_value(value).map_err(|error| Error::Invalid(error.to_string()))?;
        if wire.contract != "harness.batch.v2" {
            return Err(Error::Invalid("unsupported batch contract".into()));
        }
        let mut scope = RuntimeScope::new(wire.grants, wire.limits)?
            .with_run_limits(wire.run_limits, &mut None)?
            .with_replayed_extensions(wire.extensions)?;
        scope.resource_proofs = wire.resource_proofs;
        let request = Self {
            group_id: wire.group_id,
            batch_id: wire.batch_id,
            group_policy: wire.group_policy,
            task: wire.task,
            machine: wire.machine,
            inputs: wire.inputs,
            input_schema: wire.input_schema,
            output_schema: wire.output_schema,
            parent: wire.parent,
            scope,
            policy: wire.policy,
            execution: wire.execution,
        };
        if request.canonical_value() != canonical {
            return Err(Error::Invalid("batch request is not canonical v2".into()));
        }
        request.validate()?;
        Ok(request)
    }

    /// Canonical batch identity binds all executable inputs and authority.
    pub fn canonical_value(&self) -> Value {
        serde_json::json!({
            "contract": "harness.batch.v2",
            "group_id": self.group_id,
            "batch_id": self.batch_id,
            "group_policy": self.group_policy,
            "task": self.task,
            "machine": self.machine,
            "inputs": self.inputs,
            "input_schema": self.input_schema,
            "output_schema": self.output_schema,
            "parent": self.parent,
            "resource_proofs": self.scope.resource_proofs(),
            "grants": self.scope.grants(),
            "limits": self.scope.limits(),
            "run_limits": self.scope.run_limits(),
            "extensions": self.scope.extensions(),
            "policy": self.policy,
            "execution": self.execution,
        })
    }

    /// Derives the sole admission record for one indexed member.
    pub fn member_admission(&self, index: usize) -> Result<TaskAdmissionRecord> {
        let input = self
            .inputs
            .get(index)
            .ok_or_else(|| Error::Invalid("batch member index is missing".into()))?;
        Ok(TaskAdmissionRecord {
            resource_proofs: self.scope.resource_proofs().to_vec(),
            operation_id: self.operation_id(index),
            task: self.task.clone(),
            machine: self.machine.clone(),
            input: input.clone(),
            input_schema: self.input_schema.clone(),
            output_schema: self.output_schema.clone(),
            parent: self.parent,
            grants: self.scope.grants().clone(),
            limits: self.scope.limits(),
            run_limits: self.scope.run_limits(),
            policy: self.policy.clone(),
            extensions: self.scope.extensions().cloned(),
            execution: self.execution.clone(),
        })
    }

    /// Stable per-index operation independent of admission or completion order.
    #[must_use]
    pub fn operation_id(&self, index: usize) -> OperationId {
        batch_member_operation_id(self.group_id, self.batch_id, index)
    }
}

/// One canonical operation identity shared by native and WASM batch facades.
#[must_use]
pub fn batch_member_operation_id(
    group_id: GroupId,
    batch_id: BatchId,
    index: usize,
) -> OperationId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"harness/v2/batch-member\0");
    hasher.update(&group_id.into_bytes());
    hasher.update(&batch_id.into_bytes());
    hasher.update(&(index as u64).to_be_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    OperationId::from_bytes(bytes)
}

/// One exact input slot, preserving the admission decision before and after terminal observation without introducing another key-bearing shape.
pub struct BatchEntry<T> {
    /// Stable batch/index key.
    pub key: InputKey,
    /// Accepted value, rejection, or unresolved operation.
    pub admission: Admission<T>,
}

/// Ordered admission receipt; every submitted input retains a slot.
pub struct AdmissionBatch<O> {
    /// Owning group identity.
    pub group_id: GroupId,
    /// Immutable request identity.
    pub batch_id: BatchId,
    /// Exactly one entry per input, in request order.
    pub entries: Vec<BatchEntry<RuntimeTask<O>>>,
    pub(super) policy: BatchGroupPolicy,
    pub(super) spawner: Arc<dyn TaskSpawner>,
    pub(super) extensions: Option<Vec<crate::extension::ExtensionLease>>,
}

/// Ordered outcome of all observable batch slots.
pub struct BatchOutcome<O> {
    /// Stable group identity.
    pub group_id: GroupId,
    /// Stable batch identity.
    pub batch_id: BatchId,
    /// One result for every submitted input.
    pub entries: Vec<BatchEntry<Result<Outcome<O>>>>,
    /// Owner-retained cancellation request, if a failure triggered the policy.
    pub cancellation: Option<Result<BatchCancellationReport>>,
}

/// One cancellation request, never a claim that the task is terminal.
pub enum BatchCancellationStatus {
    /// The owner accepted an idempotent cancellation request; observe outcome separately.
    Requested,
    /// The member was definitively rejected at admission.
    NotAdmitted {
        /// Stable explanation for why this member was never admitted.
        reason: String,
    },
    /// Admission or cancellation has an unresolved operation identity.
    Indeterminate {
        /// Operation whose admission or cancellation must be reconciled.
        operation_id: OperationId,
    },
    /// The owner did not confirm the request; retry reconciliation by batch ID.
    Unresolved {
        /// Bounded provider explanation retained for batch reconciliation.
        message: String,
    },
}

/// Per-member request report for a retained batch.
pub struct BatchCancellationReport {
    /// Owning group.
    pub group_id: GroupId,
    /// Caller-retained batch.
    pub batch_id: BatchId,
    /// Every original input slot in order.
    pub entries: Vec<(InputKey, BatchCancellationStatus)>,
}

pub(super) async fn observe_batch_entry<O: DeserializeOwned + Send + 'static>(
    entry: BatchEntry<RuntimeTask<O>>,
) -> BatchEntry<Result<Outcome<O>>> {
    let admission = match entry.admission {
        Admission::Accepted(task) => Admission::Accepted(task.result().await),
        Admission::Rejected { reason } => Admission::Rejected { reason },
        Admission::Indeterminate { operation_id } => Admission::Indeterminate { operation_id },
    };
    BatchEntry {
        key: entry.key,
        admission,
    }
}

pub(super) async fn request_batch_cancellation(
    spawner: &Arc<dyn TaskSpawner>,
    group_id: GroupId,
    batch_id: BatchId,
    count: usize,
) -> Result<BatchCancellationReport> {
    let report = spawner
        .cancel_batch(batch_id)
        .await?
        .ok_or_else(|| Error::Storage("retained batch disappeared during cancellation".into()))?;
    if report.group_id != group_id
        || report.batch_id != batch_id
        || report.entries.len() != count
        || report
            .entries
            .iter()
            .enumerate()
            .any(|(index, (key, _))| key.batch_id != batch_id || key.index != index)
    {
        return Err(Error::Conflict(
            "batch owner returned another cancellation report".into(),
        ));
    }
    Ok(report)
}

pub(super) fn cancellation_unresolved(
    cancellation: &Option<Result<BatchCancellationReport>>,
) -> bool {
    cancellation.as_ref().is_some_and(|result| match result {
        Err(_) => true,
        Ok(report) => report.entries.iter().any(|(_, status)| {
            matches!(
                status,
                BatchCancellationStatus::Indeterminate { .. }
                    | BatchCancellationStatus::Unresolved { .. }
            )
        }),
    })
}

impl<O> BatchOutcome<O> {
    /// Reports whether every admission, outcome, and cancellation is definitive.
    pub fn is_complete(&self) -> bool {
        self.entries.iter().all(|entry| match &entry.admission {
            Admission::Accepted(Ok(
                Outcome::Succeeded(_) | Outcome::Failed { .. } | Outcome::Cancelled,
            ))
            | Admission::Rejected { .. } => true,
            Admission::Accepted(Ok(Outcome::Indeterminate { .. }) | Err(_))
            | Admission::Indeterminate { .. } => false,
        }) && self.cancellation.as_ref().is_none_or(|result| {
            result.as_ref().is_ok_and(|report| {
                report.entries.iter().all(|(_, status)| {
                    matches!(
                        status,
                        BatchCancellationStatus::Requested
                            | BatchCancellationStatus::NotAdmitted { .. }
                    )
                })
            })
        })
    }
}

impl<O: DeserializeOwned + Send + 'static> AdmissionBatch<O> {
    /// Observes accepted members concurrently while preserving every original input slot, including rejected and unresolved admissions.
    pub async fn join(self) -> BatchOutcome<O> {
        let Self {
            group_id,
            batch_id,
            entries,
            policy,
            spawner,
            extensions,
        } = self;
        let _extensions = extensions;
        let count = entries.len();
        let concurrency = if policy == BatchGroupPolicy::CancelOnFailure {
            count.max(1)
        } else {
            count.clamp(1, 64)
        };
        let mut slots = entries
            .iter()
            .map(|entry| {
                let admission = match &entry.admission {
                    Admission::Accepted(_) => Admission::Accepted(Err(Error::Indeterminate(
                        batch_member_operation_id(group_id, batch_id, entry.key.index),
                    ))),
                    Admission::Rejected { reason } => Admission::Rejected {
                        reason: reason.clone(),
                    },
                    Admission::Indeterminate { operation_id } => Admission::Indeterminate {
                        operation_id: *operation_id,
                    },
                };
                BatchEntry {
                    key: entry.key,
                    admission,
                }
            })
            .collect::<Vec<BatchEntry<Result<Outcome<O>>>>>();
        let mut completions = stream::iter(entries)
            .map(observe_batch_entry)
            .buffer_unordered(concurrency);
        let mut cancellation = None;
        while let Some(entry) = completions.next().await {
            let failed = matches!(
                &entry.admission,
                Admission::Rejected { .. }
                    | Admission::Accepted(Ok(Outcome::Failed { .. } | Outcome::Cancelled))
            );
            if failed && policy == BatchGroupPolicy::CancelOnFailure && cancellation.is_none() {
                cancellation =
                    Some(request_batch_cancellation(&spawner, group_id, batch_id, count).await);
            }
            let index = entry.key.index;
            let Some(slot) = slots.get_mut(index) else {
                cancellation = Some(Err(Error::Conflict(
                    "batch owner returned an invalid member index".into(),
                )));
                break;
            };
            *slot = entry;
            if cancellation_unresolved(&cancellation) {
                break;
            }
        }
        let entries = slots;
        BatchOutcome {
            group_id,
            batch_id,
            entries,
            cancellation,
        }
    }
}

/// A typed ordered map cannot silently discard a rejected admission, failed child, or unknown outcome.
pub enum RuntimeMap<O> {
    /// Every input admitted and completed successfully, in input order.
    Complete(Vec<O>),
    /// One entry per input, including admission or observation errors.
    Incomplete(Vec<Result<Outcome<O>>>),
}

impl RuntimeGroup {
    /// Admits one registered typed member into this group.
    pub async fn spawn<I, O>(
        &self,
        definition: &Arc<TaskDefinition<I, O>>,
        input: I,
    ) -> Result<RuntimeTask<O>>
    where
        I: Serialize + Send + 'static,
        O: Serialize + DeserializeOwned + Send + 'static,
    {
        if self.durable_parent.is_some() {
            return Err(Error::Unsupported(
                "durable descendants require stable batch admission".into(),
            ));
        }
        require_task_spawn(&self.scope, definition)?;
        self.harness
            .spawn_in_group(
                &self.group,
                self.scope.clone(),
                self.policy_overrides.clone(),
                definition,
                input,
                None,
            )
            .await
    }

    /// Admits independent live tasks in input order without waiting for their results.
    pub async fn spawn_many<I, O>(
        &self,
        definition: &Arc<TaskDefinition<I, O>>,
        inputs: impl IntoIterator<Item = I>,
    ) -> Vec<Result<RuntimeTask<O>>>
    where
        I: Serialize + Send + 'static,
        O: Serialize + DeserializeOwned + Send + 'static,
    {
        let mut admissions = Vec::new();
        for input in inputs {
            admissions.push(self.spawn(definition, input).await);
        }
        admissions
    }

    /// Admits all independent inputs before awaiting any result, then returns either ordered outputs or every input's exact outcome.
    pub async fn map<I, O>(
        &self,
        definition: &Arc<TaskDefinition<I, O>>,
        inputs: impl IntoIterator<Item = I>,
    ) -> RuntimeMap<O>
    where
        I: Serialize + Send + 'static,
        O: Serialize + DeserializeOwned + Send + 'static,
    {
        let admissions = self.spawn_many(definition, inputs).await;
        let mut outcomes = Vec::with_capacity(admissions.len());
        let mut accepted = Vec::new();
        for admission in admissions {
            match admission {
                Ok(task) => {
                    outcomes.push(None);
                    accepted.push(task);
                }
                Err(error) => outcomes.push(Some(Err(error))),
            }
        }
        let mut completed = join_runtime(accepted).await.into_iter();
        let outcomes = outcomes
            .into_iter()
            .map(|entry| {
                entry.unwrap_or_else(|| {
                    completed.next().unwrap_or_else(|| {
                        Err(Error::Storage("accepted task result is missing".into()))
                    })
                })
            })
            .collect::<Vec<_>>();
        if outcomes
            .iter()
            .all(|entry| matches!(entry, Ok(Outcome::Succeeded(_))))
        {
            RuntimeMap::Complete(
                outcomes
                    .into_iter()
                    .map(|entry| match entry {
                        Ok(Outcome::Succeeded(value)) => value,
                        _ => unreachable!("all outcomes were successful"),
                    })
                    .collect(),
            )
        } else {
            RuntimeMap::Incomplete(outcomes)
        }
    }

    /// Observes admitted members in input order.
    pub async fn join<O: DeserializeOwned + Send + 'static>(
        &self,
        tasks: Vec<RuntimeTask<O>>,
    ) -> Vec<Result<Outcome<O>>> {
        join_runtime(tasks).await
    }

    /// Collects the first `required` successes, or reports all non-successes.
    pub async fn quorum<O: DeserializeOwned + Send + 'static>(
        &self,
        tasks: Vec<RuntimeTask<O>>,
        required: usize,
    ) -> std::result::Result<Vec<O>, QuorumFailure<O>> {
        if required == 0 {
            return Ok(Vec::new());
        }
        let concurrency = tasks.len().clamp(1, 64);
        let completions = stream::iter(tasks)
            .map(|task| async move {
                let id = task.identity();
                (id, task.result().await)
            })
            .buffer_unordered(concurrency);
        futures::pin_mut!(completions);
        let mut successes = Vec::new();
        let mut others = Vec::new();
        while let Some((id, result)) = completions.next().await {
            match result {
                Ok(Outcome::Succeeded(value)) => {
                    successes.push((id, value));
                    if successes.len() == required {
                        return Ok(successes.into_iter().map(|(_, value)| value).collect());
                    }
                }
                result => others.push((id, result)),
            }
        }
        Err(QuorumFailure {
            required,
            successes,
            others,
        })
    }

    /// Closes admission while already accepted members and descendants drain.
    pub fn close(&self) {
        self.group.close();
    }
}
