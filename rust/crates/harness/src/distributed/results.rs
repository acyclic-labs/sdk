use super::*;

/// Owner-bound store for staged scheduler aggregate and reducer results.
pub trait SchedulerPayloadStore: acyclic_stream::ProviderPlatform {
    /// Stages JSON bytes under the caller's optional content accounting.
    fn stage<'a>(
        &'a self,
        operation_id: OperationId,
        idempotency_key: &'a str,
        bytes: &'a [u8],
        io: Option<&'a crate::conversation::ContentIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, FileRef>>;
}

/// Code-first registry for exact durable reducer implementations.
#[derive(Default)]
pub struct ReducerRegistry(
    Vec<(
        String,
        String,
        [u8; 32],
        fn(&[(String, serde_json::Value)]) -> Result<crate::Outcome<serde_json::Value>>,
    )>,
);

impl ReducerRegistry {
    /// Registers one exact reducer without ambiguous replacement.
    pub fn register(
        &mut self,
        entrypoint: EntrypointRef,
        reducer: fn(&[(String, serde_json::Value)]) -> Result<crate::Outcome<serde_json::Value>>,
    ) -> Result<()> {
        crate::contract::compile_json_schema(&entrypoint.result_schema, "reducer result", &mut None)?;
        if self.get(&entrypoint).is_some() {
            return Err(Error::Conflict("reducer is already registered".into()));
        }
        self.0.push((
            entrypoint.name,
            entrypoint.version,
            entrypoint.digest,
            reducer,
        ));
        Ok(())
    }

    pub(super) fn get(
        &self,
        entrypoint: &EntrypointRef,
    ) -> Option<fn(&[(String, serde_json::Value)]) -> Result<crate::Outcome<serde_json::Value>>>
    {
        self.0
            .iter()
            .find(|(name, version, digest, _)| {
                name == &entrypoint.name
                    && version == &entrypoint.version
                    && digest == &entrypoint.digest
            })
            .map(|(_, _, _, reducer)| *reducer)
    }
}

impl<P: StreamProvider> DistributedCoordinator<P> {
    /// Advances a ready parent and returns whether a transition was appended.
    pub async fn orchestrate(
        &mut self,
        parent: OperationId,
        idempotency_key: IdempotencyKey,
        reducers: Option<&ReducerRegistry>,
    ) -> Result<bool> {
        self.refresh(&mut None).await?;
        if let Some((_, event, ..)) = read_intent(
            &self.client,
            idempotency_key.as_str(),
            Some(self.revision),
            &mut None,
        )
        .await?
        {
            return if matches!(event, SchedulerEvent::Orchestrated { operation_id, .. } if operation_id == parent)
            {
                Ok(false)
            } else {
                Err(Error::Conflict("coordinator retry identity reused".into()))
            };
        }
        let expected_revision = self
            .scheduler
            .operation(parent)
            .ok_or_else(|| Error::NotFound(format!("operation {parent}")))?
            .revision;
        let (outcome, cancel, reducer, reduction_digest) = match self
            .scheduler
            .orchestration(parent, &mut None)?
        {
            OrchestrationDecision::Complete { outcome, cancel } => (outcome, cancel, None, None),
            OrchestrationDecision::Assemble {
                assembly,
                values,
                cancel,
            } => {
                let digest = assembly_invocation_digest(assembly, &values)?;
                let value = self.assemble_values(assembly, &values, &mut None).await?.0;
                let result = self
                    .stage_json(parent, idempotency_key.as_str(), &value)
                    .await?;
                (
                    crate::Outcome::Succeeded(result),
                    cancel,
                    None,
                    Some(digest),
                )
            }
            OrchestrationDecision::Reduce { reducer, values } => {
                let Some(reducers) = reducers else {
                    return Ok(false);
                };
                let implementation = reducers.get(&reducer).ok_or_else(|| {
                    Error::Unsupported("exact reducer implementation is not registered".into())
                })?;
                let mut decoded = Vec::with_capacity(values.len());
                for (slot, reference) in &values {
                    decoded.push((slot.clone(), self.load_json(reference, &mut None).await?.0));
                }
                let outcome = match implementation(&decoded)? {
                    crate::Outcome::Succeeded(value) => crate::Outcome::Succeeded(
                        self.stage_json(parent, idempotency_key.as_str(), &value)
                            .await?,
                    ),
                    crate::Outcome::Failed { message } => crate::Outcome::Failed { message },
                    crate::Outcome::Cancelled => crate::Outcome::Cancelled,
                    crate::Outcome::Indeterminate { .. } => {
                        return Err(Error::Invalid(
                            "a synchronous reducer cannot return an indeterminate outcome".into(),
                        ));
                    }
                };
                let digest = reduction_invocation_digest(&reducer, &values)?;
                (outcome, Vec::new(), Some(reducer), Some(digest))
            }
            OrchestrationDecision::Wait => return Ok(false),
        };
        self.apply_internal(
            idempotency_key,
            SchedulerEvent::Orchestrated {
                operation_id: parent,
                expected_revision,
                outcome,
                cancel,
                reducer,
                reduction_digest,
            },
            None,
            None,
            None,
            None,
            None,
            &mut None,
        )
        .await
    }

    pub(super) async fn load_json(
        &self,
        reference: &FileRef,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<(serde_json::Value, u64)> {
        reference.validate(work)?;
        if reference.descriptor().media_type() != "application/json" {
            return Err(coordinator_boundary(
                work,
                wire::ErrorCode::Invalid,
                "scheduler result must be a JSON file",
            ));
        }
        let mut custody = CodecQuota::new(work, 0)?;
        let (bytes, raw_bytes) = match custody.work().as_deref_mut() {
            Some(work) => {
                crate::store::history::read_content_with_work(
                    work,
                    self.content_verifier.as_ref(),
                    reference,
                )
                .await?
            }
            None => (
                self.content_verifier
                    .read(reference, reference.descriptor().byte_length(), None)?
                    .await?,
                0,
            ),
        };
        custody.adopt(raw_bytes)?;
        reference
            .descriptor()
            .verify(bytes.as_ref(), custody.work())?;
        let (value, value_bytes) = decode_json(
            bytes.as_ref(),
            crate::contract::JsonValueSchema::Root,
            crate::contract::json_value_logical_owned_bytes,
            custody.work(),
        )?;
        custody.adopt(value_bytes)?;
        let canonical = crate::store::history::canonical_bytes_with_work(
            &value,
            reference.descriptor().byte_length(),
            custody.work(),
        )?;
        let canonical_bytes = if custody.work().is_some() {
            canonical.capacity() as u64
        } else {
            0
        };
        custody.adopt(canonical_bytes)?;
        if canonical.as_slice() != bytes.as_ref() {
            return Err(coordinator_boundary(
                custody.work(),
                wire::ErrorCode::Invalid,
                "scheduler result is not canonical JSON",
            ));
        }
        drop(canonical);
        custody.relinquish(canonical_bytes)?;
        drop(bytes);
        custody.relinquish(raw_bytes)?;
        custody.transfer();
        Ok((value, value_bytes))
    }

    pub(super) async fn assemble_values(
        &self,
        assembly: AssemblyKind,
        values: &[(String, FileRef)],
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<(serde_json::Value, u64)> {
        let mut custody = CodecQuota::new(work, 0)?;
        let mut assembled = crate::core::reserve_copy_vector(values.len(), &mut custody)?;
        for (slot, reference) in values {
            let (value, value_bytes) = self.load_json(reference, custody.work()).await?;
            custody.adopt(value_bytes)?;
            let value = match assembly {
                AssemblyKind::Join => {
                    let copied_slot = crate::core::copy_text(slot, &mut custody)?;
                    let slot_key = crate::core::copy_text("slot", &mut custody)?;
                    let value_key = crate::core::copy_text("value", &mut custody)?;
                    let slots = (2 * std::mem::size_of::<(String, serde_json::Value)>()) as u64;
                    if let Some(work) = custody.work().as_deref_mut() {
                        work.admit_owned(slots)?;
                        work.retain_owned(slots)?;
                        custody.adopt(slots)?;
                    }
                    let mut fields = serde_json::Map::new();
                    fields.insert(slot_key, serde_json::Value::String(copied_slot));
                    fields.insert(value_key, value);
                    serde_json::Value::Object(fields)
                }
                AssemblyKind::Quorum => value,
            };
            assembled.push(value);
        }
        let retained = custody.retained();
        custody.transfer();
        Ok((serde_json::Value::Array(assembled), retained))
    }

    async fn stage_json(
        &self,
        operation_id: OperationId,
        key: &str,
        value: &serde_json::Value,
    ) -> Result<FileRef> {
        let store = self
            .payload_store
            .as_ref()
            .ok_or_else(|| Error::Unsupported("scheduler payload store is not bound".into()))?;
        let bytes = crate::contract::canonical_json_bytes(value)?;
        let reference = store.stage(operation_id, key, &bytes, None)?.await?;
        reference.descriptor().verify(&bytes, &mut None)?;
        if reference.descriptor().media_type() != "application/json" {
            return Err(Error::Invalid(
                "scheduler payload store returned a non-JSON file".into(),
            ));
        }
        Ok(reference)
    }
}
