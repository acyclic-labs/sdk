//! Sealed admission reads for original-ledger preparation inspection.
use super::*;
use crate::filesystem::PreparationIo;
use crate::store::HistoryIo;
use acyclic_fs::{NativeAllocation, NativeMaterializationWork};

/// An authenticated admission reader that carries no execution fence or writer.
pub struct TaskAdmissionReadonlyWitness<P> {
    host: Arc<CoordinatorTaskHost<P>>,
    admission: TaskAdmissionRecord,
    scratch: Option<crate::resource_preparation::ChildScratchCreationProof>,
    custody: NativeAllocation,
}

impl<P: StreamProvider> TaskAdmissionReadonlyWitness<P> {
    pub(crate) fn task(&self) -> TaskId {
        self.admission.task_id()
    }
    pub(crate) fn admission_operation(&self) -> OperationId {
        self.admission.operation_id
    }
    pub(crate) fn prepared_stream(&self) -> &Arc<P> {
        &self.host.stream
    }
    pub(crate) fn scratch_proof(
        &self,
    ) -> Option<&crate::resource_preparation::ChildScratchCreationProof> {
        self.scratch.as_ref()
    }

    pub(crate) async fn verify_with_native_work(
        &self,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        if !self.custody.work().shares_allocation_context(work) {
            return Err(denied());
        }
        let io = PreparationIo::from_native_work(work)?;
        let observed = self
            .host
            .admission(self.admission_operation(), Some(&io))
            .await?;
        let custody = io.take_native_custody(
            std::mem::size_of::<TaskAdmissionRecord>() as u64 + observed.logical_owned_bytes()?,
        )?;
        if observed != self.admission {
            return Err(denied());
        }
        drop(observed);
        drop(custody);
        Ok(())
    }

    fn require_query(
        &self,
        query: &crate::runtime::TaskContext,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        query.require_original_native_work(work)?;
        let scope = query.scope();
        if !self.custody.work().shares_allocation_context(work)
            || !query.native_metadata_matches(work)
            || !query.original_host_binding_matches(self.host.as_ref(), self.host.policy.as_deref())
            || query.durable_task_id() != Some(self.task())
            || query.id() != self.admission_operation()
            || scope.grants() != &self.admission.grants
            || scope.limits() != self.admission.limits
            || scope.run_limits() != self.admission.run_limits
            || scope.resource_proofs() != self.admission.resource_proofs
            || scope.extensions() != self.admission.extensions.as_ref()
        {
            return Err(denied());
        }
        Ok(())
    }

    pub(crate) async fn verify_query_with_native_work(
        &self,
        query: &Arc<crate::runtime::TaskContext>,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        self.require_query(query, work)?;
        self.verify_with_native_work(work).await?;
        self.require_query(query, work)
    }

    pub(crate) fn original_preparation_request_with_native_work<'a>(
        &'a self,
        query: &'a Arc<crate::runtime::TaskContext>,
        work: &'a NativeMaterializationWork,
    ) -> Result<
        crate::NativeFutureBundle<
            'a,
            (
                crate::resource_preparation::ResourcePreparationRequest,
                NativeAllocation,
            ),
        >,
    > {
        self.require_query(query, work)?;
        crate::NativeFutureBundle::new(work, || async move {
            self.verify_query_with_native_work(query, work).await?;
            let io = PreparationIo::from_native_work(work)?;
            let (request, held) = crate::resource_preparation::ResourcePreparationRequest::decode(
                &self.admission.input,
                &mut Some(&mut io.history_io()),
            )?;
            let custody = io.take_native_custody(held)?;
            self.verify_query_with_native_work(query, work).await?;
            Ok((request, custody))
        })
    }

    pub(crate) fn verify_preparation_origin_with_native_work(
        &self,
        query: &Arc<crate::runtime::TaskContext>,
        request: &crate::resource_preparation::ResourcePreparationRequest,
        origin: &crate::resource_preparation::ResourceCreationOrigin,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        self.require_query(query, work)?;
        let io = PreparationIo::from_native_work(work)?;
        let mut history = io.history_io();
        let mut observer: Option<&mut dyn HistoryIo> = Some(&mut history);
        request.validate(&mut observer)?;
        let admitted = self.admission.canonical_digest_with_work(&mut observer)?;
        let original = crate::executor::json::canonical_json_digest(
            &self.admission.input,
            acyclic_stream::MAX_RECORD_BYTES as u64,
            &mut observer,
        )?;
        let supplied = crate::executor::json::canonical_json_digest(
            request,
            acyclic_stream::MAX_RECORD_BYTES as u64,
            &mut observer,
        )?;
        if &origin.owner != &self.host.owner
            || origin.preparation_task != self.task()
            || self.admission.parent != Some(origin.parent_task)
            || request.child.parent != Some(origin.parent_task)
            || origin.intended_child != request.child.operation_id
            || origin.admission_digest != admitted
            || origin.source_grants != self.admission.grants
            || origin.request_digest != original
            || supplied != original
        {
            return Err(denied());
        }
        Ok(())
    }
}

fn denied() -> Error {
    Error::NativeBoundary {
        code: crate::wire::ErrorCode::Unauthorized,
        reason: "preparation reader differs from its authenticated admission or original ledger",
    }
}

impl<P: StreamProvider> CoordinatorTaskHost<P> {
    pub(crate) fn task_preparation_readonly_witness_with_native_work<'a>(
        self: &'a Arc<Self>,
        query: &'a Arc<crate::runtime::TaskContext>,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, TaskAdmissionReadonlyWitness<P>>> {
        query.require_original_native_work(work)?;
        if !query.native_metadata_matches(work)
            || !query.original_host_binding_matches(self.as_ref(), self.policy.as_deref())
        {
            return Err(denied());
        }
        let task = query.durable_task_id().ok_or_else(denied)?;
        if query.id().into_bytes() != task.into_bytes() {
            return Err(denied());
        }
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            let admission = self.admission(query.id(), Some(&io)).await?;
            let mut custody = io.take_native_custody(
                std::mem::size_of::<TaskAdmissionRecord>() as u64
                    + admission.logical_owned_bytes()?,
            )?;
            let access = query.scope().created_access();
            let scratch = if admission.parent.is_none() && admission.resource_proofs.is_empty() {
                None
            } else {
                let proof = access
                    .as_ref()
                    .map(|access| access.current_content(&mut Some(&mut io.history_io())))
                    .transpose()?
                    .flatten()
                    .and_then(|binding| binding.scratch_creation.as_ref())
                    .cloned()
                    .ok_or_else(denied)?;
                if proof.origin().intended_child.into_bytes() != task.into_bytes() {
                    return Err(denied());
                }
                Some(proof)
            };
            custody.absorb(&mut work.claim(
                std::mem::size_of::<TaskAdmissionReadonlyWitness<P>>() as u64,
                1,
                acyclic_fs::WorkCounters::default(),
            )?)?;
            let witness = TaskAdmissionReadonlyWitness {
                host: self.clone(),
                admission,
                scratch,
                custody,
            };
            witness.require_query(query, work)?;
            Ok(witness)
        })
    }
}
