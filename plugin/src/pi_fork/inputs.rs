//! Immutable fork inputs and bounded copies on the coordinator's work context.
use super::*;
use acyclic_fs::NativeAllocation;
use acyclic_harness::runtime::TaskSpawner as _;
use acyclic_harness::{JsonValueSchema, filesystem::PreparationIo, runtime::TaskContext};
use serde::de::DeserializeOwned;

impl PiForkCoordinator<'_> {
    /// Reads and validates the protected original plan.
    pub(crate) async fn load_original_plan(
        &self,
        original: OperationId,
    ) -> Result<Option<PiForkPlan>> {
        self.runtime
            .verify_owner_with_native_work(&self.owner, &self.work)?;
        let Some(mut reference) = crate::pi_root_provider::observe_pi_input(
            &self.owner,
            &self.filesystem,
            original,
            "fork-plan",
            &self.work,
        )
        .await?
        else {
            return Ok(None);
        };
        let bytes = self.read_original_input(&reference.0).await?;
        let mut plan: (PiForkPlan, NativeAllocation) = self.decode_original_input(
            &bytes,
            JsonValueSchema::RecursiveForkToolPlan,
            FORK_PLAN_DIAGNOSTIC_LABEL_BYTES,
        )?;
        self.validate_plan(&plan.0, plan.0.commit.caller)?;
        if plan.0.original != original {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi original plan locator changed",
            });
        }
        drop(reference.0);
        plan.1.absorb(&mut reference.1)?;
        plan.0._input_allocations = Some(plan.1);
        Ok(Some(plan.0))
    }

    pub(crate) async fn read_original_input(
        &self,
        reference: &FileRef,
    ) -> Result<acyclic_stream::OwnedBytes> {
        let work = &self.work;
        self.runtime
            .verify_owner_with_native_work(&self.owner, work)?;
        let context = self
            .runtime
            .runtime
            .harness()
            .durable_context(
                self.owner.policy().root_task,
                OperationId::from_bytes(self.owner.policy().root_task.into_bytes()),
                Some(&PreparationIo::from_native_work(work)?),
            )
            .await?;
        context
            .read_file(reference, Some(&PreparationIo::from_native_work(work)?))
            .await
    }

    pub(crate) fn require_input_read(&self, context: &TaskContext) -> Result<()> {
        let mut io =
            acyclic_harness::store::HistoryWork::new(PreparationIo::from_native_work(&self.work)?);
        let capability = self.owner.policy().journal.directory_read_capability(
            crate::pi_root_inputs::ROOT_INPUT_DIRECTORY,
            &mut Some(&mut io),
        )?;
        if !context.scope().grants().contains(&capability) {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi caller lacks its original immutable input-directory read grant",
            });
        }
        Ok(())
    }

    pub(super) fn decode_original_input<T: DeserializeOwned>(
        &self,
        bytes: &[u8],
        schema: &'static JsonValueSchema,
        labels: u64,
    ) -> Result<(T, NativeAllocation)> {
        let maximum = self.owner.policy().maximum_payload_bytes;
        crate::pi_root_provider::guarded_pi_decode(
            bytes,
            maximum,
            maximum,
            acyclic_harness::wire::ErrorCode::Invalid,
            schema,
            labels,
            &self.work,
        )
    }

    pub(super) fn decode_input_value<T: DeserializeOwned>(
        &self,
        value: &impl Serialize,
        schema: &'static JsonValueSchema,
        labels: u64,
    ) -> Result<(T, NativeAllocation)> {
        let bytes = crate::pi_root_provider::canonical_pi_value(
            value,
            self.owner.policy().maximum_payload_bytes,
            &self.work,
        )?;
        self.decode_original_input(&bytes.0, schema, labels)
    }

    pub(super) fn same_payload<T: Serialize>(
        &self,
        expected: &serde_json::Value,
        actual: &T,
    ) -> Result<bool> {
        let actual = crate::pi_root_provider::guarded_pi_value(
            actual,
            self.owner.policy().maximum_payload_bytes,
            &self.work,
        )?;
        Ok(expected == &actual.0)
    }

    pub(super) async fn admit_original(
        &self,
        request: &TaskAdmissionRecord,
        fence: &LeaseFence,
    ) -> Result<()> {
        let work = &self.work;
        let request = request.clone_with_native_work(work)?;
        let reservation = work.display(&fence.reservation_id)?;
        let mut placement = work.display(&fence.placement)?;
        let mut fence = (
            LeaseFence {
                reservation_id: reservation.0,
                placement: placement.0,
            },
            reservation.1,
        );
        fence.1.absorb(&mut placement.1)?;
        let admitted = self
            .runtime
            .runtime
            .task_host()
            .admit_owned(request.0, fence.0)
            .await?;
        drop(request.1);
        drop(fence.1);
        drop(admitted);
        Ok(())
    }
}
