//! Inputs of the original Pi fork.
use super::*;

impl<'a> PiForkCoordinator<'a> {
    pub(crate) async fn load_plan(
        &self,
        context: &acyclic_harness::runtime::TaskContext,
        original: OperationId,
    ) -> Result<Option<PiForkPlan>> {
        self.require_input_read(context)?;
        self.load_original_plan(original).await
    }

    /// Reads only the original protected piInputs directory.
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
        let mut decoded: (PiForkPlan, acyclic_fs::NativeAllocation) = self.decode_original_input(
            bytes.as_ref(),
            acyclic_harness::JsonValueSchema::RecursiveForkToolPlan,
            FORK_PLAN_DIAGNOSTIC_LABEL_BYTES,
        )?;
        self.validate_plan(&decoded.0, decoded.0.commit.caller)?;
        if decoded.0.original != original {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi original plan locator changed",
            });
        }
        drop(reference.0);
        decoded.1.absorb(&mut reference.1)?;
        decoded.0._input_allocations = Some(decoded.1);
        Ok(Some(decoded.0))
    }

    pub(crate) async fn read_original_input(
        &self,
        reference: &FileRef,
    ) -> Result<acyclic_stream::OwnedBytes> {
        let work = &self.work;
        self.runtime
            .verify_owner_with_native_work(&self.owner, work)?;
        let actual = self
            .runtime
            .runtime
            .readonly_task_context_with_native_work(self.owner.policy().root_task, work)
            .await?;
        return actual
            .1
            .read_file_with_native_work(reference, work)
            .await;
    }

    pub(super) fn require_input_read(
        &self,
        context: &acyclic_harness::runtime::TaskContext,
    ) -> Result<()> {
        let native = {
            let mut io = acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(&self.work)?);
            let value = self.owner.policy().journal.directory_read_capability(
                crate::pi_root_inputs::ROOT_INPUT_DIRECTORY,
                &mut Some(&mut io),
            )?;
            (value, io.into_custody()?)
        };
        let capability = native.0.as_str();
        if !context.scope().grants().contains(capability) {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi caller lacks its original immutable input-directory read grant",
            });
        }
        Ok(())
    }

    pub(super) fn decode_original_input<T: serde::de::DeserializeOwned>(
        &self,
        bytes: &[u8],
        value_schema: acyclic_harness::JsonValueSchema,
        schema_label_bytes: u64,
    ) -> Result<(T, acyclic_fs::NativeAllocation)> {
        let work = &self.work;
        crate::pi_root_provider::guarded_pi_decode(
            bytes,
            self.owner.policy().maximum_payload_bytes,
            self.owner.policy().maximum_payload_bytes,
            acyclic_harness::wire::ErrorCode::Invalid,
            value_schema,
            schema_label_bytes,
            work,
        )
    }

    pub(super) fn decode_input_value<T: serde::de::DeserializeOwned>(
        &self,
        value: &serde_json::Value,
        schema: acyclic_harness::JsonValueSchema,
        labels: u64,
    ) -> Result<(T, acyclic_fs::NativeAllocation)> {
        let work = &self.work;
        let maximum = self.owner.policy().maximum_payload_bytes;
        let bytes = crate::pi_root_provider::canonical_pi_value(value, maximum, work)?;
        let (value, allocation) = crate::pi_root_provider::guarded_pi_decode(
            &bytes.0,
            maximum,
            maximum,
            acyclic_harness::wire::ErrorCode::Invalid,
            schema,
            labels,
            work,
        )?;
        Ok((value, allocation))
    }

    pub(super) fn same_payload<T: Serialize>(
        &self,
        expected: &serde_json::Value,
        actual: &T,
    ) -> Result<bool> {
        let work = &self.work;
        let actual = crate::pi_root_provider::guarded_pi_value(
            actual,
            self.owner.policy().maximum_payload_bytes,
            work,
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
            .admit_owned_with_native_work(request, fence, work)?
            .await?;
        drop(admitted);
        Ok(())
    }
}
