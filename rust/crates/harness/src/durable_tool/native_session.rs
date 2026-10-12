use super::*;

/// The same owner-bound workflow host, with borrowed original tool metadata.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub struct NativeResumableToolSession<'a> {
    pub(super) definition: &'a ToolDefinition,
    pub(super) invocation: &'a ToolInvocation,
    pub(super) host: DurableWorkflowHost,
    pub(super) registry_custody: acyclic_fs::NativeAllocation,
    pub(super) metadata_custody: acyclic_fs::NativeAllocation,
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl NativeResumableToolSession<'_> {
    /// Borrows the exact original admitted invocation.
    pub const fn invocation(&self) -> &ToolInvocation {
        self.invocation
    }

    /// Borrows the actual latest committed checkpoint.
    pub const fn checkpoint(&self) -> &MachineCheckpoint {
        self.host.checkpoint()
    }

    /// Commits through the same native workflow host.
    pub fn step<'b>(
        &'b mut self,
        operation_id: OperationId,
        idempotency_key: IdempotencyKey,
        input: Value,
        input_custody: acyclic_fs::NativeAllocation,
        work: &'b acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'b, (MachineTransition, acyclic_fs::NativeAllocation)>>
    {
        let input = ((idempotency_key, input), input_custody);
        if !self.metadata_custody.work().shares_ledger(work)
            || !self.registry_custody.work().shares_ledger(work)
            || !input.1.work().shares_ledger(work)
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "native resumable input belongs to another work",
            });
        }
        let required = (std::mem::size_of::<Value>() as u64)
            .checked_add(crate::contract::json_value_logical_owned_bytes(&input.0.1)?)
            .and_then(|bytes| bytes.checked_add(input.0.0.0.capacity() as u64))
            .ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "native resumable input ownership overflow",
            })?;
        if input.1.bytes() < required {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "native resumable input or retry key has no paid custody",
            });
        }
        crate::NativeFutureBundle::new(work, || async move {
            let input = input;
            let io = crate::executor::PreparationIo::from_native_work(work)?;
            let output_schema = &self.definition.output_schema;
            let result = self
                .host
                .step_checked_with_native_work(
                    operation_id,
                    input.0.0,
                    input.0.1,
                    work,
                    |transition| {
                        if let MachineStatus::Completed { value } = &transition.status {
                            crate::contract::validate_json_schema_value(
                                output_schema,
                                value,
                                "tool output",
                                Some(&io),
                            )?;
                        }
                        Ok(())
                    },
                )
                .await;
            drop(input.1);
            result
        })
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) fn clone_admission(
    admission: &WorkflowAdmission,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(WorkflowAdmission, acyclic_fs::NativeAllocation)> {
    use crate::filesystem::UploadRecord as _;
    let io = crate::executor::PreparationIo::from_native_work(work)?;
    io.retain_owned(std::mem::size_of::<WorkflowAdmission>() as u64)?;
    let name = crate::executor::stock_string_copy(&admission.initial.machine.name, Some(&io))?;
    let version =
        crate::executor::stock_string_copy(&admission.initial.machine.version, Some(&io))?;
    let state = crate::model::clone_value(&admission.initial.state, Some(&io))?;
    let copied = WorkflowAdmission {
        operation_id: admission.operation_id,
        request_digest: admission.request_digest,
        initial: MachineCheckpoint {
            machine: crate::workflow::MachineIdentity {
                name,
                version,
                digest: admission.initial.machine.digest,
            },
            revision: admission.initial.revision,
            state,
        },
    };
    let retained = (std::mem::size_of::<WorkflowAdmission>() as u64)
        .checked_add(copied.logical_owned_bytes()?)
        .ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            reason: "native workflow admission ownership overflow",
        })?;
    let custody = io.into_native_custody(retained)?;
    Ok((copied, custody))
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl crate::filesystem::UploadRecord for WorkflowAdmission {
    fn diagnostic_label_bytes() -> u64 {
        crate::tool::tool_serde_diagnostic_label_bytes()
    }
    fn logical_owned_bytes(&self) -> Result<u64> {
        let deep = crate::contract::json_value_logical_owned_bytes(&self.initial.state)?;
        (self.initial.machine.name.capacity() as u64)
            .checked_add(self.initial.machine.version.capacity() as u64)
            .and_then(|bytes| bytes.checked_add(deep))
            .ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "native workflow admission ownership overflow",
            })
    }
}
