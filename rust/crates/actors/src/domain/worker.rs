use super::*;
/// Delete a named Worker deployment. Does not target an actor instance.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = DeleteWorkerRequest::validate_from_proto)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "actors/DeleteWorkerRequest.ts", rename_all = "camelCase")]
pub struct DeleteWorkerRequest {
    #[wire(tag = 1, string)]
    worker_name: ActorNamespace,
    #[wire(tag = 2)]
    idempotency_key: String,
    #[wire(tag = 3, bytes)]
    #[ts(type = r#"import("../../readonly.js").ReadonlyBytes"#)]
    account_id: Vec<u8>,
}
impl DeleteWorkerRequest {
    fn validate_from_proto(&self) -> Result<(), DomainError> {
        crate::addressing::validate_dns_label(self.worker_name.as_str().as_bytes())
            .map_err(|_| DomainError::InvalidBinding)?;
        if self.idempotency_key.is_empty() || self.idempotency_key.len() > 256 {
            return Err(DomainError::Contract(crate::ContractError::InvalidArgument));
        }
        Ok(())
    }
    /// Select the exact deployment under the authenticated account.
    pub fn new(worker_name: ActorNamespace, idempotency_key: String) -> Result<Self, DomainError> {
        let request = Self {
            worker_name,
            idempotency_key,
            account_id: Vec::new(),
        };
        request.validate_from_proto()?;
        Ok(request)
    }
}
/// Successful named Worker deployment deletion.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile)]
#[derive(Clone, Debug, Default, Eq, PartialEq, TS)]
#[ts(export_to = "actors/DeleteWorkerResponse.ts")]
#[ts(type = "Record<never, never>")]
pub struct DeleteWorkerResponse {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_deletion_admission_borrows_original_fields() -> Result<(), DomainError> {
        let request =
            DeleteWorkerRequest::new(ActorNamespace::new("worker".into())?, "delete".repeat(32))?;
        let cost = acyclic_test_support::measure(|| request.validate_from_proto().unwrap());
        assert_eq!(cost.count_total, 0);
        Ok(())
    }
}
