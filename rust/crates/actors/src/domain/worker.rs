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
        Self::new(self.worker_name.clone(), self.idempotency_key.clone()).map(|_| ())
    }
    /// Select the exact deployment under the authenticated account.
    pub fn new(worker_name: ActorNamespace, idempotency_key: String) -> Result<Self, DomainError> {
        crate::addressing::validate_dns_label(worker_name.as_str().as_bytes())
            .map_err(|_| DomainError::InvalidBinding)?;
        if idempotency_key.is_empty() || idempotency_key.len() > 256 {
            return Err(DomainError::Contract(crate::ContractError::InvalidArgument));
        }
        Ok(Self {
            worker_name,
            idempotency_key,
            account_id: Vec::new(),
        })
    }
}
/// Successful named Worker deployment deletion.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile)]
#[derive(Clone, Debug, Default, Eq, PartialEq, TS)]
#[ts(export_to = "actors/DeleteWorkerResponse.ts")]
#[ts(type = "Record<never, never>")]
pub struct DeleteWorkerResponse {}
