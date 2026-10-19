//! Live execution ownership, separate from durable replay observations.
use super::*;

/// Shared exclusion for all handles of the same execution journal.
#[derive(Default)]
pub struct ExecutionClaims(acyclic_fs::OperationClaims);

#[cfg(feature = "filesystem")]
type Storage<'a> = Option<crate::filesystem::DecodeReservation<'a>>;
#[cfg(not(feature = "filesystem"))]
type Storage<'a> = std::marker::PhantomData<&'a PreparationIo>;

/// Owns a live attempt until its execution future has finished or is dropped.
pub struct ExecutionClaim<'a> {
    _claim: acyclic_fs::OperationClaim<'a, Storage<'a>>,
}

impl ExecutionClaims {
    /// Claims an operation before inspecting its replay observations.
    pub fn claim<'a>(
        &'a self,
        namespace: [u8; 32],
        operation: OperationId,
        io: Option<&'a PreparationIo>,
    ) -> Result<ExecutionClaim<'a>> {
        claim(&self.0, namespace, operation, io)
    }
}

pub(crate) fn claim<'a>(
    claims: &'a acyclic_fs::OperationClaims,
    namespace: [u8; 32],
    operation: OperationId,
    io: Option<&'a PreparationIo>,
) -> Result<ExecutionClaim<'a>> {
    let mut key = [0; 48];
    key[..32].copy_from_slice(&namespace);
    key[32..].copy_from_slice(&operation.into_bytes());
    let claim = claims
        .claim(
            key,
            || crate::model::charge_items(io, 1),
            |bytes| {
                #[cfg(feature = "filesystem")]
                {
                    io.map(|io| {
                        let storage = io.guard_owned(bytes)?;
                        io.allocation_attempt(bytes)?;
                        Ok::<_, Error>(storage)
                    })
                    .transpose()
                }
                #[cfg(not(feature = "filesystem"))]
                {
                    let _ = (io, bytes);
                    Ok::<_, Error>(std::marker::PhantomData)
                }
            },
        )?
        .ok_or_else(|| {
            stock_boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "operation already has a live executor",
            )
        })?;
    Ok(ExecutionClaim { _claim: claim })
}
