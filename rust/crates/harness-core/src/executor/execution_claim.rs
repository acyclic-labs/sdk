//! Live execution ownership, separate from durable replay observations.
use super::*;

/// Shared exclusion for all handles of the same execution journal.
#[derive(Default)]
pub struct ExecutionClaims(acyclic_fs::OperationClaims);

#[cfg(feature = "filesystem")]
type Storage = Option<crate::filesystem::PreparationCustody>;
#[cfg(not(feature = "filesystem"))]
type Storage = ();

/// Owns a live attempt until its execution future has finished or is dropped.
pub struct ExecutionClaim<'a> {
    _claim: acyclic_fs::OperationClaim<'a>,
}

impl ExecutionClaims {
    /// Claims an operation before inspecting its replay observations.
    pub fn claim<'a>(
        &'a self,
        namespace: [u8; 32],
        operation: OperationId,
        io: Option<&PreparationIo>,
    ) -> Result<ExecutionClaim<'a>> {
        claim(&self.0, namespace, operation, io)
    }
}

pub(crate) fn claim<'a>(
    claims: &'a acyclic_fs::OperationClaims,
    namespace: [u8; 32],
    operation: OperationId,
    io: Option<&PreparationIo>,
) -> Result<ExecutionClaim<'a>> {
    let mut key = [0; 48];
    key[..32].copy_from_slice(&namespace);
    key[32..].copy_from_slice(&operation.into_bytes());
    let claim = claims
        .claim(
            key,
            || crate::model::charge_items(io, 1),
            |bytes, allocations| -> Result<Storage> {
                #[cfg(feature = "filesystem")]
                {
                    io.map(|io| {
                        for _ in 0..allocations {
                            io.allocation_attempt(bytes)?;
                        }
                        io.retain_owned(bytes)?;
                        io.take_owned_custody(bytes)
                    })
                    .transpose()
                }
                #[cfg(not(feature = "filesystem"))]
                {
                    let _ = (io, bytes, allocations);
                    Ok(())
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

#[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    #[test]
    fn claim_nodes_keep_original_native_funding_after_the_preparation_frame_drops() -> Result<()> {
        let native = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(1_000_000, 1_000_000),
            &acyclic_fs::CancellationToken::new(),
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let baseline = native.live_allocation_bytes();
        let claims = ExecutionClaims::default();
        let io = PreparationIo::from_native_work(&native)?;
        let first = claims.claim([1; 32], OperationId::from_bytes([1; 16]), Some(&io))?;
        let second = claims.claim([1; 32], OperationId::from_bytes([2; 16]), Some(&io))?;
        drop(io);
        assert!(native.live_allocation_bytes() > baseline);
        drop(first);
        assert!(native.live_allocation_bytes() > baseline);
        drop(second);
        assert_eq!(native.live_allocation_bytes(), baseline);
        Ok(())
    }
}
