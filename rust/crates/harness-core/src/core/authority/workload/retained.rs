//! One exact, funded wire identity for a retained authenticated proof.
use super::*;
use std::sync::Arc;

pub(super) struct RetainedAuthentication<T> {
    // Workload wire includes an attenuator secret. It has the same zeroizing
    // lifetime as Scope, including failed/replaced cache entries.
    wire: zeroize::Zeroizing<Vec<u8>>,
    proof: Arc<T>,
    #[cfg(feature = "filesystem")]
    _custody: Option<crate::filesystem::PreparationCustody>,
}

#[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    #[test]
    fn exact_wire_custody_retires_and_cancellation_precedes_lookup() -> Result<()> {
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(1_000_000, 1_000_000),
            &cancellation,
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
        let before = work.receipt();
        let live = work.live_allocation_bytes();
        let owner = RetainedAuthentication::new(
            b"authenticated",
            Arc::new(7),
            &mut Some(&mut io.history_io()),
        )?
        .ok_or_else(|| io.invalid("native observer must transfer custody"))?;
        assert!(work.receipt().allocation_operations > before.allocation_operations);
        assert!(work.live_allocation_bytes() > live);
        assert!(
            owner
                .get(b"authenticatEd", &mut Some(&mut io.history_io()))?
                .is_none()
        );
        assert_eq!(
            *owner
                .get(b"authenticated", &mut Some(&mut io.history_io()))?
                .unwrap(),
            7
        );
        cancellation.cancel();
        assert!(
            owner
                .get(b"authenticated", &mut Some(&mut io.history_io()))
                .is_err()
        );
        drop(owner);
        assert_eq!(work.live_allocation_bytes(), live);
        Ok(())
    }
}

impl<T> RetainedAuthentication<T> {
    pub(super) fn new(
        encoded: &[u8],
        proof: Arc<T>,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Option<Self>> {
        let measured = work.is_some();
        let mut quota = CodecQuota::new(work, 0)?;
        let wire = zeroize::Zeroizing::new(copying::copy_copy_vector(encoded, &mut quota)?);
        #[cfg(feature = "filesystem")]
        let _custody = quota.take_owned_custody()?;
        #[cfg(feature = "filesystem")]
        if measured && _custody.is_none() {
            return Ok(None);
        }
        #[cfg(not(feature = "filesystem"))]
        if measured {
            return Ok(None);
        }
        #[cfg(not(feature = "filesystem"))]
        quota.transfer();
        Ok(Some(Self {
            wire,
            proof,
            #[cfg(feature = "filesystem")]
            _custody,
        }))
    }

    pub(super) fn get(
        &self,
        encoded: &[u8],
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Option<Arc<T>>> {
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
            // Exact comparison is real work, but it is not hashing. Charge
            // every candidate byte before comparing, including a mismatch.
            io.codec_charge(crate::executor::json::JsonCodecCharge::Work {
                bytes: 0,
                items: encoded.len() as u64,
            })?;
        }
        Ok((self.wire.as_slice() == encoded).then(|| self.proof.clone()))
    }
}
