use super::*;
use acyclic_stream::OwnedBytes;

// Slice callers need an owned mutation payload. Encoded journal bytes already
// carry that ownership and can share their backing for the awaited publication.
pub(super) trait ContentInput: AsRef<[u8]> + Sync {
    fn mutation_bytes(&self, io: Option<&PreparationIo>) -> Result<OwnedBytes>;
}

impl ContentInput for [u8] {
    fn mutation_bytes(&self, io: Option<&PreparationIo>) -> Result<OwnedBytes> {
        fork_preparer_io::clone_bytes(self, io)
    }
}

impl ContentInput for OwnedBytes {
    fn mutation_bytes(&self, io: Option<&PreparationIo>) -> Result<OwnedBytes> {
        if let Some(io) = io {
            io.check_cancellation()?;
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = io.original_native_work() {
                if !self.matches_meter(&work.runtime_meter()) {
                    return Err(stock_boundary(
                        Some(io),
                        crate::wire::ErrorCode::Unauthorized,
                        "native content belongs to another allocation ledger",
                    ));
                }
                return Ok(self.clone());
            }
        }
        // Ordinary Bytes can allocate a sharing header on their first clone.
        // Retain the existing funded copy outside original native custody.
        fork_preparer_io::clone_bytes(self.as_ref(), io)
    }
}

#[cfg(all(test, feature = "native-execution", not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    #[test]
    fn owned_content_staging_shares_only_original_custody() -> Result<()> {
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(1_000_000, 8 * 1024 * 1024),
            &cancellation,
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let baseline = work.live_allocation_bytes();
        let io = PreparationIo::from_native_work(&work)?;
        let encoded = io.encode(&"q".repeat(128 * 1024), u64::MAX, false)?;
        let original = encoded.as_ptr();
        let copied = work.receipt().bytes_copied;
        let mut shared = None;
        let physical = acyclic_test_support::measure(|| {
            shared = Some(encoded.mutation_bytes(Some(&io)));
        });
        let shared = shared.expect("staging ran")?;
        assert_eq!(work.receipt().bytes_copied, copied);
        assert_eq!(physical.count_total, 0);
        assert_eq!(shared.as_ptr(), original);
        drop(encoded);
        assert!(shared.matches_meter(&work.runtime_meter()));
        assert_eq!(shared.len(), 128 * 1024 + 2);
        assert!(work.live_allocation_bytes() > baseline);

        let foreign = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(1_000_000, 8 * 1024 * 1024),
            &acyclic_fs::CancellationToken::new(),
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let foreign_io = PreparationIo::from_native_work(&foreign)?;
        assert_eq!(
            shared.mutation_bytes(Some(&foreign_io)).unwrap_err().code(),
            crate::wire::ErrorCode::Unauthorized
        );
        let plain = OwnedBytes::from(vec![1, 2, 3]);
        assert_eq!(
            plain.mutation_bytes(Some(&io)).unwrap_err().code(),
            crate::wire::ErrorCode::Unauthorized
        );

        let borrowed = shared.as_ref().mutation_bytes(Some(&io))?;
        assert_eq!(borrowed.as_ref(), shared.as_ref());
        assert_ne!(borrowed.as_ptr(), shared.as_ptr());
        assert!(borrowed.matches_meter(&work.runtime_meter()));
        assert_eq!(work.receipt().bytes_copied, copied + shared.len() as u64);
        drop(borrowed);
        cancellation.cancel();
        assert_eq!(
            shared.mutation_bytes(Some(&io)).unwrap_err().code(),
            crate::wire::ErrorCode::Cancelled
        );
        drop(shared);
        drop(io);
        assert_eq!(work.live_allocation_bytes(), baseline);
        Ok(())
    }
}
