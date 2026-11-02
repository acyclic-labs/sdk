use acyclic_fs::{NativeAllocation, WorkCounters};
use acyclic_harness_core::Result;
use bytes::Bytes;

struct Owned<T> {
    bytes: T,
    _custody: NativeAllocation,
}

impl<T: AsRef<[u8]>> AsRef<[u8]> for Owned<T> {
    fn as_ref(&self) -> &[u8] {
        self.bytes.as_ref()
    }
}

/// Attaches paid payload custody without copying its bytes. The Bytes owner
/// control allocation is admitted before Bytes constructs that allocation.
pub(crate) fn owned<T: AsRef<[u8]> + Send + 'static>(
    bytes: T,
    custody: NativeAllocation,
) -> Result<Bytes> {
    // Preserve payload-before-custody retirement even if control admission fails.
    let mut owner = Owned {
        bytes,
        _custody: custody,
    };
    let (layout, _) = std::alloc::Layout::new::<std::sync::atomic::AtomicUsize>()
        .extend(std::alloc::Layout::new::<Owned<T>>())
        .map_err(|_| acyclic_harness_core::Error::NativeBoundary {
            code: acyclic_harness_core::wire::ErrorCode::Invalid,
            reason: "provider Bytes owner size overflows",
        })?;
    let mut control = owner._custody.work().claim(
        layout.pad_to_align().size() as u64,
        1,
        WorkCounters::UNCHARGED,
    )?;
    owner._custody.absorb(&mut control)?;
    Ok(Bytes::from_owner(owner))
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_fs::{CancellationToken, NativeMaterializationWork};
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    };

    struct Observed {
        bytes: Option<acyclic_auth::operation::ProtectedBytes>,
        work: NativeMaterializationWork,
        retired_at: Arc<AtomicU64>,
    }

    impl AsRef<[u8]> for Observed {
        fn as_ref(&self) -> &[u8] {
            self.bytes
                .as_ref()
                .expect("live protected payload")
                .as_ref()
        }
    }

    impl Drop for Observed {
        fn drop(&mut self) {
            drop(self.bytes.take());
            self.retired_at
                .store(self.work.live_allocation_bytes(), Ordering::SeqCst);
        }
    }

    #[test]
    fn refused_control_retires_protected_payload_before_its_credit() -> Result<()> {
        const CAPACITY: u64 = 4096;
        let baseline = NativeMaterializationWork::accounting_state_bytes();
        for release_first in [false, true] {
            let mut budget = acyclic_fs::performance::work_budget(100_000, 8_000_000);
            budget.peak_allocation_bytes = baseline + CAPACITY;
            let work = NativeMaterializationWork::new(budget, &CancellationToken::new()).map_err(
                |(cause, work)| acyclic_harness_core::Error::NativeConstruction { cause, work },
            )?;
            let custody = work.claim(CAPACITY, 0, WorkCounters::UNCHARGED)?;
            let retired_at = Arc::new(AtomicU64::new(u64::MAX));
            let payload = Observed {
                bytes: Some(acyclic_auth::operation::ProtectedBytes::new(
                    zeroize::Zeroizing::new(vec![b'Z'; CAPACITY as usize]),
                )),
                work: work.clone(),
                retired_at: retired_at.clone(),
            };
            if release_first {
                // Restore the former parameter drop order on admission refusal.
                drop(custody);
                drop(payload);
                assert_eq!(retired_at.load(Ordering::SeqCst), baseline);
            } else {
                let error = owned(payload, custody).expect_err("control needs overlapping credit");
                assert!(matches!(
                    error,
                    acyclic_harness_core::Error::NativeWork {
                        cause: acyclic_fs::NativeTreeMaterializationError::Work(
                            acyclic_fs::WorkError::BudgetExceeded {
                                counter: "peak_allocation_bytes",
                                ..
                            }
                        ),
                        ..
                    }
                ));
                assert_eq!(retired_at.load(Ordering::SeqCst), baseline + CAPACITY);
            }
            assert_eq!(work.live_allocation_bytes(), baseline);
        }
        Ok(())
    }
}
