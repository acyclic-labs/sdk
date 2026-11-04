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
    let mut control = owner._custody.work().claim(
        acyclic_auth::operation::bytes_owner_allocation_bytes::<Owned<T>>() as u64,
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
    use proptest::prelude::*;
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    };

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]

        #[test]
        fn body_credit_covers_the_actual_owner_allocation_and_last_slice(
            length in 1usize..4096,
        ) {
            for complete_control in [true, false] {
                let work = NativeMaterializationWork::new(
                    acyclic_fs::performance::work_budget(100_000, 4_000_000),
                    &CancellationToken::new(),
                ).unwrap();
                let baseline = work.live_allocation_bytes();
                let bytes = vec![b'c'; length];
                let capacity = bytes.capacity() as u64;
                let control = acyclic_auth::operation::bytes_owner_allocation_bytes::<Owned<Vec<u8>>>() as u64;
                let custody = work.claim(
                    capacity + if complete_control { 0 } else { std::mem::size_of::<Owned<Vec<u8>>>() as u64 },
                    1,
                    WorkCounters::UNCHARGED,
                ).unwrap();
                let mut body = None;
                let allocated = acyclic_test_support::measure(|| {
                    body = Some(if complete_control {
                        owned(bytes, custody).unwrap()
                    } else {
                        // Restore the payload-plus-owner claim without Bytes' refcount.
                        Bytes::from_owner(Owned { bytes, _custody: custody })
                    });
                });
                prop_assert_eq!(allocated.count_total, 1);
                prop_assert_eq!(allocated.bytes_total, control);
                let credited = work.live_allocation_bytes() - baseline;
                prop_assert_eq!(credited == capacity + allocated.bytes_total, complete_control);
                let body = body.unwrap();
                let retained = body.slice(length - 1..);
                drop(body);
                prop_assert_eq!(retained.as_ref(), b"c");
                prop_assert_eq!(work.live_allocation_bytes() - baseline, credited);
                drop(retained);
                prop_assert_eq!(work.live_allocation_bytes(), baseline);
            }
        }
    }

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
