use super::*;
use crate::kernel::allocation::effects::{OMIT_REFUND, OMIT_RESERVATION};
use crate::kernel::codec::{CanonicalDecodeError, Encoder};
use crate::sync::atomic::{AtomicU64, Ordering};
use crate::{CancellationToken, NativeMaterializationWork, WorkBudget, WorkCounters};
use std::{
    cell::{Cell, RefCell},
    sync::Arc,
};

loom::thread_local! {
    static EFFECT: RefCell<Option<(Arc<AtomicU64>, bool)>> = RefCell::new(None);
    static ADDITIVE: Cell<bool> = Cell::new(false);
}

pub(super) fn completed_effect(step: &EncodeWorkStep) {
    EFFECT.with(|witness| {
        if let Some((actual, pages)) = &*witness.borrow() {
            let amount = match step {
                EncodeWorkStep::Completed(work) => {
                    if *pages {
                        work.page_writes
                    } else {
                        work.bytes_encoded
                    }
                }
                EncodeWorkStep::Allocation(EncodeAllocationStep::HeaderWritten { bytes })
                    if !pages =>
                {
                    *bytes
                }
                _ => 0,
            };
            if amount != 0 {
                actual.fetch_add(amount, Ordering::Relaxed);
            }
        }
    });
}

fn encode<const BROKEN: bool>(
    work: &NativeMaterializationWork,
    cancellation: &CancellationToken,
    actual: Arc<AtomicU64>,
    pages: bool,
    storage: Option<EffectStorage>,
) {
    EFFECT.with(|witness| *witness.borrow_mut() = Some((actual, pages)));
    OMIT_RESERVATION.with(|flag| flag.set(BROKEN));
    let mut accounting = allocation::KernelAccounting::native(work, cancellation);
    if let Some((wire, mut output, owner)) = storage {
        let mut observer = EncodeAccounting {
            accounting: &mut accounting,
            owned_bytes: output.len() as u64,
            allocation: Some(allocation::KernelOwnedAllocation::NativeOriginal(owner)),
            pending: WorkCounters::UNCHARGED,
        };
        let delta = WorkCounters {
            bytes_encoded: wire.len() as u64,
            ..WorkCounters::UNCHARGED
        };
        // Bound the model to the protected effect. Storage is funded
        // before contention, as in the read-credit model. Admission, page-step
        // mapping, completion and refund are the production observer paths;
        // full writer/capacity callback sequences are covered by the byte oracle.
        let (before, after) = if pages {
            (
                crate::kernel::blob::page_write_step(EncodeWorkStep::Admit(delta)),
                crate::kernel::blob::page_write_step(EncodeWorkStep::Completed(delta)),
            )
        } else {
            (
                EncodeWorkStep::Allocation(EncodeAllocationStep::BeforeHeader { bytes: 10 }),
                EncodeWorkStep::Allocation(EncodeAllocationStep::HeaderWritten { bytes: 10 }),
            )
        };
        if observer.observe(&before).is_ok() {
            output.copy_from_slice(&wire);
            assert_eq!(output, wire);
            let _ = observer.observe(&after);
        }
        drop(output);
        drop(observer);
    } else {
        let mut observer = EncodeAccounting::new(&mut accounting);
        let result = Encoder::with_exact_capacity(
            b"ACYFSFIL",
            1,
            10,
            |step| observer.observe(&EncodeWorkStep::Allocation(step)),
            |_| crate::FsError::from(CanonicalDecodeError::AllocationFailed),
        )
        .map(Encoder::finish);
        if let Ok((bytes, allocation)) = observer.finish(result) {
            assert_eq!(bytes, b"ACYFSFIL\x01\x00");
            drop(bytes);
            drop(allocation);
        }
    }
    EFFECT.with(|witness| *witness.borrow_mut() = None);
}

type EffectStorage = (Vec<u8>, Vec<u8>, crate::NativeAllocation);

fn effect_storage(work: &NativeMaterializationWork, pages: bool) -> EffectStorage {
    let page = crate::kernel::BlobPage {
        first_offset: 0,
        end_offset: 1,
        node: crate::kernel::BlobNode::Leaf(vec![crate::kernel::BlobChunkRef {
            first_offset: 0,
            end_offset: 1,
            chunk: crate::ObjectId {
                kind: crate::ObjectKind::BlobChunk,
                digest: crate::Digest::from_bytes([1; 32]),
            },
        }]),
    };
    let wire = if pages {
        crate::kernel::encode_blob_page(
            &page,
            1,
            |_| Ok::<_, CanonicalDecodeError>(()),
            |_| CanonicalDecodeError::AllocationFailed,
        )
        .unwrap()
    } else {
        b"ACYFSFIL\x01\x00".to_vec()
    };
    let owner = work
        .claim(wire.len() as u64, 1, WorkCounters::UNCHARGED)
        .unwrap();
    let output = vec![0; wire.len()];
    (wire, output, owner)
}

fn race<const BROKEN: bool, const ASSERT_LIMIT: bool>(
    maximum: u64,
    pages: bool,
    resource: bool,
    cancel: bool,
    bounded: bool,
) {
    loom::model(move || {
        let cancellation = CancellationToken::new();
        let mut budget = WorkBudget::UNBOUNDED;
        if pages {
            budget.page_writes = maximum;
        } else {
            budget.bytes_encoded = maximum;
        }
        let original = NativeMaterializationWork::new(
            if resource {
                WorkBudget::UNBOUNDED
            } else {
                budget
            },
            &cancellation,
        )
        .unwrap();
        let work = if resource {
            original.native_volume_work(budget).unwrap()
        } else {
            original.clone()
        };
        let baseline = original.live_allocation_bytes();
        let worker_page = bounded.then(|| effect_storage(&work, pages));
        let local_page = bounded.then(|| effect_storage(&work, pages));
        // Standard Arc only transports owners pinned through join. All mutable
        // production leaf/resource/cancellation state and the witness use sync.
        let actual = Arc::new(AtomicU64::new(0));
        let worker_work = work.clone();
        let worker_cancel = cancellation.clone();
        let worker_actual = actual.clone();
        let worker = loom::thread::spawn(move || {
            encode::<BROKEN>(
                &worker_work,
                &worker_cancel,
                worker_actual,
                pages,
                worker_page,
            )
        });
        if cancel {
            cancellation.cancel();
        } else {
            encode::<BROKEN>(&work, &cancellation, actual.clone(), pages, local_page);
        }
        worker.join().unwrap();
        let completed = actual.load(Ordering::Relaxed);
        if ASSERT_LIMIT {
            assert!(
                completed <= maximum,
                "concurrent encoders must not exceed admitted work"
            );
        }
        let receipt = original.receipt();
        assert_eq!(
            if pages {
                receipt.page_writes
            } else {
                receipt.bytes_encoded
            },
            completed,
            "every completed effect remains charged"
        );
        assert_eq!(original.live_allocation_bytes(), baseline);
    });
}

#[test]
fn concurrent_headers_reserve_zero_one_or_both_allowances() {
    for maximum in [0, 10, 20] {
        race::<false, true>(maximum, false, false, false, false);
        race::<false, true>(maximum, false, false, false, true);
    }
    race::<false, true>(10, false, true, false, false);
    race::<false, true>(10, false, false, true, false);
}

#[test]
fn concurrent_blob_pages_reserve_the_page_write_charge() {
    for maximum in [0, 1, 2] {
        race::<false, true>(maximum, true, false, false, true);
    }
}

fn rejected(panic: Box<dyn std::any::Any + Send>, expected: &str) {
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(message.contains(expected), "{message}");
}
#[test]
fn check_only_admission_and_deleting_the_bound_assertion_are_detected() {
    for (maximum, pages) in [(10, false), (1, true)] {
        let panic =
            std::panic::catch_unwind(|| race::<true, true>(maximum, pages, false, false, true))
                .expect_err("the old check-only admission must violate its allowance");
        rejected(panic, "concurrent encoders must not exceed admitted work");
        race::<true, false>(maximum, pages, false, false, true);
    }
}

fn refund_model<const OMIT: bool>(action: u8) {
    loom::model(move || {
        let cancellation = CancellationToken::new();
        let root = NativeMaterializationWork::new(
            WorkBudget {
                bytes_encoded: 10,
                ..WorkBudget::UNBOUNDED
            },
            &cancellation,
        )
        .unwrap();
        let parent = root
            .native_volume_work(WorkBudget {
                bytes_encoded: 10,
                ..WorkBudget::UNBOUNDED
            })
            .unwrap();
        let child = parent
            .native_volume_work(WorkBudget {
                bytes_encoded: 10,
                ..WorkBudget::UNBOUNDED
            })
            .unwrap();
        let mut accounting = allocation::KernelAccounting::native(&child, &cancellation);
        let mut observer = EncodeAccounting::new(&mut accounting);
        observer
            .admit(WorkCounters {
                bytes_encoded: 10,
                ..WorkCounters::UNCHARGED
            })
            .unwrap();
        observer
            .admit(WorkCounters {
                bytes_encoded: 4,
                ..WorkCounters::UNCHARGED
            })
            .unwrap();
        let actual = Arc::new(AtomicU64::new(0));
        let worker_root = root.clone();
        let worker_cancel = cancellation.clone();
        let worker_actual = actual.clone();
        let worker = loom::thread::spawn(move || {
            encode::<false>(&worker_root, &worker_cancel, worker_actual, false, None)
        });
        let completed = if action == 3 { 4 } else { 0 };
        if completed != 0 {
            let mut bytes = [0; 4];
            bytes.copy_from_slice(std::hint::black_box(b"data"));
            assert_eq!(std::hint::black_box(bytes), *b"data");
            observer
                .record_completed(WorkCounters {
                    bytes_encoded: 4,
                    ..WorkCounters::UNCHARGED
                })
                .unwrap();
        }
        if action == 1 {
            cancellation.cancel();
        }
        OMIT_REFUND.with(|flag| flag.set(OMIT));
        if action == 2 {
            let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                let _observer = observer;
                panic!("abandoned writer callback");
            }));
            assert!(panic.is_err());
        } else {
            drop(observer);
        }
        OMIT_REFUND.with(|flag| flag.set(false));
        worker.join().unwrap();
        let worker_completed = actual.load(Ordering::Relaxed);
        let reserved = 10 + worker_completed;
        let charged = completed + worker_completed;
        for work in [&root, &parent, &child] {
            let (pending, receipt, remaining) = work.state(|state| {
                let pending = state.original_budget().bytes_encoded - state.budget.bytes_encoded;
                (
                    pending,
                    state.work.bytes_encoded,
                    state.remaining().map(|remaining| remaining.bytes_encoded),
                )
            });
            assert_eq!(
                pending + receipt + (10 - completed),
                reserved,
                "outstanding + charged + refunded must equal reserved"
            );
            assert_eq!(pending, 0, "abandoned encoding credit must be returned");
            assert_eq!(receipt, charged);
            assert_eq!(remaining.unwrap(), 10 - charged);
        }
        // The sibling uses the root directly: ancestor and child limits must
        // recover all of their unused credit independently of root spending.
        for work in [&parent, &child] {
            assert_eq!(
                work.resource
                    .as_ref()
                    .unwrap()
                    .remaining()
                    .unwrap()
                    .bytes_encoded,
                10 - completed
            );
        }
    });
}
#[test]
fn abandonment_cancellation_panic_and_partial_completion_refund_credit() {
    for action in 0..4 {
        refund_model::<false>(action);
    }
}
#[test]
fn missing_refund_is_detected_by_conservation() {
    let panic = std::panic::catch_unwind(|| refund_model::<true>(0))
        .expect_err("lost refunds must fail conservation");
    rejected(
        panic,
        "outstanding + charged + refunded must equal reserved",
    );
}

pub(super) fn additive_reservation() -> bool {
    ADDITIVE.with(Cell::get)
}
fn overlapping_model<const BROKEN: bool, const ASSERT_COMPLETE: bool>() {
    loom::model(|| {
        let cancellation = CancellationToken::new();
        let root = NativeMaterializationWork::new(
            WorkBudget {
                bytes_encoded: 22,
                ..WorkBudget::UNBOUNDED
            },
            &cancellation,
        )
        .unwrap();
        let work = root
            .native_volume_work(WorkBudget {
                bytes_encoded: 22,
                ..WorkBudget::UNBOUNDED
            })
            .unwrap();
        let baseline = root.live_allocation_bytes();
        let actual = Arc::new(AtomicU64::new(0));
        EFFECT.with(|witness| *witness.borrow_mut() = Some((actual.clone(), false)));
        ADDITIVE.with(|flag| flag.set(BROKEN));
        let mut accounting = allocation::KernelAccounting::native(&work, &cancellation);
        let result = super::super::encode_owned(&mut accounting, |observe, reserve| {
            crate::kernel::encode_file_metadata(
                crate::kernel::FileMetadata::default(),
                observe,
                reserve,
            )
        });
        if let Ok((bytes, owner)) = result {
            assert_eq!(bytes.len(), 22);
            drop(bytes);
            drop(owner);
        }
        ADDITIVE.with(|flag| flag.set(false));
        EFFECT.with(|witness| *witness.borrow_mut() = None);
        let completed = actual.load(Ordering::Relaxed);
        if ASSERT_COMPLETE {
            assert_eq!(
                completed, 22,
                "an aggregate and its fields must share the exact allowance"
            );
        }
        assert_eq!(root.receipt().bytes_encoded, completed);
        assert_eq!(root.live_allocation_bytes(), baseline);
    });
}
#[test]
fn aggregate_header_and_fields_share_one_exact_reservation() {
    overlapping_model::<false, true>();
}
#[test]
fn double_reservation_and_deleting_the_completion_assertion_are_detected() {
    let panic = std::panic::catch_unwind(overlapping_model::<true, true>)
        .expect_err("additive nested checks must fail at the exact quota");
    rejected(
        panic,
        "an aggregate and its fields must share the exact allowance",
    );
    overlapping_model::<true, false>();
}
