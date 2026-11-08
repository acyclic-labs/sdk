//! Physical Blob funding through withdrawal and final aliases.
#![cfg(target_arch = "wasm32")]

use acyclic_fs::kernel::RetainedAllocation;
use acyclic_fs::kernel::allocation::{AllocationLedger, KernelAccounting};
use acyclic_fs::{CancellationToken, ObjectStoreError, WorkBudget, WorkCounters};
use futures::FutureExt;
use std::cell::RefCell;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;
use wasm_bindgen_test::wasm_bindgen_test;

#[wasm_bindgen(inline_js = r#"
export function heldBlob(bytes) {
    const blob = new Blob([bytes]);
    const original = blob.arrayBuffer;
    let release;
    const gate = new Promise(resolve => { release = resolve; });
    let ready;
    const materialized = new Promise(resolve => { ready = resolve; });
    let result;
    let calls = 0;
    blob.arrayBuffer = function() {
        calls += 1;
        result = original.call(this).then(buffer => {
            ready();
            return gate.then(() => buffer);
        });
        return result;
    };
    return {blob, ready: () => materialized, calls: () => calls,
        release: () => { release(); return Promise.resolve(result); }};
}
"#)]
extern "C" {
    #[wasm_bindgen(js_name = heldBlob)]
    fn held_blob(bytes: &[u8]) -> HeldBlob;
    type HeldBlob;
    #[wasm_bindgen(method, getter)]
    fn blob(this: &HeldBlob) -> web_sys::Blob;
    #[wasm_bindgen(method)]
    fn ready(this: &HeldBlob) -> js_sys::Promise;
    #[wasm_bindgen(method)]
    fn calls(this: &HeldBlob) -> u32;
    #[wasm_bindgen(method)]
    fn release(this: &HeldBlob) -> js_sys::Promise;
}

fn live(accounting: &RefCell<KernelAccounting<'_>>) -> u64 {
    accounting
        .borrow_mut()
        .standalone_parts()
        .unwrap()
        .0
        .live_bytes()
}

#[wasm_bindgen_test]
async fn physical_blob_custody_survives_withdrawal_and_final_alias() -> Result<(), JsValue> {
    for length in [1, 17, 4096] {
        let expected = vec![73; length];
        let held = held_blob(&expected);
        let blob = held.blob();
        let cancellation = CancellationToken::new();
        let mut ledger = AllocationLedger::default();
        let mut work = WorkCounters::UNCHARGED;
        let accounting = RefCell::new(KernelAccounting::borrowed_standalone(
            &mut ledger,
            &mut work,
            WorkBudget {
                peak_allocation_bytes: 0,
                ..WorkBudget::UNBOUNDED
            },
            &cancellation,
        ));
        assert!(
            acyclic_fs::browser::blob_bytes(&blob, length as u64, &accounting, &cancellation,)
                .await
                .is_err()
        );
        assert_eq!(held.calls(), 0, "denied storage started physical work");
        assert_eq!(live(&accounting), 0);

        let mut ledger = AllocationLedger::default();
        let mut work = WorkCounters::UNCHARGED;
        let cancellation = CancellationToken::new();
        let accounting = RefCell::new(KernelAccounting::borrowed_standalone(
            &mut ledger,
            &mut work,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ));
        let mut baseline = None;
        for _ in 0..2 {
            let held = held_blob(&expected);
            let blob = held.blob();
            let _ = held.release();
            let bytes =
                acyclic_fs::browser::blob_bytes(&blob, length as u64, &accounting, &cancellation)
                    .await
                    .expect("physical producer retired before settlement");
            assert_eq!(&*bytes, expected.as_slice());
            assert_eq!(
                bytes.retained_capacity(),
                Some(length),
                "Blob carrier erased original vector capacity"
            );
            assert!(
                bytes.backing().is_some(),
                "physical backing identity was erased"
            );
            let alias = bytes.clone();
            let funded = live(&accounting);
            assert!(
                funded >= length as u64,
                "returned Blob backing lost original allocation credit"
            );
            drop(bytes);
            assert_eq!(live(&accounting), funded);
            drop(alias);
            let retired = live(&accounting);
            // The promoted ledger's own control remains with the caller.
            assert!(funded >= retired + length as u64);
            if let Some(baseline) = baseline {
                assert_eq!(retired, baseline, "completed reads retained storage");
            }
            baseline = Some(retired);
        }
        let baseline = baseline.unwrap();
        for cancelled in [false, true] {
            let held = held_blob(&expected);
            let blob = held.blob();
            let cancellation = CancellationToken::new();
            let mut read = Box::pin(acyclic_fs::browser::blob_bytes(
                &blob,
                length as u64,
                &accounting,
                &cancellation,
            ));
            let ready = JsFuture::from(held.ready()).fuse();
            futures::pin_mut!(ready);
            futures::select_biased! {
                result = read.as_mut().fuse() => panic!("held physical read completed: {result:?}"),
                result = ready => { result?; }
            }
            assert_eq!(held.calls(), 1);
            assert!(
                live(&accounting) >= baseline + length as u64,
                "physical Blob lost funding before delivery"
            );
            if cancelled {
                cancellation.cancel();
                assert!(matches!(read.await, Err(ObjectStoreError::Cancelled)));
            } else {
                drop(read);
            }
            assert!(
                live(&accounting) >= baseline + length as u64,
                "withdrawal retired the physical Blob allocation"
            );
            // Both consumers observe the same physical Promise. The submitted
            // producer registered its reaction before this settlement waiter.
            JsFuture::from(held.release()).await?;
            assert_eq!(
                live(&accounting),
                baseline,
                "settled abandoned producer retained storage"
            );
        }
        // Withdrawing delivery during the first real Promise must not discard
        // the continuation that owns the second physical step and its buffers.
        for cancelled in [false, true] {
            let first = held_blob(&expected);
            let second = held_blob(&expected);
            let first_blob = first.blob();
            let second_blob = second.blob();
            let cancellation = CancellationToken::new();
            let payload =
                RetainedAllocation::claim((2 * length) as u64, 2, &mut accounting.borrow_mut())
                    .unwrap();
            let mut operation = Box::pin(acyclic_fs::browser::complete(
                || {
                    Ok(async move {
                        let first = JsFuture::from(first_blob.array_buffer()).await?;
                        let second = JsFuture::from(second_blob.array_buffer()).await?;
                        Ok::<_, JsValue>(payload.into_value((first, second)))
                    })
                },
                WorkCounters::UNCHARGED,
                &accounting,
                &cancellation,
            ));
            let ready = JsFuture::from(first.ready()).fuse();
            futures::pin_mut!(ready);
            futures::select_biased! {
                _ = operation.as_mut().fuse() => panic!("held physical pipeline completed"),
                result = ready => { result?; }
            }
            assert_eq!(second.calls(), 0);
            if cancelled {
                cancellation.cancel();
                assert!(matches!(operation.await, Err(ObjectStoreError::Cancelled)));
            } else {
                drop(operation);
            }
            JsFuture::from(first.release()).await?;
            JsFuture::from(second.ready()).await?;
            assert_eq!(first.calls(), 1);
            assert_eq!(second.calls(), 1);
            assert!(
                live(&accounting) >= baseline + (2 * length) as u64,
                "withdrawal retired an unfinished physical pipeline"
            );
            JsFuture::from(second.release()).await?;
            assert_eq!(
                live(&accounting),
                baseline,
                "completed physical pipeline retained storage"
            );
        }
    }
    Ok(())
}
