//! Exhaustive bounded publication histories over the production state lock,
//! transaction engine/apply, and the same get_locked used by public reads.
use super::*;
use crate::sync::{Arc as ModelArc, Mutex};
use loom::sync::atomic::{AtomicBool, Ordering};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Commit,
    FailAction,
    FailReceipt,
    FailCommit,
    Cancel,
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Fault {
    None,
    SplitState,
    SplitReceipt,
}

fn fixture() -> ModelArc<(MemoryObjects, AtomicBool)> {
    let (core, bucket) = MemoryObjects::with_default_bucket();
    core.put_locked(
        &mut core.lock_state().unwrap(),
        &wire::PutObjectHeader {
            bucket: Some(bucket),
            object_key: "value".into(),
            ..Default::default()
        },
        OwnedBytes::from_static(b"old"),
    )
    .unwrap();
    ModelArc::new((core, AtomicBool::new(false)))
}

fn read(core: &MemoryObjects) -> u64 {
    let state = core.lock_state().unwrap();
    let value = get_locked(
        &state,
        &wire::GetObjectRequest {
            bucket: Some(wire::BucketRef {
                name: "default".into(),
            }),
            object_key: "value".into(),
            ..Default::default()
        },
        32,
    )
    .unwrap();
    let StoredBody::Memory(body) = value.body else {
        unreachable!()
    };
    let info = value.header.object.unwrap();
    let receipt = state.receipts.get("write-once");
    let all_old = state.sequence == 1
        && body.as_slice() == b"old"
        && state.object_bytes == 3
        && receipt.is_none()
        && state.receipts.is_empty();
    let all_new = state.sequence == 2
        && body.as_slice() == b"new-value"
        && state.object_bytes == 9
        && state.receipts.len() == 1
        && receipt.is_some_and(|receipt| {
            wire::ObjectInfo::decode(receipt.response.as_slice()).unwrap() == info
                && receipt
                    .put_info
                    .as_ref()
                    .is_some_and(|stored| **stored == info)
        });
    assert!(
        all_old || all_new,
        "torn publication: state, object and receipt disagree"
    );
    assert_eq!(info.size, body.len() as u64);
    state.sequence
}

fn write(core: &MemoryObjects, cancelled: &AtomicBool, mode: Mode, fault: Fault) -> bool {
    let mut state = core.lock_state().unwrap();
    let identity = Some(wire::MutationIdentity {
        idempotency_key: "write-once".into(),
    });
    let mut captured = None;
    let result: Result<wire::ObjectInfo, NativeObjectsError> = core.mutate_locked_engine(
        &mut state,
        ([9; 32], &identity),
        |receipt| {
            wire::ObjectInfo::decode(receipt.response.as_slice())
                .map_err(|_| Error::from(Unavailable).into())
        },
        |next| {
            let info = publish(
                next,
                "default",
                "value",
                None,
                &None,
                StoredBody::memory(OwnedBytes::from_static(b"new-value")),
            )?;
            if mode == Mode::FailAction {
                return Err(Error::from(Unavailable).into());
            }
            Ok(info)
        },
        |info, identity| {
            if mode == Mode::FailReceipt {
                return Err(Error::from(Unavailable).into());
            }
            Ok((
                identity.idempotency_key.clone().into(),
                Receipt {
                    digest: [9; 32],
                    response: info.encode_to_vec().into(),
                    put_info: Some(info.clone().into()),
                    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
                    kind: 2,
                },
            ))
        },
        |before, next| {
            // Native publication's last fallible callback checks authority and
            // cancellation before apply. Model an independently racing cancel.
            if mode == Mode::FailCommit
                || (mode == Mode::Cancel && cancelled.load(Ordering::Acquire))
            {
                return Err(std::io::Error::from(std::io::ErrorKind::Interrupted).into());
            }
            if fault != Fault::None {
                // Obtain the real engine's fully prepared candidate, leaving
                // the predecessor untouched. The broken writer below releases
                // its actual publication lock between two real apply calls.
                captured = Some(std::mem::replace(
                    next,
                    Transaction {
                        state: before.clone(),
                        object: None,
                        receipt: None,
                    },
                ));
                return Err(Error::from(Unavailable).into());
            }
            Ok(())
        },
    );
    if let Some(mut next) = captured {
        let receipt = next.receipt.take();
        let object = if fault == Fault::SplitState {
            next.object.take()
        } else {
            None
        };
        next.apply(&mut state);
        drop(state);
        loom::thread::yield_now();
        let mut state = core.lock_state().unwrap();
        Transaction {
            state: state.clone(),
            object,
            receipt,
        }
        .apply(&mut state);
        true
    } else {
        result.is_ok()
    }
}

fn history(mode: Mode, fault: Fault) {
    let shared = fixture();
    let writer = ModelArc::clone(&shared);
    let writer = loom::thread::spawn(move || write(&writer.0, &writer.1, mode, fault));
    let reader = ModelArc::clone(&shared);
    let reader = loom::thread::spawn(move || {
        read(&reader.0);
    });
    let cancel = (mode == Mode::Cancel).then(|| {
        let shared = ModelArc::clone(&shared);
        loom::thread::spawn(move || shared.1.store(true, Ordering::Release))
    });
    let committed = writer.join().unwrap();
    reader.join().unwrap();
    if let Some(cancel) = cancel {
        cancel.join().unwrap();
    }
    assert_eq!(read(&shared.0), if committed { 2 } else { 1 });
    if matches!(
        mode,
        Mode::FailAction | Mode::FailReceipt | Mode::FailCommit
    ) {
        assert!(!committed);
    }
}

#[test]
fn objects_loom_readers_observe_complete_commit_failure_or_cancellation() {
    for mode in [
        Mode::Commit,
        Mode::FailAction,
        Mode::FailReceipt,
        Mode::FailCommit,
        Mode::Cancel,
    ] {
        // One writer, one reader, and (for cancellation) one cancelling actor.
        // No permutation or preemption cutoff: explore all their interleavings.
        loom::model(move || history(mode, Fault::None));
    }
}

fn rejects_model(run: impl Fn() + Send + Sync + 'static, expected: &str) {
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| loom::model(run)))
        .expect_err("broken publication/lock ordering escaped the model");
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        message.contains(expected),
        "unexpected model failure: {message}"
    );
}

#[test]
fn objects_loom_rejects_split_state_object_and_receipt_publication() {
    for fault in [Fault::SplitState, Fault::SplitReceipt] {
        rejects_model(move || history(Mode::Commit, fault), "torn publication");
    }
}

#[test]
fn objects_loom_publication_lock_order_is_checked() {
    loom::model(|| {
        let shared = fixture();
        let _state = shared.0.lock_state().unwrap();
        let _key = shared.0.token_key.lock().unwrap();
        let tail = Mutex::<_, 3>::new(());
        let _tail = tail.lock().unwrap();
    });
    rejects_model(
        || {
            let shared = fixture();
            let _key = shared.0.token_key.lock().unwrap();
            let _state = shared.0.lock_state().unwrap();
        },
        "lock order: acquisition inversion",
    );
}
