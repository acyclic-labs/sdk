use super::*;
use acyclic_fs::ObjectKind;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen::prelude::wasm_bindgen(inline_js = r#"
export function holdFirstOpfsLookup(directory) {
    const original = directory.getFileHandle;
    let release;
    const held = new Promise(resolve => { release = resolve; });
    const lookups = [];
    const files = [];
    let bodies = 0;
    directory.getFileHandle = function(...args) {
        const result = original.apply(this, args).then(handle => {
            const getFile = handle.getFile;
            handle.getFile = function(...args) {
                const result = getFile.apply(this, args).then(file => {
                    const arrayBuffer = file.arrayBuffer;
                    file.arrayBuffer = function(...args) {
                        bodies += 1;
                        return arrayBuffer.apply(this, args);
                    };
                    return file;
                });
                files.push(result);
                return result;
            };
            return handle;
        });
        lookups.push(result);
        return lookups.length === 1 ? held.then(() => result) : result;
    };
    return {
        calls: () => lookups.length,
        lookups: () => Promise.all(lookups.slice(1)),
        fileCalls: () => files.length,
        files: () => Promise.all(files),
        bodyCalls: () => bodies,
        release: () => {
            directory.getFileHandle = original;
            release();
        },
    };
}
"#)]
extern "C" {
    #[wasm_bindgen::prelude::wasm_bindgen(js_name = holdFirstOpfsLookup)]
    fn hold_first_opfs_lookup(directory: &FileSystemDirectoryHandle) -> LookupHold;

    type LookupHold;

    #[wasm_bindgen::prelude::wasm_bindgen(method)]
    fn calls(this: &LookupHold) -> u32;

    #[wasm_bindgen::prelude::wasm_bindgen(method)]
    fn lookups(this: &LookupHold) -> js_sys::Promise;

    #[wasm_bindgen::prelude::wasm_bindgen(method, js_name = fileCalls)]
    fn file_calls(this: &LookupHold) -> u32;

    #[wasm_bindgen::prelude::wasm_bindgen(method)]
    fn files(this: &LookupHold) -> js_sys::Promise;

    #[wasm_bindgen::prelude::wasm_bindgen(method, js_name = bodyCalls)]
    fn body_calls(this: &LookupHold) -> u32;

    #[wasm_bindgen::prelude::wasm_bindgen(method)]
    fn release(this: &LookupHold);
}

fn object(bytes: &'static [u8]) -> (ObjectId, OwnedBytes) {
    let bytes = OwnedBytes::from(bytes.to_vec());
    (
        ObjectId {
            storage: None,
            kind: ObjectKind::BlobChunk,
            digest: object_digest(&bytes),
        },
        bytes,
    )
}

fn unique_namespace(prefix: &str) -> String {
    format!("{prefix}-{}", js_sys::Date::now().to_bits())
}

#[wasm_bindgen_test]
async fn opfs_is_authoritative_authenticated_and_persistent() {
    let name = unique_namespace("acyclic-fs-opfs-authoritative-v1");
    let store = OpfsAcceleratedObjectStore::open(&name, 1_024)
        .await
        .unwrap_or_else(|error| unreachable!("OPFS open failed: {error}"));
    let (object_id, bytes) = object(b"persistent-opfs-body");
    let cancellation = CancellationToken::new();
    {
        let __object_args = (
            object_id,
            bytes.clone(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        );
        let mut __object_work = acyclic_fs::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result =
            AsyncObjectStore::put(&store, __object_args.0, __object_args.1, &mut __object_work)
                .await;
        __object_work.receipt(__object_result)
    }
    .unwrap_or_else(|failure| unreachable!("OPFS put failed: {}", failure.error));
    drop(store);

    let reopened = OpfsAcceleratedObjectStore::open(&name, 1_024)
        .await
        .unwrap_or_else(|error| unreachable!("OPFS reopen failed: {error}"));
    let read = {
        let __object_args = (object_id, 1_024, WorkBudget::UNBOUNDED, &cancellation);
        let mut __object_work = acyclic_fs::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result = AsyncObjectStore::read(
            &reopened,
            __object_args.0,
            __object_args.1,
            &mut __object_work,
        )
        .await;
        __object_work.receipt(__object_result)
    }
    .unwrap_or_else(|failure| unreachable!("OPFS read failed: {}", failure.error));
    assert_eq!(read.value.bytes, bytes);

    reopened
        .write(
            object_id,
            &OwnedBytes::from(b"corrupt".to_vec()),
            &cancellation,
        )
        .await
        .unwrap_or_else(|error| unreachable!("corruption setup failed: {error}"));
    let failure = {
        let __object_args = (object_id, 1_024, WorkBudget::UNBOUNDED, &cancellation);
        let mut __object_work = acyclic_fs::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result = AsyncObjectStore::read(
            &reopened,
            __object_args.0,
            __object_args.1,
            &mut __object_work,
        )
        .await;
        __object_work.receipt(__object_result)
    }
    .err()
    .unwrap_or_else(|| unreachable!("corrupt OPFS body was accepted"));
    assert!(matches!(failure.error, ObjectStoreError::Corrupt));
}

#[wasm_bindgen_test]
async fn opfs_batches_preserve_order() {
    let name = unique_namespace("acyclic-fs-opfs-batch-v1");
    let store = OpfsAcceleratedObjectStore::open(&name, 1_024)
        .await
        .unwrap_or_else(|error| unreachable!("OPFS open failed: {error}"));
    let objects = [object(b"first"), object(b"second"), object(b"third")];
    let writes: Vec<_> = objects
        .iter()
        .map(|(object_id, bytes)| ObjectWrite {
            object_id: *object_id,
            bytes: bytes.clone(),
        })
        .collect();
    let cancellation = CancellationToken::new();
    {
        let __object_args = (&writes, WorkBudget::UNBOUNDED, &cancellation);
        let mut __object_work = acyclic_fs::AuthorityWork::new(__object_args.1, __object_args.2);
        let __object_result =
            AsyncObjectStore::put_many(&store, __object_args.0, &mut __object_work).await;
        __object_work.receipt(__object_result)
    }
    .unwrap_or_else(|failure| unreachable!("OPFS batch put failed: {}", failure.error));
    let requests: Vec<_> = objects
        .iter()
        .rev()
        .map(|(object_id, _)| ObjectReadRequest {
            object_id: *object_id,
            maximum_bytes: 1_024,
        })
        .collect();
    let reads = {
        let __object_args = (&requests, WorkBudget::UNBOUNDED, &cancellation);
        let mut __object_work = acyclic_fs::AuthorityWork::new(__object_args.1, __object_args.2);
        // Hold the actual first browser lookup, without replacing the OPFS
        // result. Independent members must enter their complete read pipeline.
        let (__object_result, progress) = {
            let hold = hold_first_opfs_lookup(&store.directory);
            let read = AsyncObjectStore::read_many(&store, __object_args.0, &mut __object_work);
            let mut read = std::pin::pin!(read);
            let mut completion = futures::poll!(read.as_mut());
            let progress = async {
                if completion.is_ready() {
                    return Err("the held lookup must keep the read pending");
                }
                if hold.calls() != 3 {
                    return Err("a held metadata lookup must not serialize siblings");
                }
                // Await only browser operations already dispatched above.
                // Each fixed poll advances one settled producer; no readiness
                // loop can hide a missing producer or a stage barrier.
                JsFuture::from(hold.lookups())
                    .await
                    .map_err(|_| "a dispatched OPFS lookup failed")?;
                completion = futures::poll!(read.as_mut());
                if completion.is_ready() || hold.file_calls() != 2 {
                    return Err("siblings must reach file metadata before the first lookup");
                }
                JsFuture::from(hold.files())
                    .await
                    .map_err(|_| "dispatched OPFS file metadata failed")?;
                completion = futures::poll!(read.as_mut());
                if completion.is_ready() || hold.body_calls() != 2 {
                    return Err("siblings must reach body reads before the first lookup");
                }
                Ok(())
            }
            .await;
            hold.release();
            let result = match completion {
                std::task::Poll::Ready(result) => result,
                std::task::Poll::Pending => read.await,
            };
            (result.map(|(values, _allocation)| values), progress)
        };
        assert_eq!(progress, Ok(()));
        __object_work.receipt(__object_result)
    }
    .unwrap_or_else(|failure| unreachable!("OPFS batch read failed: {}", failure.error));
    let actual: Vec<_> = reads.value.into_iter().map(|read| read.bytes).collect();
    let expected: Vec<_> = objects
        .iter()
        .rev()
        .map(|(_, bytes)| bytes.clone())
        .collect();
    assert_eq!(actual, expected);
}

#[wasm_bindgen_test]
async fn opfs_batch_retains_live_aliases_and_denies_unfunded_payloads() {
    let name = unique_namespace("acyclic-fs-opfs-retained-carriers-v1");
    let store = OpfsAcceleratedObjectStore::open(&name, 1_024)
        .await
        .unwrap_or_else(|error| unreachable!("OPFS open failed: {error}"));
    let writes: Vec<_> = (0_u8..32)
        .map(|index| {
            let bytes = OwnedBytes::from(vec![index; 1_024]);
            ObjectWrite {
                object_id: ObjectId {
                    storage: None,
                    kind: ObjectKind::BlobChunk,
                    digest: object_digest(&bytes),
                },
                bytes,
            }
        })
        .collect();
    let requests: Vec<_> = writes
        .iter()
        .map(|write| ObjectReadRequest {
            object_id: write.object_id,
            maximum_bytes: 1_024,
        })
        .collect();
    let cancellation = CancellationToken::new();
    {
        let __object_args = (&writes, WorkBudget::UNBOUNDED, &cancellation);
        let mut __object_work = acyclic_fs::AuthorityWork::new(__object_args.1, __object_args.2);
        let __object_result = store.put_many(__object_args.0, &mut __object_work).await;
        __object_work.receipt(__object_result)
    }
    .unwrap_or_else(|failure| unreachable!("OPFS setup failed: {}", failure.error));
    let mut work = acyclic_fs::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let (reads, allocation) = store
        .read_many(&requests, &mut work)
        .await
        .unwrap_or_else(|error| unreachable!("OPFS read failed: {error}"));
    let live = |work: &mut acyclic_fs::AuthorityWork<'_>| {
        work.accounting()
            .standalone_parts()
            .unwrap_or_else(|| unreachable!("ordinary browser accounting"))
            .0
            .live_bytes()
    };
    let payloads = requests.len() as u64 * 1_024;
    let required = payloads
        + allocation_bytes::<ObjectRead>(requests.len())
            .unwrap_or_else(|error| unreachable!("vector size failed: {error}"));
    assert!(live(&mut work) >= required);
    assert_eq!(work.work.items_returned, requests.len() as u64);
    for (read, write) in reads.iter().zip(&writes) {
        assert_eq!(read.bytes, write.bytes);
    }
    let cloned = reads[0].bytes.clone();
    drop(reads);
    drop(allocation);
    assert!(live(&mut work) >= 1_024);
    assert_eq!(cloned.as_ref(), writes[0].bytes.as_ref());
    let held = live(&mut work);
    drop(cloned);
    assert!(live(&mut work) <= held - 1_024);
    let denied = {
        let __object_args = (
            &requests,
            WorkBudget {
                // Every successful result must retain all payloads together.
                // This bound is independent of browser completion ordering.
                peak_allocation_bytes: payloads - 1,
                ..WorkBudget::UNBOUNDED
            },
            &cancellation,
        );
        let mut __object_work = acyclic_fs::AuthorityWork::new(__object_args.1, __object_args.2);
        let __object_result = store
            .read_many(__object_args.0, &mut __object_work)
            .await
            .map(|(values, _allocation)| values);
        __object_work.receipt(__object_result)
    }
    .err()
    .unwrap_or_else(|| unreachable!("retained carriers escaped admission"));
    assert!(denied.work.object_bytes_read <= payloads);
    assert!(matches!(denied.error, ObjectStoreError::Work(_)));

    // Complete sibling reads after failure to retain their work and allocation peak.
    store
        .write(
            writes[16].object_id,
            &OwnedBytes::from(vec![255; 1_024]),
            &cancellation,
        )
        .await
        .unwrap_or_else(|error| unreachable!("corruption setup failed: {error}"));
    let corrupt = {
        let __object_args = (&requests, WorkBudget::UNBOUNDED, &cancellation);
        let mut __object_work = acyclic_fs::AuthorityWork::new(__object_args.1, __object_args.2);
        let __object_result = store
            .read_many(__object_args.0, &mut __object_work)
            .await
            .map(|(values, _allocation)| values);
        __object_work.receipt(__object_result)
    }
    .err()
    .unwrap_or_else(|| unreachable!("corrupt OPFS body was accepted"));
    assert!(matches!(corrupt.error, ObjectStoreError::Corrupt));
    assert_eq!(corrupt.work.object_bytes_read, 32 * 1_024);
    assert!(corrupt.work.peak_allocation_bytes >= payloads - 1_024);
}
