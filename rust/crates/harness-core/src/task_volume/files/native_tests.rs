use super::{fixtures::*, *};
use acyclic_fs::{CancellationToken, NativeMaterializationWork};
use acyclic_stream::MemoryStream;
use proptest::prelude::*;

fn work(budget: acyclic_fs::WorkBudget) -> NativeMaterializationWork {
    NativeMaterializationWork::new(budget, &CancellationToken::new()).unwrap()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    // Reopen preserves every file and cut, spends the original ledger, and
    // releases all retained index/body custody with the last shared binding.
    #[test]
    fn native_reopen_is_bounded_and_keeps_original_index_custody(
        count in 2usize..12, payload in prop::collection::vec(any::<u8>(), 0..256),
    ) {
        futures::executor::block_on(async {
            let agent = AgentId::from_bytes([1; 16]);
            let volume = volume(agent);
            let (verifier, scope) = authority(Some(agent), &volume,
                &[VolumeOperation::Read, VolumeOperation::Write]);
            let provider = Arc::new(MemoryStream::default());
            let original = TaskFiles::open(provider.clone(), &volume, &verifier, &scope, 4096).await.unwrap();
            for i in 0..count {
                let key = FileKey::parse(&format!(".tasks/task/approvals/{i}/request.json")).unwrap();
                let file = FileValue::new(key, payload.clone());
                original.inner.append(original.inner.head().await.unwrap(), &file).await.unwrap();
                // Mutable replacement histories must not retain every old body.
                original.inner.append(original.inner.head().await.unwrap(), &FileValue::new(FileKey::Goal, vec![u8::try_from(i).unwrap()])).await.unwrap();
            }
            let expected = original.inner.snapshot(&Query::Root, 64).await.unwrap();
            let native = work(acyclic_fs::performance::BOUNDARY_BUDGET);
            let baseline = native.live_allocation_bytes();
            let reopened = TaskFiles::open_with_native_work(provider.clone(), &volume, &verifier, &scope, 4096, &native).await.unwrap();
            let held = native.live_allocation_bytes();
            prop_assert!(held > baseline);
            let actual = reopened.inner.snapshot(&Query::Root, 64).await.unwrap();
            prop_assert_eq!(&actual.files, &expected.files);
            prop_assert_eq!(actual.generation, expected.generation);
            let spent = native.receipt();
            prop_assert!(spent.items_examined > count as u64);
            prop_assert!(spent.bytes_copied > 0);
            // Native memory reads borrow the provider's immutable payloads.
            // They retain that provider allocation instead of copying it onto
            // the query ledger; disk-backed reads carry their own native guard.
            prop_assert!(actual.files.iter().zip(&expected.files).all(|(file, source)|
                file.bytes.matches_meter(&native.runtime_meter())
                    || file.bytes.as_ptr() == source.bytes.as_ptr()));
            drop(actual);
            let alias = reopened.clone();
            drop(reopened);
            prop_assert_eq!(native.live_allocation_bytes(), held);
            drop(alias);
            prop_assert_eq!(native.live_allocation_bytes(), baseline);
            // Use the successful run's exact work as the independent boundary:
            // a one-item-short ledger must fail without returning an index.
            for memory in [false, true] {
                let mut budget = acyclic_fs::performance::BOUNDARY_BUDGET;
                if memory { budget.peak_allocation_bytes = spent.peak_allocation_bytes - 1; }
                else { budget.items_examined = spent.items_examined - 1; }
                let denied = work(budget);
                let before = denied.live_allocation_bytes();
                let result = TaskFiles::open_with_native_work(provider.clone(), &volume, &verifier, &scope, 4096, &denied).await;
                prop_assert!(result.as_ref().err().is_some_and(crate::Error::is_native_failure));
                drop(result);
                prop_assert_eq!(denied.live_allocation_bytes(), before);
            }
            let mut budget = acyclic_fs::performance::BOUNDARY_BUDGET;
            budget.items_examined = spent.items_examined - 1;
            let denied = work(budget);
            // Broken control restores the old unmetered open. It succeeds
            // under that same denied invocation and records no replay work.
            let broken = TaskFiles::open(provider, &volume, &verifier, &scope, 4096).await.unwrap();
            prop_assert_eq!(broken.inner.snapshot(&Query::Root, 64).await.unwrap().files, expected.files);
            prop_assert_eq!(denied.receipt().items_examined, 0);
            Ok(())
        })?;
    }
}
