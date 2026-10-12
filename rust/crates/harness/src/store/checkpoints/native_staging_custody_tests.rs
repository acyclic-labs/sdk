use super::*;

use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
use acyclic_stream::MemoryStream;
use std::sync::{Arc, atomic::Ordering};

pub(super) fn budget() -> WorkBudget {
    WorkBudget {
        authority_records_read: 32,
        authority_bytes_read: 1_048_576,
        backend_read_operations: 32,
        source_bytes_read: 1_048_576,
        bytes_copied: 1_048_576,
        bytes_encoded: 1_048_576,
        bytes_hashed: 1_048_576,
        authority_records_appended: 32,
        authority_bytes_written: 1_048_576,
        backend_write_operations: 32,
        items_examined: 1_048_576,
        allocation_operations: 1_048_576,
        peak_allocation_bytes: 1_048_576,
        ..WorkBudget::default()
    }
}

#[tokio::test]
async fn original_staged_vec_capacity_releases_on_ack_error_cancel_and_retry() -> Result<()> {
    for mode in 0..5 {
        let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
        let client = provider.clone();
        let path = StreamPath::new(format!("harness/v3/native-stage-custody/{mode}"))?;
        let cancellation = CancellationToken::new();
        let mut allowance = budget();
        if mode == 4 {
            allowance.backend_write_operations = 0;
        }
        let native = NativeMaterializationWork::new(allowance, &cancellation)
            .expect("original finite immutable publication ledger");
        let mut io = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(&native)
                .expect("original source custody"),
        );
        let mut source = Vec::new();
        crate::store::history::reserve(&mut source, 4096, &mut io)?;
        io.copy_bytes(9)?;
        source.extend_from_slice(b"immutable");
        assert!(source.capacity() > source.len());
        let source_capacity = source.capacity();
        let source_pointer = source.as_ptr();
        if mode == 1 {
            provider.forbid_writes.store(true, Ordering::SeqCst);
        }
        if mode == 2 {
            provider.commit_lose_ack.store(true, Ordering::SeqCst);
        }
        if mode == 3 {
            cancellation.cancel();
        }
        let staged = stage_immutable_vec_with_work(
            &client,
            (StreamPath::new(path.as_str())?, 0),
            Ok(source),
            &mut Some(&mut io),
        )
        .await;
        if mode == 0 {
            staged?;
        } else {
            assert!(staged.is_err());
        }
        assert_eq!(
            io.into_custody()?.bytes(),
            0,
            "source custody moved into SDK aliases"
        );
        if matches!(mode, 3 | 4) {
            assert_eq!(provider.io_snapshot().writes, 0);
        }
        if mode == 2 {
            assert_eq!(
                client
                    .bounds(acyclic_stream::StreamPath::new(path.as_str())?)
                    .await?
                    .tail,
                1
            );
            let mut io = crate::store::HistoryWork::new(
                crate::filesystem::PreparationIo::from_native_work(&native)
                    .expect("same original retry ledger"),
            );
            let mut source = Vec::new();
            crate::store::history::reserve(&mut source, 4096, &mut io)?;
            io.copy_bytes(9)?;
            source.extend_from_slice(b"immutable");
            stage_immutable_vec_with_work(
                &client,
                (StreamPath::new(path.as_str())?, 0),
                Ok(source),
                &mut Some(&mut io),
            )
            .await?;
            assert_eq!(io.into_custody()?.bytes(), 0);
            assert_eq!(
                client
                    .bounds(acyclic_stream::StreamPath::new(path.as_str())?)
                    .await?
                    .tail,
                1
            );
        }
        if matches!(mode, 0 | 2) {
            let record = operations::one_record(&client, &path, 0, &mut None)
                .await?
                .expect("original immutable publication persisted");
            assert_eq!(record.value.as_ref(), b"immutable");
            let capacity = record
                .value
                .retained_capacity()
                .expect("original native encoder capacity stays intrinsic");
            assert_eq!(capacity, source_capacity);
            assert_eq!(record.value.as_ref().as_ptr(), source_pointer);
            let alias = record.value.clone();
            let empty = alias.slice(0..0);
            drop(record);
            drop(alias);
            assert!(
                native
                    .remaining()
                    .expect("original remaining ledger")
                    .peak_allocation_bytes
                    <= budget().peak_allocation_bytes - capacity as u64
            );
            drop(client);
            drop(provider);
            assert_eq!(empty.retained_capacity(), Some(capacity));
            assert!(
                native
                    .remaining()
                    .expect("last alias retains original claim")
                    .peak_allocation_bytes
                    <= budget().peak_allocation_bytes - capacity as u64
            );
            let remaining = native
                .remaining()
                .expect("original finite quota")
                .peak_allocation_bytes;
            assert!(matches!(
                native.claim(remaining + 1, 1, acyclic_fs::WorkCounters::default()),
                Err(acyclic_fs::NativeTreeMaterializationError::Work(
                    acyclic_fs::WorkError::BudgetExceeded {
                        counter: "peak_allocation_bytes",
                        ..
                    }
                ))
            ));
            drop(empty);
            assert_eq!(
                native
                    .remaining()
                    .expect("last alias retires original claim")
                    .peak_allocation_bytes,
                budget().peak_allocation_bytes
            );
        } else if mode != 3 {
            drop(client);
            drop(provider);
            assert_eq!(
                native
                    .remaining()
                    .expect("failed write releases original claim")
                    .peak_allocation_bytes,
                budget().peak_allocation_bytes
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn competing_immutable_identity_recovers_or_rejects_without_retained_source() -> Result<()> {
    for identical in [true, false] {
        let client = Arc::new(MemoryStream::default());
        let path = StreamPath::new("harness/v3/native-stage-custody/competing")?;
        acyclic_stream::Stream::new(std::sync::Arc::clone(&client), path.as_str())?
            .append_at(OwnedBytes::from_static(b"immutable"), 0)
            .await?;
        let cancellation = CancellationToken::new();
        let native = NativeMaterializationWork::new(budget(), &cancellation)
            .expect("original finite competitor-read ledger");
        let mut io = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(&native)
                .expect("original staging source custody"),
        );
        let mut source = Vec::new();
        crate::store::history::reserve(&mut source, 4096, &mut io)?;
        let body: &[u8] = if identical { b"immutable" } else { b"hostile" };
        io.copy_bytes(body.len() as u64)?;
        source.extend_from_slice(body);
        let staged = stage_immutable_vec_with_work(
            &client,
            (StreamPath::new(path.as_str())?, 0),
            Ok(source),
            &mut Some(&mut io),
        )
        .await;
        assert_eq!(staged.is_ok(), identical);
        assert_eq!(io.into_custody()?.bytes(), 0);
        assert_eq!(
            client
                .bounds(acyclic_stream::StreamPath::new(path.as_str())?)
                .await?
                .tail,
            1
        );
        drop(client);
        assert_eq!(
            native
                .remaining()
                .expect("competitor recovery retires all original aliases")
                .peak_allocation_bytes,
            budget().peak_allocation_bytes
        );
    }
    Ok(())
}
