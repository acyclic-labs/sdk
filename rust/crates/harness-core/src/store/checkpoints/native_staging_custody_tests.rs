use super::*;

use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
use acyclic_stream::{MemoryStream, OwnedBytes};
use std::sync::Arc;

pub(in crate::store) fn budget() -> WorkBudget {
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
        output_bytes: 1_048_576,
        items_returned: 32,
        items_examined: 1_048_576,
        allocation_operations: 1_048_576,
        peak_allocation_bytes: 1_048_576,
        ..WorkBudget::default()
    }
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
        let staged = stage_immutable(
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
            budget().peak_allocation_bytes - NativeMaterializationWork::accounting_state_bytes()
        );
    }
    Ok(())
}
