//! Executes the Rust Stream scenario projected into the TypeScript consumer guide.

use acyclic_stream::{
    AppendOutcome, AppendRequest, IdempotencyKey, MemoryStream, ReadRequest, StreamPath,
    StreamProvider,
};
use bytes::Bytes;
use futures::TryStreamExt as _;
use serde_json::json;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let provider = MemoryStream::default();
    let path = StreamPath::new("typescript/events")?;
    let values = vec![
        Bytes::from_static(br#"{"kind":"created"}"#),
        Bytes::from_static(br#"{"kind":"ready"}"#),
    ];
    let idempotency_key = IdempotencyKey::new(Bytes::from_static(b"typescript-stream"))?;
    let append = provider
        .append(AppendRequest {
            path: path.clone(),
            records: values.clone(),
            if_tail: None,
            idempotency_key: Some(idempotency_key.clone()),
        })
        .await?;
    let append = match append {
        AppendOutcome::Committed(receipt) => {
            json!({"start": receipt.start, "end": receipt.end, "tail": receipt.tail})
        }
        AppendOutcome::TailConflict { actual_tail } => {
            return Err(format!("unexpected tail conflict at {actual_tail}").into());
        }
    };
    let tail = provider.tail(path.clone()).await?;
    let records = provider
        .read(ReadRequest {
            path: path.clone(),
            from: 0,
            limit: u32::try_from(values.len())?,
        })
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    println!(
        "{}",
        json!({
            "request": {
                "path": path.as_str(),
                "values": values.iter().map(|value| value.to_vec()).collect::<Vec<_>>(),
                "idempotency_key": idempotency_key.as_bytes(),
                "read_limit": values.len(),
            },
            "append": append,
            "tail": tail,
            "records": records.iter().enumerate().map(|(sequence, value)| json!({
                "sequence": sequence,
                "value": value.value.to_vec(),
            })).collect::<Vec<_>>(),
        })
    );
    Ok(())
}
