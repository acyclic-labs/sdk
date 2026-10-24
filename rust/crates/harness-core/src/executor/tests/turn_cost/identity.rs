use super::*;

#[tokio::test]
async fn replacing_a_journal_in_place_discards_its_cached_execution_prefix() -> Result<()> {
    let executor = lookup_executor()?;
    let execution = StockExecution::ordinary(&executor);
    let operation = OperationId::new();
    let mut journal = Journal::default();
    let address = std::ptr::from_ref(&journal) as usize;
    for digest in [[1; 32], [2; 32]] {
        journal.0.lock().unwrap().push(ExecutionRecord {
            operation_id: operation,
            sequence: 1,
            idempotency_key: "started".into(),
            event: ExecutionEvent::Started {
                request_digest: digest,
            },
        });
        let (tail, records) = execution
            .step_records(&journal, operation, 0, true, 1, |event| {
                matches!(event, ExecutionEvent::Started { .. })
            })
            .await?;
        assert_eq!(tail, 1);
        assert!(
            matches!(records.as_slice(), [ExecutionRecord {
            event: ExecutionEvent::Started { request_digest }, ..
        }] if request_digest == &digest),
            "negative control reused an execution prefix after replacing its journal"
        );
        journal = Journal::default();
        assert_eq!(std::ptr::from_ref(&journal) as usize, address);
    }
    Ok(())
}
