use super::*;

#[tokio::test]
async fn legacy_headers_remain_readable_and_old_schema_cursors_reset()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    for schema in 2..=4 {
        let provider = Arc::new(acyclic_stream::MemoryStream::default());
        let path = StreamPath::new(format!("schema-{schema}"))?;
        let header = Header {
            volume: Uuid::from_bytes([1; 16]),
            epoch: Uuid::from_bytes([2; 16]),
            schema,
        };
        provider
            .append(AppendRequest {
                path: path.clone(),
                records: vec![serde_json::to_vec(&header)?.into()],
                if_tail: Some(0),
                idempotency_key: None,
            })
            .await?;
        let volume = TaskVolume::open_existing(provider.clone(), path.clone()).await?;
        let snapshot = volume.snapshot(&Query::Root, 1).await?;
        assert_eq!(snapshot.cursor.schema, SCHEMA);
        let mut legacy = snapshot.cursor;
        legacy.schema = schema;
        let reset = volume.observe(Query::Root, Some(legacy), 1).await?;
        assert!(reset.reset);
        assert_eq!(
            reset.snapshot.ok_or("missing schema reset")?.cursor.schema,
            SCHEMA
        );
        let mut records = provider
            .read(ReadRequest {
                path,
                from: 0,
                limit: 1,
            })
            .await?;
        let record = records.next().await.ok_or("missing legacy header")??;
        assert!(FileValue::from_record(&record)?.is_none());
    }
    Ok(())
}
