use super::*;

async fn distinct_reads<const UNIQUE: bool>() -> Result<()> {
    for (adapter, _) in fixtures() {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(PNG)
            .unwrap();
        let (first, reads, publisher) = file(&bytes, "image/png", false, None).await?;
        let (second, _) = publisher
            .stage_once(
                acyclic_harness::OperationId::new(),
                "attachments/second.png",
                &bytes,
                "image/png",
                "second",
                None,
            )?
            .await?;
        let (mut provider, _) = provider(adapter, 200);
        enable(&mut provider);
        let provider = provider.with_content_reader(reads.clone());
        let (request, _) = prepare(
            &provider,
            vec![user(vec![image(&first), image(&second), image(&first)])],
        )?;
        let loaded =
            crate::images::load_unique::<UNIQUE>(&provider, request.request(), None).await?;
        let occurrences: Vec<_> = loaded.iter().collect();
        assert_eq!(occurrences.len(), 3);
        assert!(occurrences.iter().all(|part| *part == bytes.as_slice()));
        assert_eq!(
            reads.calls.load(Ordering::SeqCst),
            2,
            "one read/hash per pinned file"
        );
        assert!(std::ptr::eq(
            occurrences[0].as_ptr(),
            occurrences[2].as_ptr()
        ));
    }
    Ok(())
}

#[tokio::test]
async fn image_occurrences_share_unique_verified_bytes() -> Result<()> {
    distinct_reads::<true>().await
}

#[tokio::test]
#[should_panic(expected = "one read/hash per pinned file")]
async fn image_repeated_reads_fail_the_same_oracle_negative_control() {
    distinct_reads::<false>().await.unwrap();
}
