#[tokio::test]
async fn persisted_policy_blocks_s3_reads_writes_and_bucket_disclosure()
-> Result<(), Box<dyn std::error::Error>> {
    for action in [
        crate::access::Action::List,
        crate::access::Action::Read,
        crate::access::Action::Write,
    ] {
        let adapter = adapter(true).await?;
        crate::access::fixtures::publish_policy(
            &adapter.resolver.principal.workspace,
            &crate::access::fixtures::denied(action, "secrets/**"),
        )
        .await?;
        for mutation in [false, true] {
            let result = adapter
                .scoped_principal(&request(()), "bucket", mutation)
                .await;
            assert_eq!(
                result.is_err(),
                mutation || action != crate::access::Action::Write
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn streaming_s3_body_stops_before_the_next_revoked_chunk()
-> Result<(), Box<dyn std::error::Error>> {
    let adapter = adapter_with_limits(
        true,
        FilesystemS3Limits {
            response_chunk_bytes: 2,
            ..FilesystemS3Limits::default()
        },
    )
    .await?;
    let workspace = &adapter.resolver.principal.workspace;
    workspace.write_text("/file", "abcdef").await?;
    let response = S3::get_object(
        &adapter,
        request(GetObjectInput {
            bucket: "bucket".into(),
            key: "file".into(),
            ..GetObjectInput::default()
        }),
    )
    .await?;
    let mut body = response.output.body.ok_or("missing S3 body")?;
    assert_eq!(
        body.next()
            .await
            .ok_or("missing first chunk")?
            .map_err(std::io::Error::other)?,
        "ab"
    );
    crate::access::fixtures::publish_policy(
        workspace,
        &crate::access::fixtures::denied(crate::access::Action::Read, "**"),
    )
    .await?;
    assert!(body.next().await.ok_or("missing stream denial")?.is_err());
    assert!(body.next().await.is_none());
    Ok(())
}
