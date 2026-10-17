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
