use super::*;

#[tokio::test]
async fn image_native_dispatch_uses_original_work_and_releases_content() -> Result<()> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(PNG)
        .expect("PNG fixture");
    for (adapter, response) in fixtures() {
        let work = super::native_tests::work(&acyclic_fs::CancellationToken::new())?;
        let baseline = work.live_allocation_bytes();
        assert_eq!(
            baseline,
            acyclic_fs::NativeMaterializationWork::accounting_state_bytes()
        );
        let io = acyclic_harness::filesystem::PreparationIo::from_native_work(&work)?;
        let (file, reads, _) = file(&bytes, "image/png", false, Some(&io)).await?;
        let (mut provider, _) = provider(adapter, 200);
        enable(&mut provider);
        let transport = Arc::new(super::native_tests::Fixture::new(response));
        provider.transport = transport.clone();
        let provider = provider.with_content_reader(reads.clone());
        let (request, dispatch) =
            prepare(&provider, vec![user(vec![text("describe"), image(&file)])])?;
        transport
            .expected_images
            .lock()
            .expect("image expectations")
            .push((PNG.as_bytes().to_vec(), 1));
        let before = work.live_allocation_bytes();
        let (count, hold) = provider
            .count_tokens_with_native_work(&request, &work)?
            .await?;
        assert!(count.message_tokens[0] >= 1024);
        assert!(hold.work().shares_allocation_context(&work));
        drop((count, hold));
        let mut stream = provider
            .generate_with_native_work(&request, dispatch, &work)?
            .await?;
        let mut events = 0;
        let mut blocks = 0;
        while let Some(event) = stream.next().await {
            let (event, hold) = event?;
            assert!(hold.work().shares_allocation_context(&work));
            if matches!(event, ModelEvent::ReasoningBlock { .. }) {
                blocks += 1;
            }
            drop((event, hold));
            events += 1;
        }
        drop(stream);
        assert_eq!(events, 4 + reasoning_blocks(adapter), "{adapter:?}");
        assert_eq!(blocks, reasoning_blocks(adapter), "{adapter:?}");
        assert_eq!(transport.counted.load(Ordering::SeqCst), 1);
        assert_eq!(reads.calls.load(Ordering::SeqCst), 2);
        assert_eq!(transport.sent.load(Ordering::SeqCst), 1);
        assert!(work.receipt().items_examined >= bytes.len() as u64);
        assert_eq!(work.live_allocation_bytes(), before, "{adapter:?}");
        drop(provider);
        drop(reads);
        drop(io);
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
    Ok(())
}
