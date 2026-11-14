use super::*;

#[tokio::test]
async fn private_view_construction_defers_original_stream_observations()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    let streams = Arc::new(acyclic_stream::testing::Counted::new(
        acyclic_stream::MemoryStream::default(),
    ));
    let fs = acyclic_fs::Fs::memory();
    let bytes = [17; 16];
    let storage = crate::filesystem::MemoryHarnessStorage::from_providers(
        crate::AgentId::from_bytes(bytes),
        65_536,
        fs.clone(),
        Arc::clone(&streams),
    )
    .await?;
    let host = FilesystemHost::new(fs, storage.volume().provider(), None)?;
    let before = streams.requests();
    let host = host
        .for_agent_views(
            Arc::clone(&streams),
            &crate::agent::AgentPath::root(&crate::agent::AgentId::from_bytes(bytes)),
            storage.volume(),
            storage.content_bindings().reader,
        )
        .await?;
    assert_eq!(
        streams.requests(),
        before,
        "construction must not capture UI or evaluation cuts"
    );
    let cancellation = acyclic_fs::CancellationToken::new();
    let native = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .map_err(|(error, _)| error)?;
    let io = PreparationIo::from_native_work(&native)?;
    let mut ctx = KernelAccounting::NativeOriginal {
        work: &native,
        cancellation: &cancellation,
    };
    let selected = host
        .private_view
        .as_ref()
        .ok_or("missing private source")?
        .snapshot(&mut ctx, &io)?
        .await?;
    assert_ne!(
        streams.requests(),
        before,
        "a requested snapshot must capture its original sources"
    );
    drop(selected);
    Ok(())
}
