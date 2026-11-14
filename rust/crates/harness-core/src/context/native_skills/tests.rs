use super::*;
use acyclic_stream::{ReadRequest, StreamPath, StreamProvider, StreamProviderExt};
use futures::TryStreamExt as _;

#[tokio::test]
async fn large_retained_catalogs_chunk_and_replay_without_losing_source_keys() -> Result<()> {
    acyclic_native_runtime::initialize_native_io_services().map_err(Error::process_io)?;
    acyclic_fs::watch::initialize_native_watch_services().map_err(storage)?;
    let root = tempfile::tempdir_in(".").map_err(storage)?;
    let native = root.path().join("skills");
    for index in 0..192 {
        let directory = native.join(format!("skill-{index:03}-{}", "x".repeat(64)));
        std::fs::create_dir_all(&directory).map_err(storage)?;
        std::fs::write(
            directory.join("SKILL.md"),
            format!(
                "---\nname: skill-{index:03}\ndescription: retained catalog\n---\nbody {index}"
            ),
        )
        .map_err(storage)?;
    }
    let provider = crate::resources::ProviderRef::new(
        "large-retained-catalog".into(),
        "filesystem".into(),
        "1".into(),
        &mut None,
    )?;
    let volume = VolumeRef::new(
        provider,
        "journal".into(),
        crate::conversation::VolumeClass::AgentPrivate,
        crate::conversation::VolumeOwner::Agent(crate::AgentId::from_bytes([2; 16])),
        &mut None,
    )?;
    let filesystem =
        acyclic_fs::LocalFs::local(acyclic_fs::LocalOptions::new(root.path().join("state")))
            .await
            .map_err(storage)?;
    let stream = filesystem.stream_provider();
    let sources = NativeSkills::new(stream.clone(), volume.clone(), None, vec![native])?;
    let skills = sources.user_catalog(&mut DiscoveryWork::default())?.await?;
    assert_eq!(skills.len(), 192);
    let journal = StreamPath::new("harness/native-skill-sources/v1").map_err(storage)?;
    let tail = stream.tail(journal.clone()).await.map_err(storage)?;
    assert!(
        tail > 1,
        "fixture must cross the original single-record frame"
    );
    let mut read = stream
        .read(ReadRequest {
            path: journal.clone(),
            from: 0,
            limit: acyclic_stream::MAX_ITEMS as u32,
            key: None,
            at_least: Default::default(),
        })
        .await
        .map_err(storage)?;
    let mut all = std::collections::BTreeMap::new();
    let mut first_in_chunk = Vec::new();
    while let Some(record) = read.try_next().await.map_err(storage)? {
        assert!(
            record.value.len() + record.headers.payload_bytes() <= acyclic_stream::MAX_RECORD_BYTES
        );
        let key = std::str::from_utf8(record.headers.keys()[0].as_ref()).map_err(storage)?;
        first_in_chunk.push(key.to_owned());
        all.extend(retained::decode_stream_records(&record.value, key)?);
    }
    assert_eq!(
        all.len(),
        skills.len(),
        "every source is retained exactly once"
    );
    let headers = acyclic_stream::RecordHeaders::new(
        all.keys()
            .map(|key| key.as_bytes().to_vec().into())
            .collect(),
        vec![],
    )
    .map_err(storage)?;
    let value = serde_json::to_vec(&retained::RetainedRecords {
        version: 1,
        sources: RetainedSource::Batch(all),
    })
    .map_err(storage)?;
    assert!(value.len() + headers.payload_bytes() > acyclic_stream::MAX_RECORD_BYTES);
    let selected = first_in_chunk.first().unwrap();
    let mut retry = RetainedBatch::default();
    for (_, record) in retained::decode_stream_records(&value, selected)?
        .into_iter()
        .rev()
    {
        retry.retain(&volume, record)?;
    }
    // Negative control: the real provider rejects precisely the former writer.
    assert!(matches!(
        stream
            .append(acyclic_stream::AppendRequest {
                path: journal.clone(),
                records: vec![acyclic_stream::AppendRecord {
                    value: value.into(),
                    headers
                }],
                if_tail: None,
                idempotency_key: acyclic_stream::IdempotencyKey::new("oversized-catalog-control")
                    .map_err(storage)?,
            })
            .await,
        Err(acyclic_stream::StreamError::LimitExceeded)
    ));
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = native_work(&cancellation)?;
    retry.publish(&sources.records, &work).await?;
    assert_eq!(
        stream.tail(journal).await.map_err(storage)?,
        tail,
        "reverse-order retry adds no records"
    );
    // Fresh source-cache lookups select every physical chunk by its original
    // digest; a sibling in that chunk is warmed by the same bounded read.
    let reopened = NativeSkills::new(stream, volume.clone(), None, vec![])?;
    let before = work.receipt().authority_records_read;
    for key in first_in_chunk {
        assert!(reopened.records.read(key, &work).await?.is_some());
    }
    assert_eq!(work.receipt().authority_records_read - before, tail);
    for skill in [&skills[0], skills.last().unwrap()] {
        assert!(
            reopened
                .read(
                    &volume,
                    &skill.source.generation,
                    &skill.source.path,
                    None,
                    None
                )?
                .await?
                .is_some()
        );
    }
    Ok(())
}

#[tokio::test]
async fn retained_records_share_one_sync_on_the_native_io_service() -> Result<()> {
    acyclic_native_runtime::initialize_native_io_services().map_err(Error::process_io)?;
    acyclic_fs::watch::initialize_native_watch_services().map_err(storage)?;
    let root = tempfile::tempdir_in(".").map_err(storage)?;
    for name in ["greet2", "other"] {
        let directory = root.path().join("skills").join(name);
        std::fs::create_dir_all(&directory).map_err(storage)?;
        std::fs::write(
            directory.join("SKILL.md"),
            format!("---\nname: {name}\ndescription: {name}\n---\nbody"),
        )
        .map_err(storage)?;
    }
    for name in crate::context::native_discovery::excluded_native_names() {
        std::fs::create_dir(root.path().join("skills").join(name)).map_err(storage)?;
    }
    let provider = crate::resources::ProviderRef::new(
        "retained-record".into(),
        "filesystem".into(),
        "1".into(),
        &mut None,
    )?;
    let volume = VolumeRef::new(
        provider,
        "journal".into(),
        crate::conversation::VolumeClass::AgentPrivate,
        crate::conversation::VolumeOwner::Agent(crate::AgentId::from_bytes([1; 16])),
        &mut None,
    )?;
    let directory = root.path().join("state");
    let journal = StreamPath::new("harness/native-skill-sources/v1").map_err(storage)?;
    let (skills, bytes) = {
        let filesystem = acyclic_fs::LocalFs::local(acyclic_fs::LocalOptions::new(&directory))
            .await
            .map_err(storage)?;
        let stream = filesystem.stream_provider();
        let sources = NativeSkills::new(
            stream.clone(),
            volume.clone(),
            None,
            vec![root.path().join("skills")],
        )?;
        let skills = sources.user_catalog(&mut DiscoveryWork::default())?.await?;
        assert_eq!(skills.len(), 2);
        let mut exhausted = DiscoveryWork::default();
        exhausted.entries = exhausted.limits.entries;
        assert!(matches!(
            sources.user_catalog(&mut exhausted)?.await,
            Err(Error::Invalid(_))
        ));
        let mut narrow = DiscoveryWork {
            limits: crate::context::ContextDiscoveryLimits {
                header_bytes: 1,
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(matches!(
            sources.user_catalog(&mut narrow)?.await,
            Err(Error::Invalid(_))
        ));
        assert_eq!(
            stream.tail(journal.clone()).await.map_err(storage)?,
            1,
            "one atomic discovery publication"
        );
        let mut records = stream
            .read(ReadRequest {
                key: None,
                path: journal.clone(),
                from: 0,
                limit: 1,
                at_least: Default::default(),
            })
            .await
            .map_err(storage)?;
        let published = records.try_next().await.map_err(storage)?.unwrap();
        assert_eq!(published.headers.keys().len(), 2);
        assert!(records.try_next().await.map_err(storage)?.is_none());
        let selected = hex::encode(skills[0].source.generation.as_resource().key());
        let envelope: retained::RetainedRecords =
            serde_json::from_slice(&published.value).map_err(storage)?;
        let bytes = serde_json::to_vec(&envelope.sources).map_err(storage)?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = native_work(&cancellation)?;
        let mut retry = RetainedBatch::default();
        // Reversing discovery order must reproduce the same committed identity.
        for (_, record) in retained::decode_records(&bytes, &selected)?
            .into_iter()
            .rev()
        {
            let pin = retry.retain(&volume, record)?;
            assert!(skills.iter().any(|skill| skill.source == pin));
        }
        retry.publish(&sources.records, &work).await?;
        assert_eq!(stream.tail(journal.clone()).await.map_err(storage)?, 1);
        let before = work.receipt().authority_records_read;
        let read = |skill: &SkillMetadata| {
            sources.records.read(
                hex::encode(skill.source.generation.as_resource().key()),
                &work,
            )
        };
        let (first, second) = tokio::join!(read(&skills[0]), read(&skills[1]));
        let (first, second) = (first?.unwrap(), second?.unwrap());
        assert_eq!(
            work.receipt().authority_records_read - before,
            1,
            "concurrent sibling loads share the real native indexed read"
        );
        let cold = work.receipt();
        assert!(Arc::ptr_eq(&first, &read(&skills[0]).await?.unwrap()));
        assert!(Arc::ptr_eq(&second, &read(&skills[1]).await?.unwrap()));
        assert_eq!(work.receipt(), cold, "warm reads admit no native work");
        (skills, bytes)
    };
    // A new filesystem and empty source cache recover without native receipts.
    let filesystem = acyclic_fs::LocalFs::local(acyclic_fs::LocalOptions::new(&directory))
        .await
        .map_err(storage)?;
    let sources = NativeSkills::new(filesystem.stream_provider(), volume.clone(), None, vec![])?;
    for skill in &skills {
        assert!(
            sources
                .read(
                    &volume,
                    &skill.source.generation,
                    &skill.source.path,
                    None,
                    None
                )?
                .await?
                .is_some()
        );
    }
    let selected = hex::encode(skills[0].source.generation.as_resource().key());
    let sibling = hex::encode(skills[1].source.generation.as_resource().key());
    let mut changed: serde_json::Value = serde_json::from_slice(&bytes).map_err(storage)?;
    changed["Batch"][&sibling]["Native"]["path"] = "changed/SKILL.md".into();
    let bad = Arc::new(acyclic_stream::MemoryStream::default());
    bad.append(acyclic_stream::AppendRequest {
        path: journal,
        records: vec![acyclic_stream::AppendRecord {
            value: serde_json::to_vec(&serde_json::json!({"version":1,"sources":changed}))
                .map_err(storage)?
                .into(),
            headers: acyclic_stream::RecordHeaders::new(
                vec![selected.as_bytes().to_vec().into()],
                vec![],
            )
            .map_err(storage)?,
        }],
        if_tail: None,
        idempotency_key: acyclic_stream::IdempotencyKey::new("changed-sibling").map_err(storage)?,
    })
    .await
    .map_err(storage)?;
    let cache = retained::RecordCache::new(bad);
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = native_work(&cancellation)?;
    for _ in 0..2 {
        assert!(
            matches!(
                cache.read(selected.clone(), &work).await,
                Err(Error::Conflict(_))
            ),
            "changed sibling must reject the whole physical bundle"
        );
    }
    #[cfg(feature = "test-support")]
    {
        codec_fuzz::check(&bytes);
        let receipt = serde_json::to_vec(&skills[0].source.generation).map_err(storage)?;
        codec_fuzz::check(&receipt);
        if let Some(directory) = std::env::var_os("ACYCLIC_RETAINED_SKILL_CORPUS") {
            let directory = PathBuf::from(directory);
            std::fs::create_dir_all(&directory).map_err(storage)?;
            std::fs::write(directory.join("native-batch.json"), bytes).map_err(storage)?;
            std::fs::write(directory.join("materialized.json"), receipt).map_err(storage)?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn watch_admission_stays_within_the_discovery_budget() -> Result<()> {
    acyclic_native_runtime::initialize_native_io_services().map_err(Error::process_io)?;
    acyclic_fs::watch::initialize_native_watch_services().map_err(storage)?;
    let root = tempfile::tempdir().map_err(storage)?;
    for name in ["one", "two"] {
        std::fs::create_dir(root.path().join(name)).map_err(storage)?;
    }
    let watch = UserWatch::default();
    let mut bounds = DiscoveryWork::default();
    bounds.limits.directories = 3;
    assert!(watch.arm(&[root.path().to_path_buf()], &bounds).is_some());
    assert_eq!(watch.watches.lock().unwrap().len(), 3);
    let previous = watch.current();
    bounds.limits.directories = 2;
    assert!(watch.arm(&[root.path().to_path_buf()], &bounds).is_none());
    assert!(watch.watches.lock().unwrap().is_empty());
    assert_ne!(watch.current(), previous);
    Ok(())
}
