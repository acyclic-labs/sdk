//! Publication between the real layer probe and its shared tool consumer.
use super::*;
use crate::conversation::{BoundedContentFuture, ContentAccess, ContentHold, FilePathSelection};
use crate::runtime::{Bindings, ContentBindings, TaskContext, ToolContext};
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

struct ReplaceAfterProbe {
    storage: Arc<MemoryHarnessStorage>,
    replacement: Vec<u8>,
    media_type: &'static str,
    selected: Mutex<Option<FilePathSelection>>,
    unpinned_reads: AtomicUsize,
}

fn task(home: ContentBindings, seed: Option<ContentBindings>) -> Result<Arc<TaskContext>> {
    let mut grants = vec![
        home.volume.capability(VolumeOperation::Read, &mut None)?,
        home.volume.capability(VolumeOperation::Write, &mut None)?,
        "tool:call:read".into(),
    ];
    if let Some(seed) = &seed {
        grants.push(seed.volume.capability(VolumeOperation::Read, &mut None)?);
    }
    Ok(Arc::new(
        Bindings {
            scope: RuntimeScope::new(
                Capabilities::new(grants),
                Limits {
                    file_bytes: 4096,
                    ..Limits::default()
                },
            )?,
            seed: seed.as_ref().map(|seed| seed.volume.clone()),
            content: Some(seed.unwrap_or_else(|| home.clone())),
            ..Bindings::local()
        }
        .build()?
        .agent_context(&Capabilities::new(Vec::<String>::new()), home)?,
    ))
}

async fn read(task: &Arc<TaskContext>, arguments: Value) -> Result<(ToolResult, Value)> {
    let tool = crate::tool::files::read()?;
    let invocation = ToolInvocation {
        operation_id: OperationId::new(),
        call_id: "layer-generation".into(),
        name: tool.definition.name.clone(),
        arguments,
    };
    let context = ToolContext::new(task.clone(), invocation.operation_id, &invocation.call_id)?;
    let result = tool
        .executor
        .execute(invocation.clone(), Some(context))
        .await?;
    let projection = tool.projection.project(&invocation, &result, None)?.0;
    tool.definition
        .validate_projection(&projection, task.scope().limits(), None)?;
    Ok((result, projection))
}

impl ContentResidencyVerifier for ReplaceAfterProbe {
    fn read_private_range<'a, 'io>(
        &'a self,
        volume: &'a VolumeRef,
        granted_prefix: &'a str,
        path: &'a str,
        expected_generation: Option<&'a GenerationRef>,
        range: acyclic_fs::ByteRange,
        access: ContentAccess<'a, 'io>,
    ) -> Result<
        BoundedContentFuture<
            'a,
            (
                FilePathSelection,
                acyclic_stream::OwnedBytes,
                Option<ContentHold<'io>>,
            ),
        >,
    >
    where
        'io: 'a,
    {
        if expected_generation.is_none() {
            self.unpinned_reads.fetch_add(1, Ordering::Relaxed);
        }
        let original = self.storage.content_reader().read_private_range(
            volume,
            granted_prefix,
            path,
            expected_generation,
            range,
            access,
        )?;
        BoundedContentFuture::new(access.io, || async move {
            let original = original.await?;
            let replace = {
                let mut selected = self
                    .selected
                    .lock()
                    .map_err(|error| Error::Storage(error.to_string()))?;
                if selected.is_none()
                    && expected_generation.is_none()
                    && range.offset == 0
                    && range.length == 32
                {
                    *selected = Some(original.0.clone());
                    true
                } else {
                    false
                }
            };
            if replace {
                // Return the actual original result only after a new real
                // publication has replaced its head. No provider output is
                // synthesized, and no product scheduling hook is introduced.
                self.storage
                    .stage(
                        OperationId::new(),
                        path,
                        &self.replacement,
                        self.media_type,
                        path,
                    )
                    .await?;
            }
            Ok(original)
        })
    }

    fn read_private_path<'a, 'io>(
        &'a self,
        volume: &'a VolumeRef,
        granted_prefix: &'a str,
        path: &'a str,
        expected_generation: Option<&'a GenerationRef>,
        maximum_bytes: u64,
        access: ContentAccess<'a, 'io>,
    ) -> Result<
        BoundedContentFuture<
            'a,
            Option<(
                FileRef,
                acyclic_stream::OwnedBytes,
                Option<ContentHold<'io>>,
            )>,
        >,
    >
    where
        'io: 'a,
    {
        if expected_generation.is_none() {
            self.unpinned_reads.fetch_add(1, Ordering::Relaxed);
        }
        self.storage.content_reader().read_private_path(
            volume,
            granted_prefix,
            path,
            expected_generation,
            maximum_bytes,
            access,
        )
    }
}

#[tokio::test]
async fn every_effective_projection_keeps_the_original_probe_generation() -> Result<()> {
    let notebook = |text: &str| {
        serde_json::to_vec(&json!({
            "nbformat":4,"nbformat_minor":5,"metadata":{},
            "cells":[{"cell_type":"markdown","id":"original","metadata":{},"source":[text]}],
        }))
        .map_err(|error| Error::Invalid(error.to_string()))
    };
    let image = include_bytes!("../../../../tests/fixtures/notebook-output.png");
    for (path, bytes, replacement, media_type, selection) in [
        (
            "text.txt",
            b"original\n".to_vec(),
            b"replaced\n".to_vec(),
            "text/plain",
            None,
        ),
        (
            "text.txt",
            b"original\n".to_vec(),
            b"replaced\n".to_vec(),
            "text/plain",
            Some(json!({"kind":"lines","offset":0,"limit":1})),
        ),
        (
            "book.ipynb",
            notebook("original")?,
            notebook("replaced")?,
            "application/x-ipynb+json",
            Some(json!({"kind":"cells","offset":0,"limit":1})),
        ),
        (
            "book.ipynb",
            notebook("original")?,
            notebook("replaced")?,
            "application/x-ipynb+json",
            None,
        ),
        (
            "image.png",
            image.to_vec(),
            image.to_vec(),
            "image/png",
            None,
        ),
        (
            "image.png",
            image.to_vec(),
            image.to_vec(),
            "image/png",
            Some(json!({"kind":"image","detail":"auto"})),
        ),
    ] {
        let storage = Arc::new(MemoryHarnessStorage::new(AgentId::new(), 4096).await?);
        storage
            .stage(OperationId::new(), path, &bytes, media_type, path)
            .await?;
        let reader = Arc::new(ReplaceAfterProbe {
            storage: storage.clone(),
            replacement,
            media_type,
            selected: Mutex::new(None),
            unpinned_reads: AtomicUsize::new(0),
        });
        let mut binding = storage.content_bindings();
        binding.reader = reader.clone();
        let task = task(binding, None)?;
        let mut arguments = json!({"path":format!("{}{path}", crate::layers::EFFECTIVE_PATH)});
        if let Some(selection) = selection {
            arguments["selection"] = selection;
        }
        let result = read(&task, arguments).await;
        assert_eq!(
            reader.unpinned_reads.load(Ordering::Relaxed),
            1,
            "projection reselected head after its original home probe"
        );
        let selected = reader
            .selected
            .lock()
            .map_err(|error| Error::Storage(error.to_string()))?;
        let original = selected
            .as_ref()
            .ok_or_else(|| Error::Storage("actual home probe did not execute".into()))?;
        match result {
            Ok((result, _)) => {
                let source: crate::tool::files::FileReadSource = serde_json::from_value(
                    result
                        .value
                        .get("file")
                        .ok_or_else(|| Error::Storage("read omitted its source".into()))?
                        .clone(),
                )
                .map_err(|error| Error::Invalid(error.to_string()))?;
                match source {
                    crate::tool::files::FileReadSource::Authored(source) => {
                        assert_eq!(&source, original)
                    }
                    crate::tool::files::FileReadSource::Content(file) => {
                        assert!(original.matches_content(&file))
                    }
                }
            }
            Err(error) => assert_eq!(error.code(), crate::wire::ErrorCode::Conflict),
        }
    }
    Ok(())
}

#[tokio::test]
async fn explicit_versions_keep_their_selection_across_a_new_home_probe() -> Result<()> {
    for selection in [None, Some(json!({"kind":"lines","offset":0,"limit":1}))] {
        let storage = Arc::new(MemoryHarnessStorage::new(AgentId::new(), 4096).await?);
        storage
            .stage(
                OperationId::new(),
                "text.txt",
                b"original\n",
                "text/plain",
                "text.txt",
            )
            .await?;
        let reader = Arc::new(ReplaceAfterProbe {
            storage: storage.clone(),
            replacement: b"replaced\n".to_vec(),
            media_type: "text/plain",
            selected: Mutex::new(None),
            unpinned_reads: AtomicUsize::new(0),
        });
        let mut binding = storage.content_bindings();
        binding.reader = reader.clone();
        let task = task(binding, None)?;
        let mut arguments = json!({"path":format!("{}text.txt", crate::layers::HOME_PATH)});
        if let Some(selection) = selection {
            arguments["selection"] = selection;
        }
        let (original, projection) = read(&task, arguments.clone()).await?;
        let version = projection["value"]["version"]
            .as_str()
            .ok_or_else(|| Error::Storage("actual read omitted its version".into()))?;
        storage
            .stage(
                OperationId::new(),
                "text.txt",
                b"new head\n",
                "text/plain",
                "text.txt",
            )
            .await?;
        reader.unpinned_reads.store(0, Ordering::Relaxed);
        arguments["path"] = json!(format!("{}text.txt", crate::layers::EFFECTIVE_PATH));
        arguments["version"] = json!(version);
        let result = read(&task, arguments).await;
        assert_eq!(reader.unpinned_reads.load(Ordering::Relaxed), 1);
        let probe = reader
            .selected
            .lock()
            .map_err(|error| Error::Storage(error.to_string()))?;
        let probe = probe
            .as_ref()
            .ok_or_else(|| Error::Storage("actual home probe did not execute".into()))?;
        let authored: crate::tool::files::FileReadSource =
            serde_json::from_value(original.value["file"].clone())
                .map_err(|error| Error::Invalid(error.to_string()))?;
        assert_ne!(
            authored,
            crate::tool::files::FileReadSource::Authored(probe.clone())
        );
        match result {
            Ok((result, _)) => {
                assert_eq!(result.value["file"], original.value["file"]);
                assert_eq!(result.value["text"], original.value["text"]);
            }
            Err(error) => assert_eq!(error.code(), crate::wire::ErrorCode::Conflict),
        }
    }
    Ok(())
}

#[tokio::test]
async fn seed_fallback_and_off_markers_keep_the_original_layer_decision() -> Result<()> {
    for (path, home_bytes, expected_home_reads, hidden) in [
        ("text.txt", None, 1, false),
        ("text.txt", Some(b"off: true".as_slice()), 1, true),
        ("sys/kernel.md", Some(b"shadow".as_slice()), 0, false),
    ] {
        let home = Arc::new(MemoryHarnessStorage::new(AgentId::new(), 4096).await?);
        let seed = MemoryHarnessStorage::new(AgentId::new(), 4096).await?;
        let display_name = path.rsplit('/').next().unwrap_or(path);
        seed.stage(
            OperationId::new(),
            path,
            b"seed content",
            "text/plain",
            display_name,
        )
        .await?;
        if let Some(bytes) = home_bytes {
            home.stage(OperationId::new(), path, bytes, "text/plain", display_name)
                .await?;
        }
        let reader = Arc::new(ReplaceAfterProbe {
            storage: home.clone(),
            replacement: b"replacement is visible".to_vec(),
            media_type: "text/plain",
            selected: Mutex::new(None),
            unpinned_reads: AtomicUsize::new(0),
        });
        let mut binding = home.content_bindings();
        binding.reader = reader.clone();
        let task = task(binding, Some(seed.content_bindings()))?;
        let result = read(
            &task,
            json!({"path":format!("{}{path}", crate::layers::EFFECTIVE_PATH)}),
        )
        .await;
        assert_eq!(
            reader.unpinned_reads.load(Ordering::Relaxed),
            expected_home_reads
        );
        if hidden {
            match result {
                Err(error) => assert_eq!(error.code(), crate::wire::ErrorCode::NotFound),
                Ok(_) => {
                    return Err(Error::Storage(
                        "original home off marker was replaced by a later publication".into(),
                    ));
                }
            }
        } else {
            let (result, _) = result?;
            assert_eq!(result.value["text"], "seed content");
            let source: crate::tool::files::FileReadSource =
                serde_json::from_value(result.value["file"].clone())
                    .map_err(|error| Error::Invalid(error.to_string()))?;
            match source {
                crate::tool::files::FileReadSource::Authored(source) => {
                    assert_eq!(&source.volume, seed.volume());
                }
                crate::tool::files::FileReadSource::Content(file) => {
                    assert_eq!(file.volume(), seed.volume());
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn effective_reads_preserve_the_selected_layers_authority() -> Result<()> {
    let home = Arc::new(MemoryHarnessStorage::new(AgentId::new(), 4096).await?);
    let seed = MemoryHarnessStorage::new(AgentId::new(), 4096).await?;
    home.stage(
        OperationId::new(),
        "text.txt",
        b"content",
        "text/plain",
        "text.txt",
    )
    .await?;
    let seed_file = seed
        .stage(
            OperationId::new(),
            "text.txt",
            b"content",
            "text/plain",
            "text.txt",
        )
        .await?;
    let reader = Arc::new(ReplaceAfterProbe {
        storage: home.clone(),
        replacement: b"replacement".to_vec(),
        media_type: "text/plain",
        selected: Mutex::new(None),
        unpinned_reads: AtomicUsize::new(0),
    });
    let mut binding = home.content_bindings();
    binding.reader = reader.clone();
    let task = task(binding, Some(seed.content_bindings()))?;
    let denied = Arc::new(task.scoped(
        Capabilities::new([
            home.write_capability.clone(),
            seed.read_capability.clone(),
            "tool:call:read".into(),
        ]),
        task.scope().limits().clone(),
    )?);
    let arguments = json!({"path":format!("{}text.txt", crate::layers::EFFECTIVE_PATH)});
    match read(&denied, arguments.clone()).await {
        Err(error) => assert_eq!(error.code(), crate::wire::ErrorCode::Unauthorized),
        Ok(_) => {
            return Err(Error::Storage(
                "virtual read bypassed its home grant".into(),
            ));
        }
    }
    assert_eq!(reader.unpinned_reads.load(Ordering::Relaxed), 0);

    // A real content token keeps its original metadata/grant admission even
    // while the virtual path's physical layer is still unresolved.
    let denied_seed = Arc::new(task.scoped(
        Capabilities::new([
            home.read_capability.clone(),
            home.write_capability.clone(),
            "tool:call:read".into(),
        ]),
        task.scope().limits().clone(),
    )?);
    let mut content_arguments = arguments.clone();
    content_arguments["version"] = json!(
        serde_json::to_string(&seed_file).map_err(|error| Error::Invalid(error.to_string()))?
    );
    match read(&denied_seed, content_arguments).await {
        Err(error) => assert_eq!(
            error.code(),
            crate::wire::ErrorCode::Unauthorized,
            "virtual path lost the original content token grant"
        ),
        Ok(_) => {
            return Err(Error::Storage(
                "virtual path bypassed the original content token grant".into(),
            ));
        }
    }
    assert_eq!(
        reader.unpinned_reads.load(Ordering::Relaxed),
        0,
        "content token admission occurred after the home probe"
    );

    let (_, seed_projection) = read(
        &task,
        json!({"path":format!("{}text.txt", crate::layers::SEED_PATH)}),
    )
    .await?;
    let version = seed_projection["value"]["version"]
        .as_str()
        .ok_or_else(|| Error::Storage("actual seed read omitted its version".into()))?;
    let mut arguments = arguments;
    arguments["version"] = json!(version);
    match read(&task, arguments).await {
        Err(error) => assert_eq!(error.code(), crate::wire::ErrorCode::Invalid),
        Ok(_) => {
            return Err(Error::Storage(
                "seed version replaced a selected home file".into(),
            ));
        }
    }
    assert_eq!(reader.unpinned_reads.load(Ordering::Relaxed), 1);
    Ok(())
}
