//! Recorded GraphCoder calls through the real reader and both host projections.
use super::*;
use crate::runtime::{Bindings, TaskContext, ToolContext};

async fn read(task: &TaskContext, arguments: Value) -> Result<Value> {
    let tool = crate::tool::files::read()?;
    let invocation = ToolInvocation {
        operation_id: OperationId::new(),
        call_id: "recon-read-replay".into(),
        name: "read".into(),
        arguments,
    };
    let context = ToolContext::new(
        Arc::new(task.clone()),
        invocation.operation_id,
        &invocation.call_id,
    )?;
    // Replay the unreserved call, retaining the actual file/render limits.
    // Finite range-work accounting is covered by the original grant replay.
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget::UNBOUNDED,
        &acyclic_fs::CancellationToken::new(),
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let (result, _hold) = tool
        .executor
        .execute_with_native_context(&context, &invocation, &work)?
        .await?;
    let full = tool
        .projection
        .project(&invocation, &result, Some(&work))?
        .0;
    tool.definition
        .validate_projection(&full, task.scope().limits(), None)?;
    assert!(serde_json::to_vec(&full).unwrap().len() as u64 <= task.scope().limits().render_bytes);
    let reference = crate::tools::catalog::READ
        .bind_with_projection(crate::tool::schema::ProjectionMode::Reference)?;
    let compact = reference
        .projection
        .project(&invocation, &result, Some(&work))?
        .0;
    reference
        .definition
        .validate_projection(&compact, task.scope().limits(), None)?;
    let source: crate::tool::files::FileReadSource =
        serde_json::from_value(result.value["file"].clone())
            .map_err(|error| Error::Invalid(error.to_string()))?;
    match source {
        crate::tool::files::FileReadSource::Content(expected) => {
            let projected: crate::model::ToolResultContent = serde_json::from_value(compact)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            let crate::model::ToolResultContent::Parts { parts } = projected else {
                panic!("immutable read lost its reference projection")
            };
            assert!(parts.iter().any(|part| matches!(part,
                crate::model::ModelDataPart::File { file, policy: crate::model::FileProjectionPolicy::Reference }
                    if file == &expected)));
        }
        crate::tool::files::FileReadSource::Authored(_) => {
            assert_eq!(compact["value"]["page"], full["value"]["page"]);
        }
    }
    Ok(full["value"].clone())
}

#[tokio::test]
async fn recon_read_calls_clip_eof_and_page_pinned_bytes() -> Result<()> {
    let storage = MemoryHarnessStorage::new(AgentId::new(), 2_000_000).await?;
    let private = MemoryHarnessStorage::new(AgentId::new(), 65_536).await?;
    // GC-4's recorded source length, with fewer than GC-2's 20,000 lines.
    let mut source = vec![b'x'; 1_103_745];
    for byte in source.chunks_mut(128) {
        *byte.last_mut().unwrap() = b'\n';
    }
    let path = ".recon/source-index.txt";
    let file = storage
        .stage(
            OperationId::new(),
            path,
            &source,
            "text/plain",
            "source-index.txt",
        )
        .await?;
    let limits = Limits {
        file_bytes: 65_536,
        render_bytes: 32_768,
        ..Limits::default()
    };
    let task = Bindings {
        scope: RuntimeScope::new(
            Capabilities::new([
                storage.read_capability.clone(),
                private.read_capability.clone(),
                private.write_capability.clone(),
                "tool:call:read".into(),
            ]),
            limits,
        )?,
        content: Some(storage.content_bindings()),
        artifacts: Some(private.content_bindings()),
        ..Bindings::local()
    }
    .build()?
    .agent_context(&Capabilities::default(), private.content_bindings())?;

    // GC-2: the original call, twice, also exercises an already published index.
    for offset in [20000, 20000, source.len().div_ceil(128) + 1] {
        let value = read(
            &task,
            json!({"path":path,"selection":{"kind":"lines","offset":offset}}),
        )
        .await?;
        assert_eq!(value["text"], "");
        assert_eq!(
            value["page"],
            json!({"start":source.len(),"end":source.len(),
            "offset":offset,"total":source.len(),"eof":true})
        );
    }
    // GC-3: preserve both authored and content version identity while clipping
    // the recorded end (1,118,094) to the selected generation's actual EOF.
    let initial = read(
        &task,
        json!({"path":path,"range":{"start":918094,"end":1118094}}),
    )
    .await?;
    assert!(source[918094..].starts_with(initial["text"].as_str().unwrap().as_bytes()));
    let authored = initial["version"].as_str().unwrap();
    let content = serde_json::to_string(&file).unwrap();
    storage
        .stage(
            OperationId::new(),
            path,
            b"replacement",
            "text/plain",
            "source-index.txt",
        )
        .await?;
    for version in [authored, &content] {
        let value = read(
            &task,
            json!({"path":path,"version":version,"range":{"start":1100000,"end":1118094}}),
        )
        .await?;
        assert_eq!(
            value["text"].as_str().unwrap().as_bytes(),
            &source[1100000..]
        );
        assert_eq!(value["page"]["total"], source.len());
        assert_eq!(value["page"]["eof"], true);
        assert!(value["page"].get("next").is_none());
        let empty = read(
            &task,
            json!({"path":path,"version":version,"range":{"start":1118094,"end":u64::MAX}}),
        )
        .await?;
        assert_eq!(empty["text"], "");
        assert_eq!(empty["page"]["start"], source.len());
        assert_eq!(empty["page"]["eof"], true);
    }
    // GC-4/5/6: the first page is visibly incomplete; following only the
    // returned cursor reconstructs the original generation exactly once.
    let mut cursor = 0;
    let mut assembled = Vec::new();
    loop {
        let value = read(
            &task,
            json!({"path":path,"version":authored,"range":{"start":cursor,"end":u64::MAX}}),
        )
        .await?;
        let text = value["text"].as_str().unwrap();
        assert_eq!(value["page"]["start"], cursor);
        assert_eq!(value["page"]["total"], source.len());
        assembled.extend_from_slice(text.as_bytes());
        cursor += text.len() as u64;
        assert_eq!(value["page"]["end"], cursor);
        if value["page"]["eof"] == true {
            assert!(value["page"].get("next").is_none());
            break;
        }
        assert!(!text.is_empty());
        assert_eq!(value["page"]["next"], cursor);
    }
    assert_eq!(assembled, source);
    // Exact and empty files carry the same terminal page contract.
    for bytes in [b"".as_slice(), b"tail", b"one\ntwo\n"] {
        storage
            .stage(
                OperationId::new(),
                path,
                bytes,
                "text/plain",
                "source-index.txt",
            )
            .await?;
        let value = read(&task, json!({"path":path})).await?;
        assert_eq!(value["text"].as_str().unwrap().as_bytes(), bytes);
        assert_eq!(value["page"]["eof"], true);
        let value = read(
            &task,
            json!({"path":path,"selection":{"kind":"lines","offset":20000}}),
        )
        .await?;
        assert_eq!(value["text"], "");
        assert_eq!(value["page"]["total"], bytes.len());
        assert_eq!(value["page"]["eof"], true);
    }
    Ok(())
}
