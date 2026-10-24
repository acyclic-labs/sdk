use super::*;

#[tokio::test]
async fn recovery_keeps_the_admitted_request_without_a_second_encoding() -> Result<()> {
    let storage =
        crate::filesystem::MemoryHarnessStorage::new(crate::AgentId::new(), 65_536).await?;
    let journal = storage.journal();
    let journal = journal.as_ref();
    let prepared = crate::model::PreparedModelRequest::prepare(
        ModelRequest {
            model: Model::new("test".into(), "recovery".into(), "1".into(), Value::Null)?,
            messages: (0..32)
                .map(|index| ModelMessage {
                    role: ModelRole::User,
                    content: ModelContent::Text(format!("{index}:{}", "payload".repeat(16))),
                })
                .collect(),
            tools: Vec::new(),
            max_output_tokens: None,
        },
        Limits::default(),
        None,
    )?;
    let operation = OperationId::new();
    let request = stage_request(journal, operation, "request", &prepared, None, None).await?;
    let projection = ResponseProjection {
        context: crate::context::Context {
            messages: crate::model::clone_messages(&prepared.request().messages, None)?,
            metadata: Default::default(),
            current_input_index: Some(0),
        },
        canonical: None,
        checkpoint: None,
        working: None,
    };
    let projection = stage_projection(
        journal,
        operation,
        "projection",
        &projection,
        &request,
        None,
    )
    .await?;
    for native in [false, true] {
        let mut encoded = Vec::new();
        for keep in [false, true] {
            let cancellation = acyclic_fs::CancellationToken::new();
            let budget = acyclic_fs::performance::work_budget(16 * 1024 * 1024, 16 * 1024 * 1024);
            let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let io = if native {
                PreparationIo::from_native_work(&work)?
            } else {
                PreparationIo::from_budget(budget, &cancellation)?
            };
            let baseline = io.owned_bytes()?;
            encoded.push(recovery_encoding(journal, &projection, &prepared, keep, &io).await?);
            assert_eq!(io.owned_bytes()?, baseline, "native={native} keep={keep}");
        }
        let [old, retained] = encoded.as_slice() else {
            return Err(invalid(None));
        };
        assert!(
            old - retained >= prepared.bytes().len() as u64,
            "native={native}: old={old} retained={retained}"
        );
    }
    Ok(())
}

async fn recovery_encoding(
    journal: &dyn ExecutionJournal,
    reference: &FileRef,
    expected: &crate::model::PreparedModelRequest,
    keep: bool,
    original: &PreparationIo,
) -> Result<u64> {
    let scope = crate::filesystem::PreparationScope::new(Some(original))?;
    let io = scope.io();
    let before = original.receipt().bytes_encoded;
    if keep {
        let ((projection, request), _hold) =
            load_prepared_projection(journal, reference, Limits::default(), io).await?;
        assert!(
            projection.context.messages.is_empty(),
            "recovery duplicated the admitted history"
        );
        assert_eq!(projection.context.current_input_index, Some(0));
        assert_eq!(request.bytes(), expected.bytes());
    } else {
        // Former recovery: discard the already-admitted bytes, then prepare
        // a second request from the recovered projection's messages.
        let (projection, _hold) =
            load_projection(journal, reference, Limits::default(), io).await?;
        let request = crate::model::PreparedModelRequest::prepare(
            ModelRequest {
                model: crate::model::clone_model(&expected.request().model, io)?,
                messages: crate::model::clone_messages(&projection.context.messages, io)?,
                tools: Vec::new(),
                max_output_tokens: None,
            },
            Limits::default(),
            io,
        )?;
        assert_eq!(request.bytes(), expected.bytes());
    }
    drop(scope);
    Ok(original.receipt().bytes_encoded - before)
}
