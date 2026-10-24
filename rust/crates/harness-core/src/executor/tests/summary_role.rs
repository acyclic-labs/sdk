use super::*;

struct SummaryRoleProbe {
    cheap: Model,
    script: ScriptedModelProvider,
    roles: std::sync::Mutex<Vec<String>>,
}
impl ModelProvider for SummaryRoleProbe {
    fn model_for_turn<'a>(
        &'a self,
        fallback: &'a Model,
        turn: crate::model::ModelTurn<'_>,
        _: crate::NativeWork<'_>,
    ) -> Result<&'a Model> {
        self.roles
            .lock()
            .map_err(|_| Error::Storage("fixture role lock".into()))?
            .push(turn.role.to_owned());
        Ok(if turn.role == "summary" {
            &self.cheap
        } else {
            fallback
        })
    }
    fn context_capacity(&self, model: &Model) -> Result<crate::context::ModelContextCapacity> {
        self.script.context_capacity(model)
    }
    fn recorded_model<'a>(
        &'a self,
        fallback: &'a Model,
        recorded: &Model,
        _: crate::NativeWork<'_>,
    ) -> Result<&'a Model> {
        if recorded == &self.cheap {
            Ok(&self.cheap)
        } else if recorded == fallback {
            Ok(fallback)
        } else {
            Err(Error::Invalid("foreign recorded member".into()))
        }
    }
    fn count_tokens<'a>(
        &'a self,
        request: &'a crate::model::PreparedModelRequest,
    ) -> BoxFuture<'a, Result<crate::context::ModelTokenCount>> {
        self.script.count_tokens(request)
    }
    fn generate<'a>(
        &'a self,
        request: crate::model::PreparedModelRequest,
        dispatch: crate::model::ModelDispatch,
    ) -> acyclic_stream::BoxProviderStream<'a, Result<ModelEvent>> {
        self.script.generate(request, dispatch)
    }
}
#[tokio::test]
async fn summaries_resolve_their_role_and_replay_the_recorded_member() -> Result<()> {
    for entry in 0..3 {
        let parent = Model::new("test".into(), "expensive".into(), "1".into(), Value::Null)?;
        let provider = Arc::new(SummaryRoleProbe {
            cheap: Model::new("test".into(), "cheap".into(), "1".into(), Value::Null)?,
            script: ScriptedModelProvider::new([ScriptedTurn::Text("summary".into())])
                .with_context_capacity(crate::context::ModelContextCapacity {
                    context_tokens: 65_536,
                    output_tokens: Some(4096),
                })
                .with_token_counter(fixture_token_count),
            roles: std::sync::Mutex::new(Vec::new()),
        });
        let executor = uncompacted_executor(
            parent.clone(),
            provider.clone(),
            ContextPipeline::default(),
            ToolRegistry::default(),
        )
        .with_max_output_tokens(1024)?;
        let journal = Journal::default();
        let operation = OperationId::new();
        let source = crate::context::Context {
            messages: vec![ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("source".into()),
            }],
            ..Default::default()
        };
        let instruction = ModelContent::Text("summarize".into());
        let input = TurnInput {
            operation_id: operation,
            input: ModelContent::Text("parent".into()),
            selected_context: None,
            max_steps: 2,
        };
        let mut summaries = Vec::new();
        for _ in 0..2 {
            let summary = match entry {
                0 => {
                    executor
                        .summarize(&journal, operation, source.clone(), instruction.clone())
                        .await?
                }
                1 => {
                    executor
                        .summarize_in_turn(&journal, &input, 0, source.clone(), instruction.clone())
                        .await?
                }
                _ => {
                    StockExecution::ordinary(&executor)
                        .for_model(&parent)
                        .summarize(
                            &journal,
                            Some(&input),
                            operation,
                            0,
                            (source.clone(), instruction.clone()),
                            &mut DispatchPhase::new(),
                        )
                        .await?
                        .0
                }
            };
            summaries.push(summary);
        }
        let [first, replayed] = summaries.as_slice() else {
            unreachable!()
        };
        assert_eq!(first, replayed);
        let roles = provider.roles.lock().unwrap();
        let requests = provider.script.requests();
        println!(
            "SUMMARY_ROLE entry={entry} roles={roles:?} selected={:?}",
            requests
                .iter()
                .map(|request| request.model.name.as_str())
                .collect::<Vec<_>>()
        );
        assert!(!roles.is_empty());
        assert_eq!(requests.len(), 1);
        assert!(roles.iter().all(|role| role == "summary"));
        assert!(requests.iter().all(|request| request.model.name == "cheap"));
    }
    Ok(())
}
