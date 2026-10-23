//! Verify the retained request bound to every root-model effect, including orphans.
use super::*;
use crate::{
    core::SchemaRegistry,
    executor::ExecutionJournal,
    filesystem::{FilesystemExecutionJournal, FilesystemJournalBinding},
    model::{ModelRequest, ModelRole, PreparedModelRequest},
};

pub(super) async fn check(harness: &Harness, fixture: &Fixture) -> Result<()> {
    let dispatches = fixture.provider.dispatches();
    let turn = harness
        .current_turn()
        .await?
        .0
        .expect("retained root turn")
        .root;
    let operation = match fixture.scenario {
        Scenario::Compaction => dispatches.last(),
        _ => dispatches.first(),
    }
    .expect("root response reservation")
    .operation_id;
    let journal = FilesystemExecutionJournal::new(
        harness.runtime.task_host().workflow_stream(),
        harness.files.clone(),
        &SchemaRegistry::new(),
        FilesystemJournalBinding {
            volume: &harness.journal,
            verifier: &harness.verifier,
            scope: &harness.signed,
            maximum_bytes: harness.limits.file_bytes,
            io: None,
        },
    )?
    .with_turn(operation, turn);
    let mut checked = 0;
    for dispatch in dispatches
        .iter()
        .filter(|dispatch| dispatch.operation_id == operation)
    {
        let key = format!(
            "model:{}:request:{}",
            dispatch.step,
            blake3::Hash::from(dispatch.request_digest).to_hex(),
        );
        let request = journal.staged_ref_with_io(operation, &key, None).await?;
        let bytes = journal.load(&request, None)?.await?;
        let prepared = PreparedModelRequest::decode(&bytes, harness.limits, None)?;
        if matches!(fixture.scenario, Scenario::Compaction) {
            use crate::conversation::ContentResidencyVerifier;
            let prompt = prepared
                .request()
                .messages
                .iter()
                .filter(|message| message.role == ModelRole::User)
                .flat_map(|message| message.content.file_refs())
                .find(|file| file.path() == "history/2.txt")
                .ok_or_else(|| Error::Invalid("root request lost canonical prompt".into()))?;
            let reader = crate::filesystem::FilesystemContentVerifier::new(
                harness.files.clone(),
                &harness.verifier,
                &harness.signed,
                harness.limits.file_bytes,
                None,
            )?;
            assert_eq!(
                reader
                    .read(prompt, harness.limits.file_bytes, None, None)?
                    .await?
                    .as_ref(),
                PROMPT.as_bytes()
            );
        } else {
            check_prompt(prepared.request())?;
        }
        assert_eq!(
            prepared.request_digest(),
            dispatch.request_digest,
            "retained prompt differs from the actual generated/reconciled reservation"
        );
        checked += 1;
    }
    assert_eq!(
        checked,
        match fixture.scenario {
            Scenario::Text | Scenario::Compaction => 1,
            Scenario::EffectAndContinuation => 2,
        }
    );
    Ok(())
}

fn check_prompt(request: &ModelRequest) -> Result<()> {
    if request.messages.iter().any(|message| {
        message.role == ModelRole::User
            && matches!(&message.content, ModelContent::Text(text) if text == PROMPT)
    }) {
        Ok(())
    } else {
        Err(Error::Invalid(
            "root model request lost the original prompt".into(),
        ))
    }
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    #[test]
    fn request_check_rejects_replaced_and_missing_prompts(replacement in "[a-z]{0,40}") {
        let model = Model::new("scripted".into(), "test".into(), "1".into(), serde_json::Value::Null).unwrap();
        let mut request = ModelRequest {
            model,
            messages: vec![crate::model::ModelMessage { role: ModelRole::User, content: ModelContent::Text(PROMPT.into()) }],
            tools: vec![],
            max_output_tokens: None,
        };
        proptest::prop_assert!(check_prompt(&request).is_ok());
        request.messages[0].content = ModelContent::Text(replacement);
        proptest::prop_assert!(check_prompt(&request).is_err());
        request.messages.clear();
        proptest::prop_assert!(check_prompt(&request).is_err());
        // A scripted answer alone still succeeds in both broken variants; the
        // real sweep's independent request check must reject them.
    }
}
