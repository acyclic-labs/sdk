use super::*;
use crate::{
    context::ContextSummary, conversation::ContentPublisher, durable_host::tests::fixture::Fixture,
    runtime::TaskSpawner,
};
use acyclic_fs::Fs;
use acyclic_stream::MemoryStream;
use serde_json::json;

mod accounting;

async fn fixture() -> Result<Fixture> {
    Fixture::new(
        Arc::new(MemoryStream::default()),
        Fs::memory(),
        json!({"type":"integer"}),
        None,
        &[],
    )
    .await
}

fn proof(operation: OperationId, output: FileRef) -> ContextSummary {
    ContextSummary {
        operation_id: operation,
        step: 0,
        source_messages: 1,
        source_digest: [7; 32],
        output,
    }
}

async fn output(
    journal: &dyn ExecutionJournal,
    operation: OperationId,
    key: &str,
) -> Result<FileRef> {
    journal
        .stage(operation, key, b"summary", "text/plain", None)?
        .await
}

async fn publish(
    journal: &dyn ExecutionJournal,
    candidate: &FileRef,
    proof: &ContextSummary,
) -> Result<FileRef> {
    let key = candidate.read_capability(&mut None)?;
    let bytes = serde_json::to_vec(proof).map_err(|error| Error::Invalid(error.to_string()))?;
    journal
        .stage(proof.operation_id, &key, &bytes, "application/json", None)?
        .await
}

#[tokio::test]
async fn summary_authentication_binds_the_task_execution_even_in_a_shared_volume() -> Result<()> {
    use futures::FutureExt;
    use proptest::prelude::*;
    let fixture = fixture().await?;
    let turn = OperationId::new();
    let first_owner = fixture.running(80).await?;
    let first = super::super::task_runtime::execution_operation(first_owner.task_id(), turn, None)?;
    let first = fixture
        .execution(first_owner, first)?
        .with_turn(first, turn);
    let second_owner = fixture.running(81).await?;
    let second_operation =
        super::super::task_runtime::execution_operation(second_owner.task_id(), turn, None)?;
    let second = fixture
        .execution(second_owner, second_operation)?
        .with_turn(second_operation, turn);
    assert_eq!(first.volume, second.volume);
    let operation = first.execution.unwrap().0;
    let file = output(&first, operation, "summary:0:output").await?;
    publish(&first, &file, &proof(operation, file.clone())).await?;
    assert!(first.authenticate_summary(&file, None)?.await?);
    assert!(!second.authenticate_summary(&file, None)?.await?);
    // An inherited checkpoint may carry an explicit exact read capability. It
    // does not inherit the automatic exception for another task's summaries.
    let content = crate::model::ModelContent::Part(crate::model::ModelContentPart::File {
        file: file.clone(),
        policy: crate::model::FileProjectionPolicy::BoundedFull,
    });
    let denied = crate::runtime::RuntimeScope::new(
        crate::Capabilities::new([] as [&str; 0]),
        Limits::default(),
    )?;
    let explicit = crate::runtime::RuntimeScope::new(
        crate::Capabilities::new([file.read_capability(&mut None)?]),
        Limits::default(),
    )?;
    for (scope, permitted) in [(&denied, false), (&explicit, true)] {
        let result = crate::executor::verify_model_contents_scoped(
            &second,
            std::iter::once(&content),
            Limits::default(),
            Some(scope),
            None,
            None,
        )
        .await;
        assert_eq!(
            result.is_ok(),
            permitted,
            "inherited summary requires its exact read grant: {result:?}"
        );
    }
    // A caller cannot rebind another task's owner to the first task's operation.
    let forged = fixture
        .execution(fixture.owner(81).await?, operation)?
        .with_turn(operation, turn);
    assert!(!forged.authenticate_summary(&file, None)?.await?);
    // Arbitrary execution or turn substitutions must be rejected by the real
    // journal before reaching any asynchronous storage boundary.
    let first = std::cell::RefCell::new(first);
    proptest::test_runner::TestRunner::default()
        .run(&(any::<[u8; 16]>(), any::<[u8; 16]>()), |(bound, nonce)| {
            let mut first = first.borrow_mut();
            for (bound, turn) in [
                (OperationId::from_bytes(bound), turn),
                (operation, OperationId::from_bytes(nonce)),
            ] {
                let derived = super::super::task_runtime::execution_operation(
                    first.owner.as_ref().unwrap().0.task_id(),
                    turn,
                    None,
                )?;
                if bound != operation || derived != operation {
                    first.execution = Some((bound, turn));
                    prop_assert_eq!(
                        first
                            .authenticate_summary_with_io(&file, None)
                            .now_or_never()
                            .transpose()?,
                        Some(false)
                    );
                }
            }
            Ok(())
        })
        .unwrap();
    Ok(())
}

#[tokio::test]
async fn a_head_file_is_not_an_upload_receipt_and_cannot_replace_pinned_provenance() -> Result<()> {
    let fixture = fixture().await?;
    let owner = fixture.running(82).await?;
    let turn = OperationId::new();
    let operation = super::super::task_runtime::execution_operation(owner.task_id(), turn, None)?;
    let journal = fixture
        .execution(owner, operation)?
        .with_turn(operation, turn);
    let file = output(&journal, operation, "summary:0:output").await?;
    let summary = proof(operation, file.clone());
    let key = file.read_capability(&mut None)?;
    let (_, path) = journal.stage_identity_with_io(operation, &key, None)?;
    let workspace = super::super::workspace_ref(
        journal.volume.provider().clone(),
        &journal.volume.storage_name(&mut None)?,
    )?;
    let bytes = serde_json::to_vec(&summary).map_err(|error| Error::Invalid(error.to_string()))?;
    // Model-facing publication cannot mint a protected execution upload receipt.
    assert!(
        fixture
            .publisher
            .stage_once(
                OperationId::new(),
                &path,
                &bytes,
                "application/json",
                "forged.json",
                crate::conversation::ContentAccess::default(),
            )?
            .await
            .is_err()
    );
    // Raw adversarial storage injection creates the exact expected path/body,
    // but has no original harness upload under the expected operation identity.
    fixture
        .files
        .apply(
            &workspace,
            None,
            &[super::super::WorkspaceMutation::PutFile {
                path: format!("/{path}"),
                bytes: bytes.clone().into(),
            }],
            &IdempotencyKey::new("forged-summary-path")?,
        )
        .await?;
    assert!(!journal.authenticate_summary(&file, None)?.await?);
    publish(&journal, &file, &summary).await?;
    assert!(journal.authenticate_summary(&file, None)?.await?);
    let mut bad = summary;
    bad.source_messages = 0;
    fixture
        .files
        .apply(
            &workspace,
            None,
            &[super::super::WorkspaceMutation::PutFile {
                path: format!("/{path}"),
                bytes: serde_json::to_vec(&bad)
                    .map_err(|error| Error::Invalid(error.to_string()))?
                    .into(),
            }],
            &IdempotencyKey::new("replace-summary-head")?,
        )
        .await?;
    assert!(
        journal.authenticate_summary(&file, None)?.await?,
        "mutable Head substituted the original upload generation"
    );
    Ok(())
}

#[tokio::test]
async fn malformed_provenance_at_the_correct_lookup_key_exercises_payload_guards() -> Result<()> {
    let fixture = fixture().await?;
    let owner = fixture.running(83).await?;
    let turn = OperationId::new();
    let operation = super::super::task_runtime::execution_operation(owner.task_id(), turn, None)?;
    let journal = fixture
        .execution(owner, operation)?
        .with_turn(operation, turn);
    for mutation in 0..3 {
        let file = output(&journal, operation, &format!("summary:{mutation}:output")).await?;
        let mut summary = proof(operation, file.clone());
        match mutation {
            0 => {
                summary.output = FileRef::new(
                    file.volume().clone(),
                    file.path().into(),
                    file.version().into(),
                    file.descriptor().clone(),
                    "another output".into(),
                    &mut None,
                )?
            }
            1 => summary.source_messages = 0,
            _ => summary.operation_id = OperationId::new(),
        }
        // Publish a genuine receipt at the candidate's key, even for a payload
        // that names a different operation. Absence cannot explain rejection.
        let key = file.read_capability(&mut None)?;
        let bytes =
            serde_json::to_vec(&summary).map_err(|error| Error::Invalid(error.to_string()))?;
        journal
            .stage(operation, &key, &bytes, "application/json", None)?
            .await?;
        assert!(
            !journal.authenticate_summary(&file, None)?.await?,
            "accepted malformed payload {mutation}"
        );
        if mutation == 0 {
            assert!(
                super::summary_authorization::payload_matches::<false>(&summary, operation, &file),
                "assertion-deletion control no longer removes the output check"
            );
            assert!(
                !super::summary_authorization::payload_matches::<true>(&summary, operation, &file),
                "same detector missed removal of the complete output comparison"
            );
        }
    }
    Ok(())
}

#[test]
fn complete_summary_output_identity_is_required_even_at_a_valid_provenance_key() -> Result<()> {
    use proptest::prelude::*;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let storage = runtime.block_on(crate::filesystem::MemoryHarnessStorage::new(
        crate::AgentId::new(),
        65_536,
    ))?;
    let operation = OperationId::new();
    let file = runtime.block_on(output(
        storage.journal().as_ref(),
        operation,
        "summary:0:output",
    ))?;
    proptest::test_runner::TestRunner::default()
        .run(
            &(0usize..7, "[a-z]{1,16}", any::<u8>()),
            |(field, suffix, byte)| {
                let mut summary = proof(operation, file.clone());
                let mut path = file.path().to_owned();
                let mut version = file.version().to_owned();
                let mut digest = *file.descriptor().sha256();
                let mut length = file.descriptor().byte_length();
                let mut media = file.descriptor().media_type().to_owned();
                let mut label = file.display_name().to_owned();
                let mut volume = file.volume().clone();
                match field {
                    0 => path.push_str(&suffix),
                    1 => version.push_str(&suffix),
                    2 => digest[byte as usize % 32] ^= 1,
                    3 => length += 1,
                    4 => media.push_str(&suffix),
                    5 => label.push_str(&suffix),
                    _ => {
                        volume = VolumeRef::new(
                            file.volume().provider().clone(),
                            format!("foreign-{suffix}"),
                            file.volume().class(),
                            file.volume().owner().clone(),
                            &mut None,
                        )?
                    }
                }
                summary.output = FileRef::new(
                    volume,
                    path,
                    version,
                    crate::conversation::FileDescriptor::new(digest, length, media)?,
                    label,
                    &mut None,
                )?;
                prop_assert!(!super::summary_authorization::payload_matches::<true>(
                    &summary, operation, &file
                ));
                prop_assert!(
                    super::summary_authorization::payload_matches::<false>(
                        &summary, operation, &file
                    ),
                    "assertion deletion was not detected by the same property"
                );
                Ok(())
            },
        )
        .unwrap();
    Ok(())
}

#[test]
fn fixture_admissions_reject_generated_nonintegers() -> Result<()> {
    use proptest::prelude::*;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let fixture = runtime.block_on(fixture())?;
    let invalid = prop_oneof![
        Just(serde_json::Value::Null),
        any::<bool>().prop_map(serde_json::Value::Bool),
        ".{0,32}".prop_map(serde_json::Value::String),
        prop::collection::vec(any::<i16>(), 0..5).prop_map(|values| json!(values)),
        any::<i16>().prop_map(|value| json!({"value": value})),
        (-10_000i32..10_000).prop_map(|value| json!(value as f64 + 0.5)),
    ];
    proptest::test_runner::TestRunner::default()
        .run(&invalid, |input| {
            let admission = fixture.admission(90, input);
            let rejects = |admission: &crate::runtime::TaskAdmissionRecord| {
                matches!(
                    crate::executor::validate_json_schema_value(
                        &admission.input_schema,
                        &admission.input,
                        "task input",
                        &mut None,
                    ),
                    Err(Error::Invalid(_))
                )
            };
            prop_assert!(rejects(&admission), "fixture accepted a noninteger");
            prop_assert!(matches!(
                runtime.block_on(fixture.host.admit(admission.clone())),
                Err(Error::Invalid(_))
            ));
            let mut permissive = admission;
            permissive.input_schema = json!({});
            prop_assert!(
                !rejects(&permissive),
                "same detector missed the former permissive schema"
            );
            Ok(())
        })
        .unwrap();
    Ok(())
}
