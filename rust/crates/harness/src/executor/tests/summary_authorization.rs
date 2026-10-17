use super::*;
use crate::{conversation::FileDescriptor, filesystem::MemoryHarnessStorage};
use proptest::prelude::*;

fn message(file: FileRef) -> ModelContent {
    ModelContent::Part(ModelContentPart::File {
        file,
        policy: crate::model::FileProjectionPolicy::BoundedFull,
    })
}

async fn permitted(
    journal: &dyn ExecutionJournal,
    scope: &RuntimeScope,
    file: &FileRef,
) -> Result<bool> {
    let content = message(file.clone());
    match verify_model_contents_scoped(
        journal,
        std::iter::once(&content),
        Limits::default(),
        Some(scope),
        None,
        None,
    )
    .await
    {
        Ok(()) => Ok(true),
        Err(Error::Unauthorized(_)) => Ok(false),
        Err(error) => Err(error),
    }
}

#[test]
fn only_the_authenticated_immutable_summary_is_admitted() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let storage = runtime.block_on(MemoryHarnessStorage::new(crate::AgentId::new(), 65_536))?;
    let journal = storage.journal();
    let operation = OperationId::new();
    let file = runtime.block_on(journal.stage(
        operation,
        "summary:0:output",
        b"summary",
        "text/plain",
        None,
    )?)?;
    assert_eq!(
        super::super::summary_identity::summary_operation(file.path()),
        Some(operation)
    );
    let other = runtime.block_on(journal.stage(
        operation,
        "unrelated-private-file",
        b"secret",
        "text/plain",
        None,
    )?)?;
    let narrow = RuntimeScope::new(Capabilities::new([] as [&str; 0]), Limits::default())?;
    // Negative control for the former behavior: an output without authenticated
    // provenance is rejected even though its bytes exist in the journal.
    assert!(!runtime.block_on(permitted(journal.as_ref(), &narrow, &file))?);
    let summary = crate::context::ContextSummary {
        operation_id: operation,
        step: 0,
        source_messages: 1,
        source_digest: [7; 32],
        output: file.clone(),
    };
    runtime.block_on(super::super::summary::stage_summary_provenance(
        journal.as_ref(),
        &summary,
        None,
    ))?;
    assert!(
        runtime.block_on(journal.authenticate_summary(&file, None)?)?,
        "published provenance was not found by exact output identity"
    );
    let observe = |scope: &RuntimeScope| -> Result<[bool; 2]> {
        Ok([
            runtime.block_on(permitted(journal.as_ref(), scope, &file))?,
            runtime.block_on(permitted(journal.as_ref(), scope, &other))?,
        ])
    };
    let exact = |observed| observed == [true, false];
    let observed = observe(&narrow)?;
    assert!(
        exact(observed),
        "unexpected exact-summary projection permissions: {observed:?}"
    );
    // Deliberately broken grant: the same detector must reject a root-directory
    // grant, which admits unrelated private journal contents.
    let broad = RuntimeScope::new(
        Capabilities::new([file.volume().directory_read_capability("", &mut None)?]),
        Limits::default(),
    )?;
    assert!(
        !exact(observe(&broad)?),
        "detector missed a broad private-volume grant"
    );
    let strategy = (0usize..7, "[a-z]{1,16}", any::<u8>());
    proptest::test_runner::TestRunner::default()
        .run(&strategy, |(field, suffix, byte)| {
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
                    volume = crate::conversation::VolumeRef::new(
                        file.volume().provider().clone(),
                        format!("foreign-{suffix}"),
                        file.volume().class(),
                        file.volume().owner().clone(),
                        &mut None,
                    )?
                }
            }
            let candidate = FileRef::new(
                volume,
                path,
                version,
                FileDescriptor::new(digest, length, media)?,
                label,
                &mut None,
            )?;
            prop_assert!(
                !runtime.block_on(journal.authenticate_summary(&candidate, None)?)?,
                "a changed immutable reference inherited summary authority"
            );
            let mut round_trip = summary.clone();
            round_trip.output = candidate;
            let encoded = serde_json::to_vec(&round_trip)?;
            prop_assert_eq!(
                serde_json::from_slice::<crate::context::ContextSummary>(&encoded)?,
                round_trip
            );
            Ok(())
        })
        .unwrap();
    Ok(())
}
