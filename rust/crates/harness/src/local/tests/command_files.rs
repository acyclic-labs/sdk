//! The local owner consumes actual authorized request files through stock turns.
use super::*;
use crate::{
    client::{CommandResolution, CommandSession, run_local_command as consume_local_command},
    task_volume::{Component, FileKey, FileValue, Query, TaskFile},
};

use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(4))]
    // Every accepted prompt has one stock admission/provider call, and its
    // exact outcome survives both duplicate consumption and owner reopen.
    #[test]
    fn prompt_request_admits_once_and_reopens_its_file_outcome(prompt in "[a-z][a-z ]{0,48}") {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let result = runtime.block_on(roundtrip(prompt));
        prop_assert!(result.is_ok(), "{result:?}");
    }

    // Foreign task identities cannot dispatch a local model request, even if
    // the caller may write a request file in the containing private volume.
    #[test]
    fn foreign_task_requests_cannot_admit_a_local_model_turn(bytes in any::<[u8; 16]>()) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let result = runtime.block_on(foreign_request(bytes));
        prop_assert!(result.is_ok(), "{result:?}");
    }
}

async fn roundtrip(prompt: String) -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let provider = text();
    let harness = LocalHarness::open(
        LocalHarnessConfig::new(directory.path(), (model()?, provider.clone()))
            .without_default_tools(),
    )
    .await?;
    let session = uuid::Uuid::new_v4();
    let operation = OperationId::new();
    let request = FileValue::new(
        FileKey::Task {
            task: Component::new(harness.client_owner().id)?,
            file: TaskFile::Request(Component::new(operation.to_string())?),
        },
        serde_json::to_vec(&CommandResolution::Prompt {
            text: prompt,
            source: None,
            mentions: Vec::new(),
        })
        .map_err(super::super::storage)?,
    );
    let files = harness.task_files();
    let snapshot = files.snapshot(&Query::Root, 128).await?;
    files.write(snapshot.generation, &request).await?;
    let mut owner =
        CommandSession::open(Arc::new(files.inner.reopen().await?), session, 65_536, 128).await?;
    let observed = owner.next_request().await?;
    assert_eq!(observed, request);
    let output = consume_local_command(&mut owner, &harness, &observed)
        .await?
        .unwrap();
    assert_eq!(output["text"], "done");
    let mut projected = crate::client::TaskFileClient::observe(
        files.as_ref(),
        &harness.client_owner(),
        128,
        65_536,
    )
    .await?;
    projected.follow_commands(&owner).await?;
    let text: Vec<_> = projected
        .views()
        .selections()
        .flat_map(|(_, selection)| {
            selection.outputs(
                &crate::client::OutputOwner::Session(
                    crate::task_volume::Component::new(session.to_string()).unwrap(),
                ),
                0,
            )
        })
        .filter_map(|(_, row)| row.text.map(|text| text.into_owned()))
        .collect();
    assert_eq!(text, vec!["done"]);
    drop(projected);
    assert_eq!(provider.requests().len(), 1);
    assert!(
        consume_local_command(&mut owner, &harness, &observed)
            .await?
            .is_none()
    );
    assert_eq!(provider.requests().len(), 1);
    drop(owner);
    // Retain the exact native owner: successful reopening must come from close
    // detaching its namespace, rather than Arc destruction happening to win.
    let retained_mounts = Arc::clone(&harness.workspace.as_ref().unwrap().mounts);
    harness.close().await?;

    let harness = LocalHarness::open(
        LocalHarnessConfig::new(directory.path(), (model()?, provider.clone()))
            .without_default_tools(),
    )
    .await?;
    let mut owner = CommandSession::open(
        Arc::new(harness.task_files().inner.reopen().await?),
        session,
        65_536,
        128,
    )
    .await?;
    drop(retained_mounts);
    assert!(owner.take_request().is_none());
    assert!(
        consume_local_command(&mut owner, &harness, &observed)
            .await?
            .is_none()
    );
    assert_eq!(provider.requests().len(), 1);
    assert_eq!(owner.output_page(0)?.unwrap()["ok"], output);
    drop(owner);
    harness.close().await?;
    Ok(())
}

async fn foreign_request(mut bytes: [u8; 16]) -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let provider = scripted([]);
    let harness = LocalHarness::open(
        LocalHarnessConfig::new(directory.path(), (model()?, provider.clone()))
            .without_default_tools(),
    )
    .await?;
    if crate::TaskId::from_bytes(bytes).to_string() == harness.client_owner().id {
        bytes[0] ^= 1;
    }
    let operation = OperationId::new();
    let request = FileValue::new(
        FileKey::Task {
            task: Component::new(crate::TaskId::from_bytes(bytes).to_string())?,
            file: TaskFile::Request(Component::new(operation.to_string())?),
        },
        serde_json::to_vec(&CommandResolution::Prompt {
            text: "foreign request".into(),
            source: None,
            mentions: Vec::new(),
        })
        .map_err(super::super::storage)?,
    );
    let files = harness.task_files();
    let snapshot = files.snapshot(&Query::Root, 128).await?;
    files.write(snapshot.generation, &request).await?;
    let mut owner = CommandSession::open(
        Arc::new(files.inner.reopen().await?),
        uuid::Uuid::new_v4(),
        65_536,
        128,
    )
    .await?;
    // A missing target is NotFound; authority checks on existing targets remain
    // in the runtime. Neither rejection may create a child admission or dispatch.
    let rejected = consume_local_command(&mut owner, &harness, &request).await;
    assert!(matches!(rejected, Err(Error::NotFound(_))), "{rejected:?}");
    assert!(matches!(
        harness
            .runtime
            .task_host()
            .observe_admission(crate::runtime::task_id_for_admission(operation))
            .await,
        Err(Error::NotFound(_))
    ));
    assert_eq!(provider.requests().len(), 0);
    drop(owner);
    harness.close().await?;
    Ok(())
}

#[cfg(feature = "native-execution")]
#[tokio::test]
async fn retaining_a_mount_without_close_prevents_reopen_negative_control() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    // The native mount is composed by the default bash binding.
    let make = || LocalHarnessConfig::new(directory.path(), (model().unwrap(), scripted([])));
    let harness = LocalHarness::open(make()).await?;
    let retained = Arc::clone(&harness.workspace.as_ref().unwrap().mounts);
    crate::local::workspace::volumes::project(&retained).await?;
    drop(harness);
    let reopened = LocalHarness::open(make()).await?;
    let failure =
        crate::local::workspace::volumes::project(&reopened.workspace.as_ref().unwrap().mounts)
            .await
            .expect_err("negative control lost its retained native owner");
    assert!(
        failure
            .to_string()
            .contains("mount destination is already owned"),
        "{failure:?}"
    );
    for (_, mount) in retained.mounts() {
        if let Some(source) = &mount.view.source {
            let native = source.native().await;
            if let Some(native) = native.as_ref() {
                native.unmount().await?;
            }
        }
    }
    reopened.close().await?;
    Ok(())
}

#[path = "command_files/claim_gap.rs"]
mod claim_gap;
