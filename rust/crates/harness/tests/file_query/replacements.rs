use super::*;
use acyclic_harness::{
    conversation::ContentGrant,
    task_volume::{FileKey, FileValue},
};
use proptest::prelude::*;

const ORIGINAL: &str = "---\nstatus: blocked\nreason: waiting\n---\nobjective";

fn grant(fixture: &Fixture) -> Result<ContentGrant> {
    let scope = fixture.issuer.issue(
        "goal-writer",
        &Capabilities::new([
            fixture
                .volume
                .capability(VolumeOperation::Read, &mut None)?,
            fixture
                .volume
                .capability(VolumeOperation::Write, &mut None)?,
        ]),
        Some(fixture.agent),
        None,
        &mut None,
    )?;
    ContentGrant::verify(
        &fixture.issuer.verifier(&mut None)?,
        &scope,
        &fixture.volume,
        VolumeOperation::Write,
        4096,
        &mut None,
    )
}

async fn indexed(fixture: &Fixture) -> Result<()> {
    fixture
        .host
        .put_content(
            &fixture.volume,
            &grant(fixture)?,
            "GOAL.md",
            ORIGINAL.as_bytes(),
            "text/markdown",
            "GOAL.md",
            4096,
            &IdempotencyKey::new(uuid::Uuid::new_v4().to_string())?,
            None,
            None,
        )
        .await?;
    Ok(())
}

fn unindexed(file: &FileValue, bytes: &[u8]) -> bool {
    file.bytes.as_ref() == bytes && file.publication.is_none()
}

async fn snapshot(
    fixture: &Fixture,
) -> std::result::Result<acyclic_harness::task_volume::Snapshot, Box<dyn std::error::Error>> {
    Ok(fixture
        .source
        .observe_files(Query::File(FileKey::Goal), None, 1, 4096)
        .await?
        .snapshot
        .unwrap())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // Raw replacement must invalidate provenance at the same cut as its bytes.
    // This includes same-byte replacement and a later return to the old text.
    #[test]
    fn goal_raw_replacements_preserve_snapshot_and_follow(
        replacement in prop_oneof![Just(ORIGINAL.to_owned()), "[a-zA-Z0-9 ]{1,30}"],
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
        runtime.block_on(async {
            let fixture = Fixture::new().await?;
            indexed(&fixture).await?;
            let observation = fixture.source.observe_files(Query::File(FileKey::Goal), None, 1, 4096).await?;
            let initial = observation.snapshot.unwrap();
            assert!(initial.files[0].publication.is_some());
            let mut view = FileQueryView::from_snapshot(Query::File(FileKey::Goal), initial, 1, 4096)?;
            let mut changes = observation.changes;
            for bytes in [replacement.as_str(), "---\nstatus: active\n---\nintermediate", ORIGINAL] {
                let writes = [WorkspaceMutation::PutFile {
                    path: "/GOAL.md".into(), bytes: bytes.as_bytes().to_vec().into(),
                }];
                let key = IdempotencyKey::new(uuid::Uuid::new_v4().to_string())?;
                let written = fixture.host.apply(&fixture.workspace, None, &writes, &key).await?;
                match fixture.host.apply(&fixture.workspace, None, &writes, &key).await {
                    Ok(replayed) => assert_eq!(written, replayed),
                    // The SDK rejects empty commits. A no-op replay must leave
                    // both the snapshot and live fold at the successful cut.
                    Err(error) => assert_eq!(error.to_string(), acyclic_fs::FsError::NoPendingMutations.to_string()),
                }
                let fresh = snapshot(&fixture).await?;
                assert!(unindexed(&fresh.files[0], bytes.as_bytes()));
                while view.cursor().position < fresh.cursor.position {
                    view.apply(next(&mut changes).await)?;
                }
                assert!(unindexed_head(&view.files()[&FileKey::Goal], bytes.as_bytes()));
                assert_eq!(view.cursor().commit, fresh.cursor.commit);
            }
            Ok::<_, Box<dyn std::error::Error>>(())
        }).map_err(|error| proptest::test_runner::TestCaseError::fail(error.to_string()))?;
    }
}

#[tokio::test]
async fn stale_goal_metadata_control_and_descriptor_fallback()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new().await?;
    indexed(&fixture).await?;
    let path = format!(
        "/.system/harness-file-metadata/{}.json",
        blake3::hash(b"GOAL.md")
    );
    let metadata = fixture
        .host
        .read(&fixture.workspace, None, &path, 4096, None)
        .await?;
    let observation = fixture
        .source
        .observe_files(Query::File(FileKey::Goal), None, 1, 4096)
        .await?;
    let mut view = FileQueryView::from_snapshot(
        Query::File(FileKey::Goal),
        observation.snapshot.unwrap(),
        1,
        4096,
    )?;
    let mut changes = observation.changes;
    for bytes in [ORIGINAL, "a readable replacement"] {
        fixture.write(&[("GOAL.md", Some(bytes))]).await?;
        // Broken writer: preserve the old descriptor/publication after raw write.
        fixture
            .host
            .apply(
                &fixture.workspace,
                None,
                &[WorkspaceMutation::PutFile {
                    path: path.clone(),
                    bytes: metadata.clone(),
                }],
                &IdempotencyKey::new(uuid::Uuid::new_v4().to_string())?,
            )
            .await?;
        let fresh = snapshot(&fixture).await?;
        while view.cursor().position < fresh.cursor.position {
            view.apply(next(&mut changes).await)?;
        }
        let file = &view.files()[&FileKey::Goal];
        assert_eq!(file.segments[0].as_ref(), bytes.as_bytes());
        if bytes == ORIGINAL {
            assert!(
                !unindexed_head(file, bytes.as_bytes()),
                "preserved-metadata control escaped the replacement oracle"
            );
        } else {
            assert!(
                unindexed_head(file, bytes.as_bytes()),
                "an obsolete descriptor must not end a readable goal query"
            );
        }
    }
    Ok(())
}

fn unindexed_head(file: &acyclic_harness::task_volume::HeadFile, bytes: &[u8]) -> bool {
    file.publication.is_none() && file.segments.len() == 1 && file.segments[0].as_ref() == bytes
}
