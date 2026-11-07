//! Captures unchanged merge-directory writer output from a local deployment.

use acyclic_fs::foundation::OperationId;
use acyclic_fs::kernel::{FileMetadata, MetadataField};
use acyclic_fs::workspace::{
    ApplyOptions, ForkOptions, MergeConflictFile, MergeConflictFileKind, MergeDirectorySection,
};
use acyclic_fs::{
    AsyncAuthorityStore, AsyncObjectStore, CancellationToken, Fs, IdempotencyKey, LocalOptions,
    OperationReceipt, PublicationPermit, Transaction, WorkBudget, WorkCounters, WorkspaceError,
};
use std::{error::Error, num::NonZeroU32};

async fn commit<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>(
    transaction: &mut Transaction<A, O>,
    cancellation: &CancellationToken,
) -> Result<(), WorkspaceError> {
    transaction
        .commit::<WorkspaceError, _, _>(
            None,
            PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            cancellation,
            |_| {
                std::future::ready(Ok(OperationReceipt {
                    value: (),
                    work: WorkCounters::UNCHARGED,
                }))
            },
        )
        .await
        .map(|_| ())
        .map_err(|failure| failure.error)
}

fn emit(name: &str, bytes: &[u8]) -> Result<(), Box<dyn Error>> {
    println!(
        "{}",
        serde_json::to_string(&serde_json::json!({
            "name": name, "bytes": bytes,
        }))?
    );
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    if let Err(error) = capture().await {
        eprintln!(
            "step=merge-directory-capture; cause={error}; recovery: cargo run --locked -p acyclic-fs --no-default-features --features local --example capture_merge_directory"
        );
        std::process::exit(1);
    }
}

async fn capture() -> Result<(), Box<dyn Error>> {
    let root = tempfile::tempdir()?;
    let fs = Fs::local(LocalOptions::new(root.path())).await?;
    let cancellation = CancellationToken::new();
    let work = WorkBudget::UNBOUNDED;
    let main = fs
        .create_workspace("merge-main", work, &cancellation)
        .await?
        .value;
    for path in ["/binary", "/deleted"] {
        main.write_text(path, "base\0").await?;
    }
    let mut transaction = main
        .begin_transaction(IdempotencyKey::new(), work, &cancellation)
        .await?
        .value;
    transaction
        .create_directory("/mode", work, &cancellation)
        .await?;
    commit(&mut transaction, &cancellation).await?;
    let mut source = main.clone();
    for name in ["merge-child", "merge-grandchild"] {
        let generation = source.head(work, &cancellation).await?.value;
        source = source
            .fork::<_, _, WorkspaceError>(
                name,
                ForkOptions::from_generation(generation, IdempotencyKey::new()),
                PublicationPermit::Unrestricted,
                work,
                &cancellation,
                (),
            )
            .await?
            .value;
    }
    main.write_text("/binary", "ours\0").await?;
    source.write_text("/binary", "theirs\0").await?;
    main.write_text("/deleted", "changed\0").await?;
    source.remove("/deleted").await?;
    source.write_text("/clean", "retained clean body").await?;
    main.write_text("/kind", "ours").await?;
    for (workspace, mode, create_kind) in [(&main, 0o700, false), (&source, 0o770, true)] {
        let mut transaction = workspace
            .begin_transaction(IdempotencyKey::new(), work, &cancellation)
            .await?
            .value;
        transaction
            .set_metadata(
                "/mode",
                FileMetadata {
                    posix_mode: MetadataField::Value(mode),
                    ..FileMetadata::default()
                },
                work,
                &cancellation,
            )
            .await?;
        if create_kind {
            transaction
                .create_directory("/kind", work, &cancellation)
                .await?;
        }
        commit(&mut transaction, &cancellation).await?;
    }
    let plan = source
        .join_into(&main)
        .git_metadata()
        .plan(work, &cancellation)
        .await?
        .value;
    let options = ApplyOptions {
        if_target: plan.target_head(),
        idempotency_key: IdempotencyKey::new(),
    };
    let preview = plan
        .preview_pages(options, work, &cancellation)
        .await?
        .value;
    let staging = fs
        .create_workspace("merge-staging", work, &cancellation)
        .await?
        .value;
    let mut directory = preview.directory(staging.clone(), OperationId::new())?;
    let step = NonZeroU32::new(1).ok_or("zero page size")?;
    while !directory.plan().complete {
        let section = if directory.plan().conflicts_complete {
            MergeDirectorySection::Clean
        } else {
            MergeDirectorySection::Conflicts
        };
        directory
            .write_page(section, step, PublicationPermit::Unrestricted)
            .await?;
    }
    let generation = staging.head(work, &cancellation).await?.value;
    let path = directory.path().to_owned();
    let bytes = generation
        .read(&format!("{path}/plan.json"), 16384, work, &cancellation)
        .await?
        .value;
    emit("plan-v1", &bytes)?;
    for section in ["clean", "conflicts"] {
        let mut after = None;
        let mut index = 0;
        loop {
            let (page, _allocation) = generation
                .list_directory(
                    &format!("{path}/{section}"),
                    after.as_ref(),
                    1,
                    work,
                    &cancellation,
                )
                .await?
                .value;
            for entry in &page.entries {
                let name = entry.name.unicode_text(work, &cancellation)?.value;
                let bytes = generation
                    .read(
                        &format!("{path}/{section}/{name}"),
                        16384,
                        work,
                        &cancellation,
                    )
                    .await?
                    .value;
                emit(&format!("{section}-{index}-v1"), &bytes)?;
                if section == "conflicts" {
                    let descriptor: MergeConflictFile = serde_json::from_slice(&bytes)?;
                    let proposals: &[(&str, &str)] = match descriptor.kind {
                        MergeConflictFileKind::Binary => &[
                            ("take-ours", r#"{"take":"ours"}"#),
                            ("take-theirs", r#"{"take":"theirs"}"#),
                        ],
                        MergeConflictFileKind::Deleted => &[("remove", r#"{"remove":true}"#)],
                        _ => &[],
                    };
                    for (label, document) in proposals {
                        let proposal_path = format!(
                            "{}.resolve.json",
                            directory.conflict_file_path(&descriptor.conflict.key)?
                        );
                        staging.write_text(&proposal_path, document).await?;
                        let proposed = staging.head(work, &cancellation).await?.value;
                        let accepted = directory
                            .resolution(&proposed, &descriptor.conflict.key)
                            .await?;
                        let original = proposed
                            .read(&proposal_path, 16384, work, &cancellation)
                            .await?
                            .value;
                        // The expected resolution is real public admission output.
                        let trace = serde_json::to_vec(&(
                            &descriptor.conflict,
                            original.as_ref(),
                            accepted,
                        ))?;
                        emit(&format!("resolution-{label}-v1"), &trace)?;
                    }
                }
                index += 1;
            }
            if !page.has_more {
                break;
            }
            after = Some(
                page.entries
                    .last()
                    .ok_or("directory continuation without entry")?
                    .name
                    .clone(),
            );
        }
    }
    Ok(())
}
