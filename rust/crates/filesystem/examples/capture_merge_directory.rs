//! Captures unchanged merge-directory writer output from a local deployment.

use acyclic_fs::workspace::{ApplyOptions, ForkOptions, MergeDirectorySection};
use acyclic_fs::{
    CancellationToken, Fs, FsError, IdempotencyKey, LocalOptions, MeasuredResult, OperationReceipt,
    PublicationPermit, WorkBudget, WorkCounters, WorkspaceError,
};
use std::{error::Error, num::NonZeroU32};

fn publication_ready(
    _: WorkBudget,
) -> std::future::Ready<MeasuredResult<OperationReceipt<()>, FsError>> {
    std::future::ready(Ok(OperationReceipt {
        value: (),
        work: WorkCounters::UNCHARGED,
    }))
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
            "merge-directory capture failed: {error}; recovery: cargo run --locked -p acyclic-fs --no-default-features --features local --example capture_merge_directory"
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
                publication_ready,
            )
            .await?
            .value;
    }
    main.write_text("/binary", "ours\0").await?;
    source.write_text("/binary", "theirs\0").await?;
    main.write_text("/deleted", "changed\0").await?;
    source.remove("/deleted").await?;
    source.write_text("/clean", "retained clean body").await?;
    let plan = source
        .join_into(&main)
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
    let mut directory = preview.directory(staging.clone(), IdempotencyKey::new().operation_id())?;
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
    let path = directory.path();
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
