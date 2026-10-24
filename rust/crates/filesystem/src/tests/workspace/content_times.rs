use super::*;
use crate::kernel::MetadataField;
use proptest::prelude::*;

const CHANGED: [&str; 7] = [
    "/batch", "/write", "/range", "/resize", "/zero", "/grow", "/clone",
];
const KEPT: [&str; 3] = ["/untouched", "/reserved", "/source"];

fn stamped(seconds: i64) -> FileMetadata {
    let at = MetadataField::Value(seconds * 1_000_000_000);
    FileMetadata {
        created_ns: at,
        modified_ns: at,
        accessed_ns: at,
        changed_ns: at,
        ..FileMetadata::default()
    }
}

/// The instant a transaction opened with `key` gives the files it changes.
fn change_time(key: IdempotencyKey) -> Result<i64, Box<dyn Error>> {
    Ok(key
        .operation_id()
        .time_ns()
        .ok_or("fresh keys are time-ordered")?)
}

/// What a changed file's stored `stamped(seconds)` becomes under `key`.
fn changed(seconds: i64, key: IdempotencyKey) -> Result<FileMetadata, Box<dyn Error>> {
    let at = MetadataField::Value(change_time(key)?);
    Ok(FileMetadata {
        modified_ns: at,
        changed_ns: at,
        ..stamped(seconds)
    })
}

type MemoryGeneration = Generation<
    crate::distributed::StreamAuthorityStore<acyclic_stream::MemoryStream>,
    crate::LogicalObjectStore<acyclic_objects::v1::MemoryObjects>,
>;

/// Every content change takes the transaction's instant; nothing else does.
async fn changed_generation(
    seconds: i64,
) -> Result<(MemoryGeneration, IdempotencyKey), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("content-times"))?;
    let mut transaction = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    for path in CHANGED.iter().chain(&KEPT) {
        transaction.write_text(path, "before").await?;
        unbounded!(transaction, set_metadata(path, stamped(seconds)))?;
    }
    let TransactionCommit::Committed(_) = commit(&mut transaction).await? else {
        return Err("seed did not commit".into());
    };
    let key = IdempotencyKey::new();
    let mut transaction = unbounded!(workspace, begin_transaction(key))?;
    // First, so the batch takes the bulk path rather than the serial one.
    unbounded!(
        transaction,
        write_batch(&[], &[("/batch", OwnedBytes::from_static(b"after"))])
    )?;
    transaction.write_text("/write", "after").await?;
    unbounded!(
        transaction,
        write_range("/range", 1, OwnedBytes::from_static(b"X"))
    )?;
    unbounded!(transaction, resize("/resize", 2))?;
    let range = |offset, length| crate::ByteRange { offset, length };
    unbounded!(transaction, zero_range("/zero", range(0, 2), false, false))?;
    unbounded!(transaction, preallocate("/grow", range(0, 64), false))?;
    unbounded!(transaction, preallocate("/reserved", range(0, 4), true))?;
    unbounded!(transaction, clone_range("/source", 0, "/clone", 2, 3))?;
    let TransactionCommit::Committed(generation) = commit(&mut transaction).await? else {
        return Err("content changes did not commit".into());
    };
    Ok((generation, key))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(8))]
    #[test]
    fn content_changes_take_the_transaction_instant_and_metadata_only_files_keep_theirs(
        seconds in 1_i64..4_000_000_000,
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        runtime.block_on(async {
            let fail = |error: Box<dyn Error>| TestCaseError::fail(error.to_string());
            let (generation, key) = changed_generation(seconds).await.map_err(fail)?;
            let view = crate::workspace::WorkspaceMetadata::from_engine;
            let expected = changed(seconds, key).map_err(fail)?;
            for path in CHANGED {
                let actual = unbounded!(generation, stat(path))
                    .map_err(|error| TestCaseError::fail(error.to_string()))?
                    .metadata;
                prop_assert_eq!(&actual, &view(expected), "{}", path);
            }
            // Negative control: the same comparison accepts nothing else.
            for path in KEPT {
                let actual = unbounded!(generation, stat(path))
                    .map_err(|error| TestCaseError::fail(error.to_string()))?
                    .metadata;
                prop_assert_eq!(&actual, &view(stamped(seconds)), "{}", path);
                prop_assert_ne!(&actual, &view(expected));
            }
            Ok(())
        })?;
    }
}

/// Stamps one bulk batch makes replacing `count` existing files, or creating
/// them when `existing` is false.
async fn batch_stamps(count: usize, existing: bool) -> Result<u64, Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("stamps"))?;
    let paths: Vec<String> = (0..count).map(|index| format!("/file-{index}")).collect();
    if existing {
        let mut seed = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
        for path in &paths {
            seed.write_text(path, "before").await?;
        }
        let TransactionCommit::Committed(_) = commit(&mut seed).await? else {
            return Err("seed did not commit".into());
        };
    }
    let files: Vec<_> = paths
        .iter()
        .map(|path| (path.as_str(), OwnedBytes::from_static(b"after")))
        .collect();
    let mut transaction = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    let stamps = || crate::kernel::CONTENT_STAMPS.with(std::cell::Cell::get);
    let before = stamps();
    unbounded!(transaction, write_batch(&[], &files))?;
    Ok(stamps() - before)
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn a_bulk_batch_stamps_each_replaced_file_once() -> Result<(), Box<dyn Error>> {
    for count in [8, 16, 32] {
        // One metadata read and write per replaced file: linear in the batch.
        assert_eq!(batch_stamps(count, true).await?, count as u64);
        // Negative control: created files carry their authored metadata, so
        // the count tracks replacements rather than operations.
        assert_eq!(batch_stamps(count, false).await?, 0);
    }
    Ok(())
}

#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
#[tokio::test]
async fn materialized_content_changes_show_the_transaction_instant_and_unchanged_files_keep_theirs()
-> Result<(), Box<dyn Error>> {
    const SECONDS: i64 = 1_000_000_000;
    let (generation, key) = changed_generation(SECONDS).await?;
    let destination = tempfile::tempdir()?;
    generation
        .materialize(
            ["/write", "/untouched"].into_iter(),
            &crate::MaterializeOptions {
                destination: destination.path().to_path_buf(),
                expected_root_identity: None,
                maximum_directory_entries: 32,
                maximum_extent_spans: 32,
                transfer_bytes: 4096,
            },
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(|failure| failure.error)?;
    let instant =
        |nanos: i64| std::time::UNIX_EPOCH + std::time::Duration::from_nanos(nanos.unsigned_abs());
    let modified = |path: &str| std::fs::metadata(destination.path().join(path))?.modified();
    assert_eq!(modified("untouched")?, instant(SECONDS * 1_000_000_000));
    assert_eq!(modified("write")?, instant(change_time(key)?));
    assert_eq!(std::fs::read(destination.path().join("write"))?, b"after");
    Ok(())
}

/// `make` rebuilds only when a source is strictly newer than its output. A
/// build writes its output through a live mount; every later transactional
/// edit of the source must then show as newer through the same mount.
#[cfg(all(
    feature = "native-mount",
    any(target_os = "linux", target_os = "macos")
))]
#[tokio::test(flavor = "multi_thread")]
async fn a_mounted_build_sees_each_transactional_edit_as_newer_than_its_output()
-> Result<(), Box<dyn Error>> {
    const SECONDS: i64 = 1_000_000_000;
    crate::initialize_native_mount_services()?;
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("make"))?;
    let mut seed = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    for path in ["/main.c", "/kept.h"] {
        seed.write_text(path, "int main;").await?;
        unbounded!(seed, set_metadata(path, stamped(SECONDS)))?;
    }
    let TransactionCommit::Committed(_) = commit(&mut seed).await? else {
        return Err("seed did not commit".into());
    };
    let destination = tempfile::tempdir()?;
    let mount = workspace
        .mount(
            destination.path(),
            crate::MountOptions::read_write().publication(crate::MountPublication::Manual),
        )
        .await?;
    let modified = |name: &str| std::fs::metadata(destination.path().join(name))?.modified();
    let captured = std::time::UNIX_EPOCH + std::time::Duration::from_secs(SECONDS.unsigned_abs());
    let mut previous = modified("main.c")?;
    assert_eq!(previous, captured);
    for round in 0..2 {
        std::fs::write(destination.path().join("main.o"), format!("object {round}"))?;
        mount.sync().await?;
        let built = modified("main.o")?;
        // Change times have millisecond resolution; open the edit in a later
        // millisecond than the build so "newer" is decidable.
        while std::time::SystemTime::now() < built + std::time::Duration::from_millis(1) {
            tokio::task::yield_now().await;
        }
        let mut edit = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
        let source = format!("int main{round};");
        edit.write_text("/main.c", &source).await?;
        let TransactionCommit::Committed(_) = commit(&mut edit).await? else {
            return Err("edit did not commit".into());
        };
        mount.advance_to_head().await?;
        let edited = modified("main.c")?;
        assert_eq!(
            std::fs::read_to_string(destination.path().join("main.c"))?,
            source
        );
        assert!(
            edited > built,
            "round {round}: the edit is not newer than the build output"
        );
        assert!(
            edited > previous,
            "round {round}: the edit did not advance the source time"
        );
        // Negative control: the untouched header keeps its captured time.
        assert_eq!(modified("kept.h")?, captured);
        previous = edited;
    }
    mount.unmount().await?;
    Ok(())
}
