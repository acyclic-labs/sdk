//! Native demand files account for the fixture path key they retain.
use acyclic_fs::CancellationToken;
use acyclic_fs::demand::{DemandSource, native::NativeDemandSource};
use acyclic_fs::kernel::{LogicalName, NameEncoding, NamespacePath};
use acyclic_fs_format::model::{FilesystemProfile, VolumeLimits};
use acyclic_fs_work::performance::WorkBudget;

fn path(name: &str) -> NamespacePath {
    NamespacePath::new(
        vec![LogicalName::new(NameEncoding::Utf8, name.as_bytes().to_vec()).expect("name")],
        VolumeLimits::default(),
    )
    .expect("path")
}

#[test]
fn fixture_path_keys_are_accounted_in_held_files() -> std::result::Result<(), String> {
    let temporary = tempfile::tempdir().map_err(|error| error.to_string())?;
    std::fs::write(temporary.path().join("a"), b"a").map_err(|error| error.to_string())?;
    std::fs::write(temporary.path().join("longer-name"), b"b")
        .map_err(|error| error.to_string())?;
    let runtime =
        acyclic_native_runtime::testing::seeded_runtime(71).map_err(|error| error.to_string())?;
    runtime.block_on(async {
        tokio::time::pause();
        let (schedule, mut events) = acyclic_native_runtime::testing::Schedule::seeded(71);
        schedule.set_native_times_provider(|_| acyclic_native_runtime::testing::NativeTimes {
            created_ns: Some(1),
            modified_ns: Some(1),
            accessed_ns: Some(1),
            changed_ns: Some(1),
        });
        let mut world = Box::pin(schedule.scope(async {
            let source = NativeDemandSource::open(
                std::borrow::Cow::Owned(temporary.path().to_path_buf()),
                FilesystemProfile::Portable,
                VolumeLimits::default(),
                None,
                None,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error.to_string())?;
            let token = CancellationToken::new();
            let mut bytes = Vec::new();
            for name in ["a", "longer-name"] {
                let path = path(name);
                let node = source
                    .lookup(source.reference(), &path, WorkBudget::UNBOUNDED, &token)
                    .await
                    .map_err(|failure| failure.error.to_string())?
                    .value
                    .ok_or_else(|| format!("{name} absent"))?;
                let file = source
                    .open_file(
                        source.reference(),
                        &path,
                        node.version,
                        WorkBudget::UNBOUNDED,
                        &token,
                    )
                    .await
                    .map_err(|failure| failure.error.to_string())?
                    .value;
                bytes.push(file.logical_owned_bytes());
            }
            if bytes[0] >= bytes[1] {
                return Err(format!(
                    "fixture path key is not accounted: short {} >= long {}",
                    bytes[0], bytes[1]
                ));
            }
            Ok(())
        }));
        loop {
            tokio::select! {
                output = &mut world => break output,
                Some(boundary) = events.recv() => boundary.release(),
            }
        }
    })
}
