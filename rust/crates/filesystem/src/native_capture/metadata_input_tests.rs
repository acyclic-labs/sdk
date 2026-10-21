use super::*;
use acyclic_native_runtime::testing::{NativeTimes, Schedule};

#[test]
fn fixture_times_are_inputs_while_physical_revalidation_stays_strict() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("file"), b"body").unwrap();
    let root = HostRoot::open(directory.path(), &mut crate::native_host::ordinary_work()).unwrap();
    let before = test_host_stat(&root, Path::new("file")).unwrap();
    let file = test_open_file(&root, Path::new("file")).unwrap();
    let held = test_held_stat(&root, &file).unwrap();
    let physical = capture_metadata(&before);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let mut encoded = Vec::new();
    for timestamp in [17, 19] {
        let (schedule, _events) = Schedule::seeded(71);
        schedule.set_native_times_provider(move |_| NativeTimes {
            created_ns: Some(timestamp),
            modified_ns: Some(timestamp),
            accessed_ns: Some(timestamp),
            changed_ns: Some(timestamp),
        });
        encoded.push(runtime.block_on(schedule.scope(async {
            let captured = capture_metadata(&before);
            assert_eq!(captured.modified_ns, MetadataField::Value(timestamp));
            assert_eq!(capture_metadata(&held), captured);
            assert_eq!(captured.posix_mode, physical.posix_mode);
            assert_eq!(captured.windows_attributes, physical.windows_attributes);
            ensure_same_host_node(&before, &held).unwrap();
            let mut changed = before.clone();
            #[cfg(unix)]
            {
                changed.raw.st_mtime += 1;
            }
            #[cfg(windows)]
            {
                changed.raw.LastWriteTime += 1;
            }
            assert!(
                ensure_same_host_node(&before, &changed).is_err(),
                "scripted time concealed a physical timestamp-only change"
            );
            let mut replaced = before.clone();
            #[cfg(unix)]
            {
                replaced.raw.st_ino += 1;
            }
            #[cfg(windows)]
            {
                replaced.raw.FileId += 1;
            }
            assert!(
                ensure_same_host_node(&before, &replaced).is_err(),
                "scripted time concealed replacement of the physical node"
            );
            crate::kernel::encode_file_metadata(
                captured,
                |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
                |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
            )
            .unwrap()
        })));
        assert_eq!(
            capture_metadata(&before),
            physical,
            "fixture inputs escaped their schedule"
        );
    }
    assert_ne!(
        encoded[0], encoded[1],
        "fixture time vanished from durable metadata"
    );
}
