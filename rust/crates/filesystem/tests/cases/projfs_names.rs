//! Scoped reuse of native names and their existing buffers.

#[path = "../../src/native_mount/path.rs"]
mod mount_path;
pub use mount_path::MountPath;
use std::sync::{RwLock, TryLockError};

#[path = "../../src/native_mount/projfs/creating_name.rs"]
mod creating_name;
use creating_name::*;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn path(name: &str) -> MountPath {
    name.split('\\').fold(MountPath::root(), |path, component| {
        path.child(
            component
                .encode_utf16()
                .flat_map(u16::to_le_bytes)
                .collect(),
        )
    })
}

#[test]
fn creator_lends_the_existing_path_and_restores_it_on_return() -> TestResult {
    let slot = RwLock::new((MountPath::root(), None));
    let mut name = path("nested\\éclair");
    let original = name.clone();
    let outer = name.components().as_ptr();
    let leaf = name.components().get(1).ok_or("missing leaf")?.as_ptr();
    let result = with_creating_name(&slot, &mut name, 71, |borrowed| -> TestResult<usize> {
        assert_eq!(borrowed, &original);
        assert_eq!(borrowed.components().as_ptr(), outer);
        assert_eq!(
            borrowed.components().get(1).ok_or("missing leaf")?.as_ptr(),
            leaf
        );
        let callback = slot.read().map_err(|_| "slot poisoned")?;
        assert_eq!(&callback.0, &original);
        assert_eq!(callback.1, Some(71));
        Ok(19)
    })?;
    assert_eq!(result, 19);
    assert_eq!(name, original);
    assert_eq!(name.components().as_ptr(), outer);
    assert_eq!(
        name.components().get(1).ok_or("missing leaf")?.as_ptr(),
        leaf
    );
    assert_eq!(
        *slot.read().map_err(|_| "slot poisoned")?,
        (MountPath::root(), None)
    );
    Ok(())
}

#[test]
fn creator_restores_the_path_after_error_and_panic() -> TestResult {
    let slot = RwLock::new((MountPath::root(), None));
    let mut name = path("leaf");
    let original = name.clone();
    let error = with_creating_name(&slot, &mut name, (), |_| Err::<(), _>("write failed"));
    assert_eq!(error, Err("write failed"));
    assert_eq!(name, original);
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        with_creating_name(&slot, &mut name, (), |_| {
            std::panic::resume_unwind(Box::new("write panicked"))
        });
    }));
    assert!(panic.is_err());
    assert_eq!(name, original);
    assert_eq!(
        *slot.read().map_err(|_| "slot poisoned")?,
        (MountPath::root(), None)
    );
    Ok(())
}

#[test]
fn occupied_or_locked_slots_preserve_the_original_fallback_path() -> TestResult {
    let existing = path("existing");
    let slot = RwLock::new((existing.clone(), Some(1)));
    let mut name = path("fallback");
    let original = name.clone();
    with_creating_name(&slot, &mut name, 2, |borrowed| -> TestResult {
        assert_eq!(borrowed, &original);
        assert_eq!(slot.read().map_err(|_| "slot poisoned")?.0, existing);
        Ok(())
    })?;
    let read = slot.read().map_err(|_| "slot poisoned")?;
    with_creating_name(&slot, &mut name, 3, |borrowed| {
        assert_eq!(borrowed, &original);
    });
    drop(read);
    let write = slot.write().map_err(|_| "slot poisoned")?;
    with_creating_name(&slot, &mut name, 4, |borrowed| {
        assert_eq!(borrowed, &original);
    });
    drop(write);
    assert_eq!(name, original);
    assert_eq!(
        *slot.read().map_err(|_| "slot poisoned")?,
        (existing, Some(1))
    );
    Ok(())
}

#[test]
fn a_second_producer_does_not_block_the_callback_needed_by_the_first() -> TestResult {
    let slot = RwLock::new((MountPath::root(), None));
    let mut name = path("creator");
    std::thread::scope(|scope| {
        with_creating_name(&slot, &mut name, 7, |borrowed| -> TestResult {
            let (sent, received) = std::sync::mpsc::sync_channel(1);
            let slot = &slot;
            let producer = scope.spawn(move || -> TestResult {
                let mut other = path("other");
                with_creating_name(slot, &mut other, 8, |fallback| {
                    assert_eq!(fallback, &path("other"));
                });
                sent.send(())?;
                Ok(())
            });
            received.recv_timeout(std::time::Duration::from_secs(2))?;
            let (sent, received) = std::sync::mpsc::sync_channel(1);
            let callback = scope.spawn(move || -> TestResult {
                let callback = slot.read().map_err(|_| "slot poisoned")?;
                assert_eq!(*callback, (path("creator"), Some(7)));
                sent.send(())?;
                Ok(())
            });
            received.recv_timeout(std::time::Duration::from_secs(2))?;
            assert_eq!(borrowed, &path("creator"));
            producer.join().map_err(|_| "producer panicked")??;
            callback.join().map_err(|_| "callback panicked")??;
            Ok(())
        })
    })?;
    assert_eq!(name, path("creator"));
    assert_eq!(
        *slot.read().map_err(|_| "slot poisoned")?,
        (MountPath::root(), None)
    );
    Ok(())
}

#[test]
fn callback_names_match_exact_native_components_without_accepting_invalid_paths() -> TestResult {
    for name in ["leaf", "nested\\éclair", "one\\two\\three"] {
        let path = path(name);
        let wide: Vec<_> = name.encode_utf16().collect();
        assert!(callback_name_matches(&path, &wide));
        let (_, shorter) = wide.split_last().ok_or("test name is empty")?;
        assert!(!callback_name_matches(&path, shorter));
        let mut longer = wide.clone();
        longer.push(120);
        assert!(!callback_name_matches(&path, &longer));
    }
    assert!(callback_name_matches(&MountPath::root(), &[]));
    for name in ["", ".", "..", "a:b", "a/b", "a\0b", "a\\b"] {
        let component: Vec<_> = name.encode_utf16().flat_map(u16::to_le_bytes).collect();
        assert!(!callback_name_matches(
            &MountPath::root().child(component),
            &name.encode_utf16().collect::<Vec<_>>()
        ));
    }
    assert!(!callback_name_matches(
        &MountPath::root().child(vec![1]),
        &[1]
    ));
    assert!(callback_name_matches(
        &MountPath::root().child(vec![0, 216]),
        &[0xd800]
    ));
    assert!(!callback_name_matches(
        &path("Alpha"),
        &"alpha".encode_utf16().collect::<Vec<_>>()
    ));
    Ok(())
}
