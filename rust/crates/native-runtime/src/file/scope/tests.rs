use super::*;
use crate::file::FILE_IDENTITY_QUERIES;

fn queries() -> usize {
    FILE_IDENTITY_QUERIES.with(Cell::get)
}

fn expect_queries(before: usize, expected: usize) -> io::Result<()> {
    let actual = queries() - before;
    if actual != expected {
        return Err(io::Error::other(format!(
            "expected {expected} file identity queries, observed {actual}"
        )));
    }
    Ok(())
}

fn read_expected(file: &File, expected: &[u8]) -> io::Result<()> {
    let mut bytes = [0; 16];
    let count = read_at(file, 0, &mut bytes)?;
    if bytes.get(..count) != Some(expected) {
        return Err(io::Error::other("positional read returned different bytes"));
    }
    Ok(())
}

fn expect_restored(node: *const ActiveFileScopeNode) -> io::Result<()> {
    if ACTIVE_FILES.with(Cell::get) != node {
        // Refuse before another operation could wait on its own lost scope.
        return Err(io::Error::other("file admission scope was not restored"));
    }
    Ok(())
}

#[test]
fn owned_control_reuses_only_current_file_health() -> io::Result<()> {
    let original = tempfile::NamedTempFile::new()?;
    let foreign = tempfile::NamedTempFile::new()?;
    let native = NativeFile::from_file(original.reopen()?, None).map_err(|(error, _)| error)?;
    let other = NativeFile::from_file(foreign.reopen()?, None).map_err(|(error, _)| error)?;
    let alias = original.reopen()?;
    let payload = Arc::clone(&native.file);
    let health = native.health.clone();
    let (release, released) = std::sync::mpsc::sync_channel(1);
    let (checked, verification) = std::sync::mpsc::sync_channel(1);
    let mut control = native.control_async(
        move |file, _| {
            released.recv().map_err(io::Error::other)?;
            let current = ACTIVE_FILES.with(Cell::get);
            let before = queries();
            write_all_at(file, 0, b"owned", None)?;
            read_expected(file, b"owned")?;
            expect_queries(before, 0)?;

            // A different File object must identify even an alias afresh.
            let before = queries();
            read_expected(&alias, b"owned")?;
            expect_queries(before, 1)?;

            // The foreign admission retains its own health, then restores
            // the original worker's scope without another identity query.
            let before = queries();
            with_file_admission(other.as_file(), || {
                write_all_at(other.as_file(), 0, b"foreign", None)?;
                read_expected(other.as_file(), b"foreign")
            })?;
            expect_queries(before, 1)?;
            expect_restored(current)?;

            // An intervening scope without a file must not search ancestors
            // for the optimization. Its ordinary health lookup still works.
            let before = queries();
            with_file_scope(Some(&other.health), || read_expected(file, b"owned"))?;
            expect_queries(before, 1)?;
            expect_restored(current)?;

            poison_file_health(&other.file, &other.health);
            let before = queries();
            if read_at(other.as_file(), 0, &mut [0]).is_ok() {
                return Err(io::Error::other("foreign uncertain file was read"));
            }
            expect_queries(before, 1)?;
            let before = queries();
            read_expected(file, b"owned")?;
            expect_queries(before, 0)?;

            // Reusing health must preserve its live uncertainty check, and
            // the alias must observe the same refusal through the registry.
            poison_file_health(&payload, &health);
            let before = queries();
            if write_all_at(file, 0, b"wrong", None).is_ok() {
                return Err(io::Error::other("owned uncertain file was written"));
            }
            expect_queries(before, 0)?;
            let before = queries();
            if write_all_at(&alias, 0, b"wrong", None).is_ok() {
                return Err(io::Error::other("uncertain alias was written"));
            }
            expect_queries(before, 1)?;
            // Poisoning also refuses the original completion. Keep a direct
            // event proving every preceding callback check finished first.
            checked.send(()).map_err(io::Error::other)
        },
        None,
    )?;
    // Fix admission before allowing poison, then observe only after every
    // callback check. The result cannot race the initial submission poll.
    crate::tests::submit(&mut control);
    release.send(()).map_err(io::Error::other)?;
    verification.recv().map_err(io::Error::other)?;
    assert_eq!(
        crate::tests::complete(control).unwrap_err().to_string(),
        "prior native I/O completion on this file is uncertain"
    );
    assert_eq!(std::fs::read(original.path())?, b"owned");
    assert_eq!(std::fs::read(foreign.path())?, b"foreign");
    Ok(())
}

#[test]
fn borrowed_file_scope_restores_admission_after_unwind() -> io::Result<()> {
    let original = tempfile::tempfile()?;
    let foreign = tempfile::tempfile()?;
    let health = shared_file_health(&original, None)?;
    let other = shared_file_health(&foreign, None)?;
    let before_scope = ACTIVE_FILES.with(Cell::get);
    with_owned_file_scope(&original, &health, || {
        let current = ACTIVE_FILES.with(Cell::get);
        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            with_owned_file_scope(&foreign, &other, || panic!("scope unwind control"));
        }));
        if unwound.is_ok() {
            return Err(io::Error::other("scope unwind control did not unwind"));
        }
        expect_restored(current)?;
        let before = queries();
        write_all_at(&original, 0, b"restored", None)?;
        read_expected(&original, b"restored")?;
        expect_queries(before, 0)
    })?;
    expect_restored(before_scope)?;
    let before = queries();
    read_expected(&original, b"restored")?;
    expect_queries(before, 1)
}
