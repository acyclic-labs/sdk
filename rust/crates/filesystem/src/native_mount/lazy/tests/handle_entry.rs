use super::*;

#[track_caller]
fn one_boundary<T>(
    operation: impl FnOnce() -> Result<T, MountSourceError>,
) -> Result<T, MountSourceError> {
    let before = adapter::callback_boundary_count();
    let result = operation();
    assert_eq!(
        adapter::callback_boundary_count() - before,
        1,
        "ordinary open-file operation reset its original invocation"
    );
    result
}

#[test]
fn ordinary_source_open_file_methods_keep_one_invocation_through_every_wrapper()
-> Result<(), Box<dyn std::error::Error>> {
    exercise_open_file(false)
}

#[test]
fn ordinary_authored_open_file_methods_keep_one_invocation_through_every_wrapper()
-> Result<(), Box<dyn std::error::Error>> {
    exercise_open_file(true)
}

fn exercise_open_file(authored: bool) -> Result<(), Box<dyn std::error::Error>> {
    let runtime = shared_runtime()?;
    let source = tempfile::tempdir()?;
    std::fs::write(source.path().join("file"), b"abcdef")?;
    let view = runtime.block_on(mounted_view(source.path(), "single-handle-invocation"))?;
    let path = mounted(&["file"]);
    if authored {
        view.write_range(&path, 0, Bytes::from_static(b"abcdef"))?;
    }
    let file = view.open_file(&path)?;
    assert_eq!(one_boundary(|| file.lookup())?.node.logical_bytes, 6);
    assert_eq!(one_boundary(|| file.read_range(0, 3))?.as_ref(), b"abc");
    assert_eq!(one_boundary(|| file.read_up_to(0, 9))?.as_ref(), b"abcdef");
    assert_eq!(
        one_boundary(|| file.seek(0, MountSeekTarget::Data))?,
        Some(0)
    );
    one_boundary(|| file.write_range(0, Bytes::from_static(b"Q")))?;
    one_boundary(|| file.append_range(6, Bytes::from_static(b"!")))?;
    one_boundary(|| file.resize(9))?;
    one_boundary(|| file.allocate_range(0, 1, MountRangeAllocation::ZeroRange { extend: false }))?;
    assert!(
        one_boundary(|| file.list_attributes(None, 2))?
            .names
            .is_empty()
    );
    let work = crate::NativeMaterializationWork::new(
        crate::performance::BOUNDARY_BUDGET,
        &CancellationToken::new(),
    )
    .map_err(|(error, _)| error)?;
    let before = adapter::callback_boundary_count();
    assert_eq!(file.lookup_with_meter(&work)?.node.logical_bytes, 9);
    assert_eq!(
        adapter::callback_boundary_count(),
        before,
        "explicit-work call created a replacement invocation"
    );
    assert_eq!(std::fs::read(source.path().join("file"))?, b"abcdef");
    Ok(())
}
