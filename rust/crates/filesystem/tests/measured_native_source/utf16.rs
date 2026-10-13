use super::*;

#[cfg(windows)]
#[tokio::test]
async fn finite_native_utf16_paths_preserve_unpaired_names_and_restart() -> TestResult {
    use std::os::windows::ffi::OsStringExt as _;
    let directory = tempfile::tempdir()?;
    let units = [u16::from(b'o'), 0xd800, u16::from(b'p')];
    let native_name = std::ffi::OsString::from_wide(&units);
    std::fs::write(directory.path().join(native_name), b"opaque")?;
    let limits = VolumeLimits::default();
    let bytes = units
        .into_iter()
        .flat_map(u16::to_le_bytes)
        .collect::<Vec<_>>();
    let name = LogicalName::new(
        NameEncoding::WindowsUtf16Le,
        bytes,
        limits.maximum_component_bytes,
    )?;
    let path = NamespacePath::new(vec![name.clone()], limits)?;
    let source = NativeDemandSource::open(
        std::borrow::Cow::Owned(std::path::Path::new(directory.path()).to_path_buf()),
        FilesystemProfile::Windows,
        limits,
        None,
        None,
        acyclic_fs::WorkBudget::UNBOUNDED,
        &acyclic_fs::CancellationToken::new(),
    )
    .await
    .map(|receipt| receipt.value)
    .map_err(|failure| failure.error)?;
    let token = CancellationToken::new();
    let budget = finite_budget()?;
    let node = source
        .lookup(source.reference(), &path, budget, &token)
        .await?
        .value
        .ok_or("opaque file absent")?;
    let mut page = source
        .list_page(source.reference(), &requested("")?, None, 8, budget, &token)
        .await?;
    {
        let allowance = page.work.remaining(budget)?;
        let cancellation = &token;
        let mut accounting = allowance.accounting(cancellation);
        let result = match page.value.publication.as_deref_mut() {
            Some(publication) => publication.accept(&mut accounting),
            None => Ok(()),
        };
        allowance.finish(result, &accounting)
    }?;
    assert_eq!(page.value.entries.len(), 1);
    assert_eq!(page.value.entries[0].name, name);
    page.work.verify(budget)?;
    let read = source
        .read_range(
            source.reference(),
            &path,
            node.version,
            0,
            6,
            budget,
            &token,
        )
        .await?;
    assert_eq!(read.value.bytes.as_ref(), b"opaque");
    read.work.verify(budget)?;
    let mut limited = budget;
    limited.peak_allocation_bytes = read
        .work
        .peak_allocation_bytes
        .checked_sub(1)
        .ok_or("no native live buffer")?;
    let failure = source
        .read_range(
            source.reference(),
            &path,
            node.version,
            0,
            6,
            limited,
            &token,
        )
        .await
        .err()
        .ok_or("native UTF16 path/read allocation exceeded allowance")?;
    assert!(matches!(failure.error, DemandError::Work(_)));
    failure.work.verify(limited)?;
    let reopened = NativeDemandSource::open(
        std::borrow::Cow::Owned(std::path::Path::new(directory.path()).to_path_buf()),
        FilesystemProfile::Windows,
        limits,
        Some(source.reference()),
        None,
        acyclic_fs::WorkBudget::UNBOUNDED,
        &acyclic_fs::CancellationToken::new(),
    )
    .await
    .map(|receipt| receipt.value)
    .map_err(|failure| failure.error)?;
    let read = reopened
        .read_range(
            reopened.reference(),
            &path,
            node.version,
            0,
            6,
            budget,
            &token,
        )
        .await?;
    assert_eq!(read.value.bytes.as_ref(), b"opaque");
    read.work.verify(budget)?;
    Ok(())
}
